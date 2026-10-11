/*-------------------------------------------------------------------------
 *
 * pg_flashback.c
 * PostgreSQL background worker & logical decoding wrapper for flashback.
 *
 * License: BSD 2-Clause License.
 * Copyright (c) 2026 Oussama CHAOUACHI (DBACCOMPANY)
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h" 

#include "executor/spi.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/proc.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/snapmgr.h"
#include "utils/wait_event.h"

#include "catalog/pg_type.h"
#include "funcapi.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/timestamp.h"
#include <time.h>

PG_MODULE_MAGIC;

void _PG_init(void);
PGDLLEXPORT void pg_flashback_main(Datum main_arg);


PG_FUNCTION_INFO_V1(pg_flashback_enable_table);
PG_FUNCTION_INFO_V1(pg_flashback_disable_table);
PG_FUNCTION_INFO_V1(pg_flashback_set_default_undo_retention);
PG_FUNCTION_INFO_V1(pg_flashback_set_table_undo_retention);
PG_FUNCTION_INFO_V1(pg_flashback_flashback_horizon);
PG_FUNCTION_INFO_V1(pg_flashback_get_retention_horizon);
PG_FUNCTION_INFO_V1(pg_flashback_flashback_table);
PG_FUNCTION_INFO_V1(pg_flashback_flashback_to_new_table);
PG_FUNCTION_INFO_V1(pg_flashback_table_as_of);
PG_FUNCTION_INFO_V1(pg_flashback_changes_between);
PG_FUNCTION_INFO_V1(pg_flashback_get_row_at);
PG_FUNCTION_INFO_V1(pg_flashback_get_transactions);
PG_FUNCTION_INFO_V1(pg_flashback_set_storage_mode);


static void enter_silent_mode(void) {
  SPI_execute("SET LOCAL log_min_messages = 'fatal'", false, 0);
  SPI_execute("SET LOCAL client_min_messages = 'error'", false, 0);
  SPI_execute("SET LOCAL log_error_verbosity = 'terse'", false, 0);
}


static volatile sig_atomic_t got_sigterm = false;
static char *pg_flashback_database_names = "postgres";
static char *pg_flashback_table_storage_type = "logged";


static void pg_flashback_sigterm(SIGNAL_ARGS) {
  int save_errno = errno;
  got_sigterm = true;
  if (MyProc)
    SetLatch(&MyProc->procLatch);
  errno = save_errno;
}


void pg_flashback_main(Datum main_arg) {
  static char last_cleanup_storage_type[64] = "logged";
  static time_t last_cleanup = 0;

  char *dbname = MyBgworkerEntry->bgw_extra;

  
  pqsignal(SIGTERM, pg_flashback_sigterm);
  BackgroundWorkerUnblockSignals();

  BackgroundWorkerInitializeConnection(dbname, NULL, 0);

  
  StartTransactionCommand();
  SPI_connect();

  SPI_execute("SET SESSION log_min_messages = 'fatal'", false, 0);
  SPI_execute("SET SESSION client_min_messages = 'error'", false, 0);
  SPI_execute("SET SESSION log_error_verbosity = 'terse'", false, 0);
  SPI_execute("SET SESSION log_statement = 'none'", false, 0);
  SPI_execute("SET SESSION log_min_duration_statement = -1", false, 0);

  SPI_finish();
  CommitTransactionCommand();

  elog(INFO, "pg_flashback worker started for database: %s", dbname);

  
  while (!got_sigterm) {
    int ret;
    bool exists;

    StartTransactionCommand();
    SPI_connect();
    PushActiveSnapshot(GetTransactionSnapshot());

    
    ret = SPI_execute("SELECT to_regclass('flashback_internal.fb_event')", true,
                      0);
    exists = false;
    if (ret == SPI_OK_SELECT && SPI_processed > 0) {
      char *val = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1);
      if (val != NULL)
        exists = true;
    }

    if (!exists) {
      
      SPI_finish();
      PopActiveSnapshot();
      CommitTransactionCommand();
      (void)WaitLatch(&MyProc->procLatch,
                      WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH, 5000L,
                      0);
      ResetLatch(&MyProc->procLatch);
      continue;
    }

    
    if (pg_strcasecmp(pg_flashback_table_storage_type,
                      last_cleanup_storage_type) != 0) {
      if (pg_strcasecmp(pg_flashback_table_storage_type, "unlogged") == 0) {
        SPI_execute("ALTER TABLE flashback_internal.fb_event SET UNLOGGED",
                    false, 0);
        strncpy(last_cleanup_storage_type, "unlogged",
                sizeof(last_cleanup_storage_type));
      } else {
        SPI_execute("ALTER TABLE flashback_internal.fb_event SET LOGGED", false,
                    0);
        strncpy(last_cleanup_storage_type, "logged",
                sizeof(last_cleanup_storage_type));
      }
    }

    
    {
      char check_slot_query[256];
      snprintf(check_slot_query, sizeof(check_slot_query),
               "SELECT count(*) FROM pg_replication_slots WHERE slot_name = "
               "'flashback_slot_%s' AND database = current_database()",
               dbname);

      ret = SPI_execute(check_slot_query, true, 0);
      if (ret == SPI_OK_SELECT && SPI_processed > 0) {
        bool slot_exists = (DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[0],
                                                        SPI_tuptable->tupdesc,
                                                        1, &slot_exists)) > 0);
        if (!slot_exists) {
          {
            char create_slot_query[256];
            elog(INFO, "Creating replication slot flashback_slot_%s", dbname);
            snprintf(create_slot_query, sizeof(create_slot_query),
                     "SELECT "
                     "pg_create_logical_replication_slot('flashback_slot_%s', "
                     "'wal2json')",
                     dbname);
            SPI_execute(create_slot_query, false, 0);
          }
        }
      }
    }

    
    {
      const char *query_template =
          "INSERT INTO flashback_internal.fb_event ( "
          "  lsn, seq_id, commit_ts, table_schema, table_name, operation, "
          "pk_values, old_values, new_values "
          ") "
          "SELECT "
          "  f.lsn, f.seq_id, f.commit_ts, f.item_schema, f.item_table, "
          "  CASE WHEN f.change_obj->>'kind' = 'insert' THEN 'I' "
          "       WHEN f.change_obj->>'kind' = 'update' THEN 'U' "
          "       WHEN f.change_obj->>'kind' = 'delete' THEN 'D' "
          "  END, "
          "  flashback_internal.extract_pk(f.item_schema, f.item_table, "
          "f.change_obj), "
          "  "
          "flashback_internal.wal2json_zip(f.change_obj->'oldkeys'->'keynames',"
          " f.change_obj->'oldkeys'->'keyvalues'), "
          "  flashback_internal.wal2json_zip(f.change_obj->'columnnames', "
          "f.change_obj->'columnvalues') "
          "FROM ( "
          "  WITH raw_changes AS ( "
          "    SELECT lsn, xid, data::jsonb as json_data "
          "    FROM pg_logical_slot_get_changes('flashback_slot_%s', NULL, "
          "100, "
          "      'format-version', '1', 'include-pk', 'true', 'include-lsn', "
          "'true', 'include-timestamp', 'true' "
          "    ) "
          "  ) "
          "  SELECT "
          "    r.lsn, (r.json_data->>'timestamp')::timestamptz as commit_ts, "
          "    c.value as change_obj, c.value->>'schema' as item_schema, "
          "c.value->>'table' as item_table, c.seq_id "
          "  FROM raw_changes r "
          "  CROSS JOIN LATERAL jsonb_array_elements(r.json_data->'change') "
          "WITH ORDINALITY c(value, seq_id) "
          ") f "
          "JOIN flashback_internal.fb_config conf ON (f.item_schema || '.' || "
          "f.item_table) = conf.config_key "
          "WHERE f.item_schema NOT LIKE 'flashback%%' "
          "  AND conf.enabled = true "
          "  AND f.commit_ts >= conf.enabled_at "
          "ON CONFLICT DO NOTHING";

      char *query;
      int res;

      query = psprintf(query_template, dbname);
      res = SPI_execute(query, false, 0);

      if (res < 0)
        elog(ERROR, "pg_flashback: SPI_execute failed with error code %d", res);
    }

    
    {
      time_t now = time(NULL);

      
      if (last_cleanup == 0 || (now - last_cleanup) >= 15) {
        SPI_execute("SELECT flashback_internal.maintain_partitions()", false,
                    0);
        last_cleanup = now;
      }
    }

    SPI_finish();
    PopActiveSnapshot();
    CommitTransactionCommand();

    
    (void)WaitLatch(&MyProc->procLatch,
                    WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH, 1000L, 0);
    ResetLatch(&MyProc->procLatch);
  }

  proc_exit(1);
}


void _PG_init(void) {
  BackgroundWorker worker;
  char *rawstring;
  char *dbname;
  char *saveptr;

  
  DefineCustomStringVariable(
      "pg_flashback.database_names",
      "Comma separated list of databases to check for flashback events.", NULL,
      &pg_flashback_database_names, "postgres", PGC_POSTMASTER, 0, NULL, NULL,
      NULL);

  
  DefineCustomStringVariable(
      "pg_flashback.table_storage_type",
      "Storage type for event table: 'logged' or 'unlogged'.", NULL,
      &pg_flashback_table_storage_type, "logged", PGC_SIGHUP, 0, NULL, NULL,
      NULL);

  
  rawstring = pstrdup(pg_flashback_database_names);

  
  for (dbname = strtok_r(rawstring, ",", &saveptr); dbname != NULL;
       dbname = strtok_r(NULL, ",", &saveptr)) {

    
    while (*dbname == ' ')
      dbname++;

    {
      int len = strlen(dbname);
      while (len > 0 && dbname[len - 1] == ' ') {
        dbname[len - 1] = '\0';
        len--;
      }
    }

    if (strlen(dbname) == 0)
      continue;

    memset(&worker, 0, sizeof(worker));
    worker.bgw_flags =
        BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
    worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
    worker.bgw_restart_time = 1;
    snprintf(worker.bgw_library_name, BGW_MAXLEN, "pg_flashback");
    snprintf(worker.bgw_function_name, BGW_MAXLEN, "pg_flashback_main");

    snprintf(worker.bgw_name, BGW_MAXLEN, "pg_flashback worker (%s)", dbname);
    snprintf(worker.bgw_type, BGW_MAXLEN, "pg_flashback worker");

    
    strncpy(worker.bgw_extra, dbname, BGW_EXTRALEN);

    elog(LOG, "pg_flashback: Registering background worker for database '%s'",
         dbname);
    RegisterBackgroundWorker(&worker);
  }
}




Datum pg_flashback_enable_table(PG_FUNCTION_ARGS) {
  text *table_name = PG_GETARG_TEXT_P(0);
  char *table_str = text_to_cstring(table_name);
  char *query;

  SPI_connect();
  enter_silent_mode();

  if (PG_ARGISNULL(1))
    query = psprintf("SELECT flashback_internal.enable_table_logic('%s', NULL)",
                     table_str);
  else {
    Interval *ret = PG_GETARG_INTERVAL_P(1);
    query = psprintf(
        "SELECT flashback_internal.enable_table_logic('%s', '%s'::interval)",
        table_str,
        DatumGetCString(
            DirectFunctionCall1(interval_out, PointerGetDatum(ret))));
  }

  SPI_execute(query, true, 0);
  {
    char *val = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1);
    text *res = cstring_to_text(val);
    SPI_finish();
    PG_RETURN_TEXT_P(res);
  }
}


Datum pg_flashback_disable_table(PG_FUNCTION_ARGS) {
  char *table_str = text_to_cstring(PG_GETARG_TEXT_P(0));
  SPI_connect();
  enter_silent_mode();
  SPI_execute(psprintf("SELECT flashback_internal.disable_table_logic('%s')",
                       table_str),
              true, 0);
  {
    char *val = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1);
    text *res = cstring_to_text(val);
    SPI_finish();
    PG_RETURN_TEXT_P(res);
  }
}


Datum pg_flashback_set_default_undo_retention(PG_FUNCTION_ARGS) {
  Interval *ret = PG_GETARG_INTERVAL_P(0);
  char *interval_str =
      DatumGetCString(DirectFunctionCall1(interval_out, PointerGetDatum(ret)));
  SPI_connect();
  enter_silent_mode();
  SPI_execute(
      psprintf(
          "SELECT "
          "flashback_internal.set_default_undo_retention_logic('%s'::interval)",
          interval_str),
      true, 0);
  {
    char *val = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1);
    text *res = cstring_to_text(val);
    SPI_finish();
    PG_RETURN_TEXT_P(res);
  }
}


Datum pg_flashback_set_table_undo_retention(PG_FUNCTION_ARGS) {
  char *table_str = text_to_cstring(PG_GETARG_TEXT_P(0));
  Interval *ret = PG_GETARG_INTERVAL_P(1);
  char *interval_str =
      DatumGetCString(DirectFunctionCall1(interval_out, PointerGetDatum(ret)));
  SPI_connect();
  enter_silent_mode();
  SPI_execute(
      psprintf("SELECT flashback_internal.set_table_undo_retention_logic('%s', "
               "'%s'::interval)",
               table_str, interval_str),
      true, 0);
  {
    char *val = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1);
    text *res = cstring_to_text(val);
    SPI_finish();
    PG_RETURN_TEXT_P(res);
  }
}


Datum pg_flashback_flashback_table(PG_FUNCTION_ARGS) {
  char *table_str = text_to_cstring(PG_GETARG_TEXT_P(0));
  TimestampTz target_ts = PG_GETARG_TIMESTAMPTZ(1);
  char *ts_str = DatumGetCString(
      DirectFunctionCall1(timestamptz_out, TimestampTzGetDatum(target_ts)));
  char *query;

  SPI_connect();
  enter_silent_mode();

  query = psprintf("SELECT flashback_internal.flashback_table_logic('%s', "
                   "'%s'::timestamptz)",
                   table_str, ts_str);

  SPI_execute(query, true, 0);
  {
    char *val = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1);
    text *res = cstring_to_text(val);
    SPI_finish();
    PG_RETURN_TEXT_P(res);
  }
}


Datum pg_flashback_flashback_to_new_table(PG_FUNCTION_ARGS) {
  char *table_str = text_to_cstring(PG_GETARG_TEXT_P(0));
  char *new_table_str = text_to_cstring(PG_GETARG_TEXT_P(1));
  TimestampTz target_ts = PG_GETARG_TIMESTAMPTZ(2);
  char *ts_str = DatumGetCString(
      DirectFunctionCall1(timestamptz_out, TimestampTzGetDatum(target_ts)));
  char *query;

  SPI_connect();
  enter_silent_mode();

  query =
      psprintf("SELECT flashback_internal.flashback_to_new_table_logic('%s', "
               "'%s', '%s'::timestamptz)",
               table_str, new_table_str, ts_str);

  SPI_execute(query, true, 0);
  {
    char *val = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1);
    text *res = cstring_to_text(val);
    SPI_finish();
    PG_RETURN_TEXT_P(res);
  }
}


Datum pg_flashback_table_as_of(PG_FUNCTION_ARGS) {
  FuncCallContext *funcctx;
  if (SRF_IS_FIRSTCALL()) {
    MemoryContext oldcontext;
    char *table_str;
    char *ts_str;
    char *query;

    funcctx = SRF_FIRSTCALL_INIT();
    oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

    SPI_connect();
    enter_silent_mode();

    table_str = text_to_cstring(PG_GETARG_TEXT_P(0));
    ts_str = DatumGetCString(DirectFunctionCall1(
        timestamptz_out, TimestampTzGetDatum(PG_GETARG_TIMESTAMPTZ(1))));
    query = psprintf("SELECT * FROM flashback_internal.table_as_of_logic('%s', "
                     "'%s'::timestamptz)",
                     table_str, ts_str);

    SPI_execute(query, true, 0);
    funcctx->max_calls = SPI_processed;
    funcctx->user_fctx = SPI_tuptable;

    MemoryContextSwitchTo(oldcontext);
  }

  funcctx = SRF_PERCALL_SETUP();
  if (funcctx->call_cntr < funcctx->max_calls) {
    SPITupleTable *tuptable = (SPITupleTable *)funcctx->user_fctx;
    bool isnull;
    Datum res = SPI_getbinval(tuptable->vals[funcctx->call_cntr],
                              tuptable->tupdesc, 1, &isnull);
    SRF_RETURN_NEXT(funcctx, res);
  } else {
    SPI_finish();
    SRF_RETURN_DONE(funcctx);
  }
}


Datum pg_flashback_get_row_at(PG_FUNCTION_ARGS) {
  char *table_str = text_to_cstring(PG_GETARG_TEXT_P(0));
  char *pk_json =
      DatumGetCString(DirectFunctionCall1(jsonb_out, PG_GETARG_DATUM(1)));
  char *ts_str = DatumGetCString(DirectFunctionCall1(
      timestamptz_out, TimestampTzGetDatum(PG_GETARG_TIMESTAMPTZ(2))));
  bool isnull;
  Datum res;

  SPI_connect();
  enter_silent_mode();
  SPI_execute(psprintf("SELECT flashback_internal.get_row_at_logic('%s', "
                       "'%s'::jsonb, '%s'::timestamptz)",
                       table_str, pk_json, ts_str),
              true, 0);

  if (SPI_processed == 0) {
    SPI_finish();
    PG_RETURN_NULL();
  }

  res = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull);
  if (isnull) {
    SPI_finish();
    PG_RETURN_NULL();
  }

  SPI_finish();
  PG_RETURN_DATUM(res);
}


Datum pg_flashback_set_storage_mode(PG_FUNCTION_ARGS) {
  char *mode = text_to_cstring(PG_GETARG_TEXT_P(0));
  SPI_connect();
  enter_silent_mode();
  SPI_execute(
      psprintf("SELECT flashback_internal.set_storage_mode_logic('%s')", mode),
      true, 0);
  {
    char *val = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1);
    text *res = cstring_to_text(val);
    SPI_finish();
    PG_RETURN_TEXT_P(res);
  }
}


Datum pg_flashback_get_retention_horizon(PG_FUNCTION_ARGS) {
  char *table_str = text_to_cstring(PG_GETARG_TEXT_P(0));
  bool isnull;
  Datum res;

  SPI_connect();
  enter_silent_mode();
  SPI_execute(
      psprintf("SELECT flashback_internal.get_retention_horizon_logic('%s')",
               table_str),
      true, 0);

  if (SPI_processed == 0) {
    SPI_finish();
    PG_RETURN_NULL();
  }

  res = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull);
  if (isnull) {
    SPI_finish();
    PG_RETURN_NULL();
  }

  SPI_finish();
  PG_RETURN_TIMESTAMPTZ(DatumGetTimestampTz(res));
}


static Datum srf_handler(PG_FUNCTION_ARGS, const char *query_tmpl) {
  FuncCallContext *funcctx;
  if (SRF_IS_FIRSTCALL()) {
    MemoryContext oldcontext;
    TupleDesc tupdesc;
    char *table_str;
    char *arg1 = NULL;
    char *arg2 = NULL;
    char *query;

    funcctx = SRF_FIRSTCALL_INIT();
    oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

    SPI_connect();
    enter_silent_mode();

    table_str = text_to_cstring(PG_GETARG_TEXT_P(0));

    if (PG_NARGS() >= 2)
      arg1 = DatumGetCString(DirectFunctionCall1(
          timestamptz_out, TimestampTzGetDatum(PG_GETARG_TIMESTAMPTZ(1))));
    if (PG_NARGS() >= 3)
      arg2 = DatumGetCString(DirectFunctionCall1(
          timestamptz_out, TimestampTzGetDatum(PG_GETARG_TIMESTAMPTZ(2))));

    if (arg2)
      query = psprintf(query_tmpl, table_str, arg1, arg2);
    else if (arg1)
      query = psprintf(query_tmpl, table_str, arg1);
    else
      query = psprintf(query_tmpl, table_str);

    SPI_execute(query, true, 0);
    funcctx->max_calls = SPI_processed;
    funcctx->user_fctx = SPI_tuptable;

    if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
      ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                      errmsg("function returning record called in context that "
                             "cannot accept type record")));

    funcctx->tuple_desc = BlessTupleDesc(tupdesc);
    funcctx->attinmeta = TupleDescGetAttInMetadata(tupdesc);
    MemoryContextSwitchTo(oldcontext);
  }

  funcctx = SRF_PERCALL_SETUP();
  if (funcctx->call_cntr < funcctx->max_calls) {
    SPITupleTable *tuptable = (SPITupleTable *)funcctx->user_fctx;
    HeapTuple tuple = tuptable->vals[funcctx->call_cntr];
    Datum res = heap_copy_tuple_as_datum(tuple, funcctx->tuple_desc);
    SRF_RETURN_NEXT(funcctx, res);
  } else {
    SPI_finish();
    SRF_RETURN_DONE(funcctx);
  }
}


Datum pg_flashback_flashback_horizon(PG_FUNCTION_ARGS) {
  return srf_handler(
      fcinfo, "SELECT * FROM flashback_internal.flashback_horizon_logic('%s')");
}
Datum pg_flashback_changes_between(PG_FUNCTION_ARGS) {
  return srf_handler(
      fcinfo, "SELECT * FROM flashback_internal.changes_between_logic('%s', "
              "'%s'::timestamptz, '%s'::timestamptz)");
}
Datum pg_flashback_get_transactions(PG_FUNCTION_ARGS) {
  return srf_handler(
      fcinfo, "SELECT * FROM flashback_internal.get_transactions_logic('%s', "
              "'%s'::timestamptz, '%s'::timestamptz)");
}


