/*-------------------------------------------------------------------------
 *
 * pg_flashback.c
 * PostgreSQL background worker & logical decoding wrapper for flashback.
 *
 * License: GNU General Public License v3.0 (GPL-3.0).
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

#include "access/htup_details.h"
#include "catalog/pg_type.h"
#include "funcapi.h"
#include "utils/datum.h"
#include "utils/guc.h"
#include "utils/jsonb.h"
#include "utils/lsyscache.h"
#include "utils/timestamp.h"
#include <limits.h>
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


/*
 * Reduce only *client* chatter from the wrapped SPI calls. We deliberately do
 * NOT touch log_statement, log_min_messages, log_error_verbosity or
 * log_min_duration_statement: suppressing server-side logging would destroy the
 * audit trail for data-rewriting operations (flashback_table, enable/disable,
 * DDL) — exactly the operations that most need to be auditable.
 */
static void enter_silent_mode(void) {
  SPI_execute("SET LOCAL client_min_messages = 'warning'", false, 0);
}


static volatile sig_atomic_t got_sigterm = false;
static char *pg_flashback_database_names = "postgres";
static char *pg_flashback_table_storage_type = "logged";
static int pg_flashback_max_changes_per_cycle = 5000;


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
  char *slot_name;


  pqsignal(SIGTERM, pg_flashback_sigterm);
  BackgroundWorkerUnblockSignals();

  BackgroundWorkerInitializeConnection(dbname, NULL, 0);

  /* Slot name is a logical-decoding identifier derived from the (trusted,
   * postmaster-configured) database name. It is passed to the queries below as
   * a bound parameter, never concatenated into SQL text. */
  slot_name = psprintf("flashback_slot_%s", dbname);


  StartTransactionCommand();
  SPI_connect();

  /* Quiet client-side NOTICE spam only; leave server audit logging intact. */
  SPI_execute("SET SESSION client_min_messages = 'warning'", false, 0);

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
      /* The extension is not present (e.g. it was dropped). Release our
       * replication slot so it stops pinning WAL, then idle until the
       * extension (re)appears. Without this, DROP EXTENSION would leave an
       * orphaned logical slot that retains WAL indefinitely. */
      {
        Oid dargtypes[1] = {TEXTOID};
        Datum dvalues[1] = {CStringGetTextDatum(slot_name)};
        SPI_execute_with_args(
            "SELECT pg_drop_replication_slot($1) FROM pg_replication_slots "
            "WHERE slot_name = $1 AND database = current_database()",
            1, dargtypes, dvalues, NULL, false, 0);
      }

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
      Oid argtypes[1] = {TEXTOID};
      Datum values[1] = {CStringGetTextDatum(slot_name)};

      ret = SPI_execute_with_args(
          "SELECT count(*) FROM pg_replication_slots WHERE slot_name = $1 "
          "AND database = current_database()",
          1, argtypes, values, NULL, true, 0);
      if (ret == SPI_OK_SELECT && SPI_processed > 0) {
        bool slot_exists = (DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[0],
                                                        SPI_tuptable->tupdesc,
                                                        1, &slot_exists)) > 0);
        if (!slot_exists) {
          Oid cargtypes[1] = {TEXTOID};
          Datum cvalues[1] = {CStringGetTextDatum(slot_name)};
          elog(INFO, "Creating replication slot %s", slot_name);
          SPI_execute_with_args(
              "SELECT pg_create_logical_replication_slot($1, 'wal2json')",
              1, cargtypes, cvalues, NULL, false, 0);
        }
      }
    }


    {
      const char *query =
          "INSERT INTO flashback_internal.fb_event ( "
          "  lsn, seq_id, xid, commit_ts, table_schema, table_name, operation, "
          "pk_values, old_values, new_values "
          ") "
          "SELECT "
          "  f.lsn, f.seq_id, f.xid, f.commit_ts, f.item_schema, f.item_table, "
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
          "    FROM pg_logical_slot_get_changes($1, NULL, "
          "$2, "
          "      'format-version', '1', 'include-pk', 'true', 'include-lsn', "
          "'true', 'include-timestamp', 'true' "
          "    ) "
          "  ) "
          "  SELECT "
          "    r.lsn, r.xid::text AS xid, (r.json_data->>'timestamp')::timestamptz as commit_ts, "
          "    c.value as change_obj, c.value->>'schema' as item_schema, "
          "c.value->>'table' as item_table, c.seq_id "
          "  FROM raw_changes r "
          "  CROSS JOIN LATERAL jsonb_array_elements(r.json_data->'change') "
          "WITH ORDINALITY c(value, seq_id) "
          ") f "
          "JOIN flashback_internal.fb_config conf ON (f.item_schema || '.' || "
          "f.item_table) = conf.config_key "
          "WHERE f.item_schema NOT LIKE 'flashback%' "
          "  AND conf.enabled = true "
          "  AND f.commit_ts >= conf.enabled_at "
          "ON CONFLICT DO NOTHING";

      Oid argtypes[2] = {TEXTOID, INT4OID};
      Datum values[2] = {CStringGetTextDatum(slot_name),
                         Int32GetDatum(pg_flashback_max_changes_per_cycle)};
      int res;

      res = SPI_execute_with_args(query, 2, argtypes, values, NULL, false, 0);

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

  DefineCustomIntVariable(
      "pg_flashback.max_changes_per_cycle",
      "Maximum logical-decoding changes consumed from the slot per worker "
      "cycle.",
      NULL, &pg_flashback_max_changes_per_cycle, 5000, 1, INT_MAX, PGC_SIGHUP,
      0, NULL, NULL, NULL);


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

    /* snprintf guarantees NUL-termination; a plain strncpy would not if the
     * database name were >= BGW_EXTRALEN bytes. */
    snprintf(worker.bgw_extra, BGW_EXTRALEN, "%s", dbname);

    elog(LOG, "pg_flashback: Registering background worker for database '%s'",
         dbname);
    RegisterBackgroundWorker(&worker);
  }
}


/*
 * All user-facing entry points below pass their arguments to the
 * flashback_internal.*_logic functions as BOUND PARAMETERS ($1, $2, ...) via
 * SPI_execute_with_args. No caller-controlled value is ever concatenated into
 * SQL text, which removes the SQL-injection surface that string-building with
 * psprintf('%s', ...) created. Passing typed Datums directly also preserves the
 * exact argument type instead of round-tripping through *_out/*_in text casts.
 */

static text *fb_return_text_col1(void) {
  char *val = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1);
  return val ? cstring_to_text(val) : NULL;
}


Datum pg_flashback_enable_table(PG_FUNCTION_ARGS) {
  text *table_name = PG_GETARG_TEXT_PP(0);
  text *res;

  SPI_connect();
  enter_silent_mode();

  if (PG_ARGISNULL(1)) {
    Oid argtypes[1] = {TEXTOID};
    Datum values[1] = {PointerGetDatum(table_name)};
    SPI_execute_with_args(
        "SELECT flashback_internal.enable_table_logic($1, NULL)",
        1, argtypes, values, NULL, true, 0);
  } else {
    Interval *ret = PG_GETARG_INTERVAL_P(1);
    Oid argtypes[2] = {TEXTOID, INTERVALOID};
    Datum values[2] = {PointerGetDatum(table_name), IntervalPGetDatum(ret)};
    SPI_execute_with_args(
        "SELECT flashback_internal.enable_table_logic($1, $2)",
        2, argtypes, values, NULL, true, 0);
  }

  res = fb_return_text_col1();
  SPI_finish();
  if (res == NULL)
    PG_RETURN_NULL();
  PG_RETURN_TEXT_P(res);
}


Datum pg_flashback_disable_table(PG_FUNCTION_ARGS) {
  text *table_name = PG_GETARG_TEXT_PP(0);
  Oid argtypes[1] = {TEXTOID};
  Datum values[1] = {PointerGetDatum(table_name)};
  text *res;

  SPI_connect();
  enter_silent_mode();
  SPI_execute_with_args(
      "SELECT flashback_internal.disable_table_logic($1)",
      1, argtypes, values, NULL, true, 0);
  res = fb_return_text_col1();
  SPI_finish();
  if (res == NULL)
    PG_RETURN_NULL();
  PG_RETURN_TEXT_P(res);
}


Datum pg_flashback_set_default_undo_retention(PG_FUNCTION_ARGS) {
  Interval *ret = PG_GETARG_INTERVAL_P(0);
  Oid argtypes[1] = {INTERVALOID};
  Datum values[1] = {IntervalPGetDatum(ret)};
  text *res;

  SPI_connect();
  enter_silent_mode();
  SPI_execute_with_args(
      "SELECT flashback_internal.set_default_undo_retention_logic($1)",
      1, argtypes, values, NULL, true, 0);
  res = fb_return_text_col1();
  SPI_finish();
  if (res == NULL)
    PG_RETURN_NULL();
  PG_RETURN_TEXT_P(res);
}


Datum pg_flashback_set_table_undo_retention(PG_FUNCTION_ARGS) {
  text *table_name = PG_GETARG_TEXT_PP(0);
  Interval *ret = PG_GETARG_INTERVAL_P(1);
  Oid argtypes[2] = {TEXTOID, INTERVALOID};
  Datum values[2] = {PointerGetDatum(table_name), IntervalPGetDatum(ret)};
  text *res;

  SPI_connect();
  enter_silent_mode();
  SPI_execute_with_args(
      "SELECT flashback_internal.set_table_undo_retention_logic($1, $2)",
      2, argtypes, values, NULL, true, 0);
  res = fb_return_text_col1();
  SPI_finish();
  if (res == NULL)
    PG_RETURN_NULL();
  PG_RETURN_TEXT_P(res);
}


Datum pg_flashback_flashback_table(PG_FUNCTION_ARGS) {
  text *table_name = PG_GETARG_TEXT_PP(0);
  TimestampTz target_ts = PG_GETARG_TIMESTAMPTZ(1);
  Oid argtypes[2] = {TEXTOID, TIMESTAMPTZOID};
  Datum values[2] = {PointerGetDatum(table_name),
                     TimestampTzGetDatum(target_ts)};
  text *res;

  SPI_connect();
  enter_silent_mode();
  SPI_execute_with_args(
      "SELECT flashback_internal.flashback_table_logic($1, $2)",
      2, argtypes, values, NULL, true, 0);
  res = fb_return_text_col1();
  SPI_finish();
  if (res == NULL)
    PG_RETURN_NULL();
  PG_RETURN_TEXT_P(res);
}


Datum pg_flashback_flashback_to_new_table(PG_FUNCTION_ARGS) {
  text *table_name = PG_GETARG_TEXT_PP(0);
  text *new_table_name = PG_GETARG_TEXT_PP(1);
  TimestampTz target_ts = PG_GETARG_TIMESTAMPTZ(2);
  Oid argtypes[3] = {TEXTOID, TEXTOID, TIMESTAMPTZOID};
  Datum values[3] = {PointerGetDatum(table_name),
                     PointerGetDatum(new_table_name),
                     TimestampTzGetDatum(target_ts)};
  text *res;

  SPI_connect();
  enter_silent_mode();
  SPI_execute_with_args(
      "SELECT flashback_internal.flashback_to_new_table_logic($1, $2, $3)",
      3, argtypes, values, NULL, true, 0);
  res = fb_return_text_col1();
  SPI_finish();
  if (res == NULL)
    PG_RETURN_NULL();
  PG_RETURN_TEXT_P(res);
}


Datum pg_flashback_table_as_of(PG_FUNCTION_ARGS) {
  FuncCallContext *funcctx;
  if (SRF_IS_FIRSTCALL()) {
    MemoryContext oldcontext;
    text *table_name;
    TimestampTz target_ts;
    Oid argtypes[2] = {TEXTOID, TIMESTAMPTZOID};
    Datum values[2];

    funcctx = SRF_FIRSTCALL_INIT();
    oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

    SPI_connect();
    enter_silent_mode();

    table_name = PG_GETARG_TEXT_PP(0);
    target_ts = PG_GETARG_TIMESTAMPTZ(1);
    values[0] = PointerGetDatum(table_name);
    values[1] = TimestampTzGetDatum(target_ts);

    SPI_execute_with_args(
        "SELECT * FROM flashback_internal.table_as_of_logic($1, $2)",
        2, argtypes, values, NULL, true, 0);

    /* Copy the result datums into the multi-call context and finish SPI now,
     * rather than holding the SPI tuple table and connection open across
     * per-call invocations (unsafe if another SPI user runs in between). */
    funcctx->max_calls = SPI_processed;
    if (SPI_processed > 0) {
      Datum *rows = (Datum *) palloc(sizeof(Datum) * SPI_processed);
      uint64 i;
      for (i = 0; i < SPI_processed; i++) {
        bool isnull;
        Datum d = SPI_getbinval(SPI_tuptable->vals[i], SPI_tuptable->tupdesc,
                                1, &isnull);
        rows[i] = isnull ? (Datum) 0 : datumCopy(d, false, -1);
      }
      funcctx->user_fctx = rows;
    } else {
      funcctx->user_fctx = NULL;
    }

    SPI_finish();
    MemoryContextSwitchTo(oldcontext);
  }

  funcctx = SRF_PERCALL_SETUP();
  if (funcctx->call_cntr < funcctx->max_calls) {
    Datum *rows = (Datum *) funcctx->user_fctx;
    SRF_RETURN_NEXT(funcctx, rows[funcctx->call_cntr]);
  } else {
    SRF_RETURN_DONE(funcctx);
  }
}


Datum pg_flashback_get_row_at(PG_FUNCTION_ARGS) {
  text *table_name = PG_GETARG_TEXT_PP(0);
  Jsonb *pk = PG_GETARG_JSONB_P(1);
  TimestampTz target_ts = PG_GETARG_TIMESTAMPTZ(2);
  Oid argtypes[3] = {TEXTOID, JSONBOID, TIMESTAMPTZOID};
  Datum values[3] = {PointerGetDatum(table_name), JsonbPGetDatum(pk),
                     TimestampTzGetDatum(target_ts)};
  bool isnull;
  Datum res;

  SPI_connect();
  enter_silent_mode();
  SPI_execute_with_args(
      "SELECT flashback_internal.get_row_at_logic($1, $2, $3)",
      3, argtypes, values, NULL, true, 0);

  if (SPI_processed == 0) {
    SPI_finish();
    PG_RETURN_NULL();
  }

  res = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull);
  if (isnull) {
    SPI_finish();
    PG_RETURN_NULL();
  }

  res = datumCopy(res, false, -1);
  SPI_finish();
  PG_RETURN_DATUM(res);
}


Datum pg_flashback_set_storage_mode(PG_FUNCTION_ARGS) {
  text *mode = PG_GETARG_TEXT_PP(0);
  Oid argtypes[1] = {TEXTOID};
  Datum values[1] = {PointerGetDatum(mode)};
  text *res;

  SPI_connect();
  enter_silent_mode();
  SPI_execute_with_args(
      "SELECT flashback_internal.set_storage_mode_logic($1)",
      1, argtypes, values, NULL, true, 0);
  res = fb_return_text_col1();
  SPI_finish();
  if (res == NULL)
    PG_RETURN_NULL();
  PG_RETURN_TEXT_P(res);
}


Datum pg_flashback_get_retention_horizon(PG_FUNCTION_ARGS) {
  text *table_name = PG_GETARG_TEXT_PP(0);
  Oid argtypes[1] = {TEXTOID};
  Datum values[1] = {PointerGetDatum(table_name)};
  bool isnull;
  Datum res;

  SPI_connect();
  enter_silent_mode();
  SPI_execute_with_args(
      "SELECT flashback_internal.get_retention_horizon_logic($1)",
      1, argtypes, values, NULL, true, 0);

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


/*
 * Set-returning helper. query_tmpl uses bound parameters ($1 = table name text,
 * $2/$3 = timestamptz), chosen by PG_NARGS(). No value is interpolated into the
 * SQL text.
 */
static Datum srf_handler(PG_FUNCTION_ARGS, const char *query) {
  FuncCallContext *funcctx;
  if (SRF_IS_FIRSTCALL()) {
    MemoryContext oldcontext;
    TupleDesc tupdesc;
    int nargs = PG_NARGS();
    Oid argtypes[3];
    Datum values[3];

    funcctx = SRF_FIRSTCALL_INIT();
    oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

    SPI_connect();
    enter_silent_mode();

    argtypes[0] = TEXTOID;
    values[0] = PointerGetDatum(PG_GETARG_TEXT_PP(0));
    if (nargs >= 2) {
      argtypes[1] = TIMESTAMPTZOID;
      values[1] = TimestampTzGetDatum(PG_GETARG_TIMESTAMPTZ(1));
    }
    if (nargs >= 3) {
      argtypes[2] = TIMESTAMPTZOID;
      values[2] = TimestampTzGetDatum(PG_GETARG_TIMESTAMPTZ(2));
    }

    SPI_execute_with_args(query, nargs, argtypes, values, NULL, true, 0);

    if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
      ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                      errmsg("function returning record called in context that "
                             "cannot accept type record")));

    funcctx->tuple_desc = BlessTupleDesc(tupdesc);
    funcctx->attinmeta = TupleDescGetAttInMetadata(tupdesc);

    /* Copy the result tuples into the multi-call context and finish SPI now,
     * instead of holding the SPI tuple table/connection across per-call
     * invocations. */
    funcctx->max_calls = SPI_processed;
    if (SPI_processed > 0) {
      HeapTuple *tuples =
          (HeapTuple *) palloc(sizeof(HeapTuple) * SPI_processed);
      uint64 i;
      for (i = 0; i < SPI_processed; i++)
        tuples[i] = heap_copytuple(SPI_tuptable->vals[i]);
      funcctx->user_fctx = tuples;
    } else {
      funcctx->user_fctx = NULL;
    }

    SPI_finish();
    MemoryContextSwitchTo(oldcontext);
  }

  funcctx = SRF_PERCALL_SETUP();
  if (funcctx->call_cntr < funcctx->max_calls) {
    HeapTuple *tuples = (HeapTuple *) funcctx->user_fctx;
    Datum res =
        heap_copy_tuple_as_datum(tuples[funcctx->call_cntr], funcctx->tuple_desc);
    SRF_RETURN_NEXT(funcctx, res);
  } else {
    SRF_RETURN_DONE(funcctx);
  }
}


Datum pg_flashback_flashback_horizon(PG_FUNCTION_ARGS) {
  return srf_handler(
      fcinfo, "SELECT * FROM flashback_internal.flashback_horizon_logic($1)");
}
Datum pg_flashback_changes_between(PG_FUNCTION_ARGS) {
  return srf_handler(
      fcinfo, "SELECT * FROM flashback_internal.changes_between_logic($1, "
              "$2, $3)");
}
Datum pg_flashback_get_transactions(PG_FUNCTION_ARGS) {
  return srf_handler(
      fcinfo, "SELECT * FROM flashback_internal.get_transactions_logic($1, "
              "$2, $3)");
}
