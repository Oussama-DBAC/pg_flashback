\echo Use "CREATE EXTENSION pg_flashback" to load this file. \quit
CREATE SCHEMA IF NOT EXISTS flashback_internal;
REVOKE ALL ON SCHEMA flashback_internal FROM PUBLIC;
GRANT USAGE ON SCHEMA flashback_internal TO postgres;
CREATE SCHEMA IF NOT EXISTS flashback;
GRANT USAGE ON SCHEMA flashback TO PUBLIC;
CREATE TABLE IF NOT EXISTS flashback_internal.fb_config (
config_key TEXT PRIMARY KEY,
undo_retention INTERVAL NOT NULL,
enabled BOOLEAN DEFAULT TRUE,
enabled_at TIMESTAMPTZ DEFAULT NOW()
);
INSERT INTO flashback_internal.fb_config (config_key, undo_retention)
VALUES ('db_default', '1 hour')
ON CONFLICT DO NOTHING;
CREATE TABLE IF NOT EXISTS flashback_internal.fb_event (
lsn pg_lsn NOT NULL,
seq_id INT NOT NULL,
xid TEXT,
commit_ts TIMESTAMPTZ NOT NULL,
table_schema TEXT NOT NULL,
table_name TEXT NOT NULL,
operation CHAR(1) NOT NULL CHECK (operation IN ('I', 'U', 'D')),
pk_values JSONB,
old_values JSONB,
new_values JSONB,
captured_at TIMESTAMPTZ DEFAULT NOW(),
PRIMARY KEY (lsn, seq_id, commit_ts)
) PARTITION BY RANGE (commit_ts);
CREATE INDEX IF NOT EXISTS idx_fb_event_lookup
ON flashback_internal.fb_event (table_schema, table_name, commit_ts DESC);
CREATE TABLE IF NOT EXISTS flashback_internal.fb_event_default
PARTITION OF flashback_internal.fb_event DEFAULT;
CREATE OR REPLACE FUNCTION flashback_internal.extract_pk(
p_schema TEXT,
p_table TEXT,
p_data JSONB
) RETURNS JSONB LANGUAGE plpgsql AS $$
DECLARE
v_pk_cols TEXT[];
v_col_names JSONB;
v_col_values JSONB;
v_result JSONB := '{}';
v_col TEXT;
v_idx INT;
v_val JSONB;
BEGIN
SELECT array_agg(a.attname ORDER BY array_position(i.indkey, a.attnum))
INTO v_pk_cols
FROM pg_index i
JOIN pg_class c ON c.oid = i.indrelid
JOIN pg_namespace n ON n.oid = c.relnamespace
JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = ANY(i.indkey)
WHERE n.nspname = p_schema
AND c.relname = p_table
AND i.indisprimary;
IF v_pk_cols IS NULL THEN
RETURN NULL;
END IF;
v_col_names := p_data->'columnnames';
v_col_values := p_data->'columnvalues';
IF v_col_names IS NULL THEN
v_col_names := p_data->'oldkeys'->'keynames';
v_col_values := p_data->'oldkeys'->'keyvalues';
END IF;
IF v_col_names IS NULL OR v_col_values IS NULL THEN
RETURN NULL;
END IF;
FOREACH v_col IN ARRAY v_pk_cols
LOOP
FOR v_idx IN 0 .. jsonb_array_length(v_col_names) - 1
LOOP
IF (v_col_names->>v_idx) = v_col THEN
v_val := v_col_values->v_idx;
v_result := v_result || jsonb_build_object(v_col, v_val);
EXIT;
END IF;
END LOOP;
END LOOP;
RETURN v_result;
EXCEPTION WHEN OTHERS THEN
RETURN NULL;
END;
$$;
CREATE OR REPLACE FUNCTION flashback_internal.wal2json_zip(
p_names JSONB,
p_values JSONB
) RETURNS JSONB LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE
v_ret JSONB := '{}';
v_idx INT;
BEGIN
IF p_names IS NULL OR p_values IS NULL OR jsonb_array_length(p_names) = 0 THEN
RETURN NULL;
END IF;
FOR v_idx IN 0 .. jsonb_array_length(p_names) - 1 LOOP
v_ret := v_ret || jsonb_build_object(p_names->>v_idx, p_values->v_idx);
END LOOP;
RETURN v_ret;
EXCEPTION WHEN OTHERS THEN
RETURN NULL;
END;
$$;
CREATE OR REPLACE FUNCTION flashback_internal.maintain_partitions()
RETURNS void LANGUAGE plpgsql AS $$
DECLARE
v_schema TEXT := 'flashback_internal';
v_table TEXT := 'fb_event';
v_retention INTERVAL;
v_part_start TIMESTAMPTZ;
v_part_end TIMESTAMPTZ;
v_part_name TEXT;
v_max_retention INTERVAL;
v_rec RECORD;
v_default_retention INTERVAL;
v_target_schema TEXT;
v_target_table TEXT;
BEGIN
v_part_start := date_trunc('day', clock_timestamp());
v_part_end := v_part_start + INTERVAL '1 day';
v_part_name := v_table || '_' || to_char(v_part_start, 'YYYYMMDD');
IF NOT EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = v_schema AND c.relname = v_part_name) THEN
EXECUTE format('CREATE TABLE IF NOT EXISTS %I.%I PARTITION OF %I.%I FOR VALUES FROM (%L) TO (%L)',
v_schema, v_part_name, v_schema, v_table, v_part_start, v_part_end);
END IF;
v_part_start := v_part_start + INTERVAL '1 day';
v_part_end := v_part_end + INTERVAL '1 day';
v_part_name := v_table || '_' || to_char(v_part_start, 'YYYYMMDD');
IF NOT EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = v_schema AND c.relname = v_part_name) THEN
EXECUTE format('CREATE TABLE IF NOT EXISTS %I.%I PARTITION OF %I.%I FOR VALUES FROM (%L) TO (%L)',
v_schema, v_part_name, v_schema, v_table, v_part_start, v_part_end);
END IF;
SELECT MAX(undo_retention) INTO v_max_retention FROM flashback_internal.fb_config;
IF v_max_retention IS NULL THEN v_max_retention := INTERVAL '1 hour'; END IF;
FOR v_rec IN
SELECT config_key, undo_retention
FROM flashback_internal.fb_config
WHERE config_key != 'db_default'
LOOP
v_target_schema := split_part(v_rec.config_key, '.', 1);
v_target_table := split_part(v_rec.config_key, '.', 2);
DELETE FROM flashback_internal.fb_event
WHERE table_schema = v_target_schema
AND table_name = v_target_table
AND commit_ts < (clock_timestamp() - v_rec.undo_retention);
END LOOP;
SELECT undo_retention INTO v_default_retention FROM flashback_internal.fb_config WHERE config_key = 'db_default';
IF v_default_retention IS NULL THEN v_default_retention := INTERVAL '1 hour'; END IF;
DELETE FROM flashback_internal.fb_event e
WHERE NOT EXISTS (
SELECT 1 FROM flashback_internal.fb_config c WHERE c.config_key = (e.table_schema || '.' || e.table_name)
)
AND e.commit_ts < (clock_timestamp() - v_default_retention);
FOR v_part_name IN
SELECT relname FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = v_schema AND c.relname LIKE 'fb_event_2%'
LOOP
BEGIN
v_part_start := to_timestamp(split_part(v_part_name, '_', 3), 'YYYYMMDD');
v_part_end := v_part_start + INTERVAL '1 day';
IF v_part_end < (clock_timestamp() - v_max_retention) THEN
RAISE NOTICE 'Dropping expired partition %', v_part_name;
EXECUTE format('DROP TABLE IF EXISTS %I.%I', v_schema, v_part_name);
END IF;
EXCEPTION WHEN OTHERS THEN
NULL;
END;
END LOOP;
END;
$$;
CREATE OR REPLACE FUNCTION flashback_internal.get_effective_retention(p_table TEXT)
RETURNS INTERVAL LANGUAGE sql STABLE AS $$
SELECT COALESCE(
(SELECT undo_retention FROM flashback_internal.fb_config WHERE config_key = p_table),
(SELECT undo_retention FROM flashback_internal.fb_config WHERE config_key = 'db_default'),
INTERVAL '1 hour'
);
$$;
CREATE OR REPLACE FUNCTION flashback_internal.enable_table_logic(
p_table_name TEXT,
p_undo_retention INTERVAL DEFAULT NULL
) RETURNS TEXT LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
DECLARE
v_schema TEXT;
v_table TEXT;
v_full_name TEXT;
BEGIN
IF p_table_name LIKE '%.%' THEN
v_schema := split_part(p_table_name, '.', 1);
v_table := split_part(p_table_name, '.', 2);
ELSE
v_schema := 'public';
v_table := p_table_name;
END IF;
v_full_name := v_schema || '.' || v_table;
EXECUTE format('ALTER TABLE %I.%I REPLICA IDENTITY FULL', v_schema, v_table);
IF NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'flashback_pub') THEN
CREATE PUBLICATION flashback_pub;
END IF;
BEGIN
EXECUTE format('ALTER PUBLICATION flashback_pub ADD TABLE %I.%I', v_schema, v_table);
EXCEPTION WHEN duplicate_object THEN
NULL;
END;
INSERT INTO flashback_internal.fb_config (config_key, undo_retention, enabled, enabled_at)
VALUES (v_full_name, COALESCE(p_undo_retention, (SELECT undo_retention FROM flashback_internal.fb_config WHERE config_key='db_default')), true, NOW())
ON CONFLICT (config_key) DO UPDATE SET
undo_retention = EXCLUDED.undo_retention,
enabled = true,
enabled_at = NOW();
RETURN format('Table %s enabled for Flashback with retention %s', v_full_name,
COALESCE(p_undo_retention, (SELECT undo_retention FROM flashback_internal.fb_config WHERE config_key='db_default')));
END;
$$;
CREATE OR REPLACE FUNCTION flashback_internal.set_default_undo_retention_logic(period INTERVAL)
RETURNS TEXT LANGUAGE sql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
INSERT INTO flashback_internal.fb_config (config_key, undo_retention)
VALUES ('db_default', period)
ON CONFLICT (config_key) DO UPDATE SET undo_retention = EXCLUDED.undo_retention;
SELECT 'Default retention updated to ' || period::text;
$$;
CREATE OR REPLACE FUNCTION flashback_internal.set_table_undo_retention_logic(p_table_name TEXT, p_period INTERVAL)
RETURNS TEXT LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
DECLARE
v_key TEXT := p_table_name;
BEGIN
INSERT INTO flashback_internal.fb_config (config_key, undo_retention)
VALUES (v_key, p_period)
ON CONFLICT (config_key) DO UPDATE SET undo_retention = EXCLUDED.undo_retention;
RETURN 'Table retention updated to ' || p_period::text;
END;
$$;
CREATE OR REPLACE VIEW flashback.undo_config AS
SELECT config_key as target, undo_retention, enabled
FROM flashback_internal.fb_config
WHERE config_key != 'db_default';
CREATE OR REPLACE VIEW flashback.database_config AS
SELECT undo_retention as default_retention
FROM flashback_internal.fb_config
WHERE config_key = 'db_default';
CREATE OR REPLACE FUNCTION flashback_internal.flashback_horizon_logic(p_table TEXT)
RETURNS TABLE(target text, retention interval, horizon timestamptz)
LANGUAGE sql STABLE AS $$
SELECT
p_table,
flashback_internal.get_effective_retention(p_table),
NOW() - flashback_internal.get_effective_retention(p_table);
$$;
CREATE OR REPLACE VIEW flashback.database_flashback_horizon AS
SELECT
'DATABASE'::text as target,
undo_retention,
NOW() - undo_retention as horizon
FROM flashback_internal.fb_config
WHERE config_key = 'db_default';
CREATE OR REPLACE FUNCTION flashback_internal.flashback_table_logic(
p_table_name TEXT,
p_target_ts TIMESTAMPTZ
) RETURNS TEXT LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
DECLARE
v_schema TEXT;
v_table TEXT;
v_rec RECORD;
v_sql TEXT;
v_pk_cond TEXT;
v_count INT := 0;
v_start_time TIMESTAMPTZ := clock_timestamp();
v_horizon TIMESTAMPTZ;
BEGIN
IF p_table_name LIKE '%.%' THEN
v_schema := split_part(p_table_name, '.', 1);
v_table := split_part(p_table_name, '.', 2);
ELSE
v_schema := 'public';
v_table := p_table_name;
END IF;
v_horizon := NOW() - flashback_internal.get_effective_retention(p_table_name);
IF p_target_ts < v_horizon THEN
RAISE EXCEPTION 'Target time % is beyond the undo retention horizon (%)', p_target_ts, v_horizon;
END IF;
EXECUTE format('LOCK TABLE %I.%I IN EXCLUSIVE MODE', v_schema, v_table);
FOR v_rec IN
SELECT operation, pk_values, old_values, lsn
FROM flashback_internal.fb_event
WHERE table_schema = v_schema
AND table_name = v_table
AND commit_ts > p_target_ts
ORDER BY commit_ts DESC, lsn DESC
LOOP
v_pk_cond := '';
DECLARE k text; v text; BEGIN
FOR k, v IN SELECT key, value::text FROM jsonb_each_text(v_rec.pk_values) LOOP
IF v_pk_cond <> '' THEN v_pk_cond := v_pk_cond || ' AND '; END IF;
v_pk_cond := v_pk_cond || format('%I = %L', k, v);
END LOOP;
END;
IF v_pk_cond = '' OR v_pk_cond IS NULL THEN
CONTINUE;
END IF;
IF v_rec.operation = 'I' THEN
v_sql := format('DELETE FROM %I.%I WHERE %s', v_schema, v_table, v_pk_cond);
ELSIF v_rec.operation = 'D' THEN
DECLARE cols text := ''; vals text := ''; k text; v text; BEGIN
FOR k, v IN SELECT key, value::text FROM jsonb_each_text(v_rec.old_values) LOOP
IF cols <> '' THEN cols := cols || ', '; vals := vals || ', '; END IF;
cols := cols || format('%I', k);
vals := vals || format('%L', v);
END LOOP;
v_sql := format('INSERT INTO %I.%I (%s) VALUES (%s)', v_schema, v_table, cols, vals);
END;
ELSIF v_rec.operation = 'U' THEN
DECLARE updates text := ''; k text; v text; BEGIN
FOR k, v IN SELECT key, value::text FROM jsonb_each_text(v_rec.old_values) LOOP
IF updates <> '' THEN updates := updates || ', '; END IF;
updates := updates || format('%I = %L', k, v);
END LOOP;
v_sql := format('UPDATE %I.%I SET %s WHERE %s', v_schema, v_table, updates, v_pk_cond);
END;
END IF;
EXECUTE v_sql;
v_count := v_count + 1;
END LOOP;
RETURN format('Flashback completed. Reverted %s changes.', v_count);
END;
$$;
CREATE OR REPLACE FUNCTION flashback_internal.table_as_of_logic(
p_table_name TEXT,
p_target_ts TIMESTAMPTZ
) RETURNS SETOF JSONB LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
DECLARE
v_schema TEXT;
v_table TEXT;
v_row JSONB;
v_pk_list TEXT[];
v_pk_name TEXT;
v_pk_val TEXT;
v_map JSONB;
v_rec RECORD;
v_horizon TIMESTAMPTZ;
BEGIN
v_horizon := NOW() - flashback_internal.get_effective_retention(p_table_name);
IF p_target_ts < v_horizon THEN
RAISE EXCEPTION 'Target time % is beyond the undo retention horizon (%)', p_target_ts, v_horizon;
END IF;
IF p_table_name LIKE '%.%' THEN
v_schema := split_part(p_table_name, '.', 1);
v_table := split_part(p_table_name, '.', 2);
ELSE
v_schema := 'public';
v_table := p_table_name;
END IF;
SELECT a.attname
INTO v_pk_name
FROM pg_index i
JOIN pg_class c ON c.oid = i.indrelid
JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = ANY(i.indkey)
WHERE c.relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = v_schema)
AND c.relname = v_table
AND i.indisprimary
LIMIT 1;
IF v_pk_name IS NULL THEN
RAISE EXCEPTION 'Table % has no primary key.', p_table_name;
END IF;
FOR v_row IN
EXECUTE format('
WITH all_pks AS (
SELECT jsonb_build_object(%1$L, %1$I) as pk_json
FROM %2$I.%3$I
UNION
SELECT pk_values
FROM flashback_internal.fb_event
WHERE table_schema = %4$L
AND table_name = %5$L
AND operation = ''D''
AND commit_ts > %6$L
)
SELECT flashback_internal.get_row_at_logic(%7$L, pk_json, %6$L)
FROM all_pks
', v_pk_name, v_schema, v_table, v_schema, v_table, p_target_ts, p_table_name)
LOOP
IF v_row IS NOT NULL THEN
RETURN NEXT v_row;
END IF;
END LOOP;
RETURN;
END;
$$;
CREATE OR REPLACE FUNCTION flashback_internal.changes_between_logic(
p_table_name TEXT,
p_start_ts TIMESTAMPTZ,
p_end_ts TIMESTAMPTZ
) RETURNS TABLE(
commit_ts TIMESTAMPTZ,
operation CHAR(1),
pk JSONB,
old_row JSONB,
new_row JSONB,
xid TEXT,
lsn pg_lsn
) LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
DECLARE
v_schema TEXT; v_table TEXT;
BEGIN
IF p_table_name LIKE '%.%' THEN
v_schema := split_part(p_table_name, '.', 1);
v_table := split_part(p_table_name, '.', 2);
ELSE
v_schema := 'public';
v_table := p_table_name;
END IF;
RETURN QUERY
SELECT e.commit_ts, e.operation, e.pk_values, e.old_values, e.new_values, e.xid, e.lsn
FROM flashback_internal.fb_event e
WHERE e.table_schema = v_schema AND e.table_name = v_table
AND e.commit_ts >= p_start_ts AND e.commit_ts <= p_end_ts
ORDER BY e.commit_ts ASC, e.lsn ASC;
END;
$$;
CREATE OR REPLACE FUNCTION flashback_internal.get_row_at_logic(
p_table_name TEXT,
pk_json JSONB,
target_ts TIMESTAMPTZ
) RETURNS JSONB LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
DECLARE
v_schema TEXT; v_table TEXT;
v_current JSONB;
v_state JSONB;
v_rec RECORD;
v_pk_cond TEXT := '';
v_horizon TIMESTAMPTZ;
k TEXT; v TEXT;
BEGIN
v_horizon := NOW() - flashback_internal.get_effective_retention(p_table_name);
IF target_ts < v_horizon THEN
RETURN NULL;
RAISE EXCEPTION 'Target time % is beyond the undo retention horizon (%)', target_ts, v_horizon;
END IF;
IF p_table_name LIKE '%.%' THEN
v_schema := split_part(p_table_name, '.', 1);
v_table := split_part(p_table_name, '.', 2);
ELSE
v_schema := 'public';
v_table := p_table_name;
END IF;
FOR k, v IN SELECT key, value::text FROM jsonb_each_text(pk_json) LOOP
IF v_pk_cond <> '' THEN v_pk_cond := v_pk_cond || ' AND '; END IF;
v_pk_cond := v_pk_cond || format('%I = %L', k, v);
END LOOP;
BEGIN
EXECUTE format('SELECT to_jsonb(t) FROM %I.%I t WHERE %s', v_schema, v_table, v_pk_cond) INTO v_current;
EXCEPTION WHEN OTHERS THEN v_current := NULL; END;
v_state := v_current;
FOR v_rec IN
SELECT operation, old_values, new_values
FROM flashback_internal.fb_event
WHERE table_schema = v_schema
AND table_name = v_table
AND commit_ts > target_ts
AND pk_values @> pk_json
ORDER BY commit_ts DESC, lsn DESC
LOOP
IF v_rec.operation = 'I' THEN
v_state := NULL;
ELSIF v_rec.operation = 'D' THEN
v_state := v_rec.old_values;
ELSIF v_rec.operation = 'U' THEN
IF v_state IS NULL THEN
v_state := v_rec.old_values;
ELSE
v_state := v_state || v_rec.old_values;
END IF;
END IF;
END LOOP;
RETURN v_state;
END;
$$;
CREATE OR REPLACE FUNCTION flashback_internal.set_storage_mode_logic(p_mode TEXT)
RETURNS TEXT LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
DECLARE
r RECORD;
BEGIN
IF upper(p_mode) NOT IN ('LOGGED', 'UNLOGGED') THEN
RAISE EXCEPTION 'Invalid storage mode. Use LOGGED or UNLOGGED.';
END IF;
FOR r IN SELECT inhrelid::regclass::text AS child
FROM pg_inherits
WHERE inhparent = 'flashback_internal.fb_event'::regclass
LOOP
EXECUTE format('ALTER TABLE %s SET %s', r.child, upper(p_mode));
END LOOP;
RETURN format('Storage mode set to %s for all partitions', upper(p_mode));
END;
$$;
CREATE OR REPLACE VIEW flashback.storage_stats AS
SELECT
table_schema || '.' || table_name AS real_table,
count(*) AS total_events,
pg_size_pretty(sum(pg_column_size(old_values) + pg_column_size(new_values) + pg_column_size(pk_values))) AS estimated_size
FROM flashback_internal.fb_event
GROUP BY table_schema, table_name
ORDER BY sum(pg_column_size(old_values)) DESC;
CREATE OR REPLACE VIEW flashback.transaction_stats AS
SELECT
date_trunc('hour', commit_ts) as hour,
table_schema || '.' || table_name AS table_name,
operation,
count(*) as event_count
FROM flashback_internal.fb_event
GROUP BY 1, 2, 3
ORDER BY 1 DESC, 4 DESC;
CREATE OR REPLACE FUNCTION flashback_internal.disable_table_logic(p_table_name TEXT)
RETURNS TEXT LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
DECLARE
v_schema TEXT;
v_table TEXT;
v_full_name TEXT;
BEGIN
IF p_table_name LIKE '%.%' THEN
v_schema := split_part(p_table_name, '.', 1);
v_table := split_part(p_table_name, '.', 2);
ELSE
v_schema := 'public';
v_table := p_table_name;
END IF;
v_full_name := v_schema || '.' || v_table;
DELETE FROM flashback_internal.fb_config WHERE config_key = v_full_name;
BEGIN
EXECUTE format('ALTER PUBLICATION flashback_pub DROP TABLE %I.%I', v_schema, v_table);
EXCEPTION WHEN OTHERS THEN NULL; END;
EXECUTE format('ALTER TABLE %I.%I REPLICA IDENTITY DEFAULT', v_schema, v_table);
DELETE FROM flashback_internal.fb_event WHERE table_schema = v_schema AND table_name = v_table;
RETURN format('Table %s disabled for Flashback and history cleared.', v_full_name);
END;
$$;
CREATE OR REPLACE FUNCTION flashback_internal.get_retention_horizon_logic(p_table_name TEXT)
RETURNS TIMESTAMPTZ LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
DECLARE
v_schema TEXT; v_table TEXT;
v_min_ts TIMESTAMPTZ;
BEGIN
IF p_table_name LIKE '%.%' THEN
v_schema := split_part(p_table_name, '.', 1);
v_table := split_part(p_table_name, '.', 2);
ELSE
v_schema := 'public';
v_table := p_table_name;
END IF;
SELECT min(commit_ts) INTO v_min_ts
FROM flashback_internal.fb_event
WHERE table_schema = v_schema AND table_name = v_table;
RETURN v_min_ts;
END;
$$;
CREATE OR REPLACE FUNCTION flashback_internal.get_transactions_logic(
p_table_name TEXT,
p_start_ts TIMESTAMPTZ,
p_end_ts TIMESTAMPTZ
) RETURNS TABLE(
commit_ts TIMESTAMPTZ,
operation CHAR(1),
identifier JSONB,
changes JSONB
) LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
DECLARE
v_schema TEXT; v_table TEXT;
BEGIN
IF p_table_name LIKE '%.%' THEN
v_schema := split_part(p_table_name, '.', 1);
v_table := split_part(p_table_name, '.', 2);
ELSE
v_schema := 'public';
v_table := p_table_name;
END IF;
RETURN QUERY
SELECT e.commit_ts, e.operation, e.pk_values,
CASE WHEN e.operation = 'D' THEN e.old_values
WHEN e.operation = 'I' THEN e.new_values
ELSE e.new_values
END
FROM flashback_internal.fb_event e
WHERE e.table_schema = v_schema AND e.table_name = v_table
AND e.commit_ts >= p_start_ts AND e.commit_ts <= p_end_ts
ORDER BY e.commit_ts ASC;
END;
$$;
CREATE OR REPLACE FUNCTION flashback_internal.flashback_to_new_table_logic(
p_table_name TEXT,
p_new_table_name TEXT,
p_target_ts TIMESTAMPTZ
) RETURNS TEXT LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
DECLARE
v_schema TEXT; v_table TEXT;
v_new_schema TEXT; v_new_table TEXT;
v_rec RECORD;
v_sql TEXT;
v_pk_cond TEXT;
v_count INT := 0;
v_horizon TIMESTAMPTZ;
BEGIN
IF p_table_name LIKE '%.%' THEN
v_schema := split_part(p_table_name, '.', 1);
v_table := split_part(p_table_name, '.', 2);
ELSE
v_schema := 'public';
v_table := p_table_name;
END IF;
IF p_new_table_name LIKE '%.%' THEN
v_new_schema := split_part(p_new_table_name, '.', 1);
v_new_table := split_part(p_new_table_name, '.', 2);
ELSE
v_new_schema := 'public';
v_new_table := p_new_table_name;
END IF;
v_horizon := NOW() - flashback_internal.get_effective_retention(p_table_name);
IF p_target_ts < v_horizon THEN
RAISE EXCEPTION 'Flashback Too Old: Target time % is beyond retention horizon. You can only flashback until %', p_target_ts, v_horizon;
END IF;
EXECUTE format('CREATE UNLOGGED TABLE %I.%I AS SELECT * FROM %I.%I', v_new_schema, v_new_table, v_schema, v_table);
FOR v_rec IN
SELECT operation, pk_values, old_values, lsn
FROM flashback_internal.fb_event
WHERE table_schema = v_schema
AND table_name = v_table
AND commit_ts > p_target_ts
ORDER BY commit_ts DESC, lsn DESC
LOOP
v_pk_cond := '';
DECLARE k text; v text; BEGIN
FOR k, v IN SELECT key, value::text FROM jsonb_each_text(v_rec.pk_values) LOOP
IF v_pk_cond <> '' THEN v_pk_cond := v_pk_cond || ' AND '; END IF;
v_pk_cond := v_pk_cond || format('%I = %L', k, v);
END LOOP;
END;
IF v_pk_cond = '' OR v_pk_cond IS NULL THEN
CONTINUE;
END IF;
IF v_rec.operation = 'I' THEN
v_sql := format('DELETE FROM %I.%I WHERE %s', v_new_schema, v_new_table, v_pk_cond);
ELSIF v_rec.operation = 'D' THEN
DECLARE cols text := ''; vals text := ''; k text; v text; BEGIN
FOR k, v IN SELECT key, value::text FROM jsonb_each_text(v_rec.old_values) LOOP
IF cols <> '' THEN cols := cols || ', '; vals := vals || ', '; END IF;
cols := cols || format('%I', k);
vals := vals || format('%L', v);
END LOOP;
v_sql := format('INSERT INTO %I.%I (%s) VALUES (%s)', v_new_schema, v_new_table, cols, vals);
END;
ELSIF v_rec.operation = 'U' THEN
DECLARE updates text := ''; k text; v text; BEGIN
FOR k, v IN SELECT key, value::text FROM jsonb_each_text(v_rec.old_values) LOOP
IF updates <> '' THEN updates := updates || ', '; END IF;
updates := updates || format('%I = %L', k, v);
END LOOP;
v_sql := format('UPDATE %I.%I SET %s WHERE %s', v_new_schema, v_new_table, updates, v_pk_cond);
END;
END IF;
EXECUTE v_sql;
v_count := v_count + 1;
END LOOP;
RETURN format('Flashback Clone created at %s.%s. Reverted %s changes.', v_new_schema, v_new_table, v_count);
END;
$$;
CREATE OR REPLACE FUNCTION flashback.enable_table(TEXT, INTERVAL DEFAULT NULL)
RETURNS TEXT AS '$libdir/pg_flashback', 'pg_flashback_enable_table' LANGUAGE C;
CREATE OR REPLACE FUNCTION flashback.disable_table(TEXT)
RETURNS TEXT AS '$libdir/pg_flashback', 'pg_flashback_disable_table' LANGUAGE C;
CREATE OR REPLACE FUNCTION flashback.set_default_undo_retention(INTERVAL)
RETURNS TEXT AS '$libdir/pg_flashback', 'pg_flashback_set_default_undo_retention' LANGUAGE C;
CREATE OR REPLACE FUNCTION flashback.set_table_undo_retention(TEXT, INTERVAL)
RETURNS TEXT AS '$libdir/pg_flashback', 'pg_flashback_set_table_undo_retention' LANGUAGE C;
CREATE OR REPLACE FUNCTION flashback.flashback_horizon(p_table TEXT)
RETURNS TABLE(target text, retention interval, horizon timestamptz)
LANGUAGE sql AS $$
SELECT * FROM flashback_internal.flashback_horizon_logic(p_table);
$$;
CREATE OR REPLACE FUNCTION flashback.get_retention_horizon(TEXT)
RETURNS TIMESTAMPTZ AS '$libdir/pg_flashback', 'pg_flashback_get_retention_horizon' LANGUAGE C;
CREATE OR REPLACE FUNCTION flashback.flashback_table(TEXT, TIMESTAMPTZ)
RETURNS TEXT AS '$libdir/pg_flashback', 'pg_flashback_flashback_table' LANGUAGE C;
CREATE OR REPLACE FUNCTION flashback.flashback_to_new_table(TEXT, TEXT, TIMESTAMPTZ)
RETURNS TEXT AS '$libdir/pg_flashback', 'pg_flashback_flashback_to_new_table' LANGUAGE C;
CREATE OR REPLACE FUNCTION flashback.table_as_of(p_table_name TEXT, p_target_ts TIMESTAMPTZ)
RETURNS SETOF JSONB LANGUAGE sql AS $$
SELECT * FROM flashback_internal.table_as_of_logic(p_table_name, p_target_ts);
$$;
CREATE OR REPLACE FUNCTION flashback.changes_between(p_table_name TEXT, p_start_ts TIMESTAMPTZ, p_end_ts TIMESTAMPTZ)
RETURNS TABLE(commit_ts timestamptz, operation char(1), pk jsonb, old_row jsonb, new_row jsonb, xid text, lsn pg_lsn)
LANGUAGE sql AS $$
SELECT * FROM flashback_internal.changes_between_logic(p_table_name, p_start_ts, p_end_ts);
$$;
CREATE OR REPLACE FUNCTION flashback.get_row_at(TEXT, JSONB, TIMESTAMPTZ)
RETURNS JSONB AS '$libdir/pg_flashback', 'pg_flashback_get_row_at' LANGUAGE C;
CREATE OR REPLACE FUNCTION flashback.get_transactions(p_table_name TEXT, p_start_ts TIMESTAMPTZ, p_end_ts TIMESTAMPTZ)
RETURNS TABLE(commit_ts timestamptz, operation char(1), identifier jsonb, changes jsonb)
LANGUAGE sql AS $$
SELECT * FROM flashback_internal.get_transactions_logic(p_table_name, p_start_ts, p_end_ts);
$$;
CREATE OR REPLACE FUNCTION flashback.set_storage_mode(TEXT)
RETURNS TEXT AS '$libdir/pg_flashback', 'pg_flashback_set_storage_mode' LANGUAGE C;

-- Least privilege: the public API can rewrite live tables (flashback_table),
-- clone arbitrary tables (flashback_to_new_table) and purge history
-- (disable_table). It is DBA/operator tooling and must NOT be executable by
-- PUBLIC by default. Deny by default; grant EXECUTE explicitly to the role(s)
-- that are authorized to operate flashback, e.g.:
--
--   CREATE ROLE flashback_operator NOLOGIN;
--   GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA flashback TO flashback_operator;
--   GRANT flashback_operator TO <your_dba_role>;
--
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA flashback FROM PUBLIC;
