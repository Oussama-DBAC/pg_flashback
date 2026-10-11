-- Smoke test for pg_flashback.
--
-- Run as a superuser, against a database where the extension is installed:
--
--     psql -d <db> -v ON_ERROR_STOP=1 -f test/smoke.sql
--
-- This tests the point-in-time *reconstruction logic* deterministically by
-- seeding the event log directly, instead of relying on the background worker
-- and a live logical-decoding (wal2json) setup. End-to-end capture is covered
-- by running against a server configured with wal_level=logical and wal2json
-- (see the README). Every check RAISEs on failure, so a clean run = all passed.

\set ON_ERROR_STOP on

CREATE TABLE IF NOT EXISTS public.fb_test (id int PRIMARY KEY, val text);
TRUNCATE public.fb_test;
INSERT INTO public.fb_test VALUES (1, 'v2');  -- current ("live") state

-- Enable tracking (sets REPLICA IDENTITY FULL, registers config). Default
-- retention is 1 hour, so a recent target timestamp is within the horizon.
SELECT flashback.enable_table('public.fb_test');

-- Seed one UPDATE event as the worker would have recorded it: the row was
-- 'v1' and was updated to 'v2' "now".
INSERT INTO flashback_internal.fb_event
    (lsn, seq_id, xid, commit_ts, table_schema, table_name, operation,
     pk_values, old_values, new_values)
VALUES
    ('0/1', 1, '1001', now(), 'public', 'fb_test', 'U',
     '{"id":1}', '{"id":1,"val":"v1"}', '{"id":1,"val":"v2"}');

-- 1. get_row_at reconstructs the pre-update value at a time before "now".
DO $$
DECLARE v jsonb;
BEGIN
    v := flashback.get_row_at('public.fb_test', '{"id":1}'::jsonb,
                              now() - interval '30 seconds');
    ASSERT v IS NOT NULL, 'get_row_at returned NULL';
    ASSERT v->>'val' = 'v1',
        format('expected reconstructed val "v1", got %L', v->>'val');
    RAISE NOTICE 'PASS: get_row_at reconstructs historical value';
END $$;

-- 2. changes_between returns the seeded event with the right operation and xid.
DO $$
DECLARE n int; op "char"; x text;
BEGIN
    SELECT count(*), max(operation), max(xid)
      INTO n, op, x
      FROM flashback.changes_between('public.fb_test',
                                     now() - interval '1 minute',
                                     now() + interval '1 minute');
    ASSERT n = 1, format('expected 1 change, got %s', n);
    ASSERT op = 'U', format('expected operation U, got %L', op);
    ASSERT x = '1001', format('expected xid 1001, got %L', x);
    RAISE NOTICE 'PASS: changes_between returns the event (incl. xid)';
END $$;

-- 3. Retention management + horizon are callable and consistent.
DO $$
DECLARE h timestamptz;
BEGIN
    PERFORM flashback.set_table_undo_retention('public.fb_test', '2 days');
    SELECT horizon INTO h FROM flashback.flashback_horizon('public.fb_test');
    ASSERT h IS NOT NULL, 'flashback_horizon returned no row';
    RAISE NOTICE 'PASS: retention + horizon API';
END $$;

-- Cleanup.
SELECT flashback.disable_table('public.fb_test');
DROP TABLE public.fb_test;

\echo 'pg_flashback smoke test: ALL CHECKS PASSED'
