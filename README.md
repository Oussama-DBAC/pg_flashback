# pg_flashback

**An Enterprise-Grade, Native PostgreSQL Extension for Table and Schema Flashback to a Point-in-Time Without Backups.**

`pg_flashback` is a powerful, low-overhead extension that brings highly sought-after capabilities—comparable to Oracle's "Flashback Query" and transaction-level Point-In-Time Recovery (PITR) directly into the open-source PostgreSQL ecosystem. 

Unlike traditional trigger-based solutions that heavily penalize database performance during high-throughput operations, `pg_flashback` operates completely asynchronously. By intelligently leveraging PostgreSQL's Logical Decoding mechanism (`wal2json`) and executing entirely within isolated C Background Workers, the extension silently monitors and records row-level data changes (Inserts, Updates, Deletes) as they commit.

This architectural design empowers (DBAs) and developers to:

Query Historical Data: Seamlessly travel back in time to view a table exactly as it existed at a specific timestamp.
Undo Catastrophes Instantly: Reverse massive, erroneous UPDATE statements or accidental DELETE operations without dropping constraints or suffering the downtime of restoring an entire database cluster from a physical backup.
Perform Forensic Auditing: Gain granular visibility into every transaction, tracking the exact lifecycle of data changes.

## Key Features

*   **Native Integration:** Built completely in C and PL/pgSQL for maximum efficiency.
*   **Time Travel (Table As As Of):** Query the exact state of a table as it existed at any timestamp in the past.
*   **Granular State Recovery:** Rollback specific tables (or an entire schema) to a past state without affecting other tables in the database.
*   **Comprehensive Transaction Audit:** Track exactly *what* and *when* changed using `flashback.changes_between()`.
*   **Partitioned Architecture:** Historical events are automatically stored in auto-rotating daily partitions to guarantee high query performance and automatic cleanup.
*   **Zero-Downtime Recovery:** Fix `UPDATE` loops or accidental `DELETE`s securely, instantly, and without dropping constraints.
*   **Multi-Database Support:** Handles multiple target databases simultaneously via isolated C Background Workers.

---

## Architecture & Mechanics

Unlike traditional trigger-based auditing extensions (which slow down heavy `INSERT`/`UPDATE` operations by 50-200%), `pg_flashback` reads the **Write-Ahead Logs (WAL)** asynchronously via Logical Replication Slots.

1.  A C **Background Worker** connects to the database.
2.  It creates a logical slot using the `wal2json` output plugin.
3.  It streams the changes (Inserts, Updates, Deletes) of protected tables *after* they are committed.
4.  It inserts these events into a hidden, daily-partitioned "vault" table (`flashback_internal.fb_event`).
5.  All public read operations simply read and replay these events mathematically against the live table.

---

## Installation & Prerequisites

### Prerequisites
*   PostgreSQL 14, 15, 16, or 17 (Tested successfully on PG 17).
*   The logical decoding plugin `wal2json` must be installed.
*   PostgreSQL headers (`postgresql-server-dev-X` or `postgresql-devel`).

### 1. Configure postgresql.conf
For the extension to work, your PostgreSQL server instance **MUST** be configured with the logical replication level. Ensure these parameters are set in your `postgresql.conf`:

```ini
wal_level = logical
max_replication_slots = 3     # Must be >= number of databases monitored
max_worker_processes = 16      # Must have slots for pg_flashback workers

# Load the core extension engine at Postmaster startup
shared_preload_libraries = 'pg_flashback'

# Define which databases the extension should monitor (comma-separated list)
pg_flashback.database_names = 'postgres, my_app_db, demo_db'
```

*Restart the PostgreSQL service after changing these shared parameters.*

### 2. Compile and Install
```bash
# Clone the repository
git clone https://github.com/Oussama-DBAC/pg_flashback.git
cd pg_flashback

# Compile using the PGXS build framework
make 
sudo make install
```

### 3. Create the Extension in your Database
Connect to your target database (e.g., `psql -d my_app_db`) and install the SQL schema:
```sql
CREATE EXTENSION pg_flashback;
```

---

## Quick Start Guide & API

### 1. Activating Flashback on Tables
Any table you wish to monitor **must** be explicitly enabled.

```sql
-- Enable tracking with the default retention (1 hour)
SELECT flashback.enable_table('public.clients');

-- Enable tracking with a custom retention period of 30 days
SELECT flashback.enable_table('public.invoices', '30 days');
```
*Note: This automatically sets `REPLICA IDENTITY FULL` on the table to ensure the `old_values` of UPDATEs and DELETEs are captured in the WAL.*

### 2. Time Travel: Querying the Past (`table_as_of`)
A developer accidentally deleted all users at 14:00. You want to see the table exactly as it was at 13:59:

```sql
SELECT * FROM flashback.table_as_of('public.clients', '2026-10-10 13:59:00+00');
```

### 3. Auditing: Tracking Line-level Changes (`changes_between`)
You want to know exactly what happened to the `clients` table between yesterday and today:

```sql
SELECT commit_ts, operation, old_values, new_values 
FROM flashback.changes_between('public.clients', NOW() - INTERVAL '1 day', NOW());
```

### 4. Surgical Recovery: Rollback (`flashback_table`)
**DANGER / RED BUTTON:** This physically rewinds the live table back to the exact state it was at the designated time, overwriting all changes made since that timestamp.

```sql
-- Revert the entire "clients" table back to exactly 2 hours ago
SELECT flashback.flashback_table('public.clients', NOW() - INTERVAL '2 hours');
```

### 5. Table Management (Retention & Disabling)
You can modify the length of time historical data is kept, or completely stop tracking a table when it is no longer needed.

```sql
-- Modify the retention window of an already enabled table
SELECT flashback.set_table_undo_retention('public.clients', '30 days');  

-- Disable tracking completely and purge its historical data
SELECT flashback.disable_table('public.clients');  
```

### 6. System Horizon & Audit
Find out exactly how far back in time a table can be safely restored, and get granular transaction logs for auditing purposes.

```sql
-- Discover the exact timestamp of the oldest historical record available for a table
SELECT * FROM flashback.flashback_horizon('public.clients');  

-- Retrieve formatted transaction logs (PK identification and changed values) for a specific time window
SELECT * FROM flashback.get_transactions('public.clients', '2026-03-03 08:00', '2026-03-03 12:00');  
```

### 7. Optimization and Storage Logs (DBA Administration)
`pg_flashback` provides built-in views to help administrators monitor disk space consumption and activity spikes.

```sql
-- View the disk size consumed by the historical data of each active table
-- (Crucial for DBAs to monitor which tables consume the most Flashback space)
SELECT * FROM flashback.storage_stats;

-- View transaction activity metrics (Spikes/Load) per hour and per table
SELECT * FROM flashback.transaction_stats;     
```

---

## Best Practices & Constraints

### Managing Foreign Keys (Cascade Rollbacks)
If you attempt to use `flashback_table()` on a table that is linked to others via Foreign Keys (`Parent -> Child`), PostgreSQL will natively block the transaction (`SQL state: 23503`) to prevent orphaned records. 

To restore complex relational schemas safely, you must orchestrate a "Global Restore":
1. Suspend constraints (`DISABLE TRIGGER ALL`).
2. Rollback all related tables to the **exact same timestamp**.
3. Re-enable constraints (`ENABLE TRIGGER ALL`).

For a concrete global-restore example, apply `flashback_table()` to each
related table at the same timestamp between the `DISABLE TRIGGER ALL` /
`ENABLE TRIGGER ALL` steps above.

### Tuning capture throughput
The background worker consumes changes from the replication slot in batches.
The batch size is configurable:

```ini
# Max logical-decoding changes consumed per worker cycle (default 5000).
# Raise it if a high-write workload makes the slot fall behind.
pg_flashback.max_changes_per_cycle = 5000
```
If the slot lag (and WAL usage) grows under sustained writes, increase this.

### Replication slot and WAL retention (important)
Each monitored database gets a logical replication slot named
`flashback_slot_<db>`. **A logical slot retains WAL until it is consumed or
dropped**, so an abandoned slot can fill `pg_wal` and take the server down.

- On `DROP EXTENSION pg_flashback`, the worker detects the extension is gone
  and **drops its slot automatically** on its next cycle.
- If you instead stop monitoring a database by removing it from
  `pg_flashback.database_names` (and reloading), the worker for it no longer
  runs, so drop its slot manually:
  ```sql
  SELECT pg_drop_replication_slot('flashback_slot_' || current_database());
  ```
- Monitor slot lag with:
  ```sql
  SELECT slot_name, pg_size_pretty(
           pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) AS retained
  FROM pg_replication_slots WHERE slot_name LIKE 'flashback_slot_%';
  ```

### Limitations
- **Primary key required.** `table_as_of()` and the rollback functions identify
  rows by primary key. `table_as_of()` currently keys on a single-column PK;
  tables without a PK are rejected.
- **Type fidelity on rollback.** Rows are reconstructed from the captured JSON
  values via text, which can be lossy for some types (arrays, `bytea`,
  composites). Since `flashback_table()` overwrites live data, validate on a
  copy (`flashback_to_new_table()`) first for exotic column types.
- **Transaction id.** `xid` is captured into the event log and surfaced by
  `changes_between()` for auditing.

## License
This project is licensed under the **GNU General Public License v3.0 (GPL-3.0)**.
Copyright (c) 2026 Oussama CHAOUACHI (DBACCOMPANY). All rights reserved.
