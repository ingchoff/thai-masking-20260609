"""
Migration: allow transcription_secondary in jobs.job_type.

Usage example:
    uv run python migrate_db_20260609.py --source db/jobs.db --target db/jobs_migrated.db --backup db/jobs_backup_20260609.db --verify --replace
"""

import argparse
import os
import sqlite3
import time


class DatabaseConnectionError(Exception):
    pass


def get_db_connection(db_path: str, timeout: float = 5.0) -> sqlite3.Connection:
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    deadline = time.time() + timeout
    last_err = None
    while time.time() < deadline:
        try:
            conn = sqlite3.connect(
                db_path,
                timeout=timeout,
                isolation_level=None,
                check_same_thread=False,
            )
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL")
            return conn
        except sqlite3.OperationalError as exc:
            last_err = exc
            time.sleep(0.1)
    raise DatabaseConnectionError(
        f"Could not open database at {db_path} after {timeout:.1f}s: {last_err}"
    )


def create_backup(src: str, backup: str) -> bool:
    if not os.path.exists(src):
        print(f"Source database '{src}' does not exist.")
        return False
    try:
        os.makedirs(os.path.dirname(backup), exist_ok=True)
        with sqlite3.connect(src, timeout=30.0, isolation_level=None) as src_conn:
            journal_mode = src_conn.execute("PRAGMA journal_mode").fetchone()[0]
            if journal_mode and journal_mode.upper() == "WAL":
                checkpoint_result = src_conn.execute("PRAGMA wal_checkpoint(FULL)").fetchone()
                if checkpoint_result and checkpoint_result[0] != 0:
                    print("Failed to checkpoint WAL before backup. Stop DB writers and retry.")
                    return False
            with sqlite3.connect(backup, timeout=30.0) as backup_conn:
                src_conn.backup(backup_conn)
        print(f"Backup created at {backup}")
        return True
    except sqlite3.Error as exc:
        print(f"Failed to create backup: {exc}")
        if os.path.exists(backup):
            os.remove(backup)
        return False


def create_new_schema(conn: sqlite3.Connection) -> None:
    with conn:
        conn.execute(
            """
            CREATE TABLE jobs (
                taskId INTEGER PRIMARY KEY,
                trackId INTEGER NOT NULL,
                sourcePath TEXT NOT NULL,
                destPath TEXT,
                destPathJson TEXT NOT NULL,
                status TEXT CHECK(status IN ('pending', 'running', 'completed', 'completed_no_card', 'failed')) DEFAULT 'pending',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                started_at TIMESTAMP,
                completed_at TIMESTAMP,
                attempts INTEGER DEFAULT 0,
                local_source_path TEXT,
                local_dest_dir TEXT,
                download_status TEXT CHECK(download_status IN ('pending', 'downloading', 'completed', 'failed')) DEFAULT 'pending',
                upload_status TEXT CHECK(upload_status IN ('pending', 'uploading', 'completed', 'failed')) DEFAULT 'pending',
                job_type TEXT CHECK(job_type IN ('masking', 'transcription', 'transcription_secondary')) DEFAULT 'masking'
            )
            """
        )


def copy_jobs(old_conn: sqlite3.Connection, new_conn: sqlite3.Connection) -> int:
    columns = {row["name"] for row in old_conn.execute("PRAGMA table_info(jobs)").fetchall()}
    has_job_type = "job_type" in columns
    rows = old_conn.execute("SELECT * FROM jobs").fetchall()
    count = 0
    with new_conn:
        for row in rows:
            job_type = row["job_type"] if has_job_type and row["job_type"] else "masking"
            new_conn.execute(
                """
                INSERT INTO jobs (
                    taskId, trackId, sourcePath, destPath, destPathJson, status,
                    created_at, started_at, completed_at, attempts, local_source_path,
                    local_dest_dir, download_status, upload_status, job_type
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    row["taskId"],
                    row["trackId"],
                    row["sourcePath"],
                    row["destPath"],
                    row["destPathJson"],
                    row["status"],
                    row["created_at"],
                    row["started_at"],
                    row["completed_at"],
                    row["attempts"],
                    row["local_source_path"],
                    row["local_dest_dir"],
                    row["download_status"],
                    row["upload_status"],
                    job_type,
                ),
            )
            count += 1
    return count


def migrate_db(source: str, target: str) -> bool:
    print(f"Starting migration from {source} to {target}")
    old_conn = None
    new_conn = None
    try:
        old_conn = get_db_connection(source)
        if os.path.exists(target):
            os.remove(target)
        new_conn = get_db_connection(target)
        create_new_schema(new_conn)
        count = copy_jobs(old_conn, new_conn)
        print(f"Copied {count} rows.")
        return True
    except (sqlite3.Error, DatabaseConnectionError) as exc:
        print(f"Migration failed: {exc}")
        return False
    finally:
        if old_conn:
            old_conn.close()
        if new_conn:
            new_conn.close()
        print("Migration connections closed.")


def verify_counts(source: str, target: str) -> bool:
    try:
        src_conn = get_db_connection(source)
        tgt_conn = get_db_connection(target)
        src_count = src_conn.execute("SELECT COUNT(*) FROM jobs").fetchone()[0]
        tgt_count = tgt_conn.execute("SELECT COUNT(*) FROM jobs").fetchone()[0]
        if src_count != tgt_count:
            print(f"Row count mismatch: src={src_count}, tgt={tgt_count}")
            return False
        print(f"Row counts match: {src_count}")
        return True
    except (sqlite3.Error, DatabaseConnectionError) as exc:
        print(f"Verification failed: {exc}")
        return False
    finally:
        try:
            src_conn.close()
        except Exception:
            pass
        try:
            tgt_conn.close()
        except Exception:
            pass


def replace_db(source: str, target: str, backup: str | None) -> bool:
    if not os.path.exists(target):
        print(f"Target database '{target}' does not exist; cannot replace.")
        return False

    if os.path.exists(source):
        if backup and os.path.exists(backup):
            print(f"Backup exists at {backup}; replacing source directly.")
        else:
            source_old = f"{source}.old"
            try:
                os.replace(source, source_old)
                print(f"Renamed existing source to {source_old}")
            except OSError as exc:
                print(f"Failed to rename source DB: {exc}")
                return False

    try:
        os.replace(target, source)
        print(f"Replaced {source} with {target}")
        return True
    except OSError as exc:
        print(f"Failed to replace DB: {exc}")
        return False


def main(args: argparse.Namespace) -> None:
    if args.backup and not create_backup(args.source, args.backup):
        return

    migration_ok = migrate_db(args.source, args.target)
    verify_ok = True
    if migration_ok and args.verify:
        verify_ok = verify_counts(args.source, args.target)

    if migration_ok and verify_ok and args.replace:
        replace_db(args.source, args.target, args.backup)
    elif migration_ok and verify_ok:
        print("Migration complete. Use --replace to swap the migrated DB into place.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Allow transcription_secondary jobs.")
    parser.add_argument("--source", default="db/jobs.db", help="Path to source DB")
    parser.add_argument("--target", default="db/jobs_migrated.db", help="Path to target DB")
    parser.add_argument("--backup", help="Optional path to write a backup before migrating")
    parser.add_argument("--verify", action="store_true", help="Verify row counts after migration")
    parser.add_argument("--replace", action="store_true", help="Replace source DB with migrated target on success")
    main(parser.parse_args())
