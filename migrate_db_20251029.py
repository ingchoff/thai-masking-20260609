import sqlite3
import os
import time
import json

class DatabaseConnectionError(Exception):
    """Custom exception for database connection errors."""
    pass

def get_db_connection(db_path: str, timeout: float = 5.0) -> sqlite3.Connection:
    """
    Return an SQLite connection that
    - creates the db folder if missing,
    - switches the database to WAL mode,
    - waits up to *timeout* seconds when the DB is locked,
    - retries briefly if the open itself fails.
    """
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    deadline = time.time() + timeout
    last_err = None
    while time.time() < deadline:
        try:
            conn = sqlite3.connect(
                db_path,
                timeout=timeout,          # SQLite busy-timeout (seconds)
                isolation_level=None,     # autocommit; transactions via 'with'
                check_same_thread=False   # every thread gets its own conn anyway
            )
            conn.row_factory = sqlite3.Row
            # Switch to WAL once per database (harmless if already WAL)
            conn.execute("PRAGMA journal_mode=WAL")
            return conn
        except sqlite3.OperationalError as exc:
            last_err = exc
            time.sleep(0.1)              # quick back-off
    raise DatabaseConnectionError(
        f"Could not open database at {db_path} after {timeout:.1f}s: {last_err}"
    )

def migrate_db(old_db_path: str, new_db_path: str):
    """
    Migrates data from the old database schema to the new database schema.
    Reads from old_db_path and writes to a new file at new_db_path.
    """
    print("\n--- Starting Database Migration ---")

    if os.path.exists(new_db_path):
        print(f"Warning: Target database '{new_db_path}' already exists. It will be deleted and recreated.")
        os.remove(new_db_path)

    try:
        old_conn = get_db_connection(old_db_path)
        new_conn = get_db_connection(new_db_path)
        print("Successfully connected to databases.")

        # Create the new jobs table
        with new_conn:
            new_conn.execute('''
            CREATE TABLE jobs (
                taskId INTEGER PRIMARY KEY, trackId INTEGER NOT NULL, sourcePath TEXT NOT NULL,
                destPath TEXT NOT NULL, destPathJson TEXT NOT NULL,
                status TEXT CHECK(status IN ('pending', 'running', 'completed', 'completed_no_card', 'failed')) DEFAULT 'pending',
                created_at TIMESTAMP, started_at TIMESTAMP, completed_at TIMESTAMP,
                attempts INTEGER DEFAULT 0, local_source_path TEXT, local_dest_dir TEXT,
                download_status TEXT CHECK(download_status IN ('pending', 'downloading', 'completed', 'failed')) DEFAULT 'pending',
                upload_status TEXT CHECK(upload_status IN ('pending', 'uploading', 'completed', 'failed')) DEFAULT 'pending'
            )''')
        
        # Fetch records from the backup table
        old_cursor = old_conn.cursor()
        old_cursor.execute("PRAGMA table_info(jobs)")
        old_columns = {row[1] for row in old_cursor.fetchall()}
        has_dest_path_json = "destPathJson" in old_columns

        old_cursor.execute("SELECT * FROM jobs")
        old_jobs = old_cursor.fetchall()
        print(f"Found {len(old_jobs)} records to migrate.")

        # Insert records into the new table with the transformed schema
        with new_conn:
            for job in old_jobs:
                if has_dest_path_json:
                    dest_path_json = job["destPathJson"]
                else:
                    dest_path_json = job["destPath"]

                new_conn.execute('''
                    INSERT INTO jobs (
                        taskId, trackId, sourcePath, destPath, destPathJson, status,
                        created_at, started_at, completed_at, attempts, local_source_path,
                        local_dest_dir, download_status, upload_status
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    job['taskId'], job['trackId'], job['sourcePath'], job['destPath'],
                    dest_path_json, job['status'], job['created_at'], job['started_at'],
                    job['completed_at'], job['attempts'], job['local_source_path'],
                    job['local_dest_dir'], job['download_status'], job['upload_status']
                ))
        print("Data migration completed successfully.")
        return True

    except (sqlite3.Error, DatabaseConnectionError) as e:
        print(f"An error occurred during migration: {e}")
        return False
    finally:
        if 'old_conn' in locals(): old_conn.close()
        if 'new_conn' in locals(): new_conn.close()
        print("Migration database connections closed.")


def create_backup(original_db: str, backup_db: str) -> bool:
    """Create a consistent backup copy of *original_db* at *backup_db*."""
    print(f"Attempting to create backup '{backup_db}' from '{original_db}'.")

    if not os.path.exists(original_db):
        print(f"Error: Source database '{original_db}' does not exist.")
        return False

    try:
        os.makedirs(os.path.dirname(backup_db), exist_ok=True)
        with sqlite3.connect(original_db, timeout=30.0, isolation_level=None) as src_conn:
            # Flush any pending WAL changes so the backup includes the latest data.
            journal_mode = src_conn.execute("PRAGMA journal_mode").fetchone()[0]
            if journal_mode and journal_mode.upper() == "WAL":
                checkpoint_result = src_conn.execute("PRAGMA wal_checkpoint(FULL)").fetchone()
                if checkpoint_result and checkpoint_result[0] != 0:
                    print("Failed to checkpoint WAL before backup (database is busy). Stop writers and try again.")
                    return False
            with sqlite3.connect(backup_db, timeout=30.0) as backup_conn:
                src_conn.backup(backup_conn)
        print("Backup created successfully using Python's sqlite3 module.")
        return True
    except sqlite3.Error as exc:
        if os.path.exists(backup_db):
            os.remove(backup_db)
        print(f"Failed to create backup automatically: {exc}")
        return False

def verify_migration(original_db_path: str, new_db_path: str) -> bool:
    """
    Compares the original database with the new one to ensure migration was successful.
    """
    print("\n--- Starting Migration Verification ---")
    try:
        orig_conn = get_db_connection(original_db_path)
        new_conn = get_db_connection(new_db_path)
        print("Successfully connected to databases for verification.")

        orig_cur = orig_conn.cursor()
        new_cur = new_conn.cursor()

        # 1. Compare row counts
        orig_count = orig_cur.execute("SELECT COUNT(*) FROM jobs").fetchone()[0]
        new_count = new_cur.execute("SELECT COUNT(*) FROM jobs").fetchone()[0]

        if orig_count != new_count:
            print("🔴 VERIFICATION FAILED: Row count mismatch.")
            print(f"   - Original DB '{os.path.basename(original_db_path)}' has {orig_count} rows.")
            print(f"   - New DB      '{os.path.basename(new_db_path)}' has {new_count} rows.")
            return False
        print(f"✅ Row counts match: {orig_count} rows.")

        # Inspect original schema to determine available columns
        orig_columns = {
            row[1] for row in orig_cur.execute("PRAGMA table_info(jobs)").fetchall()
        }
        orig_has_dest_path_json = "destPathJson" in orig_columns

        # 2. Compare data row by row
        orig_jobs = orig_cur.execute("SELECT * FROM jobs ORDER BY taskId").fetchall()
        new_jobs_list = new_cur.execute("SELECT * FROM jobs ORDER BY taskId").fetchall()
        new_jobs_dict = {job['taskId']: job for job in new_jobs_list}

        for orig_job in orig_jobs:
            task_id = orig_job['taskId']
            new_job = new_jobs_dict.get(task_id)

            # Compare all original columns
            for col in orig_job.keys():
                if orig_job[col] != new_job[col]:
                    print(f"🔴 VERIFICATION FAILED: Mismatch in column '{col}' for taskId {task_id}.")
                    return False
            
            # Verify the transformed column
            if orig_has_dest_path_json:
                expected_json = orig_job["destPathJson"]
            else:
                expected_json = orig_job["destPath"]
            if new_job['destPathJson'] != expected_json:
                print(f"🔴 VERIFICATION FAILED: Mismatch in new column 'destPathJson' for taskId {task_id}.")
                return False

        print("✅ All row data is correct.")
        print("🟢 VERIFICATION PASSED!")
        return True

    except (sqlite3.Error, DatabaseConnectionError) as e:
        print(f"An error occurred during verification: {e}")
        return False
    finally:
        if 'orig_conn' in locals(): orig_conn.close()
        if 'new_conn' in locals(): new_conn.close()
        print("Verification database connections closed.")


if __name__ == '__main__':
    # Define file paths
    original_db = "db/jobs.db"
    backup_db = "db/jobs_bak.db"
    new_db = "db/jobs_new.db"

    print("Migration Workflow Started.")
    if not os.path.isfile(backup_db):
        import sys
        print(f"Backup file '{backup_db}' not found; attempting to create one automatically.")
        if not create_backup(original_db, backup_db):
            print("Step 1: Manually back up your database by running:")
            print(f'   sqlite3 {original_db} ".backup {backup_db}"')
            print("Or use the Python fallback described in the README if the sqlite3 CLI is unavailable.")
            print("Then run this script again.")
            sys.exit(1)

    if not os.path.exists(backup_db):
        print(f"Error: Backup file '{backup_db}' not found. Aborting.")
    else:
        # Step 2: Run the migration from the backup file
        migration_success = migrate_db(old_db_path=backup_db, new_db_path=new_db)

        # Step 3: Automatically verify the result against the original database
        if migration_success:
            verification_passed = verify_migration(
                original_db_path=original_db,
                new_db_path=new_db,
            )
            if verification_passed:
                print("\nNext steps:")
                print(f"  1. Stop any services using {original_db}.")
                print(f"  2. Make a final backup copy of {original_db} if desired.")
                print(f"  3. Replace {original_db} with {new_db} (or point your service at the new file).")
                print("  4. Start your services again.")

