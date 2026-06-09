import sqlite3
import time
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any

# Custom exception classes for database operations
class DatabaseError(Exception):
    """Base exception for database errors."""
    status_code = 500
    detail = "Database error occurred"

class JobNotFoundError(DatabaseError):
    """Raised when a job is not found."""
    status_code = 404
    detail = "Job not found"

class JobInsertError(DatabaseError):
    """Raised when job insertion fails."""
    status_code = 400
    detail = "Failed to insert job"

class JobUpdateError(DatabaseError):
    """Raised when job update fails."""
    status_code = 400
    detail = "Failed to update job"

class DatabaseConnectionError(DatabaseError):
    """Raised when database connection fails."""
    status_code = 503
    detail = "Database connection error"

def get_db_connection(timeout: float = 5.0) -> sqlite3.Connection:
    """
    Return an SQLite connection that
    - creates the db folder if missing,
    - switches the database to WAL mode,
    - waits up to *timeout* seconds when the DB is locked,
    - retries briefly if the open itself fails.
    """
    os.makedirs("db", exist_ok=True)
    db_path = "db/jobs.db"

    deadline = time.time() + timeout
    last_err = None
    while time.time() < deadline:
        try:
            conn = sqlite3.connect(
                db_path,
                timeout=timeout,          # SQLite busy-timeout (ms)
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
        f"Could not open database after {timeout:.1f}s: {last_err}"
    )

def init_db():
    """
    Initialize the database by creating the jobs table if it doesn't exist.
    """
    with get_db_connection() as conn:
        # Create jobs table with schema from design doc
        conn.execute('''
        CREATE TABLE IF NOT EXISTS jobs (
            taskId INTEGER PRIMARY KEY,
            trackId INTEGER NOT NULL,
            sourcePath TEXT NOT NULL,
            destPath TEXT NOT NULL,
            destPathJson TEXT NOT NULL,
            status TEXT CHECK(status IN ('pending', 'running', 'completed', 'completed_no_card', 'failed')) DEFAULT 'pending',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            started_at TIMESTAMP,
            completed_at TIMESTAMP,
            attempts INTEGER DEFAULT 0,
            local_source_path TEXT,
            local_dest_dir TEXT,
            download_status TEXT CHECK(download_status IN ('pending', 'downloading', 'completed', 'failed')) DEFAULT 'pending',
            upload_status TEXT CHECK(upload_status IN ('pending', 'uploading', 'completed', 'failed')) DEFAULT 'pending'
        )
        ''')

def insert_job(task_id: int, track_id: int, source_path: str, dest_path: str, dest_path_json: str,
              local_source_path: str = None, local_dest_dir: str = None) -> int:
    """
    Insert a new job into the database.
    Uses context manager for auto-commit on success.
    
    Args:
        task_id: The task ID (now serves as primary key)
        track_id: The track ID
        source_path: Path to the source audio file
        dest_path: Path where the processed file should be saved
        dest_path_json: Path where the transcription json file should be saved
        local_source_path: Local path for downloaded source file
        local_dest_dir: Local directory for processed outputs
        
    Returns:
        The task ID of the newly inserted job
        
    Raises:
        JobInsertError: If job insertion fails
        DatabaseConnectionError: If database connection fails
    """
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT INTO jobs (taskId, trackId, sourcePath, destPath, destPathJson, status,
                                local_source_path, local_dest_dir, download_status, upload_status)
                VALUES (?, ?, ?, ?, ?, 'pending', ?, ?, 'pending', 'pending')
                """,
                (task_id, track_id, source_path, dest_path, dest_path_json, local_source_path, local_dest_dir)
            )
            if cursor.rowcount == 0:
                raise JobInsertError(f"Failed to insert job with task ID {task_id}")
            return task_id
    except sqlite3.IntegrityError as e:
        raise JobInsertError(f"Job with task ID {task_id} already exists: {str(e)}")
    except sqlite3.Error as e:
        raise DatabaseConnectionError(f"Database error during job insertion: {str(e)}")

def get_job(task_id: int) -> Dict[str, Any]:
    """
    Get a job by its task ID.
    
    Args:
        task_id: The task ID (primary key)
        
    Returns:
        A dictionary containing the job data
        
    Raises:
        JobNotFoundError: If the job is not found
        DatabaseConnectionError: If database connection fails
    """
    try:
        with get_db_connection() as conn:
            job = conn.execute("SELECT * FROM jobs WHERE taskId = ?", (task_id,)).fetchone()
            if not job:
                raise JobNotFoundError(f"Job with task ID {task_id} not found")
            return dict(job)
    except sqlite3.Error as e:
        raise DatabaseConnectionError(f"Database connection failed: {str(e)}")

def update_job_status(task_id: int, status: str) -> bool:
    """
    Update the status of a job.
    Uses context manager for auto-commit/rollback.
    
    Args:
        task_id: The task ID (primary key)
        status: The new status ('pending', 'running', 'completed', 'completed_no_card', 'failed')
        
    Returns:
        True if the update was successful, False otherwise
        
    Raises:
        JobUpdateError: If status is invalid or job update fails
        JobNotFoundError: If the job is not found
        DatabaseConnectionError: If database connection fails
    """
    valid_statuses = ('pending', 'running', 'completed', 'completed_no_card', 'failed')
    if status not in valid_statuses:
        raise JobUpdateError(
            f"Invalid status: {status}. Must be one of: {', '.join(valid_statuses)}"
        )
    
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            # First check if the job exists
            job = conn.execute("SELECT 1 FROM jobs WHERE taskId = ?", (task_id,)).fetchone()
            if not job:
                raise JobNotFoundError(f"Job with task ID {task_id} not found")
            
            # Update timestamp based on status
            timestamp_field = None
            if status == 'running':
                timestamp_field = 'started_at'
            elif status in ('completed', 'completed_no_card', 'failed'):
                timestamp_field = 'completed_at'
            
            if timestamp_field:
                cursor.execute(
                    f"UPDATE jobs SET status = ?, {timestamp_field} = ? WHERE taskId = ?",
                    (status, datetime.now().isoformat(), task_id)
                )
            else:
                cursor.execute(
                    "UPDATE jobs SET status = ? WHERE taskId = ?",
                    (status, task_id)
                )
            
            if cursor.rowcount == 0:
                raise JobUpdateError(f"Failed to update job with task ID {task_id}")
            
            return True
    except (JobNotFoundError, JobUpdateError):
        raise
    except sqlite3.Error as e:
        raise DatabaseConnectionError(f"Database error during job status update: {str(e)}")

def get_queue_stats() -> Dict[str, int]:
    """
    Get statistics about the job queue.
    
    Returns:
        A dictionary with counts of jobs by status
        
    Raises:
        DatabaseConnectionError: If database connection fails
    """
    try:
        with get_db_connection() as conn:
            stats = {
                'pending': 0,
                'running': 0,
                'completed': 0,
                'completed_no_card': 0,
                'failed': 0
            }
            
            for status in stats.keys():
                count = conn.execute(
                    "SELECT COUNT(*) FROM jobs WHERE status = ?", 
                    (status,)
                ).fetchone()[0]
                stats[status] = count
            
            return stats
    except sqlite3.Error as e:
        raise DatabaseConnectionError(f"Database error while getting queue stats: {str(e)}")

def get_download_queue_stats() -> Dict[str, int]:
    """
    Get statistics about the job download queue.
    
    Returns:
        A dictionary with counts of jobs by download_status
        
    Raises:
        DatabaseConnectionError: If database connection fails
    """
    try:
        with get_db_connection() as conn:
            stats = {
                'pending': 0,
                'downloading': 0,
                'completed': 0,
                'failed': 0
            }
            
            for status in stats.keys():
                count = conn.execute(
                    "SELECT COUNT(*) FROM jobs WHERE download_status = ?", 
                    (status,)
                ).fetchone()[0]
                stats[status] = count
            
            return stats
    except sqlite3.Error as e:
        raise DatabaseConnectionError(f"Database error while getting download queue stats: {str(e)}")

def get_upload_queue_stats() -> Dict[str, int]:
    """
    Get statistics about the job upload queue.
    
    Returns:
        A dictionary with counts of jobs by upload_status
        
    Raises:
        DatabaseConnectionError: If database connection fails
    """
    try:
        with get_db_connection() as conn:
            stats = {
                'pending': 0,
                'uploading': 0,
                'completed': 0,
                'failed': 0
            }
            
            for status in stats.keys():
                count = conn.execute(
                    "SELECT COUNT(*) FROM jobs WHERE upload_status = ?", 
                    (status,)
                ).fetchone()[0]
                stats[status] = count
            
            return stats
    except sqlite3.Error as e:
        raise DatabaseConnectionError(f"Database error while getting upload queue stats: {str(e)}")

def lock_job(task_id: int) -> bool:
    """
    Atomically update job status to 'running' and set started_at timestamp.
    
    Args:
        task_id: The task ID (primary key)
        
    Returns:
        True if the job was successfully locked, False otherwise
        
    Raises:
        JobNotFoundError: If the job is not found
        JobUpdateError: If job update fails
        DatabaseConnectionError: If database connection fails
    """
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            # First check if the job exists
            job_exists = conn.execute(
                "SELECT 1 FROM jobs WHERE taskId = ?", 
                (task_id,)
            ).fetchone()
            
            if not job_exists:
                raise JobNotFoundError(f"Job with task ID {task_id} not found")
            
            # Then check if the job is still pending
            job = conn.execute(
                "SELECT status FROM jobs WHERE taskId = ? AND status = 'pending'", 
                (task_id,)
            ).fetchone()
            
            if not job:
                # Job exists but is not in pending status
                return False
            
            # Update status to running and increment attempts
            cursor.execute(
                """
                UPDATE jobs 
                SET status = 'running', 
                    started_at = ?, 
                    attempts = attempts + 1 
                WHERE taskId = ?
                """,
                (datetime.now().isoformat(), task_id)
            )
            
            if cursor.rowcount == 0:
                raise JobUpdateError(f"Failed to lock job with task ID {task_id}")
            
            return True
    except (JobNotFoundError, JobUpdateError):
        raise
    except sqlite3.Error as e:
        raise DatabaseConnectionError(f"Database error during job locking: {str(e)}")

def get_pending_jobs(limit: int) -> List[Dict[str, Any]]:
    """
    Get a batch of pending jobs that are ready for processing.
    Only returns jobs where download_status='completed'.
    
    Args:
        limit: Maximum number of jobs to return
        
    Returns:
        A list of dictionaries containing job data
        
    Raises:
        DatabaseConnectionError: If database connection fails
    """
    try:
        with get_db_connection() as conn:
            jobs = conn.execute(
                """
                SELECT * FROM jobs 
                WHERE status = 'pending' 
                AND download_status = 'completed'
                ORDER BY created_at ASC 
                LIMIT ?
                """,
                (limit,)
            ).fetchall()
            
            return [dict(job) for job in jobs]
    except sqlite3.Error as e:
        raise DatabaseConnectionError(f"Database error while getting pending jobs: {str(e)}")


def update_download_status(task_id: int, download_status: str) -> bool:
    """
    Update the download status of a job.
    
    Args:
        task_id: The task ID (primary key)
        download_status: The new download status ('pending', 'downloading', 'completed', 'failed')
        
    Returns:
        True if the update was successful, False otherwise
        
    Raises:
        JobUpdateError: If status is invalid or job update fails
        JobNotFoundError: If the job is not found
        DatabaseConnectionError: If database connection fails
    """
    # Validate input
    if download_status not in ('pending', 'downloading', 'completed', 'failed'):
        raise JobUpdateError(f"Invalid download status: {download_status}. Must be one of: pending, downloading, completed, failed")

    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            # First check if the job exists
            job = conn.execute("SELECT 1 FROM jobs WHERE taskId = ?", (task_id,)).fetchone()
            if not job:
                raise JobNotFoundError(f"Job with task ID {task_id} not found")
                
            query = """
                UPDATE jobs 
                SET download_status = ? 
                WHERE taskId = ?
            """
            cursor.execute(query, (download_status, task_id))

            if cursor.rowcount == 0:
                raise JobUpdateError(f"Failed to update download status for job with task ID {task_id}")
                
            return True
    except (JobNotFoundError, JobUpdateError):
        raise
    except sqlite3.Error as e:
        raise DatabaseConnectionError(f"Database error during download status update: {str(e)}")
    
def update_upload_status(task_id: int, upload_status: str) -> bool:
    """
    Update the upload status of a job.
    
    Args:
        task_id: The task ID (primary key)
        upload_status: The new upload status ('pending', 'uploading', 'completed', 'failed')
        
    Returns:
        True if the update was successful, False otherwise
        
    Raises:
        JobUpdateError: If status is invalid or job update fails
        JobNotFoundError: If the job is not found
        DatabaseConnectionError: If database connection fails
    """
    # Validate input
    if upload_status not in ('pending', 'uploading', 'completed', 'failed'):
        raise JobUpdateError(f"Invalid upload status: {upload_status}. Must be one of: pending, uploading, completed, failed")

    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            # First check if the job exists
            job = conn.execute("SELECT 1 FROM jobs WHERE taskId = ?", (task_id,)).fetchone()
            if not job:
                raise JobNotFoundError(f"Job with task ID {task_id} not found")
                
            query = """
                UPDATE jobs 
                SET upload_status = ? 
                WHERE taskId = ?
            """
            cursor.execute(query, (upload_status, task_id))

            if cursor.rowcount == 0:
                raise JobUpdateError(f"Failed to update upload status for job with task ID {task_id}")
                
            return True
    except (JobNotFoundError, JobUpdateError):
        raise
    except sqlite3.Error as e:
        raise DatabaseConnectionError(f"Database error during upload status update: {str(e)}")

def find_and_lock_job_for_download() -> Optional[Dict[str, Any]]:
    """
    Atomically finds the oldest job needing download and locks it.

    It finds a job where status and download_status are 'pending',
    immediately updates its download_status to 'downloading', and returns the job data.
    This prevents race conditions between multiple workers.

    Returns:
        The job data as a dictionary if a job was locked, otherwise None.
        
    Raises:
        JobUpdateError: If job update fails
        DatabaseConnectionError: If database connection fails
    """
    try:
        # Ensure this runs in a transaction
        with get_db_connection() as conn:
            cursor = conn.cursor()

            # Step 1: Find the ID of a candidate job.
            cursor.execute(
                """
                SELECT taskId FROM jobs
                WHERE status = 'pending' AND download_status = 'pending'
                ORDER BY created_at ASC
                LIMIT 1
                """
            )
            job_row = cursor.fetchone()

            if not job_row:
                return None # No jobs to process

            task_id_to_lock = job_row['taskId']

            # Step 2: Lock this specific job by updating its status.
            # This is the crucial atomic "claim".
            cursor.execute(
                """
                UPDATE jobs
                SET download_status = 'downloading',
                    started_at = ?
                WHERE taskId = ?
                """,
                (datetime.now().isoformat(), task_id_to_lock)
            )
            
            if cursor.rowcount == 0:
                raise JobUpdateError(f"Failed to lock job with task ID {task_id_to_lock} for download")

            # Step 3: Retrieve the full data for the job we just locked.
            cursor.execute("SELECT * FROM jobs WHERE taskId = ?", (task_id_to_lock,))
            locked_job = cursor.fetchone()

            if not locked_job:
                raise JobNotFoundError(f"Job with task ID {task_id_to_lock} was locked but could not be retrieved")

            return dict(locked_job)
    except (JobNotFoundError, JobUpdateError):
        raise
    except sqlite3.Error as e:
        raise DatabaseConnectionError(f"Database error during job locking for download: {str(e)}")

def get_all_jobs(limit: int = 100, offset: int = 0, status: Optional[str] = None, 
                download_status: Optional[str] = None, upload_status: Optional[str] = None,
                order_by: str = "created_at", order_direction: str = "desc") -> List[Dict[str, Any]]:
    """
    Get all jobs with pagination support.
    
    Args:
        limit: Maximum number of jobs to return (default: 100)
        offset: Number of jobs to skip (default: 0)
        status: Optional filter by job status
        download_status: Optional filter by download status
        upload_status: Optional filter by upload status
        order_by: Field to order by (default: created_at)
        order_direction: Order direction (default: desc)
        
    Returns:
        A list of dictionaries containing job data
        
    Raises:
        DatabaseConnectionError: If database connection fails
    """
    try:
        with get_db_connection() as conn:
            # Build the query dynamically based on parameters
            query = "SELECT * FROM jobs WHERE 1=1"
            params = []
            
            # Add status filters if provided
            if status is not None:
                query += " AND status = ?"
                params.append(status)
                
            if download_status is not None:
                query += " AND download_status = ?"
                params.append(download_status)
                
            if upload_status is not None:
                query += " AND upload_status = ?"
                params.append(upload_status)
            
            # Add ordering
            # Basic SQL injection prevention by validating order_by against known columns
            valid_columns = [
                'taskId', 'trackId', 'sourcePath', 'destPath', 'destPathJson', 'status', 
                'created_at', 'started_at', 'completed_at', 'attempts',
                'local_source_path', 'local_dest_dir', 'download_status', 'upload_status'
            ]
            
            if order_by not in valid_columns:
                raise DatabaseError(f"Invalid order_by column: {order_by}")
                
            if order_direction.lower() == "asc":
                query += f" ORDER BY {order_by} ASC"
            else:
                query += f" ORDER BY {order_by} DESC"
            
            # Add pagination
            query += " LIMIT ? OFFSET ?"
            params.extend([limit, offset])
            
            jobs = conn.execute(query, params).fetchall()
            
            return [dict(job) for job in jobs]
    except sqlite3.Error as e:
        raise DatabaseConnectionError(f"Database error while getting all jobs: {str(e)}")

def reset_failed_jobs(failure_type: str = 'all', task_id: Optional[int] = None, reset_running: bool = False) -> tuple[int, List[int]]:
    """
    Reset the status of failed jobs to 'pending' based on the specified failure type.
    Optionally also reset jobs that are stuck in the 'running' state.
    
    Args:
        failure_type: Type of failure to reset ('download', 'upload', 'masking', 'all')
        task_id: Optional task ID to filter by
        reset_running: If True, also reset jobs that are in the 'running' state
        
    Returns:
        Tuple of (number of jobs that were reset, list of reset task IDs)
        
    Raises:
        JobUpdateError: If failure_type is invalid
        DatabaseConnectionError: If database connection fails
    """
    valid_failure_types = ['download', 'upload', 'masking', 'all']
    if failure_type not in valid_failure_types:
        raise JobUpdateError(f"Invalid failure_type: {failure_type}. Must be one of: {', '.join(valid_failure_types)}")
    
    try:
        reset_count = 0
        reset_task_ids = []
        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            # Build the WHERE clause based on task_id
            task_filter = ""
            params = []
            if task_id is not None:
                task_filter = " AND taskId = ?"
                params.append(task_id)
            
            # Reset download status
            if failure_type == 'download' or failure_type == 'all':
                # Optionally reset stuck downloads (downloading)
                if reset_running:
                    # First get the task IDs that will be affected
                    cursor.execute(
                        f"SELECT taskId FROM jobs WHERE download_status = 'downloading'{task_filter}",
                        params
                    )
                    affected_ids = [row['taskId'] for row in cursor.fetchall()]
                    
                    cursor.execute(
                        f"UPDATE jobs SET download_status = 'pending' WHERE download_status = 'downloading'{task_filter}",
                        params
                    )
                    reset_count += cursor.rowcount
                    reset_task_ids.extend(affected_ids)
                    return reset_count, reset_task_ids
                
                # Reset failed downloads
                # First get the task IDs that will be affected
                cursor.execute(
                    f"SELECT taskId FROM jobs WHERE download_status = 'failed'{task_filter}",
                    params
                )
                affected_ids = [row['taskId'] for row in cursor.fetchall()]
                
                cursor.execute(
                    f"UPDATE jobs SET download_status = 'pending' WHERE download_status = 'failed'{task_filter}",
                    params
                )
                reset_count += cursor.rowcount
                reset_task_ids.extend(affected_ids)
                
            
            # Reset upload status
            if failure_type == 'upload' or failure_type == 'all':
                # Optionally reset stuck uploads (uploading)
                if reset_running:
                    # First get the task IDs that will be affected
                    cursor.execute(
                        f"SELECT taskId FROM jobs WHERE upload_status = 'uploading'{task_filter}",
                        params
                    )
                    affected_ids = [row['taskId'] for row in cursor.fetchall()]
                    
                    cursor.execute(
                        f"UPDATE jobs SET upload_status = 'pending' WHERE upload_status = 'uploading'{task_filter}",
                        params
                    )
                    reset_count += cursor.rowcount
                    reset_task_ids.extend(affected_ids)
                    return reset_count, reset_task_ids
                
                # Reset failed uploads
                # First get the task IDs that will be affected
                cursor.execute(
                    f"SELECT taskId FROM jobs WHERE upload_status = 'failed'{task_filter}",
                    params
                )
                affected_ids = [row['taskId'] for row in cursor.fetchall()]
                
                cursor.execute(
                    f"UPDATE jobs SET upload_status = 'pending' WHERE upload_status = 'failed'{task_filter}",
                    params
                )
                reset_count += cursor.rowcount
                reset_task_ids.extend(affected_ids)
            
            # Reset masking status
            if failure_type == 'masking' or failure_type == 'all':
                # Optionally reset stuck masking jobs (running)
                if reset_running:
                    # First get the task IDs that will be affected
                    cursor.execute(
                        f"SELECT taskId FROM jobs WHERE status = 'running'{task_filter}",
                        params
                    )
                    affected_ids = [row['taskId'] for row in cursor.fetchall()]
                    
                    cursor.execute(
                        f"UPDATE jobs SET status = 'pending', started_at = NULL WHERE status = 'running'{task_filter}",
                        params
                    )
                    reset_count += cursor.rowcount
                    reset_task_ids.extend(affected_ids)
                    return reset_count, reset_task_ids
                
                # Reset failed masking jobs
                # First get the task IDs that will be affected
                cursor.execute(
                    f"SELECT taskId FROM jobs WHERE status = 'failed'{task_filter}",
                    params
                )
                affected_ids = [row['taskId'] for row in cursor.fetchall()]
                
                cursor.execute(
                    f"UPDATE jobs SET status = 'pending', started_at = NULL WHERE status = 'failed'{task_filter}",
                    params
                )
                reset_count += cursor.rowcount
                reset_task_ids.extend(affected_ids)
        
        return reset_count, reset_task_ids
    except sqlite3.Error as e:
        raise DatabaseConnectionError(f"Database error during job reset: {str(e)}")


def list_jobs_for_cleanup(retention_days: int, statuses: List[str]) -> List[Dict[str, Any]]:
    """
    Return all jobs that should be deleted:
      - status IN statuses
      - if status != 'failed': completed_at older than retention_days
    """
    cutoff = (datetime.now() - timedelta(days=retention_days)).isoformat()
    placeholders = ",".join("?" * len(statuses))

    sql = f"""
        SELECT * FROM jobs
        WHERE status IN ({placeholders})
          AND (
                status = 'failed'
                OR (completed_at IS NOT NULL AND completed_at < ?)
              )
    """
    params = statuses + [cutoff]

    with get_db_connection() as conn:
        rows = conn.execute(sql, params).fetchall()
        return [dict(r) for r in rows]


def force_reset_upload_status(task_ids: List[int]) -> tuple[int, List[int]]:
    """
    Force reset upload_status to 'pending' for specific task IDs regardless of current status.
    
    Args:
        task_ids: List of task IDs to reset
        
    Returns:
        Tuple of (number of jobs that were reset, list of reset task IDs)
        
    Raises:
        DatabaseConnectionError: If database connection fails
    """
    if not task_ids:
        return 0, []
    
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            # Create placeholders for the IN clause
            placeholders = ",".join("?" * len(task_ids))
            
            # First, get the task IDs that will be affected (for tracking)
            cursor.execute(
                f"SELECT taskId FROM jobs WHERE taskId IN ({placeholders})",
                task_ids
            )
            existing_task_ids = [row['taskId'] for row in cursor.fetchall()]
            
            # Update upload_status to 'pending' for the specified task IDs
            cursor.execute(
                f"UPDATE jobs SET upload_status = 'pending' WHERE taskId IN ({placeholders})",
                task_ids
            )
            
            reset_count = cursor.rowcount
            return reset_count, existing_task_ids
    except sqlite3.Error as e:
        raise DatabaseConnectionError(f"Database error during force reset of upload status: {str(e)}")


def get_jobs_by_ids(task_ids: List[int]) -> List[Dict[str, Any]]:
    """
    Get jobs by specific task IDs using efficient SQL IN clause.
    
    Args:
        task_ids: List of task IDs to retrieve
        
    Returns:
        A list of dictionaries containing job data for the specified task IDs
        
    Raises:
        DatabaseConnectionError: If database connection fails
    """
    if not task_ids:
        return []
    
    try:
        with get_db_connection() as conn:
            # Create placeholders for the IN clause
            placeholders = ",".join("?" * len(task_ids))
            
            # Execute query with IN clause to fetch only the specified jobs
            jobs = conn.execute(
                f"SELECT * FROM jobs WHERE taskId IN ({placeholders})",
                task_ids
            ).fetchall()
            
            return [dict(job) for job in jobs]
    except sqlite3.Error as e:
        raise DatabaseConnectionError(f"Database error while getting jobs by IDs: {str(e)}")


def delete_jobs_by_ids(task_ids: List[int]) -> int:
    """
    Bulk-delete jobs by taskId.  Returns number of rows deleted.
    """
    if not task_ids:
        return 0
    placeholders = ",".join("?" * len(task_ids))
    with get_db_connection() as conn:
        cur = conn.execute(f"DELETE FROM jobs WHERE taskId IN ({placeholders})", task_ids)
        return cur.rowcount
