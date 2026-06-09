#!/usr/bin/env python3
"""
FastAPI application for the audio masking streaming system.
Provides endpoints for job submission, queue status, and job monitoring.
"""

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
from typing import List, Optional, Callable, Any
import uvicorn
from datetime import datetime
import functools
import sqlite3
from Masking.logging_utils import LOGGER

from db import (
    init_db, 
    insert_job, 
    get_job, 
    get_queue_stats,
    get_all_jobs,
    get_db_connection,
    reset_failed_jobs,
    DatabaseError,
    JobInsertError,
    DatabaseConnectionError,
)
from sftp_utils import generate_local_paths

# Initialize logger from logging_utils
logger = LOGGER.bind(name="api", service_name="api")
# Exception handler decorator
def handle_exceptions(func: Callable) -> Callable:
    """
    Decorator to handle exceptions in API endpoints.
    Catches custom exceptions and converts them to appropriate HTTP responses.
    """
    @functools.wraps(func)
    async def wrapper(*args: Any, **kwargs: Any) -> Any:
        try:
            return await func(*args, **kwargs)
        except DatabaseError as e:
            # Handle our custom database exceptions
            status_code = getattr(e, 'status_code', 500)
            detail = f"{getattr(e, 'detail', 'Database error')}: {str(e)}"
            logger.error(f"Database error in {func.__name__}: {detail}")
            raise HTTPException(status_code=status_code, detail=detail)
        except sqlite3.Error as e:
            # Handle SQLite errors
            logger.error(f"SQLite error in {func.__name__}: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
        except Exception as e:
            # Handle unexpected errors
            logger.error(f"Unexpected error in {func.__name__}: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")
    return wrapper

# Initialize database on startup
init_db()

app = FastAPI(
    title="Audio Masking API",
    description="API for submitting and monitoring audio masking jobs",
    version="0.0.1"
)

# Define request/response models
class TrackData(BaseModel):
    trackId: int
    sourcePath: str
    destPath: str
    destPathJson: str

class TranscriptionTrackData(BaseModel):
    trackId: int
    sourcePath: str
    destPathJson: str

class JobRequest(BaseModel):
    taskId: int
    data: List[TrackData]

class TranscriptionJobRequest(BaseModel):
    taskId: int
    data: List[TranscriptionTrackData]

class QueueResponse(BaseModel):
    pending: int
    running: int
    completed: int
    failed: int

class JobResponse(BaseModel):
    taskId: int
    trackId: int
    status: str

class JobDetailResponse(BaseModel):
    taskId: int
    status: str
    created: Optional[str] = None
    started: Optional[str] = None
    completed: Optional[str] = None
    sourcePath: str
    outputPath: Optional[str] = None
    outputPathJson: str
    downloadStatus: Optional[str] = None
    uploadStatus: Optional[str] = None
    localSourcePath: Optional[str] = None
    localDestDir: Optional[str] = None
    jobType: Optional[str] = None

class JobListResponse(BaseModel):
    jobs: List[JobDetailResponse]
    total: int
    limit: int
    offset: int
    page: int

class RetryRequest(BaseModel):
    failure_type: str = 'all'  # 'download', 'upload', 'masking', or 'all'
    task_id: Optional[int] = None  # Optional task ID to filter by
    reset_running: bool = False  # If True, also reset jobs that are in the 'running' state

class RetryResponse(BaseModel):
    reset_count: int
    status: str

@app.post("/v1/tasks/create", response_model=List[JobResponse])
@handle_exceptions
async def create_jobs(job_requests: List[JobRequest]):
    """
    Submit new audio masking jobs.
    
    Accepts a list of job requests, each containing a task ID and track data.
    Each track will be processed as a separate job and to be download with a seprate worker.
    """
    jobs = []
    
    for job_request in job_requests:
        task_id = job_request.taskId

        for track in job_request.data:
            # check to ensure destPath is a directory ends with '/'
            # if not track.destPath.endswith('/'):
            #    raise Exception(f"destPath: {track.destPath} must be a directory ending with '/'.")
        
            # check to ensure destPathJson is a json file ends with '.json'
            if not track.destPathJson.endswith('.json'):
                raise Exception(f"destPathJson: {track.destPathJson} must be a json file ending with '.json'.")
        
        for track in job_request.data:
            # Generate local paths
            local_src, local_dest = generate_local_paths(str(task_id), track.sourcePath)
            
            try:
                # Insert job with local paths
                insert_job(
                    task_id=task_id,
                    track_id=track.trackId,
                    source_path=track.sourcePath,
                    dest_path=track.destPath,
                    dest_path_json=track.destPathJson,
                    local_source_path=local_src,
                    local_dest_dir=local_dest,
                    job_type="masking"
                )
                jobs.append({
                    "taskId": task_id,
                    "trackId": track.trackId,
                    "status": "success" # success just means request received
                })
            except JobInsertError as e:
                logger.error(f"Error inserting job: {str(e)}")
                jobs.append({
                    "taskId": task_id,
                    "trackId": track.trackId,
                    "status": "error" # error means request received but failed
                })
    
    return jobs

@app.post("/v1/tasks/transcription/create", response_model=List[JobResponse])
@handle_exceptions
async def create_transcription_jobs(job_requests: List[TranscriptionJobRequest]):
    """
    Submit new transcription-only jobs.
    Validates destPathJson and stores jobs with job_type='transcription' and destPath NULL.
    """
    jobs = []

    for job_request in job_requests:
        task_id = job_request.taskId

        for track in job_request.data:
            if not track.destPathJson.endswith('.json'):
                raise Exception(f"destPathJson: {track.destPathJson} must be a json file ending with '.json'.")

        for track in job_request.data:
            local_src, local_dest = generate_local_paths(str(task_id), track.sourcePath)

            try:
                insert_job(
                    task_id=task_id,
                    track_id=track.trackId,
                    source_path=track.sourcePath,
                    dest_path="None",
                    dest_path_json=track.destPathJson,
                    local_source_path=local_src,
                    local_dest_dir=local_dest,
                    job_type="transcription"
                )
                jobs.append({
                    "taskId": task_id,
                    "trackId": track.trackId,
                    "status": "success"
                })
            except JobInsertError as e:
                logger.error(f"Error inserting transcription job: {str(e)}")
                jobs.append({
                    "taskId": task_id,
                    "trackId": track.trackId,
                    "status": "error"
                })

    return jobs

@app.get("/v1/tasks/queue", response_model=QueueResponse)
@handle_exceptions
async def get_queue():
    """
    Get statistics about the job queue.
    
    Returns counts of jobs by status (pending, running, completed, failed).
    """
    return get_queue_stats()

@app.get("/v1/tasks", response_model=JobDetailResponse)
@handle_exceptions
async def get_job_status(task_id: int = Query(..., description="The task ID to query")):
    """
    Get status of all jobs for a specific task.
    
    Args:
        task_id: The task ID to query
        
    Returns:
        A list of job details for the specified task
    """
    job = get_job(task_id)
    
    result = {
        "taskId": job["taskId"],
        "status": job["status"],
        "created": job["created_at"],
        "started": job["started_at"],
        "completed": job["completed_at"],
        "sourcePath": job["sourcePath"],
        "outputPath": job["destPath"],
        "outputPathJson": job["destPathJson"],
        "localSourcePath": job.get("local_source_path"),
        "localDestDir": job.get("local_dest_dir"),
        "downloadStatus": job.get("download_status"),
        "uploadStatus": job.get("upload_status"),
        "jobType": job.get("job_type")
    }
    
    return result

@app.get("/v1/tasks/transcription", response_model=JobDetailResponse)
@handle_exceptions
async def get_transcription_job_status(task_id: int = Query(..., description="The task ID to query for transcription job")):
    """
    Get status of a transcription-only job.
    """
    job = get_job(task_id, job_type="transcription")

    return {
        "taskId": job["taskId"],
        "status": job["status"],
        "created": job["created_at"],
        "started": job["started_at"],
        "completed": job["completed_at"],
        "sourcePath": job["sourcePath"],
        "outputPath": job["destPath"],
        "outputPathJson": job["destPathJson"],
        "localSourcePath": job.get("local_source_path"),
        "localDestDir": job.get("local_dest_dir"),
        "downloadStatus": job.get("download_status"),
        "uploadStatus": job.get("upload_status"),
        "jobType": job.get("job_type")
    }

# Health check endpoint
@app.get("/v1/tasks/list", response_model=JobListResponse)
@handle_exceptions
async def list_jobs(
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of jobs to return"),
    offset: int = Query(0, ge=0, description="Number of jobs to skip (deprecated, use page instead)"),
    page: int = Query(None, ge=1, description="Page number"),
    status: str = Query(None, description="Filter by job status (pending, running, completed, failed)"),
    download_status: str = Query(None, description="Filter by download status (pending, downloading, completed, failed)"),
    upload_status: str = Query(None, description="Filter by upload status (pending, uploading, completed, failed)"),
    order_by: str = Query("created_at", description="Field to order by"),
    order_direction: str = Query("desc", description="Order direction (asc, desc)"),
    job_type: str = Query(None, description="Filter by job type (masking, transcription)")
):
    """
    List all jobs with pagination support.
    
    Args:
        limit: Maximum number of jobs to return (default: 100)
        offset: Number of jobs to skip (default: 0)
        page: Page number (alternative to offset, 1-based)
        status: Filter by job status (pending, running, completed, failed)
        download_status: Filter by download status (pending, downloading, completed, failed)
        upload_status: Filter by upload status (pending, uploading, completed, failed)
        order_by: Field to order by (default: created_at)
        order_direction: Order direction (asc, desc)
        
    Returns:
        A list of job details with pagination metadata
    """
    # If page is provided, calculate offset
    if page is not None:
        offset = (page - 1) * limit
    
    # Validate status parameter
    valid_statuses = ['pending', 'running', 'completed', 'completed_no_card', 'failed']
    if status is not None and status not in valid_statuses:
        raise HTTPException(
            status_code=400, 
            detail=f"Invalid status: {status}. Must be one of: {', '.join(valid_statuses)}"
        )
    
    # Validate download_status parameter
    valid_download_statuses = ['pending', 'downloading', 'completed', 'failed']
    if download_status is not None and download_status not in valid_download_statuses:
        raise HTTPException(
            status_code=400, 
            detail=f"Invalid download_status: {download_status}. Must be one of: {', '.join(valid_download_statuses)}"
        )
    
    # Validate upload_status parameter
    valid_upload_statuses = ['pending', 'uploading', 'completed', 'failed']
    if upload_status is not None and upload_status not in valid_upload_statuses:
        raise HTTPException(
            status_code=400, 
            detail=f"Invalid upload_status: {upload_status}. Must be one of: {', '.join(valid_upload_statuses)}"
        )
    
    # Validate order_direction parameter
    if order_direction.lower() not in ['asc', 'desc']:
        raise HTTPException(
            status_code=400, 
            detail=f"Invalid order_direction: {order_direction}. Must be 'asc' or 'desc'"
        )
    
    if job_type is not None and job_type not in ['masking', 'transcription']:
        raise HTTPException(
            status_code=400,
            detail="Invalid job_type. Must be 'masking' or 'transcription'"
        )
    
    jobs = get_all_jobs(limit, offset, status, download_status, upload_status, order_by, order_direction, job_type)
    
    # Convert jobs to response format
    job_details = []
    for job in jobs:
        job_details.append({
            "taskId": job["taskId"],
            "status": job["status"],
            "created": job["created_at"],
            "started": job["started_at"],
            "completed": job["completed_at"],
            "sourcePath": job["sourcePath"],
            "outputPath": job["destPath"],
            "outputPathJson": job["destPathJson"],
            "downloadStatus": job.get("download_status"),
            "uploadStatus": job.get("upload_status"),
            "localSourcePath": job.get("local_source_path"),
            "localDestDir": job.get("local_dest_dir"),
            "jobType": job.get("job_type")
        })
    
    # Get total count of jobs for pagination metadata
    try:
        conn = get_db_connection()
        
        # Build WHERE clause for count query
        count_query = "SELECT COUNT(*) FROM jobs WHERE 1=1"
        count_params = []
        
        if status is not None:
            count_query += " AND status = ?"
            count_params.append(status)
            
        if download_status is not None:
            count_query += " AND download_status = ?"
            count_params.append(download_status)
            
        if upload_status is not None:
            count_query += " AND upload_status = ?"
            count_params.append(upload_status)
            
        if job_type is not None:
            count_query += " AND job_type = ?"
            count_params.append(job_type)

        total = conn.execute(count_query, count_params).fetchone()[0]
        conn.close()
    except sqlite3.Error as e:
        raise DatabaseConnectionError(f"Failed to get total job count: {str(e)}")
    
    # Calculate page if offset was used
    if page is None:
        page = (offset // limit) + 1 if limit > 0 else 1
    
    return {
        "jobs": job_details,
        "total": total,
        "limit": limit,
        "offset": offset,
        "page": page
    }

@app.get("/v1/tasks/transcription/list", response_model=JobListResponse)
@handle_exceptions
async def list_transcription_jobs(
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of jobs to return"),
    offset: int = Query(0, ge=0, description="Number of jobs to skip (deprecated, use page instead)"),
    page: int = Query(None, ge=1, description="Page number"),
    status: str = Query(None, description="Filter by job status (pending, running, completed, failed)"),
    download_status: str = Query(None, description="Filter by download status (pending, downloading, completed, failed)"),
    upload_status: str = Query(None, description="Filter by upload status (pending, uploading, completed, failed)"),
    order_by: str = Query("created_at", description="Field to order by"),
    order_direction: str = Query("desc", description="Order direction (asc, desc)")
):
    """
    List transcription-only jobs with pagination support.
    """
    return await list_jobs(
        limit=limit,
        offset=offset,
        page=page,
        status=status,
        download_status=download_status,
        upload_status=upload_status,
        order_by=order_by,
        order_direction=order_direction,
        job_type="transcription"
    )

@app.post("/v1/tasks/retry", response_model=RetryResponse)
@handle_exceptions
async def retry_failed_jobs(retry_request: RetryRequest):
    """
    Retry failed jobs by resetting their status to 'pending'.
    Optionally also reset jobs that are stuck in the 'running' state.
    
    Args:
        retry_request: Request containing failure type, optional task ID, and reset_running flag
        
    Returns:
        Number of jobs that were reset and status message
    """
    # Reset failed jobs - validation is now handled in the db function
    reset_count, _ = reset_failed_jobs(
        failure_type=retry_request.failure_type,
        task_id=retry_request.task_id,
        reset_running=retry_request.reset_running
    )
    
    return {
        "reset_count": reset_count,
        "status": "success"
    }

@app.post("/v1/tasks/transcription/retry", response_model=RetryResponse)
@handle_exceptions
async def retry_failed_transcription_jobs(retry_request: RetryRequest):
    """
    Retry failed transcription jobs by resetting their status to 'pending'.
    """
    reset_count, _ = reset_failed_jobs(
        failure_type=retry_request.failure_type,
        task_id=retry_request.task_id,
        reset_running=retry_request.reset_running,
        job_type="transcription"
    )

    return {
        "reset_count": reset_count,
        "status": "success"
    }

@app.get("/health")
@handle_exceptions
async def health_check():
    """
    Health check endpoint.
    
    Returns a simple status message to confirm the API is running.
    """
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}

if __name__ == "__main__":
    uvicorn.run("api:app", host="0.0.0.0", port=8000, reload=True)
