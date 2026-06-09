#!/usr/bin/env python3
"""
Background worker for the audio masking streaming system.
This script polls the database for pending jobs, processes them in batches using the existing masking pipeline,
and updates their status.
"""
import os
import time
import signal
import threading
import traceback
import concurrent.futures
from Masking.logging_utils import LOGGER
from typing import Dict, Any, List
from dotenv import load_dotenv
# Import the pipeline module
from pipeline import run_masking_pipeline_batch
# Import database functions
from db import (
    init_db,
    get_pending_jobs,
    get_queue_stats,
    lock_job,
    update_job_status,
    update_upload_status,
)
from sftp_utils import upload_file_to_sftp
from notification_utils import send_notification

# Load environment variables from .env file
load_dotenv()

# Initialize logger from logging_utils
logger = LOGGER.bind(name="worker", service_name="worker")

# Initialize database
init_db()

# Constants
POLL_INTERVAL = int(os.getenv('POLL_INTERVAL', '5'))  # seconds
# Batch processing configuration
BATCH_SIZE = int(os.getenv('BATCH_SIZE', '10'))
MIN_PENDING_JOBS = int(os.getenv('MIN_PENDING_JOBS', '10'))
MIN_PROCESS_INTERVAL = int(os.getenv('MIN_PROCESS_INTERVAL', '60'))  # seconds (1 minute)
MASKING_API_URL_GPU0 = os.getenv(
    'MASKING_API_URL_GPU0',
    os.getenv('MASKING_API_URL', 'http://localhost:28000/v1/audio/transcriptions')
)
MASKING_API_URL_GPU1 = os.getenv(
    'MASKING_API_URL_GPU1',
    'http://localhost:28002/v1/audio/transcriptions'
)

# Add a global flag for shutdown
shutdown_requested = False
shutdown_lock = threading.Lock()

def signal_handler(signum, frame):
    """Handle shutdown signals from systemd"""
    global shutdown_requested
    with shutdown_lock:
        shutdown_requested = True

# Register signal handlers
signal.signal(signal.SIGTERM, signal_handler)  # Sent by systemd on stop/restart
signal.signal(signal.SIGINT, signal_handler)   # Sent by Ctrl+C

class TransientError(Exception):
    """Error that may be resolved by retrying the operation."""
    pass

class PermanentError(Exception):
    """Error that cannot be resolved by retrying the operation."""
    pass

def finalize_batch_results(locked_jobs: List[Dict[str, Any]], results: List[Dict[str, Any]]) -> None:
    for result in results:
        task_id = next((job['taskId'] for job in locked_jobs
                      if job['taskId'] == result.get('taskId') and
                         job['trackId'] == result.get('trackId')), None)

        if task_id:
            status = result.get('final_status') or result.get('status', 'failed')
            if status == 'success':
                status = 'completed'

            valid_statuses = {'pending', 'running', 'completed', 'completed_no_card', 'failed'}
            if status not in valid_statuses:
                status = 'failed'

            update_job_status(task_id, status)

def lock_pending_batch(job_type: str) -> List[Dict[str, Any]]:
    jobs = get_pending_jobs(BATCH_SIZE, job_type=job_type)
    if not jobs:
        return []

    logger.info(f"Processing batch of {len(jobs)} {job_type} jobs")

    locked_jobs = []
    for job in jobs:
        if lock_job(job['taskId']):
            locked_jobs.append(job)
        else:
            logger.warning(f"Failed to lock job {job['taskId']}, skipping")

    return locked_jobs

def process_job_type_batch(job_type: str) -> int:
    locked_jobs = lock_pending_batch(job_type)
    if not locked_jobs:
        return 0

    results = process_batch_jobs(locked_jobs, job_type=job_type)
    finalize_batch_results(locked_jobs, results)
    return len(locked_jobs)

def process_batch(tasks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Process batch of tasks using the masking pipeline
    
    Args:
        tasks: List of task dictionaries with taskId, trackId, srcPath, destPath
        
    Returns:
        List of task dictionaries with:
        - status: "success" or "failed"
        - masked_output: Path to masked file if successful
        - error: Error message if failed
    """
    logger.info(f"Processing batch of {len(tasks)} tasks through masking pipeline")
    
    try:
        # Call the masking pipeline
        results = run_masking_pipeline_batch(tasks)
        results = results.get("tasks")
        # Map pipeline results to expected format
        formatted_results = []
        for task, result in zip(tasks, results):
            formatted = task.copy()
            formatted['status'] = 'success' if result.get('status') == 'success' else 'failed'
            formatted['masked_output'] = result.get('masked_output')
            formatted['error'] = result.get('error')
            formatted['needs_masking'] = result.get('needs_masking')
            formatted_results.append(formatted)
            
        return formatted_results
        
    except Exception as e:
        logger.error(f"Pipeline processing failed: {str(e)}\n{traceback.format_exc()}")
        return [{
            'taskId': t['taskId'],
            'trackId': t['trackId'],
            'status': 'failed',
            'error': str(e),
            'masked_output': None
        } for t in tasks]

def process_batch_jobs(jobs: List[Dict[str, Any]], job_type: str = "masking") -> List[Dict[str, Any]]:
    """
    Process a batch of jobs using the masking pipeline or transcription-only path.
    
    Args:
        jobs: List of job dictionaries
        job_type: 'masking' or 'transcription'
    
    Returns:
        List of job dictionaries with added status attribute
    """
    try:
        logger.info(f"Processing batch of {len(jobs)} jobs")
        
        # Prepare the task list using local paths
        tasks = [
            {
                "taskId": job["taskId"],
                "trackId": job["trackId"],
                "srcPath": job["local_source_path"],
                "destPath": job["local_dest_dir"]
            }
            for job in jobs
        ]
        
        # Call the appropriate batch processing function
        if job_type in ("transcription", "transcription_secondary"):
            from pipeline import run_transcription_only_batch
            api_url = MASKING_API_URL_GPU1 if job_type == "transcription_secondary" else MASKING_API_URL_GPU0
            logger.info(f"Routing {job_type} batch to transcription API: {api_url}")
            results = run_transcription_only_batch(tasks, api_url=api_url, job_type=job_type).get("tasks")
        else:
            results = process_batch(tasks)
        
        # Update job statuses in database
        for job, result in zip(jobs, results):
            job_id = job['taskId']
            status = result.get('status', 'failed')
            job_need_masking = result.get('needs_masking')
            current_job_type = job.get("job_type", job_type)

            # Map pipeline result to database value
            job_status = 'completed' if status == 'success' else 'failed'
            if current_job_type == "masking" and not job_need_masking:
                job_status = 'completed_no_card'

            update_job_status(job_id, job_status)

            # Propagate the final status so the caller doesn't overwrite it
            result['final_status'] = job_status
            result['status'] = job_status

            # Upload results if processing succeeded or masking was skipped
            notification_msg = ""
            final_status = job_status
            if job_status in ('completed', 'completed_no_card'):
                remote_dest = job['destPath']
                remote_dest_json = job['destPathJson']
                local_dest = result.get('masked_output')
                src_path = job['local_source_path']
                transcription_path = os.path.join(job['local_dest_dir'], os.path.basename(src_path))
                transcription_file = ".".join(transcription_path.split('.')[:-1]) + ".json"

                update_upload_status(job_id, 'uploading')
                if current_job_type == "masking":
                    upload_dest = local_dest if job_need_masking else src_path
                    if not upload_file_to_sftp(upload_dest, remote_dest):
                        final_status = 'failed'
                        update_upload_status(job_id, 'failed')
                        notification_msg += f"Failed to upload masked file for task ID: {job['taskId']}\n"
                    else:
                        update_upload_status(job_id, 'completed')

                    if not upload_file_to_sftp(transcription_file, remote_dest_json):
                        logger.error(f"Failed to upload transcription file for task ID: {job['taskId']}")
                        notification_msg += f"Failed to upload transcription file for task ID: {job['taskId']}"
                else:
                    # Transcription-only: upload only transcription JSON
                    if not upload_file_to_sftp(transcription_file, remote_dest_json):
                        logger.error(f"Failed to upload transcription file for task ID: {job['taskId']}")
                        final_status = 'failed'
                        update_upload_status(job_id, 'failed')
                        notification_msg += f"Failed to upload transcription file for task ID: {job['taskId']}"
                    else:
                        update_upload_status(job_id, 'completed')

            if final_status in ('completed', 'failed', 'completed_no_card'):
                if final_status == 'completed_no_card':
                    notification_status = 'completed_no_card'
                else:
                    notification_status = 'success' if final_status == 'completed' else 'failure'

                send_notification(job, notification_msg or notification_status)
        
        logger.info(f"Batch processing completed with {len(results)} results")
        return results
        
    except Exception as e:
        logger.error(f"Batch processing failed: {str(e)}\n{traceback.format_exc()}")
        
        # Mark all jobs as failed in database
        for job in jobs:
            update_job_status(job['taskId'], 'failed')
            send_notification(job, 'failure')
        
        return [{"status": "failed"} for _ in jobs]

def main():
    """
    Main worker loop.
    Polls the database for pending jobs and processes them in batches.
    """
    logger.info("Starting audio masking worker")
    
    last_process_time = 0
    
    while True:
        # Check if shutdown is requested
        with shutdown_lock:
            if shutdown_requested:
                logger.info("Shutting down gracefully")
                break
        
        try:
            current_time = time.time()
            time_since_last_process = current_time - last_process_time
            
            # Get queue stats to check pending job count
            stats_all = get_queue_stats()
            stats_transcription = get_queue_stats(job_type="transcription")
            stats_transcription_secondary = get_queue_stats(job_type="transcription_secondary")
            pending_count = stats_all['pending']
            running_count = stats_all['running']
            transcription_pending = stats_transcription['pending']
            transcription_secondary_pending = stats_transcription_secondary['pending']
            
            # Process jobs if:
            # 1. At least MIN_PROCESS_INTERVAL seconds have passed since the last processing, or
            # 2. There are at least MIN_PENDING_JOBS pending jobs
            # 3. No running jobs
            if (time_since_last_process >= MIN_PROCESS_INTERVAL or 
                pending_count >= MIN_PENDING_JOBS or
                transcription_pending > 0 or
                transcription_secondary_pending > 0):
                if running_count > 0:
                    time.sleep(POLL_INTERVAL)
                    continue
                # Check if shutdown is requested before trying to start a new batch of jobs
                with shutdown_lock:
                    if shutdown_requested:
                        logger.info("Shutdown requested quitting worker process...")
                        break
                transcription_job_types = []
                if transcription_pending > 0:
                    transcription_job_types.append("transcription")
                if transcription_secondary_pending > 0:
                    transcription_job_types.append("transcription_secondary")

                if transcription_job_types:
                    logger.info(f"Processing transcription queues in parallel: {transcription_job_types}")
                    with concurrent.futures.ThreadPoolExecutor(max_workers=len(transcription_job_types)) as executor:
                        futures = {
                            executor.submit(process_job_type_batch, job_type): job_type
                            for job_type in transcription_job_types
                        }
                        for future in concurrent.futures.as_completed(futures):
                            job_type = futures[future]
                            try:
                                processed_count = future.result()
                                logger.info(f"Finished {job_type} batch with {processed_count} locked jobs")
                            except Exception as e:
                                logger.error(f"Parallel {job_type} batch failed: {str(e)}\n{traceback.format_exc()}")
                    last_process_time = time.time()
                else:
                    processed_count = process_job_type_batch("masking")
                    if processed_count:
                        last_process_time = time.time()
                    else:
                        time.sleep(POLL_INTERVAL)
            else:
                # Not time to process yet, wait before checking again
                time.sleep(POLL_INTERVAL)
                
        except Exception as e:
            logger.error(f"Unexpected error in main loop: {str(e)}\n{traceback.format_exc()}")
            time.sleep(POLL_INTERVAL)
    
    logger.info("Worker shutdown complete")

if __name__ == "__main__":
    main()
