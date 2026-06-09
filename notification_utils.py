import requests
import os
from db import get_job
from Masking.logging_utils import LOGGER
from typing import Any

logger = LOGGER.bind(name="notification", service_name="notification")

# Load environment variables
NOTIFY_ENDPOINT = os.getenv('NOTIFY_ENDPOINT', '')
NOTIFY_USER = os.getenv('NOTIFY_USER', '')
NOTIFY_PASSWORD = os.getenv('NOTIFY_PASSWORD', '')


def send_notification(job_or_task_id: Any, status: str) -> bool:
    """
    Send a notification to the client about the job status.
    
    The notification is sent with the following JSON structure:
    {
        "taskId": 174490,
        "data": [
            {
                "trackId": 13710882,
                "sourcePath": "/EPro/Contact/1/13710882.wav",
                "destPath": "/EPro/JAMAI/2025/2025-06/2025-06-11/13710882/",
                "status": "success"
            }
        ]
    }
    
    Args:
        job_or_task_id: Either the job dictionary or the task ID
        status: The job status
        
    Returns:
        True if the notification was sent successfully, False otherwise
    """
    if not NOTIFY_ENDPOINT:
        logger.warning("Notification endpoint not configured, skipping notification")
        return False
    
    try:
        # Determine if we were passed a job dictionary or just a task ID
        if isinstance(job_or_task_id, dict):
            job = job_or_task_id
            task_id = job['taskId']
        else:
            task_id = job_or_task_id
            # Retrieve the job data from the database
            job = get_job(task_id)
            if not job:
                logger.error(f"Failed to retrieve job data for task {task_id}")
                return False
        
        logger.info(f"Sending notification for task {task_id}: {status}")
        
        # Construct the notification payload with the new structure
        payload = {
            "taskId": task_id,
            "data": [
                {
                    "trackId": job['trackId'],
                    "sourcePath": job['sourcePath'],
                    "destPath": job.get('destPath'),
                    "destPathJson": job.get('destPathJson'),
                    "status": status
                }
            ]
        }
        logger.info(f"payload: {payload}")
        response = requests.post(
            NOTIFY_ENDPOINT,
            json=[payload],
            auth=(NOTIFY_USER, NOTIFY_PASSWORD),
            timeout=10
        )
        
        response.raise_for_status()
        logger.info(f"Notification sent successfully: {response.status_code}")
        return True
        
    except Exception as e:
        logger.error(f"Failed to send notification: {str(e)}")
        return False
