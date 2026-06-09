#!/usr/bin/env python3
"""
Enhanced cleanup script for the audio masking streaming system.

Deletes jobs by status and wipes their local files/directories.
"""

import os
import shutil
from typing import List
from dotenv import load_dotenv
from Masking.logging_utils import LOGGER
from db import list_jobs_for_cleanup, delete_jobs_by_ids, DatabaseConnectionError


load_dotenv()

# Initialize logger from logging_utils
logger = LOGGER.bind(name="cleanup", service_name="cleanup")

DEFAULT_RETENTION_DAYS = int(os.getenv("FILE_RETENTION_DAYS", 1))
VALID_STATUSES = {'pending', 'running', 'completed', 'completed_no_card', 'failed'}


def delete_local_files(local_source_path: str, local_dest_dir: str) -> None:
    """Remove the local files referenced by a job."""
    for path in (local_source_path, local_dest_dir):
        if not path or not os.path.exists(path):
            continue
        try:
            if os.path.isdir(path):
                shutil.rmtree(path)
                logger.debug(f"Removed directory {path}")
            else:
                os.remove(path)
                logger.debug(f"Removed file {path}")
        except Exception as e:
            logger.warning(f"Could not remove {path}: {e}")


def cleanup_jobs(
    retention_days: int = DEFAULT_RETENTION_DAYS,
    statuses: List[str] = None,
    job_type: str = None
) -> None:
    """
    Delete jobs matching the given status(es) that are older than `retention_days`.

    Args:
        retention_days: Number of days to keep jobs (ignored for 'failed').
        statuses: List of statuses to delete.  If omitted, defaults to ['completed'].
    """
    if not statuses:
        statuses = ['completed']

    invalid = set(statuses) - VALID_STATUSES
    if invalid:
        logger.error(f"Invalid status(es): {invalid}.  Allowed: {VALID_STATUSES}")
        return

    logger.info(f"Cleaning up jobs with status {statuses} older than {retention_days} day(s)")
    try:
        jobs = list_jobs_for_cleanup(retention_days, statuses, job_type=job_type)
    except DatabaseConnectionError as e:
        logger.error(e)
        return

    if not jobs:
        logger.info("No jobs to clean up")
        return

    task_ids = [j["taskId"] for j in jobs]
    try:
        deleted = delete_jobs_by_ids(task_ids)
    except DatabaseConnectionError as e:
        logger.error(e)
        return
    
    for j in jobs:
        delete_local_files(j["local_source_path"], j["local_dest_dir"])

    logger.info(f"Deleted {deleted} job(s)")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Clean up jobs by status and wipe local files")
    parser.add_argument(
        "--days",
        type=int,
        default=DEFAULT_RETENTION_DAYS,
        help=f"Keep jobs younger than N days (default: {DEFAULT_RETENTION_DAYS})"
    )
    parser.add_argument(
        "--status",
        default="completed",
        help="Comma-separated list of statuses to delete (default: completed)"
    )
    parser.add_argument(
        "--job-type",
        choices=["masking", "transcription"],
        help="Optional job type filter"
    )

    args = parser.parse_args()
    cleanup_jobs(
        retention_days=args.days,
        statuses=[s.strip() for s in args.status.split(",")],
        job_type=args.job_type
    )
