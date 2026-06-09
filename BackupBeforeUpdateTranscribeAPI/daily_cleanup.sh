#!/usr/bin/env bash
# daily_cleanup.sh
# Cron-safe wrapper for the uv-based cleanup script.

set -euo pipefail

# ------------- CONFIG -------------
PROJECT_DIR="/home/akk/git/thai-masking"          # <-- change to your repo root
RETENTION_DAYS=1                      # keep 1 day of completed/failed jobs
STATUSES="completed,failed"           # what to clean (choices: completed, completed_no_card, pending, failed, running), can use multiple by comma-sperate
# ----------------------------------

cd "$PROJECT_DIR"

# Run the cleanup
exec uv run --env-file .env cleanup_jobs.py \
     --status "$STATUSES" \
     --days "$RETENTION_DAYS"
