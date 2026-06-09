# Audio Masking Streaming System

A streaming processing system for Thai audio masking and transcription-only jobs. Supports e2e masking (transcription + masking + uploads) and transcription-only flows that upload just the JSON.

## System Components

### Deployment & Maintenance (Implemented)

5. **Deployment Configuration**
   - Systemd service file, in services/ (`masking-api.service`, `masking-download.service`, `masking-worker.service`)
   - Log files for worker and cleanup operations
   - Cleanup script for completed jobs/files, a bash script to be used with cron. (`daily_cleanup.sh`)

## Getting Started

### Prerequisites

- uv

1. To install uv
   ```bash
   curl -LsSf https://astral.sh/uv/install.sh | sh
   ```

- vLLM services needs to be running

1. run vllm with docker
  ```bash
  docker run \
  --restart always -d --name vllm-th -e VLLM_SERVER_DEV_MODE=1 \
  -e HF_HUB_OFFLINE=1 -e HF_HOME=/models -v /voices/install/JamAI/hf:/models \
  --gpus all --shm-size 16g -p 18000:8000 --entrypoint vllm \
  vllm/vllm-openai:latest \
  serve Qwen/Qwen3-30B-A3B-Instruct-2507-FP8 \
  --served-model-name Qwen/Qwen3-30B-A3B-Instruct-2507-FP8 \
  -tp 2 --gpu-memory-utilization 0.8 \
  --max-num-seq 32 --disable-log-requests \
  --enable-sleep-mode --max-model-len 46960
  ```

### Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/ingchoff/thai-masking.git
   cd thai-masking
   ```

2. Install dependencies:
   ```bash
   uv sync
   ```

3. Configure the configurations properly with .env:
   ```bash
   # copy .env.example into .env
   # then edit .env with the intended configurations1
   cp .env.example .env
   ```

4. Initialize the database (only need once):
   ```bash
   uv run --env-file .env init_db.py
   ```

### Database migrations

When pulling new code that changes the schema, run the latest migration script to
upgrade your existing database.

1. Stop the API/worker services so nothing writes to the database during the migration.
2. (Optional) Back up the live database yourself before running the migration:
   ```bash
   sqlite3 db/jobs.db ".backup db/jobs_bak.db"
   ```
   If the `sqlite3` CLI is not available on the host, you can run the Python
   standard-library equivalent:
   ```bash
   uv run python - <<'PY'
   import sqlite3, pathlib

   source = pathlib.Path("db/jobs.db")
   backup = pathlib.Path("db/jobs_bak.db")
   backup.parent.mkdir(parents=True, exist_ok=True)

   with sqlite3.connect(source) as src_conn, sqlite3.connect(backup) as dst_conn:
       src_conn.backup(dst_conn)
   PY
   ```
   This step is optional because the migration script will automatically create
   `db/jobs_bak.db` using the same Python fallback if it does not find an
   existing backup. The script checkpoints any pending WAL changes before it
   copies the database; if you see a message that the checkpoint failed, make
   sure all services that might be writing to `db/jobs.db` are stopped and run
   the script again so the WAL contents are flushed.
3. Execute the latest migration (allows secondary transcription jobs):
   ```bash
   uv run python migrate_db_20260609.py --source db/jobs.db --target db/jobs_migrated.db --backup db/jobs_backup_20260609.db --verify --replace
   ```
   - `--backup` writes a backup before migrating.
   - `--verify` checks row counts after migration.
   - `--replace` swaps the migrated DB into place if migration/verification succeed. Without it, the migrated DB is left as `db/jobs_migrated.db` for manual swap.
4. Restart the services.

### Running the System

1. Start the API server:
   ```bash
   uv run --env-file .env run_api.py
   ```

2. Start the background worker:
   ```bash
   uv run --env-file .env worker.py
   ```

3. Start the background download worker:
   ```bash
   uv run --env-file .env download_worker.py
   ```

### Deployment

1. Install the systemd service:
   ```bash
   sudo cp services/*.service /etc/systemd/system/
   sudo systemctl daemon-reload
   sudo systemctl enable masking-api
   sudo systemctl enable masking-download
   sudo systemctl enable masking-worker
   sudo systemctl start masking-api
   sudo systemctl start masking-download
   sudo systemctl start masking-worker
   ```

2. Set up a cron job for cleanup:
   ```bash
   # Add to crontab (crontab -e)
   # remember to update the path to the script
   # remember to update the config in the daily_cleanup.sh
   # this example will run daily at 3am. change as needed
   0 3 * * * /home/trbsysadmin/git/thai-masking/daily_cleanup.sh
   ```

## API Endpoints

### POST /v1/tasks/create
Submit new audio masking jobs (full pipeline).

Accepts a list of job requests, each containing a task ID and track data.
Each track will be processed as a separate job and to be download with a separate worker.

**Request Body (Example):**
```json
[
  {
    "taskId": 174490,
    "data": [
      {
        "trackId": 13710882,
        "sourcePath": "/EPro/Contact/1/13710882.wav",
        "destPath": "/EPro/JAMAI/2025/2025-06/2025-06-11/13710883/",
        "destPathJson": "/EPro/JAMAI/json/13710882.json"
      }
    ]
  }
]
```

**Response (Example):**
```json
[
  {
    "taskId": 174490,
    "trackId": 13710882,
    "status": "success"
  }
]
```

### POST /v1/tasks/transcription/create
Submit transcription-only jobs on GPU0 (runs only transcription; uploads JSON).

**Request Body (Example):**
```json
[
  {
    "taskId": 200001,
    "data": [
      {
        "trackId": 111,
        "sourcePath": "/EPro/Contact/1/13710882.wav",
        "destPathJson": "/EPro/JAMAI/json/13710882.json"
      }
    ]
  }
]
```

Response shape matches `/v1/tasks/create` (taskId/trackId/status).

### POST /v1/tasks/transcription/create-secondary
Submit transcription-only jobs on GPU1. Request and response shape match `/v1/tasks/transcription/create`.

### GET /v1/tasks/queue
Get statistics about the job queue.

Returns counts of jobs by status (pending, running, completed, failed).

**Response (Example):**
```json
{
  "pending": 5,
  "running": 2,
  "completed": 120,
  "failed": 3
}
```

### GET /v1/tasks
Get status of a single job by `task_id`.

**Query Parameters:**
- `task_id` (required): Task ID of the job to fetch.

**Example:** `?task_id=174490`

**Response (Example):**
```json
{
  "taskId": 174490,
  "status": "completed",
  "created": "2025-07-08T01:23:45Z",
  "started": "2025-07-08T01:25:12Z",
  "completed": "2025-07-08T01:28:33Z",
  "sourcePath": "/EPro/Contact/1/13710882.wav",
  "outputPath": "/EPro/JAMAI/2025/2025-06/2025-06-11/13710883/",
  "outputPathJson": "/EPro/JAMAI/json/13710882.json",
  "downloadStatus": "completed",
  "uploadStatus": "completed",
  "localSourcePath": "/tmp/thai-masking/2025-07-08/ORG/174490_13710882.wav",
  "localDestDir": "/tmp/thai-masking/2025-07-08/MASK/174490",
  "jobType": "masking"
}
```

### GET /v1/tasks/transcription
Get status of a transcription-only job by `task_id`.

**Query Parameters:**
- `task_id` (required): Task ID of the transcription job to fetch.

**Example:** `?task_id=200001`

**Response (Example):**
```json
{
  "taskId": 200001,
  "status": "completed",
  "created": "2025-07-08T02:00:01Z",
  "started": "2025-07-08T02:00:30Z",
  "completed": "2025-07-08T02:02:10Z",
  "sourcePath": "/EPro/Contact/1/13710882.wav",
  "outputPath": null,
  "outputPathJson": "/EPro/JAMAI/json/13710882.json",
  "downloadStatus": "completed",
  "uploadStatus": "completed",
  "localSourcePath": "/tmp/thai-masking/2025-07-08/ORG/200001_13710882.wav",
  "localDestDir": "/tmp/thai-masking/2025-07-08/MASK/200001",
  "jobType": "transcription"
}
```

### GET /v1/tasks/list
List jobs with pagination and filters.
- Filters:
  - `status`: `pending|running|completed|completed_no_card|failed`
  - `download_status`: `pending|downloading|completed|failed`
  - `upload_status`: `pending|uploading|completed|failed`
  - `job_type`: `masking|transcription|transcription_secondary`
- Sorting:
  - `order_by`: any DB column exposed by the API (e.g., `created_at`, `destPathJson`, `job_type`, `taskId`, `trackId`)
  - `order_direction`: `asc|desc`
- Pagination:
  - `limit` (default 100, 1–1000)
  - `offset` (deprecated; use `page`)
  - `page` (1-based)

**Example:** `?limit=2&page=1&job_type=transcription&order_by=created_at&order_direction=desc`

**Response (Example):**
```json
{
  "jobs": [
    {
      "taskId": 200001,
      "status": "completed",
      "created": "2025-07-08T02:00:01Z",
      "started": "2025-07-08T02:00:30Z",
      "completed": "2025-07-08T02:02:10Z",
      "sourcePath": "/EPro/Contact/1/13710882.wav",
      "outputPath": null,
      "outputPathJson": "/EPro/JAMAI/json/13710882.json",
      "downloadStatus": "completed",
      "uploadStatus": "completed",
      "localSourcePath": "/tmp/thai-masking/2025-07-08/ORG/200001_13710882.wav",
      "localDestDir": "/tmp/thai-masking/2025-07-08/MASK/200001",
      "jobType": "transcription"
    },
    {
      "taskId": 200000,
      "status": "failed",
      "created": "2025-07-08T01:50:00Z",
      "started": "2025-07-08T01:50:10Z",
      "completed": "2025-07-08T01:50:40Z",
      "sourcePath": "/EPro/Contact/1/13710881.wav",
      "outputPath": null,
      "outputPathJson": "/EPro/JAMAI/json/13710881.json",
      "downloadStatus": "completed",
      "uploadStatus": "failed",
      "localSourcePath": "/tmp/thai-masking/2025-07-08/ORG/200000_13710881.wav",
      "localDestDir": "/tmp/thai-masking/2025-07-08/MASK/200000",
      "jobType": "transcription"
    }
  ],
  "total": 2,
  "limit": 2,
  "offset": 0,
  "page": 1
}
```

### GET /v1/tasks/transcription/list
List transcription-only jobs (same params as /v1/tasks/list, includes both transcription job types).

**Query Parameters:**
- Same as `/v1/tasks/list`, but `job_type` is implicitly `transcription` and `transcription_secondary`.

**Example:** `?limit=1&page=1`

**Response (Example):**
```json
{
  "jobs": [
    {
      "taskId": 200001,
      "status": "completed",
      "created": "2025-07-08T02:00:01Z",
      "started": "2025-07-08T02:00:30Z",
      "completed": "2025-07-08T02:02:10Z",
      "sourcePath": "/EPro/Contact/1/13710882.wav",
      "outputPath": null,
      "outputPathJson": "/EPro/JAMAI/json/13710882.json",
      "downloadStatus": "completed",
      "uploadStatus": "completed",
      "localSourcePath": "/tmp/thai-masking/2025-07-08/ORG/200001_13710882.wav",
      "localDestDir": "/tmp/thai-masking/2025-07-08/MASK/200001",
      "jobType": "transcription"
    }
  ],
  "total": 1,
  "limit": 1,
  "offset": 0,
  "page": 1
}
```

### POST /v1/tasks/retry
Reset failed jobs (filters by failure_type and optional task_id).

**Request Body (Example):**
```json
{
  "failure_type": "all",
  "task_id": null,
  "reset_running": false
}
```

**Response (Example):**
```json
{
  "reset_count": 5,
  "status": "success"
}
```

### POST /v1/tasks/transcription/retry
Reset failed transcription jobs only, including secondary transcription jobs.

**Request/Response:** Same shape as `/v1/tasks/retry`; affects only transcription jobs.

### GET /health
Health check endpoint.

Returns a simple status message to confirm the API is running.

**Response (Example):**
```json
{
  "status": "healthy",
  "timestamp":"2025-07-22T03:11:45.542730"
}
```

## How to run batch process

1. Batch masking process
uv run --env-file .env python Masking/maskaudio_stereoloop_v2.py --input /home/akk/chubb-test --transcribe /home/akk/chubb-test-junk/junk --output /home/akk/chubb-test-junk/junk --option mask
