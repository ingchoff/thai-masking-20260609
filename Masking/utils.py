import os
from dotenv import load_dotenv
import requests
import subprocess
import sys
import time
try:
    from Masking.logging_utils import LOGGER
except ImportError:
    from logging_utils import LOGGER

# Load environment variables from .env file
load_dotenv()

utils_logger = LOGGER.bind(type="pipeline", service_name="utils")

# vLLM service
VLLM_API_HOST = os.getenv("VLLM_API_HOST", "http://localhost:8000")

# Service Restart Configuration
TRANSCRIPTION_SERVICE_NAME_GPU0 = os.getenv(
    "TRANSCRIPTION_SERVICE_NAME_GPU0",
    os.getenv("TRANSCRIPTION_SERVICE_NAME", "whisper-th")
) # Name of the GPU0 whisper docker
TRANSCRIPTION_SERVICE_NAME_GPU1 = os.getenv(
    "TRANSCRIPTION_SERVICE_NAME_GPU1",
    "typhoon-th-gpu1"
) # Name of the GPU1 whisper docker
RESTART_DELAY_SECONDS = int(os.getenv("TRANSCRIPTION_SERVICE_RESTART_DELAY_SECONDS", 10)) # Seconds to wait after restarting
VLLM_SERVICE_NAME = os.getenv("VLLM_SERVICE_NAME", "vllm.service") # Name of the vLLM systemd service

def resolve_transcription_service_name(job_type: str = None) -> str:
    if job_type == "transcription_secondary":
        return TRANSCRIPTION_SERVICE_NAME_GPU1
    return TRANSCRIPTION_SERVICE_NAME_GPU0

# --- Helper Function to Run System Commands ---
def run_system_command(command_list):
    """Executes a system command using subprocess."""
    try:
        # Use check=True to raise error on failure
        # Capture output to avoid cluttering main log unless there's an error
        subprocess.run(command_list, check=True, text=True, capture_output=True)
        # utils_logger.error(process.stdout) # Uncomment to see command output on success
        return True
    except subprocess.CalledProcessError as e:
        utils_logger.error(f"✗ FATAL ERROR: Command failed with exit code {e.returncode}.")
        utils_logger.error(f"  Stderr: {e.stderr}")
        utils_logger.error(f"  Stdout: {e.stdout}")
        return False
    except Exception as e:
        utils_logger.opt(exception=True).error(f"✗ FATAL ERROR: An unexpected error occurred while running command: {e}")
        return False
    

def restart_whisper_services(restart: bool, job_type: str = None):
    if restart:
        service_name = resolve_transcription_service_name(job_type)
        utils_logger.info(f"\nAttempting to restart transcription service: {service_name}")
#        restart_command = ["sudo", "systemctl", "restart", service_name]
        restart_command = ["docker", "restart", service_name]
        if not run_system_command(restart_command):
            utils_logger.error("Service restart failed. Exiting pipeline.")
            # sys.exit(1) # Stop if restart fails

        utils_logger.info(f"Waiting {RESTART_DELAY_SECONDS} seconds for service to initialize...")
        time.sleep(RESTART_DELAY_SECONDS)
        utils_logger.info("Wait complete.")

def stop_vllm_services(sleep: bool = True):
    # sleep (shortcut)
    if sleep:
        try:
            requests.post(f"{VLLM_API_HOST}/sleep")
        # if post failed, likely service not running
        except Exception:
            pass
        return
    utils_logger.info("\nAttempting to stop vLLM service")
    stop_command = ["docker", "stop", VLLM_SERVICE_NAME]
    if not run_system_command(stop_command):
        utils_logger.error("Service stop failed. Exiting pipeline.")
        # sys.exit(1) # Stop if restart fails


def start_vllm_services(sleep: bool = True):
    # sleep (shortcut)
    if sleep:
        requests.post(f"{VLLM_API_HOST}/wake_up")
        return
    # if not sleep then start vLLM
    utils_logger.info("\nAttempting to start vLLM service")
    start_command = ["docker", "start", VLLM_SERVICE_NAME]
    if not run_system_command(start_command):
        utils_logger.error("Service stop failed. Exiting pipeline.")
        return
        # sys.exit(1) # Stop if restart fails

    # wait 1 minutes to test server started
    time.sleep(60)
    # test for another 5 minutes to test server started
    for _ in range(30):
        try:
            response = requests.get(f"{VLLM_API_HOST}/v1/models")
            if response.status_code == 200:
                utils_logger.info("vLLM server started successfully")
                break
        except Exception:
            pass
        time.sleep(10)
