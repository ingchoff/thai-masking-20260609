"""
Audio masking pipeline orchestrator module.
Coordinates the execution of all masking pipeline steps in sequence.
"""

from typing import List, Dict
from Masking.logging_utils import LOGGER
from Masking.transcribe_stereoloop_schema_v2 import run_transcription_step_pipeline
from Masking.add_masking_column_schema import run_add_masking_column_pipeline
from Masking.step2_mask_chunk_schema import run_mask_chunk_step_pipeline
from Masking.step2b_phase_chunk_schema import run_phase_chunk_step_pipeline
from Masking.getTS_mask_chunk_schema_v2 import run_get_ts_step_pipeline
from Masking.get_wordTS_mask_chunk import run_get_word_ts_step_pipeline
from Masking.step3c import run_step as run_step3c_recheck
from Masking.mask_with_jamai_schema_v2 import run_mask_with_jamai_step_pipeline
from Masking.utils import restart_whisper_services, stop_vllm_services, start_vllm_services
from dotenv import load_dotenv
import os

load_dotenv()

# Initialize pipeline logger
pipeline_logger = LOGGER.bind(type="pipeline", service_name="pipeline")

VLLM_USE_SLEEP=int(os.getenv("VLLM_USE_SLEEP", 1)) == 1

def run_masking_pipeline_batch(tasks: List[Dict]) -> List[Dict]:
    """
    Main pipeline orchestrator function.
    Processes a batch of tasks through the complete masking pipeline.

    Args:
        tasks: List of task dictionaries with structure:
            {
                "taskId": str,       # Unique task identifier
                "trackId": str,       # Track identifier  
                "srcPath": str,       # Source audio file path
                "destPath": str       # Destination directory for outputs
            }

    Returns:
        List of task dictionaries with updated status:
            {
                "taskId": str,
                "trackId": str,
                "status": str,        # "success" or "failed"
                "error": Optional[str],  # Error message if failed
                "masked_output": Optional[str]  # Path to masked file if successful
            }
    """
    try:
        # Initialize all tasks with pending status
        for task in tasks:
            task['status'] = 'pending'
            task['error'] = None
            task['masked_output'] = None

        pipeline_logger.info(f"Starting masking pipeline for {len(tasks)} tasks")

        # Step 1: Transcription
        restart_whisper_services(True)
        stop_vllm_services(VLLM_USE_SLEEP)
        step1_result = run_transcription_step_pipeline(tasks)
        tasks = step1_result['tasks']
        if step1_result['stats']['failed'] > 0:
            pipeline_logger.warning(f"Transcription step had {step1_result['stats']['failed']} failures")

        # Step 2: Add Masking Column
        restart_whisper_services(True)
        start_vllm_services(VLLM_USE_SLEEP)
        step2_result = run_add_masking_column_pipeline(
            table_id=step1_result['table_id'], # step 1 table
            tasks=tasks
        )
        tasks = step2_result['tasks']
        if step2_result['stats']['failed'] > 0:
            pipeline_logger.warning(f"Add masking column step had {step2_result['stats']['failed']} failures")
        pipeline_logger.info(f"Number file needed to be masked: {step2_result['stats']['masking']}/{len(tasks)}")

        # Step 3: Mask Chunk
        step3_result = run_mask_chunk_step_pipeline(
            input_table_id=step2_result['table_id'] # step 1 table
        )
        pipeline_logger.info(f"Step 3: {step3_result}")
        if step3_result['stats']['status'] == 'failed':
            pipeline_logger.error(f"Mask chunk step failed: {step3_result['stats']['message']}")
            return _format_failed_tasks(tasks, f"mask_chunk_failed: {step3_result['stats']['message']}")
        
        # check if step1 detected no credit card 
        # return all tasks as success
        if step3_result["table_id"] == "":
            pipeline_logger.info("No payment card detected...")
            return {
                'tasks': [{
                    'taskId': t['taskId'],
                    'trackId': t['trackId'],
                    'status': "success",
                    'error': None,
                    'needs_masking': False,
                    'masked_output': t.get('masked_output')
                } for t in tasks],
                'stats': {
                    "total": len(tasks),
                    "successful": len(tasks),
                    "failed": 0,
                    "skipped": 0,
                    "status": "completed"
                },
                'success': True
            }

        # Step 4: Phase Chunk
        step4_result = run_phase_chunk_step_pipeline(
            input_table_id=step3_result['table_id'] # step 2 table
        )
        pipeline_logger.info(f"Step 4: {step4_result}")
        if step4_result['stats']['status'] == 'failed':
            pipeline_logger.error(f"Phase chunk step failed: {step4_result['stats']['message']}")
            return _format_failed_tasks(tasks, f"mask_chunk_failed: {step4_result['stats']['message']}")

        # Step 5: Get Timestamps
        step5_result = run_get_ts_step_pipeline(
            tasks=tasks,
            input_table_step2=step4_result['table_id'] # step 2b table
        )
        tasks = step5_result['tasks']
        if step5_result['stats']['status'] == 'failed':
            pipeline_logger.error(f"Get timestamps step failed: {step5_result['stats']['message']}")
            return _format_failed_tasks(tasks, f"mask_chunk_failed: {step5_result['stats']['message']}")
        
        # Step 6: Refine Timestamp with word level timestamp
        step6_result = run_get_word_ts_step_pipeline(
            tasks=tasks,
            input_table_step1=step1_result['table_id'], # step 1 table
            input_table_step3=step5_result['table_id']  # step 3 table
        )
        tasks = step6_result['tasks']
        if step6_result['stats']['status'] == 'failed':
            pipeline_logger.error(f"Get word timestamps step failed: {step6_result['stats']['message']}")
            return _format_failed_tasks(tasks, f"mask_chunk_failed: {step6_result['stats']['message']}")
        
        # check if step3 detected no credit card 
        # return all tasks as success
        if step6_result["table_id"] == "":
            pipeline_logger.info("No payment card detected...")
            return {
                'tasks': [{
                    'taskId': t['taskId'],
                    'trackId': t['trackId'],
                    'status': "success",
                    'error': None,
                    'needs_masking': False,
                    'masked_output': t.get('masked_output')
                } for t in tasks],
                'stats': {
                    "total": len(tasks),
                    "successful": len(tasks),
                    "failed": 0,
                    "skipped": 0,
                    "status": "completed"
                },
                'success': True
            }

        # Step 7: Apply Masking
        final_result = run_mask_with_jamai_step_pipeline(
            table_id=step6_result['table_id'], # step 3 table
            tasks=tasks
        )
        tasks = final_result['tasks']
        if final_result['stats']['status'] == 'failed':
            pipeline_logger.error(f"Masking application step failed: {final_result['stats']['message']}")
            return _format_failed_tasks(tasks, "masking_application_failed")

        # Format final results for worker.py
        return {
            'tasks': [{
                'taskId': t['taskId'],
                'trackId': t['trackId'],
                'status': t.get('status', 'failed'),
                'error': t.get('error'),
                'needs_masking': t['needs_masking'],
                'masked_output': t.get('masked_output')
            } for t in tasks],
            'stats': final_result['stats'],
            'success': final_result['stats']['failed'] == 0
        }

    except Exception as e:
        pipeline_logger.error(f"Pipeline failed with unexpected error: {str(e)}")
        return _format_failed_tasks(tasks, f"pipeline_error: {str(e)}")


def run_transcription_only_batch(tasks: List[Dict]) -> Dict:
    """
    Run only the transcription step for a batch of tasks.
    """
    try:
        for task in tasks:
            task['status'] = 'pending'
            task['error'] = None
            task['masked_output'] = None
            task['needs_masking'] = False

        pipeline_logger.info(f"Starting transcription-only pipeline for {len(tasks)} tasks")

        restart_whisper_services(True)
        stop_vllm_services(VLLM_USE_SLEEP)
        step1_result = run_transcription_step_pipeline(tasks)
        tasks = step1_result['tasks']

        return {
            'tasks': [{
                'taskId': t['taskId'],
                'trackId': t['trackId'],
                'status': 'success' if t.get('status') == 'success' else 'failed',
                'error': t.get('error'),
                'needs_masking': False,
                'masked_output': None
            } for t in tasks],
            'stats': step1_result.get('stats', {}),
            'success': step1_result.get('stats', {}).get('failed', 0) == 0
        }
    except Exception as e:
        pipeline_logger.error(f"Transcription-only pipeline failed: {str(e)}")
        return _format_failed_tasks(tasks, f"transcription_only_error: {str(e)}")

# def _format_failed_tasks(tasks: List[Dict], error_reason: str) -> List[Dict]:
#     """
#     Helper function to format failed tasks consistently.
    
#     Args:
#         tasks: Original task list
#         error_reason: Description of failure reason
        
#     Returns:
#         List of failed task dictionaries
#     """
#     return [{
#         'taskId': t['taskId'],
#         'trackId': t['trackId'],
#         'status': 'failed',
#         'error': error_reason,
#         'needs_masking': t['needs_masking'],
#         'masked_output': None
#     } for t in tasks]
def _format_failed_tasks(tasks: List[Dict], error_reason: str) -> Dict:
    """
    Helper function to format failed tasks consistently.
    
    Args:
        tasks: Original task list
        error_reason: Description of failure reason
        
    Returns:
        Dictionary with:
        {
            'tasks': List of failed task dictionaries,
            'stats': {
                'status': 'failed',
                'message': error_reason,
                'total': len(tasks),
                'failed': len(tasks),
                'success': 0,
                'masking': sum(1 for t in tasks if t.get('needs_masking'))
            },
            'success': False
        }
    """
    failed_tasks = [{
        'taskId': t['taskId'],
        'trackId': t['trackId'],
        'status': 'failed',
        'error': error_reason,
        'needs_masking': t.get('needs_masking'),
        'masked_output': None
    } for t in tasks]
    
    return {
        'tasks': failed_tasks,
        'stats': {
            'status': 'failed',
            'message': error_reason,
            'total': len(tasks),
            'failed': len(tasks),
            'success': 0,
            'masking': sum(1 for t in tasks if t.get('needs_masking', False))
        },
        'success': False
    }
