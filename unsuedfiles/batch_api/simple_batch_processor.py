#!/usr/bin/env python3
"""
Simple batch processor that uses the EXACT same processing as single_step_processor.py
Only handles OpenAI Batch API mechanics, all processing logic comes from pythonProject2
"""

import os
import sys
import json
import logging
import time
from datetime import datetime
from typing import List, Dict, Optional

# Add parent directory to path to import from pythonProject2
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import EVERYTHING from the main pythonProject2 modules
from single_step_processor import (
    create_unified_prompt,
    parse_unified_response,
    process_single_resume_unified
)
from date_processor import process_resume_with_enhanced_dates
from resume_utils import (
    update_candidate_record_with_retry,
    openai,
    DEFAULT_MODEL,
    get_resume_batch
)
from db_connection import create_pyodbc_connection

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

def submit_batch(batch_size: int = 25) -> Optional[str]:
    """
    Submit a batch job to OpenAI using the exact same prompt as single_step_processor
    """
    try:
        # Get unprocessed resumes using the same function as main.py
        resumes = get_resume_batch(batch_size)

        if not resumes:
            logging.info("No unprocessed resumes found")
            return None

        logging.info(f"Found {len(resumes)} resumes to process")

        # Create JSONL file with requests
        requests = []
        for userid, resume_text in resumes:
            # Use THE EXACT SAME prompt creation as single_step_processor
            messages = create_unified_prompt(resume_text, userid=userid)

            request = {
                "custom_id": f"user_{userid}",
                "method": "POST",
                "url": "/v1/chat/completions",
                "body": {
                    "model": DEFAULT_MODEL,
                    "messages": messages,
                    "temperature": 0.3
                }
            }
            requests.append(request)

        # Write to JSONL file
        batch_file = f"batch_input_{datetime.now().strftime('%Y%m%d_%H%M%S')}.jsonl"
        with open(batch_file, 'w') as f:
            for request in requests:
                f.write(json.dumps(request) + '\n')

        logging.info(f"Created batch file: {batch_file}")

        # Upload file to OpenAI
        with open(batch_file, 'rb') as f:
            file_response = openai.files.create(
                file=f,
                purpose="batch"
            )

        logging.info(f"Uploaded file: {file_response.id}")

        # Submit batch job
        batch_response = openai.batches.create(
            input_file_id=file_response.id,
            endpoint="/v1/chat/completions",
            completion_window="24h"
        )

        logging.info(f"Submitted batch: {batch_response.id}")

        # Clean up local file
        os.remove(batch_file)

        return batch_response.id

    except Exception as e:
        logging.error(f"Error submitting batch: {str(e)}")
        return None

def check_batch(batch_id: str) -> bool:
    """
    Check status of a batch and process results if completed
    """
    try:
        # Get batch status from OpenAI
        batch = openai.batches.retrieve(batch_id)

        logging.info(f"Batch {batch_id} status: {batch.status}")
        logging.info(f"  Request counts - Total: {batch.request_counts.total}, "
                     f"Completed: {batch.request_counts.completed}, "
                     f"Failed: {batch.request_counts.failed}")

        if batch.status == "completed":
            return process_batch_results(batch)
        elif batch.status == "failed":
            logging.error(f"Batch failed: {batch.errors}")
            return False
        else:
            logging.info("Batch still processing...")
            return False

    except Exception as e:
        logging.error(f"Error checking batch: {str(e)}")
        return False

def process_batch_results(batch) -> bool:
    """
    Process completed batch results using the EXACT same parsing as single_step_processor
    """
    try:
        if not batch.output_file_id:
            logging.error("No output file ID in batch response")
            return False

        # Download results file
        output_response = openai.files.content(batch.output_file_id)
        output_content = output_response.read()

        # Parse results
        results = []
        for line in output_content.decode('utf-8').strip().split('\n'):
            if line:
                results.append(json.loads(line))

        logging.info(f"Processing {len(results)} results")

        success_count = 0
        error_count = 0

        for result in results:
            try:
                # Extract user ID from custom_id
                userid = int(result['custom_id'].replace('user_', ''))

                if result['response']['status_code'] != 200:
                    logging.error(f"UserID {userid}: API error - {result['response']['body']}")
                    error_count += 1
                    continue

                # Get the response content
                response_body = result['response']['body']
                content = response_body['choices'][0]['message']['content']

                logging.info(f"Processing UserID {userid}")

                # Use THE EXACT SAME parser as single_step_processor
                parsed_results = parse_unified_response(content)

                # Use THE EXACT SAME date enhancement as single_step_processor
                enhanced_results = process_resume_with_enhanced_dates(userid, parsed_results)

                # Use THE EXACT SAME database update as single_step_processor
                success = update_candidate_record_with_retry(userid, enhanced_results)

                if success:
                    success_count += 1
                    logging.info(f"UserID {userid}: Successfully updated database")
                else:
                    error_count += 1
                    logging.error(f"UserID {userid}: Failed to update database")

            except Exception as e:
                logging.error(f"Error processing result: {str(e)}")
                error_count += 1
                continue

        logging.info(f"Batch processing complete - Success: {success_count}, Errors: {error_count}")
        return success_count > 0

    except Exception as e:
        logging.error(f"Error processing batch results: {str(e)}")
        return False

def main():
    """Main entry point for batch processing"""
    import argparse

    parser = argparse.ArgumentParser(description='Simple batch processor using single_step_processor logic')
    parser.add_argument('--submit', action='store_true', help='Submit a new batch')
    parser.add_argument('--batch-size', type=int, default=25, help='Batch size for submission')
    parser.add_argument('--check-batch', type=str, help='Check status of a batch by ID')

    args = parser.parse_args()

    if args.submit:
        batch_id = submit_batch(args.batch_size)
        if batch_id:
            print(f"Batch submitted successfully: {batch_id}")
        else:
            print("Failed to submit batch")

    elif args.check_batch:
        success = check_batch(args.check_batch)
        if success:
            print("Batch processed successfully")
        else:
            print("Batch not ready or failed")

    else:
        parser.print_help()

if __name__ == "__main__":
    main()