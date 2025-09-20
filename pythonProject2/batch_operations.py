"""
OpenAI Batch API Operations
This module contains functions for submitting and checking batch jobs using OpenAI's Batch API.
Uses the unified prompts from single_step_processor.py
"""

import os
import json
import logging
from datetime import datetime
from typing import List, Tuple, Dict, Optional
import concurrent.futures
import threading

from resume_utils import openai, DEFAULT_MODEL, get_resume_batch, get_model_params
from single_step_processor import create_unified_prompt, parse_unified_response
from date_processor import process_resume_with_enhanced_dates
from two_step_processor_taxonomy import prepare_update_data, validate_date_format, validate_linkedin_url
from skills_detector import get_taxonomy_context

# Configure logging
logging.basicConfig(level=logging.INFO)

def create_batch_input_file_with_taxonomy(resume_batch: List[Tuple[int, str]],
                                          filename_prefix: str = "batch_input",
                                          workers: int = 10) -> str:
    """
    Create a JSONL file for OpenAI batch processing with concurrent taxonomy enhancement

    Args:
        resume_batch: List of (userid, resume_text) tuples
        filename_prefix: Prefix for the batch file name
        workers: Number of concurrent workers for taxonomy processing

    Returns:
        Path to the created JSONL file
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    batch_file = f"{filename_prefix}_{timestamp}.jsonl"

    # Get model-specific parameters
    model_params = get_model_params(DEFAULT_MODEL)

    # Process taxonomy enhancement concurrently
    def process_single_resume_for_batch(resume_data):
        userid, resume_text = resume_data

        # Add taxonomy enhancement
        taxonomy_prompt = get_taxonomy_context(resume_text, userid=userid)

        # Create unified prompt with taxonomy
        messages = create_unified_prompt(resume_text, userid=userid)

        # Add taxonomy to the system message if provided
        if taxonomy_prompt:
            messages.insert(1, {"role": "system", "content": taxonomy_prompt})

        # Build request body
        body = {
            "model": DEFAULT_MODEL,
            "messages": messages
        }

        # Only add temperature if the model supports custom values
        if model_params["supports_custom_temp"]:
            body["temperature"] = model_params["temperature"]

        return {
            "custom_id": f"user_{userid}",
            "method": "POST",
            "url": "/v1/chat/completions",
            "body": body
        }

    # Process all resumes concurrently
    requests = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(process_single_resume_for_batch, resume_data): resume_data
                  for resume_data in resume_batch}

        for future in concurrent.futures.as_completed(futures):
            try:
                request = future.result()
                requests.append(request)
            except Exception as e:
                resume_data = futures[future]
                logging.error(f"Error processing resume {resume_data[0]}: {str(e)}")

    # Write all requests to file
    with open(batch_file, 'w') as f:
        for request in requests:
            f.write(json.dumps(request) + '\n')

    logging.info(f"Created enhanced batch input file: {batch_file} with {len(requests)} requests")
    return batch_file

def create_batch_input_file(resume_batch: List[Tuple[int, str]], filename_prefix: str = "batch_input") -> str:
    """
    Create a JSONL file for OpenAI batch processing using unified prompts

    Args:
        resume_batch: List of (userid, resume_text) tuples
        filename_prefix: Prefix for the batch file name

    Returns:
        Path to the created JSONL file
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    batch_file = f"{filename_prefix}_{timestamp}.jsonl"

    # Get model-specific parameters
    model_params = get_model_params(DEFAULT_MODEL)

    with open(batch_file, 'w') as f:
        for userid, resume_text in resume_batch:
            # Use the EXACT same prompt as single unified processing
            messages = create_unified_prompt(resume_text, userid=userid)

            # Build request body with model-specific parameters
            body = {
                "model": DEFAULT_MODEL,
                "messages": messages
            }

            # Only add temperature if the model supports custom values
            if model_params["supports_custom_temp"]:
                body["temperature"] = model_params["temperature"]

            request = {
                "custom_id": f"user_{userid}",
                "method": "POST",
                "url": "/v1/chat/completions",
                "body": body
            }
            f.write(json.dumps(request) + '\n')

    logging.info(f"Created batch input file: {batch_file} with {len(resume_batch)} requests")
    return batch_file

def upload_batch_file(filepath: str) -> Optional[str]:
    """
    Upload a batch file to OpenAI

    Args:
        filepath: Path to the JSONL file

    Returns:
        File ID from OpenAI or None if upload fails
    """
    try:
        with open(filepath, 'rb') as f:
            file_response = openai.files.create(
                file=f,
                purpose="batch"
            )
        logging.info(f"Uploaded batch file to OpenAI: {file_response.id}")
        return file_response.id
    except Exception as e:
        logging.error(f"Failed to upload batch file: {str(e)}")
        return None

def submit_batch_job(file_id: str, endpoint: str = "/v1/chat/completions") -> Optional[str]:
    """
    Submit a batch job to OpenAI

    Args:
        file_id: OpenAI file ID of the uploaded batch
        endpoint: API endpoint for the batch

    Returns:
        Batch ID from OpenAI or None if submission fails
    """
    try:
        batch_response = openai.batches.create(
            input_file_id=file_id,
            endpoint=endpoint,
            completion_window="24h"
        )
        logging.info(f"Submitted batch job to OpenAI: {batch_response.id}")
        return batch_response.id
    except Exception as e:
        logging.error(f"Failed to submit batch job: {str(e)}")
        return None

def get_batch_status(batch_id: str) -> Dict:
    """
    Get the status of a batch job

    Args:
        batch_id: OpenAI batch ID

    Returns:
        Dictionary with batch status information
    """
    try:
        batch = openai.batches.retrieve(batch_id)
        return {
            'id': batch.id,
            'status': batch.status,
            'created_at': batch.created_at,
            'completed_at': batch.completed_at,
            'request_counts': {
                'total': batch.request_counts.total,
                'completed': batch.request_counts.completed,
                'failed': batch.request_counts.failed
            },
            'output_file_id': batch.output_file_id if hasattr(batch, 'output_file_id') else None,
            'error_file_id': batch.error_file_id if hasattr(batch, 'error_file_id') else None
        }
    except Exception as e:
        logging.error(f"Failed to get batch status: {str(e)}")
        return {'status': 'error', 'message': str(e)}

def download_batch_results(file_id: str) -> Optional[List[Dict]]:
    """
    Download and parse batch results from OpenAI

    Args:
        file_id: OpenAI file ID of the results

    Returns:
        List of result dictionaries or None if download fails
    """
    try:
        file_response = openai.files.content(file_id)
        content = file_response.read()

        results = []
        for line in content.decode('utf-8').strip().split('\n'):
            if line:
                results.append(json.loads(line))

        logging.info(f"Downloaded {len(results)} results from batch")
        return results
    except Exception as e:
        logging.error(f"Failed to download batch results: {str(e)}")
        return None

def submit_single_batch_streaming(resume_batch: List[Tuple[int, str]],
                                  workers: int = 10,
                                  use_taxonomy: bool = True) -> Optional[Dict]:
    """
    Submit a single batch of resumes immediately for streaming processing

    Args:
        resume_batch: List of (userid, resume_text) tuples
        workers: Number of concurrent workers for batch preparation
        use_taxonomy: Whether to use taxonomy enhancement

    Returns:
        Dictionary with batch submission information
    """
    if not resume_batch:
        logging.info("No resumes provided for batch submission")
        return None

    logging.info(f"Processing {len(resume_batch)} resumes for immediate batch submission")

    # CRITICAL: Mark these records as "in progress" immediately
    from datetime import datetime
    from resume_utils import update_candidate_record_with_retry

    batch_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    userids_to_process = [userid for userid, _ in resume_batch]

    logging.info(f"Marking {len(userids_to_process)} records as in-progress using {workers} workers")

    def mark_single_record(userid):
        # Update only the LastProcessed field to reserve this record
        update_data = {"LastProcessed": batch_timestamp}
        success = update_candidate_record_with_retry(userid, update_data)
        if not success:
            logging.warning(f"Failed to mark UserID {userid} as in-progress")
        return success

    # Mark records concurrently
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [executor.submit(mark_single_record, userid) for userid in userids_to_process]
        results = [future.result() for future in concurrent.futures.as_completed(futures)]
        successful_marks = sum(1 for r in results if r)
        logging.info(f"Successfully marked {successful_marks}/{len(userids_to_process)} records as in-progress")

    # Create batch input file with or without taxonomy enhancement
    if use_taxonomy:
        batch_file = create_batch_input_file_with_taxonomy(resume_batch, workers=workers)
    else:
        batch_file = create_batch_input_file(resume_batch)

    # Upload to OpenAI
    file_id = upload_batch_file(batch_file)
    if not file_id:
        logging.error("Failed to upload batch file")
        return None

    # Submit batch job
    batch_id = submit_batch_job(file_id)
    if not batch_id:
        logging.error("Failed to submit batch job")
        return None

    # Clean up local file
    try:
        os.remove(batch_file)
    except:
        pass

    return {
        'batch_id': batch_id,
        'file_id': file_id,
        'resume_count': len(resume_batch),
        'userids': userids_to_process
    }

def submit_resume_batch(batch_size: int = 25) -> Optional[Dict]:
    """
    Submit a batch of resumes for processing using unified prompts

    Args:
        batch_size: Number of resumes to process

    Returns:
        Dictionary with batch submission information
    """
    # Get resumes from database
    resume_batch = get_resume_batch(batch_size)

    if not resume_batch:
        logging.info("No unprocessed resumes found")
        return None

    # Limit to requested batch size
    resume_batch = resume_batch[:batch_size]
    logging.info(f"Processing {len(resume_batch)} resumes for batch submission")

    # CRITICAL: Mark these records as "in progress" immediately to prevent duplicates
    # This must happen BEFORE creating the batch file to avoid race conditions
    from datetime import datetime
    from resume_utils import update_candidate_record_with_retry

    batch_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    userids_to_process = [userid for userid, _ in resume_batch]

    logging.info(f"Marking {len(userids_to_process)} records as in-progress with timestamp {batch_timestamp}")
    for userid in userids_to_process:
        # Update only the LastProcessed field to reserve this record
        update_data = {"LastProcessed": batch_timestamp}
        success = update_candidate_record_with_retry(userid, update_data)
        if not success:
            logging.warning(f"Failed to mark UserID {userid} as in-progress")

    logging.info("All records marked as in-progress, creating batch file...")

    # Create batch input file with unified prompts
    batch_file = create_batch_input_file(resume_batch)

    # Upload to OpenAI
    file_id = upload_batch_file(batch_file)
    if not file_id:
        logging.error("Failed to upload batch file")
        return None

    # Submit batch job
    batch_id = submit_batch_job(file_id)
    if not batch_id:
        logging.error("Failed to submit batch job")
        return None

    # Clean up local file
    try:
        os.remove(batch_file)
    except:
        pass

    return {
        'batch_id': batch_id,
        'file_id': file_id,
        'resume_count': len(resume_batch),
        'userids': [userid for userid, _ in resume_batch]
    }

def check_and_process_batch(batch_id: str) -> Dict:
    """
    Check batch status and process results if complete using unified processing

    Args:
        batch_id: OpenAI batch ID

    Returns:
        Dictionary with processing results
    """
    import re
    from resume_utils import update_candidate_record_with_retry

    try:
        # Get batch status
        batch = openai.batches.retrieve(batch_id)

        logging.info(f"Batch {batch_id} status: {batch.status}")
        logging.info(f"  Request counts - Total: {batch.request_counts.total}, "
                     f"Completed: {batch.request_counts.completed}, "
                     f"Failed: {batch.request_counts.failed}")

        if batch.status != "completed":
            return {
                'status': batch.status,
                'message': f"Batch is {batch.status}, not ready to process"
            }

        # Download results
        if not batch.output_file_id:
            logging.error("No output file ID")
            return {'status': 'error', 'message': 'No output file ID'}

        results = download_batch_results(batch.output_file_id)
        if not results:
            return {'status': 'error', 'message': 'Failed to download results'}

        logging.info(f"Processing {len(results)} results")

        success_count = 0
        failure_count = 0
        successful_userids = []
        failed_userids = []

        for result in results:
            try:
                # Extract user ID from custom_id (handles both 'user_' and 'unified_' prefixes)
                custom_id = result['custom_id']
                if custom_id.startswith('user_'):
                    userid = int(custom_id.replace('user_', ''))
                elif custom_id.startswith('unified_'):
                    userid = int(custom_id.replace('unified_', ''))
                else:
                    # Try to extract any number from the custom_id
                    userid = int(''.join(filter(str.isdigit, custom_id)))

                # Check for API errors
                if result['response']['status_code'] != 200:
                    logging.error(f"UserID {userid}: API error - {result['response']['body']}")
                    failure_count += 1
                    failed_userids.append(userid)
                    continue

                # Get the response content
                response_body = result['response']['body']
                unified_text = response_body['choices'][0]['message']['content']

                logging.info(f"UserID {userid}: Parsing unified response")

                # EXACT SAME as process_single_resume_unified:
                # Step 1: Parse unified response
                parsed_results = parse_unified_response(unified_text)

                # Step 2: Process with enhanced dates
                enhanced_results = process_resume_with_enhanced_dates(userid, parsed_results)

                # Step 3: Process Top10Skills into Skill1-10
                top10_skills_raw = enhanced_results.get("Top10Skills", "")

                if top10_skills_raw and top10_skills_raw != "NULL":
                    # Handle different formats (comma-separated or numbered list)
                    if ", " in top10_skills_raw:
                        skills_list = top10_skills_raw.split(", ")
                    elif "," in top10_skills_raw:
                        skills_list = [s.strip() for s in top10_skills_raw.split(",")]
                    else:
                        # Clean numbered list format (e.g., "1. Skill Name" -> "Skill Name")
                        skills_list = [re.sub(r'^\d+\.\s*', '', top10_skills_raw.strip())]
                else:
                    skills_list = []
                    # Try to extract from other fields
                    if "PrimarySoftwareLanguage" in enhanced_results and enhanced_results["PrimarySoftwareLanguage"]:
                        skills_list.append(enhanced_results["PrimarySoftwareLanguage"])
                    if "SecondarySoftwareLanguage" in enhanced_results and enhanced_results["SecondarySoftwareLanguage"]:
                        skills_list.append(enhanced_results["SecondarySoftwareLanguage"])

                # Ensure we have exactly 10 skills with placeholders
                skills_list.extend([""] * (10 - len(skills_list)))
                skills_list = skills_list[:10]

                # Clean up phone numbers
                phone1 = enhanced_results.get("Phone1", "")
                phone2 = enhanced_results.get("Phone2", "")

                def normalize_phone(phone):
                    if not phone or phone == "NULL":
                        return ""
                    # Extract only digits
                    digits = re.sub(r'\D', '', phone)
                    if 7 <= len(digits) <= 15:
                        return digits
                    return phone

                normalized_phone1 = normalize_phone(phone1)
                normalized_phone2 = normalize_phone(phone2)

                if (normalized_phone1 and normalized_phone2 and normalized_phone1 == normalized_phone2) or phone2 == "NULL":
                    enhanced_results["Phone2"] = ""

                # Create update data using prepare_update_data
                update_data = prepare_update_data(enhanced_results, skills_list=skills_list)

                # Clean and validate data
                for key, value in update_data.items():
                    if isinstance(value, str):
                        value = value.strip()
                        if value.upper() == "NULL" or not value:
                            update_data[key] = ""
                        else:
                            # Handle date fields
                            if key.endswith('Date'):
                                formatted_date = validate_date_format(value)
                                if formatted_date:
                                    update_data[key] = formatted_date
                                else:
                                    update_data[key] = ""
                            # Handle LinkedIn URL
                            elif key == "Linkedin":
                                valid_url = validate_linkedin_url(value)
                                if valid_url:
                                    update_data[key] = valid_url
                                else:
                                    update_data[key] = ""
                            else:
                                update_data[key] = value

                # Add LastProcessed timestamp to mark this record as processed
                from datetime import datetime
                update_data["LastProcessed"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

                # Step 4: Update database
                update_success = update_candidate_record_with_retry(userid, update_data)

                if update_success:
                    success_count += 1
                    successful_userids.append(userid)
                    logging.info(f"UserID {userid}: Successfully updated database")
                else:
                    failure_count += 1
                    failed_userids.append(userid)
                    logging.error(f"UserID {userid}: Failed to update database")

            except Exception as e:
                logging.error(f"Error processing result: {str(e)}")
                failure_count += 1
                if 'userid' in locals():
                    failed_userids.append(userid)

        return {
            'status': 'completed',
            'success_count': success_count,
            'failure_count': failure_count,
            'successful_userids': successful_userids,
            'failed_userids': failed_userids
        }

    except Exception as e:
        logging.error(f"Fatal error checking batch: {str(e)}")
        return {
            'status': 'error',
            'message': str(e)
        }