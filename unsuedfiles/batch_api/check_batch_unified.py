#!/usr/bin/env python3
"""
Batch checker that uses the EXACT unified pathway from main.py --unified
"""

import os
import sys
import json
import logging

# Add parent directory to import from pythonProject2
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import the EXACT functions from single_step_processor
from single_step_processor import parse_unified_response
from date_processor import process_resume_with_enhanced_dates
from resume_utils import update_candidate_record_with_retry, openai

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def check_and_process_batch(batch_id):
    """
    Check and process a batch using the EXACT same pathway as --unified
    This matches: process_single_resume_unified but for batch results
    """
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

        output_response = openai.files.content(batch.output_file_id)
        output_content = output_response.read()

        # Parse JSONL results
        results = []
        for line in output_content.decode('utf-8').strip().split('\n'):
            if line:
                results.append(json.loads(line))

        logging.info(f"Processing {len(results)} results")

        success_count = 0
        failure_count = 0
        successful_userids = []
        failed_userids = []

        for result in results:
            try:
                # Extract user ID (handle both 'user_' and 'unified_' prefixes)
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

                # Get the response content (this is the unified_text in process_single_resume_unified)
                response_body = result['response']['body']
                unified_text = response_body['choices'][0]['message']['content']

                logging.info(f"UserID {userid}: Parsing unified response")

                # EXACT SAME STEP 1: Parse unified response (line 409-410 in process_single_resume_unified)
                parsed_results = parse_unified_response(unified_text)

                # EXACT SAME STEP 2: Process with enhanced dates (line 413 in process_single_resume_unified)
                enhanced_results = process_resume_with_enhanced_dates(userid, parsed_results)

                # EXACT SAME STEP 2.5: Process Top10Skills into Skill1-10 (lines 426-492 in process_single_resume_unified)
                import re

                # Extract skills for database format
                top10_skills_raw = enhanced_results.get("Top10Skills", "")

                if top10_skills_raw and top10_skills_raw != "NULL":
                    # Try different separators
                    if ", " in top10_skills_raw:
                        skills_list = top10_skills_raw.split(", ")
                    elif "," in top10_skills_raw:
                        skills_list = [s.strip() for s in top10_skills_raw.split(",")]
                    else:
                        # Last resort - try to use the value as a single skill
                        skills_list = [top10_skills_raw]
                else:
                    skills_list = []

                    # Try to extract individual skills from the response if possible
                    if "PrimarySoftwareLanguage" in enhanced_results and enhanced_results["PrimarySoftwareLanguage"]:
                        skills_list.append(enhanced_results["PrimarySoftwareLanguage"])
                    if "SecondarySoftwareLanguage" in enhanced_results and enhanced_results["SecondarySoftwareLanguage"]:
                        skills_list.append(enhanced_results["SecondarySoftwareLanguage"])

                # Ensure we have exactly 10 skills with placeholders for empty spots
                skills_list.extend([""] * (10 - len(skills_list)))  # Ensure we have 10 skills
                skills_list = skills_list[:10]  # Limit to exactly 10

                # Clean up phone numbers - prevent duplicates and normalize format
                phone1 = enhanced_results.get("Phone1", "")
                phone2 = enhanced_results.get("Phone2", "")

                # Normalize phone numbers by removing all non-digit characters for comparison
                def normalize_phone(phone):
                    if not phone or phone == "NULL":
                        return ""
                    # Extract only digits
                    digits = re.sub(r'\D', '', phone)
                    # If we have a reasonable number of digits for a phone number
                    if 7 <= len(digits) <= 15:
                        return digits
                    return phone

                normalized_phone1 = normalize_phone(phone1)
                normalized_phone2 = normalize_phone(phone2)

                # If Phone1 and Phone2 have the same digits (even if formatted differently) or Phone2 is NULL, clear Phone2
                if (normalized_phone1 and normalized_phone2 and normalized_phone1 == normalized_phone2) or phone2 == "NULL":
                    enhanced_results["Phone2"] = ""

                # Create a dictionary with all the data for database update using the same structure as the two-step processor
                from two_step_processor_taxonomy import prepare_update_data
                update_data = prepare_update_data(enhanced_results, skills_list=skills_list)

                # Replace "NULL" strings with empty string for database and clean whitespace
                # Also validate and format date fields
                from two_step_processor_taxonomy import validate_date_format, validate_linkedin_url
                for key, value in update_data.items():
                    if isinstance(value, str):
                        value = value.strip()
                        if value.upper() == "NULL" or not value:
                            update_data[key] = ""
                        else:
                            # Special handling for date fields
                            if key.endswith('Date'):  # All date fields end with 'Date'
                                # Validate and convert to SQL-compatible format
                                formatted_date = validate_date_format(value)
                                if formatted_date:
                                    update_data[key] = formatted_date
                                else:
                                    # If unable to parse, set to empty to avoid DB errors
                                    update_data[key] = ""
                            # Special handling for LinkedIn URL
                            elif key == "Linkedin":
                                # Validate and format LinkedIn URL
                                valid_url = validate_linkedin_url(value)
                                if valid_url:
                                    update_data[key] = valid_url
                                else:
                                    # If invalid URL, set to empty
                                    update_data[key] = ""
                            else:
                                update_data[key] = value

                # EXACT SAME STEP 3: Update database (line 554 in process_single_resume_unified)
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

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('batch_id', help='Batch ID to check')
    args = parser.parse_args()

    result = check_and_process_batch(args.batch_id)
    print(json.dumps(result, indent=2))