"""
Hardware Extraction Test Script

This script tests the hardware extraction functionality specifically, 
processing a single resume and logging detailed information about the extraction process.
"""

import logging
import time
import sys
import os
import re
from typing import Dict, Any, List, Tuple

from resume_utils import (
    DEFAULT_MODEL, MAX_TOKENS, 
    num_tokens_from_string, apply_token_truncation,
    update_candidate_record_with_retry,
    openai
)
import pyodbc
from two_step_prompts_taxonomy import create_step1_prompt, create_step2_prompt

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('hardware_extraction_test.log')
    ]
)

# Configuration
MODEL = DEFAULT_MODEL
TEMPERATURE = 0.7  # Higher temperature to encourage more diverse hardware identification

def extract_hardware_section(response_text: str) -> Tuple[List[str], Dict[str, str]]:
    """
    Extract hardware section from response and analyze it in detail
    
    Args:
        response_text: The full response text from the API
        
    Returns:
        Tuple of (raw_hardware_lines, extracted_hardware_items)
    """
    # Dictionary to store extracted hardware items
    extracted = {}
    raw_hardware_lines = []
    hardware_section = ""
    
    # Log the full response for reference
    logging.info(f"FULL RESPONSE:\n{response_text[:5000]}...")
    
    # Try to find the hardware section
    # First look for a section that starts with hardware-related content
    hardware_section_pattern = re.compile(
        r'(?:hardware|physical device|physical hardware).*?(?:^- Based on their skills|^Based on their skills|\Z)',
        re.IGNORECASE | re.MULTILINE | re.DOTALL
    )
    match = hardware_section_pattern.search(response_text)
    
    if match:
        hardware_section = match.group(0)
        logging.info(f"HARDWARE SECTION FOUND:\n{hardware_section}")
        
        # Extract the raw lines
        raw_hardware_lines = [
            line.strip() for line in hardware_section.split('\n')
            if line.strip() and not line.strip().startswith('-')
        ]
        
        # Find hardware items in the expected format
        hardware_format_matches = re.findall(
            r'Hardware (\d): (.+?)(?:\n|$)', 
            hardware_section
        )
        
        if hardware_format_matches:
            logging.info(f"FORMATTED HARDWARE ITEMS FOUND: {len(hardware_format_matches)}")
            for idx, value in hardware_format_matches:
                if idx.isdigit() and 1 <= int(idx) <= 5:
                    field_name = f"Hardware{idx}"
                    clean_value = value.strip()
                    if clean_value.upper() != "NULL" and clean_value:
                        extracted[field_name] = clean_value
                        logging.info(f"EXTRACTED: {field_name}: '{clean_value}'")
        else:
            logging.warning("NO FORMATTED HARDWARE ITEMS FOUND")
            
            # Try alternate formats
            alternate_formats = [
                # Some possible format variations
                r"(\d)\. (.+?):?\s*(.+?)(?:\n|$)",  # 1. Hardware: Value or 1. Value
                r"Hardware\s*(\d):\s*(.+?)(?:\n|$)",  # Hardware1: Value
                r"What physical hardware.+?most\?:\s*(.+?)(?:\n|$)",  # Direct question format
                r"physical hardware.+?(\d).+?:\s*(.+?)(?:\n|$)",  # Physical hardware #1: Value
                r"([Hh]ardware|[Dd]evice)\s*\((\d)\):\s*(.+?)(?:\n|$)"  # Hardware(1): Value
            ]
            
            # Try to match any alternate format
            for pattern in alternate_formats:
                alt_matches = re.findall(pattern, hardware_section)
                if alt_matches:
                    logging.info(f"ALTERNATE FORMAT MATCH: {pattern}")
                    logging.info(f"MATCHES: {alt_matches}")
                    
                    for match in alt_matches:
                        if len(match) == 2:  # Pattern has 2 groups
                            idx, value = match
                            if idx.isdigit() and 1 <= int(idx) <= 5:
                                field_name = f"Hardware{idx}"
                                clean_value = value.strip()
                                if clean_value.upper() != "NULL" and clean_value:
                                    extracted[field_name] = clean_value
                                    logging.info(f"EXTRACTED (ALT): {field_name}: '{clean_value}'")
                        elif len(match) == 3:  # Pattern has 3 groups
                            # Use the 2nd and 3rd groups, or 1st and 3rd depending on pattern
                            if match[0].isdigit() and 1 <= int(match[0]) <= 5:
                                idx, value = match[0], match[2] or match[1]
                                field_name = f"Hardware{idx}"
                                clean_value = value.strip()
                                if clean_value.upper() != "NULL" and clean_value:
                                    extracted[field_name] = clean_value
                                    logging.info(f"EXTRACTED (ALT): {field_name}: '{clean_value}'")
    else:
        logging.error("NO HARDWARE SECTION FOUND IN RESPONSE")
        
        # Search for any line that might contain hardware information
        hardware_mentions = re.findall(
            r'(?:hardware|physical device|equipment).*?:.*?(\w.+?)(?:\n|$)', 
            response_text, 
            re.IGNORECASE
        )
        
        if hardware_mentions:
            logging.info(f"FOUND {len(hardware_mentions)} UNFORMATTED HARDWARE MENTIONS:")
            for i, mention in enumerate(hardware_mentions[:5], 1):
                logging.info(f"  {i}. {mention.strip()}")
                raw_hardware_lines.append(mention.strip())
        else:
            logging.warning("NO HARDWARE MENTIONS FOUND ANYWHERE IN RESPONSE")
    
    # Summary log
    logging.info(f"HARDWARE EXTRACTION SUMMARY:")
    logging.info(f"  Raw hardware lines found: {len(raw_hardware_lines)}")
    logging.info(f"  Extracted hardware items: {len(extracted)}")
    for key, value in extracted.items():
        logging.info(f"  {key}: {value}")
    
    return raw_hardware_lines, extracted

def get_single_resume(userid: str) -> tuple:
    """Get a single resume by userid"""
    server_ip = '172.19.115.25'
    database = 'BH_Mirror'
    username = 'silver'
    password = 'ltechmatlen'
    
    try:
        # Connect to the database
        connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server_ip};DATABASE={database};UID={username};PWD={password}'
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()
        
        # Query to get a specific resume
        query = """
            SELECT userid, Resume as cleaned_resume
            FROM dbo.aicandidate 
            WHERE userid = ?
        """
        
        cursor.execute(query, userid)
        row = cursor.fetchone()
        
        if row:
            userid = row[0]
            cleaned_resume = row[1]
            
            if cleaned_resume and len(str(cleaned_resume).strip()) > 0:
                logging.info(f"Retrieved resume for UserID {userid} (length: {len(cleaned_resume)})")
                return (userid, cleaned_resume)
            else:
                logging.warning(f"Empty resume text for UserID {userid}")
                return None
        else:
            logging.warning(f"No resume found for UserID {userid}")
            return None
            
    except Exception as e:
        logging.error(f"Error retrieving resume: {str(e)}")
        return None
    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn:
            conn.close()

def test_hardware_extraction(userid: str = "226507"):
    """Test hardware extraction on a single resume"""
    try:
        # Use a default userid if none provided - no input prompt to avoid pytest issues
        if not userid or userid == "None":
            # Default to a known userid instead of prompting
            userid = "226507"  # Using Michael Voight's userid as default
            logging.info(f"No userid provided, using default: {userid}")
        
        logging.info(f"Starting hardware extraction test for UserID: {userid}")
        
        # Get resume data
        resume_data = get_single_resume(userid)
        if not resume_data:
            logging.error(f"No resume found for UserID: {userid}")
            return
        
        resume_text = resume_data[1]
        
        # Process using only step 2 which contains hardware extraction
        total_start_time = time.time()
        
        # Need to create step 1 results first to use as input for step 2
        step1_start_time = time.time()
        step1_messages = create_step1_prompt(resume_text, userid=userid)
        step1_messages = apply_token_truncation(step1_messages)
        
        logging.info(f"Sending Step 1 request")
        step1_response = openai.chat.completions.create(
            model=MODEL,
            messages=step1_messages,
            temperature=0.0,  # Use 0 temperature for consistent results in step 1
            max_tokens=MAX_TOKENS
        )
        
        step1_time = time.time() - step1_start_time
        logging.info(f"Step 1 completed in {step1_time:.2f}s")
        
        # Parse step 1 response (minimal processing, just to get required fields for step 2)
        step1_text = step1_response.choices[0].message.content
        
        # Extract basics to feed into step 2
        titles_pattern = re.compile(r'Best job title that fits?.*?experience:\s*(.+)', re.IGNORECASE)
        industry_pattern = re.compile(r'Primary industry.*?:\s*(.+)', re.IGNORECASE)
        
        titles_match = titles_pattern.search(step1_text)
        industry_match = industry_pattern.search(step1_text)
        
        step1_results = {}
        if titles_match:
            step1_results["PrimaryTitle"] = titles_match.group(1).strip()
        if industry_match:
            step1_results["PrimaryIndustry"] = industry_match.group(1).strip()
            
        logging.info(f"Basic Step 1 results: {step1_results}")
        
        # Create step 2 prompt with the minimal step 1 results
        step2_start_time = time.time()
        step2_messages = create_step2_prompt(resume_text, step1_results, userid=userid)
        step2_messages = apply_token_truncation(step2_messages)
        
        # Log the step 2 prompt for reference
        logging.info("STEP 2 PROMPT:")
        for msg in step2_messages:
            if "hardware" in msg.get("content", "").lower():
                logging.info(f"{msg['role']} - {msg['content'][:500]}...")
        
        # Send Step 2 to OpenAI API with high temperature for hardware
        logging.info(f"Sending Step 2 request with temperature {TEMPERATURE}")
        step2_response = openai.chat.completions.create(
            model=MODEL,
            messages=step2_messages,
            temperature=TEMPERATURE,  # High temperature for better hardware extraction
            max_tokens=MAX_TOKENS
        )
        
        step2_time = time.time() - step2_start_time
        logging.info(f"Step 2 completed in {step2_time:.2f}s")
        
        # Parse step 2 response
        step2_text = step2_response.choices[0].message.content
        
        # Detailed hardware section extraction and analysis
        raw_hardware_lines, extracted_hardware = extract_hardware_section(step2_text)
        
        # Log timing statistics
        total_time = time.time() - total_start_time
        logging.info(f"Total processing time: {total_time:.2f}s")
        
        return {
            'userid': userid,
            'raw_hardware_lines': raw_hardware_lines,
            'extracted_hardware': extracted_hardware,
            'processing_time': total_time
        }
    
    except Exception as e:
        logging.error(f"Error in hardware extraction test: {str(e)}", exc_info=True)
        return None

def run_test():
    """Run the hardware extraction test as a standalone script"""
    print("=== HARDWARE EXTRACTION TEST ===")
    
    # If a user ID is provided as command line argument, use it
    userid = sys.argv[1] if len(sys.argv) > 1 else "226507"
    
    result = test_hardware_extraction(userid)
    
    if result:
        print("\nTest completed. Results:")
        print(f"UserID: {result['userid']}")
        print(f"Raw hardware lines found: {len(result['raw_hardware_lines'])}")
        print(f"Extracted hardware items: {len(result['extracted_hardware'])}")
        
        for key, value in result['extracted_hardware'].items():
            print(f"{key}: {value}")
        
        print(f"\nSee hardware_extraction_test.log for detailed output.")
    else:
        print("\nTest failed. Check hardware_extraction_test.log for details.")
    
    return result

if __name__ == "__main__":
    run_test()