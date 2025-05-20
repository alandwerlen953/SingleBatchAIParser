"""
Process a single resume by user ID with detailed logging
"""

import sys
import logging
import time
import json
import re
from datetime import datetime
from resume_utils import (
    get_resume_by_userid, update_candidate_record_with_retry, diagnose_database_fields,
    is_valid_sql_date, openai, DEFAULT_MODEL, MAX_TOKENS, DEFAULT_TEMPERATURE,
    num_tokens_from_string, apply_token_truncation
)
from two_step_processor_taxonomy import (
    process_single_resume_two_step, 
    create_step1_prompt, create_step2_prompt,
    parse_step1_response, parse_step2_response,
    extract_fields_directly
)

def process_with_detailed_logging(userid, resume_text):
    """Process a resume with detailed logging of each step"""
    try:
        logging.info(f"Starting detailed processing for UserID: {userid}")
        total_start_time = time.time()
        
        # Store the raw resume and all processing steps
        processing_log = {
            "userid": userid,
            "resume_length": len(resume_text),
            "resume_preview": resume_text[:500] + "..." if len(resume_text) > 500 else resume_text,
            "step1": {},
            "step2": {},
            "final_output": {}
        }
        
        # STEP 1: Personal info, work history, and industry
        step1_start_time = time.time()
        
        # Create step 1 prompt
        step1_messages = create_step1_prompt(resume_text, userid=userid)
        step1_messages = apply_token_truncation(step1_messages)
        
        # Store prompts for logging
        processing_log["step1"]["system_prompts"] = [msg["content"] for msg in step1_messages if msg["role"] == "system"]
        processing_log["step1"]["user_prompt"] = next((msg["content"] for msg in step1_messages if msg["role"] == "user"), "")
        
        # Send to OpenAI API
        logging.info(f"Sending Step 1 request for UserID: {userid}")
        step1_response = openai.chat.completions.create(
            model=DEFAULT_MODEL,
            messages=step1_messages,
            temperature=DEFAULT_TEMPERATURE,
            max_tokens=MAX_TOKENS
        )
        
        # Get and parse step 1 response
        step1_text = step1_response.choices[0].message.content
        processing_log["step1"]["raw_response"] = step1_text
        
        step1_results = parse_step1_response(step1_text)
        processing_log["step1"]["parsed_fields"] = step1_results
        
        step1_time = time.time() - step1_start_time
        processing_log["step1"]["processing_time"] = f"{step1_time:.2f}s"
        logging.info(f"Step 1 completed in {step1_time:.2f}s")
        
        # STEP 2: Skills, technical info, and experience calculations
        step2_start_time = time.time()
        
        # Create step 2 prompt using results from step 1
        step2_messages = create_step2_prompt(resume_text, step1_results, userid=userid)
        step2_messages = apply_token_truncation(step2_messages)
        
        # Store prompts for logging
        processing_log["step2"]["system_prompts"] = [msg["content"] for msg in step2_messages if msg["role"] == "system"]
        processing_log["step2"]["user_prompt"] = next((msg["content"] for msg in step2_messages if msg["role"] == "user"), "")
        
        # Send Step 2 to OpenAI API
        logging.info(f"Sending Step 2 request for UserID: {userid}")
        step2_response = openai.chat.completions.create(
            model=DEFAULT_MODEL,
            messages=step2_messages,
            temperature=0.5,  # Higher temperature for hardware section
            max_tokens=MAX_TOKENS
        )
        
        # Get and parse step 2 response
        step2_text = step2_response.choices[0].message.content
        processing_log["step2"]["raw_response"] = step2_text
        
        step2_results = parse_step2_response(step2_text)
        processing_log["step2"]["parsed_fields"] = step2_results
        
        step2_time = time.time() - step2_start_time
        processing_log["step2"]["processing_time"] = f"{step2_time:.2f}s"
        logging.info(f"Step 2 completed in {step2_time:.2f}s")
        
        # Combine and process the results
        from date_processor import process_resume_with_enhanced_dates
        combined_results = {**step1_results, **step2_results}
        
        # Apply date processing
        enhanced_results = process_resume_with_enhanced_dates(userid, combined_results)
        processing_log["date_processing"] = {
            "input": combined_results,
            "output": enhanced_results
        }
        
        # Extract skills and format final data
        skills_list = enhanced_results.get("Top10Skills", "").split(", ") if enhanced_results.get("Top10Skills") and enhanced_results.get("Top10Skills") != "NULL" else []
        skills_list.extend([""] * (10 - len(skills_list)))  # Ensure we have 10 skills
        Skill1, Skill2, Skill3, Skill4, Skill5, Skill6, Skill7, Skill8, Skill9, Skill10 = skills_list[:10]
        
        # Create final output dictionary (same structure as the database update)
        update_data = {
            "PrimaryTitle": enhanced_results.get("PrimaryTitle") or step1_results.get("PrimaryTitle") or "",
            "SecondaryTitle": enhanced_results.get("SecondaryTitle") or step1_results.get("SecondaryTitle") or "",
            "TertiaryTitle": enhanced_results.get("TertiaryTitle") or step1_results.get("TertiaryTitle") or "",
            "Address": enhanced_results.get("Address", ""),
            "City": enhanced_results.get("City", ""),
            "State": enhanced_results.get("State", ""),
            "ZipCode": enhanced_results.get("ZipCode", ""),
            "Certifications": enhanced_results.get("Certifications", ""),
            "Bachelors": enhanced_results.get("Bachelors", ""),
            "Masters": enhanced_results.get("Masters", ""),
            "Phone1": enhanced_results.get("Phone1", ""),
            "Phone2": enhanced_results.get("Phone2", ""),
            "Email": enhanced_results.get("Email", ""),
            "Email2": enhanced_results.get("Email2", ""),
            "FirstName": enhanced_results.get("FirstName", ""),
            "MiddleName": enhanced_results.get("MiddleName", ""),
            "LastName": enhanced_results.get("LastName", ""),
            "Linkedin": enhanced_results.get("Linkedin", ""),
            "MostRecentCompany": enhanced_results.get("MostRecentCompany", ""),
            "MostRecentStartDate": enhanced_results.get("MostRecentStartDate", ""),
            "MostRecentEndDate": enhanced_results.get("MostRecentEndDate", ""),
            "MostRecentLocation": enhanced_results.get("MostRecentLocation", ""),
            "SecondMostRecentCompany": enhanced_results.get("SecondMostRecentCompany", ""),
            "SecondMostRecentStartDate": enhanced_results.get("SecondMostRecentStartDate", ""),
            "SecondMostRecentEndDate": enhanced_results.get("SecondMostRecentEndDate", ""),
            "SecondMostRecentLocation": enhanced_results.get("SecondMostRecentLocation", ""),
            "ThirdMostRecentCompany": enhanced_results.get("ThirdMostRecentCompany", ""),
            "ThirdMostRecentStartDate": enhanced_results.get("ThirdMostRecentStartDate", ""),
            "ThirdMostRecentEndDate": enhanced_results.get("ThirdMostRecentEndDate", ""),
            "ThirdMostRecentLocation": enhanced_results.get("ThirdMostRecentLocation", ""),
            "FourthMostRecentCompany": enhanced_results.get("FourthMostRecentCompany", ""),
            "FourthMostRecentStartDate": enhanced_results.get("FourthMostRecentStartDate", ""),
            "FourthMostRecentEndDate": enhanced_results.get("FourthMostRecentEndDate", ""),
            "FourthMostRecentLocation": enhanced_results.get("FourthMostRecentLocation", ""),
            "FifthMostRecentCompany": enhanced_results.get("FifthMostRecentCompany", ""),
            "FifthMostRecentStartDate": enhanced_results.get("FifthMostRecentStartDate", ""),
            "FifthMostRecentEndDate": enhanced_results.get("FifthMostRecentEndDate", ""),
            "FifthMostRecentLocation": enhanced_results.get("FifthMostRecentLocation", ""),
            "SixthMostRecentCompany": enhanced_results.get("SixthMostRecentCompany", ""),
            "SixthMostRecentStartDate": enhanced_results.get("SixthMostRecentStartDate", ""),
            "SixthMostRecentEndDate": enhanced_results.get("SixthMostRecentEndDate", ""),
            "SixthMostRecentLocation": enhanced_results.get("SixthMostRecentLocation", ""),
            "SeventhMostRecentCompany": enhanced_results.get("SeventhMostRecentCompany", ""),
            "SeventhMostRecentStartDate": enhanced_results.get("SeventhMostRecentStartDate", ""),
            "SeventhMostRecentEndDate": enhanced_results.get("SeventhMostRecentEndDate", ""),
            "SeventhMostRecentLocation": enhanced_results.get("SeventhMostRecentLocation", ""),
            "PrimaryIndustry": enhanced_results.get("PrimaryIndustry", ""),
            "SecondaryIndustry": enhanced_results.get("SecondaryIndustry", ""),
            "Skill1": Skill1,
            "Skill2": Skill2,
            "Skill3": Skill3,
            "Skill4": Skill4,
            "Skill5": Skill5,
            "Skill6": Skill6,
            "Skill7": Skill7,
            "Skill8": Skill8,
            "Skill9": Skill9,
            "Skill10": Skill10,
            "PrimarySoftwareLanguage": enhanced_results.get("PrimarySoftwareLanguage", ""),
            "SecondarySoftwareLanguage": enhanced_results.get("SecondarySoftwareLanguage", ""),
            "TertiarySoftwareLanguage": enhanced_results.get("TertiarySoftwareLanguage", ""),
            "SoftwareApp1": enhanced_results.get("SoftwareApp1", ""),
            "SoftwareApp2": enhanced_results.get("SoftwareApp2", ""),
            "SoftwareApp3": enhanced_results.get("SoftwareApp3", ""),
            "SoftwareApp4": enhanced_results.get("SoftwareApp4", ""),
            "SoftwareApp5": enhanced_results.get("SoftwareApp5", ""),
            "Hardware1": enhanced_results.get("Hardware1", ""),
            "Hardware2": enhanced_results.get("Hardware2", ""),
            "Hardware3": enhanced_results.get("Hardware3", ""),
            "Hardware4": enhanced_results.get("Hardware4", ""),
            "Hardware5": enhanced_results.get("Hardware5", ""),
            "PrimaryCategory": enhanced_results.get("PrimaryCategory", ""),
            "SecondaryCategory": enhanced_results.get("SecondaryCategory", ""),
            "ProjectTypes": enhanced_results.get("ProjectTypes", ""),
            "Specialty": enhanced_results.get("Specialty", ""),
            "Summary": enhanced_results.get("Summary", ""),
            "LengthinUS": enhanced_results.get("LengthinUS", ""),  # AI value preserved
            "YearsofExperience": enhanced_results.get("YearsofExperience", ""),  # AI value preserved
            "AvgTenure": enhanced_results.get("AvgTenure", "")  # AI value preserved
        }
        
        # Clean up NULL values and whitespace
        date_fields = ["MostRecentStartDate", "MostRecentEndDate", "SecondMostRecentStartDate", "SecondMostRecentEndDate", 
                     "ThirdMostRecentStartDate", "ThirdMostRecentEndDate", "FourthMostRecentStartDate", "FourthMostRecentEndDate", 
                     "FifthMostRecentStartDate", "FifthMostRecentEndDate", "SixthMostRecentStartDate", "SixthMostRecentEndDate", 
                     "SeventhMostRecentStartDate", "SeventhMostRecentEndDate"]
        
        date_field_fixes = {}
        
        for key, value in update_data.items():
            if isinstance(value, str):
                value = value.strip()
                if value.upper() == "NULL" or not value:
                    update_data[key] = ""
                # Handle date fields with 'Present' or invalid formats
                elif key in date_fields and not is_valid_sql_date(value):
                    date_field_fixes[key] = {"original": value, "final": ""}
                    logging.warning(f"Invalid date format for {key}: '{value}' - will be sent as empty string to database")
                    update_data[key] = ""
                    
        processing_log["final_output"] = update_data
        
        # Update the database
        logging.info(f"Starting database update for UserID {userid}")
        logging.info(f"Database update will include {len(update_data)} fields")
        
        # List critical fields and their values before database update
        critical_fields = ["LengthinUS", "YearsofExperience", "AvgTenure", "PrimaryTitle", "SecondaryTitle", "Linkedin", "ZipCode"]
        critical_field_values = {}
        field_validations = {}
        
        for field in critical_fields:
            if field in update_data:
                value = update_data[field]
                critical_field_values[field] = value
                # Check for potential problematic values
                if field in ["LengthinUS", "YearsofExperience", "AvgTenure"] and value:
                    try:
                        # Verify these are valid numbers
                        float_val = float(value)
                        logging.info(f"Pre-DB update field value - {field}: '{value}' (valid number: {float_val})")
                        field_validations[field] = {"valid": True, "value": value, "converted": float_val}
                    except ValueError:
                        logging.warning(f"Pre-DB update field value - {field}: '{value}' (NOT A VALID NUMBER!)")
                        field_validations[field] = {"valid": False, "value": value, "error": "Not a valid number"}
                else:
                    logging.info(f"Pre-DB update field value - {field}: '{value}'")
                    field_validations[field] = {"valid": True, "value": value, "type": type(value).__name__}
            else:
                critical_field_values[field] = "MISSING"
                field_validations[field] = {"valid": False, "error": "Missing from update data"}
                logging.warning(f"Pre-DB update field missing - {field}: Not in update data")
        
        # Perform a pre-check on LengthinUS value format
        length_in_us_cleaning = {}
        if "LengthinUS" in update_data and update_data["LengthinUS"]:
            length_in_us = update_data["LengthinUS"]
            length_in_us_cleaning["original"] = length_in_us
            length_in_us_cleaning["type"] = type(length_in_us).__name__
            logging.info(f"LengthinUS value type: {type(length_in_us).__name__}, value: '{length_in_us}'")
            
            # Try to clean it up if it's a string and looks problematic
            if isinstance(length_in_us, str):
                # Remove any non-numeric characters except decimal point
                import re
                cleaned_value = re.sub(r'[^0-9.]', '', length_in_us)
                length_in_us_cleaning["cleaned"] = cleaned_value
                if cleaned_value != length_in_us:
                    logging.warning(f"Cleaned LengthinUS from '{length_in_us}' to '{cleaned_value}'")
                    length_in_us_cleaning["modified"] = True
                    update_data["LengthinUS"] = cleaned_value
                else:
                    length_in_us_cleaning["modified"] = False
            try:
                # Try to convert to float to verify it's a valid number
                float_val = float(update_data["LengthinUS"])
                length_in_us_cleaning["float_conversion"] = {"success": True, "value": float_val}
            except ValueError:
                length_in_us_cleaning["float_conversion"] = {"success": False, "error": "Cannot convert to float"}
        
        # Track database update details
        db_details = {
            "pre_update": {
                "field_validations": field_validations,
                "length_in_us_cleaning": length_in_us_cleaning,
                "date_field_fixes": date_field_fixes,
                "total_fields": len(update_data)
            }
        }
        
        # Run database field diagnostics first
        logging.info(f"Running database field diagnostics before update for UserID {userid}")
        diagnostic_issues = diagnose_database_fields(userid, update_data)
        db_details["pre_update"]["diagnostic_issues"] = diagnostic_issues
        
        db_update_start = time.time()
        logging.info(f"Calling update_candidate_record_with_retry for UserID {userid}")
        update_success = update_candidate_record_with_retry(userid, update_data)
        db_update_time = time.time() - db_update_start
        
        # Record post-update information
        db_details["post_update"] = {
            "success": update_success,
            "time_seconds": db_update_time
        }
        
        if update_success:
            logging.info(f"Database update succeeded in {db_update_time:.2f}s")
        else:
            logging.error(f"Database update FAILED in {db_update_time:.2f}s")
            # Additional diagnostics for failed updates
            logging.error(f"Update data has {len(update_data)} fields with data")
            
            # Log all critical fields that might cause issues
            problem_fields_log = []
            problematic_fields = {}
            for field in critical_fields:
                value = update_data.get(field, "MISSING")
                problem_fields_log.append(f"{field}='{value}'")
                problematic_fields[field] = value
            
            logging.error(f"Fields that might cause issues: {', '.join(problem_fields_log)}")
            db_details["post_update"]["problematic_fields"] = problematic_fields
            
            # Try to diagnose other common issues
            long_fields = {}
            for field, value in update_data.items():
                if value and len(str(value)) > 500:
                    long_fields[field] = len(str(value))
                    logging.warning(f"Field {field} has unusually long value ({len(str(value))} chars)")
            if long_fields:
                db_details["post_update"]["long_fields"] = long_fields
        
        # Add detailed information about field name mapping
        field_mapping_info = {
            "ZipCode": "Zipcode"  # Add other field mappings here if needed
        }
        
        # Store database update information in the processing log
        processing_log["database_update"] = {
            "success": update_success,
            "time": f"{db_update_time:.2f}s",
            "critical_fields": critical_field_values,
            "timestamp": str(datetime.now()),
            "details": db_details,
            "field_mapping": field_mapping_info
        }
        
        # Calculate total processing time
        total_time = time.time() - total_start_time
        processing_log["total_processing_time"] = f"{total_time:.2f}s"
        
        # Log summary stats
        logging.info(f"Total processing time: {total_time:.2f}s")
        logging.info(f"Step 1 time: {step1_time:.2f}s")
        logging.info(f"Step 2 time: {step2_time:.2f}s")
        logging.info(f"Database update time: {db_update_time:.2f}s")
        logging.info(f"Database update success: {update_success}")
        
        # Create detailed field report
        create_detailed_field_report(processing_log)
        
        return {
            'userid': userid,
            'success': update_success,
            'processing_time': total_time,
            'step1_time': step1_time,
            'step2_time': step2_time,
            'token_count': num_tokens_from_string(resume_text) + num_tokens_from_string(step1_text) + num_tokens_from_string(step2_text),
        }
    
    except Exception as e:
        logging.error(f"Error processing UserID {userid}: {str(e)}")
        return {
            'userid': userid,
            'success': False,
            'error': str(e)
        }

def create_detailed_field_report(processing_log):
    """Create a detailed report of each field's processing"""
    userid = processing_log['userid']
    
    # Write detailed log to file
    with open(f"{userid}_detailed_processing.json", "w") as f:
        json.dump(processing_log, f, indent=2)
    
    # Log the highlights
    logging.info(f"===== DETAILED FIELD REPORT FOR USERID {userid} =====")
    
    # Create a table format for the field report
    template = "{:<30} | {:<50} | {:<50}"
    logging.info(template.format("FIELD", "AI OUTPUT", "FINAL DATABASE VALUE"))
    logging.info("-" * 130)
    
    # Important fields to highlight
    key_fields = [
        "PrimaryTitle", "SecondaryTitle", "TertiaryTitle", 
        "FirstName", "LastName", "Email", "Phone1",
        "Address", "City", "State", "ZipCode",
        "MostRecentCompany", "MostRecentStartDate", "MostRecentEndDate", "MostRecentLocation",
        "PrimaryIndustry", "SecondaryIndustry", 
        "PrimarySoftwareLanguage", "SecondarySoftwareLanguage", 
        "Hardware1", "Hardware2",
        "PrimaryCategory", "SecondaryCategory",
        "YearsofExperience", "AvgTenure", "LengthinUS"
    ]
    
    # Track any potentially problematic fields
    problem_fields = []
    
    # For each key field, show the extracted value and final database value
    for field in key_fields:
        # Get the original AI output (from step1 or step2)
        ai_output = "Unknown"
        if field in processing_log["step1"].get("parsed_fields", {}):
            ai_output = processing_log["step1"]["parsed_fields"].get(field, "")
        elif field in processing_log["step2"].get("parsed_fields", {}):
            ai_output = processing_log["step2"]["parsed_fields"].get(field, "")
        
        # Format as NULL for display if empty
        if ai_output == "" or ai_output is None:
            ai_output = "NULL"
            
        # Get the final database value after all processing (including date enhancements)
        db_value = processing_log["final_output"].get(field, "")
        
        # Check for potential issues - AI said NULL but we have a value or vice versa
        if (ai_output == "NULL" and db_value and db_value != "") or (ai_output != "NULL" and not db_value):
            problem_fields.append((field, ai_output, db_value))
        
        # Truncate long values
        if isinstance(ai_output, str) and len(ai_output) > 50:
            ai_output = ai_output[:47] + "..."
        if isinstance(db_value, str) and len(db_value) > 50:
            db_value = db_value[:47] + "..."
            
        logging.info(template.format(field, ai_output, db_value))
    
    # Highlight any fields with discrepancies
    if problem_fields:
        logging.info("")
        logging.info("!!! FIELDS WITH POTENTIAL ISSUES !!!")
        for field, ai_val, db_val in problem_fields:
            logging.info(f"- {field}: AI output was '{ai_val}' but final value is '{db_val}'")
            # For experience fields, explain calculation or why AI value was kept
            if field in ["YearsofExperience", "AvgTenure", "LengthinUS"]:
                if ai_val == "NULL" and db_val:
                    logging.info(f"  Note: {field} was calculated from date information because AI returned NULL")
                elif ai_val != "NULL" and ai_val == db_val:
                    logging.info(f"  Note: Using AI's value for {field} ({ai_val}) as specified by requirements")
    
    # Add database operation summary
    logging.info("")
    logging.info("-" * 130)
    logging.info("DATABASE OPERATION SUMMARY")
    logging.info("-" * 130)
    
    # Get database update information
    db_update = processing_log.get("database_update", {})
    update_success = db_update.get("success", False)
    update_time = db_update.get("time", "unknown")
    
    # Log field name mappings if present
    if "field_mapping" in db_update:
        logging.info("Field name mappings for database:")
        for code_field, db_field in db_update["field_mapping"].items():
            logging.info(f"  - Code field '{code_field}' mapped to database field '{db_field}'")
    
    # Add information about date field fixes
    if "details" in db_update and "pre_update" in db_update["details"] and "date_field_fixes" in db_update["details"]["pre_update"]:
        date_fixes = db_update["details"]["pre_update"]["date_field_fixes"]
        if date_fixes:
            logging.info("Date field formats fixed for database compatibility:")
            for field, fix_info in date_fixes.items():
                logging.info(f"  - {field}: '{fix_info['original']}' -> '{fix_info['final']}'")
            logging.info("  (Note: 'Present' and other invalid date formats can't be stored in SQL Server date columns)")
    
    
    
    if update_success:
        logging.info(f"[SUCCESS] Database update SUCCESSFUL (time: {update_time})")
    else:
        logging.info(f"[FAILED] Database update FAILED (time: {update_time})")
    
    # Display critical fields that were sent to the database
    if "critical_fields" in db_update:
        logging.info("")
        logging.info("Critical fields sent to database:")
        for field, value in db_update["critical_fields"].items():
            logging.info(f"  - {field}: '{value}'")
    
    # If there was date processing, show what happened to important date-related fields
    if "date_processing" in processing_log:
        date_input = processing_log["date_processing"].get("input", {})
        date_output = processing_log["date_processing"].get("output", {})
        
        important_date_fields = ["LengthinUS", "YearsofExperience", "AvgTenure"]
        
        logging.info("")
        logging.info("Date processing changes:")
        for field in important_date_fields:
            input_val = date_input.get(field, "MISSING")
            output_val = date_output.get(field, "MISSING")
            
            if input_val != output_val:
                logging.info(f"  - {field}: '{input_val}' -> '{output_val}'")
                
                # For LengthinUS specifically, highlight if it was enhanced but the DB update failed
                if field == "LengthinUS" and input_val == "NULL" and output_val != "NULL" and not update_success:
                    logging.warning(f"  [WARNING] {field} was successfully calculated as '{output_val}' but database update failed")
    
    logging.info("=" * 130)
    logging.info(f"Detailed log saved to {userid}_detailed_processing.json")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python process_single_user.py <userid>")
        sys.exit(1)
    
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        force=True,
        handlers=[
            logging.FileHandler(f"userid_processing.log"),
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    # Get the user ID from command line
    userid = sys.argv[1]
    
    logging.info(f"Fetching resume for userid {userid}")
    
    # Fetch the resume for this user ID
    resume_data = get_resume_by_userid(userid)
    
    if not resume_data:
        logging.error(f"No resume found for userid {userid}")
        sys.exit(1)
    
    # Process the resume with detailed logging
    userid, resume_text = resume_data
    result = process_with_detailed_logging(userid, resume_text)
    
    # Print the final result
    try:
        if result.get('success', False):
            logging.info(f"Successfully processed resume for userid {userid}")
            logging.info(f"Check {userid}_detailed_processing.json for complete details")
        else:
            error_message = result.get('error', 'No error details available')
            logging.warning(f"Resume processing for userid {userid} was not fully successful")
            logging.warning(f"Reason: {error_message}")
            
            # Add more specific information about the database operation
            if 'processing_time' in result:
                logging.info(f"Processing completed in {result['processing_time']:.2f}s, but database update failed")
                
                # Try to read the detailed processing file to extract database error information
                try:
                    with open(f"{userid}_detailed_processing.json", "r") as f:
                        details = json.load(f)
                        db_update = details.get("database_update", {})
                        
                        # Display critical field values that were sent to the database
                        if "critical_fields" in db_update:
                            logging.info("Critical fields sent to database:")
                            for field, value in db_update["critical_fields"].items():
                                if field in ["LengthinUS", "YearsofExperience", "AvgTenure"]:
                                    logging.info(f"  - {field}: '{value}'")
                                    
                        # Display details about LengthinUS field specifically
                        if "details" in db_update and "pre_update" in db_update["details"]:
                            pre_update = db_update["details"]["pre_update"]
                            if "length_in_us_cleaning" in pre_update:
                                cleaning = pre_update["length_in_us_cleaning"]
                                if cleaning:
                                    logging.info(f"LengthinUS processing details:")
                                    for k, v in cleaning.items():
                                        logging.info(f"  - {k}: {v}")
                except Exception as json_err:
                    logging.warning(f"Could not extract detailed DB info: {str(json_err)}")
            
            logging.info(f"The field report was generated successfully. Check {userid}_detailed_processing.json for details.")
    except Exception as e:
        logging.warning(f"Error displaying final results: {str(e)}")
        logging.info(f"Check {userid}_detailed_processing.json for processing details.")
    
    logging.info("Processing complete")
    logging.info("Database error investigation tips:")
    logging.info("1. Check LengthinUS and other numeric field formats")
    logging.info("2. Verify SQL Server field type compatibility")
    logging.info("3. Look for unusually long field values")
    logging.info("4. Check the database log for more specific error messages")