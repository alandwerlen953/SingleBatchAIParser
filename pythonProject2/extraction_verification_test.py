"""
Extraction Verification Test Script

This script tests the extraction of all fields for a resume, allowing you to
verify the accuracy and completeness of all extracted fields.
"""

import logging
import time
import sys
import os
import re
import json
from typing import Dict, Any, List, Tuple, Optional
from datetime import datetime

from resume_utils import (
    DEFAULT_MODEL, MAX_TOKENS, 
    num_tokens_from_string, apply_token_truncation,
    update_candidate_record_with_retry,
    openai
)
from two_step_prompts_taxonomy import create_step1_prompt, create_step2_prompt
from two_step_processor_taxonomy import (
    parse_step1_response, 
    parse_step2_response, 
    process_resume_with_enhanced_dates
)
import pyodbc

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('extraction_verification.log')
    ]
)

# Configuration
MODEL = DEFAULT_MODEL
TEMPERATURE = 0.5  # Moderate temperature for balanced output

def get_single_resume(userid: str) -> Optional[Tuple[str, str]]:
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

def get_current_db_values(userid: str) -> Dict[str, Any]:
    """Get current values from the database for comparison"""
    server_ip = '172.19.115.25'
    database = 'BH_Mirror'
    username = 'silver'
    password = 'ltechmatlen'
    
    try:
        # Connect to the database
        connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server_ip};DATABASE={database};UID={username};PWD={password}'
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()
        
        # Get column names
        cursor.execute("SELECT * FROM aicandidate WHERE 1=0")
        columns = [column[0] for column in cursor.description]
        
        # Query to get current values
        query = f"""
            SELECT * FROM aicandidate 
            WHERE userid = ?
        """
        
        cursor.execute(query, userid)
        row = cursor.fetchone()
        
        if row:
            # Create a dictionary of column names and values
            result = {}
            for i, column in enumerate(columns):
                result[column] = row[i]
            
            logging.info(f"Retrieved current DB values for UserID {userid} ({len(result)} fields)")
            return result
        else:
            logging.warning(f"No record found for UserID {userid}")
            return {}
            
    except Exception as e:
        logging.error(f"Error retrieving DB values: {str(e)}")
        return {}
    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn:
            conn.close()

def run_extraction_test(userid: str = None, do_print: bool = True):
    """Test extraction of all fields for a resume"""
    try:
        # Use a default userid if none provided
        if not userid or userid == "None":
            userid = "226507"  # Using Michael Voight's userid as default
            logging.info(f"No userid provided, using default: {userid}")
        
        # Get current values from DB for comparison
        current_db_values = get_current_db_values(userid)
        
        logging.info(f"Starting extraction verification for UserID: {userid}")
        
        # Get resume data
        resume_data = get_single_resume(userid)
        if not resume_data:
            logging.error(f"No resume found for UserID: {userid}")
            return
        
        resume_text = resume_data[1]
        
        # Process step 1
        total_start_time = time.time()
        step1_start_time = time.time()
        
        step1_messages = create_step1_prompt(resume_text, userid=userid)
        step1_messages = apply_token_truncation(step1_messages)
        
        logging.info(f"Sending Step 1 request")
        step1_response = openai.chat.completions.create(
            model=MODEL,
            messages=step1_messages,
            temperature=TEMPERATURE,
            max_tokens=MAX_TOKENS
        )
        
        step1_time = time.time() - step1_start_time
        logging.info(f"Step 1 completed in {step1_time:.2f}s")
        
        # Parse step 1 response
        step1_text = step1_response.choices[0].message.content
        step1_results = parse_step1_response(step1_text)
        
        logging.info(f"Step 1 parsed {len(step1_results)} fields")
        
        # Analyze step 1 results
        step1_fields = {
            "Personal Info": ["FirstName", "MiddleName", "LastName", "Address", "City", "State", 
                         "Phone1", "Phone2", "Email", "Email2", "Linkedin", "Bachelors", "Masters", 
                         "Certifications"],
            "Job Titles": ["PrimaryTitle", "SecondaryTitle", "TertiaryTitle"],
            "Companies": ["MostRecentCompany", "SecondMostRecentCompany", "ThirdMostRecentCompany", 
                      "FourthMostRecentCompany", "FifthMostRecentCompany", "SixthMostRecentCompany", 
                      "SeventhMostRecentCompany"],
            "Start Dates": ["MostRecentStartDate", "SecondMostRecentStartDate", "ThirdMostRecentStartDate", 
                        "FourthMostRecentStartDate", "FifthMostRecentStartDate", "SixthMostRecentStartDate", 
                        "SeventhMostRecentStartDate"],
            "End Dates": ["MostRecentEndDate", "SecondMostRecentEndDate", "ThirdMostRecentEndDate", 
                      "FourthMostRecentEndDate", "FifthMostRecentEndDate", "SixthMostRecentEndDate", 
                      "SeventhMostRecentEndDate"],
            "Locations": ["MostRecentLocation", "SecondMostRecentLocation", "ThirdMostRecentLocation", 
                      "FourthMostRecentLocation", "FifthMostRecentLocation", "SixthMostRecentLocation", 
                      "SeventhMostRecentLocation"],
            "Industries": ["PrimaryIndustry", "SecondaryIndustry"],
            "Skills": ["Top10Skills"]
        }
        
        # Analyze step 1 results by category
        step1_analysis = {}
        
        for category, fields in step1_fields.items():
            present = 0
            missing = []
            
            for field in fields:
                if field in step1_results and step1_results[field] != "NULL" and step1_results[field]:
                    present += 1
                else:
                    missing.append(field)
            
            step1_analysis[category] = {
                "total": len(fields),
                "present": present,
                "missing": missing,
                "percentage": (present / len(fields)) * 100 if fields else 0
            }
        
        # Process step 2
        step2_start_time = time.time()
        
        step2_messages = create_step2_prompt(resume_text, step1_results, userid=userid)
        step2_messages = apply_token_truncation(step2_messages)
        
        logging.info(f"Sending Step 2 request")
        step2_response = openai.chat.completions.create(
            model=MODEL,
            messages=step2_messages,
            temperature=TEMPERATURE,
            max_tokens=MAX_TOKENS
        )
        
        step2_time = time.time() - step2_start_time
        logging.info(f"Step 2 completed in {step2_time:.2f}s")
        
        # Parse step 2 response
        step2_text = step2_response.choices[0].message.content
        step2_results = parse_step2_response(step2_text)
        
        logging.info(f"Step 2 parsed {len(step2_results)} fields")
        
        # Analyze step 2 results
        step2_fields = {
            "Languages": ["PrimarySoftwareLanguage", "SecondarySoftwareLanguage", "TertiarySoftwareLanguage"],
            "Software": ["SoftwareApp1", "SoftwareApp2", "SoftwareApp3", "SoftwareApp4", "SoftwareApp5"],
            "Hardware": ["Hardware1", "Hardware2", "Hardware3", "Hardware4", "Hardware5"],
            "Categories": ["PrimaryCategory", "SecondaryCategory"],
            "Project Info": ["ProjectTypes", "Specialty", "Summary"],
            "Experience Metrics": ["LengthinUS", "YearsofExperience", "AvgTenure"]
        }
        
        # Analyze step 2 results by category
        step2_analysis = {}
        
        for category, fields in step2_fields.items():
            present = 0
            missing = []
            
            for field in fields:
                if field in step2_results and step2_results[field] != "NULL" and step2_results[field]:
                    present += 1
                else:
                    missing.append(field)
            
            step2_analysis[category] = {
                "total": len(fields),
                "present": present,
                "missing": missing,
                "percentage": (present / len(fields)) * 100 if fields else 0
            }
        
        # Combine results from both steps
        combined_results = {**step1_results, **step2_results}
        
        # Run date enhancement
        enhanced_results = process_resume_with_enhanced_dates(userid, combined_results)
        
        # Extract skills for database format
        skills_list = enhanced_results.get("Top10Skills", "").split(", ") if enhanced_results.get("Top10Skills") and enhanced_results.get("Top10Skills") != "NULL" else []
        skills_list.extend([""] * (10 - len(skills_list)))  # Ensure we have 10 skills
        skill_fields = {}
        for i, skill in enumerate(skills_list[:10], 1):
            skill_fields[f"Skill{i}"] = skill
        
        # Create the final data structure with all fields
        final_data = {
            "PrimaryTitle": enhanced_results.get("PrimaryTitle", ""),
            "SecondaryTitle": enhanced_results.get("SecondaryTitle", ""),
            "TertiaryTitle": enhanced_results.get("TertiaryTitle", ""),
            "Address": enhanced_results.get("Address", ""),
            "City": enhanced_results.get("City", ""),
            "State": enhanced_results.get("State", ""),
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
            **skill_fields,
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
            "LengthinUS": enhanced_results.get("LengthinUS", ""),
            "YearsofExperience": enhanced_results.get("YearsofExperience", ""),
            "AvgTenure": enhanced_results.get("AvgTenure", "")
        }
        
        # Compare with current DB values
        changes = {}
        for key in final_data:
            if key in current_db_values:
                db_value = current_db_values[key] or ""
                new_value = final_data[key] or ""
                
                # Convert to string for comparison
                if not isinstance(db_value, str):
                    db_value = str(db_value)
                if not isinstance(new_value, str):
                    new_value = str(new_value)
                
                # Compare values
                if db_value != new_value:
                    changes[key] = {
                        "old": db_value,
                        "new": new_value
                    }
        
        # Calculate overall field coverage
        all_fields = []
        for fields in step1_fields.values():
            all_fields.extend(fields)
        for fields in step2_fields.values():
            all_fields.extend(fields)
            
        covered_fields = sum(1 for field in all_fields if field in enhanced_results and enhanced_results[field] != "NULL" and enhanced_results[field])
        coverage_percentage = (covered_fields / len(all_fields)) * 100
        
        # Calculate total processing time
        total_time = time.time() - total_start_time
        
        # Create result summary
        result = {
            'userid': userid,
            'step1_analysis': step1_analysis,
            'step2_analysis': step2_analysis,
            'overall_coverage': {
                'total_fields': len(all_fields),
                'covered_fields': covered_fields,
                'coverage_percentage': coverage_percentage
            },
            'hardware_fields': {
                'Hardware1': enhanced_results.get('Hardware1', ''),
                'Hardware2': enhanced_results.get('Hardware2', ''),
                'Hardware3': enhanced_results.get('Hardware3', ''),
                'Hardware4': enhanced_results.get('Hardware4', ''),
                'Hardware5': enhanced_results.get('Hardware5', '')
            },
            'changes_from_db': changes,
            'processing_time': {
                'total': total_time,
                'step1': step1_time,
                'step2': step2_time
            }
        }
        
        # Print report if requested
        if do_print:
            print("\n=== EXTRACTION VERIFICATION REPORT ===")
            print(f"UserID: {userid}")
            print(f"Processing time: {total_time:.2f}s (Step 1: {step1_time:.2f}s, Step 2: {step2_time:.2f}s)")
            print()
            
            print("STEP 1 ANALYSIS:")
            for category, analysis in step1_analysis.items():
                print(f"- {category}: {analysis['present']}/{analysis['total']} ({analysis['percentage']:.1f}%)")
                if analysis['missing']:
                    print(f"  Missing: {', '.join(analysis['missing'])}")
            print()
            
            print("STEP 2 ANALYSIS:")
            for category, analysis in step2_analysis.items():
                print(f"- {category}: {analysis['present']}/{analysis['total']} ({analysis['percentage']:.1f}%)")
                if analysis['missing']:
                    print(f"  Missing: {', '.join(analysis['missing'])}")
            print()
            
            print(f"OVERALL COVERAGE: {covered_fields}/{len(all_fields)} fields ({coverage_percentage:.1f}%)")
            print()
            
            print("HARDWARE FIELDS:")
            for key, value in result['hardware_fields'].items():
                print(f"- {key}: {value or 'NULL'}")
            print()
            
            if changes:
                print(f"CHANGES FROM DATABASE ({len(changes)} fields):")
                for key, values in changes.items():
                    print(f"- {key}: '{values['old']}' -> '{values['new']}'")
            else:
                print("NO CHANGES FROM DATABASE")
            print()
            
            print("See extraction_verification.log for details.")
        
        return result
    
    except Exception as e:
        logging.error(f"Error in extraction verification test: {str(e)}", exc_info=True)
        return None

def run_test():
    """Run the extraction verification test script"""
    print("=== EXTRACTION VERIFICATION TEST ===")
    
    # If a user ID is provided as command line argument, use it
    userid = sys.argv[1] if len(sys.argv) > 1 else "226507"
    
    result = run_extraction_test(userid)
    
    return result

if __name__ == "__main__":
    run_test()