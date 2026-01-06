"""
Two-step resume processor with skills taxonomy integration
"""

import os
import logging
import time
import concurrent.futures
from datetime import datetime

# Check if we're in quiet mode and configure logging appropriately
if os.environ.get('QUIET_MODE', '').lower() in ('1', 'true', 'yes'):
    logging.getLogger().setLevel(logging.ERROR)

from resume_utils import (
    DEFAULT_MODEL, MAX_TOKENS, DEFAULT_TEMPERATURE,
    num_tokens_from_string, apply_token_truncation, 
    get_resume_batch, update_candidate_record_with_retry,
    openai
)
# from two_step_prompts_taxonomy import create_step1_prompt, create_step2_prompt  # Not using two-step approach
from date_processor import process_resume_with_enhanced_dates
from error_logger import get_error_logger

# Application Configuration
BATCH_SIZE = 50        # Number of resumes to process in a single batch - PRIMARY SETTING
MAX_WORKERS = 50       # Maximum number of concurrent API requests
MODEL = DEFAULT_MODEL  # Using the default model from resume_utils
USE_BATCH_API = True   # Use the new OpenAI batch API for better efficiency

# Function copied from removed file to preserve functionality
def extract_fields_directly(response_text):
    """Extract various fields directly using regex patterns"""
    import re
    
    # Dictionary to store extracted fields
    extracted = {}
    
    # === JOB TITLE PATTERNS ===
    # Patterns to look for job titles - different possible phrasings
    primary_patterns = [
        r"Best job title that fits? their primary experience:\s*(.+)",
        r"Best job title that fit their primary experience:\s*(.+)",
        r"Best job title that fits their primary experience:\s*(.+)",
        r"Primary Job Title:\s*(.+)"
    ]
    
    secondary_patterns = [
        r"Best secondary job title that fits their secondary experience:\s*(.+)",
        r"Best job title that fits their secondary experience:\s*(.+)",
        r"Secondary Job Title:\s*(.+)"
    ]
    
    tertiary_patterns = [
        r"Best tertiary job title that fits their tertiary experience:\s*(.+)",
        r"Best job title that fits their tertiary experience:\s*(.+)",
        r"Tertiary Job Title:\s*(.+)"
    ]
    
    # === COMPANY PATTERNS ===
    # Patterns for company information
    company_patterns = {
        "MostRecentCompany": [
            r"Most Recent Company Worked for:\s*(.+)",
            r"Most Recent Company:\s*(.+)"
        ],
        "SecondMostRecentCompany": [
            r"Second Most Recent Company Worked for:\s*(.+)",
            r"Second Most Recent Company:\s*(.+)" 
        ],
        "ThirdMostRecentCompany": [
            r"Third Most Recent Company Worked for:\s*(.+)",
            r"Third Most Recent Company:\s*(.+)"
        ],
        "FourthMostRecentCompany": [
            r"Fourth Most Recent Company Worked for:\s*(.+)",
            r"Fourth Most Recent Company:\s*(.+)"
        ],
        "FifthMostRecentCompany": [
            r"Fifth Most Recent Company Worked for:\s*(.+)",
            r"Fifth Most Recent Company:\s*(.+)"
        ],
        "SixthMostRecentCompany": [
            r"Sixth Most Recent Company Worked for:\s*(.+)",
            r"Sixth Most Recent Company:\s*(.+)"
        ],
        "SeventhMostRecentCompany": [
            r"Seventh Most Recent Company Worked for:\s*(.+)",
            r"Seventh Most Recent Company:\s*(.+)"
        ]
    }
    
    # === DATE PATTERNS ===
    # Patterns for dates
    date_patterns = {
        "MostRecentStartDate": [
            r"Most Recent Start Date \(YYYY-MM-DD\):\s*(.+)",
            r"Most Recent Start Date:\s*(.+)"
        ],
        "MostRecentEndDate": [
            r"Most Recent End Date \(YYYY-MM-DD\):\s*(.+)",
            r"Most Recent End Date:\s*(.+)"
        ],
        "SecondMostRecentStartDate": [
            r"Second Most Recent Start Date \(YYYY-MM-DD\):\s*(.+)",
            r"Second Most Recent Start Date:\s*(.+)"
        ],
        "SecondMostRecentEndDate": [
            r"Second Most Recent End Date \(YYYY-MM-DD\):\s*(.+)",
            r"Second Most Recent End Date:\s*(.+)"
        ],
        "ThirdMostRecentStartDate": [
            r"Third Most Recent Start Date \(YYYY-MM-DD\):\s*(.+)",
            r"Third Most Recent Start Date:\s*(.+)"
        ],
        "ThirdMostRecentEndDate": [
            r"Third Most Recent End Date \(YYYY-MM-DD\):\s*(.+)",
            r"Third Most Recent End Date:\s*(.+)"
        ],
        "FourthMostRecentStartDate": [
            r"Fourth Most Recent Start Date \(YYYY-MM-DD\):\s*(.+)",
            r"Fourth Most Recent Start Date:\s*(.+)"
        ],
        "FourthMostRecentEndDate": [
            r"Fourth Most Recent End Date \(YYYY-MM-DD\):\s*(.+)",
            r"Fourth Most Recent End Date:\s*(.+)"
        ],
        "FifthMostRecentStartDate": [
            r"Fifth Most Recent Start Date \(YYYY-MM-DD\):\s*(.+)",
            r"Fifth Most Recent Start Date:\s*(.+)"
        ],
        "FifthMostRecentEndDate": [
            r"Fifth Most Recent End Date \(YYYY-MM-DD\):\s*(.+)",
            r"Fifth Most Recent End Date:\s*(.+)"
        ],
        "SixthMostRecentStartDate": [
            r"Sixth Most Recent Start Date \(YYYY-MM-DD\):\s*(.+)",
            r"Sixth Most Recent Start Date:\s*(.+)"
        ],
        "SixthMostRecentEndDate": [
            r"Sixth Most Recent End Date \(YYYY-MM-DD\):\s*(.+)",
            r"Sixth Most Recent End Date:\s*(.+)"
        ],
        "SeventhMostRecentStartDate": [
            r"Seventh Most Recent Start Date \(YYYY-MM-DD\):\s*(.+)",
            r"Seventh Most Recent Start Date:\s*(.+)"
        ],
        "SeventhMostRecentEndDate": [
            r"Seventh Most Recent End Date \(YYYY-MM-DD\):\s*(.+)",
            r"Seventh Most Recent End Date:\s*(.+)"
        ]
    }
    
    # === LOCATION PATTERNS ===
    # Patterns for locations
    location_patterns = {
        "MostRecentLocation": [
            r"Most Recent Job Location:\s*(.+)",
            r"Most Recent Location:\s*(.+)"
        ],
        "SecondMostRecentLocation": [
            r"Second Most Recent Job Location:\s*(.+)",
            r"Second Most Recent Location:\s*(.+)"
        ],
        "ThirdMostRecentLocation": [
            r"Third Most Recent Job Location:\s*(.+)",
            r"Third Most Recent Location:\s*(.+)"
        ],
        "FourthMostRecentLocation": [
            r"Fourth Most Recent Job Location:\s*(.+)",
            r"Fourth Most Recent Location:\s*(.+)"
        ],
        "FifthMostRecentLocation": [
            r"Fifth Most Recent Job Location:\s*(.+)",
            r"Fifth Most Recent Location:\s*(.+)"
        ],
        "SixthMostRecentLocation": [
            r"Sixth Most Recent Job Location:\s*(.+)",
            r"Sixth Most Recent Location:\s*(.+)"
        ],
        "SeventhMostRecentLocation": [
            r"Seventh Most Recent Job Location:\s*(.+)",
            r"Seventh Most Recent Location:\s*(.+)"
        ]
    }
    
    # === INDUSTRY PATTERNS ===
    # Patterns for industry
    industry_patterns = {
        "PrimaryIndustry": [
            r"Based on all 7 of their most recent companies above, what is the Primary industry they work in:\s*(.+)",
            r"Primary Industry:\s*(.+)",
            r"What is the Primary industry they work in:\s*(.+)",
            r"Primary industry they work in:\s*(.+)",
            r"Primary industry:\s*(.+)"
        ],
        "SecondaryIndustry": [
            r"Based on all 7 of their most recent companies above, what is the Secondary industry they work in:\s*(.+)",
            r"Secondary Industry:\s*(.+)",
            r"What is the Secondary industry they work in:\s*(.+)",
            r"Secondary industry they work in:\s*(.+)",
            r"Secondary industry:\s*(.+)",
            r"Second most common industry:\s*(.+)",
            r"Second industry:\s*(.+)"
        ]
    }
    
    # === PERSONAL INFO PATTERNS ===
    # Patterns for personal information
    personal_info_patterns = {
        "Address": [
            r"Their street address:\s*(.+)",
            r"Street Address:\s*(.+)",
            r"Address:\s*(.+)"  # Add pattern for single_step_processor format
        ],
        "City": [
            r"Their City:\s*(.+)",
            r"City:\s*(.+)"
        ],
        "State": [
            r"Their State:\s*(.+)",
            r"State:\s*(.+)"
        ],
        "ZipCode": [
            r"Their Zip Code:\s*(.+)",
            r"Zip Code:\s*(.+)",
            r"Their Zip:\s*(.+)",
            r"Zip:\s*(.+)",
            r"Zipcode:\s*(.+)"  # Add pattern for single_step_processor format
        ],
        "Phone1": [
            r"Their Phone Number:\s*(.+)",
            r"Phone Number 1:\s*(.+)",
            r"Their Phone Number 1:\s*(.+)",
            r"Phone1:\s*(.+)"  # Add pattern for single_step_processor format
        ],
        "Phone2": [
            r"Their Second Phone Number:\s*(.+)",
            r"Phone Number 2:\s*(.+)",
            r"Their Phone Number 2:\s*(.+)",
            r"Phone2:\s*(.+)"  # Add pattern for single_step_processor format
        ],
        "Email": [
            r"Their Email:\s*(.+)",
            r"Email 1:\s*(.+)",
            r"Their Email 1:\s*(.+)",
            r"Email:\s*(.+)"  # Add pattern for single_step_processor format
        ],
        "Email2": [
            r"Their Second Email:\s*(.+)",
            r"Email 2:\s*(.+)",
            r"Their Email 2:\s*(.+)",
            r"Email2:\s*(.+)"  # Add pattern for single_step_processor format
        ],
        "FirstName": [
            r"Their First Name:\s*(.+)",
            r"First Name:\s*(.+)",
            r"- First Name:\s*(.+)"  # Add pattern with hyphen prefix for single_step_processor
        ],
        "MiddleName": [
            r"Their Middle Name:\s*(.+)",
            r"Middle Name:\s*(.+)"
        ],
        "LastName": [
            r"Their Last Name:\s*(.+)",
            r"Last Name:\s*(.+)"
        ],
        "Linkedin": [
            r"Their Linkedin URL:\s*(.+)",
            r"LinkedIn URL:\s*(.+)",
            r"LinkedIn:\s*(.+)"  # Add pattern for single_step_processor format
        ],
        "Bachelors": [
            r"Their Bachelor's Degree:\s*(.+)",
            r"Bachelor's Degree:\s*(.+)",
            r"Bachelors:\s*(.+)"  # Add pattern for single_step_processor format
        ],
        "Masters": [
            r"Their Master's Degree:\s*(.+)",
            r"Master's Degree:\s*(.+)",
            r"Masters:\s*(.+)"  # Add pattern for single_step_processor format
        ],
        "Certifications": [
            r"Their Certifications Listed:\s*(.+)",
            r"Certifications:\s*(.+)",
            r"Certifications Listed:\s*(.+)"
        ]
    }
    
    # === EXTRACT JOB TITLES ===
    # Try to find primary title
    for pattern in primary_patterns:
        match = re.search(pattern, response_text)
        if match:
            extracted["PrimaryTitle"] = match.group(1).strip()
            logging.info(f"Direct extract: Found PrimaryTitle '{extracted['PrimaryTitle']}' using pattern '{pattern}'")
            break
    
    # Try to find secondary title
    for pattern in secondary_patterns:
        match = re.search(pattern, response_text)
        if match:
            extracted["SecondaryTitle"] = match.group(1).strip()
            logging.info(f"Direct extract: Found SecondaryTitle '{extracted['SecondaryTitle']}' using pattern '{pattern}'")
            break
    
    # Try to find tertiary title
    for pattern in tertiary_patterns:
        match = re.search(pattern, response_text)
        if match:
            extracted["TertiaryTitle"] = match.group(1).strip()
            logging.info(f"Direct extract: Found TertiaryTitle '{extracted['TertiaryTitle']}' using pattern '{pattern}'")
            break
    
    # === EXTRACT COMPANIES ===
    # Extract company information
    for field, patterns in company_patterns.items():
        for pattern in patterns:
            match = re.search(pattern, response_text)
            if match:
                value = match.group(1).strip()
                if value.upper() != "NULL" and value != "":
                    extracted[field] = value
                    logging.info(f"Direct extract: Found {field} '{value}' using pattern '{pattern}'")
                break
    
    # === EXTRACT DATES ===
    # Extract date information
    for field, patterns in date_patterns.items():
        for pattern in patterns:
            match = re.search(pattern, response_text)
            if match:
                value = match.group(1).strip()
                if value.upper() != "NULL" and value != "":
                    extracted[field] = value
                    logging.info(f"Direct extract: Found {field} '{value}'")
                break
    
    # === EXTRACT LOCATIONS ===
    # Extract location information
    for field, patterns in location_patterns.items():
        for pattern in patterns:
            match = re.search(pattern, response_text)
            if match:
                value = match.group(1).strip()
                if value.upper() != "NULL" and value != "":
                    extracted[field] = value
                    logging.info(f"Direct extract: Found {field} '{value}'")
                break
    
    # === EXTRACT INDUSTRY ===
    # Extract industry information
    for field, patterns in industry_patterns.items():
        for pattern in patterns:
            match = re.search(pattern, response_text)
            if match:
                value = match.group(1).strip()
                if value.upper() != "NULL" and value != "":
                    extracted[field] = value
                    logging.info(f"Direct extract: Found {field} '{value}'")
                break
                
    # === EXTRACT PERSONAL INFO ===
    # Extract personal information
    for field, patterns in personal_info_patterns.items():
        for pattern in patterns:
            match = re.search(pattern, response_text)
            if match:
                value = match.group(1).strip()
                if value.upper() != "NULL" and value != "":
                    extracted[field] = value
                    logging.info(f"Direct extract: Found {field} '{value}'")
                break
    
    return extracted

def parse_step1_response(response_text):
    """Parse the response from step 1"""
    # Log the raw response for debugging
    logging.info(f"Step 1 raw response first 500 chars: {response_text[:500]}")
    
    # Try direct extraction of all fields first
    direct_fields = extract_fields_directly(response_text)
    
    result = {}
    sections = {
        "PERSONAL INFORMATION": [
            "First Name", "Middle Name", "Last Name", "Street Address", "City", 
            "State", "Phone Number 1", "Phone Number 2", "Email 1", "Email 2", 
            "LinkedIn URL", "Bachelor's Degree", "Master's Degree", "Certifications"
        ],
        "JOB TITLES": [
            "Primary Job Title", "Secondary Job Title", "Tertiary Job Title"
        ],
        "WORK HISTORY": [
            "Most Recent Company", "Most Recent Start Date", "Most Recent End Date", "Most Recent Job Location",
            "Second Most Recent Company", "Second Most Recent Start Date", "Second Most Recent End Date", "Second Most Recent Job Location",
            "Third Most Recent Company", "Third Most Recent Start Date", "Third Most Recent End Date", "Third Most Recent Job Location",
            "Fourth Most Recent Company", "Fourth Most Recent Start Date", "Fourth Most Recent End Date", "Fourth Most Recent Job Location",
            "Fifth Most Recent Company", "Fifth Most Recent Start Date", "Fifth Most Recent End Date", "Fifth Most Recent Job Location",
            "Sixth Most Recent Company", "Sixth Most Recent Start Date", "Sixth Most Recent End Date", "Sixth Most Recent Job Location",
            "Seventh Most Recent Company", "Seventh Most Recent Start Date", "Seventh Most Recent End Date", "Seventh Most Recent Job Location"
        ],
        "INDUSTRY": [
            "Primary Industry", "Secondary Industry"
        ],
        "SKILLS": [
            "Top 10 Technical Skills"
        ]
    }
    
    lines = response_text.strip().split('\n')
    current_section = None
    
    for line in lines:
        line = line.strip()
        if not line:
            continue
            
        # Check if this is a section header
        if line.endswith(':') and line.isupper():
            current_section = line[:-1]
            continue
            
        # Parse key-value pair
        if ':' in line:
            parts = line.split(':', 1)
            key = parts[0].strip('- \t')
            value = parts[1].strip()
            
            # Normalize NULL values
            if value.upper() == 'NULL' or value == '':
                value = 'NULL'
                
            result[key] = value
    
    # Map to standard field names
    field_mapping = {
        "First Name": "FirstName",
        "Middle Name": "MiddleName",
        "Last Name": "LastName",
        "Street Address": "Address",
        "City": "City",
        "State": "State", 
        "Phone Number 1": "Phone1",
        "Phone Number 2": "Phone2",
        "Email 1": "Email",
        "Email 2": "Email2",
        "LinkedIn URL": "Linkedin",
        "Bachelor's Degree": "Bachelors",
        "Master's Degree": "Masters",
        "Certifications": "Certifications",
        "Primary Job Title": "PrimaryTitle",
        "Secondary Job Title": "SecondaryTitle",
        "Tertiary Job Title": "TertiaryTitle",
        
        # Add alternative phrasings that appear in the actual API response
        "Best job title that fit their primary experience": "PrimaryTitle",
        "Best secondary job title that fits their secondary experience": "SecondaryTitle", 
        "Best tertiary job title that fits their tertiary experience": "TertiaryTitle",
        
        # Additional variations that might appear
        "Best job title that fits their primary experience": "PrimaryTitle",
        "Best job title fitting their primary experience": "PrimaryTitle",
        "Most Recent Company": "MostRecentCompany",
        "Most Recent Start Date": "MostRecentStartDate",
        "Most Recent End Date": "MostRecentEndDate",
        "Most Recent Job Location": "MostRecentLocation",
        "Second Most Recent Company": "SecondMostRecentCompany",
        "Second Most Recent Start Date": "SecondMostRecentStartDate",
        "Second Most Recent End Date": "SecondMostRecentEndDate",
        "Second Most Recent Job Location": "SecondMostRecentLocation",
        "Third Most Recent Company": "ThirdMostRecentCompany",
        "Third Most Recent Start Date": "ThirdMostRecentStartDate",
        "Third Most Recent End Date": "ThirdMostRecentEndDate",
        "Third Most Recent Job Location": "ThirdMostRecentLocation",
        "Fourth Most Recent Company": "FourthMostRecentCompany",
        "Fourth Most Recent Start Date": "FourthMostRecentStartDate",
        "Fourth Most Recent End Date": "FourthMostRecentEndDate",
        "Fourth Most Recent Job Location": "FourthMostRecentLocation",
        "Fifth Most Recent Company": "FifthMostRecentCompany",
        "Fifth Most Recent Start Date": "FifthMostRecentStartDate",
        "Fifth Most Recent End Date": "FifthMostRecentEndDate",
        "Fifth Most Recent Job Location": "FifthMostRecentLocation",
        "Sixth Most Recent Company": "SixthMostRecentCompany",
        "Sixth Most Recent Start Date": "SixthMostRecentStartDate",
        "Sixth Most Recent End Date": "SixthMostRecentEndDate",
        "Sixth Most Recent Job Location": "SixthMostRecentLocation",
        "Seventh Most Recent Company": "SeventhMostRecentCompany",
        "Seventh Most Recent Start Date": "SeventhMostRecentStartDate",
        "Seventh Most Recent End Date": "SeventhMostRecentEndDate",
        "Seventh Most Recent Job Location": "SeventhMostRecentLocation",
        "Primary Industry": "PrimaryIndustry",
        "Secondary Industry": "SecondaryIndustry",
        "Top 10 Technical Skills": "Top10Skills"
    }
    
    mapped_result = {}
    for original_key, mapped_key in field_mapping.items():
        # Get the value, strip any whitespace, and handle NULL standardization
        value = result.get(original_key, "NULL")
        if isinstance(value, str):
            value = value.strip()
            if value.upper() == "NULL" or not value:
                value = "NULL"
                
        # Only update if the field doesn't exist yet or the existing value is NULL
        if mapped_key not in mapped_result or mapped_result[mapped_key] == "NULL":
            mapped_result[mapped_key] = value
    
    # Add all directly extracted fields if they're available and not already set
    for field, value in direct_fields.items():
        if value and (mapped_result.get(field, "NULL") == "NULL"):
            mapped_result[field] = value
            logging.info(f"Using directly extracted {field}: '{value}'")
    
    # Verify titles were successfully extracted
    if mapped_result.get("PrimaryTitle", "NULL") == "NULL":
        logging.error(f"PRIMARY TITLE STILL MISSING AFTER ALL PARSING ATTEMPTS")
        # Log all keys for debugging
        logging.error(f"All available keys: {list(result.keys())}")
        
    # Verify company fields were extracted
    missing_companies = []
    for company_field in ["MostRecentCompany", "SecondMostRecentCompany", "ThirdMostRecentCompany"]:
        if mapped_result.get(company_field, "NULL") == "NULL":
            missing_companies.append(company_field)
    
    if missing_companies:
        logging.error(f"MISSING COMPANY FIELDS: {', '.join(missing_companies)}")
        
    # Verify industry fields
    missing_industry = []
    for industry_field in ["PrimaryIndustry", "SecondaryIndustry"]:
        if mapped_result.get(industry_field, "NULL") == "NULL":
            missing_industry.append(industry_field)
            
    if missing_industry:
        logging.error(f"MISSING INDUSTRY FIELDS: {', '.join(missing_industry)}")
        
    # Verify category fields from Step 2
    # We'll check these later when parsing Step 2 response
        
    # Verify date fields were extracted
    missing_dates = []
    for date_field in ["MostRecentStartDate", "MostRecentEndDate"]:
        if mapped_result.get(date_field, "NULL") == "NULL":
            missing_dates.append(date_field)
            
    if missing_dates:
        logging.error(f"MISSING DATE FIELDS: {', '.join(missing_dates)}")
        
    # Verify personal info fields were extracted
    missing_personal_info = []
    for personal_field in ["FirstName", "LastName", "Email", "Phone1"]:
        if mapped_result.get(personal_field, "NULL") == "NULL":
            missing_personal_info.append(personal_field)
            
    if missing_personal_info:
        logging.error(f"MISSING PERSONAL INFO FIELDS: {', '.join(missing_personal_info)}")
        
    return mapped_result

# Custom version of parse_step2_response with updated field mappings for technical languages
def extract_step2_fields_directly(response_text):
    """Extract step 2 fields directly using regex patterns"""
    import re
    
    # Dictionary to store extracted fields
    extracted = {}
    
    # Track hardware extraction statistics
    hardware_mentions = []
    
    # First try to extract hardware using the formatted pattern we requested
    hardware_section_match = re.search(r'(Hardware 1:.+?)(?=Based on their skills|$)', response_text, re.DOTALL)
    if hardware_section_match:
        hardware_section = hardware_section_match.group(1).strip()
        logging.info(f"Found formatted hardware section: {hardware_section}")
        
        # Extract individual hardware items
        hardware_matches = re.findall(r'Hardware (\d): (.+?)(?:\n|$)', hardware_section)
        for idx, value in hardware_matches:
            if idx.isdigit() and 1 <= int(idx) <= 5:
                field_name = f"Hardware{idx}"
                clean_value = value.strip()
                if clean_value.upper() != "NULL" and clean_value:
                    extracted[field_name] = clean_value
                    hardware_mentions.append(f"{field_name}: {clean_value}")
                    logging.info(f"Direct extract (Step 2): Found {field_name} '{clean_value}' from formatted section")
    
    # If we didn't find the formatted section, look for the common Q&A format
    qa_hardware_patterns = [
        (r"(?:- )?What physical hardware do they talk about using the most\?:\s*(.+?)(?:\n|$)", "Hardware1"),
        (r"(?:- )?What physical hardware do they talk about using the second most\?:\s*(.+?)(?:\n|$)", "Hardware2"),
        (r"(?:- )?What physical hardware do they talk about using the third most\?:\s*(.+?)(?:\n|$)", "Hardware3"),
        (r"(?:- )?What physical hardware do they talk about using the fourth most\?:\s*(.+?)(?:\n|$)", "Hardware4"),
        (r"(?:- )?What physical hardware do they talk about using the fifth most\?:\s*(.+?)(?:\n|$)", "Hardware5")
    ]
    
    # Try to extract each hardware item using Q&A format
    for pattern, field_name in qa_hardware_patterns:
        match = re.search(pattern, response_text)
        if match:
            value = match.group(1).strip()
            if value.upper() != "NULL" and value != "":
                extracted[field_name] = value
                hardware_mentions.append(f"{field_name}: {value}")
                logging.info(f"Direct extract (Step 2): Found {field_name} '{value}' from Q&A format")
    
    # Patterns for technical fields
    tech_patterns = {
        "PrimarySoftwareLanguage": [
            r"What technical language do they use most often\?:\s*(.+)",
            r"What programming language do they talk most about the most\?:\s*(.+)",
            r"Primary technical language:\s*(.+)",
            r"Most used programming language:\s*(.+)"
        ],
        "SecondarySoftwareLanguage": [
            r"What technical language do they use second most often\?:\s*(.+)",
            r"What programming language do they talk most about the second most\?:\s*(.+)",
            r"Secondary technical language:\s*(.+)",
            r"Second most used programming language:\s*(.+)"
        ],
        "TertiarySoftwareLanguage": [
            r"What technical language do they use third most often\?:\s*(.+)",
            r"What programming language do they talk most about the third the most\?:\s*(.+)",
            r"Tertiary technical language:\s*(.+)",
            r"Third most used programming language:\s*(.+)"
        ],
        "SoftwareApp1": [
            r"(?:- )?What software do they talk about using the most\?:\s*(.+)",
            r"Primary software application:\s*(.+)",
            r"Most used software:\s*(.+)"
        ],
        "SoftwareApp2": [
            r"(?:- )?What software do they talk about using the second most\?:\s*(.+)",
            r"Secondary software application:\s*(.+)",
            r"Second most used software:\s*(.+)"
        ],
        "SoftwareApp3": [
            r"(?:- )?What software do they talk about using the third most\?:\s*(.+)",
            r"Tertiary software application:\s*(.+)",
            r"Third most used software:\s*(.+)"
        ],
        "SoftwareApp4": [
            r"(?:- )?What software do they talk about using the fourth most\?:\s*(.+)",
            r"Fourth software application:\s*(.+)",
            r"Fourth most used software:\s*(.+)"
        ],
        "SoftwareApp5": [
            r"(?:- )?What software do they talk about using the fifth most\?:\s*(.+)",
            r"Fifth software application:\s*(.+)",
            r"Fifth most used software:\s*(.+)"
        ],
        "Hardware1": [
            r"What physical hardware do they talk about using the most\?:\s*(.+)",
            r"Primary hardware:\s*(.+)",
            r"Most used hardware:\s*(.+)",
            r"Primary physical device:\s*(.+)",
            r"Most common hardware device:\s*(.+)",
            r"Hardware 1:\s*(.+)"
        ],
        "Hardware2": [
            r"What physical hardware do they talk about using the second most\?:\s*(.+)",
            r"Secondary hardware:\s*(.+)",
            r"Second most used hardware:\s*(.+)",
            r"Secondary physical device:\s*(.+)",
            r"Second most common hardware device:\s*(.+)",
            r"Hardware 2:\s*(.+)"
        ],
        "Hardware3": [
            r"What physical hardware do they talk about using the third most\?:\s*(.+)",
            r"Tertiary hardware:\s*(.+)",
            r"Third most used hardware:\s*(.+)",
            r"Tertiary physical device:\s*(.+)",
            r"Third most common hardware device:\s*(.+)",
            r"Hardware 3:\s*(.+)"
        ],
        "Hardware4": [
            r"What physical hardware do they talk about using the fourth most\?:\s*(.+)",
            r"Fourth hardware:\s*(.+)",
            r"Fourth most used hardware:\s*(.+)",
            r"Fourth physical device:\s*(.+)",
            r"Fourth most common hardware device:\s*(.+)",
            r"Hardware 4:\s*(.+)"
        ],
        "Hardware5": [
            r"What physical hardware do they talk about using the fifth most\?:\s*(.+)",
            r"Fifth hardware:\s*(.+)",
            r"Fifth most used hardware:\s*(.+)",
            r"Fifth physical device:\s*(.+)",
            r"Fifth most common hardware device:\s*(.+)",
            r"Hardware 5:\s*(.+)"
        ],
        "PrimaryCategory": [
            r"Based on their skills, put them in a primary technical category:\s*(.+)"
        ],
        "SecondaryCategory": [
            r"Based on their skills, put them in a subsidiary technical category:\s*(.+)",
            r"Based on their skills, put them in a secondary technical category:\s*(.+)",
            r"Secondary technical category:\s*(.+)",
            r"subsidiary technical category:\s*(.+)",
            r"Second technical category:\s*(.+)",
            r"Second most relevant technical category:\s*(.+)"
        ],
        "ProjectTypes": [
            r"Types of projects they have worked on:\s*(.+)"
        ],
        # DISABLED to reduce output tokens
        # "Specialty": [
        #     r"Based on their skills, categories, certifications, and industries, determine what they specialize in:\s*(.+)"
        # ],
        # "Summary": [
        #     r"Based on all this knowledge, write a summary of this candidate that could be sellable to an employer:\s*(.+)",
        #     r"Based on all this knowledge, write a summary of this candidate:\s*(.+)"
        # ],
        "LengthinUS": [
            r"How long have they lived in the United States\(numerical answer only\):\s*(.+)"
        ],
        "YearsofExperience": [
            r"Total years of professional experience \(numerical answer only\):\s*(.+)"
        ],
        "AvgTenure": [
            r"Average tenure at companies in years \(numerical answer only\):\s*(.+)"
        ]
    }
    
    # Extract all technology fields
    for field, patterns in tech_patterns.items():
        for pattern in patterns:
            match = re.search(pattern, response_text)
            if match:
                value = match.group(1).strip()
                if value.upper() != "NULL" and value != "":
                    extracted[field] = value
                    # Track hardware field extractions specifically
                    if field.startswith("Hardware"):
                        hardware_mentions.append(f"{field}: {value}")
                    logging.info(f"Direct extract (Step 2): Found {field} '{value}'")
                break
    
    # Log hardware extraction stats
    if any(field.startswith("Hardware") for field in extracted.keys()):
        logging.info(f"Hardware extraction successful: {len(hardware_mentions)} hardware fields found")
        logging.info(f"Hardware mentions: {', '.join(hardware_mentions)}")
    
    return extracted

def parse_step2_response(response_text):
    """Parse the response from step 2 with updated field mappings"""
    # Try direct extraction first
    direct_fields = extract_step2_fields_directly(response_text)
    
    result = {}
    sections = {
        "TECHNICAL SKILLS AND LANGUAGES": [
            "Primary Technical Language", 
            "Secondary Technical Language", "Tertiary Technical Language"
        ],
        "SOFTWARE AND HARDWARE": [
            "Primary Software Application", "Secondary Software Application", 
            "Tertiary Software Application", "Fourth Software Application",
            "Fifth Software Application", "Primary Hardware", "Secondary Hardware",
            "Tertiary Hardware", "Fourth Hardware", "Fifth Hardware"
        ],
        "TECHNICAL CATEGORIZATION": [
            "Primary Technical Category", "Secondary Technical Category",
            "Types of Projects", "Technical Specialization"
        ],
        "EXPERIENCE CALCULATIONS": [
            "Years of Experience in United States", 
            "Total Years of Professional Experience",
            "Average Job Tenure in Years"
        ],
        "CANDIDATE SUMMARY": [
            "Professional Summary"
        ]
    }
    
    lines = response_text.strip().split('\n')
    current_section = None
    
    for line in lines:
        line = line.strip()
        if not line:
            continue
            
        # Check if this is a section header
        if line.endswith(':') and line.isupper():
            current_section = line[:-1]
            continue
            
        # Parse key-value pair
        if ':' in line:
            parts = line.split(':', 1)
            key = parts[0].strip('- \t')
            value = parts[1].strip()
            
            # Normalize NULL values
            if value.upper() == 'NULL' or value == '':
                value = 'NULL'
                
            result[key] = value
    
    # Map to standard field names - updated to match exact prompt questions with new technical language fields
    field_mapping = {
        "What technical language do they use most often?": "PrimarySoftwareLanguage",
        "What technical language do they use second most often?": "SecondarySoftwareLanguage",
        "What technical language do they use third most often?": "TertiarySoftwareLanguage",
        "What software do they talk about using the most?": "SoftwareApp1",
        "What software do they talk about using the second most?": "SoftwareApp2",
        "What software do they talk about using the third most?": "SoftwareApp3",
        "What software do they talk about using the fourth most?": "SoftwareApp4",
        "What software do they talk about using the fifth most?": "SoftwareApp5",
        "What physical hardware do they talk about using the most?": "Hardware1",
        "What physical hardware do they talk about using the second most?": "Hardware2",
        "What physical hardware do they talk about using the third most?": "Hardware3",
        "What physical hardware do they talk about using the fourth most?": "Hardware4",
        "What physical hardware do they talk about using the fifth most?": "Hardware5",
        "Based on their skills, put them in a primary technical category": "PrimaryCategory",
        "Based on their skills, put them in a subsidiary technical category": "SecondaryCategory",
        "Types of projects they have worked on": "ProjectTypes",
        # DISABLED to reduce output tokens
        # "Based on their skills, categories, certifications, and industries, determine what they specialize in": "Specialty",
        # "Based on all this knowledge, write a summary of this candidate that could be sellable to an employer": "Summary",
        "How long have they lived in the United States(numerical answer only)": "LengthinUS",
        "Total years of professional experience (numerical answer only)": "YearsofExperience",
        "Average tenure at companies in years (numerical answer only)": "AvgTenure"
    }
    
    mapped_result = {}
    for original_key, mapped_key in field_mapping.items():
        # Get the value, strip any whitespace, and handle NULL standardization
        value = result.get(original_key, "NULL")
        if isinstance(value, str):
            value = value.strip()
            if value.upper() == "NULL" or not value:
                value = "NULL"
        mapped_result[mapped_key] = value
    
    # Add directly extracted fields if available
    for field, value in direct_fields.items():
        if value and (mapped_result.get(field, "NULL") == "NULL"):
            mapped_result[field] = value
            logging.info(f"Using directly extracted Step 2 field: {field} = '{value}'")
    
    # Verify category fields
    missing_categories = []
    for category_field in ["PrimaryCategory", "SecondaryCategory"]:
        if mapped_result.get(category_field, "NULL") == "NULL":
            missing_categories.append(category_field)
            
    if missing_categories:
        logging.error(f"MISSING CATEGORY FIELDS: {', '.join(missing_categories)}")
        # Log what is available in the response to debug
        if "PrimaryCategory" in missing_categories:
            logging.error(f"Looking for any category-like fields in Step 2 response")
            for key in result.keys():
                if "category" in key.lower() or "technical" in key.lower():
                    logging.error(f"Possible category field: '{key}': '{result[key]}'")
    
    # Verify hardware fields
    missing_hardware_fields = []
    hardware_fields = ["Hardware1", "Hardware2", "Hardware3", "Hardware4", "Hardware5"]
    
    # Check how many hardware fields are populated
    populated_hardware = [field for field in hardware_fields if mapped_result.get(field, "NULL") != "NULL"]
    
    # If we have at least one hardware field but not all five, log the missing ones
    if populated_hardware and len(populated_hardware) < 5:
        missing_hardware = [field for field in hardware_fields if field not in populated_hardware]
        logging.warning(f"INCOMPLETE HARDWARE FIELDS: {len(populated_hardware)}/5 hardware fields populated")
        logging.warning(f"Missing hardware fields: {', '.join(missing_hardware)}")
    # If we have no hardware fields populated at all, that's worth logging as an error
    elif not populated_hardware:
        logging.error(f"MISSING HARDWARE FIELDS: No hardware fields populated")
    else:
        logging.info(f"All 5 hardware fields successfully populated")
    
    return mapped_result



def log_title_fields(results, userid, step):
    """Log the job title fields for debugging"""
    # Log primary title
    primary = results.get("PrimaryTitle", "NULL")
    if not primary or primary == "NULL":
        logging.error(f"UserID {userid} - Step {step}: PrimaryTitle is empty or NULL")
    else:
        logging.info(f"UserID {userid} - Step {step}: PrimaryTitle = '{primary}'")
        
    # Log secondary title
    secondary = results.get("SecondaryTitle", "NULL")
    if not secondary or secondary == "NULL":
        logging.error(f"UserID {userid} - Step {step}: SecondaryTitle is empty or NULL")
    else:
        logging.info(f"UserID {userid} - Step {step}: SecondaryTitle = '{secondary}'")
        
    # Log tertiary title
    tertiary = results.get("TertiaryTitle", "NULL")
    if not tertiary or tertiary == "NULL":
        logging.warning(f"UserID {userid} - Step {step}: TertiaryTitle is empty or NULL")
    else:
        logging.info(f"UserID {userid} - Step {step}: TertiaryTitle = '{tertiary}'")

def validate_linkedin_url(url_value):
    """
    Validate and format a LinkedIn URL
    Returns a properly formatted URL or empty string if invalid
    Only accepts valid LinkedIn profile URLs with proper usernames
    """
    if not url_value or url_value == "NULL" or url_value.strip() == "":
        return ""
    
    import re
    
    # Clean up the input
    url = url_value.strip()
    
    # If the URL is a generic LinkedIn URL without a specific profile, reject it
    generic_linkedin_patterns = [
        r'^https?://(?:www\.)?linkedin\.com/?$',  # LinkedIn homepage
        r'^https?://(?:www\.)?linkedin\.com/in/?$',  # Generic /in/ URL
        r'^https?://(?:www\.)?linkedin\.com/pub/?$',  # Generic /pub/ URL
        r'^https?://(?:www\.)?linkedin\.com/profile/?$',  # Generic /profile/ URL
        r'^https?://(?:www\.)?linkedin\.com/company/?$',  # Generic /company/ URL
        r'^https?://(?:www\.)?linkedin\.com/in/linkedin/?$',  # Specific invalid case
        r'^https?://(?:www\.)?linkedin\.com/in/profile/?$',  # Generic profiles
        r'^https?://(?:www\.)?linkedin\.com/in/user/?$',  # Generic profiles
        r'^linkedin$',  # Just the word
        r'^linkedin\.com$'  # Just domain
    ]
    
    # Reject generic LinkedIn URLs
    for pattern in generic_linkedin_patterns:
        if re.match(pattern, url):
            logging.warning(f"Generic LinkedIn URL rejected: '{url_value}'")
            return ""
    
    # Extract username for validation
    username_match = re.search(r'linkedin\.com/in/([\w\-\.%]+)', url)
    if username_match:
        username = username_match.group(1)
        
        # Reject usernames that are too short (likely generic) or contain generic terms
        if len(username) < 4 or username.lower() in ['user', 'profile', 'linkedin', 'my', 'page', 'me']:
            logging.warning(f"LinkedIn URL with generic username rejected: '{url_value}'")
            return ""
            
        # Format to standardized URL
        return f"https://www.linkedin.com/in/{username}"
    
    # Check for other valid LinkedIn URL patterns
    linkedin_patterns = [
        r'^https?://(?:www\.)?linkedin\.com/pub/([\w\-\.%/]+)$',  # Public profile URL
        r'^https?://(?:www\.)?linkedin\.com/profile/([\w\-\.%]+)$',  # Other profile format
        r'^https?://(?:www\.)?linkedin\.com/company/([\w\-\.%]+)/?$'  # Company profile
    ]
    
    # Test the URL against other valid patterns
    for pattern in linkedin_patterns:
        match = re.match(pattern, url)
        if match:
            # Extract the identifier and ensure it's not generic
            identifier = match.group(1)
            if len(identifier) < 4 or identifier.lower() in ['user', 'profile', 'linkedin', 'my', 'page', 'me']:
                logging.warning(f"LinkedIn URL with generic identifier rejected: '{url_value}'")
                return ""
            return url
    
    # If it's just a username (handle), convert to proper URL if it's valid
    if re.match(r'^[\w\-\.%]+$', url) and not url.startswith('http') and not '/' in url and not ' ' in url:
        # Validate the username
        if len(url) < 4 or url.lower() in ['user', 'profile', 'linkedin', 'my', 'page', 'me']:
            logging.warning(f"Generic LinkedIn username rejected: '{url_value}'")
            return ""
        return f"https://www.linkedin.com/in/{url}"
    
    # If we can't validate or fix it, return empty string
    logging.warning(f"Invalid LinkedIn URL: '{url_value}'")
    return ""

def validate_date_format(date_value):
    """
    Validate and format a date string for SQL Server DATE type
    Returns a properly formatted date string or None if invalid
    """
    if not date_value or date_value == "NULL" or date_value.strip() == "":
        return None
        
    # Handle 'Present' specially
    if date_value.lower() == 'present':
        return None
    
    import re
    from datetime import datetime
    
    # Define valid date formats to try
    date_formats = [
        '%Y-%m-%d',  # Standard ISO format
        '%m/%d/%Y',  # MM/DD/YYYY
        '%Y/%m/%d',  # YYYY/MM/DD
        '%d-%m-%Y',  # DD-MM-YYYY
        '%Y-%m',     # YYYY-MM
        '%b %Y',     # 'Jan 2023'
        '%B %Y',     # 'January 2023'
        '%m-%Y',     # '01-2023'
        '%Y'         # Just year
    ]
    
    clean_value = date_value.strip()
    
    # Try each format
    for fmt in date_formats:
        try:
            date_obj = datetime.strptime(clean_value, fmt)
            # Return in SQL Server compatible format
            return date_obj.strftime('%Y-%m-%d')
        except ValueError:
            continue
    
    # If nothing worked, extract year-month-day using regex as a fallback
    try:
        # Try to extract YYYY-MM-DD pattern
        date_match = re.search(r'(\d{4})[/-](\d{1,2})[/-](\d{1,2})', clean_value)
        if date_match:
            year, month, day = date_match.groups()
            return f"{int(year):04d}-{int(month):02d}-{int(day):02d}"
            
        # Try to extract just YYYY-MM pattern
        date_match = re.search(r'(\d{4})[/-](\d{1,2})', clean_value)
        if date_match:
            year, month = date_match.groups()
            return f"{int(year):04d}-{int(month):02d}-01"  # Default to first day of month
            
        # Try to extract just year
        year_match = re.search(r'(\d{4})', clean_value)
        if year_match:
            year = year_match.group(1)
            return f"{int(year):04d}-01-01"  # Default to January 1st
    except Exception:
        pass
        
    # If we get here, we couldn't parse the date
    logging.warning(f"Could not parse date value: '{date_value}'")
    return None

def prepare_update_data(enhanced_results, step1_results=None, skills_list=None):
    """Prepare update data dictionary, safely accessing fields with .get() method"""
    # Prepare skills list if provided
    if skills_list is None:
        skills_list = ["" for _ in range(10)]
    Skill1, Skill2, Skill3, Skill4, Skill5, Skill6, Skill7, Skill8, Skill9, Skill10 = skills_list[:10]
    
    # Create safe update data dictionary - use .get() for all fields
    return {
        "PrimaryTitle": enhanced_results.get("PrimaryTitle", "") or (step1_results.get("PrimaryTitle", "") if step1_results else ""),
        "SecondaryTitle": enhanced_results.get("SecondaryTitle", "") or (step1_results.get("SecondaryTitle", "") if step1_results else ""),
        "TertiaryTitle": enhanced_results.get("TertiaryTitle", "") or (step1_results.get("TertiaryTitle", "") if step1_results else ""),
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
        # DISABLED to reduce output tokens
        # "Specialty": enhanced_results.get("Specialty", ""),
        # "Summary": enhanced_results.get("Summary", ""),
        "LengthinUS": enhanced_results.get("LengthinUS", ""),
        "YearsofExperience": enhanced_results.get("YearsofExperience", ""),
        "AvgTenure": enhanced_results.get("AvgTenure", ""),
        "LastProcessed": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]  # Add LastProcessed timestamp
    }

def process_single_resume_two_step(resume_data):
    """Process a single resume using two API calls with taxonomy enhancement"""
    userid, resume_text = resume_data
    try:
        logging.info(f"Starting taxonomy-enhanced two-step processing for UserID: {userid}")
        total_start_time = time.time()
        
        # Calculate token count and cost
        resume_token_count = num_tokens_from_string(resume_text)
        input_cost_step1 = resume_token_count * 0.00000025  # $0.25 per million tokens for input (GPT-5 mini)
        estimated_output_tokens = 500
        output_cost_step1 = estimated_output_tokens * 0.000002  # $2.00 per million tokens for output (GPT-5 mini)
        
        logging.info(f"UserID {userid}: {resume_token_count} tokens")
        
        # STEP 1: Personal info, work history, and industry with taxonomy enhancement
        step1_start_time = time.time()
        
        # Create step 1 prompt with taxonomy context - pass userid for logging
        step1_messages = create_step1_prompt(resume_text, userid=userid)
        step1_messages = apply_token_truncation(step1_messages)
        
        # Send to OpenAI API
        logging.info(f"UserID {userid}: Sending taxonomy-enhanced Step 1 request")
        step1_response = openai.chat.completions.create(
            model=MODEL,
            messages=step1_messages,
            temperature=1  # New model only supports default temperature of 1
            # Note: gpt-5-mini returns empty responses with max_completion_tokens
        )
        
        step1_time = time.time() - step1_start_time

        # Capture actual token usage from API response
        step1_actual_tokens = 0
        if hasattr(step1_response, 'usage'):
            step1_actual_tokens = step1_response.usage.total_tokens
            logging.info(f"UserID {userid}: Step 1 actual tokens - Input: {step1_response.usage.prompt_tokens}, Output: {step1_response.usage.completion_tokens}")

        logging.info(f"UserID {userid}: Taxonomy-enhanced Step 1 completed in {step1_time:.2f}s")
        
        if not step1_response or not step1_response.choices:
            logging.error(f"UserID {userid}: No response from Step 1")
            return {
                'userid': userid,
                'success': False,
                'error': "No response from Step 1",
                'token_count': resume_token_count
            }
        
        # Parse step 1 response
        step1_text = step1_response.choices[0].message.content
        step1_results = parse_step1_response(step1_text)
        
        # Log first 200 chars of response for debugging
        logging.info(f"UserID {userid}: Step 1 response first 200 chars: {step1_text[:200]}")
        
        # Log the title fields for debugging
        log_title_fields(step1_results, userid, "Step 1")
        
        logging.info(f"UserID {userid}: Step 1 parsed {len(step1_results)} fields")
        
        # STEP 2: Skills, technical info, and experience calculations with taxonomy enhancement
        step2_start_time = time.time()
        
        # Create step 2 prompt using results from step 1 and taxonomy context - pass userid for logging
        step2_messages = create_step2_prompt(resume_text, step1_results, userid=userid)
        step2_messages = apply_token_truncation(step2_messages)
        
        # Calculate cost for step 2
        step2_prompt_text = "\n".join(msg["content"] for msg in step2_messages)
        step2_token_count = num_tokens_from_string(step2_prompt_text)
        input_cost_step2 = step2_token_count * 0.00000025  # $0.25 per million tokens for input (GPT-5 mini)
        output_cost_step2 = estimated_output_tokens * 0.000002  # $2.00 per million tokens for output (GPT-5 mini)
        
        total_cost = input_cost_step1 + output_cost_step1 + input_cost_step2 + output_cost_step2
        logging.info(f"UserID {userid}: Step 2 tokens: {step2_token_count}, Est. total cost: ${total_cost:.6f}")
        
        # Send Step 2 to OpenAI API with higher temperature for hardware section
        logging.info(f"UserID {userid}: Sending taxonomy-enhanced Step 2 request")
        step2_response = openai.chat.completions.create(
            model=MODEL,
            messages=step2_messages,
            temperature=1  # New model only supports default temperature of 1
            # Note: gpt-5-mini returns empty responses with max_completion_tokens
        )
        
        step2_time = time.time() - step2_start_time

        # Capture actual token usage from API response
        step2_actual_tokens = 0
        if hasattr(step2_response, 'usage'):
            step2_actual_tokens = step2_response.usage.total_tokens
            logging.info(f"UserID {userid}: Step 2 actual tokens - Input: {step2_response.usage.prompt_tokens}, Output: {step2_response.usage.completion_tokens}")

        logging.info(f"UserID {userid}: Taxonomy-enhanced Step 2 completed in {step2_time:.2f}s")
        
        if not step2_response or not step2_response.choices:
            logging.error(f"UserID {userid}: No response from Step 2")
            return {
                'userid': userid,
                'success': False,
                'error': "No response from Step 2",
                'token_count': resume_token_count + step2_token_count
            }
        
        # Parse step 2 response
        step2_text = step2_response.choices[0].message.content
        
        # Log the full response for hardware analysis
        if "hardware" in step2_text.lower() or "physical" in step2_text.lower():
            logging.info(f"UserID {userid}: HARDWARE ANALYSIS - Full Step 2 response segment:")
            
            # Find the section with hardware mentions
            response_lines = step2_text.split('\n')
            hardware_section = []
            capturing = False
            
            for line in response_lines:
                # Start capturing when we hit hardware-related questions
                if "physical hardware" in line.lower() or ("hardware" in line.lower() and "?" in line) or "hardware " in line.lower():
                    capturing = True
                    hardware_section.append(line)
                # Continue capturing for a few lines after we start
                elif capturing and len(hardware_section) < 15:
                    hardware_section.append(line)
                # Stop after we've captured enough or hit the next section
                elif capturing and ("?" in line or len(hardware_section) >= 15) and "hardware" not in line.lower():
                    break
            
            # Log the hardware section if found
            if hardware_section:
                for line in hardware_section:
                    logging.info(f"UserID {userid}: HARDWARE RAW: {line}")
        
        step2_results = parse_step2_response(step2_text)
        
        logging.info(f"UserID {userid}: Step 2 parsed {len(step2_results)} fields")
        
        # Combine results from both steps
        combined_results = {**step1_results, **step2_results}
        
        # Log the combined results title fields for debugging
        logging.info(f"UserID {userid}: Combined results before date processing")
        log_title_fields(combined_results, userid, "Combined")
        
        # Apply enhanced date processing
        enhanced_results = process_resume_with_enhanced_dates(userid, combined_results)
        
        # Extract skills for database format
        skills_list = enhanced_results["Top10Skills"].split(", ") if enhanced_results["Top10Skills"] and enhanced_results["Top10Skills"] != "NULL" else []
        skills_list.extend([""] * (10 - len(skills_list)))  # Ensure we have 10 skills
        Skill1, Skill2, Skill3, Skill4, Skill5, Skill6, Skill7, Skill8, Skill9, Skill10 = skills_list[:10]
        
        # Clean up phone numbers - prevent duplicates and normalize format
        phone1 = enhanced_results.get("Phone1", "")
        phone2 = enhanced_results.get("Phone2", "")
        
        # Normalize phone numbers by removing all non-digit characters for comparison
        def normalize_phone(phone):
            import re
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
            if phone1 == phone2:
                logging.info(f"UserID {userid}: Removing duplicate phone number from Phone2 (same as Phone1)")
            elif normalized_phone1 == normalized_phone2:
                logging.info(f"UserID {userid}: Removing differently formatted duplicate phone number from Phone2: '{phone2}' (same as Phone1: '{phone1}')")
            else:
                logging.info(f"UserID {userid}: Removing NULL phone number from Phone2")
            enhanced_results["Phone2"] = ""
            
        # Create a dictionary with all the data for database update using the helper function
        update_data = prepare_update_data(enhanced_results, step1_results, skills_list)
        
        # Replace "NULL" strings with empty string for database and clean whitespace
        # Also validate and format date fields
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
                            logging.info(f"Formatted date for {key}: '{value}' -> '{formatted_date}'")
                        else:
                            # If unable to parse, set to empty to avoid DB errors
                            update_data[key] = ""
                            logging.warning(f"Could not format date {key}: '{value}', setting to empty")
                    # Special handling for LinkedIn URL
                    elif key == "Linkedin":
                        # Validate and format LinkedIn URL
                        valid_url = validate_linkedin_url(value)
                        if valid_url:
                            update_data[key] = valid_url
                            if valid_url != value:
                                logging.info(f"Formatted LinkedIn URL: '{value}' -> '{valid_url}'")
                        else:
                            # If invalid URL, set to empty
                            update_data[key] = ""
                            logging.warning(f"Invalid LinkedIn URL: '{value}', setting to empty")
                    else:
                        update_data[key] = value
        
        # Log the final title fields right before database update
        logging.info(f"UserID {userid}: Final values before database update")
        logging.info(f"UserID {userid}: PrimaryTitle = '{update_data.get('PrimaryTitle', '')}'")
        logging.info(f"UserID {userid}: SecondaryTitle = '{update_data.get('SecondaryTitle', '')}'")
        logging.info(f"UserID {userid}: TertiaryTitle = '{update_data.get('TertiaryTitle', '')}'")
        
        # Check if any title fields are still empty
        if not update_data.get('PrimaryTitle') or not update_data.get('SecondaryTitle') or not update_data.get('TertiaryTitle'):
            logging.warning(f"UserID {userid}: Missing titles right before DB update!")
            logging.warning(f"UserID {userid}: Raw response snippet: {step1_text[:300]}")
            
            # Log to error file
            error_logger = get_error_logger()
            missing_titles = []
            if not update_data.get('PrimaryTitle'): missing_titles.append('PrimaryTitle')
            if not update_data.get('SecondaryTitle'): missing_titles.append('SecondaryTitle')
            if not update_data.get('TertiaryTitle'): missing_titles.append('TertiaryTitle')
            
            error_logger.log_candidate_warning(
                userid=str(userid),
                warning_type='MISSING_TITLES',
                warning_details=f"Missing: {', '.join(missing_titles)}",
                additional_info={'response_snippet': step1_text[:200]}
            )
        
        # Update database with retry for deadlocks
        update_success = update_candidate_record_with_retry(userid, update_data)
        
        if not update_success:
            # Log database update failure
            error_logger = get_error_logger()
            error_logger.log_candidate_error(
                userid=str(userid),
                error_type='DB_UPDATE_FAILED',
                error_details='Failed to update candidate record in database',
                additional_info={'fields_attempted': len(update_data)}
            )
        
        total_time = time.time() - total_start_time
        logging.info(f"UserID {userid} taxonomy-enhanced two-step processing completed in {total_time:.2f}s - DB update: {'Success' if update_success else 'Failed'}")
        
        return {
            'userid': userid,
            'success': update_success,
            'processing_time': total_time,
            'step1_time': step1_time,
            'step2_time': step2_time,
            'token_count': step1_actual_tokens + step2_actual_tokens if (step1_actual_tokens and step2_actual_tokens) else resume_token_count + step2_token_count,
            'actual_input_tokens': (step1_response.usage.prompt_tokens + step2_response.usage.prompt_tokens) if (hasattr(step1_response, 'usage') and hasattr(step2_response, 'usage')) else 0,
            'actual_output_tokens': (step1_response.usage.completion_tokens + step2_response.usage.completion_tokens) if (hasattr(step1_response, 'usage') and hasattr(step2_response, 'usage')) else 0,
            'cost': total_cost
        }
    
    except Exception as e:
        error_message = str(e)
        logging.error(f"Error processing UserID {userid} with taxonomy-enhanced two-step approach: {error_message}")
        
        # Check for specific OpenAI errors
        error_type = 'PROCESSING_EXCEPTION'
        if 'rate_limit' in error_message.lower() or '429' in error_message:
            error_type = 'RATE_LIMIT_ERROR'
            logging.error(f"Rate limit error for UserID {userid}: {error_message}")
        elif 'timeout' in error_message.lower():
            error_type = 'TIMEOUT_ERROR'
            logging.error(f"Timeout error for UserID {userid}: {error_message}")
        elif 'api' in error_message.lower() and 'key' in error_message.lower():
            error_type = 'API_KEY_ERROR'
            logging.error(f"API key error for UserID {userid}: {error_message}")
        elif '503' in error_message or 'service_unavailable' in error_message.lower():
            error_type = 'SERVICE_UNAVAILABLE'
            logging.error(f"OpenAI service unavailable for UserID {userid}: {error_message}")
        
        # Log to error file
        error_logger = get_error_logger()
        import traceback
        error_logger.log_candidate_error(
            userid=str(userid),
            error_type=error_type,
            error_details=error_message,
            additional_info={'traceback': traceback.format_exc()[:500]}
        )
        
        return {
            'userid': userid,
            'success': False,
            'error': error_message,
            'error_type': error_type,
            'token_count': num_tokens_from_string(resume_text) if 'resume_text' in locals() else 0
        }

def process_batch_with_shared_prompts(resume_batch):
    """
    Process a batch of resumes using shared system prompts and batch API
    This dramatically reduces token usage by only counting system messages once
    """
    if not resume_batch:
        logging.info("Empty batch, nothing to process")
        return []
    
    # Get system messages (shared across all resumes)
    # We'll extract these from the first resume's prompts
    userid, first_resume = resume_batch[0]
    
    # Extract shared system messages from Step 1 prompt
    step1_messages = create_step1_prompt(first_resume, userid=userid)
    shared_system_step1 = [msg for msg in step1_messages if msg["role"] == "system"]
    
    # Log system message token count (which will only be counted once)
    system_tokens_step1 = sum(num_tokens_from_string(msg["content"]) for msg in shared_system_step1)
    logging.info(f"Step 1 shared system messages: {len(shared_system_step1)} messages, {system_tokens_step1} tokens (counted only once)")
    
    # Prepare batch of requests for Step 1
    batch_requests_step1 = []
    userid_map = {}
    
    for i, (userid, resume_text) in enumerate(resume_batch):
        # Create a user message with just the resume content
        user_message = {
            "role": "user",
            "content": f"Based on this resume, extract the requested information:\n\n{resume_text}"
        }
        
        # Combine shared system messages with user-specific content
        messages = shared_system_step1 + [user_message]
        
        # Add to batch requests
        batch_requests_step1.append({
            "model": MODEL,
            "messages": messages,
            "temperature": DEFAULT_TEMPERATURE,
            "max_tokens": MAX_TOKENS
        })
        
        # Track which userids correspond to which batch index
        userid_map[i] = userid
    
    # Log batch preparation
    logging.info(f"Prepared Step 1 batch with {len(batch_requests_step1)} requests")
    
    results = []
    
    # Use batch API if enabled, otherwise fall back to individual calls
    if USE_BATCH_API:
        try:
            logging.info("Using OpenAI batch API for Step 1 requests")
            batch_start_time = time.time()
            
            # Send batch request
            batch_responses = openai.beta.chat.completions.batch_create(
                requests=batch_requests_step1
            )
            
            batch_time = time.time() - batch_start_time
            logging.info(f"Batch API for Step 1 completed in {batch_time:.2f}s")
            
            # Process each response
            step1_results = []
            for i, response in enumerate(batch_responses):
                userid = userid_map[i]
                if response and response.choices:
                    step1_text = response.choices[0].message.content
                    parsed_step1 = parse_step1_response(step1_text)
                    step1_results.append((userid, resume_batch[i][1], parsed_step1, step1_text))
                    logging.info(f"Successfully processed Step 1 for UserID: {userid}")
                else:
                    logging.error(f"Empty or invalid Step 1 response for UserID: {userid}")
            
            # Now we need to prepare and send Step 2 requests using batch API
            # Extract shared system messages from Step 2 prompt using first result from Step 1
            if step1_results:
                userid, resume_text, step1_result, step1_text = step1_results[0]
                step2_messages = create_step2_prompt(resume_text, step1_result, userid=userid)
                shared_system_step2 = [msg for msg in step2_messages if msg["role"] == "system"]
                
                # Log system message token count (which will only be counted once)
                system_tokens_step2 = sum(num_tokens_from_string(msg["content"]) for msg in shared_system_step2)
                logging.info(f"Step 2 shared system messages: {len(shared_system_step2)} messages, {system_tokens_step2} tokens (counted only once)")
                
                # Prepare batch of requests for Step 2
                batch_requests_step2 = []
                
                for userid, resume_text, step1_result, step1_text in step1_results:
                    # Create a user message with just the resume and Step 1 results
                    user_message = {
                        "role": "user",
                        "content": f"Resume:\n\n{resume_text}\n\nStep 1 Results:\n\n{step1_text}\n\nNow extract the technical skills and experience details."
                    }
                    
                    # Combine shared system messages with user-specific content
                    messages = shared_system_step2 + [user_message]
                    
                    # Add to batch requests
                    batch_requests_step2.append({
                        "model": MODEL,
                        "messages": messages,
                        "temperature": 0.5,  # Higher temperature for hardware section
                        "max_tokens": MAX_TOKENS
                    })
                
                # Log batch preparation
                logging.info(f"Prepared Step 2 batch with {len(batch_requests_step2)} requests")
                
                # Send batch request for Step 2
                batch_start_time = time.time()
                
                # Send batch request
                batch_responses_step2 = openai.beta.chat.completions.batch_create(
                    requests=batch_requests_step2
                )
                
                batch_time = time.time() - batch_start_time
                logging.info(f"Batch API for Step 2 completed in {batch_time:.2f}s")
                
                # Process Step 2 responses and update database
                for i, response in enumerate(batch_responses_step2):
                    userid, resume_text, step1_result, _ = step1_results[i]
                    
                    if response and response.choices:
                        step2_text = response.choices[0].message.content
                        step2_result = parse_step2_response(step2_text)
                        
                        # Combine results from both steps
                        combined_results = {**step1_result, **step2_result}
                        
                        # Apply enhanced date processing
                        enhanced_results = process_resume_with_enhanced_dates(userid, combined_results)
                        
                        # Extract skills for database format
                        skills_list = enhanced_results["Top10Skills"].split(", ") if enhanced_results["Top10Skills"] and enhanced_results["Top10Skills"] != "NULL" else []
                        skills_list.extend([""] * (10 - len(skills_list)))  # Ensure we have 10 skills
                        Skill1, Skill2, Skill3, Skill4, Skill5, Skill6, Skill7, Skill8, Skill9, Skill10 = skills_list[:10]
                        
                        # Clean up phone numbers - prevent duplicates and normalize format
                        phone1 = enhanced_results.get("Phone1", "")
                        phone2 = enhanced_results.get("Phone2", "")
                        
                        # Normalize phone numbers by removing all non-digit characters for comparison
                        def normalize_phone(phone):
                            import re
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
                            if phone1 == phone2:
                                logging.info(f"UserID {userid}: Removing duplicate phone number from Phone2 (same as Phone1)")
                            elif normalized_phone1 == normalized_phone2:
                                logging.info(f"UserID {userid}: Removing differently formatted duplicate phone number from Phone2: '{phone2}' (same as Phone1: '{phone1}')")
                            else:
                                logging.info(f"UserID {userid}: Removing NULL phone number from Phone2")
                            enhanced_results["Phone2"] = ""
                        
                        # Create update data dictionary with fixed field values
                        update_data = prepare_update_data(enhanced_results, step1_result, skills_list)
                        
                        # Update database
                        update_success = update_candidate_record_with_retry(userid, update_data)
                        
                        # Calculate token usage (only counting resume content + shared system prompts once)
                        resume_token_count = num_tokens_from_string(resume_text)
                        
                        # Calculate accurate cost with shared prompts - using more aggressive token efficiency
                        # Instead of duplicating the resume and step1 results, we'll use condensed versions
                        user_tokens_step1 = num_tokens_from_string(resume_text)
                        
                        # For step2, we'll only count critical fields from step1 results instead of full text
                        critical_fields = []
                        for key in ["PrimaryTitle", "SecondaryTitle", "TertiaryTitle", "MostRecentCompany", "SecondMostRecentCompany"]:
                            if key in step1_result and step1_result[key] != "NULL":
                                critical_fields.append(f"{key}: {step1_result[key]}")
                        
                        step1_summary = "\n".join(critical_fields)
                        user_tokens_step2 = num_tokens_from_string(step1_summary) + (num_tokens_from_string(resume_text) * 0.1)  # Only counting 10% of resume since it's duplicate
                        
                        # Only add system tokens once for the whole batch, divided by batch size
                        # The taxonomy files are large, so this is a big savings
                        per_resume_system_tokens = (system_tokens_step1 + system_tokens_step2) / len(resume_batch)
                        
                        # Calculate token total with more realistic token usage
                        total_tokens = per_resume_system_tokens + user_tokens_step1 + user_tokens_step2
                        
                        # Add some overhead for API communications
                        overhead_tokens = 100
                        total_tokens += overhead_tokens
                        
                        # Calculate cost - GPT-5 mini pricing
                        input_cost_rate = 0.00000025  # $0.25 per million tokens for input (GPT-5 mini)
                        output_cost_rate = 0.000002  # $2.00 per million tokens for output (GPT-5 mini)
                        
                        # Accurately account for both API calls
                        input_cost = total_tokens * input_cost_rate
                        # More realistic output estimate based on actual usage
                        # Each step typically generates 500-1000 tokens
                        estimated_output_tokens_total = 1500  # Total for both steps
                        output_cost = estimated_output_tokens_total * output_cost_rate
                        total_cost = input_cost + output_cost
                        
                        # Log the token calculation details
                        logging.info(f"UserID {userid} token breakdown:")
                        logging.info(f"- System tokens (shared): {per_resume_system_tokens:.1f} tokens/resume")
                        logging.info(f"- User tokens step1: {user_tokens_step1} tokens")
                        logging.info(f"- User tokens step2: {user_tokens_step2} tokens")
                        logging.info(f"- Total tokens: {total_tokens} tokens")
                        logging.info(f"- Estimated cost: ${total_cost:.6f}")
                        
                        result = {
                            'userid': userid,
                            'success': update_success,
                            'processing_time': batch_time,
                            'token_count': total_tokens,
                            'cost': total_cost
                        }
                        
                        results.append(result)
                        logging.info(f"UserID {userid} processed via batch API - DB update: {'Success' if update_success else 'Failed'}")
                    else:
                        logging.error(f"Empty or invalid Step 2 response for UserID: {userid}")
            else:
                logging.error("No valid Step 1 results to process for Step 2")
        
        except Exception as e:
            logging.error(f"Error using batch API: {str(e)}")
            logging.warning("Falling back to individual processing")
            
            # If batch API fails, fall back to original processing method
            results = []
            with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                # Submit all tasks using the original method
                future_to_resume = {executor.submit(process_single_resume_two_step, resume_data): resume_data for resume_data in resume_batch}
                
                # Process results as they complete
                for future in concurrent.futures.as_completed(future_to_resume):
                    resume_data = future_to_resume[future]
                    try:
                        result = future.result()
                        results.append(result)
                        # Update progress
                        logging.info(f"Progress: {len(results)}/{len(resume_batch)} resumes completed")
                    except Exception as e:
                        userid = resume_data[0]
                        logging.error(f"Exception for UserID {userid}: {str(e)}")
    
    else:
        # Use the original processing method if batch API is disabled
        logging.info("Using individual API calls (batch API disabled)")
        results = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            # Submit all tasks using the original method
            future_to_resume = {executor.submit(process_single_resume_two_step, resume_data): resume_data for resume_data in resume_batch}
            
            # Process results as they complete
            for future in concurrent.futures.as_completed(future_to_resume):
                resume_data = future_to_resume[future]
                try:
                    result = future.result()
                    results.append(result)
                    # Update progress
                    logging.info(f"Progress: {len(results)}/{len(resume_batch)} resumes completed")
                except Exception as e:
                    userid = resume_data[0]
                    logging.error(f"Exception for UserID {userid}: {str(e)}")
                    
                    # Log to error file
                    error_logger.log_candidate_error(
                        userid=str(userid),
                        error_type='BATCH_PROCESSING_EXCEPTION',
                        error_details=str(e)
                    )
                    
                    results.append({
                        'userid': userid,
                        'success': False,
                        'error': str(e)
                    })
    
    return results

def run_taxonomy_enhanced_batch():
    """Run a batch of resume processing with the taxonomy-enhanced two-step approach"""
    error_logger = get_error_logger()
    
    try:
        # Start timing
        batch_start_time = time.time()
        
        # Get batch of resumes
        resume_batch = get_resume_batch(batch_size=BATCH_SIZE)
        
        if not resume_batch:
            logging.info("No resumes to process.")
            return
        
        logging.info(f"Starting taxonomy-enhanced two-step processing of {len(resume_batch)} resumes with {MAX_WORKERS} workers")
        
        # Process with shared prompts and batch API if enabled
        if USE_BATCH_API and len(resume_batch) > 1:
            logging.info(f"Using OpenAI batch API with shared prompts for {len(resume_batch)} resumes")
            results = process_batch_with_shared_prompts(resume_batch)
        else:
            # Fall back to original method for single resume or if batch API is disabled
            logging.info(f"Using individual processing for {len(resume_batch)} resumes")
            results = []
            with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                # Submit all tasks
                future_to_resume = {executor.submit(process_single_resume_two_step, resume_data): resume_data for resume_data in resume_batch}
                
                # Process results as they complete
                for future in concurrent.futures.as_completed(future_to_resume):
                    resume_data = future_to_resume[future]
                    try:
                        result = future.result()
                        results.append(result)
                        # Update progress
                        logging.info(f"Progress: {len(results)}/{len(resume_batch)} resumes completed")
                    except Exception as e:
                        userid = resume_data[0]
                        logging.error(f"Exception for UserID {userid}: {str(e)}")
        # Calculate statistics
        successful = [r for r in results if r.get('success', False)]
        failed = [r for r in results if not r.get('success', False)]
        
        if results:
            total_tokens = sum(r.get('token_count', 0) for r in results)
            total_actual_input = sum(r.get('actual_input_tokens', 0) for r in results)
            total_actual_output = sum(r.get('actual_output_tokens', 0) for r in results)

            # Calculate actual cost based on real token usage if available
            if total_actual_input > 0 and total_actual_output > 0:
                actual_input_cost = total_actual_input * 0.00000025  # $0.25 per million for GPT-5 mini
                actual_output_cost = total_actual_output * 0.000002  # $2.00 per million for GPT-5 mini
                total_cost = actual_input_cost + actual_output_cost
                logging.info(f"Actual token usage - Input: {total_actual_input:,}, Output: {total_actual_output:,}")
            else:
                total_cost = sum(r.get('cost', 0) for r in results)
            avg_processing_time = sum(r.get('processing_time', 0) for r in results if 'processing_time' in r) / len([r for r in results if 'processing_time' in r]) if any('processing_time' in r for r in results) else 0
            
            # Check if we're using batch API (may not have step1_time and step2_time in results)
            if any('step1_time' in r for r in results):
                avg_step1_time = sum(r.get('step1_time', 0) for r in results if 'step1_time' in r) / len([r for r in results if 'step1_time' in r]) if any('step1_time' in r for r in results) else 0
                avg_step2_time = sum(r.get('step2_time', 0) for r in results if 'step2_time' in r) / len([r for r in results if 'step2_time' in r]) if any('step2_time' in r for r in results) else 0
            else:
                # Using batch API, which doesn't report individual step times
                avg_step1_time = avg_processing_time / 2  # Estimate
                avg_step2_time = avg_processing_time / 2  # Estimate
        else:
            total_tokens = 0
            total_cost = 0
            avg_processing_time = 0
            avg_step1_time = 0
            avg_step2_time = 0
        
        batch_processing_time = time.time() - batch_start_time

        # Calculate time per resume to complete all processing
        per_resume_time = batch_processing_time / len(resume_batch) if resume_batch else 0
        total_elapsed = time.strftime("%H:%M:%S", time.gmtime(batch_processing_time))

        # Check if we used batch API with shared prompts
        used_batch_api = USE_BATCH_API and len(resume_batch) > 1
        
        # If we used the batch API with shared prompts, calculate the actual token savings
        if used_batch_api:
            # Get a sample resume to analyze token usage
            sample_resume = resume_batch[0][1]
            
            # Create step 1 prompt to analyze tokens
            step1_messages = create_step1_prompt(sample_resume)
            system_step1_tokens = sum(num_tokens_from_string(msg["content"]) for msg in step1_messages if msg["role"] == "system")
            
            # Estimate step 2 system tokens (typically similar to step 1)
            system_step2_tokens = system_step1_tokens * 1.2  # Step 2 is typically a bit larger
            
            # Calculate total system tokens that would be duplicated for each resume
            system_tokens_per_resume = system_step1_tokens + system_step2_tokens
            
            # Total system tokens if not shared (duplicated for each resume)
            total_system_tokens_if_not_shared = system_tokens_per_resume * len(resume_batch)
            
            # Total system tokens with sharing (only counted once + small overhead)
            total_system_tokens_shared = system_tokens_per_resume + (50 * len(resume_batch))  # 50 tokens overhead per resume
            
            # Calculate token savings from shared prompts
            token_savings = total_system_tokens_if_not_shared - total_system_tokens_shared
            cost_savings = token_savings * 0.00000025  # $0.25 per million tokens (GPT-5 mini)
            
            # Calculate percentage savings
            avg_tokens_per_resume = total_tokens / len(resume_batch)
            avg_tokens_per_resume_if_not_shared = avg_tokens_per_resume + (system_tokens_per_resume - (system_tokens_per_resume / len(resume_batch)))
            percent_reduction = (1 - (avg_tokens_per_resume / avg_tokens_per_resume_if_not_shared)) * 100
            
            logging.info(f"==== BATCH API WITH SHARED PROMPTS DETAILED ANALYSIS ====")
            logging.info(f"- System tokens per resume (not shared): {system_tokens_per_resume} tokens")
            logging.info(f"- System tokens with sharing: {system_tokens_per_resume/len(resume_batch):.1f} tokens per resume")
            logging.info(f"- Token savings from shared prompts: {int(token_savings)} tokens")
            logging.info(f"- Cost savings: ${cost_savings:.4f}")
            logging.info(f"- Per resume reduction: {percent_reduction:.1f}% fewer tokens")
            logging.info(f"- Per resume cost: ${total_cost/len(resume_batch):.6f} with sharing")
            logging.info(f"- Equivalent cost for 1000 resumes: ${(total_cost/len(resume_batch))*1000:.2f}")
            logging.info(f"=======================================================")

        # Log summary with formatted time
        logging.info(f"==== TAXONOMY-ENHANCED BATCH PROCESSING SUMMARY ====")
        logging.info(f"- Total batch time: {total_elapsed} (HH:MM:SS)")
        logging.info(f"- Resumes processed: {len(resume_batch)}")
        logging.info(f"- Processing method: {'Batch API with shared prompts' if used_batch_api else 'Individual API calls'}")
        logging.info(f"- Average time per resume: {per_resume_time:.2f} seconds")
        logging.info(f"- Time to process 1000 resumes (estimate): {(per_resume_time * 1000) / 3600:.2f} hours")
        logging.info(f"- Successfully processed: {len(successful)}/{len(resume_batch)}")
        logging.info(f"- Total estimated cost: ${total_cost:.4f}")
        logging.info(f"====================================================")

        # Log summary
        logging.info(f"Taxonomy-enhanced two-step batch processing complete:")
        logging.info(f"- Total time: {batch_processing_time:.2f} seconds")
        logging.info(f"- Average time per resume: {avg_processing_time:.2f} seconds")
        logging.info(f"- Average Step 1 time: {avg_step1_time:.2f} seconds")
        logging.info(f"- Average Step 2 time: {avg_step2_time:.2f} seconds")
        logging.info(f"- Successfully processed: {len(successful)}/{len(resume_batch)}")
        logging.info(f"- Failed: {len(failed)}/{len(resume_batch)}")
        if 'total_actual_input' in locals() and total_actual_input > 0:
            logging.info(f"- Total actual tokens - Input: {total_actual_input:,}, Output: {total_actual_output:,}")
            logging.info(f"- Total cost (actual): ${total_cost:.4f}")
        else:
            logging.info(f"- Total estimated tokens: {total_tokens}")
            logging.info(f"- Estimated total cost: ${total_cost:.4f}")
        
        # Log batch summary to error file
        error_logger.log_batch_summary(
            total_processed=len(resume_batch),
            successful=len(successful),
            failed=len(failed)
        )
        
        return {
            'total_time': batch_processing_time,
            'avg_time': avg_processing_time,
            'avg_step1_time': avg_step1_time,
            'avg_step2_time': avg_step2_time,
            'success_rate': len(successful) / len(resume_batch) if resume_batch else 0,
            'total_tokens': total_tokens,
            'total_cost': total_cost
        }
    
    except Exception as e:
        logging.error(f"Error in taxonomy-enhanced two-step batch processing: {str(e)}")
        return None


def process_single_user_by_id(user_id):
    """
    Process a single resume specified by user ID
    
    Args:
        user_id: The user ID to process
    
    Returns:
        Result of the processing operation
    """
    from resume_utils import get_resume_by_userid
    
    logging.info(f"Fetching resume for UserID: {user_id}")
    
    try:
        # Get resume for the specified user ID
        resume_data = get_resume_by_userid(user_id)
        
        if not resume_data:
            logging.error(f"No resume found for UserID: {user_id}")
            return None
        
        logging.info(f"Starting single resume processing for UserID: {user_id}")
        
        # Process the resume
        result = process_single_resume_two_step(resume_data)
        
        logging.info(f"Resume processing complete for UserID: {user_id}")
        logging.info(f"Success: {result.get('success', False)}")
        logging.info(f"Processing time: {result.get('processing_time', 0):.2f} seconds")
        logging.info(f"Estimated cost: ${result.get('cost', 0):.6f}")
        
        return result
    
    except Exception as e:
        logging.error(f"Error processing UserID {user_id}: {str(e)}")
        return None

if __name__ == "__main__":
    import argparse
    
    # Set up argument parser
    parser = argparse.ArgumentParser(description='Process resumes with two-step taxonomy approach')
    parser.add_argument('--userid', type=int, help='Process a single resume with the specified user ID')
    parser.add_argument('--batch', action='store_true', help='Run in batch mode processing multiple resumes')
    parser.add_argument('--continuous', action='store_true', help='Run continuously in batch mode')
    args = parser.parse_args()
    
    # Set up model variable
    model = MODEL
    
    # Configure logging for more detailed output
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        force=True
    )
    
    # Initialize counters for continuous processing
    batch_count = 0
    total_resumes_processed = 0
    total_cost = 0.0
    continuous_mode = args.continuous  # Use argument to determine continuous mode
    start_time = time.time()
    
    try:
        # Check if a specific user ID was provided
        if args.userid:
            # Process single resume by user ID
            logging.info(f"Processing single resume for UserID: {args.userid}")
            result = process_single_user_by_id(args.userid)
            
            if result and result.get('success', False):
                logging.info(f"Successfully processed UserID: {args.userid}")
            else:
                logging.error(f"Failed to process UserID: {args.userid}")
        
        # Run in batch mode if specified or if no user ID was provided and batch flag is set
        elif args.batch or args.continuous:
            logging.info("Starting taxonomy-enhanced resume processing in batch mode")
            
            while continuous_mode or batch_count < 1:  # Run once if not continuous
                batch_count += 1
                logging.info(f"======= STARTING BATCH #{batch_count} =======")
                
                # Run taxonomy-enhanced two-step batch
                results = run_taxonomy_enhanced_batch()
                
                if results:
                    # Track statistics
                    batch_size = int(results.get('success_rate', 0) * BATCH_SIZE)
                    total_resumes_processed += batch_size
                    total_cost += results.get('total_cost', 0)
                    
                    # Log batch completion
                    logging.info(f"Batch #{batch_count} completed in {results['total_time']:.2f}s")
                    logging.info(f"Success rate: {results['success_rate'] * 100:.1f}%")
                    logging.info(f"Average step 1 time: {results['avg_step1_time']:.2f}s, Step 2: {results['avg_step2_time']:.2f}s")
                    logging.info(f"Batch cost: ${results['total_cost']:.4f}")
                    
                    # Calculate overall runtime
                    total_runtime = time.time() - start_time
                    hours, remainder = divmod(total_runtime, 3600)
                    minutes, seconds = divmod(remainder, 60)
                    total_runtime_formatted = f"{int(hours)}:{int(minutes):02d}:{int(seconds):02d}"
                    
                    # Log cumulative statistics
                    logging.info(f"=== CUMULATIVE STATISTICS ===")
                    logging.info(f"Total runtime: {total_runtime_formatted} (HH:MM:SS)")
                    logging.info(f"Total batches processed: {batch_count}")
                    logging.info(f"Total resumes processed: {total_resumes_processed}")
                    logging.info(f"Total cost so far: ${total_cost:.4f}")
                    logging.info(f"Average cost per resume: ${total_cost/total_resumes_processed if total_resumes_processed > 0 else 0:.6f}")
                    logging.info(f"Estimated cost for 865,000 resumes: ${(total_cost/total_resumes_processed)*865000 if total_resumes_processed > 0 else 0:.2f}")
                    
                    # No resumes found, wait longer before checking again
                    if batch_size == 0:
                        logging.info("No resumes found in this batch. Waiting 5 minutes before checking again...")
                        time.sleep(300)  # 5 minutes
                    else:
                        # Short pause between batches to prevent overloading the database
                        logging.info("Pausing briefly before starting next batch...")
                        time.sleep(5)  # 5 seconds
                else:
                    logging.info("No results returned from batch processing. Waiting 5 minutes before trying again...")
                    time.sleep(300)  # 5 minutes
        else:
            # No arguments provided, show usage
            parser.print_help()
    
    except KeyboardInterrupt:
        logging.info("\nProcess interrupted by user. Shutting down...")
        
        # Calculate overall runtime
        total_runtime = time.time() - start_time
        hours, remainder = divmod(total_runtime, 3600)
        minutes, seconds = divmod(remainder, 60)
        total_runtime_formatted = f"{int(hours)}:{int(minutes):02d}:{int(seconds):02d}"
        
        logging.info(f"Final statistics:")
        logging.info(f"- Total runtime: {total_runtime_formatted} (HH:MM:SS)")
        logging.info(f"- Total batches processed: {batch_count}")
        logging.info(f"- Total resumes processed: {total_resumes_processed}")
        logging.info(f"- Total cost: ${total_cost:.4f}")
        if total_resumes_processed > 0:
            logging.info(f"- Average cost per resume: ${total_cost/total_resumes_processed:.6f}")
            logging.info(f"- Estimated cost for 865,000 resumes: ${(total_cost/total_resumes_processed)*865000:.2f}")
    
    except Exception as e:
        logging.error(f"Unexpected error in processing: {str(e)}")
    
    finally:
        logging.info("Taxonomy-enhanced processing complete. Exiting...")