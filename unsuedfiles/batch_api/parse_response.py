#!/usr/bin/env python3
"""
Parse and analyze a raw OpenAI API response into a structured format
"""

import os
import sys
import json
import re
import logging
import argparse
from typing import Dict, Any, List, Optional
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(os.path.dirname(__file__), "response_parser.log")),
        logging.StreamHandler()
    ]
)

def extract_fields_from_response(response_text: str) -> Dict[str, str]:
    """
    Extract fields from the raw API response
    
    Args:
        response_text: Raw API response text
        
    Returns:
        Dictionary of field names and values
    """
    extracted = {}
    
    # Split the response into lines
    lines = response_text.strip().split('\n')
    
    # Process each line
    for line in lines:
        line = line.strip()
        if not line or not line.startswith('-'):
            continue
        
        # Extract the question and answer parts
        parts = line.split(':', 1)  # Split only on the first colon
        if len(parts) != 2:
            continue
            
        question = parts[0].strip('- \t')
        answer = parts[1].strip()
        
        # Skip empty answers
        if not answer or answer.upper() == 'NULL':
            answer = 'NULL'
            
        extracted[question] = answer
    
    return extracted

def map_fields_to_db_structure(extracted: Dict[str, str]) -> Dict[str, Any]:
    """
    Map extracted fields to the database structure
    
    Args:
        extracted: Dictionary of extracted fields
        
    Returns:
        Dictionary with fields organized by category
    """
    # Initialize result structure
    result = {
        "PERSONAL_INFORMATION": {
            "Name": {
                "FirstName": "NULL",
                "MiddleName": "NULL",
                "LastName": "NULL"
            },
            "Contact": {
                "Phone": "NULL",
                "SecondaryPhone": "NULL",
                "Email": "NULL",
                "SecondaryEmail": "NULL",
                "LinkedIn": "NULL",
                "Address": "NULL",
                "City": "NULL",
                "State": "NULL"
            },
            "Education": {
                "BachelorsDegree": "NULL",
                "MastersDegree": "NULL",
                "Certifications": "NULL"
            }
        },
        "WORK_HISTORY": {
            "MostRecent": {
                "Company": "NULL",
                "StartDate": "NULL",
                "EndDate": "NULL",
                "Location": "NULL"
            },
            "SecondMostRecent": {
                "Company": "NULL",
                "StartDate": "NULL",
                "EndDate": "NULL",
                "Location": "NULL"
            },
            "ThirdMostRecent": {
                "Company": "NULL",
                "StartDate": "NULL",
                "EndDate": "NULL",
                "Location": "NULL"
            },
            "FourthMostRecent": {
                "Company": "NULL",
                "StartDate": "NULL",
                "EndDate": "NULL",
                "Location": "NULL"
            },
            "FifthMostRecent": {
                "Company": "NULL",
                "StartDate": "NULL",
                "EndDate": "NULL",
                "Location": "NULL"
            },
            "SixthMostRecent": {
                "Company": "NULL",
                "StartDate": "NULL",
                "EndDate": "NULL",
                "Location": "NULL"
            },
            "SeventhMostRecent": {
                "Company": "NULL",
                "StartDate": "NULL",
                "EndDate": "NULL",
                "Location": "NULL"
            }
        },
        "CAREER_INFO": {
            "PrimaryTitle": "NULL",
            "SecondaryTitle": "NULL",
            "TertiaryTitle": "NULL",
            "PrimaryIndustry": "NULL",
            "SecondaryIndustry": "NULL",
            "TopSkills": "NULL"
        },
        "TECHNICAL_INFO": {
            "PrimaryLanguage": "NULL",
            "SecondaryLanguage": "NULL",
            "TertiaryLanguage": "NULL",
            "SoftwareApp1": "NULL",
            "SoftwareApp2": "NULL",
            "SoftwareApp3": "NULL",
            "SoftwareApp4": "NULL",
            "SoftwareApp5": "NULL",
            "Hardware1": "NULL",
            "Hardware2": "NULL",
            "Hardware3": "NULL",
            "Hardware4": "NULL",
            "Hardware5": "NULL",
            "PrimaryCategory": "NULL",
            "SecondaryCategory": "NULL",
            "ProjectTypes": "NULL",
            "Specialty": "NULL",
            "Summary": "NULL",
            "LengthInUS": "NULL",
            "YearsOfExperience": "NULL",
            "AvgTenure": "NULL"
        }
    }
    
    # Map personal information
    field_mapping = {
        # Personal information
        "Their First Name": "PERSONAL_INFORMATION.Name.FirstName",
        "First Name": "PERSONAL_INFORMATION.Name.FirstName",
        "Their Middle Name": "PERSONAL_INFORMATION.Name.MiddleName",
        "Middle Name": "PERSONAL_INFORMATION.Name.MiddleName",
        "Their Last Name": "PERSONAL_INFORMATION.Name.LastName",
        "Last Name": "PERSONAL_INFORMATION.Name.LastName",
        "Their Phone Number": "PERSONAL_INFORMATION.Contact.Phone",
        "Phone Number": "PERSONAL_INFORMATION.Contact.Phone",
        "Their Second Phone Number": "PERSONAL_INFORMATION.Contact.SecondaryPhone",
        "Second Phone Number": "PERSONAL_INFORMATION.Contact.SecondaryPhone",
        "Their Email": "PERSONAL_INFORMATION.Contact.Email",
        "Email": "PERSONAL_INFORMATION.Contact.Email",
        "Their Second Email": "PERSONAL_INFORMATION.Contact.SecondaryEmail",
        "Second Email": "PERSONAL_INFORMATION.Contact.SecondaryEmail",
        "Their Linkedin URL": "PERSONAL_INFORMATION.Contact.LinkedIn",
        "LinkedIn URL": "PERSONAL_INFORMATION.Contact.LinkedIn",
        "Their street address": "PERSONAL_INFORMATION.Contact.Address",
        "Street Address": "PERSONAL_INFORMATION.Contact.Address",
        "Their City": "PERSONAL_INFORMATION.Contact.City",
        "City": "PERSONAL_INFORMATION.Contact.City",
        "Their State": "PERSONAL_INFORMATION.Contact.State",
        "State": "PERSONAL_INFORMATION.Contact.State",
        "Their Bachelor's Degree": "PERSONAL_INFORMATION.Education.BachelorsDegree",
        "Bachelor's Degree": "PERSONAL_INFORMATION.Education.BachelorsDegree",
        "Their Master's Degree": "PERSONAL_INFORMATION.Education.MastersDegree",
        "Master's Degree": "PERSONAL_INFORMATION.Education.MastersDegree",
        "Their Certifications Listed": "PERSONAL_INFORMATION.Education.Certifications",
        "Certifications": "PERSONAL_INFORMATION.Education.Certifications",
        
        # Jobs
        "Most Recent Company Worked for": "WORK_HISTORY.MostRecent.Company",
        "Most Recent Company": "WORK_HISTORY.MostRecent.Company",
        "Most Recent Start Date (YYYY-MM-DD)": "WORK_HISTORY.MostRecent.StartDate",
        "Most Recent Start Date": "WORK_HISTORY.MostRecent.StartDate",
        "Most Recent End Date (YYYY-MM-DD)": "WORK_HISTORY.MostRecent.EndDate",
        "Most Recent End Date": "WORK_HISTORY.MostRecent.EndDate",
        "Most Recent Job Location": "WORK_HISTORY.MostRecent.Location",
        
        "Second Most Recent Company Worked for": "WORK_HISTORY.SecondMostRecent.Company",
        "Second Most Recent Company": "WORK_HISTORY.SecondMostRecent.Company",
        "Second Most Recent Start Date (YYYY-MM-DD)": "WORK_HISTORY.SecondMostRecent.StartDate",
        "Second Most Recent Start Date": "WORK_HISTORY.SecondMostRecent.StartDate",
        "Second Most Recent End Date (YYYY-MM-DD)": "WORK_HISTORY.SecondMostRecent.EndDate",
        "Second Most Recent End Date": "WORK_HISTORY.SecondMostRecent.EndDate",
        "Second Most Recent Job Location": "WORK_HISTORY.SecondMostRecent.Location",
        
        "Third Most Recent Company Worked for": "WORK_HISTORY.ThirdMostRecent.Company",
        "Third Most Recent Company": "WORK_HISTORY.ThirdMostRecent.Company",
        "Third Most Recent Start Date (YYYY-MM-DD)": "WORK_HISTORY.ThirdMostRecent.StartDate",
        "Third Most Recent Start Date": "WORK_HISTORY.ThirdMostRecent.StartDate", 
        "Third Most Recent End Date (YYYY-MM-DD)": "WORK_HISTORY.ThirdMostRecent.EndDate",
        "Third Most Recent End Date": "WORK_HISTORY.ThirdMostRecent.EndDate",
        "Third Most Recent Job Location": "WORK_HISTORY.ThirdMostRecent.Location",
        
        "Fourth Most Recent Company Worked for": "WORK_HISTORY.FourthMostRecent.Company",
        "Fourth Most Recent Company": "WORK_HISTORY.FourthMostRecent.Company",
        "Fourth Most Recent Start Date (YYYY-MM-DD)": "WORK_HISTORY.FourthMostRecent.StartDate",
        "Fourth Most Recent Start Date": "WORK_HISTORY.FourthMostRecent.StartDate",
        "Fourth Most Recent End Date (YYYY-MM-DD)": "WORK_HISTORY.FourthMostRecent.EndDate",
        "Fourth Most Recent End Date": "WORK_HISTORY.FourthMostRecent.EndDate",
        "Fourth Most Recent Job Location": "WORK_HISTORY.FourthMostRecent.Location",
        
        "Fifth Most Recent Company Worked for": "WORK_HISTORY.FifthMostRecent.Company",
        "Fifth Most Recent Company": "WORK_HISTORY.FifthMostRecent.Company",
        "Fifth Most Recent Start Date (YYYY-MM-DD)": "WORK_HISTORY.FifthMostRecent.StartDate",
        "Fifth Most Recent Start Date": "WORK_HISTORY.FifthMostRecent.StartDate",
        "Fifth Most Recent End Date (YYYY-MM-DD)": "WORK_HISTORY.FifthMostRecent.EndDate",
        "Fifth Most Recent End Date": "WORK_HISTORY.FifthMostRecent.EndDate",
        "Fifth Most Recent Job Location": "WORK_HISTORY.FifthMostRecent.Location",
        
        "Sixth Most Recent Company Worked for": "WORK_HISTORY.SixthMostRecent.Company",
        "Sixth Most Recent Company": "WORK_HISTORY.SixthMostRecent.Company",
        "Sixth Most Recent Start Date (YYYY-MM-DD)": "WORK_HISTORY.SixthMostRecent.StartDate",
        "Sixth Most Recent Start Date": "WORK_HISTORY.SixthMostRecent.StartDate",
        "Sixth Most Recent End Date (YYYY-MM-DD)": "WORK_HISTORY.SixthMostRecent.EndDate",
        "Sixth Most Recent End Date": "WORK_HISTORY.SixthMostRecent.EndDate",
        "Sixth Most Recent Job Location": "WORK_HISTORY.SixthMostRecent.Location",
        
        "Seventh Most Recent Company Worked for": "WORK_HISTORY.SeventhMostRecent.Company",
        "Seventh Most Recent Company": "WORK_HISTORY.SeventhMostRecent.Company",
        "Seventh Most Recent Start Date (YYYY-MM-DD)": "WORK_HISTORY.SeventhMostRecent.StartDate",
        "Seventh Most Recent Start Date": "WORK_HISTORY.SeventhMostRecent.StartDate",
        "Seventh Most Recent End Date (YYYY-MM-DD)": "WORK_HISTORY.SeventhMostRecent.EndDate",
        "Seventh Most Recent End Date": "WORK_HISTORY.SeventhMostRecent.EndDate",
        "Seventh Most Recent Job Location": "WORK_HISTORY.SeventhMostRecent.Location",
        
        # Career info
        "Best job title that fit their primary experience": "CAREER_INFO.PrimaryTitle",
        "Best job title that fits their primary experience": "CAREER_INFO.PrimaryTitle",
        "Primary Job Title": "CAREER_INFO.PrimaryTitle",
        
        "Best secondary job title that fits their secondary experience": "CAREER_INFO.SecondaryTitle",
        "Secondary Job Title": "CAREER_INFO.SecondaryTitle",
        
        "Best tertiary job title that fits their tertiary experience": "CAREER_INFO.TertiaryTitle",
        "Tertiary Job Title": "CAREER_INFO.TertiaryTitle",
        
        "Based on all 7 of their most recent companies above, what is the Primary industry they work in": "CAREER_INFO.PrimaryIndustry",
        "Primary Industry": "CAREER_INFO.PrimaryIndustry",
        
        "Based on all 7 of their most recent companies above, what is the Secondary industry they work in": "CAREER_INFO.SecondaryIndustry",
        "Secondary Industry": "CAREER_INFO.SecondaryIndustry",
        
        "Top 10 Technical Skills": "CAREER_INFO.TopSkills",
        
        # Technical info
        "What technical language do they use most often?": "TECHNICAL_INFO.PrimaryLanguage",
        "What technical language do they use most often": "TECHNICAL_INFO.PrimaryLanguage",
        
        "What technical language do they use second most often?": "TECHNICAL_INFO.SecondaryLanguage",
        "What technical language do they use second most often": "TECHNICAL_INFO.SecondaryLanguage",
        
        "What technical language do they use third most often?": "TECHNICAL_INFO.TertiaryLanguage",
        "What technical language do they use third most often": "TECHNICAL_INFO.TertiaryLanguage",
        
        "What software do they talk about using the most?": "TECHNICAL_INFO.SoftwareApp1",
        "What software do they talk about using the most": "TECHNICAL_INFO.SoftwareApp1",
        
        "What software do they talk about using the second most?": "TECHNICAL_INFO.SoftwareApp2",
        "What software do they talk about using the second most": "TECHNICAL_INFO.SoftwareApp2",
        
        "What software do they talk about using the third most?": "TECHNICAL_INFO.SoftwareApp3",
        "What software do they talk about using the third most": "TECHNICAL_INFO.SoftwareApp3",
        
        "What software do they talk about using the fourth most?": "TECHNICAL_INFO.SoftwareApp4",
        "What software do they talk about using the fourth most": "TECHNICAL_INFO.SoftwareApp4",
        
        "What software do they talk about using the fifth most?": "TECHNICAL_INFO.SoftwareApp5",
        "What software do they talk about using the fifth most": "TECHNICAL_INFO.SoftwareApp5",
        
        "What physical hardware do they talk about using the most?": "TECHNICAL_INFO.Hardware1",
        "What physical hardware do they talk about using the most": "TECHNICAL_INFO.Hardware1",
        
        "What physical hardware do they talk about using the second most?": "TECHNICAL_INFO.Hardware2",
        "What physical hardware do they talk about using the second most": "TECHNICAL_INFO.Hardware2",
        
        "What physical hardware do they talk about using the third most?": "TECHNICAL_INFO.Hardware3",
        "What physical hardware do they talk about using the third most": "TECHNICAL_INFO.Hardware3",
        
        "What physical hardware do they talk about using the fourth most?": "TECHNICAL_INFO.Hardware4",
        "What physical hardware do they talk about using the fourth most": "TECHNICAL_INFO.Hardware4",
        
        "What physical hardware do they talk about using the fifth most?": "TECHNICAL_INFO.Hardware5",
        "What physical hardware do they talk about using the fifth most": "TECHNICAL_INFO.Hardware5",
        
        "Based on their experience, put them in a primary technical category if they are technical or functional category if they are functional": "TECHNICAL_INFO.PrimaryCategory",
        
        "Based on their experience, put them in a subsidiary technical category if they are technical or functional category if they are functional": "TECHNICAL_INFO.SecondaryCategory",
        
        "Types of projects they have worked on": "TECHNICAL_INFO.ProjectTypes",
        
        "Based on their skills, categories, certifications, and industries, determine what they specialize in": "TECHNICAL_INFO.Specialty",
        
        "Based on all this knowledge, write a summary of this candidate that could be sellable to an employer": "TECHNICAL_INFO.Summary",
        
        "How long have they lived in the United States(numerical answer only)": "TECHNICAL_INFO.LengthInUS",
        
        "Total years of professional experience (numerical answer only)": "TECHNICAL_INFO.YearsOfExperience",
        
        "Average tenure at companies in years (numerical answer only)": "TECHNICAL_INFO.AvgTenure"
    }
    
    # Apply the mapping
    for field, value in extracted.items():
        if field in field_mapping:
            path = field_mapping[field].split('.')
            
            # Navigate to the correct part of the structure
            current = result
            for part in path[:-1]:
                current = current[part]
                
            # Set the value
            current[path[-1]] = value
    
    return result

def analyze_response(response_text: str) -> Dict[str, Any]:
    """
    Analyze a raw API response text
    
    Args:
        response_text: Raw API response text
        
    Returns:
        Dictionary with analysis results
    """
    # Extract fields from the response
    extracted_fields = extract_fields_from_response(response_text)
    
    # Map to structured output
    structured_data = map_fields_to_db_structure(extracted_fields)
    
    # Analyze which fields have values
    field_analysis = {
        "total_fields": len(extracted_fields),
        "fields_with_values": len([f for f in extracted_fields.values() if f != "NULL"]),
        "fields_missing": len([f for f in extracted_fields.values() if f == "NULL"]),
        "extraction_rate": 0.0  # Will calculate below
    }
    
    if field_analysis["total_fields"] > 0:
        field_analysis["extraction_rate"] = (field_analysis["fields_with_values"] / field_analysis["total_fields"]) * 100
    
    # Count fields by category
    category_counts = {
        "PERSONAL_INFORMATION": count_filled_fields(structured_data["PERSONAL_INFORMATION"]),
        "WORK_HISTORY": count_filled_fields(structured_data["WORK_HISTORY"]),
        "CAREER_INFO": count_filled_fields(structured_data["CAREER_INFO"]),
        "TECHNICAL_INFO": count_filled_fields(structured_data["TECHNICAL_INFO"])
    }
    
    result = {
        "structured_data": structured_data,
        "field_analysis": field_analysis,
        "category_counts": category_counts,
        "raw_extraction": extracted_fields
    }
    
    return result

def count_filled_fields(data, prefix="") -> Dict[str, int]:
    """
    Count how many fields have values in a nested structure
    
    Args:
        data: The nested data structure
        prefix: Field prefix for recursion
        
    Returns:
        Dictionary with field counts
    """
    result = {
        "total": 0,
        "filled": 0,
        "fields": []
    }
    
    if isinstance(data, dict):
        for key, value in data.items():
            field_name = f"{prefix}.{key}" if prefix else key
            
            if isinstance(value, dict):
                # Recurse for nested dictionaries
                sub_counts = count_filled_fields(value, field_name)
                result["total"] += sub_counts["total"]
                result["filled"] += sub_counts["filled"]
                result["fields"].extend(sub_counts["fields"])
            else:
                # Count leaf nodes
                result["total"] += 1
                if value != "NULL":
                    result["filled"] += 1
                    result["fields"].append(field_name)
    
    return result

def load_response_from_file(file_path: str) -> Optional[Dict[str, Any]]:
    """
    Load response data from a JSON file
    
    Args:
        file_path: Path to the JSON file
        
    Returns:
        Dictionary with the loaded data or None if error
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        return data
    except Exception as e:
        logging.error(f"Error loading response file: {str(e)}")
        return None

def save_analysis_to_file(analysis: Dict[str, Any], input_path: str) -> str:
    """
    Save analysis results to a JSON file
    
    Args:
        analysis: Analysis results
        input_path: Path to the input file (used for naming)
        
    Returns:
        Path to the output file
    """
    # Create filename based on the input path
    basename = os.path.basename(input_path)
    filename = f"analysis_{basename}"
    file_path = os.path.join(os.path.dirname(__file__), "debug_output", filename)
    
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(analysis, f, indent=2)
    
    logging.info(f"Saved analysis to {file_path}")
    return file_path

def print_analysis_summary(analysis: Dict[str, Any]):
    """
    Print a summary of the analysis results
    
    Args:
        analysis: Analysis results
    """
    # Print field statistics
    field_analysis = analysis["field_analysis"]
    print(f"\nField Extraction Analysis:")
    print(f"  Total fields: {field_analysis['total_fields']}")
    print(f"  Fields with values: {field_analysis['fields_with_values']} ({field_analysis['extraction_rate']:.1f}%)")
    print(f"  Fields without values: {field_analysis['fields_missing']}")
    
    # Print category statistics
    category_counts = analysis["category_counts"]
    print(f"\nCategory Extraction Rates:")
    for category, counts in category_counts.items():
        if counts["total"] > 0:
            rate = (counts["filled"] / counts["total"]) * 100
            print(f"  {category}: {counts['filled']}/{counts['total']} fields filled ({rate:.1f}%)")
    
    # Print some key fields
    data = analysis["structured_data"]
    print(f"\nKey Fields:")
    print(f"  Name: {data['PERSONAL_INFORMATION']['Name']['FirstName']} {data['PERSONAL_INFORMATION']['Name']['LastName']}")
    print(f"  Primary Title: {data['CAREER_INFO']['PrimaryTitle']}")
    print(f"  Primary Industry: {data['CAREER_INFO']['PrimaryIndustry']}")
    print(f"  Primary Language: {data['TECHNICAL_INFO']['PrimaryLanguage']}")
    print(f"  Current Company: {data['WORK_HISTORY']['MostRecent']['Company']}")
    print(f"  Experience: {data['TECHNICAL_INFO']['YearsOfExperience']} years")
    
    # Print summary from the response if available
    if data['TECHNICAL_INFO']['Summary'] != "NULL":
        print(f"\nCandidate Summary:")
        summary = data['TECHNICAL_INFO']['Summary']
        print(f"  {summary}")

def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Parse and analyze a raw OpenAI API response")
    parser.add_argument("file", help="Path to the JSON file with the API response")
    args = parser.parse_args()
    
    # Load the response file
    response_data = load_response_from_file(args.file)
    
    if not response_data or "raw_response" not in response_data:
        logging.error("Failed to load response data from file")
        return
    
    # Analyze the response
    analysis = analyze_response(response_data["raw_response"])
    
    # Add metadata to the analysis
    analysis["metadata"] = {
        "userid": response_data.get("userid", "unknown"),
        "processing_time": response_data.get("processing_time_seconds", 0),
        "prompt_tokens": response_data.get("prompt_tokens", 0),
        "completion_tokens": response_data.get("completion_tokens", 0),
        "total_tokens": response_data.get("total_tokens", 0),
        "input_file": args.file,
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    
    # Save the analysis to a file
    output_file = save_analysis_to_file(analysis, args.file)
    
    # Print summary
    print(f"\nAnalysis complete!")
    print(f"Analyzed response for UserID {analysis['metadata']['userid']}")
    print(f"Analysis saved to: {output_file}")
    
    # Print detailed summary
    print_analysis_summary(analysis)

if __name__ == "__main__":
    main()