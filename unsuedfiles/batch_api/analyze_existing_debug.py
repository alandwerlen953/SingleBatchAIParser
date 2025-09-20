#!/usr/bin/env python3
"""
Analyze an existing debug file to examine its structure and content
"""

import os
import sys
import json
import argparse
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(os.path.dirname(__file__), "analysis.log")),
        logging.StreamHandler()
    ]
)

def load_debug_file(file_path):
    """Load and parse a debug JSON file"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return data
    except Exception as e:
        logging.error(f"Error loading debug file: {str(e)}")
        return None

def save_analysis(analysis, output_dir, userid):
    """Save analysis results to a file"""
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    filename = f"analysis_{userid}_{timestamp}.json"
    file_path = os.path.join(output_dir, filename)
    
    try:
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(analysis, f, indent=2)
        logging.info(f"Analysis saved to {file_path}")
        return file_path
    except Exception as e:
        logging.error(f"Error saving analysis: {str(e)}")
        return None

def analyze_debug_file(data):
    """Analyze the content of a debug file"""
    analysis = {
        "file_type": "unknown",
        "content_summary": {},
        "fields_found": [],
        "missing_fields": []
    }
    
    if not isinstance(data, dict):
        analysis["file_type"] = "non-dictionary"
        return analysis
    
    # Check for userid to identify file type
    if "userid" in data:
        analysis["file_type"] = "debug_response"
        analysis["userid"] = data.get("userid")
    
    # Check for success flag
    if "success" in data:
        analysis["content_summary"]["success"] = data.get("success")
    
    # Check for parsed data
    if "parsed_data" in data and isinstance(data["parsed_data"], dict):
        analysis["content_summary"]["parsed_data_fields"] = len(data["parsed_data"])
        analysis["fields_found"] = list(data["parsed_data"].keys())
        
        # Check for missing common fields
        common_fields = [
            "PrimaryTitle", "SecondaryTitle", "TertiaryTitle",
            "FirstName", "MiddleName", "LastName",
            "Email", "Phone1",
            "MostRecentCompany", "MostRecentStartDate", "MostRecentEndDate",
            "PrimaryIndustry", "SecondaryIndustry",
            "PrimarySoftwareLanguage", "SecondarySoftwareLanguage",
            "SoftwareApp1", "Hardware1",
            "PrimaryCategory", "SecondaryCategory",
            "Summary", "YearsofExperience"
        ]
        
        analysis["missing_fields"] = [field for field in common_fields if field not in data["parsed_data"]]
    
    # Check for metrics
    if "metrics" in data and isinstance(data["metrics"], dict):
        analysis["content_summary"]["metrics"] = {
            "model": data["metrics"].get("model"),
            "token_counts": {
                "prompt_tokens": data["metrics"].get("prompt_tokens"),
                "completion_tokens": data["metrics"].get("completion_tokens"),
                "total_tokens": data["metrics"].get("total_tokens")
            },
            "processing_time": data["metrics"].get("processing_time_seconds"),
            "cost": data["metrics"].get("cost", {}).get("total_cost")
        }
    
    # Check for raw response
    if "raw_response" in data:
        analysis["content_summary"]["has_raw_response"] = True
        analysis["content_summary"]["raw_response_length"] = len(data["raw_response"])
        
        # Extract first 500 chars of response for preview
        analysis["content_summary"]["raw_response_preview"] = data["raw_response"][:500]
    else:
        analysis["content_summary"]["has_raw_response"] = False
    
    return analysis

def main():
    parser = argparse.ArgumentParser(description="Analyze an existing debug file")
    parser.add_argument("file", help="Path to the debug JSON file")
    parser.add_argument("--output-dir", help="Directory to save analysis results", 
                       default=os.path.join(os.path.dirname(__file__), "debug_output"))
    args = parser.parse_args()
    
    # Create output directory if it doesn't exist
    os.makedirs(args.output_dir, exist_ok=True)
    
    # Load the debug file
    logging.info(f"Loading debug file: {args.file}")
    data = load_debug_file(args.file)
    
    if not data:
        logging.error("Failed to load debug file")
        return
    
    # Analyze the file contents
    logging.info("Analyzing debug file content")
    analysis = analyze_debug_file(data)
    
    # Save the analysis
    userid = analysis.get("userid", "unknown")
    output_file = save_analysis(analysis, args.output_dir, userid)
    
    if output_file:
        logging.info(f"Analysis complete, saved to {output_file}")
    
    # Print a summary of the analysis
    print("\nDebug File Analysis Summary:")
    print(f"File type: {analysis['file_type']}")
    
    if "userid" in analysis:
        print(f"User ID: {analysis['userid']}")
    
    if "content_summary" in analysis:
        summary = analysis["content_summary"]
        
        if "success" in summary:
            print(f"Success: {summary['success']}")
        
        if "parsed_data_fields" in summary:
            print(f"Fields extracted: {summary['parsed_data_fields']}")
        
        if "metrics" in summary and summary["metrics"]:
            metrics = summary["metrics"]
            print(f"Model used: {metrics.get('model', 'unknown')}")
            
            if "token_counts" in metrics:
                token_counts = metrics["token_counts"]
                print(f"Token usage: {token_counts.get('total_tokens')} total tokens")
            
            if "processing_time" in metrics:
                print(f"Processing time: {metrics.get('processing_time'):.2f} seconds")
            
            if "cost" in metrics:
                print(f"API cost: ${metrics.get('cost'):.5f}")
        
        if "has_raw_response" in summary:
            if summary["has_raw_response"]:
                print(f"Raw response: Present ({summary.get('raw_response_length', 0)} chars)")
            else:
                print("Raw response: Not present")
    
    print("\nFields found:", len(analysis.get("fields_found", [])))
    for field in sorted(analysis.get("fields_found", []))[:10]:  # Show first 10
        print(f"  - {field}")
    
    if len(analysis.get("fields_found", [])) > 10:
        print(f"  ... and {len(analysis.get('fields_found', [])) - 10} more")
    
    print("\nMissing common fields:", len(analysis.get("missing_fields", [])))
    for field in sorted(analysis.get("missing_fields", [])):
        print(f"  - {field}")
    
    # If the original file has parsed_data, print some important fields
    if data and "parsed_data" in data:
        print("\nImportant field values:")
        
        fields_to_show = [
            "PrimaryTitle", "PrimaryCategory", "PrimarySoftwareLanguage",
            "MostRecentCompany", "YearsofExperience"
        ]
        
        for field in fields_to_show:
            if field in data["parsed_data"]:
                print(f"  {field}: {data['parsed_data'][field]}")
        
        if "Summary" in data["parsed_data"]:
            print("\nCandidate Summary:")
            summary = data["parsed_data"]["Summary"]
            print(f"  {summary}")

if __name__ == "__main__":
    main()