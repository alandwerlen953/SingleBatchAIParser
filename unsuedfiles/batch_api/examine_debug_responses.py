#!/usr/bin/env python3
"""
Examine the debug response JSON files in the debug_output directory
"""

import os
import sys
import json
import glob
import argparse
import logging
from datetime import datetime
from typing import Dict, List, Any

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(os.path.dirname(__file__), "examination.log")),
        logging.StreamHandler()
    ]
)

def find_debug_files(directory: str) -> List[str]:
    """Find all debug JSON files in the given directory"""
    pattern = os.path.join(directory, "debug_*.json")
    return glob.glob(pattern)

def load_debug_file(file_path: str) -> Dict[str, Any]:
    """Load and parse a debug JSON file"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return data
    except Exception as e:
        logging.error(f"Error loading debug file {os.path.basename(file_path)}: {str(e)}")
        return {}

def extract_userid_from_filename(filename: str) -> str:
    """Extract the user ID from the filename"""
    try:
        # Assuming format: debug_TIMESTAMP_USERID.json
        parts = os.path.basename(filename).split('_')
        if len(parts) >= 3:
            return parts[2].split('.')[0]  # Get the part before .json
        return "unknown"
    except Exception:
        return "unknown"

def examine_debug_file(file_path: str) -> Dict[str, Any]:
    """Examine a debug file and return a summary"""
    data = load_debug_file(file_path)
    
    if not data:
        return {
            "file_path": file_path,
            "file_name": os.path.basename(file_path),
            "userid": extract_userid_from_filename(file_path),
            "error": "Failed to load file"
        }
    
    summary = {
        "file_path": file_path,
        "file_name": os.path.basename(file_path),
        "userid": data.get("userid", extract_userid_from_filename(file_path)),
        "success": data.get("success", False),
        "size_bytes": os.path.getsize(file_path)
    }
    
    # Check for parsed data
    if "parsed_data" in data and isinstance(data["parsed_data"], dict):
        parsed_data = data["parsed_data"]
        summary["parsed_fields_count"] = len(parsed_data)
        
        # Include some key fields
        key_fields = [
            "PrimaryTitle", "SecondaryTitle", "TertiaryTitle",
            "FirstName", "LastName",
            "MostRecentCompany", 
            "PrimaryIndustry",
            "PrimaryCategory",
            "PrimarySoftwareLanguage"
        ]
        
        summary["key_fields"] = {}
        for field in key_fields:
            if field in parsed_data:
                summary["key_fields"][field] = parsed_data[field]
    
    # Include metrics if available
    if "metrics" in data and isinstance(data["metrics"], dict):
        metrics = data["metrics"]
        summary["model"] = metrics.get("model", "unknown")
        summary["tokens"] = {
            "prompt": metrics.get("prompt_tokens", 0),
            "completion": metrics.get("completion_tokens", 0),
            "total": metrics.get("total_tokens", 0)
        }
        summary["processing_time"] = metrics.get("processing_time_seconds", 0)
        
        if "cost" in metrics and isinstance(metrics["cost"], dict):
            summary["cost"] = metrics["cost"].get("total_cost", 0)
    
    # Check if raw response is included
    if "raw_response" in data:
        summary["has_raw_response"] = True
        summary["raw_response_length"] = len(data["raw_response"])
    else:
        summary["has_raw_response"] = False
    
    return summary

def main():
    parser = argparse.ArgumentParser(description="Examine debug response JSON files")
    parser.add_argument("--directory", default="debug_output", 
                       help="Directory containing debug files")
    parser.add_argument("--userid", help="Filter by user ID")
    parser.add_argument("--output", help="Output file for results")
    args = parser.parse_args()
    
    # Find all debug files
    debug_dir = os.path.join(os.path.dirname(__file__), args.directory)
    debug_files = find_debug_files(debug_dir)
    
    if not debug_files:
        logging.error(f"No debug files found in {debug_dir}")
        return
    
    logging.info(f"Found {len(debug_files)} debug files in {debug_dir}")
    
    # Examine each file
    results = []
    for file_path in debug_files:
        # Skip if filtering by userid and this file doesn't match
        if args.userid:
            userid = extract_userid_from_filename(file_path)
            if userid != args.userid:
                continue
        
        result = examine_debug_file(file_path)
        results.append(result)
    
    # Sort results by userid
    results.sort(key=lambda x: x.get("userid", "unknown"))
    
    # Save results if output file specified
    if args.output:
        output_path = args.output
        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(results, f, indent=2)
            logging.info(f"Results saved to {output_path}")
        except Exception as e:
            logging.error(f"Error saving results: {str(e)}")
    
    # Print a summary
    print(f"\nExamined {len(results)} debug files:")
    
    for result in results:
        print(f"\nFile: {result['file_name']}")
        print(f"User ID: {result['userid']}")
        print(f"Success: {result.get('success', 'unknown')}")
        
        if "parsed_fields_count" in result:
            print(f"Fields extracted: {result['parsed_fields_count']}")
        
        if "model" in result:
            print(f"Model: {result['model']}")
        
        if "tokens" in result:
            tokens = result["tokens"]
            print(f"Tokens: {tokens.get('total', 0)} ({tokens.get('prompt', 0)} prompt, {tokens.get('completion', 0)} completion)")
        
        if "processing_time" in result:
            print(f"Processing time: {result['processing_time']:.2f} seconds")
        
        if "cost" in result:
            print(f"Cost: ${result['cost']:.5f}")
        
        if "key_fields" in result and result["key_fields"]:
            print("\nKey fields:")
            for field, value in result["key_fields"].items():
                print(f"  {field}: {value}")
        
        if "has_raw_response" in result:
            if result["has_raw_response"]:
                print(f"Raw response: Present ({result.get('raw_response_length', 0)} chars)")
            else:
                print("Raw response: Not present")

if __name__ == "__main__":
    main()