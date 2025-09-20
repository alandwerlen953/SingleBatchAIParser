#!/usr/bin/env python3
"""
Test script to get the raw OpenAI API response for a single user ID
"""

import os
import sys
import json
import time
import logging
import argparse
from typing import Dict, Any, Optional
from datetime import datetime
import pyodbc
import openai
from dotenv import load_dotenv

# Add parent directory to path so we can import from the main project
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, current_dir)
sys.path.insert(0, parent_dir)

# Import our custom modules
from one_step_processor import create_unified_prompt
from direct_api_utils import create_openai_client, call_openai_with_retry

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(os.path.dirname(__file__), "user_response_test.log")),
        logging.StreamHandler()
    ]
)

# Default OpenAI model
DEFAULT_MODEL = "gpt-4o-mini"

def create_pyodbc_connection():
    """
    Create a connection to the database
    
    Returns:
        pyodbc connection object
    """
    try:
        server_ip = '172.19.115.25'
        database = 'BH_Mirror'
        username = 'silver'
        password = 'ltechmatlen'
        
        # Check for available drivers
        available_drivers = pyodbc.drivers()
        logging.info(f"Available ODBC drivers: {available_drivers}")
        
        # If no drivers available, raise error
        if not available_drivers:
            error_msg = "No ODBC drivers found on this system!"
            logging.error(error_msg)
            raise RuntimeError(error_msg)
            
        # Use first available driver
        selected_driver = available_drivers[0]
        logging.info(f"Using driver: {selected_driver}")
        
        # Build connection string
        connection_string = (
            f'DRIVER={{{selected_driver}}};'
            f'SERVER={server_ip};'
            f'DATABASE={database};'
            f'UID={username};'
            f'PWD={password}'
        )
        
        # Create connection
        conn = pyodbc.connect(connection_string, timeout=60)
        return conn
        
    except Exception as e:
        logging.error(f"Error creating database connection: {str(e)}")
        raise

def get_resume_by_userid(userid: int) -> Optional[str]:
    """
    Retrieve the resume text for a specific user ID
    
    Args:
        userid: User ID to look up
        
    Returns:
        Resume text as string or None if not found
    """
    try:
        # Connect to the database
        conn = create_pyodbc_connection()
        cursor = conn.cursor()
        
        # Query the database for this specific user
        query = """
        SELECT markdownResume
        FROM dbo.aicandidate WITH (NOLOCK)
        WHERE userid = ?
        """
        
        cursor.execute(query, userid)
        row = cursor.fetchone()
        
        if row:
            resume_text = row[0]
            logging.info(f"Retrieved resume for UserID {userid} ({len(resume_text)} chars)")
            return resume_text
        else:
            logging.error(f"No resume found for UserID {userid}")
            return None
            
    except Exception as e:
        logging.error(f"Error retrieving resume: {str(e)}")
        return None
    finally:
        try:
            cursor.close()
            conn.close()
        except:
            pass

def get_raw_api_response(userid: int, resume_text: str, model: str = DEFAULT_MODEL, save_prompts: bool = False) -> Dict[str, Any]:
    """
    Get the raw API response for a resume
    
    Args:
        userid: User ID for tracking
        resume_text: Resume text to process
        model: OpenAI model to use
        save_prompts: Whether to save the prompt messages in the response
        
    Returns:
        Dictionary with the raw response and processing info
    """
    start_time = time.time()
    client = create_openai_client()
    
    try:
        # Create unified prompt using the one_step_processor
        messages = create_unified_prompt(resume_text, userid=userid)
        
        # Log the messages for debugging
        logging.info(f"Sending {len(messages)} messages to OpenAI API")
        
        # Count message lengths
        for i, msg in enumerate(messages):
            if "content" in msg:
                logging.info(f"Message {i+1} ({msg['role']}): {len(msg['content'])} chars")
        
        # Make API call
        response = call_openai_with_retry(
            client=client,
            messages=messages,
            model=model,
            temperature=0.2,
            max_tokens=4000
        )
        
        # Get the raw response
        completion_text = response.choices[0].message.content
        
        # Log metrics
        processing_time = time.time() - start_time
        
        logging.info(f"API call successful, got {len(completion_text)} chars in {processing_time:.2f}s")
        
        # Prepare response data
        result = {
            "userid": userid,
            "resume_length": len(resume_text),
            "success": True,
            "raw_response": completion_text,
            "prompt_tokens": response.usage.prompt_tokens,
            "completion_tokens": response.usage.completion_tokens,
            "total_tokens": response.usage.total_tokens,
            "processing_time_seconds": processing_time,
        }
        
        # Include prompt messages if requested
        if save_prompts:
            # Create simplified message list with just role and content
            simplified_messages = []
            for msg in messages:
                simplified_message = {
                    "role": msg["role"],
                    "content": msg["content"]
                }
                simplified_messages.append(simplified_message)
            
            result["prompt_messages"] = simplified_messages
            logging.info(f"Saved {len(simplified_messages)} prompt messages")
        
        return result
    
    except Exception as e:
        processing_time = time.time() - start_time
        logging.error(f"Error getting API response: {str(e)}")
        return {
            "userid": userid,
            "success": False,
            "error": str(e),
            "processing_time_seconds": processing_time,
        }

def save_response_to_file(data: Dict[str, Any], userid: int) -> str:
    """
    Save the API response to a JSON file
    
    Args:
        data: API response data
        userid: User ID
        
    Returns:
        Path to the output file
    """
    # Create unique filename with timestamp
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    filename = f"user_response_{userid}_{timestamp}.json"
    file_path = os.path.join(os.path.dirname(__file__), "debug_output", filename)
    
    # Create directory if it doesn't exist
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2)
    
    logging.info(f"Saved response to {file_path}")
    return file_path

def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Get raw API response for a user ID")
    parser.add_argument("userid", type=int, help="User ID to process")
    parser.add_argument("--model", type=str, default=DEFAULT_MODEL, help="OpenAI model to use")
    parser.add_argument("--save-prompts", action="store_true", help="Save the prompt messages to the output file")
    args = parser.parse_args()
    
    userid = args.userid
    model = args.model
    save_prompts = args.save_prompts
    
    logging.info(f"Starting process for UserID {userid} with model {model}")
    
    # Get resume text
    resume_text = get_resume_by_userid(userid)
    
    if not resume_text:
        logging.error(f"Failed to get resume for UserID {userid}")
        return
    
    # Get API response
    response_data = get_raw_api_response(userid, resume_text, model, save_prompts)
    
    if not response_data["success"]:
        logging.error(f"Failed to get API response: {response_data.get('error', 'Unknown error')}")
        return
    
    # Save to file
    output_file = save_response_to_file(response_data, userid)
    
    # Print summary
    print(f"\nSuccess! Processed UserID {userid}")
    print(f"Resume length: {response_data['resume_length']} chars")
    print(f"API response: {len(response_data['raw_response'])} chars")
    print(f"Token usage: {response_data['prompt_tokens']} input, {response_data['completion_tokens']} output, {response_data['total_tokens']} total")
    print(f"Processing time: {response_data['processing_time_seconds']:.2f} seconds")
    print(f"Raw response saved to: {output_file}")
    print("\nFirst 500 characters of response:")
    print("-" * 50)
    print(response_data['raw_response'][:500])
    print("-" * 50)
    print("For the full response, check the output file.")

if __name__ == "__main__":
    main()