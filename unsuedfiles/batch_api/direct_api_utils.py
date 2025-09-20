"""
Utility functions for direct OpenAI API processing without external dependencies

This module replaces batch_api_utils.py, implementing direct synchronous API calls
instead of batch processing, while maintaining similar functionality and interfaces.
"""

import os
import logging
import time
import json
import sys
from datetime import datetime, timedelta
import pyodbc
import tiktoken
from openai import OpenAI
from openai.types.chat import ChatCompletion
from openai.types.chat.chat_completion import Choice
from openai import RateLimitError, APIError, APIConnectionError
from tenacity import retry, wait_random_exponential, stop_after_attempt, retry_if_exception_type
from dotenv import load_dotenv
import concurrent.futures
from typing import Dict, List, Tuple, Any, Optional, Union
import random

# Add parent directory to path so we can import from the main project
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)

# Import our robust db connection function
try:
    from db_connection import create_pyodbc_connection
    logging.info("Successfully imported create_pyodbc_connection")
except ImportError as e:
    logging.error(f"Failed to import db_connection module: {e}")
    # Define a fallback function
    def create_pyodbc_connection():
        logging.warning("Using fallback connection function")
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

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(os.path.dirname(__file__), "direct_api.log")),
        logging.StreamHandler()
    ]
)

# Default model and configuration
DEFAULT_MODEL = "gpt-4o-mini"
MAX_TOKENS = 16000
DEFAULT_TEMPERATURE = 0

# OpenAI API client initialization
def create_openai_client():
    """
    Creates and returns an OpenAI client with API key from environment
    
    Returns:
        OpenAI client instance
    """
    api_key = os.getenv('OPENAI_API_KEY')
    if not api_key:
        error_msg = "API key is not set in the environment variables."
        logging.error(error_msg)
        raise ValueError(error_msg)
    
    return OpenAI(api_key=api_key)

# Token encoding
def num_tokens_from_string(string, encoding_name="cl100k_base"):
    """Returns the number of tokens in a text string."""
    try:
        # Try to get encoding for the model first
        try:
            encoding = tiktoken.encoding_for_model(DEFAULT_MODEL)
        except KeyError:
            # If that fails, use the explicit get_encoding method
            encoding = tiktoken.get_encoding(encoding_name)
        
        num_tokens = len(encoding.encode(string))
        return num_tokens
    except Exception as e:
        logging.error(f"Error counting tokens: {str(e)}")
        # Return an estimate if token counting fails (average 4 characters per token)
        return len(string) // 4

def apply_token_truncation(messages, max_input_tokens=128000):
    """Truncates the messages if they exceed the token limit."""
    # Calculate current tokens
    total_tokens = 0
    for message in messages:
        if isinstance(message, dict) and "content" in message:
            total_tokens += num_tokens_from_string(message["content"])
    
    # If under limit, return as is
    if total_tokens <= max_input_tokens:
        return messages
    
    # If over limit, truncate the user content (usually the resume)
    truncated_messages = messages.copy()
    for i, message in enumerate(truncated_messages):
        if message["role"] == "user" and "content" in message:
            # Calculate how many tokens to keep
            user_tokens = num_tokens_from_string(message["content"])
            tokens_to_remove = total_tokens - max_input_tokens
            
            if tokens_to_remove >= user_tokens:
                # Extreme case - just keep minimal text
                truncated_messages[i]["content"] = "Resume text was too large and had to be removed."
                logging.warning("Resume text was completely truncated due to excessive size.")
            else:
                # Calculate proportion to keep
                keep_ratio = (user_tokens - tokens_to_remove) / user_tokens
                keep_chars = int(len(message["content"]) * keep_ratio)
                
                # Truncate from the middle to keep beginning and end
                if keep_chars < len(message["content"]):
                    half_keep = keep_chars // 2
                    truncated_messages[i]["content"] = (
                        message["content"][:half_keep] + 
                        "\n\n... [content truncated due to length] ...\n\n" + 
                        message["content"][-half_keep:]
                    )
                    logging.warning(f"Resume text was truncated from {user_tokens} to approximately {user_tokens - tokens_to_remove} tokens.")
            
            break  # Only truncate one message
            
    return truncated_messages

# Rate limiting and error handling for OpenAI API calls
@retry(
    wait=wait_random_exponential(min=1, max=60),
    stop=stop_after_attempt(6),
    retry=retry_if_exception_type((RateLimitError, APIConnectionError))
)
def call_openai_with_retry(client: OpenAI, messages: List[Dict[str, str]], model: str, **kwargs) -> ChatCompletion:
    """
    Makes an OpenAI API call with retry logic for rate limits and connection errors
    
    Args:
        client: OpenAI client instance
        messages: List of message dictionaries for the conversation
        model: Model name to use
        **kwargs: Additional parameters for the API call
        
    Returns:
        ChatCompletion response object
    """
    try:
        # Add jitter to avoid rate limits when making multiple parallel calls
        jitter = random.uniform(0.1, 0.5)
        time.sleep(jitter)
        
        return client.chat.completions.create(
            model=model,
            messages=messages,
            **kwargs
        )
    except RateLimitError as e:
        logging.warning(f"Rate limit exceeded: {e}. Retrying...")
        raise
    except APIConnectionError as e:
        logging.warning(f"API connection error: {e}. Retrying...")
        raise
    except APIError as e:
        logging.error(f"API error: {e}")
        raise
    except Exception as e:
        logging.error(f"Unexpected error during API call: {e}")
        raise

def get_resume_batch(batch_size=None):
    """
    Get a batch of resumes from the database.
    This is a compatible replacement for the batch API version.
    
    Args:
        batch_size: Number of resumes to retrieve. If None, defaults to 25.
    """
    # Use default value if none provided
    if batch_size is None:
        batch_size = 25
    
    try:
        # Debug log
        logging.info(f"Starting get_resume_batch with batch_size={batch_size}")
        
        # Connect to the database using our robust connection function
        conn = create_pyodbc_connection()
        logging.info("Connected to database successfully")
        
        cursor = conn.cursor()
        
        # First get the records to process - with NOLOCK hint to reduce resource usage
        select_query = f"""
            SELECT TOP {batch_size} 
                userid,
                markdownResume as cleaned_resume
            FROM dbo.aicandidate WITH (NOLOCK)
            WHERE LastProcessed IS NULL
                AND markdownresume <> ''
                AND markdownresume IS NOT NULL
            ORDER BY datelastmodified DESC
        """
        
        logging.info(f"Executing query: {select_query}")
        cursor.execute(select_query)
        logging.info("Query executed successfully")
        
        rows = cursor.fetchall()
        logging.info(f"Fetched {len(rows)} rows from database")
        
        resume_batch = []
        user_ids = []
        
        for row in rows:
            userid = row[0]
            cleaned_resume = row[1]
            
            if cleaned_resume and len(str(cleaned_resume).strip()) > 0:
                resume_batch.append((userid, cleaned_resume))
                user_ids.append(userid)
                logging.info(f"Added UserID {userid} to batch (resume length: {len(cleaned_resume)})")
        
        if user_ids:
            logging.info(f"Found {len(user_ids)} valid resumes to process")
            logging.info(f"First userID: {user_ids[0]}")
            
            # Now mark these as processed - keep it simple with a single update
            ids_string = ','.join(str(id) for id in user_ids)
            update_query = f"UPDATE dbo.aicandidate SET LastProcessed = GETDATE() WHERE userid IN ({ids_string})"
            logging.info(f"Executing update: {update_query}")
            cursor.execute(update_query)
            logging.info(f"Update executed, {cursor.rowcount} rows affected")
            
            # Commit changes
            conn.commit()
            logging.info("Changes committed successfully")
        else:
            logging.info("No records found to process")
            
        cursor.close()
        conn.close()
        logging.info("Database connection closed")
        
        return resume_batch
        
    except Exception as e:
        logging.error(f"Error retrieving resume batch: {str(e)}")
        try:
            conn.close()
            logging.info("Closed database connection after error")
        except:
            pass
        return []

def process_resume_with_direct_api(userid: str, resume_text: str, model: str = DEFAULT_MODEL, temperature: float = DEFAULT_TEMPERATURE, save_raw_response: bool = False) -> Dict[str, Any]:
    """
    Process a single resume using OpenAI's direct API calls
    
    Args:
        userid: User ID for tracking
        resume_text: Raw resume text to process
        model: OpenAI model to use
        temperature: Temperature for generation

    Returns:
        Dictionary with processing results including parsed data and cost metrics
    """
    start_time = time.time()
    client = create_openai_client()
    
    try:
        from one_step_processor import create_unified_prompt
        messages = create_unified_prompt(resume_text, userid)
        
        # Track token usage
        prompt_tokens = 0
        for message in messages:
            if "content" in message:
                prompt_tokens += num_tokens_from_string(message["content"])
        
        # Make API call with retry logic
        response = call_openai_with_retry(
            client=client,
            messages=messages,
            model=model,
            temperature=temperature,
            max_tokens=MAX_TOKENS
        )
        
        # Extract and parse the result
        completion_text = response.choices[0].message.content
        
        # Log full response for debugging when needed
        if save_raw_response:
            logging.info(f"UserID {userid}: Raw API response:\n{completion_text}")
        
        # Parse response using our enhanced parser functions
        
        # First parse step 1 data (user details, etc.)
        step1_data = parse_step1_response(completion_text)
        
        # Then parse step 2 data (technical details, etc.)
        step2_data = parse_step2_response(completion_text)
        
        # Combine both results
        parsed_data = {**step1_data, **step2_data}
        
        # Log what fields we extracted
        logging.info(f"UserID {userid}: Extracted {len(parsed_data)} fields")
        logging.info(f"UserID {userid}: Extracted fields: {', '.join(sorted(parsed_data.keys()))}")
        
        # Verify critical fields were extracted
        missing_critical = []
        for field in ['PrimaryTitle', 'MostRecentCompany']:
            if field not in parsed_data or parsed_data[field] == 'NULL':
                missing_critical.append(field)
        
        if missing_critical:
            logging.warning(f"UserID {userid}: Missing critical fields: {', '.join(missing_critical)}")
            
        # Additional processing for special fields
        from date_processor import process_resume_with_enhanced_dates
        enhanced_data = process_resume_with_enhanced_dates(userid, parsed_data)
        parsed_data = enhanced_data
        
        # Calculate cost
        completion_tokens = response.usage.completion_tokens
        total_tokens = response.usage.total_tokens
        
        # Current pricing model (approximate - may need adjustment)
        input_cost_per_1k = 0.001  # $0.001 per 1K input tokens for gpt-4o-mini
        output_cost_per_1k = 0.003  # $0.003 per 1K output tokens for gpt-4o-mini
        
        input_cost = (prompt_tokens / 1000) * input_cost_per_1k
        output_cost = (completion_tokens / 1000) * output_cost_per_1k
        total_cost = input_cost + output_cost
        
        processing_time = time.time() - start_time
        
        result = {
            "userid": userid,
            "success": True,
            "parsed_data": parsed_data,
            "metrics": {
                "model": model,
                "prompt_tokens": prompt_tokens,
                "completion_tokens": completion_tokens,
                "total_tokens": total_tokens,
                "processing_time_seconds": processing_time,
                "cost": {
                    "input_cost": input_cost,
                    "output_cost": output_cost,
                    "total_cost": total_cost
                }
            }
        }
        
        # Save raw response if requested
        if save_raw_response:
            result["raw_response"] = completion_text
        
        return result
    
    except Exception as e:
        processing_time = time.time() - start_time
        logging.error(f"Error processing resume for user {userid}: {str(e)}")
        return {
            "userid": userid,
            "success": False,
            "error": str(e),
            "metrics": {
                "model": model,
                "processing_time_seconds": processing_time,
            }
        }

def get_valid_columns(cursor):
    """Get a list of valid column names in the aicandidate table"""
    try:
        cursor.execute("SELECT TOP 0 * FROM aicandidate")
        return [column[0] for column in cursor.description]
    except Exception as e:
        logging.error(f"Error getting column names: {str(e)}")
        # Return a default set of common column names if we can't query the schema
        return ["userid", "PrimaryTitle", "SecondaryTitle", "TertiaryTitle", "Address", "City", "State", 
                "Certifications", "Bachelors", "Masters", "Phone1", "Phone2", "Email", "Email2", 
                "FirstName", "MiddleName", "LastName", "Linkedin", "MostRecentCompany", 
                "MostRecentStartDate", "MostRecentEndDate", "MostRecentLocation", "SecondMostRecentCompany", 
                "SecondMostRecentStartDate", "SecondMostRecentEndDate", "SecondMostRecentLocation", 
                "ThirdMostRecentCompany", "ThirdMostRecentStartDate", "ThirdMostRecentEndDate", 
                "ThirdMostRecentLocation", "FourthMostRecentCompany", "FourthMostRecentStartDate", 
                "FourthMostRecentEndDate", "FourthMostRecentLocation", "FifthMostRecentCompany", 
                "FifthMostRecentStartDate", "FifthMostRecentEndDate", "FifthMostRecentLocation", 
                "SixthMostRecentCompany", "SixthMostRecentStartDate", "SixthMostRecentEndDate", 
                "SixthMostRecentLocation", "SeventhMostRecentCompany", "SeventhMostRecentStartDate", 
                "SeventhMostRecentEndDate", "SeventhMostRecentLocation", "PrimaryIndustry", 
                "SecondaryIndustry", "Top10Skills", "PrimarySoftwareLanguage", "SecondarySoftwareLanguage", 
                "TertiarySoftwareLanguage", "SoftwareApp1", "SoftwareApp2", "SoftwareApp3", "SoftwareApp4", 
                "SoftwareApp5", "Hardware1", "Hardware2", "Hardware3", "Hardware4", "Hardware5", 
                "PrimaryCategory", "SecondaryCategory", "ProjectTypes", "Specialty", "Summary", 
                "LengthinUS", "YearsofExperience", "AvgTenure"]

def update_candidate_record_with_retry(userid, parsed_data, max_retries=3):
    """Update the aicandidate table with parsed resume data with deadlock retry logic"""
    # Define values that should be treated as null
    null_values = {'NULL'}

    # Max text length for trimming
    max_text_length = 7000  # Adjust this based on your database schema

    retry_count = 0
    while retry_count < max_retries:
        try:
            # Connect to the database using our robust connection function
            conn = create_pyodbc_connection()
            cursor = conn.cursor()
            
            # Start a transaction
            conn.autocommit = False
            
            # Check if the record already exists
            cursor.execute("SELECT COUNT(*) FROM aicandidate WHERE userid = ?", userid)
            exists = cursor.fetchone()[0] > 0
            
            # Log explicit debug information about the update
            logging.info(f"Updating record for UserID {userid}, record exists: {exists}")
            
            # Get valid column names from the database
            valid_columns = get_valid_columns(cursor)
            
            # Prepare field lists and parameter markers for the SQL query
            fields = []
            params = []
            param_markers = []
            filtered_field_count = 0
            
            # Add userid to fields and params
            fields.append("userid")
            params.append(userid)
            param_markers.append("?")
            
            # Process each field in parsed_data
            for field, value in parsed_data.items():
                # Only include fields that exist in the database schema
                if field in valid_columns:
                    # Always include fields in update, even if empty
                    fields.append(field)
                    
                    # Convert empty strings and "NULL" to None for SQL NULL
                    if value in null_values or value == "":
                        params.append(None)
                    else:
                        # Truncate text fields if needed to prevent SQL errors
                        if isinstance(value, str) and len(value) > max_text_length:
                            logging.warning(f"Truncating {field} for UserID {userid} from {len(value)} to {max_text_length} characters")
                            params.append(value[:max_text_length])
                        else:
                            params.append(value)
                    
                    param_markers.append("?")
                else:
                    filtered_field_count += 1
            
            if filtered_field_count > 0:
                logging.warning(f"Filtered out {filtered_field_count} fields that don't exist in the database schema")
            
            # Note: We no longer need to set LastProcessed here as it's set during initial selection
            
            # Define date fields for special handling in the SQL query
            date_fields = [
                "MostRecentStartDate", "MostRecentEndDate",
                "SecondMostRecentStartDate", "SecondMostRecentEndDate",
                "ThirdMostRecentStartDate", "ThirdMostRecentEndDate",
                "FourthMostRecentStartDate", "FourthMostRecentEndDate",
                "FifthMostRecentStartDate", "FifthMostRecentEndDate",
                "SixthMostRecentStartDate", "SixthMostRecentEndDate",
                "SeventhMostRecentStartDate", "SeventhMostRecentEndDate"
            ]
            
            # Use a simpler UPDATE/INSERT approach instead of MERGE
            # First check if the record exists
            if exists:
                # Build UPDATE query
                update_fields = [f"{field} = ?" for field in fields[1:]]
                update_query = f"""
                UPDATE aicandidate 
                SET {', '.join(update_fields)}
                WHERE userid = ?
                """
                # Parameters for update (skip userid in the SET clause but add it for the WHERE)
                update_params = params[1:] + [userid]
                cursor.execute(update_query, update_params)
            else:
                # Build INSERT query
                insert_query = f"""
                INSERT INTO aicandidate ({', '.join(fields)})
                VALUES ({', '.join(['?' for _ in range(len(fields))])});
                """
                cursor.execute(insert_query, params)
            
            # Log query for debugging (without sensitive data)
            logging.info(f"Executing database {'UPDATE' if exists else 'INSERT'} for UserID {userid}")
            
            # Commit changes (the queries themselves were executed in the conditional block above)
            rows_affected = cursor.rowcount
            conn.commit()
            
            # Log success
            logging.info(f"Database update completed successfully for UserID {userid} ({rows_affected} rows affected)")
            
            # Close cursor and connection
            cursor.close()
            conn.close()
            
            return True
        
        except pyodbc.Error as e:
            retry_count += 1
            
            # Check for deadlock, which is a retryable error
            if "deadlock" in str(e).lower() and retry_count < max_retries:
                # Exponential backoff with jitter
                wait_time = (2 ** retry_count) + random.random()
                logging.warning(f"Deadlock detected for UserID {userid}, retry {retry_count}/{max_retries} after {wait_time:.2f}s")
                time.sleep(wait_time)
            else:
                # Non-deadlock error or max retries reached
                logging.error(f"Database error for UserID {userid}: {str(e)}")
                try:
                    conn.rollback()
                    logging.info(f"Transaction rolled back for UserID {userid}")
                except:
                    pass
                    
                try:
                    cursor.close()
                    conn.close()
                    logging.info(f"Connection closed after error for UserID {userid}")
                except:
                    pass
                    
                return False

def process_resumes_in_parallel(resume_batch, max_workers=5, model=DEFAULT_MODEL, temperature=DEFAULT_TEMPERATURE):
    """
    Process multiple resumes in parallel using a thread pool
    
    Args:
        resume_batch: List of (userid, resume_text) tuples
        max_workers: Maximum number of concurrent workers
        model: OpenAI model to use
        temperature: Temperature setting for generation
        
    Returns:
        Dictionary with processing results and metrics
    """
    if not resume_batch:
        logging.warning("Empty resume batch provided for parallel processing")
        return {"success": True, "results": [], "metrics": {"total_cost": 0, "processing_time_seconds": 0}}
    
    start_time = time.time()
    total_cost = 0
    successful_count = 0
    failed_count = 0
    results = []
    
    # Process resumes in parallel using ThreadPoolExecutor
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Create future tasks
        future_to_userid = {}
        for userid, resume_text in resume_batch:
            future = executor.submit(
                process_resume_with_direct_api,
                userid=userid,
                resume_text=resume_text,
                model=model,
                temperature=temperature
            )
            future_to_userid[future] = userid
        
        # Process results as they complete
        for future in concurrent.futures.as_completed(future_to_userid):
            userid = future_to_userid[future]
            try:
                result = future.result()
                results.append(result)
                
                if result["success"]:
                    successful_count += 1
                    # Update database with parsed data
                    update_success = update_candidate_record_with_retry(userid, result["parsed_data"])
                    if update_success:
                        logging.info(f"Successfully updated database for UserID {userid}")
                    else:
                        logging.error(f"Failed to update database for UserID {userid}")
                    
                    # Add cost to total if available
                    if "metrics" in result and "cost" in result["metrics"]:
                        total_cost += result["metrics"]["cost"]["total_cost"]
                else:
                    failed_count += 1
                    logging.error(f"Processing failed for UserID {userid}: {result.get('error', 'Unknown error')}")
            
            except Exception as e:
                failed_count += 1
                logging.error(f"Exception during processing for UserID {userid}: {str(e)}")
                results.append({
                    "userid": userid,
                    "success": False,
                    "error": str(e)
                })
    
    # Calculate overall metrics
    total_time = time.time() - start_time
    
    # Compile overall results
    final_result = {
        "success": True,
        "results": results,
        "metrics": {
            "total_resumes": len(resume_batch),
            "successful_count": successful_count,
            "failed_count": failed_count,
            "success_rate": successful_count / len(resume_batch) if resume_batch else 0,
            "total_cost": total_cost,
            "processing_time_seconds": total_time,
            "average_time_per_resume": total_time / len(resume_batch) if resume_batch else 0
        }
    }
    
    return final_result

# Parsers from the original batch_api_utils.py
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
    
    # Try to extract primary job title
    for pattern in primary_patterns:
        match = re.search(pattern, response_text, re.IGNORECASE)
        if match:
            extracted["PrimaryTitle"] = match.group(1).strip()
            break
    
    # Try to extract secondary job title
    for pattern in secondary_patterns:
        match = re.search(pattern, response_text, re.IGNORECASE)
        if match:
            extracted["SecondaryTitle"] = match.group(1).strip()
            break
    
    # Try to extract tertiary job title
    for pattern in tertiary_patterns:
        match = re.search(pattern, response_text, re.IGNORECASE)
        if match:
            extracted["TertiaryTitle"] = match.group(1).strip()
            break
    
    # Extract company information
    for field, patterns in company_patterns.items():
        for pattern in patterns:
            match = re.search(pattern, response_text, re.IGNORECASE)
            if match:
                value = match.group(1).strip()
                if value.upper() != "NULL" and value:
                    extracted[field] = value
                break
    
    # Add patterns for date fields
    date_patterns = {
        "MostRecentStartDate": [
            r"Most Recent Start Date \(YYYY-MM-DD\):\s*(.+)",
            r"Most Recent Start Date:\s*(.+)"
        ],
        "MostRecentEndDate": [
            r"Most Recent End Date \(YYYY-MM-DD\):\s*(.+)",
            r"Most Recent End Date:\s*(.+)"
        ],
        # Add patterns for other dates...
    }
    
    # Extract dates
    for field, patterns in date_patterns.items():
        for pattern in patterns:
            match = re.search(pattern, response_text, re.IGNORECASE)
            if match:
                value = match.group(1).strip()
                if value.upper() != "NULL" and value:
                    extracted[field] = value
                break
                
    # Add patterns for personal information
    personal_info_patterns = {
        "FirstName": [r"Their First Name:\s*(.+)", r"First Name:\s*(.+)"],
        "MiddleName": [r"Their Middle Name:\s*(.+)", r"Middle Name:\s*(.+)"],
        "LastName": [r"Their Last Name:\s*(.+)", r"Last Name:\s*(.+)"],
        "Address": [r"Their street address:\s*(.+)", r"Street Address:\s*(.+)"],
        "City": [r"Their City:\s*(.+)", r"City:\s*(.+)"],
        "State": [r"Their State:\s*(.+)", r"State:\s*(.+)"],
        "Phone1": [r"Their Phone Number:\s*(.+)", r"Phone Number 1:\s*(.+)"],
        "Phone2": [r"Their Second Phone Number:\s*(.+)", r"Phone Number 2:\s*(.+)"],
        "Email": [r"Their Email:\s*(.+)", r"Email 1:\s*(.+)"],
        "Email2": [r"Their Second Email:\s*(.+)", r"Email 2:\s*(.+)"],
        "Linkedin": [r"Their Linkedin URL:\s*(.+)", r"LinkedIn URL:\s*(.+)"],
    }
    
    # Extract personal information
    for field, patterns in personal_info_patterns.items():
        for pattern in patterns:
            match = re.search(pattern, response_text, re.IGNORECASE)
            if match:
                value = match.group(1).strip()
                if value.upper() != "NULL" and value:
                    extracted[field] = value
                break
    
    return extracted

def parse_step1_response(response_text):
    """Parse GPT response for step 1 (user information)"""
    try:
        # Log the raw response for debugging (limited to first 200 chars)
        logging.debug(f"Step 1 raw response first 200 chars: {response_text[:200]}")
        
        # Try direct extraction of all fields first
        direct_fields = extract_fields_directly(response_text)
        
        # Now parse line by line
        result = {}
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
            "Their street address": "Address",
            "Their City": "City",
            "Their State": "State",
            "Their Certifications Listed": "Certifications",
            "Their Bachelor's Degree": "Bachelors",
            "Their Master's Degree": "Masters",
            "Their Phone Number": "Phone1",
            "Their Second Phone Number": "Phone2",
            "Their Email": "Email",
            "Their Second Email": "Email2",
            "Their First Name": "FirstName",
            "Their Middle Name": "MiddleName",
            "Their Last Name": "LastName",
            "Their Linkedin URL": "Linkedin",
            
            # Additional variations that might appear
            "Best job title that fits their primary experience": "PrimaryTitle",
            "Best job title fitting their primary experience": "PrimaryTitle",
            "Most Recent Company": "MostRecentCompany",
            "Most Recent Start Date": "MostRecentStartDate",
            "Most Recent End Date": "MostRecentEndDate",
            "Most Recent Job Location": "MostRecentLocation",
            "Most Recent Company Worked for": "MostRecentCompany",
            "Most Recent Start Date (YYYY-MM-DD)": "MostRecentStartDate",
            "Most Recent End Date (YYYY-MM-DD)": "MostRecentEndDate",
            "Second Most Recent Company": "SecondMostRecentCompany",
            "Second Most Recent Start Date": "SecondMostRecentStartDate",
            "Second Most Recent End Date": "SecondMostRecentEndDate",
            "Second Most Recent Job Location": "SecondMostRecentLocation",
            "Second Most Recent Company Worked for": "SecondMostRecentCompany",
            "Second Most Recent Start Date (YYYY-MM-DD)": "SecondMostRecentStartDate",
            "Second Most Recent End Date (YYYY-MM-DD)": "SecondMostRecentEndDate",
            "Third Most Recent Company": "ThirdMostRecentCompany",
            "Third Most Recent Start Date": "ThirdMostRecentStartDate",
            "Third Most Recent End Date": "ThirdMostRecentEndDate",
            "Third Most Recent Job Location": "ThirdMostRecentLocation",
            "Third Most Recent Company Worked for": "ThirdMostRecentCompany",
            "Third Most Recent Start Date (YYYY-MM-DD)": "ThirdMostRecentStartDate",
            "Third Most Recent End Date (YYYY-MM-DD)": "ThirdMostRecentEndDate",
            "Fourth Most Recent Company": "FourthMostRecentCompany",
            "Fourth Most Recent Start Date": "FourthMostRecentStartDate",
            "Fourth Most Recent End Date": "FourthMostRecentEndDate",
            "Fourth Most Recent Job Location": "FourthMostRecentLocation",
            "Fourth Most Recent Company Worked for": "FourthMostRecentCompany",
            "Fourth Most Recent Start Date (YYYY-MM-DD)": "FourthMostRecentStartDate",
            "Fourth Most Recent End Date (YYYY-MM-DD)": "FourthMostRecentEndDate",
            "Fifth Most Recent Company": "FifthMostRecentCompany",
            "Fifth Most Recent Start Date": "FifthMostRecentStartDate",
            "Fifth Most Recent End Date": "FifthMostRecentEndDate",
            "Fifth Most Recent Job Location": "FifthMostRecentLocation",
            "Fifth Most Recent Company Worked for": "FifthMostRecentCompany",
            "Fifth Most Recent Start Date (YYYY-MM-DD)": "FifthMostRecentStartDate",
            "Fifth Most Recent End Date (YYYY-MM-DD)": "FifthMostRecentEndDate",
            "Sixth Most Recent Company": "SixthMostRecentCompany",
            "Sixth Most Recent Company Worked for": "SixthMostRecentCompany",
            "Sixth Most Recent Start Date (YYYY-MM-DD)": "SixthMostRecentStartDate",
            "Sixth Most Recent End Date (YYYY-MM-DD)": "SixthMostRecentEndDate",
            "Sixth Most Recent Job Location": "SixthMostRecentLocation",
            "Seventh Most Recent Company": "SeventhMostRecentCompany",
            "Seventh Most Recent Company Worked for": "SeventhMostRecentCompany",
            "Seventh Most Recent Start Date (YYYY-MM-DD)": "SeventhMostRecentStartDate",
            "Seventh Most Recent End Date (YYYY-MM-DD)": "SeventhMostRecentEndDate",
            "Seventh Most Recent Job Location": "SeventhMostRecentLocation",
            "Primary Industry": "PrimaryIndustry",
            "Secondary Industry": "SecondaryIndustry",
            "Based on all 7 of their most recent companies above, what is the Primary industry they work in": "PrimaryIndustry",
            "Based on all 7 of their most recent companies above, what is the Secondary industry they work in": "SecondaryIndustry",
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
            if value and (field not in mapped_result or mapped_result.get(field, "NULL") == "NULL"):
                mapped_result[field] = value
                logging.debug(f"Using directly extracted {field}: '{value}'")
        
        # Process special fields like dates
        for field in ['MostRecentStartDate', 'MostRecentEndDate', 
                     'SecondMostRecentStartDate', 'SecondMostRecentEndDate',
                     'ThirdMostRecentStartDate', 'ThirdMostRecentEndDate',
                     'FourthMostRecentStartDate', 'FourthMostRecentEndDate',
                     'FifthMostRecentStartDate', 'FifthMostRecentEndDate',
                     'SixthMostRecentStartDate', 'SixthMostRecentEndDate',
                     'SeventhMostRecentStartDate', 'SeventhMostRecentEndDate']:
            if field in mapped_result:
                if mapped_result[field] == 'Present':
                    # For 'Present', use current date
                    mapped_result[field] = datetime.now().strftime('%Y-%m-%d')
                elif mapped_result[field] == 'NULL':
                    # For 'NULL', set to None
                    mapped_result[field] = None
                    
        return mapped_result
    except Exception as e:
        logging.error(f"Error parsing step 1 response: {str(e)}")
        return {}

def extract_technical_fields(response_text):
    """Extract technical fields using regex patterns"""
    import re
    
    # Dictionary to store extracted fields
    extracted = {}
    
    # Define patterns for technical fields
    tech_patterns = {
        "PrimarySoftwareLanguage": [
            r"What technical language do they use most often\?:\s*(.+)",
            r"What technical language do they use most often:\s*(.+)"
        ],
        "SecondarySoftwareLanguage": [
            r"What technical language do they use second most often\?:\s*(.+)",
            r"What technical language do they use second most often:\s*(.+)"
        ],
        "TertiarySoftwareLanguage": [
            r"What technical language do they use third most often\?:\s*(.+)",
            r"What technical language do they use third most often:\s*(.+)"
        ]
    }
    
    # Extract technical fields
    for field, patterns in tech_patterns.items():
        for pattern in patterns:
            match = re.search(pattern, response_text, re.IGNORECASE)
            if match:
                value = match.group(1).strip()
                if value.upper() != "NULL" and value:
                    extracted[field] = value
                break
    
    # Define patterns for software fields
    software_patterns = {
        "SoftwareApp1": [
            r"What software do they talk about using the most\?:\s*(.+)",
            r"What software do they talk about using the most:\s*(.+)"
        ],
        "SoftwareApp2": [
            r"What software do they talk about using the second most\?:\s*(.+)",
            r"What software do they talk about using the second most:\s*(.+)"
        ],
        "SoftwareApp3": [
            r"What software do they talk about using the third most\?:\s*(.+)",
            r"What software do they talk about using the third most:\s*(.+)"
        ],
        "SoftwareApp4": [
            r"What software do they talk about using the fourth most\?:\s*(.+)",
            r"What software do they talk about using the fourth most:\s*(.+)"
        ],
        "SoftwareApp5": [
            r"What software do they talk about using the fifth most\?:\s*(.+)",
            r"What software do they talk about using the fifth most:\s*(.+)"
        ]
    }
    
    # Extract software fields
    for field, patterns in software_patterns.items():
        for pattern in patterns:
            match = re.search(pattern, response_text, re.IGNORECASE)
            if match:
                value = match.group(1).strip()
                if value.upper() != "NULL" and value:
                    extracted[field] = value
                break
    
    # Define patterns for hardware fields
    hardware_patterns = {
        "Hardware1": [
            r"What physical hardware do they talk about using the most\?:\s*(.+)",
            r"What physical hardware do they talk about using the most:\s*(.+)"
        ],
        "Hardware2": [
            r"What physical hardware do they talk about using the second most\?:\s*(.+)",
            r"What physical hardware do they talk about using the second most:\s*(.+)"
        ],
        "Hardware3": [
            r"What physical hardware do they talk about using the third most\?:\s*(.+)",
            r"What physical hardware do they talk about using the third most:\s*(.+)"
        ],
        "Hardware4": [
            r"What physical hardware do they talk about using the fourth most\?:\s*(.+)",
            r"What physical hardware do they talk about using the fourth most:\s*(.+)"
        ],
        "Hardware5": [
            r"What physical hardware do they talk about using the fifth most\?:\s*(.+)",
            r"What physical hardware do they talk about using the fifth most:\s*(.+)"
        ]
    }
    
    # Extract hardware fields
    for field, patterns in hardware_patterns.items():
        for pattern in patterns:
            match = re.search(pattern, response_text, re.IGNORECASE)
            if match:
                value = match.group(1).strip()
                if value.upper() != "NULL" and value:
                    extracted[field] = value
                break
    
    return extracted

def parse_step2_response(response_text):
    """Parse GPT response for step 2 (technical information)"""
    try:
        # Log the raw response for debugging (limited to first 200 chars)
        logging.debug(f"Step 2 raw response first 200 chars: {response_text[:200]}")
        
        # Try direct extraction of all fields first
        direct_fields = extract_technical_fields(response_text)
        
        # Now parse line by line
        result = {}
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
            "What technical language do they use most often": "PrimarySoftwareLanguage",
            "What technical language do they use second most often": "SecondarySoftwareLanguage",
            "What technical language do they use third most often": "TertiarySoftwareLanguage",
            "What software do they talk about using the most": "SoftwareApp1",
            "What software do they talk about using the second most": "SoftwareApp2",
            "What software do they talk about using the third most": "SoftwareApp3",
            "What software do they talk about using the fourth most": "SoftwareApp4",
            "What software do they talk about using the fifth most": "SoftwareApp5",
            "What physical hardware do they talk about using the most": "Hardware1",
            "What physical hardware do they talk about using the second most": "Hardware2",
            "What physical hardware do they talk about using the third most": "Hardware3",
            "What physical hardware do they talk about using the fourth most": "Hardware4",
            "What physical hardware do they talk about using the fifth most": "Hardware5",
            "Based on their experience, put them in a primary technical category if they are technical or functional category if they are functional": "PrimaryCategory",
            "Based on their experience, put them in a subsidiary technical category if they are technical or functional category if they are functional": "SecondaryCategory",
            "Types of projects they have worked on": "ProjectTypes",
            "Based on their skills, categories, certifications, and industries, determine what they specialize in": "Specialty",
            "Based on all this knowledge, write a summary of this candidate that could be sellable to an employer": "Summary",
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
                    
            # Only update if the field doesn't exist yet or the existing value is NULL
            if mapped_key not in mapped_result or mapped_result[mapped_key] == "NULL":
                mapped_result[mapped_key] = value
        
        # Add all directly extracted fields if they're available and not already set
        for field, value in direct_fields.items():
            if value and (field not in mapped_result or mapped_result.get(field, "NULL") == "NULL"):
                mapped_result[field] = value
                logging.debug(f"Using directly extracted {field}: '{value}'")
        
        # For numeric fields, ensure they are numeric
        for field in ['LengthinUS', 'YearsofExperience', 'AvgTenure']:
            if field in mapped_result and mapped_result[field] != 'NULL':
                try:
                    # Try to convert to float and then round to integer
                    mapped_result[field] = str(round(float(mapped_result[field])))
                except:
                    # If conversion fails, keep as is
                    pass
        
        return mapped_result
    except Exception as e:
        logging.error(f"Error parsing step 2 response: {str(e)}")
        return {}