"""
Utility functions for batch API processing without external dependencies

This module contains all the required utility functions needed for the batch API
to operate independently without requiring files from the parent project.
"""

import os
import logging
import time
import json
import sys
from datetime import datetime, timedelta
import pyodbc
import tiktoken
import openai
from dotenv import load_dotenv
from typing import Dict, List, Tuple, Any, Optional

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
        logging.FileHandler(os.path.join(os.path.dirname(__file__), "batch_api.log")),
        logging.StreamHandler()
    ]
)

# Set up OpenAI client with API key from environment
api_key = os.getenv('OPENAI_API_KEY')
openai.api_key = api_key
if not api_key:
    logging.error("API key is not set in the environment variables.")

# Import the model from the main project to ensure consistency
try:
    # Use the same model as the main single_step_processor
    from resume_utils import DEFAULT_MODEL
    logging.info(f"Using model from main project: {DEFAULT_MODEL}")
except ImportError:
    # Fallback if import fails
    DEFAULT_MODEL = "gpt-5-mini-2025-08-07"
    logging.warning(f"Could not import model from main project, using fallback: {DEFAULT_MODEL}")
MAX_TOKENS = 16000
DEFAULT_TEMPERATURE = 0

# Token encoding
def num_tokens_from_string(string, encoding_name="cl100k_base"):
    """Returns the number of tokens in a text string."""
    try:
        # Try to get encoding for the model first
        try:
            # Handle gpt-5 models by using gpt-4 encoding
            model_for_encoding = DEFAULT_MODEL
            if "gpt-5" in DEFAULT_MODEL.lower():
                model_for_encoding = "gpt-4"  # Use gpt-4 encoding for gpt-5 models
            encoding = tiktoken.encoding_for_model(model_for_encoding)
        except (KeyError, Exception):
            # If that fails, use the explicit get_encoding method
            encoding = tiktoken.get_encoding(encoding_name)
        
        num_tokens = len(encoding.encode(string))
        return num_tokens
    except Exception as e:
        logging.error(f"Error counting tokens: {str(e)}")
        # Return an estimate if token counting fails (average 4 characters per token)
        return len(string) // 4

def apply_token_truncation(messages, max_input_tokens=128000):  # Increased to 128K
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

def get_resume_batch(batch_size=None):
    """
    Get a batch of resumes from the database.
    
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
        conn_result = create_pyodbc_connection()
        # Handle the tuple return from create_pyodbc_connection
        if isinstance(conn_result, tuple):
            conn, success, message = conn_result
            if not success:
                logging.error(f"Failed to connect to database: {message}")
                return []
        else:
            # Fallback if it returns just connection
            conn = conn_result
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
            conn_result = create_pyodbc_connection()
            # Handle the tuple return from create_pyodbc_connection
            if isinstance(conn_result, tuple):
                conn, success, message = conn_result
                if not success:
                    logging.error(f"Failed to connect to database: {message}")
                    retry_count += 1
                    continue
            else:
                # Fallback if it returns just connection
                conn = conn_result
            cursor = conn.cursor()
            
            # Start a transaction
            conn.autocommit = False
            
            # Check if the record already exists
            cursor.execute("SELECT COUNT(*) FROM aicandidate WHERE userid = ?", userid)
            exists = cursor.fetchone()[0] > 0
            
            # Log explicit debug information about the update
            logging.info(f"Updating record for UserID {userid}, record exists: {exists}")
            
            # Prepare field lists and parameter markers for the SQL query
            fields = []
            params = []
            param_markers = []
            
            # Add userid to fields and params
            fields.append("userid")
            params.append(userid)
            param_markers.append("?")
            
            # Process each field in parsed_data
            for field, value in parsed_data.items():
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
            
            # Define date fields that need special handling
            date_fields = [
                "MostRecentStartDate", "MostRecentEndDate",
                "SecondMostRecentStartDate", "SecondMostRecentEndDate",
                "ThirdMostRecentStartDate", "ThirdMostRecentEndDate",
                "FourthMostRecentStartDate", "FourthMostRecentEndDate",
                "FifthMostRecentStartDate", "FifthMostRecentEndDate",
                "SixthMostRecentStartDate", "SixthMostRecentEndDate",
                "SeventhMostRecentStartDate", "SeventhMostRecentEndDate"
            ]
            
            # Text field maximum lengths based on DB schema
            field_max_lengths = {
                "PrimaryTitle": 255,
                "SecondaryTitle": 255,
                "TertiaryTitle": 255,
                "Address": 255,
                "City": 100,
                "State": 50,
                "Bachelors": 255,
                "Masters": 255,
                "Phone1": 50,
                "Phone2": 50,
                "Email": 255,
                "Email2": 255,
                "FirstName": 100,
                "MiddleName": 100,
                "LastName": 100,
                "Linkedin": 255,
                "MostRecentCompany": 255,
                "MostRecentLocation": 255,
                "SecondMostRecentCompany": 255,
                "SecondMostRecentLocation": 255,
                "ThirdMostRecentCompany": 255,
                "ThirdMostRecentLocation": 255,
                "FourthMostRecentCompany": 255,
                "FourthMostRecentLocation": 255,
                "FifthMostRecentCompany": 255,
                "FifthMostRecentLocation": 255,
                "SixthMostRecentCompany": 255,
                "SixthMostRecentLocation": 255,
                "SeventhMostRecentCompany": 255,
                "SeventhMostRecentLocation": 255,
                "PrimaryIndustry": 255,
                "SecondaryIndustry": 255,
                "Skill1": 100,
                "Skill2": 100,
                "Skill3": 100,
                "Skill4": 100,
                "Skill5": 100,
                "Skill6": 100,
                "Skill7": 100,
                "Skill8": 100,
                "Skill9": 100,
                "Skill10": 100,
                "PrimarySoftwareLanguage": 255,
                "SecondarySoftwareLanguage": 255,
                "TertiarySoftwareLanguage": 255,
                "SoftwareApp1": 255,
                "SoftwareApp2": 255,
                "SoftwareApp3": 255,
                "SoftwareApp4": 255,
                "SoftwareApp5": 255,
                "Hardware1": 255,
                "Hardware2": 255,
                "Hardware3": 255,
                "Hardware4": 255,
                "Hardware5": 255,
                "PrimaryCategory": 255,
                "SecondaryCategory": 255,
                "LengthinUS": 50,
                "YearsofExperience": 50,
                "AvgTenure": 50,
                # NVARCHAR(MAX) fields don't need length limit
                "Certifications": 8000,
                "ProjectTypes": 8000,
                "Specialty": 8000,
                "Summary": 8000
            }
            
            # Build and execute the appropriate SQL query (INSERT or UPDATE)
            if exists:
                # Database connection check - just log that we're updating the record
                logging.info(f"Verifying database connection for UserID {userid}")
                
                # Split fields for separate handling of date fields and text fields
                text_update_clauses = []
                text_update_params = []
                date_update_clauses = []
                
                # Handle text fields
                for field, value in parsed_data.items():
                    if field not in date_fields:
                        # Skip userid as it's for the WHERE clause
                        if field != "userid":
                            # Handle text fields
                            if value is None or value == "NULL" or value == "":
                                # Set empty/NULL values to NULL in SQL
                                text_update_clauses.append(f"{field} = NULL")
                            else:
                                # For non-empty values, apply max length limit
                                max_length = field_max_lengths.get(field, 255)  # Default to 255 if not specified
                                if isinstance(value, str) and len(value) > max_length:
                                    logging.warning(f"Truncating {field} for UserID {userid} from {len(value)} to {max_length} characters")
                                    value = value[:max_length]
                                text_update_clauses.append(f"{field} = ?")
                                text_update_params.append(value)
                
                # Handle date fields without using TRY_CONVERT
                for field in date_fields:
                    if field in parsed_data:
                        value = parsed_data[field]
                        value_str = str(value).lower() if value else ""
                        
                        # Check for NULL/empty values or "present"/"current" indicators
                        if (value is None or value == "NULL" or value == "" or 
                            "present" in value_str or "current" in value_str or
                            "now" in value_str or "ongoing" in value_str):
                            # For NULL values or present/current
                            date_update_clauses.append(f"{field} = NULL")
                            logging.info(f"Setting date field '{field}' to NULL - value was: '{value}'")
                        else:
                            try:
                                # Try to parse the date manually
                                date_string = str(value).strip("'\"")
                                
                                # If it's YYYY format, add month and day
                                if len(date_string) == 4 and date_string.isdigit():
                                    date_string += "-01-01"  # Add month and day
                                
                                # If it's YYYY-MM format, add day
                                elif len(date_string) == 7 and date_string[4] == '-':
                                    date_string += "-01"  # Add day
                                
                                # Convert to SQL date format YYYY-MM-DD
                                # Use a parameter and let SQL Server handle the conversion
                                date_update_clauses.append(f"{field} = ?")
                                text_update_params.append(date_string)
                                logging.info(f"Setting date field '{field}' to '{date_string}'")
                            except Exception as date_error:
                                # If any error occurs, set to NULL
                                date_update_clauses.append(f"{field} = NULL")
                                logging.warning(f"Error parsing date '{value}' for field '{field}': {str(date_error)}. Setting to NULL.")
                
                # Combine all clauses
                all_update_clauses = text_update_clauses + date_update_clauses
                
                if all_update_clauses:
                    # Create the complete UPDATE query
                    update_query = f"UPDATE dbo.aicandidate SET {', '.join(all_update_clauses)} WHERE userid = ?"
                    text_update_params.append(userid)  # Add userid for WHERE clause
                    
                    logging.info(f"Executing UPDATE query with {len(all_update_clauses)} clauses for UserID {userid}")
                    logging.debug(f"Query: {update_query}")
                    logging.debug(f"Params: {text_update_params}")
                    
                    cursor.execute(update_query, text_update_params)
                else:
                    logging.warning(f"No fields to update for UserID {userid}")
            else:
                # For INSERT case, first prepare a minimal insert to create the record
                minimal_query = "INSERT INTO dbo.aicandidate (userid) VALUES (?)"
                logging.info(f"Creating new record for UserID {userid}")
                cursor.execute(minimal_query, [userid])
                
                # Then call this function again to do the UPDATE
                logging.info(f"Record created for UserID {userid}, now updating with full data")
                cursor.close()
                conn.commit()  # Commit the INSERT transaction
                
                # Re-run with the update path
                return update_candidate_record_with_retry(userid, parsed_data, max_retries)
            
            # Explicitly commit the transaction
            conn.commit()
            logging.info(f"Successfully committed transaction for UserID {userid}")
            
            cursor.close()
            conn.close()
            
            return True
            
        except pyodbc.Error as e:
            error_message = str(e)
            
            # Check specifically for deadlock error
            if "deadlock" in error_message.lower() or "40001" in error_message:
                retry_count += 1
                logging.warning(f"Database deadlock detected for UserID {userid}. Retry {retry_count}/{max_retries}.")
                
                # Exponential backoff
                sleep_time = 0.5 * (2 ** retry_count)  # 1, 2, 4, 8... seconds
                time.sleep(sleep_time)
                
                continue  # Try again
            else:
                # Other database error
                logging.error(f"Database error for UserID {userid}: {error_message}")
                return False
                
        except Exception as e:
            # Any other error
            logging.error(f"Error updating record for UserID {userid}: {str(e)}")
            return False
            
    # If we've exhausted retries
    logging.error(f"Failed to update record for UserID {userid} after {max_retries} retries due to deadlocks.")
    return False

# Step 1 prompt creation function (simplified version of what's in two_step_prompts_taxonomy.py)
def create_step1_prompt(resume_text, userid=None):
    """Create a prompt for step 1 of resume processing (personal info extraction)"""
    system_message = """You are an AI assistant specialized in resume parsing. Extract personal and work history information from the resume text provided by the user.
Focus on:
1. Name (First, Middle, Last)
2. Contact info (Phone, Email, LinkedIn)
3. Location (Address, City, State)
4. Education (Bachelors, Masters, Certifications)
5. Work history (up to 7 most recent positions, with company names, dates, locations)
6. Primary industry and secondary industry
7. Primary job title and secondary job title
8. Top 10 skills (comma-separated)
9. Summary of career

Format your response as a structured JSON with these fields. If a field is not found in the resume, use "NULL" as the value.
"""

    # Create the user message with the resume
    user_message = f"Please extract information from the following resume:\n\n{resume_text}"

    # Build the messages array
    messages = [
        {"role": "system", "content": system_message},
        {"role": "user", "content": user_message}
    ]
    
    # Apply token truncation if needed
    return apply_token_truncation(messages)

# Step 2 prompt creation function (simplified version of what's in two_step_prompts_taxonomy.py)
def create_step2_prompt(resume_text, step1_results, userid=None):
    """Create a prompt for step 2 of resume processing (technical details extraction)"""
    system_message = """You are an AI assistant specialized in technical resume analysis. Extract detailed technical information from the resume text provided by the user.
Focus on:
1. Programming languages (Primary, Secondary, Tertiary)
2. Software applications (up to 5)
3. Hardware skills (up to 5)
4. Project types
5. Primary category and secondary category
6. Specialty
7. Years of experience
8. Average tenure
9. Length in US

Format your response as a structured JSON with these fields. If a field is not found in the resume, use "NULL" as the value.
"""

    # Create a user message that combines the resume and step 1 results
    step1_json = json.dumps(step1_results, indent=2)
    user_message = f"""I've already extracted basic information from this resume:

{step1_json}

Now please extract detailed technical information from the following resume:

{resume_text}"""

    # Build the messages array
    messages = [
        {"role": "system", "content": system_message},
        {"role": "user", "content": user_message}
    ]
    
    # Apply token truncation if needed
    return apply_token_truncation(messages)

# Response parsing functions (simplified versions of what's in two_step_processor_taxonomy.py)
def parse_step1_response(response_text):
    """Parse the LLM response from step 1 to extract structured data"""
    # Try to find and parse JSON from the response
    try:
        # Find JSON content - look for everything between the first { and the last }
        start_idx = response_text.find('{')
        end_idx = response_text.rfind('}')
        
        if start_idx >= 0 and end_idx > start_idx:
            json_str = response_text[start_idx:end_idx+1]
            
            # Parse the JSON
            parsed_data = json.loads(json_str)
            
            # Log the result
            logging.debug(f"Successfully parsed step 1 response: {len(parsed_data)} fields extracted")
            
            return parsed_data
        else:
            logging.error("No JSON content found in the response")
            return {}
    except json.JSONDecodeError as e:
        logging.error(f"Error parsing JSON from response: {str(e)}")
        return {}
    except Exception as e:
        logging.error(f"Unexpected error parsing step 1 response: {str(e)}")
        return {}

def parse_step2_response(response_text):
    """Parse the LLM response from step 2 to extract structured data"""
    # Try to find and parse JSON from the response
    try:
        # Find JSON content - look for everything between the first { and the last }
        start_idx = response_text.find('{')
        end_idx = response_text.rfind('}')
        
        if start_idx >= 0 and end_idx > start_idx:
            json_str = response_text[start_idx:end_idx+1]
            
            # Parse the JSON
            parsed_data = json.loads(json_str)
            
            # Log the result
            logging.debug(f"Successfully parsed step 2 response: {len(parsed_data)} fields extracted")
            
            return parsed_data
        else:
            logging.error("No JSON content found in the response")
            return {}
    except json.JSONDecodeError as e:
        logging.error(f"Error parsing JSON from response: {str(e)}")
        return {}
    except Exception as e:
        logging.error(f"Unexpected error parsing step 2 response: {str(e)}")
        return {}

# Import the enhanced date processor
from date_processor import process_resume_with_enhanced_dates