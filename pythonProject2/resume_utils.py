"""
Consolidated utilities for resume processing
"""

import os
import logging
import time
import json
import concurrent.futures
from datetime import datetime
import pyodbc
import tiktoken
import openai
from dotenv import load_dotenv

# Import our enhanced database connection module
from db_connection import (
    get_resume_batch_with_retry,
    get_resume_by_userid_with_retry,
    update_candidate_record,
    test_connection as test_db_connection
)

# Load environment variables
load_dotenv()

# Check if we're in quiet mode
if os.environ.get('QUIET_MODE', '').lower() in ('1', 'true', 'yes'):
    # Set root logger to ERROR level for quiet mode
    logging.getLogger().setLevel(logging.ERROR)

# Configure logging only if not already configured
if not logging.getLogger().handlers:
    # Get the appropriate level based on quiet mode
    log_level = logging.ERROR if os.environ.get('QUIET_MODE', '').lower() in ('1', 'true', 'yes') else logging.INFO
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(levelname)s - %(message)s',
    force=True
)

# Set up OpenAI client
api_key = os.getenv('OPENAI_API_KEY')
openai.api_key = api_key
if not api_key:
    logging.error("API key is not set in the environment variables.")

# Default model and configuration
DEFAULT_MODEL = "gpt-4o-mini-2024-07-18"
MAX_TOKENS = 16000
DEFAULT_TEMPERATURE = 0

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

def apply_token_truncation(messages, max_input_tokens=120000):
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

# Use the enhanced database-fetching functions from db_connection module
def get_resume_batch(batch_size=None, reset_skipped=True):
    """
    Get a batch of resumes from the database using enhanced retry logic.
    
    Args:
        batch_size: Number of resumes to retrieve. If None, defaults to 25.
        reset_skipped: Whether to reset the skipped userids set. Default is True.
    """
    return get_resume_batch_with_retry(
        batch_size=batch_size if batch_size else 25, 
        max_retries=3,
        reset_skipped=reset_skipped
    )

def get_resume_by_userid(userid):
    """
    Get a specific resume by user ID using enhanced retry logic.
    
    Args:
        userid: The user ID to retrieve
        
    Returns:
        A tuple of (userid, resume_text) or None if not found
    """
    return get_resume_by_userid_with_retry(userid, max_retries=3)

def is_valid_sql_date(date_str):
    """Check if a string is a valid SQL Server date format"""
    if not date_str or date_str == "NULL" or date_str == "":
        return True  # NULL values are fine
    
    if date_str == "Present":
        return False  # 'Present' is not a valid SQL date
    
    try:
        # Check if it's in YYYY-MM-DD format
        import datetime
        datetime.datetime.strptime(date_str, "%Y-%m-%d")
        return True
    except ValueError:
        return False

def diagnose_database_fields(userid, parsed_data):
    """Diagnose potential issues with database fields"""
    logging.info(f"[DB DIAGNOSE] Running diagnostic checks on fields for UserID {userid}")
    
    # Check for important fields
    important_fields = [
        "LengthinUS", "YearsofExperience", "AvgTenure",
        "PrimaryTitle", "MostRecentCompany", "MostRecentStartDate",
        "MostRecentEndDate", "ZipCode"
    ]
    
    issues_found = []
    
    # Check date fields for validity
    date_fields = ["MostRecentStartDate", "MostRecentEndDate", "SecondMostRecentStartDate", "SecondMostRecentEndDate", 
                  "ThirdMostRecentStartDate", "ThirdMostRecentEndDate", "FourthMostRecentStartDate", "FourthMostRecentEndDate", 
                  "FifthMostRecentStartDate", "FifthMostRecentEndDate", "SixthMostRecentStartDate", "SixthMostRecentEndDate", 
                  "SeventhMostRecentStartDate", "SeventhMostRecentEndDate"]
    
    for field in date_fields:
        if field in parsed_data:
            value = parsed_data[field]
            if value == "Present":
                logging.warning(f"[DB DIAGNOSE] Date field {field} has value 'Present' which is not valid for SQL Server date columns")
                issues_found.append(f"Date field {field} has value 'Present' which is not valid for SQL Server")
            elif value and value != "NULL":
                try:
                    # Check if it's in YYYY-MM-DD format
                    import datetime
                    datetime.datetime.strptime(value, "%Y-%m-%d")
                    logging.info(f"[DB DIAGNOSE] Date field {field} has valid date format: {value}")
                except ValueError:
                    logging.warning(f"[DB DIAGNOSE] Date field {field} has invalid date format: {value}")
                    issues_found.append(f"Date field {field} has invalid format: {value}")
    
    # Check numeric fields
    numeric_fields = ["LengthinUS", "YearsofExperience", "AvgTenure"]
    for field in numeric_fields:
        if field in parsed_data:
            value = parsed_data[field]
            if value:
                try:
                    float_val = float(value)
                    logging.info(f"[DB DIAGNOSE] {field} = '{value}' (valid number: {float_val})")
                except ValueError:
                    issue = f"{field} value '{value}' is not a valid number"
                    issues_found.append(issue)
                    logging.warning(f"[DB DIAGNOSE] {issue}")
    
    # Check for unusually long fields
    for field, value in parsed_data.items():
        if isinstance(value, str) and len(value) > 500:
            issue = f"Field {field} is unusually long ({len(value)} characters)"
            issues_found.append(issue)
            logging.warning(f"[DB DIAGNOSE] {issue}")
    
    # Check for missing important fields
    for field in important_fields:
        if field not in parsed_data or not parsed_data[field]:
            issue = f"Important field {field} is missing or empty"
            issues_found.append(issue)
            logging.warning(f"[DB DIAGNOSE] {issue}")
    
    # Check for problematic characters in string fields
    for field, value in parsed_data.items():
        if isinstance(value, str):
            # Check for special characters that might cause SQL issues
            if "'" in value or ";" in value or "--" in value:
                issue = f"Field {field} contains special characters that might cause SQL issues"
                issues_found.append(issue)
                logging.warning(f"[DB DIAGNOSE] {issue}")
    
    if issues_found:
        logging.warning(f"[DB DIAGNOSE] Found {len(issues_found)} potential issues with database fields")
    else:
        logging.info(f"[DB DIAGNOSE] No obvious issues found with database fields")
    
    return issues_found

def update_candidate_record_with_retry(userid, parsed_data, max_retries=3):
    """
    Update the aicandidate table with parsed resume data using enhanced error handling and retry logic.
    
    Args:
        userid: User ID to update
        parsed_data: Dictionary of field values to update
        max_retries: Maximum number of update attempts
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Diagnose potential issues before trying to update
        issues = diagnose_database_fields(userid, parsed_data)
        if issues:
            logging.warning(f"Found {len(issues)} potential issues with fields for UserID {userid}")
            # Continue anyway - the issues are logged and db_connection will handle them
        
        # Use the enhanced update function from the db_connection module
        success, message = update_candidate_record(userid, parsed_data, max_retries=max_retries)
        
        if success:
            logging.info(f"Successfully updated record for UserID {userid}")
        else:
            logging.error(f"Failed to update record for UserID {userid}: {message}")
        
        return success
    
    except Exception as e:
        import traceback
        logging.error(f"Unexpected error in update_candidate_record_with_retry: {str(e)}")
        logging.error(f"Traceback: {traceback.format_exc()}")
        return False

# Test the database connection
def test_database_connection():
    """Test the database connection and report results"""
    logging.info("Testing database connection...")
    if test_db_connection():
        logging.info("✅ Database connection test successful!")
        return True
    else:
        logging.error("❌ Database connection test failed")
        return False

# For standalone testing
if __name__ == "__main__":
    # Test database connection
    test_database_connection()
    
    # Try to get a single resume
    test_userid = "12345"  # Replace with a valid user ID
    resume = get_resume_by_userid(test_userid)
    if resume:
        logging.info(f"Successfully retrieved resume for UserID {test_userid}")
    else:
        logging.warning(f"Could not retrieve resume for UserID {test_userid}")
    
    # Try to get a batch of resumes
    batch = get_resume_batch(batch_size=2)
    logging.info(f"Retrieved {len(batch)} resumes in test batch")