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

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
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

def get_resume_batch(batch_size=None):
    """
    Get a batch of resumes from the database.
    
    Args:
        batch_size: Number of resumes to retrieve. If None, defaults to 25.
                   This is typically overridden by BATCH_SIZE in two_step_processor_taxonomy.py
    """
    # Use default value if none provided
    if batch_size is None:
        batch_size = 25
    # Initialize skipped userids if not already done
    if not hasattr(get_resume_batch, 'skipped_userids'):
        get_resume_batch.skipped_userids = set()
        
    server_ip = '172.19.115.25'
    database = 'BH_Mirror'
    username = 'silver'
    password = 'ltechmatlen' 
    
    try:
        # Connect to the database
        connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server_ip};DATABASE={database};UID={username};PWD={password}'
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()
        
        # Create the skipped IDs string for the IN clause
        skipped_ids_str = ','.join(str(id) for id in get_resume_batch.skipped_userids) or '0'
        
        # Query to get unprocessed resumes - fix for the correct table name
        query = f"""
            SELECT TOP {batch_size} 
                    userid,
                    markdownResume as cleaned_resume
                FROM dbo.aicandidate WITH (NOLOCK)
                WHERE LastProcessed IS NULL
                    AND userid NOT IN ({skipped_ids_str})
                    AND markdownresume <> ''
                    AND markdownresume IS NOT NULL
            ORDER BY datelastmodified desc
        """
        
        cursor.execute(query)
        rows = cursor.fetchall()
        
        resume_batch = []
        if rows:
            for row in rows:
                userid = row[0]
                cleaned_resume = row[1]
                
                if cleaned_resume and len(str(cleaned_resume).strip()) > 0:
                    resume_batch.append((userid, cleaned_resume))
                    logging.info(f"Added UserID {userid} to batch (resume length: {len(cleaned_resume)})")
                else:
                    logging.warning(f"Empty resume text for UserID {userid} - skipping")
                    get_resume_batch.skipped_userids.add(userid)
            
            logging.info(f"Retrieved {len(resume_batch)} valid resumes for processing")
        else:
            logging.info("No unprocessed records found")
            get_resume_batch.skipped_userids.clear()
            
        cursor.close()
        conn.close()
        
        return resume_batch
        
    except Exception as e:
        logging.error(f"Error retrieving resume batch: {str(e)}")
        return []

def get_resume_by_userid(userid):
    """
    Get a specific resume by user ID
    
    Args:
        userid: The user ID to retrieve
        
    Returns:
        A tuple of (userid, resume_text) or None if not found
    """
    server_ip = '172.19.115.25'
    database = 'BH_Mirror'
    username = 'silver'
    password = 'ltechmatlen'
    
    try:
        # Connect to the database
        connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server_ip};DATABASE={database};UID={username};PWD={password}'
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()
        
        # Query to get the specific resume
        query = """
            SELECT userid, markdownResume as cleaned_resume
            FROM dbo.aicandidate WITH (NOLOCK)
            WHERE userid = ?
        """
        
        cursor.execute(query, userid)
        row = cursor.fetchone()
        
        if row:
            userid = row[0]
            cleaned_resume = row[1]
            
            if cleaned_resume and len(str(cleaned_resume).strip()) > 0:
                logging.info(f"Retrieved resume for UserID {userid} (resume length: {len(cleaned_resume)})")
                return (userid, cleaned_resume)
            else:
                logging.warning(f"Empty resume text for UserID {userid}")
                return None
        else:
            logging.error(f"No record found for UserID {userid}")
            return None
            
        cursor.close()
        conn.close()
        
    except Exception as e:
        logging.error(f"Error retrieving resume for UserID {userid}: {str(e)}")
        return None

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

    
    issues_found = []
    
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
    """Update the aicandidate table with parsed resume data with deadlock retry"""
    server_ip = '172.19.115.25'
    database = 'BH_Mirror'
    username = 'silver'
    password = 'ltechmatlen'

    # Define values that should be treated as null
    null_values = {'NULL'}

    # Max text length for trimming
    max_text_length = 7000  # Adjust this based on your database schema

    logging.info(f"[DB] Preparing to update database for UserID {userid}")
    retry_count = 0
    while retry_count < max_retries:
        try:
            # Connect to the database
            logging.info(f"[DB] Connecting to database server {server_ip}, database {database}")
            connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server_ip};DATABASE={database};UID={username};PWD={password}'
            conn = pyodbc.connect(connection_string)
            cursor = conn.cursor()
            logging.info(f"[DB] Successfully connected to database")
            
            # Start a transaction
            conn.autocommit = False
            logging.info(f"[DB] Starting database transaction")
            
            # Check if the record already exists
            check_query = "SELECT COUNT(*) FROM aicandidate WHERE userid = ?"
            logging.info(f"[DB] Executing query: {check_query} with params: [{userid}]")
            cursor.execute(check_query, userid)
            exists = cursor.fetchone()[0] > 0
            logging.info(f"[DB] Record exists check result: {exists}")
            
            # Prepare field lists and parameter markers for the SQL query
            fields = []
            params = []
            param_markers = []
            
            # Add userid to fields and params
            fields.append("userid")
            params.append(userid)
            param_markers.append("?")
            
            # Process each field in parsed_data
            skipped_fields = []
            for field, value in parsed_data.items():
                # Skip empty or null values if updating
                if exists and (value in null_values or value == ""):
                    skipped_fields.append(field)
                    continue
                
                # Handle case sensitivity issues in field names
                db_field = field
                if field == "ZipCode":
                    db_field = "Zipcode"  # Match the actual column name in the database
                    logging.info(f"[DB] Field name corrected: Using 'Zipcode' instead of 'ZipCode' for database")
                
                # Special handling for date fields
                date_fields = ["MostRecentStartDate", "MostRecentEndDate", "SecondMostRecentStartDate", "SecondMostRecentEndDate", 
                              "ThirdMostRecentStartDate", "ThirdMostRecentEndDate", "FourthMostRecentStartDate", "FourthMostRecentEndDate", 
                              "FifthMostRecentStartDate", "FifthMostRecentEndDate", "SixthMostRecentStartDate", "SixthMostRecentEndDate", 
                              "SeventhMostRecentStartDate", "SeventhMostRecentEndDate"]
                
                if field in date_fields and not is_valid_sql_date(value):
                    # Skip this field for database update as SQL Server won't accept it
                    logging.info(f"[DB] Skipping field {field} with value '{value}' as it's not a valid SQL date format")
                    skipped_fields.append(field)
                    continue
                
                fields.append(db_field)
                
                # Convert empty strings and "NULL" to None for SQL NULL
                if value in null_values or value == "":
                    params.append(None)
                    logging.debug(f"[DB] Field {field}: Converting empty/NULL value to SQL NULL")
                else:
                    # Truncate text fields if needed to prevent SQL errors
                    if isinstance(value, str) and len(value) > max_text_length:
                        logging.warning(f"[DB] Truncating {field} for UserID {userid} from {len(value)} to {max_text_length} characters")
                        params.append(value[:max_text_length])
                    else:
                        params.append(value)
                        # For important fields, log the value being sent to the database
                        if field in ["LengthinUS", "YearsofExperience", "AvgTenure", "PrimaryTitle", "MostRecentCompany", "ZipCode"]:
                            logging.info(f"[DB] Field {field} -> DB field '{db_field}' value: '{value}'")
                
                param_markers.append("?")
            
            # Log skipped fields if any
            if skipped_fields:
                logging.info(f"[DB] Skipped {len(skipped_fields)} empty fields: {', '.join(skipped_fields)}")
            
            # Add LastProcessed timestamp
            current_time = datetime.now()
            fields.append("LastProcessed")
            params.append(current_time)
            param_markers.append("?")
            logging.info(f"[DB] Setting LastProcessed to {current_time}")
            
            # Build and execute the appropriate SQL query (INSERT or UPDATE)
            if exists:
                # UPDATE query
                set_clauses = [f"{field} = ?" for field in fields if field != "userid"]
                query = f"UPDATE aicandidate SET {', '.join(set_clauses)} WHERE userid = ?"
                
                # Prepare parameters: all except userid, then userid at the end
                update_params = [params[i] for i in range(len(fields)) if fields[i] != "userid"]
                update_params.append(userid)  # Add userid for WHERE clause
                
                logging.info(f"[DB] Executing UPDATE query for UserID {userid} with {len(update_params)} parameters")
                logging.debug(f"[DB] SQL: {query}")
                # Log field names being updated
                field_names = [fields[i] for i in range(len(fields)) if fields[i] != "userid"]
                logging.info(f"[DB] Updating fields: {', '.join(field_names)}")
                
                cursor.execute(query, update_params)
                logging.info(f"[DB] UPDATE query executed successfully, rows affected: {cursor.rowcount}")
            else:
                # INSERT query
                query = f"INSERT INTO aicandidate ({', '.join(fields)}) VALUES ({', '.join(param_markers)})"
                logging.info(f"[DB] Executing INSERT query for UserID {userid} with {len(params)} parameters")
                logging.debug(f"[DB] SQL: {query}")
                logging.info(f"[DB] Inserting fields: {', '.join(fields)}")
                
                cursor.execute(query, params)
                logging.info(f"[DB] INSERT query executed successfully, rows affected: {cursor.rowcount}")
            
            # Commit the transaction
            logging.info(f"[DB] Committing transaction")
            conn.commit()
            logging.info(f"[DB] Transaction committed successfully")
            
            cursor.close()
            conn.close()
            logging.info(f"[DB] Database connection closed")
            
            return True
            
        except pyodbc.Error as e:
            error_message = str(e)
            error_code = getattr(e, 'args', [None])[0] if hasattr(e, 'args') else 'Unknown'
            
            # Check specifically for deadlock error
            if "deadlock" in error_message.lower() or "40001" in error_message:
                retry_count += 1
                logging.warning(f"[DB] Database deadlock detected for UserID {userid}. Error code: {error_code}. Retry {retry_count}/{max_retries}.")
                logging.warning(f"[DB] Deadlock error details: {error_message}")
                
                # Exponential backoff
                sleep_time = 0.5 * (2 ** retry_count)  # 1, 2, 4, 8... seconds
                logging.info(f"[DB] Waiting {sleep_time}s before retry")
                time.sleep(sleep_time)
                
                continue  # Try again
            else:
                # Other database error
                logging.error(f"[DB] Database error for UserID {userid}: Error code: {error_code}")
                logging.error(f"[DB] Error message: {error_message}")
                
                # Examine common database error types
                if "syntax error" in error_message.lower() or "invalid column" in error_message.lower() or "column name" in error_message.lower():
                    logging.error(f"[DB] SQL syntax or column error detected - check field names and values")
                    if exists:
                        logging.error(f"[DB] Fields being updated: {', '.join([fields[i] for i in range(len(fields)) if fields[i] != 'userid'])}")
                    else:
                        logging.error(f"[DB] Fields being inserted: {', '.join(fields)}")
                elif "data type" in error_message.lower() or "convert" in error_message.lower():
                    logging.error(f"[DB] Data type conversion error detected - check field data types")
                    # Try to identify problematic fields by checking numeric fields
                    for field in ["LengthinUS", "YearsofExperience", "AvgTenure"]:
                        if field in parsed_data:
                            val = parsed_data[field]
                            if val:
                                logging.error(f"[DB] Field {field} value: '{val}' (type: {type(val).__name__})")
                                try:
                                    # Try to convert to float to check if it's a valid number
                                    float_val = float(val)
                                    logging.error(f"[DB] {field} can be converted to float: {float_val}")
                                except ValueError:
                                    logging.error(f"[DB] {field} CANNOT be converted to a valid number!")
                elif "violation of primary key" in error_message.lower():
                    logging.error(f"[DB] Primary key violation - record with userid {userid} might already exist")
                elif "string or binary data would be truncated" in error_message.lower():
                    logging.error(f"[DB] String truncation error - a field value is too long for its column")
                    # Check for long field values
                    for field, value in parsed_data.items():
                        if isinstance(value, str) and len(value) > 100:
                            logging.error(f"[DB] Field {field} has long value: {len(value)} characters")
                elif "constraint" in error_message.lower():
                    logging.error(f"[DB] Constraint violation - a field value violates a database constraint")
                
                # Log the transaction size
                logging.error(f"[DB] Transaction contained {len(fields)} fields")
                return False
                
        except Exception as e:
            # Any other error
            import traceback
            logging.error(f"[DB] Error updating record for UserID {userid}: {str(e)}")
            logging.error(f"[DB] Error traceback: {traceback.format_exc()}")
            return False
            
    # If we've exhausted retries
    logging.error(f"[DB] Failed to update record for UserID {userid} after {max_retries} retries due to deadlocks.")
    return False