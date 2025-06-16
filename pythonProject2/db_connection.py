#!/usr/bin/env python3
"""
Database Connection Infrastructure

Provides database connection functionality with improved retry mechanisms.
"""

import logging
import pyodbc
import sys
import os
import time
import platform
from datetime import datetime
import traceback

# Configure module logger
logger = logging.getLogger("resume_parser.db_connection")

# Database connection defaults
DEFAULT_SERVER = '172.19.115.25'
DEFAULT_DATABASE = 'BH_Mirror'
DEFAULT_USERNAME = 'silver'
DEFAULT_PASSWORD = 'ltechmatlen'

# Max retry configuration
MAX_RETRIES = 5
RETRY_BASE_DELAY = 1.0  # Base delay in seconds for exponential backoff

def get_best_driver():
    """
    Find the best available SQL Server driver 
    based on system platform and installed drivers.
    
    Returns:
        tuple: (driver_name, driver_availability_message)
    """
    # Check for available drivers
    available_drivers = pyodbc.drivers()
    selected_driver = None
    
    # List of drivers to try, in order of preference
    driver_candidates = [
        "ODBC Driver 18 for SQL Server",
        "ODBC Driver 17 for SQL Server",
        "ODBC Driver 13 for SQL Server",
        "SQL Server Native Client 11.0",
        "SQL Server"
        # FreeTDS intentionally removed as requested
    ]
    
    message = f"Available ODBC drivers: {', '.join(available_drivers)}"
    logger.info(message)
    
    # Try to find an installed driver
    for driver in driver_candidates:
        if driver in available_drivers:
            selected_driver = driver
            logger.info(f"Selected ODBC driver: {selected_driver}")
            return selected_driver, message
    
    # If no driver found in the candidate list, use the first available one if any exist
    if not selected_driver and available_drivers:
        selected_driver = available_drivers[0]
        logger.info(f"Using alternative ODBC driver: {selected_driver}")
        return selected_driver, message + "\nUsing alternative driver not in preferred list."
    
    # If still no driver found, prepare detailed error message
    error_msg = message + "\nNo suitable SQL Server driver found!"
    
    if platform.system() == 'Linux':
        error_msg += """
To install the Microsoft ODBC Driver for SQL Server on Linux:
For Ubuntu/Debian:
1. Download Microsoft repository config: 
curl https://packages.microsoft.com/keys/microsoft.asc | sudo apt-key add -
curl https://packages.microsoft.com/config/ubuntu/$(lsb_release -rs)/prod.list | sudo tee /etc/apt/sources.list.d/msprod.list

2. Install the ODBC Driver:
sudo apt-get update
sudo ACCEPT_EULA=Y apt-get install msodbcsql17
"""
    elif platform.system() == 'Windows':
        error_msg += """
To install the Microsoft ODBC Driver for SQL Server on Windows:
Download from: https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server
"""
    else:
        error_msg += "Please install an appropriate ODBC driver for SQL Server on your platform."
    
    return None, error_msg

def create_connection_string(server=DEFAULT_SERVER, database=DEFAULT_DATABASE, 
                             username=DEFAULT_USERNAME, password=DEFAULT_PASSWORD):
    """
    Create a connection string for SQL Server
    
    Args:
        server: Database server address
        database: Database name
        username: Database username
        password: Database password
        
    Returns:
        tuple: (connection_string, message)
    """
    # Get the best driver
    driver, message = get_best_driver()
    
    if not driver:
        return None, message
    
    # Build the connection string
    connection_string = (
        f'DRIVER={{{driver}}};'
        f'SERVER={server};'
        f'DATABASE={database};'
        f'UID={username};'
        f'PWD={password};'
        f'Timeout=60;'  # Connection timeout
        f'Connection Timeout=60'  # Alternative syntax
    )
    
    return connection_string, message

def create_pyodbc_connection(server=DEFAULT_SERVER, database=DEFAULT_DATABASE, 
                           username=DEFAULT_USERNAME, password=DEFAULT_PASSWORD,
                           retries=MAX_RETRIES):
    """
    Create a PYODBC connection with retry logic.
    
    Args:
        server: Database server address
        database: Database name
        username: Database username
        password: Database password
        retries: Number of connection retry attempts
        
    Returns:
        tuple: (connection, success_flag, message)
    """
    # Create connection string
    connection_string, message = create_connection_string(server, database, username, password)
    
    if not connection_string:
        return None, False, message
    
    # Try to connect with retries
    for attempt in range(1, retries + 1):
        try:
            logger.info(f"Connection attempt {attempt}/{retries} to database {database} on server {server}")
            
            # Connect with autocommit enabled
            conn = pyodbc.connect(connection_string, autocommit=True)
            
            # Test the connection
            cursor = conn.cursor()
            try:
                logger.info("Testing database connection...")
                cursor.execute("SELECT 1")
                cursor.fetchone()
                logger.info("Connection test successful")
                
                # Set session options for better performance and reliability
                try:
                    cursor.execute("SET ARITHABORT ON")
                    cursor.execute("SET LOCK_TIMEOUT 60000")  # 60 second lock timeout
                    # QUERY_TIMEOUT is not supported in this SQL Server version
                    # cursor.execute("SET QUERY_TIMEOUT 300")   # 5 minute query timeout
                except Exception as e:
                    logger.warning(f"Could not set all SQL Server options (non-critical): {e}")
            finally:
                cursor.close()
            
            return conn, True, "Connection successful"
            
        except pyodbc.Error as e:
            error_message = str(e)
            error_code = getattr(e, 'args', [None])[0] if hasattr(e, 'args') else 'Unknown'
            
            # Check for specific error types
            if "login" in error_message.lower() or "authentication" in error_message.lower():
                # Authentication issues - don't retry
                message = f"Authentication error: {error_message} (Error code: {error_code})"
                logger.error(message)
                return None, False, message
                
            elif "server not found" in error_message.lower() or "network" in error_message.lower():
                # Network or server availability issues - retry with longer delay
                message = f"Network or server error: {error_message} (Error code: {error_code})"
                logger.warning(f"{message}. Attempt {attempt}/{retries}")
                
                if attempt < retries:
                    sleep_time = RETRY_BASE_DELAY * (2 ** (attempt - 1))  # Exponential backoff
                    logger.info(f"Waiting {sleep_time}s before retry")
                    time.sleep(sleep_time)
                    continue
                else:
                    return None, False, f"{message}. Max retries exceeded."
                    
            elif "timeout" in error_message.lower():
                # Timeout issues - retry with longer delay
                message = f"Connection timeout: {error_message} (Error code: {error_code})"
                logger.warning(f"{message}. Attempt {attempt}/{retries}")
                
                if attempt < retries:
                    sleep_time = RETRY_BASE_DELAY * (2 ** (attempt - 1))
                    logger.info(f"Waiting {sleep_time}s before retry")
                    time.sleep(sleep_time)
                    continue
                else:
                    return None, False, f"{message}. Max retries exceeded."
            
            else:
                # Other database errors - may or may not be worth retrying
                message = f"Database error: {error_message} (Error code: {error_code})"
                logger.error(message)
                
                if attempt < retries:
                    sleep_time = RETRY_BASE_DELAY * (2 ** (attempt - 1))
                    logger.info(f"Waiting {sleep_time}s before retry")
                    time.sleep(sleep_time)
                    continue
                else:
                    return None, False, f"{message}. Max retries exceeded."
                
        except Exception as e:
            # Unexpected errors
            message = f"Unexpected error connecting to database: {str(e)}"
            logger.error(message)
            logger.error(f"Traceback: {traceback.format_exc()}")
            return None, False, message
    
    # Should not reach here, but just in case
    return None, False, "Failed to connect after exhausting all retry attempts."

def execute_query_with_retry(conn, query, params=None, retries=MAX_RETRIES):
    """
    Execute a SQL query with retry logic for transient errors
    
    Args:
        conn: pyodbc connection
        query: SQL query string
        params: Parameters for the query
        retries: Number of retry attempts
        
    Returns:
        tuple: (success_flag, result, message)
    """
    params = params if params is not None else []
    
    for attempt in range(1, retries + 1):
        try:
            cursor = conn.cursor()
            cursor.execute(query, params)
            
            # For SELECT queries, fetch results
            if query.strip().upper().startswith('SELECT'):
                result = cursor.fetchall()
                cursor.close()
                return True, result, "Query executed successfully"
            else:
                # For non-SELECT queries, return number of affected rows
                rowcount = cursor.rowcount
                cursor.close()
                return True, rowcount, f"Query affected {rowcount} rows"
                
        except pyodbc.Error as e:
            error_message = str(e)
            error_code = getattr(e, 'args', [None])[0] if hasattr(e, 'args') else 'Unknown'
            
            # Handle deadlocks, timeouts, and connection-related errors
            if "deadlock" in error_message.lower() or "40001" in error_message:
                logger.warning(f"Deadlock detected - Error {error_code}: {error_message}")
                
                if attempt < retries:
                    sleep_time = RETRY_BASE_DELAY * (2 ** (attempt - 1))  # Exponential backoff
                    logger.info(f"Waiting {sleep_time}s before retry {attempt+1}/{retries}")
                    time.sleep(sleep_time)
                    continue
                else:
                    return False, None, f"Query failed after {retries} attempts due to deadlocks"
                    
            elif "timeout" in error_message.lower():
                logger.warning(f"Query timeout - Error {error_code}: {error_message}")
                
                if attempt < retries:
                    sleep_time = RETRY_BASE_DELAY * (2 ** (attempt - 1))
                    logger.info(f"Waiting {sleep_time}s before retry")
                    time.sleep(sleep_time)
                    continue
                else:
                    return False, None, f"Query timed out after {retries} attempts"
                
            else:
                # Other database errors - detailed logging
                logger.error(f"Database query error - Error {error_code}: {error_message}")
                
                # Add more detailed error analysis
                if "syntax error" in error_message.lower():
                    logger.error(f"SQL syntax error in query: {query}")
                elif "invalid column" in error_message.lower() or "invalid object" in error_message.lower():
                    logger.error(f"Invalid column or object in query: {query}")
                elif "permission" in error_message.lower():
                    logger.error(f"Permission denied for query: {query}")
                
                return False, None, f"Query failed: {error_message}"
                
        except Exception as e:
            # Unexpected errors
            logger.error(f"Unexpected error executing query: {str(e)}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            return False, None, f"Unexpected error: {str(e)}"
        
    # Should not reach here, but just in case
    return False, None, "Failed to execute query after exhausting all retry attempts."

def update_candidate_record(userid, parsed_data, max_retries=3):
    """
    Update the aicandidate table with parsed resume data with enhanced error handling and retry logic.
    
    Args:
        userid: User ID to update
        parsed_data: Dictionary of field values to update
        max_retries: Maximum number of update attempts
        
    Returns:
        tuple: (success_flag, message)
    """
    # Process parsed_data to ensure valid format
    try:
        # Ensure all keys are strings
        for key in list(parsed_data.keys()):
            if not isinstance(key, str):
                value = parsed_data[key]
                del parsed_data[key]
                parsed_data[str(key)] = value
        
        # Fix field names to match database columns
        if "ZipCode" in parsed_data and "Zipcode" not in parsed_data:
            parsed_data["Zipcode"] = parsed_data["ZipCode"]
            del parsed_data["ZipCode"]
    except Exception as e:
        logger.error(f"Error preprocessing parsed_data: {str(e)}")
        # Continue anyway - we've done our best to clean the data
    
    # Field-specific length limits based on actual database schema
    field_limits = {
        # nvarchar(max) fields - no limit
        'Summary': None,
        'Certifications': None,
        'ProjectTypes': None,
        'Specialty': None,
        'resume': None,
        'markdownresume': None,
        
        # Limited length fields
        'PrimaryTitle': 255,
        'SecondaryTitle': 255,
        'TertiaryTitle': 255,
        'Address': 255,
        'City': 100,
        'State': 50,
        'Bachelors': 255,
        'Masters': 255,
        'Phone1': 50,
        'Phone2': 50,
        'Email': 255,
        'Email2': 255,
        'FirstName': 100,
        'MiddleName': 100,
        'LastName': 100,
        'LinkedIn': 255,
        'Linkedin': 255,  # Case variation
        'MostRecentCompany': 255,
        'SecondMostRecentCompany': 255,
        'ThirdMostRecentCompany': 255,
        'FourthMostRecentCompany': 255,
        'FifthMostRecentCompany': 255,
        'SixthMostRecentCompany': 255,
        'SeventhMostRecentCompany': 255,
        'MostRecentLocation': 255,
        'SecondMostRecentLocation': 255,
        'ThirdMostRecentLocation': 255,
        'FourthMostRecentLocation': 255,
        'FifthMostRecentLocation': 255,
        'SixthMostRecentLocation': 255,
        'SeventhMostRecentLocation': 255,
        'PrimaryIndustry': 255,
        'SecondaryIndustry': 255,
        'Skill1': 100,
        'Skill2': 100,
        'Skill3': 100,
        'Skill4': 100,
        'Skill5': 100,
        'Skill6': 100,
        'Skill7': 100,
        'Skill8': 100,
        'Skill9': 100,
        'Skill10': 100,
        'PrimarySoftwareLanguage': 255,
        'SecondarySoftwareLanguage': 255,
        'TertiarySoftwareLanguage': 255,
        'SoftwareApp1': 255,
        'SoftwareApp2': 255,
        'SoftwareApp3': 255,
        'SoftwareApp4': 255,
        'SoftwareApp5': 255,
        'Hardware1': 255,
        'Hardware2': 255,
        'Hardware3': 255,
        'Hardware4': 255,
        'Hardware5': 255,
        'PrimaryCategory': 255,
        'SecondaryCategory': 255,
        'LengthinUS': 50,
        'YearsofExperience': 50,
        'AvgTenure': 50,
        'status': 50,
        'employeetype': 100,
        'Zipcode': 9,
        'MostRecentPlacementTitle': 255,
        'MostRecentPlacementClient': 255
    }
    
    # Default max length for unknown fields
    default_max_length = 255
    
    # First establish connection
    conn, conn_success, conn_message = create_pyodbc_connection(retries=max_retries)
    
    if not conn_success:
        return False, f"Failed to connect to database: {conn_message}"
    
    try:
        # Check if the record already exists
        check_query = "SELECT COUNT(*) FROM aicandidate WITH (NOLOCK) WHERE userid = ?"
        success, result, message = execute_query_with_retry(conn, check_query, [userid])
        
        if not success:
            conn.close()
            return False, f"Failed to check if record exists: {message}"
        
        exists = result[0][0] > 0
        logger.info(f"Record for UserID {userid} exists check: {exists}")
        
        # Prepare for update or insert
        fields = []
        params = []
        
        # Process fields and values
        for field, value in parsed_data.items():
            # Normalize field name
            db_field = "Zipcode" if field == "ZipCode" else field
            
            # Handle NULL values
            if value == "NULL" or value == "":
                if exists:
                    # Skip empty values for UPDATE to preserve existing data
                    continue
                else:
                    # For INSERT, include as NULL
                    fields.append(db_field)
                    params.append(None)
                continue
            
            # Handle date fields
            date_fields = ["MostRecentStartDate", "MostRecentEndDate", "SecondMostRecentStartDate", 
                           "SecondMostRecentEndDate", "ThirdMostRecentStartDate", "ThirdMostRecentEndDate", 
                           "FourthMostRecentStartDate", "FourthMostRecentEndDate", "FifthMostRecentStartDate", 
                           "FifthMostRecentEndDate", "SixthMostRecentStartDate", "SixthMostRecentEndDate", 
                           "SeventhMostRecentStartDate", "SeventhMostRecentEndDate"]
            
            if field in date_fields:
                if value == "Present" or not value:
                    # Skip non-SQL-compatible dates
                    continue
                try:
                    # Validate date format
                    datetime.strptime(value, "%Y-%m-%d")
                except ValueError:
                    # Skip invalid dates
                    logger.warning(f"Skipping invalid date in field {field}: '{value}'")
                    continue
            
            # Process text fields with field-specific limits
            if isinstance(value, str):
                # Get field-specific limit
                limit = field_limits.get(db_field, default_max_length)
                
                # Only truncate if there's a limit and value exceeds it
                if limit and len(value) > limit:
                    logger.warning(f"Truncating field {db_field} from {len(value)} to {limit} characters")
                    params.append(value[:limit])
                else:
                    params.append(value)
            else:
                params.append(value)
            
            fields.append(db_field)
        
        # Add LastProcessed timestamp
        fields.append("LastProcessed")
        params.append(datetime.now())
        
        # Execute update or insert
        if exists:
            # Build UPDATE statement
            set_clauses = [f"{field} = ?" for field in fields]
            query = f"UPDATE aicandidate SET {', '.join(set_clauses)} WHERE userid = ?"
            
            # Add userid parameter for the WHERE clause
            params.append(userid)
            
            logger.info(f"Executing UPDATE for UserID {userid} with {len(fields)} fields")
            success, result, message = execute_query_with_retry(conn, query, params, retries=max_retries)
            
            if not success:
                conn.close()
                return False, f"Failed to update record: {message}"
            
            if result == 0:
                logger.warning(f"UPDATE query succeeded but no rows affected for UserID {userid}")
            
        else:
            # Build INSERT statement including userid
            fields.insert(0, "userid")
            params.insert(0, userid)
            
            param_markers = ["?"] * len(fields)
            query = f"INSERT INTO aicandidate ({', '.join(fields)}) VALUES ({', '.join(param_markers)})"
            
            logger.info(f"Executing INSERT for UserID {userid} with {len(fields)} fields")
            success, result, message = execute_query_with_retry(conn, query, params, retries=max_retries)
            
            if not success:
                conn.close()
                return False, f"Failed to insert record: {message}"
        
        logger.info(f"Database update successful for UserID {userid}")
        conn.close()
        return True, "Record updated successfully"
        
    except Exception as e:
        logger.error(f"Unexpected error in update_candidate_record: {str(e)}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        
        try:
            conn.close()
        except:
            pass
            
        return False, f"Unexpected error: {str(e)}"

# Utility function to get a batch of resumes with retry logic
def get_resume_batch_with_retry(batch_size=25, max_retries=3, reset_skipped=True):
    """
    Get a batch of unprocessed resumes with retry logic
    
    Args:
        batch_size: Number of resumes to retrieve
        max_retries: Maximum number of connection/query attempts
        reset_skipped: Whether to reset the skipped userids set
        
    Returns:
        list: List of (userid, resume_text) tuples
    """
    # Initialize skipped userids if not already done
    if not hasattr(get_resume_batch_with_retry, 'skipped_userids'):
        get_resume_batch_with_retry.skipped_userids = set()
    
    # Reset skipped userids if requested
    if reset_skipped:
        get_resume_batch_with_retry.skipped_userids.clear()
        logger.info("Reset skipped userids list")
    
    # First establish connection
    conn, conn_success, conn_message = create_pyodbc_connection(retries=max_retries)
    
    if not conn_success:
        logger.error(f"Failed to connect to database: {conn_message}")
        return []
    
    try:
        # Create the skipped IDs string for the IN clause
        skipped_ids = get_resume_batch_with_retry.skipped_userids
        skipped_ids_str = ','.join(str(id) for id in skipped_ids) or '0'
        
        # Log current skipped IDs for debugging
        if skipped_ids:
            logger.info(f"Currently skipped userids ({len(skipped_ids)}): {sorted(skipped_ids)}")
        else:
            logger.info("No userids currently in skipped list")
        
        # Query to get ALL unprocessed resumes from the last 3 days where markdownResume is processed but not LastProcessed
        # Removed TOP clause to process all matching records
        # Fixed date comparison to use date-only comparison for better matching
        query = f"""
            SELECT 
                userid,
                markdownResume as cleaned_resume
            FROM dbo.aicandidate WITH (NOLOCK)
            WHERE LastProcessed IS NULL
                AND markdownresume <> ''
                AND markdownresume IS NOT NULL
                AND lastprocessedmarkdown IS NOT NULL
                AND CAST(lastprocessedmarkdown AS DATE) >= CAST(DATEADD(day, -3, GETDATE()) AS DATE)
            ORDER BY lastprocessedmarkdown desc
        """
        
        # Execute query with retry logic
        success, result, message = execute_query_with_retry(conn, query, retries=max_retries)
        
        if not success:
            logger.error(f"Failed to get resume batch: {message}")
            conn.close()
            return []
        
        resume_batch = []
        if result:
            # Log all userids found by the query
            found_userids = [row[0] for row in result]
            logger.info(f"SQL query found {len(found_userids)} total records: {sorted(found_userids)}")
            
            for row in result:
                userid = row[0]
                cleaned_resume = row[1]
                
                if cleaned_resume and len(str(cleaned_resume).strip()) > 0:
                    resume_batch.append((userid, cleaned_resume))
                    logger.info(f"Added UserID {userid} to batch (resume length: {len(cleaned_resume)})")
                else:
                    logger.warning(f"Empty resume text for UserID {userid} - skipping")
                    get_resume_batch_with_retry.skipped_userids.add(userid)
            
            logger.info(f"Retrieved {len(resume_batch)} valid resumes for processing")
        else:
            logger.info("No unprocessed records found")
            get_resume_batch_with_retry.skipped_userids.clear()
        
        conn.close()
        return resume_batch
        
    except Exception as e:
        logger.error(f"Error retrieving resume batch: {str(e)}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        
        try:
            conn.close()
        except:
            pass
            
        return []

# Utility function to get a single resume by ID with retry logic
def get_resume_by_userid_with_retry(userid, max_retries=3):
    """
    Get a specific resume by user ID with retry logic
    
    Args:
        userid: The user ID to retrieve
        max_retries: Maximum number of connection/query attempts
        
    Returns:
        tuple: (userid, resume_text) or None if not found
    """
    # First establish connection
    conn, conn_success, conn_message = create_pyodbc_connection(retries=max_retries)
    
    if not conn_success:
        logger.error(f"Failed to connect to database: {conn_message}")
        return None
    
    try:
        # Query to get the specific resume
        query = """
            SELECT userid, markdownResume as cleaned_resume
            FROM dbo.aicandidate WITH (NOLOCK)
            WHERE userid = ?
        """
        
        # Execute query with retry logic
        success, result, message = execute_query_with_retry(conn, query, [userid], retries=max_retries)
        
        if not success:
            logger.error(f"Failed to get resume for UserID {userid}: {message}")
            conn.close()
            return None
        
        if not result:
            logger.error(f"No record found for UserID {userid}")
            conn.close()
            return None
        
        row = result[0]
        userid = row[0]
        cleaned_resume = row[1]
        
        if cleaned_resume and len(str(cleaned_resume).strip()) > 0:
            logger.info(f"Retrieved resume for UserID {userid} (resume length: {len(cleaned_resume)})")
            conn.close()
            return (userid, cleaned_resume)
        else:
            logger.warning(f"Empty resume text for UserID {userid}")
            conn.close()
            return None
            
    except Exception as e:
        logger.error(f"Error retrieving resume for UserID {userid}: {str(e)}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        
        try:
            conn.close()
        except:
            pass
            
        return None

# Test connection and available drivers
def test_connection():
    """Test database connection and report on available drivers"""
    drivers = pyodbc.drivers()
    logger.info(f"Available ODBC drivers: {', '.join(drivers)}")
    
    driver, message = get_best_driver()
    logger.info(message)
    
    if driver:
        # Test connection
        conn, success, message = create_pyodbc_connection()
        if success:
            logger.info("✅ Database connection test successful!")
            conn.close()
            return True
        else:
            logger.error(f"❌ Database connection test failed: {message}")
            return False
    else:
        logger.error("❌ No suitable SQL Server driver found")
        return False

# For standalone testing
if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    )
    
    # Test the connection
    if test_connection():
        logger.info("Database connection infrastructure is working properly")
    else:
        logger.error("Database connection test failed")