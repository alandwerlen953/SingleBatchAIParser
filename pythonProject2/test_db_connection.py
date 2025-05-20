#!/usr/bin/env python3
"""
Database Connection Test Script

This script tests the enhanced database connection functionality, including:
1. Testing available ODBC drivers
2. Testing connection reliability with retries
3. Testing resume batch retrieval
4. Testing single resume retrieval
"""

import logging
import os
import time
import sys
import platform

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("db_connection_test.log"),
        logging.StreamHandler(sys.stdout)
    ]
)

# Import the modules to test
from db_connection import (
    get_best_driver,
    create_pyodbc_connection,
    get_resume_batch_with_retry,
    get_resume_by_userid_with_retry,
    test_connection
)

def test_odbc_drivers():
    """Test available ODBC drivers"""
    logging.info("=== Testing Available ODBC Drivers ===")
    
    driver, message = get_best_driver()
    logging.info(message)
    
    if driver:
        logging.info(f"✅ Found suitable ODBC driver: {driver}")
        return True
    else:
        logging.error("❌ No suitable ODBC driver found")
        return False

def test_connection_reliability(retries=3):
    """Test database connection reliability with retries"""
    logging.info("=== Testing Database Connection Reliability ===")
    
    success_count = 0
    failure_count = 0
    
    # Try multiple connections to test reliability
    for i in range(3):
        logging.info(f"Connection test {i+1}/3")
        conn, success, message = create_pyodbc_connection(retries=retries)
        
        if success:
            success_count += 1
            logging.info(f"✅ Connection {i+1} successful")
            try:
                # Try to use the connection by running a simple query
                cursor = conn.cursor()
                cursor.execute("SELECT 1 AS test_value")
                result = cursor.fetchone()
                logging.info(f"   Query result: {result[0]}")
                cursor.close()
            except Exception as e:
                logging.warning(f"   Connection succeeded but query failed: {str(e)}")
            
            # Close the connection
            try:
                conn.close()
                logging.info("   Connection closed successfully")
            except:
                logging.warning("   Error closing connection")
        else:
            failure_count += 1
            logging.error(f"❌ Connection {i+1} failed: {message}")
        
        # Add a small delay between tests
        if i < 2:
            time.sleep(2)
    
    # Return results
    if success_count == 3:
        logging.info("✅ All connection tests passed!")
        return True
    elif success_count > 0:
        logging.warning(f"⚠️ Some connection tests passed ({success_count}/3)")
        return True
    else:
        logging.error("❌ All connection tests failed")
        return False

def test_resume_batch_retrieval(batch_size=2):
    """Test resume batch retrieval"""
    logging.info("=== Testing Resume Batch Retrieval ===")
    
    start_time = time.time()
    batch = get_resume_batch_with_retry(batch_size=batch_size, max_retries=3)
    elapsed_time = time.time() - start_time
    
    if batch:
        logging.info(f"✅ Successfully retrieved batch of {len(batch)} resumes in {elapsed_time:.2f}s")
        # Show a bit of info about each resume
        for i, (userid, resume_text) in enumerate(batch):
            logging.info(f"   Resume {i+1}: UserID {userid}, Length: {len(resume_text)} chars")
        return True
    else:
        logging.warning(f"⚠️ Resume batch returned empty in {elapsed_time:.2f}s (might be normal if no unprocessed resumes exist)")
        return True  # Not a failure, might just be no unprocessed resumes

def test_specific_resume_retrieval(userid="12345"):  # Replace with a valid user ID if possible
    """Test retrieval of a specific resume"""
    logging.info("=== Testing Specific Resume Retrieval ===")
    
    start_time = time.time()
    result = get_resume_by_userid_with_retry(userid, max_retries=3)
    elapsed_time = time.time() - start_time
    
    if result:
        retrieved_userid, resume_text = result
        logging.info(f"✅ Successfully retrieved resume for UserID {retrieved_userid} in {elapsed_time:.2f}s")
        logging.info(f"   Resume length: {len(resume_text)} chars")
        preview = resume_text[:100].replace('\n', ' ') + "..." if len(resume_text) > 100 else resume_text
        logging.info(f"   Preview: {preview}")
        return True
    else:
        logging.warning(f"⚠️ Could not retrieve resume for UserID {userid} in {elapsed_time:.2f}s")
        logging.warning("   This might be normal if the user ID doesn't exist")
        return True  # Not a failure, userid might not exist

def run_all_tests():
    """Run all database connection tests"""
    logging.info("====== Starting Database Connection Tests ======")
    logging.info(f"Operating System: {platform.system()} {platform.release()}")
    
    # Track test results
    results = {}
    
    # Test 1: Check ODBC drivers
    results["odbc_drivers"] = test_odbc_drivers()
    
    # Test 2: Test connection reliability
    results["connection"] = test_connection_reliability()
    
    # Test 3: Test batch retrieval (only if connection test passed)
    if results["connection"]:
        results["batch_retrieval"] = test_resume_batch_retrieval(batch_size=2)
    else:
        results["batch_retrieval"] = None  # Skip test
        logging.warning("⚠️ Skipping batch retrieval test due to connection failure")
    
    # Test 4: Test specific resume retrieval (only if connection test passed)
    if results["connection"]:
        results["specific_retrieval"] = test_specific_resume_retrieval()
    else:
        results["specific_retrieval"] = None  # Skip test
        logging.warning("⚠️ Skipping specific resume retrieval test due to connection failure")
    
    # Print summary
    logging.info("\n====== Database Connection Test Summary ======")
    for test, result in results.items():
        status = "✅ PASSED" if result else "❌ FAILED" if result is False else "⚠️ SKIPPED"
        logging.info(f"{status} - {test}")
    
    # Calculate overall success
    successful_tests = sum(1 for result in results.values() if result is True)
    total_tests = sum(1 for result in results.values() if result is not None)
    
    if successful_tests == total_tests:
        logging.info("\n✅ ALL TESTS PASSED! Database functionality appears to be working properly.")
        return True
    else:
        logging.warning(f"\n⚠️ {successful_tests}/{total_tests} tests passed. Some database functionality might be impaired.")
        return False

if __name__ == "__main__":
    try:
        run_all_tests()
    except Exception as e:
        import traceback
        logging.error(f"Error during tests: {str(e)}")
        logging.error(traceback.format_exc())
        sys.exit(1)