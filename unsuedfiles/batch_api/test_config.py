"""
Test configuration for Batch API processor
This script verifies that all required components are properly set up
"""

import os
import sys
import json
import logging
import pyodbc
import openai
from dotenv import load_dotenv

# Add parent directory to path so we can import from the main project
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)

# Import our robust db connection function
from db_connection import create_pyodbc_connection

# Try importing required modules
try:
    from resume_utils import update_candidate_record_with_retry, DEFAULT_MODEL
    from two_step_prompts_taxonomy import create_step1_prompt, create_step2_prompt
    from two_step_processor_taxonomy import parse_step1_response, parse_step2_response
    from date_processor import process_resume_with_enhanced_dates
    print("✅ Successfully imported required modules")
except ImportError as e:
    print(f"❌ Failed to import required modules: {str(e)}")
    sys.exit(1)

# Load environment variables
load_dotenv()

# Check OpenAI API key
api_key = os.getenv('OPENAI_API_KEY')
if not api_key:
    print("❌ OPENAI_API_KEY not found in environment variables")
    sys.exit(1)
print("✅ Found OPENAI_API_KEY in environment variables")

# Set up OpenAI client
openai.api_key = api_key

# Check database connection using robust connection function
try:
    # Connect to the database using the robust connection function
    conn = create_pyodbc_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT TOP 1 userid FROM dbo.aicandidate")
    cursor.fetchone()
    cursor.close()
    conn.close()
    print(f"✅ Successfully connected to database BH_Mirror")
except Exception as e:
    print(f"❌ Failed to connect to database: {str(e)}")
    sys.exit(1)

# Check if we can create a file in the batch_api directory
try:
    test_file = os.path.join(os.path.dirname(__file__), "test_file.txt")
    with open(test_file, 'w') as f:
        f.write("Test file for batch API processor")
    os.remove(test_file)
    print("✅ Successfully created and removed test file")
except Exception as e:
    print(f"❌ Failed to create test file: {str(e)}")
    sys.exit(1)

# Test OpenAI API connection
try:
    # Just check models list to verify API connection
    models = openai.models.list()
    print("✅ Successfully connected to OpenAI API")
except Exception as e:
    print(f"❌ Failed to connect to OpenAI API: {str(e)}")
    sys.exit(1)

print("\n🎉 All configurations are valid. You can now run the batch processor.")
print("   To submit a batch: python batch_api/processor.py --submit")
print("   To check batches:  python batch_api/processor.py --check")