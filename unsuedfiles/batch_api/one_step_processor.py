"""
One-step batch processor for resume analysis with OpenAI

This module implements file-based batch processing using OpenAI's Batch API,
enabling cost-effective processing of large volumes with the 50% discount offered
for 24-hour batch processing. It processes both personal info and technical details
in a single step rather than the previous two-step approach.
"""

import os
import json
import time
import logging
import pyodbc
import re
import glob
from typing import List, Dict, Tuple, Any, Optional
from datetime import datetime, timedelta
import tiktoken
import uuid
import openai
from dotenv import load_dotenv
import argparse
import sys

# Add parent directory to path so we can import from the main project
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)

# Add batch_api directory to path as well for local imports
batch_api_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, batch_api_dir)

# Import our robust db connection function
from db_connection import create_pyodbc_connection

# Import our standalone utilities without external dependencies
from batch_api_utils import (
    DEFAULT_MODEL,
    get_resume_batch,
    apply_token_truncation,
    num_tokens_from_string,
    parse_step1_response,
    parse_step2_response
)

# Use the SAME update function as single_step_processor
from resume_utils import update_candidate_record_with_retry

# Import our enhanced helper modules
from skills_detector import get_taxonomy_context
from date_processor import process_resume_with_enhanced_dates

# IMPORTANT: Import the SAME unified prompt and parser from single_step_processor
# This ensures batch API produces identical results to regular processing
from single_step_processor import (
    create_unified_prompt as original_create_unified_prompt,
    parse_unified_response
)

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(os.path.dirname(__file__), "one_step_processor.log")),
        logging.StreamHandler()
    ]
)

# Set up OpenAI client with API key from environment
api_key = os.getenv('OPENAI_API_KEY')
openai.api_key = api_key
if not api_key:
    logging.error("API key is not set in the environment variables.")

# Configuration
BATCH_SIZE = 500  # Default batch size (can be overridden via command line)
MODEL = DEFAULT_MODEL  # Use the same model as the main app
BATCH_STATUS_TABLE = "aicandidateBatchStatus"  # Table to track batch processing status

# Wrapper to use the SAME unified prompt as single_step_processor for consistency
def create_unified_prompt(resume_text, userid=None):
    """
    Wrapper that uses the exact same prompt as single_step_processor
    to ensure batch API produces identical results to regular processing

    Args:
        resume_text: The resume text to analyze
        userid: Optional user ID for tracking

    Returns:
        A list of messages for the chat completion API
    """
    # Use the original create_unified_prompt from single_step_processor
    return original_create_unified_prompt(resume_text, userid)

# ALL OLD PROMPT CODE HAS BEEN REMOVED - WE NOW USE single_step_processor's EXACT PROMPTS

def count_tokens(content: str) -> int:
    """Count the number of tokens in a string"""
    try:
        # Use tiktoken to count tokens
        import tiktoken
        encoding = tiktoken.encoding_for_model("gpt-4")
        return len(encoding.encode(content))
    except Exception:
        # Fallback to character-based estimation
        return len(content) // 4

def setup_batch_status_table(): YYYY-MM-DD if full date is known, YYYY-MM if only month/year, or YYYY if only year is known. For current positions, use 'Present' as the end date. If a date is completely unknown, output NULL.\n"
                       "When identifying skills, software languages, applications, and hardware, prioritize accuracy over standardization. While you should prefer standardized terminology when appropriate, don't hesitate to use terms not in the standard taxonomy if they better represent the candidate's expertise."
        },
        {
            "role": "system",
            "content": f"BACKGROUND INFORMATION:\n\n"
                       f"You are analyzing a professional resume for the first time in this conversation.\n"
                       f"Focus on understanding the candidate's complete professional profile including their technical and non-technical capabilities.\n"
                       f"For technical candidates, pay close attention to languages, frameworks, hardware, and project types.\n"
                       f"For non-technical candidates, focus on business skills, functions performed, and transferable skills.\n"
                       f"Always provide a professional category for every candidate - never return NULL for categories."
        },
        {
            "role": "system",
            "content": f"SKILLS TAXONOMY INTERPRETATION GUIDANCE:\n"
                       f"The skills taxonomy provides standardized categorization of technical skills for this resume.\n"
                       f"Use this taxonomy to guide your analysis of programming languages, software applications, and hardware.\n"
                       f"When identifying skills, prefer terminology from the appropriate taxonomy categories, but don't hesitate to use different terms when they better represent the candidate's expertise.\n"
                       f"Align your responses with the skill categories most relevant to this candidate's profile.\n"
                       f"For software languages, applications, and hardware, use the taxonomy as a reference but feel empowered to include technologies that aren't listed if they are clearly important to the candidate's profile.\n"
                       f"Example: If the taxonomy lists 'Java' but the resume shows extensive React.js experience, it's appropriate to list React.js even if it's not in the taxonomy.\n"
                       f"Balance standardization with accuracy - prioritize capturing the candidate's true expertise over strict adherence to the taxonomy.\n"
                       f"IMPORTANT: You MUST provide BOTH a Primary AND Secondary technical category. These must be different from each other. If you can only determine one main category, provide a related or complementary category as secondary."
        },
        {
            "role": "system",
            "content": "Use the following rules when assessing Primary and Secondary Industry:"
                       "You are required to give the user the requested information using the following rules."
                       "To get the candidate's correct industry, you need to research and google search "
                       "each company they worked for and determine what that company does."
                       "Industry should be defined based on the clients they have worked for."
                       "Information Technology is not an industry and should not be an answer."
                       "IMPORTANT: You MUST provide BOTH a Primary AND Secondary Industry. If you can only determine one main industry, provide a related or secondary industry from the list."
                       "Primary and Secondary Industry are required to come from this list and be based on the companies "
                       "they have worked for:"
                       "Agriculture, Amusement, Gambling, and Recreation Industries, Animal Production, Arts, "
                       "Entertainment, and Recreation, Broadcasting, Clothing, Construction, Data Processing, "
                       "Hosting, and Related Services, Education, Financial Services, Insurance, Fishing, "
                       "Hunting and Trapping, Food Manufacturing, Food Services, Retail, Forestry and Logging, "
                       "Funds, Trusts, and Other Financial Vehicles, Furniture and Home Furnishings Stores, "
                       "Furniture and Related Product Manufacturing, Oil and Gas, HealthCare, Civil Engineering, "
                       "Hospitals, Leisure and Hospitality, Machinery, Manufacturing, Merchant Wholesalers, "
                       "Mining, Motion Picture, Motor Vehicle and Parts Dealers, Natural Resources, Nursing, "
                       "Public Administration, Paper Manufacturing, Performing Arts, Spectator Sports, "
                       "and Related Industries, Primary Metal Manufacturing, Chemistry and Biology, Publishing, "
                       "Rail Transportation, Real Estate, Retail Trade, Transportation, Securities, "
                       "Commodity Contracts, and Other Financial Investments and Related Activities, "
                       "Supply Chain, Telecommunications, Textiles, Transportation, Utilities, Warehousing and "
                       "Storage, Waste Management."
        },
        {
            "role": "system",
            "content": "Use the following rules when assessing Job Titles:"
                       "Definition: Job Titles are what others call this person in the professional space."
                       "Ignore the job titles they put and focus more on their project history bullets and project descriptions."
                       "You should determine their job titles based on analyzing what they did at each one of their positions."
                       "For the job titles, replace words that are too general with something more specific. "
                       "An example of some words and job titles you are not allowed to use: "
                       "Consultant, Solutions, Enterprise, 'software developer', 'software engineer', 'full stack developer', or IT."
                       "For job title, use a different title for primary, secondary, and tertiary."
                       "All three job titles must have an answer."
                       "Each title must be different from each other."
        },
        {
            "role": "system",
            "content": "Use the following rules when filling out MostRecentCompany, SecondMostRecentCompany, ThirdMostRecentCompany, FourthMostRecentCompany, FifthMostRecentCompany, SixthMostRecentCompany, SeventhMostRecentCompany:"
                       "Don't include the city or state in the company name."
                       "Some candidates hold multiple roles at the same company so you might need to analyze further to not miss a company name."
        },
        {
            "role": "system",
            "content": "Use the following rules when finding company locations:"
                       "1. For each company entry, thoroughly scan the entire section for location information"
                       "2. Look anywhere in the job entry for city or state mentions, including:"
                       "   - Next to company name"
                       "   - In the job header"
                       "   - Within first few lines of the job description"
                       "   - Near dates or titles"
                       "3. When you find a location, format it ONLY as:"
                       "   - City, ST (if you find both)"
                       "   - ST (if you only find state)"
                       "4. Always convert full state names to 2-letter abbreviations"
                       "5. Strip out any extra information (zip codes, countries, etc.)"
                       "6. Use NULL only if you truly cannot find any location information in that job entry"
                       "IMPORTANT: Be thorough - check the entire job section before deciding there's no location."
        },
        {
            "role": "system",
            "content": "Abbreviate the state if it is not already done so."
                       "When needing to do a list, separate by commas."
                       "If there is no last name or the last name is one letter, look in their email for their last name."
        },
        {
            "role": "system",
            "content": "Use the following rules when assessing Skills, and refer to industry standards:"
                       "Definition: Skills are a list of keywords from the resume that repeat and are consistent "
                       "throughout their project work."
                       "Skills should be 2 words max."
                       "Prioritize skills that match standard industry categories that are most relevant to this resume."
                       "After listing the 10 skills, re-analyze the 10 skills chosen, combine like skills "
                       "together and do not repeat the same skill word multiple times. Then, if you have less "
                       "than 10 skills, find more skills to add."
        },
        {
            "role": "user",
            "content": "- Please analyze the following resume and give me the following details(If you can't find an answer or it's not provided/listed, just put NULL):"
                       "- Best job title that fit their primary experience:\n"
                       "- Best secondary job title that fits their secondary experience(Must differ from Primary):\n"
                       "- Best tertiary job title that fits their tertiary experience(Must differ from Primary and Secondary):\n"
                       "- Their street address:\n"
                       "- Their City:\n"
                       "- Their State:\n"
                       "- Their Certifications Listed:\n"
                       "- Their Bachelor's Degree:\n"
                       "- Their Master's Degree:\n"
                       "- Their Phone Number:\n"
                       "- Their Second Phone Number:\n"
                       "- Their Email:\n"
                       "- Their Second Email:\n"
                       "- Their First Name:\n"
                       "- Their Middle Name:\n"
                       "- Their Last Name:\n"
                       "- Their Linkedin URL:\n"
                       "- Most Recent Company Worked for:\n"
                       "- Most Recent Start Date (YYYY-MM-DD):\n"
                       "- Most Recent End Date (YYYY-MM-DD):\n"
                       "- Most Recent Job Location:\n"
                       "- Second Most Recent Company Worked for:\n"
                       "- Second Most Recent Start Date (YYYY-MM-DD):\n"
                       "- Second Most Recent End Date (YYYY-MM-DD):\n"
                       "- Second Most Recent Job Location:\n"
                       "- Third Most Recent Company Worked for:\n"
                       "- Third Most Recent Start Date (YYYY-MM-DD):\n"
                       "- Third Most Recent End Date (YYYY-MM-DD):\n"
                       "- Third Most Recent Job Location:\n"
                       "- Fourth Most Recent Company Worked for:\n"
                       "- Fourth Most Recent Start Date (YYYY-MM-DD):\n"
                       "- Fourth Most Recent End Date (YYYY-MM-DD):\n"
                       "- Fourth Most Recent Job Location:\n"
                       "- Fifth Most Recent Company Worked for:\n"
                       "- Fifth Most Recent Start Date (YYYY-MM-DD):\n"
                       "- Fifth Most Recent End Date (YYYY-MM-DD):\n"
                       "- Fifth Most Recent Job Location:\n"
                       "- Sixth Most Recent Company Worked for:\n"
                       "- Sixth Most Recent Start Date (YYYY-MM-DD):\n"
                       "- Sixth Most Recent End Date (YYYY-MM-DD):\n"
                       "- Sixth Most Recent Job Location:\n"
                       "- Seventh Most Recent Company Worked for:\n"
                       "- Seventh Most Recent Start Date (YYYY-MM-DD):\n"
                       "- Seventh Most Recent End Date (YYYY-MM-DD):\n"
                       "- Seventh Most Recent Job Location:\n"
                       "- Based on all 7 of their most recent companies above, what is the Primary industry they work in:\n"
                       "- Based on all 7 of their most recent companies above, what is the Secondary industry they work in:\n"
                       "- Top 10 Technical Skills:"
        },
        {
            "role": "user",
            "content": "- Please return the answer in this exact structure(If you can't find an answer or it's not provided/listed, just put NULL):"
                       "- Best job title that fit their primary experience:\n"
                       "- Best secondary job title that fits their secondary experience:\n"
                       "- Best tertiary job title that fits their tertiary experience:\n"
                       "- Their street address:\n"
                       "- Their City:\n"
                       "- Their State:\n"
                       "- Their Certifications Listed:\n"
                       "- Their Bachelor's Degree:\n"
                       "- Their Master's Degree:\n"
                       "- Their Phone Number:\n"
                       "- Their Second Phone Number:\n"
                       "- Their Email:\n"
                       "- Their Second Email:\n"
                       "- Their First Name:\n"
                       "- Their Middle Name:\n"
                       "- Their Last Name:\n"
                       "- Their Linkedin URL:\n"
                       "- Most Recent Company Worked for:\n"
                       "- Most Recent Start Date (YYYY-MM-DD):\n"
                       "- Most Recent End Date (YYYY-MM-DD):\n"
                       "- Most Recent Job Location:\n"
                       "- Second Most Recent Company Worked for:\n"
                       "- Second Most Recent Start Date (YYYY-MM-DD):\n"
                       "- Second Most Recent End Date (YYYY-MM-DD):\n"
                       "- Second Most Recent Job Location:\n"
                       "- Third Most Recent Company Worked for:\n"
                       "- Third Most Recent Start Date (YYYY-MM-DD):\n"
                       "- Third Most Recent End Date (YYYY-MM-DD):\n"
                       "- Third Most Recent Job Location:\n"
                       "- Fourth Most Recent Company Worked for:\n"
                       "- Fourth Most Recent Start Date (YYYY-MM-DD):\n"
                       "- Fourth Most Recent End Date (YYYY-MM-DD):\n"
                       "- Fourth Most Recent Job Location:\n"
                       "- Fifth Most Recent Company Worked for:\n"
                       "- Fifth Most Recent Start Date (YYYY-MM-DD):\n"
                       "- Fifth Most Recent End Date (YYYY-MM-DD):\n"
                       "- Fifth Most Recent Job Location:\n"
                       "- Sixth Most Recent Company Worked for:\n"
                       "- Sixth Most Recent Start Date (YYYY-MM-DD):\n"
                       "- Sixth Most Recent End Date (YYYY-MM-DD):\n"
                       "- Sixth Most Recent Job Location:\n"
                       "- Seventh Most Recent Company Worked for:\n"
                       "- Seventh Most Recent Start Date (YYYY-MM-DD):\n"
                       "- Seventh Most Recent End Date (YYYY-MM-DD):\n"
                       "- Seventh Most Recent Job Location:\n"
                       "- Based on all 7 of their most recent companies above, what is the Primary industry they work in:\n"
                       "- Based on all 7 of their most recent companies above, what is the Secondary industry they work in:\n"
                       "- Top 10 Technical Skills:"
        }
    ]
    
    # Step 2 prompt messages - complete version from two_step_prompts_taxonomy.py with all system messages
    step2_messages = [
        {
            "role": "system",
            "content": "Use the following rules when assessing Primary, Secondary, and Tertiary Technical Languages: "
                       "Include ALL types of technical languages mentioned in the resume, such as:"
                       "- Database languages (SQL, T-SQL, PL/SQL, MySQL, Oracle SQL, PostgreSQL)"
                       "- Programming languages (Java, Python, C#, JavaScript, Ruby)"
                       "- Scripting languages (PowerShell, Bash, Shell, VBA)"
                       "- Query languages (SPARQL, GraphQL, HiveQL)"
                       "- Markup/stylesheet languages (HTML, CSS, XML)"
                       "- There are more but these are just a few examples. You don't have to stick to just the above list"
                       "Prioritize languages based on:"
                       "1. Frequency of mention throughout work history and resume"
                       "2. Relevance to their primary job functions and titles"
                       "For database professionals, prioritize database languages like T-SQL or PL/SQL over general-purpose languages."
        },
        {
            "role": "system",
            "content": "Use the following rules when assessing Most used Software Applications: "
                       "Please only list out actual software applications"
                       "It can be any kind of software in any industry used for anything. The goal is to list out any software they could easily use again at another job"
                       "Analyze their resume and determine what software they use most"
                       "If none can be found put NULL"
        },
        {
            "role": "system",
            "content": "Use the following rules when assessing Hardware: "
                       "Please list 5 different specific hardware devices the candidate has worked with. "
                       "Hardware devices include many categories such as:\n"
                       "- Network equipment (firewalls, routers, switches, load balancers)\n"
                       "- Server hardware (blade servers, rack servers, chassis systems)\n"
                       "- Storage devices (SANs, NAS, RAID arrays, disk systems)\n"
                       "- Security appliances (TACALANEs, hardware encryption devices)\n"
                       "- Management interfaces (iDRAC, iLO, IMM, IPMI, BMC)\n"
                       "- Virtualization hardware (ESXi hosts, hyperconverged systems)\n"
                       "- Physical components (CPUs, RAM modules, hard drives, SSDs)\n"
                       "- Communication hardware (modems, wireless access points, VPN concentrators)\n"
                       "- Specialized hardware (tape libraries, KVM switches, console servers)\n\n"
                       "IMPORTANT: Even if they worked with multiple hardware items from the same brand, list different types. "
                       "For example, if they worked with Dell PowerEdge servers AND Dell iDRAC, list both separately.\n\n"
                       "Look beyond obvious hardware to find specialized equipment, management interfaces, and components. "
                       "Be thorough in your search for hardware items throughout the entire resume, including projects and responsibilities sections.\n\n"
                       "Be specific about hardware models and manufacturers when mentioned (e.g. 'Palo Alto PA-5200 series' rather than just 'firewalls'). "
                       "Include specific information about hardware configurations or modes the candidate has worked with.\n\n"
                       "Please provide each hardware item on a separate line in this exact format:\n"
                       "Hardware 1: [Specific hardware device]\n"
                       "Hardware 2: [Specific hardware device]\n"
                       "Hardware 3: [Specific hardware device]\n"
                       "Hardware 4: [Specific hardware device]\n"
                       "Hardware 5: [Specific hardware device]\n\n"
                       "Try your best to identify 5 different hardware items. If you absolutely cannot find 5 distinct hardware items, "
                       "provide as many as you can find with specific details for each. Only use NULL if no hardware at all is mentioned."
        },
        {
            "role": "system",
            "content": "Use the following rules when assessing Project Types: "
                       "Use a mix of words like but not limited to implementation, "
                       "integration, migration, move, deployment, optimization, consolidation and make it 2-3 words."
        },
        {
            "role": "system",
            "content": "Use the following rules when assessing their Specialty:"
                       "For their specialty, emphasize the project types they have done and relate them to "
                       "their industry."
        },
        {
            "role": "system",
            "content": "Use the following rules when assessing their Category:"
                       "For candidates with technical skills, use technical categories like Software Development, Database Administration, Network Engineering, etc."
                       "For non-technical candidates, use functional/business categories like Business Administration, Finance, Marketing, Customer Service, etc."
                       "For the categories, do not repeat the same category."
                       "Both primary and secondary categories MUST have an answer - NEVER return NULL!"
                       "Every candidate fits into at least one professional category, even if not a technical one."
        },
        {
            "role": "system",
            "content": "Use the following rules when writing their summary:"
                       "For their summary, give a brief summary of their resume in a few sentences"
                       "Based on their project types, industry, and specialty, skills, degrees, certifications, and job titles, write the summary"
        },
        {
            "role": "system",
            "content": "Use the following rules when determining length in US:"
                       "Calculate how many years they have been WORKING in the United States based on their employment history. "
                       "Only count years where they had jobs at US-based companies or locations. "
                       "If all their work experience is in the US, this should equal their total years of professional experience. "
                       "If they have no international work experience visible, assume all work was in the US. "
                       "This is about PROFESSIONAL WORK TIME, not their entire life or education duration. "
                       "Just put a number and no other characters. "
                       "Result should not exceed their total years of professional experience. "
                       "Result should only be numerical"
        },
        {
            "role": "system",
            "content": "Use the following rules when determining Average Tenure and Year of Experience:"
                       "Use all previous start date and end date questions answers to determine this."
                       "Just put a number and no other characters"
                       "Result should not be 0"
                       "Result should only be numerical"
        },
        {
            "role": "user",
            "content": "- Please analyze the following resume and give me the following details(If you can't find an answer or it's not provided/listed, just put NULL):"
                       "- What technical language do they use most often?:\n"
                       "- What technical language do they use second most often?:\n"
                       "- What technical language do they use third most often?:\n"
                       "- What software do they talk about using the most?:\n"
                       "- What software do they talk about using the second most?:\n"
                       "- What software do they talk about using the third most?:\n"
                       "- What software do they talk about using the fourth most?:\n"
                       "- What software do they talk about using the fifth most?:\n"
                       "- What physical hardware do they talk about using the most?:\n"
                       "- What physical hardware do they talk about using the second most?:\n"
                       "- What physical hardware do they talk about using the third most?:\n"
                       "- What physical hardware do they talk about using the fourth most?:\n"
                       "- What physical hardware do they talk about using the fifth most?:\n"
                       "- Based on their experience, put them in a primary technical category if they are technical or functional category if they are functional:\n"
                       "- Based on their experience, put them in a subsidiary technical category if they are technical or functional category if they are functional:\n"
                       "- Types of projects they have worked on:\n"
                       "- Based on their skills, categories, certifications, and industries, determine what they specialize in:\n"
                       "- Based on all this knowledge, write a summary of this candidate that could be sellable to an employer:\n"
                       "- How long have they lived in the United States(numerical answer only):\n"
                       "- Total years of professional experience (numerical answer only):\n"
                       "- Average tenure at companies in years (numerical answer only):"
        },
        {
            "role": "user",
            "content": "- Please return the answer in this exact structure(If you can't find an answer or it's not provided/listed, just put NULL):"
                       "- What technical language do they use most often?:\n"
                       "- What technical language do they use second most often?:\n"
                       "- What technical language do they use third most often?:\n"
                       "- What software do they talk about using the most?:\n"
                       "- What software do they talk about using the second most?:\n"
                       "- What software do they talk about using the third most?:\n"
                       "- What software do they talk about using the fourth most?:\n"
                       "- What software do they talk about using the fifth most?:\n"
                       "- What physical hardware do they talk about using the most?:\n"
                       "- What physical hardware do they talk about using the second most?:\n"
                       "- What physical hardware do they talk about using the third most?:\n"
                       "- What physical hardware do they talk about using the fourth most?:\n"
                       "- What physical hardware do they talk about using the fifth most?:\n"
                       "- Based on their experience, put them in a primary technical category if they are technical or functional category if they are functional:\n"
                       "- Based on their experience, put them in a subsidiary technical category if they are technical or functional category if they are functional:\n"
                       "- Types of projects they have worked on:\n"
                       "- Based on their skills, categories, certifications, and industries, determine what they specialize in:\n"
                       "- Based on all this knowledge, write a summary of this candidate that could be sellable to an employer:\n"
                       "- How long have they lived in the United States(numerical answer only):\n"
                       "- Total years of professional experience (numerical answer only):\n"
                       "- Average tenure at companies in years (numerical answer only):"
        }
    ]
    
    # Get taxonomy context from skills detector
    taxonomy_context = get_taxonomy_context(resume_text, max_categories=2, userid=userid)
    
    # If we have taxonomy context, add it to the system message for enhanced processing
    if taxonomy_context:
        # Add taxonomy context to the first system message
        for i, msg in enumerate(step1_messages):
            if msg["role"] == "system":
                step1_messages[i]["content"] += "\n\n" + taxonomy_context
                logging.info(f"UserID {userid}: Added taxonomy context to prompt")
                break
                
    # Combine all messages exactly as in the original two_step_prompts_taxonomy.py
    messages = step1_messages
    
    # Only add step 2 messages without any intermediate instruction or system message
    messages.extend(step2_messages)

def count_tokens(content: str) -> int:
    """Count the number of tokens in a string"""
    try:
        # Handle gpt-5 models by using gpt-4 encoding
        model_for_encoding = MODEL
        if "gpt-5" in MODEL.lower():
            model_for_encoding = "gpt-4"  # Use gpt-4 encoding for gpt-5 models
        encoding = tiktoken.encoding_for_model(model_for_encoding)
        return len(encoding.encode(content))
    except Exception:
        # Fall back to a simple approximation if encoding fails
        return len(content) // 4  # Approximate 4 chars per token

def setup_batch_status_table():
    """Check if we can access the database - don't try to create tables"""
    try:
        # Connect to the database using the robust connection function
        conn = create_pyodbc_connection()
        cursor = conn.cursor()
        
        # Simple test query to verify connection works
        cursor.execute("SELECT 1")
        cursor.fetchone()
        
        cursor.close()
        conn.close()
        
        logging.info(f"Database connection verified successfully")
        return True
        
    except Exception as e:
        logging.error(f"Error connecting to database: {str(e)}")
        return False

def mark_records_as_queued(batch_id: str, userids: List[int]) -> bool:
    """Just log the userids instead of using a database table"""
    if not userids:
        return True
    
    try:
        # Just log the information instead of inserting to a table
        logging.info(f"Batch {batch_id}: Queued {len(userids)} records for processing")
        logging.debug(f"Batch {batch_id}: UserIDs: {userids[:10]}...")
        
        return True
        
    except Exception as e:
        logging.error(f"Error logging queued records: {str(e)}")
        return False

def update_batch_status(batch_id: str, status: str, userids: Optional[List[int]] = None, 
                        input_file_id: Optional[str] = None, output_file_id: Optional[str] = None, 
                        error_file_id: Optional[str] = None, error_message: Optional[str] = None) -> bool:
    """Update batch status in the tracking table"""
    try:
        # Connect to the database using the robust connection function
        conn = create_pyodbc_connection()
        cursor = conn.cursor()
        
        # Update fields
        update_fields = [f"status = '{status}'", "updated_at = GETDATE()"]
        
        if status in ['completed', 'failed']:
            update_fields.append("completed_at = GETDATE()")
            
        if input_file_id:
            update_fields.append(f"input_file_id = '{input_file_id}'")
            
        if output_file_id:
            update_fields.append(f"output_file_id = '{output_file_id}'")
            
        if error_file_id:
            update_fields.append(f"error_file_id = '{error_file_id}'")
            
        if error_message:
            # Escape single quotes in error message
            escaped_error = error_message.replace("'", "''")
            update_fields.append(f"error_message = '{escaped_error}'")
        
        # Build WHERE clause
        if userids:
            userid_list = ', '.join(map(str, userids))
            where_clause = f"batch_id = '{batch_id}' AND userid IN ({userid_list})"
        else:
            where_clause = f"batch_id = '{batch_id}'"
        
        query = f"""
            UPDATE {BATCH_STATUS_TABLE}
            SET {', '.join(update_fields)}
            WHERE {where_clause}
        """
        
        cursor.execute(query)
        conn.commit()
        
        row_count = cursor.rowcount
        cursor.close()
        conn.close()
        
        logging.info(f"Updated status to '{status}' for batch {batch_id}, affected {row_count} records")
        return True
        
    except Exception as e:
        logging.error(f"Error updating batch status: {str(e)}")
        return False

def get_batch_status(batch_id: str) -> Dict:
    """Get the current status of a batch job from the OpenAI API"""
    try:
        response = openai.batches.retrieve(batch_id=batch_id)
        return {
            "id": response.id,
            "status": response.status,
            "created_at": response.created_at,
            "input_file_id": getattr(response, "input_file_id", None),
            "output_file_id": getattr(response, "output_file_id", None),
            "error_file_id": getattr(response, "error_file_id", None)
        }
    except Exception as e:
        logging.error(f"Error retrieving batch status: {str(e)}")
        return {}

def generate_unified_request(userid: int, resume_text: str) -> Dict:
    """
    Generate a unified request for a single resume

    Args:
        userid: The user ID
        resume_text: The resume text

    Returns:
        A dictionary with the request payload
    """
    # Create unified prompt
    unified_messages = create_unified_prompt(resume_text, userid=userid)

    # Don't add JSON formatting - use the original text format
    # This ensures better field extraction as originally designed
    # The prompts already specify the exact text format to return
    
    # Format for batch API
    # Build the request body - for gpt-5-mini we don't use temperature or max_tokens
    body = {
        "model": MODEL,
        "messages": unified_messages
    }

    # Only add temperature if not using gpt-5 models (they only support default temp of 1)
    if "gpt-5" not in MODEL.lower():
        body["temperature"] = 0.2
        body["max_tokens"] = 16000
    # For gpt-5 models, let them use defaults (temp=1, no max_tokens limit)

    return {
        "custom_id": f"unified_{userid}",
        "method": "POST",
        "url": "/v1/chat/completions",
        "body": body
    }

def create_batch_input_file(resume_batch: List[Tuple[int, str]]) -> str:
    """
    Create a JSONL file for batch processing
    
    Args:
        resume_batch: List of (userid, resume_text) tuples
        
    Returns:
        Path to the created JSONL file
    """
    # Create a unique filename
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    batch_id = f"batch_{timestamp}"
    filename = f"batch_input_unified_{timestamp}.jsonl"
    filepath = os.path.join(os.path.dirname(__file__), filename)
    
    # Track token usage for monitoring
    total_tokens = 0
    request_count = 0
    
    try:
        with open(filepath, 'w', encoding='utf-8') as f:
            for userid, resume_text in resume_batch:
                request = generate_unified_request(userid, resume_text)
                token_count = count_tokens(json.dumps(request))
                total_tokens += token_count
                request_count += 1
                f.write(json.dumps(request) + '\n')
            
        logging.info(f"Created batch input file {filepath} with {request_count} requests ({total_tokens} tokens)")
        return filepath, batch_id, request_count
    except Exception as e:
        logging.error(f"Error creating batch input file: {str(e)}")
        return "", "", 0

def upload_batch_file(filepath: str) -> str:
    """
    Upload a file to OpenAI for batch processing
    
    Args:
        filepath: Path to the JSONL file
        
    Returns:
        File ID if successful, empty string otherwise
    """
    try:
        with open(filepath, 'rb') as file:
            response = openai.files.create(
                file=file,
                purpose="batch"
            )
        file_id = response.id
        logging.info(f"Uploaded file {filepath} with ID {file_id}")
        return file_id
    except Exception as e:
        logging.error(f"Error uploading file: {str(e)}")
        return ""

def submit_batch_job(file_id: str, endpoint: str = "/v1/chat/completions") -> str:
    """
    Submit a batch job to OpenAI
    
    Args:
        file_id: ID of the uploaded file
        endpoint: API endpoint to use
        
    Returns:
        Batch ID if successful, empty string otherwise
    """
    try:
        response = openai.batches.create(
            input_file_id=file_id,
            endpoint=endpoint,
            completion_window="24h"  # For 50% discount
        )
        batch_id = response.id
        logging.info(f"Submitted batch job with ID {batch_id} using file {file_id}")
        return batch_id
    except Exception as e:
        logging.error(f"Error submitting batch job: {str(e)}")
        return ""

def get_file_content(file_id: str) -> List[Dict]:
    """
    Download and parse a file from OpenAI
    
    Args:
        file_id: ID of the file to download
        
    Returns:
        List of parsed JSON objects from the file
    """
    try:
        # Download the file
        response = openai.files.content(file_id)
        content = response.read().decode('utf-8')
        
        # Parse each line as JSON
        results = []
        for line in content.splitlines():
            if line.strip():
                try:
                    results.append(json.loads(line))
                except json.JSONDecodeError as e:
                    logging.error(f"Error parsing JSON line: {str(e)}")
        
        logging.info(f"Downloaded and parsed file {file_id} with {len(results)} results")
        return results
    except Exception as e:
        logging.error(f"Error downloading file: {str(e)}")
        return []

def OLD_parse_unified_response_DO_NOT_USE(response_text, debug_mode=True, debug_limit=20, debug_counter=None):
    """
    Parse the LLM response from the unified prompt to extract structured data
    with support for both JSON format and text format
    
    Args:
        response_text: The response text from the LLM
        debug_mode: Whether to generate debug files
        debug_limit: Maximum number of debug files to generate per batch
        debug_counter: Counter to track how many debug files have been generated
        
    Returns:
        A dictionary of parsed fields
    """
    # Make sure re module is imported
    import re
    import json
    import uuid
    
    # Save the full response to a debug file if debug mode is enabled
    # and we haven't exceeded the limit
    debug_id = uuid.uuid4().hex[:8]
    save_debug = False
    
    if debug_mode:
        if debug_counter is not None:
            if debug_counter.value < debug_limit:
                save_debug = True
                debug_counter.value += 1
        else:
            save_debug = True
            
    if save_debug:
        debug_path = os.path.join(os.path.dirname(__file__), f"debug_response_{debug_id}.json")
        with open(debug_path, "w", encoding="utf-8") as debug_file:
            debug_file.write(response_text)
        logging.info(f"Saved full raw response to {debug_path}")
    
    # Log the raw response for debugging (truncated for log size)
    logging.info(f"Unified response first 500 chars: {response_text[:500]}...")
    
    # Initialize result dictionary with default NULL values
    result = {
        # Step 1 fields - Personal Info
        "FirstName": "NULL", 
        "MiddleName": "NULL", 
        "LastName": "NULL",
        "Phone1": "NULL", 
        "Phone2": "NULL", 
        "Email": "NULL", 
        "Email2": "NULL", 
        "LinkedIn": "NULL",
        "Address": "NULL", 
        "City": "NULL", 
        "State": "NULL",
        "Bachelors": "NULL", 
        "Masters": "NULL", 
        "Certifications": "NULL",
        
        # Step 1 fields - Work History
        "MostRecentCompany": "NULL", 
        "MostRecentStartDate": "NULL", 
        "MostRecentEndDate": "NULL", 
        "MostRecentLocation": "NULL",
        "SecondMostRecentCompany": "NULL", 
        "SecondMostRecentStartDate": "NULL", 
        "SecondMostRecentEndDate": "NULL", 
        "SecondMostRecentLocation": "NULL",
        "ThirdMostRecentCompany": "NULL", 
        "ThirdMostRecentStartDate": "NULL", 
        "ThirdMostRecentEndDate": "NULL", 
        "ThirdMostRecentLocation": "NULL",
        "FourthMostRecentCompany": "NULL", 
        "FourthMostRecentStartDate": "NULL", 
        "FourthMostRecentEndDate": "NULL", 
        "FourthMostRecentLocation": "NULL",
        "FifthMostRecentCompany": "NULL", 
        "FifthMostRecentStartDate": "NULL", 
        "FifthMostRecentEndDate": "NULL", 
        "FifthMostRecentLocation": "NULL",
        "SixthMostRecentCompany": "NULL", 
        "SixthMostRecentStartDate": "NULL", 
        "SixthMostRecentEndDate": "NULL", 
        "SixthMostRecentLocation": "NULL",
        "SeventhMostRecentCompany": "NULL", 
        "SeventhMostRecentStartDate": "NULL", 
        "SeventhMostRecentEndDate": "NULL", 
        "SeventhMostRecentLocation": "NULL",
        
        # Step 1 fields - Career/Job Info
        "PrimaryTitle": "NULL", 
        "SecondaryTitle": "NULL", 
        "TertiaryTitle": "NULL",
        "PrimaryIndustry": "NULL", 
        "SecondaryIndustry": "NULL",
        "Top10Skills": "NULL",
        
        # Step 2 fields - Technical Info
        "PrimarySoftwareLanguage": "NULL", 
        "SecondarySoftwareLanguage": "NULL", 
        "TertiarySoftwareLanguage": "NULL",
        "SoftwareApp1": "NULL", 
        "SoftwareApp2": "NULL", 
        "SoftwareApp3": "NULL", 
        "SoftwareApp4": "NULL", 
        "SoftwareApp5": "NULL",
        "Hardware1": "NULL", 
        "Hardware2": "NULL", 
        "Hardware3": "NULL", 
        "Hardware4": "NULL", 
        "Hardware5": "NULL",
        "PrimaryCategory": "NULL", 
        "SecondaryCategory": "NULL",
        "ProjectTypes": "NULL",
        "Specialty": "NULL",
        "Summary": "NULL",
        "LengthinUS": "NULL",
        "YearsofExperience": "NULL",
        "AvgTenure": "NULL"
    }
    
    # Clean up content if it contains markdown code blocks
    if '```' in response_text:
        # Extract content from code blocks if present
        code_block_pattern = r'```(?:json)?\s*([\s\S]*?)\s*```'
        code_blocks = re.findall(code_block_pattern, response_text)
        if code_blocks:
            # Use the largest code block (assuming it's the most complete)
            largest_block = max(code_blocks, key=len)
            # Save the JSON from code block for analysis
            if save_debug:
                code_path = os.path.join(os.path.dirname(__file__), f"debug_json_block_{debug_id}.json")
                with open(code_path, "w", encoding="utf-8") as code_file:
                    code_file.write(largest_block)
                logging.info(f"Extracted JSON from code block, saved to {code_path}")
            response_text = largest_block

    # First, try to parse as JSON (which is our preferred format)
    try:
        # Find JSON object in the response (look for everything between the first { and the last })
        start_idx = response_text.find('{')
        end_idx = response_text.rfind('}')
        
        if start_idx >= 0 and end_idx > start_idx:
            json_str = response_text[start_idx:end_idx+1]
            parsed_json = json.loads(json_str)
            
            # Log the JSON structure
            top_level_keys = list(parsed_json.keys())
            logging.info(f"Successfully parsed JSON with top-level keys: {top_level_keys}")
            
            # Look for direct matches first
            for field in result.keys():
                if field in parsed_json:
                    # Only update if the JSON field has a value
                    json_value = parsed_json[field]
                    if json_value and json_value != "NULL":
                        result[field] = json_value
                        logging.info(f"Extracted '{field}' directly from JSON: '{json_value}'")

            # Handle nested structure with PERSONAL_INFORMATION object
            if 'PERSONAL_INFORMATION' in parsed_json:
                logging.info("Found PERSONAL_INFORMATION in parsed JSON")
                personal_info = parsed_json['PERSONAL_INFORMATION']
                
                # Handle Name object
                if isinstance(personal_info, dict) and 'Name' in personal_info and isinstance(personal_info['Name'], dict):
                    name_info = personal_info['Name']
                    name_mapping = {
                        'FirstName': 'FirstName',
                        'MiddleName': 'MiddleName',
                        'LastName': 'LastName'
                    }
                    for json_field, result_field in name_mapping.items():
                        if json_field in name_info and name_info[json_field] and name_info[json_field] != "NULL":
                            result[result_field] = name_info[json_field]
                            logging.info(f"Extracted '{result_field}' from nested Name JSON: '{name_info[json_field]}'")
                
                # Handle Contact object
                if isinstance(personal_info, dict) and 'Contact' in personal_info and isinstance(personal_info['Contact'], dict):
                    contact_info = personal_info['Contact']
                    contact_mapping = {
                        'Phone': 'Phone1',
                        'SecondaryPhone': 'Phone2',
                        'Email': 'Email',
                        'SecondaryEmail': 'Email2',
                        'LinkedIn': 'LinkedIn',
                        'Address': 'Address',
                        'City': 'City',
                        'State': 'State'
                    }
                    for json_field, result_field in contact_mapping.items():
                        if json_field in contact_info and contact_info[json_field] and contact_info[json_field] != "NULL":
                            result[result_field] = contact_info[json_field]
                            logging.info(f"Extracted '{result_field}' from nested Contact JSON: '{contact_info[json_field]}'")
                
                # Handle Education object
                if isinstance(personal_info, dict) and 'Education' in personal_info and isinstance(personal_info['Education'], dict):
                    education_info = personal_info['Education']
                    education_mapping = {
                        'BachelorsDegree': 'Bachelors',
                        'MastersDegree': 'Masters',
                        'Certifications': 'Certifications'
                    }
                    for json_field, result_field in education_mapping.items():
                        if json_field in education_info and education_info[json_field] and education_info[json_field] != "NULL":
                            result[result_field] = education_info[json_field]
                            logging.info(f"Extracted '{result_field}' from nested Education JSON: '{education_info[json_field]}'")
            
            # Handle WORK_HISTORY object
            if 'WORK_HISTORY' in parsed_json and isinstance(parsed_json['WORK_HISTORY'], dict):
                work_history = parsed_json['WORK_HISTORY']
                
                # Handle different company entries
                company_prefixes = [
                    ('MostRecent', 'MostRecent'),
                    ('SecondMostRecent', 'SecondMostRecent'),
                    ('ThirdMostRecent', 'ThirdMostRecent'),
                    ('FourthMostRecent', 'FourthMostRecent'),
                    ('FifthMostRecent', 'FifthMostRecent'),
                    ('SixthMostRecent', 'SixthMostRecent'),
                    ('SeventhMostRecent', 'SeventhMostRecent')
                ]
                
                for json_prefix, result_prefix in company_prefixes:
                    company_key = f"{json_prefix}Company"
                    
                    # Try to find either using the full key or within a nested object
                    if company_key in work_history and work_history[company_key] and work_history[company_key] != "NULL":
                        # Direct company entry
                        result[f"{result_prefix}Company"] = work_history[company_key]
                        logging.info(f"Extracted '{result_prefix}Company' from WORK_HISTORY: '{work_history[company_key]}'")
                        
                        # Look for related fields
                        related_fields = {
                            f"{json_prefix}StartDate": f"{result_prefix}StartDate",
                            f"{json_prefix}EndDate": f"{result_prefix}EndDate",
                            f"{json_prefix}Location": f"{result_prefix}Location"
                        }
                        
                        for json_field, result_field in related_fields.items():
                            if json_field in work_history and work_history[json_field] and work_history[json_field] != "NULL":
                                result[result_field] = work_history[json_field]
                                logging.info(f"Extracted '{result_field}' from WORK_HISTORY: '{work_history[json_field]}'")
                    
                    # Check if there's a nested object with the prefix
                    elif json_prefix in work_history and isinstance(work_history[json_prefix], dict):
                        company_info = work_history[json_prefix]
                        field_mapping = {
                            'Company': f"{result_prefix}Company",
                            'StartDate': f"{result_prefix}StartDate",
                            'EndDate': f"{result_prefix}EndDate",
                            'Location': f"{result_prefix}Location"
                        }
                        
                        for json_field, result_field in field_mapping.items():
                            if json_field in company_info and company_info[json_field] and company_info[json_field] != "NULL":
                                result[result_field] = company_info[json_field]
                                logging.info(f"Extracted '{result_field}' from nested {json_prefix} JSON: '{company_info[json_field]}'")
            
            # Handle CAREER_INFO object
            if 'CAREER_INFO' in parsed_json and isinstance(parsed_json['CAREER_INFO'], dict):
                career_info = parsed_json['CAREER_INFO']
                
                # Handle job titles
                title_mapping = {
                    'PrimaryTitle': 'PrimaryTitle',
                    'SecondaryTitle': 'SecondaryTitle',
                    'TertiaryTitle': 'TertiaryTitle',
                    'PrimaryIndustry': 'PrimaryIndustry',
                    'SecondaryIndustry': 'SecondaryIndustry',
                    'TopSkills': 'Top10Skills'
                }
                
                for json_field, result_field in title_mapping.items():
                    if json_field in career_info:
                        if career_info[json_field] and career_info[json_field] != "NULL":
                            result[result_field] = career_info[json_field]
                            logging.info(f"Extracted '{result_field}' from CAREER_INFO: '{career_info[json_field]}'")
                        else:
                            # Still map the field even if it's NULL, to ensure the field exists in the result
                            result[result_field] = "NULL"
                            logging.info(f"Field '{json_field}' is NULL, mapping to '{result_field}'")
            
            # Handle TECHNICAL_INFO object
            if 'TECHNICAL_INFO' in parsed_json and isinstance(parsed_json['TECHNICAL_INFO'], dict):
                tech_info = parsed_json['TECHNICAL_INFO']
                
                # Handle programming languages
                lang_mapping = {
                    'PrimaryLanguage': 'PrimarySoftwareLanguage',
                    'SecondaryLanguage': 'SecondarySoftwareLanguage',
                    'TertiaryLanguage': 'TertiarySoftwareLanguage'
                }
                
                for json_field, result_field in lang_mapping.items():
                    if json_field in tech_info:
                        if tech_info[json_field] and tech_info[json_field] != "NULL":
                            result[result_field] = tech_info[json_field]
                            logging.info(f"Extracted '{result_field}' from TECHNICAL_INFO: '{tech_info[json_field]}'")
                        else:
                            # Still map the field even if it's NULL, to ensure the field exists in the result
                            result[result_field] = "NULL"
                            logging.info(f"Field '{json_field}' is NULL, mapping to '{result_field}'")
                
                # Handle software applications
                software_mapping = {
                    'SoftwareApp1': 'SoftwareApp1',
                    'SoftwareApp2': 'SoftwareApp2',
                    'SoftwareApp3': 'SoftwareApp3',
                    'SoftwareApp4': 'SoftwareApp4',
                    'SoftwareApp5': 'SoftwareApp5'
                }
                
                for json_field, result_field in software_mapping.items():
                    if json_field in tech_info:
                        if tech_info[json_field] and tech_info[json_field] != "NULL":
                            result[result_field] = tech_info[json_field]
                            logging.info(f"Extracted '{result_field}' from TECHNICAL_INFO: '{tech_info[json_field]}'")
                        else:
                            # Still map the field even if it's NULL, to ensure the field exists in the result
                            result[result_field] = "NULL"
                            logging.info(f"Field '{json_field}' is NULL, mapping to '{result_field}'")
                
                # Handle hardware
                hardware_mapping = {
                    'Hardware1': 'Hardware1',
                    'Hardware2': 'Hardware2',
                    'Hardware3': 'Hardware3',
                    'Hardware4': 'Hardware4',
                    'Hardware5': 'Hardware5'
                }
                
                for json_field, result_field in hardware_mapping.items():
                    if json_field in tech_info:
                        if tech_info[json_field] and tech_info[json_field] != "NULL":
                            result[result_field] = tech_info[json_field]
                            logging.info(f"Extracted '{result_field}' from TECHNICAL_INFO: '{tech_info[json_field]}'")
                        else:
                            # Still map the field even if it's NULL, to ensure the field exists in the result
                            result[result_field] = "NULL"
                            logging.info(f"Field '{json_field}' is NULL, mapping to '{result_field}'")
                
                # Handle categories and other fields
                other_tech_mapping = {
                    'PrimaryCategory': 'PrimaryCategory',
                    'SecondaryCategory': 'SecondaryCategory',
                    'ProjectTypes': 'ProjectTypes',
                    'Specialty': 'Specialty',
                    'Summary': 'Summary',
                    'LengthInUS': 'LengthinUS',
                    'YearsOfExperience': 'YearsofExperience',
                    'AvgTenure': 'AvgTenure'
                }
                
                for json_field, result_field in other_tech_mapping.items():
                    if json_field in tech_info:
                        if tech_info[json_field] and tech_info[json_field] != "NULL":
                            result[result_field] = tech_info[json_field]
                            logging.info(f"Extracted '{result_field}' from TECHNICAL_INFO: '{tech_info[json_field]}'")
                        else:
                            # Still map the field even if it's NULL, to ensure the field exists in the result
                            result[result_field] = "NULL"
                            logging.info(f"Field '{json_field}' is NULL, mapping to '{result_field}'")
            
            # Count JSON fields
            json_fields_count = sum(1 for val in result.values() if val != "NULL")
            logging.info(f"Successfully extracted {json_fields_count} fields from JSON format")
            
            # If we got fields from JSON, return the result
            if json_fields_count > 0:
                return result
            else:
                logging.warning("No fields extracted from JSON, falling back to text parsing")
                
    except (json.JSONDecodeError, Exception) as e:
        logging.warning(f"JSON parsing failed: {str(e)}, falling back to text parsing")
    
    # If JSON parsing failed or no fields were extracted, fall back to text parsing
    # Define patterns for extracting data from response
    patterns = {
        # (the rest of your patterns remain the same)
        # Step 1 patterns - Personal Info
        "PrimaryTitle": [r"- Best job title that fit their primary experience:\s*(.+)"],
        "SecondaryTitle": [r"- Best secondary job title that fits their secondary experience.*?:\s*(.+)"],
        "TertiaryTitle": [r"- Best tertiary job title that fits their tertiary experience.*?:\s*(.+)"],
        "Address": [r"- Their street address:\s*(.+)"],
        "City": [r"- Their City:\s*(.+)"],
        "State": [r"- Their State:\s*(.+)"],
        "Certifications": [r"- Their Certifications Listed:\s*(.+)"],
        "Bachelors": [r"- Their Bachelor's Degree:\s*(.+)"],
        "Masters": [r"- Their Master's Degree:\s*(.+)"],
        "Phone1": [r"- Their Phone Number:\s*(.+)"],
        "Phone2": [r"- Their Second Phone Number:\s*(.+)"],
        "Email": [r"- Their Email:\s*(.+)"],
        "Email2": [r"- Their Second Email:\s*(.+)"],
        "FirstName": [r"- Their First Name:\s*(.+)"],
        "MiddleName": [r"- Their Middle Name:\s*(.+)"],
        "LastName": [r"- Their Last Name:\s*(.+)"],
        "LinkedIn": [r"- Their Linkedin URL:\s*(.+)"],
        
        # Step 1 patterns - Work History
        "MostRecentCompany": [r"- Most Recent Company Worked for:\s*(.+)"],
        "MostRecentStartDate": [r"- Most Recent Start Date.*?:\s*(.+)"],
        "MostRecentEndDate": [r"- Most Recent End Date.*?:\s*(.+)"],
        "MostRecentLocation": [r"- Most Recent Job Location.*?:\s*(.+)"],
        "SecondMostRecentCompany": [r"- Second Most Recent Company Worked for:\s*(.+)"],
        "SecondMostRecentStartDate": [r"- Second Most Recent Start Date.*?:\s*(.+)"],
        "SecondMostRecentEndDate": [r"- Second Most Recent End Date.*?:\s*(.+)"],
        "SecondMostRecentLocation": [r"- Second Most Recent Job Location.*?:\s*(.+)"],
        "ThirdMostRecentCompany": [r"- Third Most Recent Company Worked for:\s*(.+)"],
        "ThirdMostRecentStartDate": [r"- Third Most Recent Start Date.*?:\s*(.+)"],
        "ThirdMostRecentEndDate": [r"- Third Most Recent End Date.*?:\s*(.+)"],
        "ThirdMostRecentLocation": [r"- Third Most Recent Job Location.*?:\s*(.+)"],
        "FourthMostRecentCompany": [r"- Fourth Most Recent Company Worked for:\s*(.+)"],
        "FourthMostRecentStartDate": [r"- Fourth Most Recent Start Date.*?:\s*(.+)"],
        "FourthMostRecentEndDate": [r"- Fourth Most Recent End Date.*?:\s*(.+)"],
        "FourthMostRecentLocation": [r"- Fourth Most Recent Job Location.*?:\s*(.+)"],
        "FifthMostRecentCompany": [r"- Fifth Most Recent Company Worked for:\s*(.+)"],
        "FifthMostRecentStartDate": [r"- Fifth Most Recent Start Date.*?:\s*(.+)"],
        "FifthMostRecentEndDate": [r"- Fifth Most Recent End Date.*?:\s*(.+)"],
        "FifthMostRecentLocation": [r"- Fifth Most Recent Job Location.*?:\s*(.+)"],
        "SixthMostRecentCompany": [r"- Sixth Most Recent Company Worked for:\s*(.+)"],
        "SixthMostRecentStartDate": [r"- Sixth Most Recent Start Date.*?:\s*(.+)"],
        "SixthMostRecentEndDate": [r"- Sixth Most Recent End Date.*?:\s*(.+)"],
        "SixthMostRecentLocation": [r"- Sixth Most Recent Job Location.*?:\s*(.+)"],
        "SeventhMostRecentCompany": [r"- Seventh Most Recent Company Worked for:\s*(.+)"],
        "SeventhMostRecentStartDate": [r"- Seventh Most Recent Start Date.*?:\s*(.+)"],
        "SeventhMostRecentEndDate": [r"- Seventh Most Recent End Date.*?:\s*(.+)"],
        "SeventhMostRecentLocation": [r"- Seventh Most Recent Job Location.*?:\s*(.+)"],
        
        # Step 1 patterns - Industry and Skills
        "PrimaryIndustry": [r"- Based on all 7 of their most recent companies above, what is the Primary industry they work in:\s*(.+)"],
        "SecondaryIndustry": [r"- Based on all 7 of their most recent companies above, what is the Secondary industry they work in:\s*(.+)"],
        "Top10Skills": [r"- Top 10 Technical Skills:\s*(.+)"],
        
        # Step 2 patterns - Technical Info
        "PrimarySoftwareLanguage": [r"- What technical language do they use most often\?:\s*(.+)"],
        "SecondarySoftwareLanguage": [r"- What technical language do they use second most often\?:\s*(.+)"],
        "TertiarySoftwareLanguage": [r"- What technical language do they use third most often\?:\s*(.+)"],
        "SoftwareApp1": [r"- What software do they talk about using the most\?:\s*(.+)"],
        "SoftwareApp2": [r"- What software do they talk about using the second most\?:\s*(.+)"],
        "SoftwareApp3": [r"- What software do they talk about using the third most\?:\s*(.+)"],
        "SoftwareApp4": [r"- What software do they talk about using the fourth most\?:\s*(.+)"],
        "SoftwareApp5": [r"- What software do they talk about using the fifth most\?:\s*(.+)"],
        "Hardware1": [r"- What physical hardware do they talk about using the most\?:\s*(.+)"],
        "Hardware2": [r"- What physical hardware do they talk about using the second most\?:\s*(.+)"],
        "Hardware3": [r"- What physical hardware do they talk about using the third most\?:\s*(.+)"],
        "Hardware4": [r"- What physical hardware do they talk about using the fourth most\?:\s*(.+)"],
        "Hardware5": [r"- What physical hardware do they talk about using the fifth most\?:\s*(.+)"],
        "PrimaryCategory": [r"- Based on their experience, put them in a primary technical category if they are technical or functional category if they are functional:\s*(.+)"],
        "SecondaryCategory": [r"- Based on their experience, put them in a subsidiary technical category if they are technical or functional category if they are functional:\s*(.+)"],
        "ProjectTypes": [r"- Types of projects they have worked on:\s*(.+)"],
        "Specialty": [r"- Based on their skills, categories, certifications, and industries, determine what they specialize in:\s*(.+)"],
        "Summary": [r"- Based on all this knowledge, write a summary of this candidate.*?:\s*(.+)"],
        "LengthinUS": [r"- How long have they lived in the United States.*?:\s*(.+)"],
        "YearsofExperience": [r"- Total years of professional experience.*?:\s*(.+)"],
        "AvgTenure": [r"- Average tenure at companies in years.*?:\s*(.+)"]
    }
    
    # Process the response line by line to cleanly extract each field
    lines = response_text.split('\n')
    
    # Process each line of the response
    for line in lines:
        line = line.strip()
        if not line or not line.startswith('-'):
            continue
        
        # Extract the question and answer parts
        parts = line.split(':', 1)  # Split only on the first colon
        if len(parts) != 2:
            continue
            
        question = parts[0].strip()
        answer = parts[1].strip()
        
        # Skip empty or NULL answers
        if not answer or answer.upper() == 'NULL':
            continue
            
        # Match the question to the correct field
        matched = False
        for field, pattern_list in patterns.items():
            for pattern in pattern_list:
                if re.search(pattern, question, re.DOTALL):
                    result[field] = answer
                    logging.info(f"Extracted '{field}': '{answer}'")
                    matched = True
                    break
            if matched:
                break
    
    # Count how many fields we successfully extracted
    populated_fields = sum(1 for val in result.values() if val != "NULL")
    logging.info(f"Successfully extracted {populated_fields} fields out of {len(result)} fields")
    
    # Additional logging to help diagnose field extraction issues
    step1_fields_count = sum(1 for field, val in result.items() 
                          if val != "NULL" and field not in ["PrimarySoftwareLanguage", "SecondarySoftwareLanguage", 
                                                            "TertiarySoftwareLanguage", "SoftwareApp1", "SoftwareApp2", 
                                                            "SoftwareApp3", "SoftwareApp4", "SoftwareApp5",
                                                            "Hardware1", "Hardware2", "Hardware3", "Hardware4", "Hardware5",
                                                            "PrimaryCategory", "SecondaryCategory", "ProjectTypes", 
                                                            "Specialty", "Summary", "LengthinUS", "YearsofExperience", "AvgTenure"])
    step2_fields_count = sum(1 for field, val in result.items() 
                          if val != "NULL" and field in ["PrimarySoftwareLanguage", "SecondarySoftwareLanguage", 
                                                        "TertiarySoftwareLanguage", "SoftwareApp1", "SoftwareApp2", 
                                                        "SoftwareApp3", "SoftwareApp4", "SoftwareApp5",
                                                        "Hardware1", "Hardware2", "Hardware3", "Hardware4", "Hardware5",
                                                        "PrimaryCategory", "SecondaryCategory", "ProjectTypes", 
                                                        "Specialty", "Summary", "LengthinUS", "YearsofExperience", "AvgTenure"])
    
    logging.info(f"Step 1 fields extracted: {step1_fields_count}, Step 2 fields extracted: {step2_fields_count}")
    
    return result

def process_unified_results(results: List[Dict], resume_map: Dict[int, str], debug_mode=True, debug_limit=20) -> Dict[int, Dict]:
    """
    Process the results from unified batch processing
    
    Args:
        results: List of result objects from the batch API
        resume_map: Dictionary mapping userids to resume texts
        debug_mode: Whether to generate debug files
        debug_limit: Maximum number of debug files to generate per batch
        
    Returns:
        Dictionary mapping userids to processed results
    """
    processed_results = {}
    
    for result in results:
        try:
            # Extract userid from custom_id (format: unified_<userid>)
            custom_id = result.get("custom_id", "")
            if not custom_id.startswith("unified_"):
                logging.warning(f"Unexpected custom_id format: {custom_id}")
                continue
                
            userid = int(custom_id.split("_")[1])
            
            # Check for errors
            error = result.get("error")
            if error:
                logging.error(f"Error in unified processing for UserID {userid}: {error}")
                continue
                
            # Extract the response content
            # Extract content from batch API response
            try:
                # Log the raw result structure for debugging
                logging.info(f"Response structure for UserID {userid}: {json.dumps(list(result.keys()))}")
                
                # The API response structure is typically:
                # {
                #    "custom_id": "unified_12345",
                #    "response": {
                #        "body": {
                #            "choices": [
                #                {"message": {"content": "The actual content"}}
                #            ]
                #        }
                #    }
                # }
                
                # Initialize content
                content = ""
                
                # First, try the typical OpenAI response structure
                if "response" in result:
                    response_obj = result.get("response", {})
                    
                    if isinstance(response_obj, dict):
                        # Standard OpenAI API format for batch API
                        if "body" in response_obj:
                            body = response_obj.get("body", {})
                            
                            # The body might be a string that needs parsing
                            if isinstance(body, str):
                                try:
                                    # Try to parse it as JSON
                                    body_obj = json.loads(body)
                                    body = body_obj
                                    logging.info(f"Successfully parsed body string as JSON for UserID {userid}")
                                except json.JSONDecodeError:
                                    # Keep it as a string if it's not JSON
                                    content = body
                                    logging.info(f"Body is a direct string for UserID {userid}")
                            
                            # Extract content from parsed body
                            if isinstance(body, dict):
                                # Standard OpenAI format with choices
                                if "choices" in body:
                                    choices = body.get("choices", [])
                                    if choices and len(choices) > 0:
                                        # Standard OpenAI response format
                                        choice = choices[0]
                                        
                                        if isinstance(choice, dict):
                                            if "message" in choice:
                                                message = choice.get("message", {})
                                                if isinstance(message, dict) and "content" in message:
                                                    content = message.get("content", "")
                                                    logging.info(f"Found content in choices[0].message.content for UserID {userid}")
                                            elif "content" in choice:
                                                # Direct content in choice
                                                content = choice.get("content", "")
                                                logging.info(f"Found content in choices[0].content for UserID {userid}")
                                
                                # Another format: content directly in body
                                elif "content" in body:
                                    content = body.get("content", "")
                                    logging.info(f"Found content directly in body for UserID {userid}")
                        
                        # Try to find content directly in response_obj
                        elif not content and "content" in response_obj:
                            content = response_obj.get("content", "")
                            logging.info(f"Found content directly in response_obj for UserID {userid}")
                        
                        # Try message format directly in response_obj
                        elif not content and "message" in response_obj:
                            message = response_obj.get("message", {})
                            if isinstance(message, dict) and "content" in message:
                                content = message.get("content", "")
                                logging.info(f"Found content in response_obj.message.content for UserID {userid}")
                    
                    # If response_obj itself is a string
                    elif isinstance(response_obj, str):
                        # Try to parse it as JSON
                        try:
                            response_json = json.loads(response_obj)
                            # Log the keys to help diagnose
                            logging.info(f"Parsed response_obj string as JSON with keys: {list(response_json.keys())}")
                            
                            # Try standard OpenAI response structure
                            if "choices" in response_json:
                                choices = response_json.get("choices", [])
                                if choices and len(choices) > 0:
                                    choice = choices[0]
                                    if isinstance(choice, dict) and "message" in choice:
                                        message = choice.get("message", {})
                                        if isinstance(message, dict) and "content" in message:
                                            content = message.get("content", "")
                                            logging.info(f"Found content in parsed response string for UserID {userid}")
                            
                            # If we still don't have content, try to use the whole object
                            if not content:
                                content = json.dumps(response_json)
                                logging.info(f"Using entire parsed response object as content for UserID {userid}")
                        except json.JSONDecodeError:
                            # If it's not JSON, use it directly
                            content = response_obj
                            logging.info(f"Using response_obj string directly as content for UserID {userid}")
                
                # If we don't have content yet, look for any "content" key in the result
                if not content:
                    # Try to find content field anywhere in the result
                    if "content" in result:
                        content = result.get("content", "")
                        logging.info(f"Found content directly in result for UserID {userid}")
                    else:
                        # Last resort: use the entire result
                        content = json.dumps(result)
                        logging.warning(f"Using entire result as content for UserID {userid}")
                
                # Clean up content if it contains markdown code blocks
                if content.startswith('```') and '```' in content:
                    # Extract content from code blocks
                    code_block_pattern = r'```(?:json)?\s*([\s\S]*?)\s*```'
                    code_blocks = re.findall(code_block_pattern, content)
                    if code_blocks:
                        # Use the largest code block (assuming it's the most complete)
                        content = max(code_blocks, key=len)
                        logging.info(f"Extracted content from markdown code block for UserID {userid}")
                
                # Debugging is now handled by parse_unified_response based on debug settings
                
                logging.info(f"Final extracted content length: {len(content)} chars")
                
                # Debug logging to help identify structure
                if len(content) < 100:
                    logging.warning(f"Very short content for UserID {userid}: '{content}'")
                else:
                    # Log a snippet of the content for debugging
                    logging.info(f"Content snippet for UserID {userid}: {content[:200]}...")
                
            except Exception as extraction_error:
                logging.error(f"Error extracting content for UserID {userid}: {str(extraction_error)}")
                content = ""
            
            if not content:
                logging.error(f"No content found in unified result for UserID {userid}")
                continue
                
            # Since we're not using JSON format instruction anymore,
            # we expect text-based responses, not JSON
            # But still try JSON parsing in case the model returns JSON anyway
            content_fixed = re.sub(r'\bNULL\b', '"NULL"', content)

            # Try to parse as JSON if the model returns it
            parsed_results = {}
            json_parsed = False

            if content.strip().startswith('{'):
                try:
                    json_data = json.loads(content_fixed)
                    logging.info(f"Successfully parsed JSON response for UserID {userid}")
                    json_parsed = True

                    # Map the nested JSON structure to database fields
                    if isinstance(json_data, dict):
                        # Personal Information
                        if "PERSONAL_INFORMATION" in json_data:
                            personal = json_data["PERSONAL_INFORMATION"]
                        if "Name" in personal:
                            parsed_results["FirstName"] = personal["Name"].get("FirstName")
                            parsed_results["MiddleName"] = personal["Name"].get("MiddleName")
                            parsed_results["LastName"] = personal["Name"].get("LastName")
                        if "Contact" in personal:
                            parsed_results["Phone1"] = personal["Contact"].get("Phone")
                            parsed_results["Phone2"] = personal["Contact"].get("SecondaryPhone")
                            parsed_results["Email"] = personal["Contact"].get("Email")
                            parsed_results["Email2"] = personal["Contact"].get("SecondaryEmail")
                            parsed_results["LinkedIn"] = personal["Contact"].get("LinkedIn")
                            parsed_results["Address"] = personal["Contact"].get("Address")
                            parsed_results["City"] = personal["Contact"].get("City")
                            parsed_results["State"] = personal["Contact"].get("State")
                        if "Education" in personal:
                            parsed_results["Bachelors"] = personal["Education"].get("BachelorsDegree")
                            parsed_results["Masters"] = personal["Education"].get("MastersDegree")
                            parsed_results["Certifications"] = personal["Education"].get("Certifications")

                    # Work History
                    if "WORK_HISTORY" in json_data:
                        work = json_data["WORK_HISTORY"]
                        work_fields = [
                            ("MostRecent", "MostRecent"),
                            ("SecondMostRecent", "SecondMostRecent"),
                            ("ThirdMostRecent", "ThirdMostRecent"),
                            ("FourthMostRecent", "FourthMostRecent"),
                            ("FifthMostRecent", "FifthMostRecent"),
                            ("SixthMostRecent", "SixthMostRecent"),
                            ("SeventhMostRecent", "SeventhMostRecent")
                        ]
                        for json_key, db_prefix in work_fields:
                            if json_key in work and work[json_key]:
                                parsed_results[f"{db_prefix}Company"] = work[json_key].get("Company")
                                parsed_results[f"{db_prefix}StartDate"] = work[json_key].get("StartDate")
                                parsed_results[f"{db_prefix}EndDate"] = work[json_key].get("EndDate")
                                parsed_results[f"{db_prefix}Location"] = work[json_key].get("Location")

                    # Career Info
                    if "CAREER_INFO" in json_data:
                        career = json_data["CAREER_INFO"]
                        parsed_results["PrimaryTitle"] = career.get("PrimaryTitle")
                        parsed_results["SecondaryTitle"] = career.get("SecondaryTitle")
                        parsed_results["TertiaryTitle"] = career.get("TertiaryTitle")
                        parsed_results["PrimaryIndustry"] = career.get("PrimaryIndustry")
                        parsed_results["SecondaryIndustry"] = career.get("SecondaryIndustry")

                        # Handle TopSkills - can be either array or comma-separated string
                        top_skills = career.get("TopSkills")
                        if top_skills:
                            if isinstance(top_skills, list):
                                # It's already an array
                                skills = top_skills
                                parsed_results["Top10Skills"] = ", ".join(skills[:10])
                            else:
                                # It's a string
                                parsed_results["Top10Skills"] = top_skills
                                skills = [s.strip() for s in top_skills.split(",")]

                            # Parse skills into individual fields
                            for i, skill in enumerate(skills[:10], 1):
                                parsed_results[f"Skill{i}"] = skill

                    # Technical Info
                    if "TECHNICAL_INFO" in json_data:
                        tech = json_data["TECHNICAL_INFO"]
                        parsed_results["PrimarySoftwareLanguage"] = tech.get("PrimaryLanguage")
                        parsed_results["SecondarySoftwareLanguage"] = tech.get("SecondaryLanguage")
                        parsed_results["TertiarySoftwareLanguage"] = tech.get("TertiaryLanguage")
                        for i in range(1, 6):
                            parsed_results[f"SoftwareApp{i}"] = tech.get(f"SoftwareApp{i}")
                            parsed_results[f"Hardware{i}"] = tech.get(f"Hardware{i}")
                        parsed_results["PrimaryCategory"] = tech.get("PrimaryCategory")
                        parsed_results["SecondaryCategory"] = tech.get("SecondaryCategory")
                        parsed_results["ProjectTypes"] = tech.get("ProjectTypes")
                        parsed_results["Specialty"] = tech.get("Specialty")
                        parsed_results["Summary"] = tech.get("Summary")
                        parsed_results["LengthinUS"] = tech.get("LengthInUS")
                        parsed_results["YearsofExperience"] = tech.get("YearsOfExperience")
                        parsed_results["AvgTenure"] = tech.get("AvgTenure")

                    # Clean up NULL values
                    for key, value in parsed_results.items():
                        if value == "NULL" or value == "null":
                            parsed_results[key] = None

                    logging.info(f"Extracted {len(parsed_results)} fields from JSON for UserID {userid}")

                except json.JSONDecodeError as e:
                    logging.warning(f"JSON parsing failed for UserID {userid}: {e}")
                    json_parsed = False

            # If JSON parsing didn't work or wasn't attempted, use text parser
            if not json_parsed:
                logging.info(f"Using text parser for UserID {userid}")

                # Use the EXACT SAME parser as single_step_processor
                # Already imported at the top of the file!
                parsed_results = parse_unified_response(content)

                logging.info(f"Extracted {len(parsed_results)} fields from unified parser for UserID {userid}")

            # Apply enhanced date processing
            enhanced_results = process_resume_with_enhanced_dates(userid, parsed_results)
            
            # Store the final results
            processed_results[userid] = enhanced_results
            
            logging.info(f"Processed unified results for UserID {userid}: {len(enhanced_results)} fields extracted")
            
        except Exception as e:
            userid_info = f"UserID {userid}" if 'userid' in locals() else "Unknown UserID"
            logging.error(f"Error processing unified result for {userid_info}: {str(e)}")
    
    return processed_results

def update_database_with_results(results: Dict[int, Dict]) -> Dict[int, bool]:
    """
    Update the database with the processed results

    Args:
        results: Dictionary mapping userids to processed results

    Returns:
        Dictionary mapping userids to success status
    """
    update_status = {}

    # Log all userids being updated
    userid_list = list(results.keys())
    logging.info(f"Starting database updates for {len(userid_list)} UserIDs: {userid_list}")

    for userid, data in results.items():
        try:
            # Extract skills for database format
            if "Top10Skills" in data:
                if data.get("Top10Skills") and data.get("Top10Skills") != "NULL":
                    # If skills exist and aren't NULL, split them
                    skills_list = data.get("Top10Skills", "").split(", ")
                else:
                    # If skills are NULL, use an empty list but log it
                    skills_list = []
                    logging.info(f"UserID {userid}: Top10Skills is NULL or empty")
            else:
                # Field doesn't exist in data, use empty list
                skills_list = []
                logging.info(f"UserID {userid}: Top10Skills field not found in data")
                
            # Ensure we have 10 skills
            skills_list.extend([""] * (10 - len(skills_list)))
            Skill1, Skill2, Skill3, Skill4, Skill5, Skill6, Skill7, Skill8, Skill9, Skill10 = skills_list[:10]
            
            # Debug log skills
            logging.info(f"UserID {userid}: Skills extracted - {', '.join([s for s in skills_list if s])}")
            if not any(skills_list):
                logging.info(f"UserID {userid}: No skills found in data")
            
            # Function to convert "NULL" string to None for SQL NULL
            def convert_null(value):
                if value is None or value == "" or value == "NULL":
                    return None
                return value
                
            # Create update data dictionary
            update_data = {
                "PrimaryTitle": convert_null(data.get("PrimaryTitle")),
                "SecondaryTitle": convert_null(data.get("SecondaryTitle")),
                "TertiaryTitle": convert_null(data.get("TertiaryTitle")),
                "Address": convert_null(data.get("Address")),
                "City": convert_null(data.get("City")),
                "State": convert_null(data.get("State")),
                "Certifications": convert_null(data.get("Certifications")),
                "Bachelors": convert_null(data.get("Bachelors")),
                "Masters": convert_null(data.get("Masters")),
                "Phone1": convert_null(data.get("Phone1")),
                "Phone2": convert_null(data.get("Phone2")),
                "Email": convert_null(data.get("Email")),
                "Email2": convert_null(data.get("Email2")),
                "FirstName": convert_null(data.get("FirstName")),
                "MiddleName": convert_null(data.get("MiddleName")),
                "LastName": convert_null(data.get("LastName")),
                "Linkedin": convert_null(data.get("Linkedin")),
                "MostRecentCompany": convert_null(data.get("MostRecentCompany")),
                "MostRecentStartDate": convert_null(data.get("MostRecentStartDate")),
                "MostRecentEndDate": convert_null(data.get("MostRecentEndDate")),
                "MostRecentLocation": convert_null(data.get("MostRecentLocation")),
                "SecondMostRecentCompany": convert_null(data.get("SecondMostRecentCompany")),
                "SecondMostRecentStartDate": convert_null(data.get("SecondMostRecentStartDate")),
                "SecondMostRecentEndDate": convert_null(data.get("SecondMostRecentEndDate")),
                "SecondMostRecentLocation": convert_null(data.get("SecondMostRecentLocation")),
                "ThirdMostRecentCompany": convert_null(data.get("ThirdMostRecentCompany")),
                "ThirdMostRecentStartDate": convert_null(data.get("ThirdMostRecentStartDate")),
                "ThirdMostRecentEndDate": convert_null(data.get("ThirdMostRecentEndDate")),
                "ThirdMostRecentLocation": convert_null(data.get("ThirdMostRecentLocation")),
                "FourthMostRecentCompany": convert_null(data.get("FourthMostRecentCompany")),
                "FourthMostRecentStartDate": convert_null(data.get("FourthMostRecentStartDate")),
                "FourthMostRecentEndDate": convert_null(data.get("FourthMostRecentEndDate")),
                "FourthMostRecentLocation": convert_null(data.get("FourthMostRecentLocation")),
                "FifthMostRecentCompany": convert_null(data.get("FifthMostRecentCompany")),
                "FifthMostRecentStartDate": convert_null(data.get("FifthMostRecentStartDate")),
                "FifthMostRecentEndDate": convert_null(data.get("FifthMostRecentEndDate")),
                "FifthMostRecentLocation": convert_null(data.get("FifthMostRecentLocation")),
                "SixthMostRecentCompany": convert_null(data.get("SixthMostRecentCompany")),
                "SixthMostRecentStartDate": convert_null(data.get("SixthMostRecentStartDate")),
                "SixthMostRecentEndDate": convert_null(data.get("SixthMostRecentEndDate")),
                "SixthMostRecentLocation": convert_null(data.get("SixthMostRecentLocation")),
                "SeventhMostRecentCompany": convert_null(data.get("SeventhMostRecentCompany")),
                "SeventhMostRecentStartDate": convert_null(data.get("SeventhMostRecentStartDate")),
                "SeventhMostRecentEndDate": convert_null(data.get("SeventhMostRecentEndDate")),
                "SeventhMostRecentLocation": convert_null(data.get("SeventhMostRecentLocation")),
                "PrimaryIndustry": convert_null(data.get("PrimaryIndustry")),
                "SecondaryIndustry": convert_null(data.get("SecondaryIndustry")),
                "Skill1": convert_null(Skill1),
                "Skill2": convert_null(Skill2),
                "Skill3": convert_null(Skill3),
                "Skill4": convert_null(Skill4),
                "Skill5": convert_null(Skill5),
                "Skill6": convert_null(Skill6),
                "Skill7": convert_null(Skill7),
                "Skill8": convert_null(Skill8),
                "Skill9": convert_null(Skill9),
                "Skill10": convert_null(Skill10),
                "PrimarySoftwareLanguage": convert_null(data.get("PrimarySoftwareLanguage")),
                "SecondarySoftwareLanguage": convert_null(data.get("SecondarySoftwareLanguage")),
                "TertiarySoftwareLanguage": convert_null(data.get("TertiarySoftwareLanguage")),
                "SoftwareApp1": convert_null(data.get("SoftwareApp1")),
                "SoftwareApp2": convert_null(data.get("SoftwareApp2")),
                "SoftwareApp3": convert_null(data.get("SoftwareApp3")),
                "SoftwareApp4": convert_null(data.get("SoftwareApp4")),
                "SoftwareApp5": convert_null(data.get("SoftwareApp5")),
                "Hardware1": convert_null(data.get("Hardware1")),
                "Hardware2": convert_null(data.get("Hardware2")),
                "Hardware3": convert_null(data.get("Hardware3")),
                "Hardware4": convert_null(data.get("Hardware4")),
                "Hardware5": convert_null(data.get("Hardware5")),
                "PrimaryCategory": convert_null(data.get("PrimaryCategory")),
                "SecondaryCategory": convert_null(data.get("SecondaryCategory")),
                "ProjectTypes": convert_null(data.get("ProjectTypes")),
                "Specialty": convert_null(data.get("Specialty")),
                "Summary": convert_null(data.get("Summary")),
                "LengthinUS": convert_null(data.get("LengthinUS")),
                "YearsofExperience": convert_null(data.get("YearsofExperience")),
                "AvgTenure": convert_null(data.get("AvgTenure"))
            }
            
            # Log what we're about to update
            logging.info(f"Updating database for UserID {userid} with {len(update_data)} fields")
            
            # Define date fields to handle specially
            date_fields = [
                "MostRecentStartDate", "MostRecentEndDate",
                "SecondMostRecentStartDate", "SecondMostRecentEndDate",
                "ThirdMostRecentStartDate", "ThirdMostRecentEndDate",
                "FourthMostRecentStartDate", "FourthMostRecentEndDate",
                "FifthMostRecentStartDate", "FifthMostRecentEndDate",
                "SixthMostRecentStartDate", "SixthMostRecentEndDate",
                "SeventhMostRecentStartDate", "SeventhMostRecentEndDate"
            ]
            
            # Clean up data with special handling for date fields
            for key, value in update_data.items():
                if key in date_fields:
                    # Special handling for date fields
                    if value is None or (isinstance(value, str) and (value.upper() == "NULL" or not value.strip() or "present" in value.lower() or "current" in value.lower())):
                        # Set to NULL for empty or present/current dates
                        update_data[key] = None
                        logging.info(f"Setting date field '{key}' to NULL")
                    elif isinstance(value, str):
                        # Remove any quotes that might be in the date string
                        cleaned_value = value.strip("'").strip('"')
                        # Check for "NULL" string returned from process_resume_with_enhanced_dates
                        if cleaned_value.upper() == "NULL":
                            update_data[key] = None
                            logging.info(f"Converting 'NULL' string to NULL for date field '{key}'")
                        elif cleaned_value != value:
                            logging.info(f"Cleaned date value for {key}: '{value}' -> '{cleaned_value}'")
                            update_data[key] = cleaned_value
                else:
                    # Standard handling for non-date fields
                    if isinstance(value, str) and (value.upper() == "NULL" or not value.strip()):
                        update_data[key] = None  # Use None instead of empty string for proper SQL NULL values
            
            # Update database
            success = update_candidate_record_with_retry(userid, update_data)
            update_status[userid] = success
            
            if success:
                logging.info(f"Successfully updated database for UserID {userid}")
            else:
                logging.error(f"Failed to update database for UserID {userid}")
                
        except Exception as e:
            logging.error(f"Error updating database for UserID {userid}: {str(e)}")
            update_status[userid] = False
    
    return update_status

def run_unified_processing(batch_size=BATCH_SIZE, debug_mode=True, debug_limit=20):
    """
    Main function to run the unified batch processing pipeline
    
    Args:
        batch_size: Number of records to process in the batch
        debug_mode: Whether to generate debug files
        debug_limit: Maximum number of debug files to generate per batch
    """
    logging.info(f"Starting unified batch processing using OpenAI Batch API (debug_mode={debug_mode}, debug_limit={debug_limit})")
    
    # Get unprocessed resume batch
    resume_batch = get_resume_batch(batch_size=batch_size)
    
    if not resume_batch:
        logging.info("No resumes to process, exiting")
        return
    
    # Create batch input file
    batch_filepath, batch_id, request_count = create_batch_input_file(resume_batch)
    
    if not batch_filepath or request_count == 0:
        logging.error("Failed to create batch input file, aborting")
        return
    
    # Extract userids
    userids = [userid for userid, _ in resume_batch]
    
    # Create a mapping of userids to resume texts for later use
    resume_map = {userid: resume_text for userid, resume_text in resume_batch}
    
    logging.info(f"Processing batch {batch_id} with {len(resume_batch)} resumes")
    
    # Upload the file to OpenAI
    input_file_id = upload_batch_file(batch_filepath)
    if not input_file_id:
        logging.error("Failed to upload batch input file, aborting")
        return
    
    # Submit the batch job
    openai_batch_id = submit_batch_job(input_file_id)
    if not openai_batch_id:
        logging.error("Failed to submit batch job, aborting")
        return
    
    logging.info(f"Submitted unified batch job {openai_batch_id}, waiting for completion (up to 24 hours)")
    
    # For now, set a reminder to check back - a production system would use a scheduler
    next_check_time = datetime.now() + timedelta(hours=24)
    logging.info(f"Check back after {next_check_time.strftime('%Y-%m-%d %H:%M:%S')} using --check-batch {openai_batch_id}")
    
    # Return info for the user
    return {
        "batch_id": batch_id,
        "openai_batch_id": openai_batch_id,
        "input_file_id": input_file_id,
        "request_count": request_count,
        "next_check_time": next_check_time
    }

def check_and_process_batch(openai_batch_id: str, debug_mode=True, debug_limit=20):
    """
    Check a specific batch job and process the results if completed
    
    Args:
        openai_batch_id: The OpenAI batch ID to check
        debug_mode: Whether to generate debug files
        debug_limit: Maximum number of debug files to generate per batch
    """
    logging.info(f"Checking batch job {openai_batch_id}")
    
    # Check batch status
    batch_status = get_batch_status(openai_batch_id)
    status = batch_status.get("status")
    
    if not status:
        logging.error(f"Failed to get status for batch job {openai_batch_id}")
        return {"status": "error", "message": f"Failed to get status for batch job {openai_batch_id}"}
    
    logging.info(f"Batch job {openai_batch_id} status: {status}")
    
    if status == "completed":
        # Process the results
        logging.info(f"Batch job {openai_batch_id} completed, processing results")
        
        # Get the output file ID
        output_file_id = batch_status.get("output_file_id")
        if not output_file_id:
            logging.error(f"No output file ID found for batch job {openai_batch_id}")
            return {"status": "error", "message": f"No output file ID found for batch job {openai_batch_id}"}
        
        # Download and parse the results
        results = get_file_content(output_file_id)
        if not results:
            logging.error(f"No results found in output file {output_file_id}")
            return {"status": "error", "message": f"No results found in output file {output_file_id}"}
        
        # Get the affected userids from the results
        userids = []
        for result in results:
            custom_id = result.get("custom_id", "")
            if custom_id.startswith("unified_"):
                userid = int(custom_id.split("_")[1])
                userids.append(userid)
        
        # Get resume texts for these userids
        try:
            # Connect to the database using the robust connection function
            conn_result = create_pyodbc_connection()
            # Handle the tuple return from create_pyodbc_connection
            if isinstance(conn_result, tuple):
                conn, success, message = conn_result
                if not success:
                    logging.error(f"Failed to connect to database: {message}")
                    resume_map = {}
                else:
                    cursor = conn.cursor()
            else:
                conn = conn_result
                cursor = conn.cursor()
            
            # Get resumes for all userids
            resume_map = {}
            for userid in userids:
                cursor.execute("SELECT markdownResume FROM dbo.aicandidate WHERE userid = ?", userid)
                row = cursor.fetchone()
                if row:
                    resume_map[userid] = row[0]
            
            cursor.close()
            conn.close()
        except Exception as e:
            logging.error(f"Error fetching resume texts: {str(e)}")
            resume_map = {}
        
        # Process the unified results with debug parameters
        processed_results = process_unified_results(results, resume_map, debug_mode=debug_mode, debug_limit=debug_limit)
        
        if not processed_results:
            logging.error(f"No processed results for batch job {openai_batch_id}")
            return {"status": "error", "message": f"No processed results for batch job {openai_batch_id}"}
        
        # Update the database
        update_status = update_database_with_results(processed_results)
        
        success_count = sum(1 for status in update_status.values() if status)
        
        # Calculate cost estimates based on actual token counts
        total_unified_requests = len(results)
        
        # Load model encoding (use gpt-4 encoding for gpt-5 models)
        model_for_encoding = MODEL
        if "gpt-5" in MODEL.lower():
            model_for_encoding = "gpt-4"  # Use gpt-4 encoding for gpt-5 models
        encoding = tiktoken.encoding_for_model(model_for_encoding)
        
        # Get actual token count for each resume's request and response
        input_tokens = 0
        output_tokens = 0
        
        # First pass to collect actual token counts
        for result in results:
            try:
                # Get the request tokens from the input file if available
                # This will give a more accurate count than estimation
                custom_id = result.get("custom_id", "")
                if custom_id.startswith("unified_"):
                    userid = int(custom_id.split("_")[1])
                    if userid in resume_map:
                        # Count tokens in the messages array (more accurate)
                        resume_text = resume_map.get(userid, "")
                        messages = create_unified_prompt(resume_text, userid=userid)
                        # Add the JSON formatting instruction
                        format_instruction = {
                            "role": "system",
                            "content": "IMPORTANT: Format your response as JSON..."  # Abbreviated for counting
                        }
                        messages.append(format_instruction)
                        
                        # Count actual tokens in the messages
                        for msg in messages:
                            if isinstance(msg, dict) and "content" in msg:
                                input_tokens += len(encoding.encode(msg["content"]))
                
                # Count actual tokens in the response content
                content = ""
                if "response" in result:
                    response_obj = result.get("response", {})
                    body = response_obj.get("body", {})
                    if isinstance(body, dict) and "choices" in body:
                        choices = body.get("choices", [])
                        if choices and len(choices) > 0:
                            choice = choices[0]
                            if isinstance(choice, dict) and "message" in choice:
                                message = choice.get("message", {})
                                if isinstance(message, dict) and "content" in message:
                                    content = message.get("content", "")
                
                if content:
                    output_tokens += len(encoding.encode(content))
                else:
                    # Fallback to estimate if we couldn't get actual content
                    output_tokens += 1000  # Default estimate
                    
            except Exception as e:
                logging.warning(f"Error counting tokens for result: {str(e)}")
                # Fallback to estimates if token counting fails
                input_tokens += 12800  # Conservative estimate for prompt size
                output_tokens += 1000  # Default estimate for response
        
        # If we couldn't get a reasonable token count, use fallback estimates
        if input_tokens == 0:
            input_tokens = total_unified_requests * 12800  # Higher estimate to be safe
            
        if output_tokens == 0:
            output_tokens = total_unified_requests * 1000  # Estimate 1000 tokens per response
        
        # Calculate costs using OpenAI pricing based on the model
        # Get the pricing for the current model
        pricing = {
            # GPT-4 models
            "gpt-4": {"input": 0.00003, "output": 0.00006},  # $30/M input, $60/M output
            "gpt-4-32k": {"input": 0.00006, "output": 0.00012},  # $60/M input, $120/M output
            "gpt-4-turbo": {"input": 0.00001, "output": 0.00003},  # $10/M input, $30/M output
            "gpt-4o": {"input": 0.00001, "output": 0.00003},  # $10/M input, $30/M output
            
            # GPT-4 mini/micro models
            "gpt-4o-mini": {"input": 0.000000075, "output": 0.0000003},  # $0.075/M input, $0.30/M output
            "gpt-4o-mini-2024-07-18": {"input": 0.000000075, "output": 0.0000003},  # $0.075/M input, $0.30/M output
            
            # Fallback to default pricing
            "default": {"input": 0.000000075, "output": 0.0000003}  # Default to gpt-4o-mini pricing
        }
        
        # Get rates for the current model or fall back to default
        model_pricing = pricing.get(MODEL, pricing["default"])
        standard_input_rate = model_pricing["input"]
        standard_output_rate = model_pricing["output"]
        
        # Batch API provides a 50% discount
        batch_discount = 0.5  # 50% discount for batch API
        
        # Calculate costs with batch discount
        input_cost = input_tokens * standard_input_rate * batch_discount
        output_cost = output_tokens * standard_output_rate * batch_discount
        total_cost = input_cost + output_cost
        
        # Calculate what it would have cost with standard API
        standard_cost = (input_tokens * standard_input_rate) + (output_tokens * standard_output_rate)
        savings = standard_cost - total_cost
        
        # Log cost information with detailed breakdown
        logging.info(f"Cost Analysis for Unified Batch Processing with model {MODEL}:")
        logging.info(f"- Processed {total_unified_requests} records")
        logging.info(f"- Total input tokens: {input_tokens:,}")
        logging.info(f"- Total output tokens: {output_tokens:,}")
        logging.info(f"- Average input tokens per record: {input_tokens/total_unified_requests:,.1f}")
        logging.info(f"- Average output tokens per record: {output_tokens/total_unified_requests:,.1f}")
        logging.info(f"- Model pricing (per million tokens): Input=${standard_input_rate*1000000:.2f}, Output=${standard_output_rate*1000000:.2f}")
        logging.info(f"- Input cost (with 50% batch discount): ${input_cost:.6f}")
        logging.info(f"- Output cost (with 50% batch discount): ${output_cost:.6f}")
        logging.info(f"- Total batch cost: ${total_cost:.6f}")
        logging.info(f"- Standard API cost would be: ${standard_cost:.6f}")
        logging.info(f"- Savings from batch API: ${savings:.6f} ({(savings/standard_cost)*100:.1f}%)")
        logging.info(f"- Per-record cost: ${total_cost/total_unified_requests:.6f}")

        # Create lists of successful and failed userids
        successful_userids = [uid for uid, status in update_status.items() if status]
        failed_userids = [uid for uid, status in update_status.items() if not status]

        # Log final summary with UserIDs
        logging.info(f"===== BATCH PROCESSING COMPLETE for batch {openai_batch_id} =====")
        logging.info(f"Successfully updated {len(successful_userids)} UserIDs: {successful_userids}")
        if failed_userids:
            logging.info(f"Failed to update {len(failed_userids)} UserIDs: {failed_userids}")

        return {
            "status": "completed",
            "total_records": len(processed_results),
            "success_count": success_count,
            "failure_count": len(processed_results) - success_count,
            "successful_userids": successful_userids,
            "failed_userids": failed_userids,
            "cost_estimates": {
                "total_cost": total_cost,
                "standard_cost": standard_cost,
                "savings": savings,
                "cost_per_record": total_cost/total_unified_requests
            }
        }
    
    elif status == "failed":
        # Batch job failed
        error_file_id = batch_status.get("error_file_id")
        error_message = f"Batch job {openai_batch_id} failed"
        
        if error_file_id:
            # Download and check the error file
            error_results = get_file_content(error_file_id)
            if error_results:
                error_message = f"{error_message}. First error: {error_results[0] if error_results else 'Unknown error'}"
        
        logging.error(error_message)
        return {"status": "failed", "message": error_message}
    
    else:
        # Batch job still processing
        created_at = batch_status.get("created_at")
        if created_at:
            # Add 24 hours to created_at for estimated completion
            created_timestamp = datetime.fromtimestamp(created_at)
            estimated_completion = created_timestamp + timedelta(hours=24)
            now = datetime.now()
            hours_remaining = (estimated_completion - now).total_seconds() / 3600
            return {
                "status": status,
                "message": f"Batch job {openai_batch_id} status: {status}",
                "hours_remaining": round(hours_remaining, 1)
            }
        else:
            return {"status": status, "message": f"Batch job {openai_batch_id} status: {status}"}

def run_parallel_processing(batch_size: int, num_batches: int = 1, batch_delay: int = 600, check_interval: int = 600, debug_mode=True, debug_limit=20):
    """
    Run batch processing in parallel with automatic processing of results
    
    Args:
        batch_size: Number of records to process in each batch
        num_batches: Number of batches to process in total
        batch_delay: Seconds to wait between submitting batches
        check_interval: How often to check for batch completion in seconds
        debug_mode: Whether to generate debug files
        debug_limit: Maximum number of debug files to generate per batch
    """
    logging.info(f"Starting parallel batch processing with {num_batches} batches of {batch_size} records each")
    print(f"Starting parallel batch processing with {num_batches} batches of {batch_size} records each")
    print(f"Will submit a new batch every {batch_delay} seconds")
    print(f"Will check batch status every {check_interval} seconds")
    
    # Track all batch IDs
    submitted_batches = []
    completed_batches = []
    batch_start_times = {}
    
    # Phase 1: Submit all batches with delay
    for i in range(num_batches):
        print(f"\nSubmitting batch {i+1}/{num_batches}...")
        logging.info(f"Submitting batch {i+1}/{num_batches}")
        
        result = run_unified_processing(
            batch_size=batch_size,
            debug_mode=debug_mode,
            debug_limit=debug_limit
        )
        if result and "openai_batch_id" in result:
            batch_id = result["openai_batch_id"]
            submitted_batches.append(batch_id)
            batch_start_times[batch_id] = time.time()
            
            logging.info(f"Successfully submitted batch {i+1}/{num_batches}: {batch_id}")
            print(f"✓ Batch {i+1}/{num_batches}: OpenAI batch ID {batch_id}")
            print(f"  Processing {result['request_count']} records")
            print(f"  Expected completion: {result['next_check_time'].strftime('%Y-%m-%d %H:%M:%S')}")
        else:
            logging.error(f"Failed to submit batch {i+1}/{num_batches}")
            print(f"✗ Failed to submit batch {i+1}/{num_batches}. Check logs for details.")
        
        # Wait before submitting next batch (unless it's the last one)
        if i < num_batches - 1:
            print(f"Waiting {batch_delay} seconds before submitting next batch...")
            time.sleep(batch_delay)
    
    print(f"\nAll {len(submitted_batches)} batches submitted successfully")
    print(f"Checking batch status every {check_interval} seconds until all complete")
    
    # Phase 2: Monitor and process completed batches
    while submitted_batches:
        print(f"\nChecking status of {len(submitted_batches)} active batches...")
        logging.info(f"Checking status of {len(submitted_batches)} active batches")
        
        still_processing = []
        newly_completed = []
        
        for batch_id in submitted_batches:
            elapsed_time = (time.time() - batch_start_times[batch_id]) / 60
            print(f"Checking batch {batch_id} (running for {elapsed_time:.1f} minutes)")
            
            result = check_and_process_batch(
                batch_id,
                debug_mode=debug_mode,
                debug_limit=debug_limit
            )
            if result and result['status'] == 'completed':
                logging.info(f"Batch {batch_id} completed successfully")
                print(f"✓ Batch {batch_id} completed successfully")
                if 'total_records' in result:
                    print(f"  Processed {result['total_records']} records")
                    print(f"  Success: {result['success_count']}, Failed: {result['failure_count']}")
                    if 'cost_estimates' in result:
                        print(f"  Cost: ${result['cost_estimates']['total_cost']:.4f}")
                        print(f"  Cost per record: ${result['cost_estimates']['cost_per_record']:.6f}")
                
                # Mark as completed
                completed_batches.append(batch_id)
                newly_completed.append(batch_id)
                
            elif result and result['status'] == 'failed':
                logging.error(f"Batch {batch_id} failed: {result.get('message', 'Unknown error')}")
                print(f"✗ Batch {batch_id} FAILED: {result.get('message', 'Unknown error')}")
                
                # Consider it completed (failed)
                completed_batches.append(batch_id)
                newly_completed.append(batch_id)
                
            else:
                # Still processing
                still_processing.append(batch_id)
                status = result.get('status', 'unknown') if result else 'unknown'
                print(f"⏳ Batch {batch_id} still processing (status: {status})")
                if result and 'hours_remaining' in result:
                    hours = result['hours_remaining']
                    print(f"  Estimated time remaining: {hours:.1f} hours")
        
        # Update tracking lists
        submitted_batches = [b for b in submitted_batches if b not in newly_completed]
        
        # Summary
        if completed_batches:
            print(f"\nProgress: {len(completed_batches)}/{len(completed_batches) + len(submitted_batches)} batches completed")
        
        # Exit if all done
        if not submitted_batches:
            break
            
        # Wait before checking again
        print(f"\nWaiting {check_interval} seconds before checking again...")
        time.sleep(check_interval)
    
    # Final report
    print(f"\n✅ All {len(completed_batches)} batches completed!")
    print(f"Total records processed: {num_batches * batch_size}")
    logging.info(f"All {len(completed_batches)} batches completed")
    
    return completed_batches

def recover_failed_records(batch_id: str = None, debug_dir: str = None, debug_mode=True, debug_limit=20):
    """
    Recover records that failed processing due to permission issues
    
    This function looks for debug response files and identifies records that have valid JSON
    responses but might have failed during database update due to permission issues.
    It focuses on records that have technical content but NULL skills.
    
    Args:
        batch_id: Optional batch ID to restrict to responses from a specific batch
        debug_dir: Directory containing debug response files (defaults to current directory)
        debug_mode: Whether to generate debug files
        debug_limit: Maximum number of debug files to generate per batch
    
    Returns:
        Number of records successfully recovered
    """
    if debug_dir is None:
        debug_dir = os.path.dirname(__file__)
    
    logging.info(f"Starting recovery process for failed records")
    
    # Get list of debug response files
    debug_file_pattern = "debug_response_*.json"
    if batch_id:
        # If we have a batch ID, we'd need a way to filter files by batch
        # For now we'll process all debug files
        pass
    
    debug_files = glob.glob(os.path.join(debug_dir, debug_file_pattern))
    logging.info(f"Found {len(debug_files)} debug response files to analyze")
    
    recovered_count = 0
    failed_count = 0
    
    for debug_file in debug_files:
        try:
            # Extract user ID from filename
            filename = os.path.basename(debug_file)
            if filename.startswith("debug_response_") and filename.endswith(".json"):
                user_id_str = filename[len("debug_response_"):-len(".json")]
                
                # Check if it's a numeric user ID
                if user_id_str.isdigit():
                    userid = int(user_id_str)
                    
                    # Load the debug response file
                    with open(debug_file, 'r', encoding='utf-8') as f:
                        content = f.read()
                    
                    # Check if it's a valid JSON response
                    if content and content.strip().startswith('{') and content.strip().endswith('}'):
                        # Parse the response with debug parameters
                        import threading
                        if not hasattr(recover_failed_records, 'debug_counter'):
                            recover_failed_records.debug_counter = threading.local()
                            recover_failed_records.debug_counter.value = 0
                            
                        parsed_results = parse_unified_response(
                            content, 
                            debug_mode=debug_mode, 
                            debug_limit=debug_limit, 
                            debug_counter=recover_failed_records.debug_counter
                        )
                        
                        # Check if it has technical content but NULL skills
                        has_technical_content = False
                        has_null_skills = True
                        
                        # Check for technical indicators
                        tech_indicators = [
                            "Software", "Developer", "Engineer", "DevOps", "AWS", "Azure", 
                            "Cloud", "Docker", "Kubernetes", "Python", "Java", "C#", "JavaScript",
                            "SQL", "Database", "Programming", "Coding", "API", "Backend", "Frontend"
                        ]
                        
                        # Check summary and other fields for technical indicators
                        if "Summary" in parsed_results and parsed_results["Summary"] and parsed_results["Summary"] != "NULL":
                            summary = parsed_results["Summary"]
                            if any(tech in summary for tech in tech_indicators):
                                has_technical_content = True
                        
                        # Also check job titles
                        for title_field in ["PrimaryTitle", "SecondaryTitle", "TertiaryTitle"]:
                            if title_field in parsed_results and parsed_results[title_field] and parsed_results[title_field] != "NULL":
                                title = parsed_results[title_field]
                                if any(tech in title for tech in tech_indicators):
                                    has_technical_content = True
                        
                        # Check if skills are NULL
                        if "Top10Skills" in parsed_results and parsed_results["Top10Skills"] and parsed_results["Top10Skills"] != "NULL":
                            has_null_skills = False
                        
                        # If it has technical content but NULL skills, reprocess it
                        if has_technical_content and has_null_skills:
                            logging.info(f"Found technical record with NULL skills: UserID {userid}")
                            
                            # Apply enhanced date processing
                            enhanced_results = process_resume_with_enhanced_dates(userid, parsed_results)
                            
                            # Create a mapping from userid to results for database update
                            results_to_update = {userid: enhanced_results}
                            
                            # Update database
                            update_status = update_database_with_results(results_to_update)
                            
                            if update_status.get(userid, False):
                                recovered_count += 1
                                logging.info(f"Successfully recovered record for UserID {userid}")
                            else:
                                failed_count += 1
                                logging.error(f"Failed to recover record for UserID {userid}")
        except Exception as e:
            logging.error(f"Error processing debug file {debug_file}: {str(e)}")
    
    logging.info(f"Recovery completed: Recovered {recovered_count} records, Failed {failed_count} records")
    return recovered_count

def monitor_all_batches(check_interval: int = 30):
    """
    Monitor all pending batch jobs continuously

    Args:
        check_interval: Seconds between status checks
    """
    logging.info("Starting continuous batch monitoring mode")
    pending_batches = {}

    try:
        while True:
            # Get list of all batches from OpenAI
            try:
                all_batches = openai.batches.list(limit=100)

                # Track active batches
                active_count = 0
                for batch in all_batches.data:
                    batch_id = batch.id
                    status = batch.status

                    # Check if this is a batch we care about (in_progress, queued, or validating)
                    if status in ['in_progress', 'queued', 'validating']:
                        active_count += 1

                        if batch_id not in pending_batches:
                            logging.info(f"Found pending batch: {batch_id} (status: {status})")
                            pending_batches[batch_id] = status
                        elif pending_batches[batch_id] != status:
                            logging.info(f"Batch {batch_id} status changed: {pending_batches[batch_id]} -> {status}")
                            pending_batches[batch_id] = status

                    # Check if a previously pending batch has completed
                    elif status == 'completed' and batch_id in pending_batches:
                        logging.info(f"Batch {batch_id} has COMPLETED! Processing results...")

                        # Process the completed batch
                        result = check_and_process_batch(batch_id)
                        if result and result['status'] == 'completed':
                            logging.info(f"Successfully processed batch {batch_id}")
                            logging.info(f"Updated UserIDs: {result.get('successful_userids', [])}")
                            if result.get('failed_userids'):
                                logging.info(f"Failed UserIDs: {result['failed_userids']}")

                        # Remove from pending list
                        del pending_batches[batch_id]

                    # Check for failed batches
                    elif status == 'failed' and batch_id in pending_batches:
                        logging.error(f"Batch {batch_id} FAILED!")
                        del pending_batches[batch_id]

                # Status summary
                if active_count > 0:
                    logging.info(f"Currently monitoring {active_count} active batch(es)")
                else:
                    logging.info("No active batches found. Waiting for new batches...")

                # Wait before next check
                time.sleep(check_interval)

            except Exception as e:
                logging.error(f"Error checking batch status: {str(e)}")
                time.sleep(check_interval)

    except KeyboardInterrupt:
        logging.info("Batch monitoring stopped by user")

def run_continuous_processing(batch_size: int, num_batches: int = 1, check_interval: int = 20, debug_mode=True, debug_limit=20):
    """
    Run continuous batch processing without manual intervention
    
    Args:
        batch_size: Number of records to process in each batch
        num_batches: Number of batches to process in total
        check_interval: How often to check for batch completion in seconds (default: 1 hour)
        debug_mode: Whether to generate debug files
        debug_limit: Maximum number of debug files to generate per batch
    """
    logging.info(f"Starting continuous processing with batch size {batch_size}, {num_batches} batches")
    
    # Track batches we've submitted
    submitted_batches = []
    completed_batches = []
    batches_to_submit = num_batches
    
    # First, submit the initial batch
    if batches_to_submit > 0:
        result = run_unified_processing(
            batch_size=batch_size,
            debug_mode=debug_mode,
            debug_limit=debug_limit
        )
        if result:
            batch_id = result['openai_batch_id']
            submitted_batches.append(batch_id)
            logging.info(f"Submitted initial batch: {batch_id}")
            print(f"Submitted batch {len(submitted_batches)}/{num_batches} with ID: {batch_id}")
            batches_to_submit -= 1
    
    # Enter the main loop
    while submitted_batches or batches_to_submit > 0:
        # Submit any new batches if we have capacity
        while batches_to_submit > 0:
            # Don't submit too many at once, leave some headroom
            if len(submitted_batches) < 5:  # Limit concurrent batches to 5
                result = run_unified_processing(
                    batch_size=batch_size,
                    debug_mode=debug_mode,
                    debug_limit=debug_limit
                )
                if result:
                    batch_id = result['openai_batch_id']
                    submitted_batches.append(batch_id)
                    logging.info(f"Submitted new batch: {batch_id}")
                    print(f"Submitted batch {len(submitted_batches) + len(completed_batches)}/{num_batches} with ID: {batch_id}")
                    batches_to_submit -= 1
                    # Sleep briefly to avoid rate limits
                    time.sleep(10)
                else:
                    logging.warning("Failed to submit new batch, will retry")
                    time.sleep(60)  # Wait a minute before retrying
            else:
                # We have 5+ batches in flight, wait for some to complete
                break
        
        # Check status of all submitted batches
        still_processing = []
        for batch_id in submitted_batches:
            logging.info(f"Checking batch {batch_id}")
            result = check_and_process_batch(
                batch_id,
                debug_mode=debug_mode,
                debug_limit=debug_limit
            )
            
            if result and result['status'] == 'completed':
                logging.info(f"Batch {batch_id} completed successfully")
                print(f"Batch {batch_id} completed")
                print(f"Processed {result['total_records']} records")
                print(f"Success: {result['success_count']}, Failure: {result['failure_count']}")
                
                # Move to completed list
                completed_batches.append(batch_id)
            elif result and result['status'] == 'failed':
                logging.error(f"Batch {batch_id} failed: {result.get('message', 'Unknown error')}")
                print(f"Batch {batch_id} failed: {result.get('message', 'Unknown error')}")
                
                # Still consider it completed for our purposes
                completed_batches.append(batch_id)
            else:
                # Batch is still processing
                still_processing.append(batch_id)
                if result and 'hours_remaining' in result:
                    logging.info(f"Batch {batch_id} still processing, ~{result['hours_remaining']} hours remaining")
                    print(f"Batch {batch_id} still processing, ~{result['hours_remaining']} hours remaining")
                else:
                    logging.info(f"Batch {batch_id} still processing")
                    print(f"Batch {batch_id} status: {result['status'] if result else 'unknown'}")
        
        # Update our tracking list
        submitted_batches = still_processing
        
        # If we're done, break out of the loop
        if not submitted_batches and batches_to_submit == 0:
            break
            
        # Wait before checking again
        if submitted_batches:
            logging.info(f"Waiting {check_interval} seconds before checking batches again")
            print(f"Waiting {check_interval/60:.1f} minutes before checking {len(submitted_batches)} active batch(es) again")
            print(f"Progress: {len(completed_batches)}/{num_batches} batches completed")
            time.sleep(check_interval)
    
    # Final report
    logging.info(f"All {len(completed_batches)} batches completed")
    print(f"All {len(completed_batches)} batches completed")
    return completed_batches

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Unified batch processor for resume analysis')
    parser.add_argument('--submit', action='store_true', help='Submit a new unified batch job')
    parser.add_argument('--check-batch', type=str, help='Check a specific batch job by ID')
    parser.add_argument('--batch-size', type=int, default=BATCH_SIZE, help=f'Batch size (default: {BATCH_SIZE})')
    parser.add_argument('--continuous', action='store_true', help='Run in continuous mode to process batches without manual intervention')
    parser.add_argument('--num-batches', type=int, default=1, help='Number of batches to process in continuous mode (default: 1)')
    parser.add_argument('--check-interval', type=int, default=20, help='Seconds between status checks in continuous mode (default: 20)')
    parser.add_argument('--debug-mode', action='store_true', help='Enable debug file generation (default: True)')
    parser.add_argument('--no-debug-mode', action='store_true', help='Disable debug file generation')
    parser.add_argument('--debug-limit', type=int, default=20, help='Maximum number of debug files to generate per batch (default: 20)')
    parser.add_argument('--multi-batch', action='store_true', help='Run multiple batches in parallel')
    parser.add_argument('--batch-delay', type=int, default=10, help='Seconds to wait between launching batches (default: 10)')
    parser.add_argument('--recover', action='store_true', help='Recover failed records by reprocessing debug files')
    parser.add_argument('--recover-batch', type=str, help='Recover failed records from a specific batch ID')
    parser.add_argument('--tech-focus', action='store_true', help='Focus recovery on technical records with NULL skills')
    
    args = parser.parse_args()
    
    # Determine debug mode setting
    debug_mode = True  # Default is True
    if args.no_debug_mode:
        debug_mode = False
    elif args.debug_mode:
        debug_mode = True
        
    if args.recover or args.recover_batch:
        # Run the recovery process
        batch_id = args.recover_batch if args.recover_batch else None
        recovered = recover_failed_records(
            batch_id=batch_id,
            debug_mode=debug_mode,
            debug_limit=args.debug_limit
        )
        print(f"Recovery process completed. Recovered {recovered} records.")
        
    elif args.multi_batch:
        # Run multiple batches in parallel with automatic processing
        if args.batch_size != BATCH_SIZE:
            BATCH_SIZE = args.batch_size
            logging.info(f"Using custom batch size: {BATCH_SIZE}")
            
        run_parallel_processing(
            batch_size=BATCH_SIZE,
            num_batches=args.num_batches,
            batch_delay=args.batch_delay,
            check_interval=args.check_interval,
            debug_mode=debug_mode,
            debug_limit=args.debug_limit
        )
        
    elif args.continuous:
        # Run in continuous mode
        if args.batch_size != BATCH_SIZE:
            BATCH_SIZE = args.batch_size
            logging.info(f"Using custom batch size: {BATCH_SIZE}")
        
        run_continuous_processing(
            batch_size=BATCH_SIZE, 
            num_batches=args.num_batches, 
            check_interval=args.check_interval,
            debug_mode=debug_mode,
            debug_limit=args.debug_limit
        )
    elif args.submit:
        # Override batch size if provided
        if args.batch_size != BATCH_SIZE:
            BATCH_SIZE = args.batch_size
            logging.info(f"Using custom batch size: {BATCH_SIZE}")
        
        # Submit a new unified batch job
        result = run_unified_processing(
            batch_size=BATCH_SIZE,
            debug_mode=debug_mode,
            debug_limit=args.debug_limit
        )
        if result:
            print(f"Submitted unified batch job with OpenAI batch ID: {result['openai_batch_id']}")
            print(f"Number of records: {result['request_count']}")
            print(f"Check back after: {result['next_check_time'].strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"Use this command to check status: python one_step_processor.py --check-batch {result['openai_batch_id']}")
    
    elif args.check_batch:
        # Check a specific batch job
        result = check_and_process_batch(
            args.check_batch,
            debug_mode=debug_mode,
            debug_limit=args.debug_limit
        )
        if result:
            if result['status'] == 'completed':
                print(f"Batch job {args.check_batch} completed")
                print(f"Processed {result['total_records']} records")
                print(f"Success: {result['success_count']}, Failure: {result['failure_count']}")
                print(f"Estimated cost: ${result['cost_estimates']['total_cost']:.4f}")
                print(f"Saved ${result['cost_estimates']['savings']:.4f} compared to standard API")
                print(f"Cost per record: ${result['cost_estimates']['cost_per_record']:.6f}")
                print(f"\nIf some records have missing fields, you can run recovery with:")
                print(f"python one_step_processor.py --recover-batch {args.check_batch}")
            elif result['status'] == 'failed':
                print(f"Batch job {args.check_batch} failed: {result['message']}")
            else:
                print(f"Batch job {args.check_batch} status: {result['status']}")
                if 'hours_remaining' in result:
                    print(f"Estimated hours remaining: {result['hours_remaining']}")
    
    else:
        parser.print_help()