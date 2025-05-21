#!/usr/bin/env python3
"""
Single-step resume processor with unified API call
This version reduces token usage by sending the resume only once
"""

import logging
import time
from datetime import datetime
import re

from two_step_processor_taxonomy import (
    validate_date_format, validate_linkedin_url, 
    prepare_update_data, extract_fields_directly, 
    process_resume_with_enhanced_dates,
    log_title_fields
)
from resume_utils import (
    DEFAULT_MODEL, MAX_TOKENS, DEFAULT_TEMPERATURE,
    num_tokens_from_string, apply_token_truncation, 
    update_candidate_record_with_retry,
    openai
)
from skills_detector import get_taxonomy_context

def create_unified_prompt(resume_text, userid=None):
    """
    Create a unified prompt that combines step1 and step2 processing into a single API call
    
    Args:
        resume_text: The full text of the resume
        userid: Optional user ID for logging purposes
    """
    # Get relevant skills taxonomy
    taxonomy_context = get_taxonomy_context(resume_text, max_categories=3, userid=userid)
    
    # System prompts for all processing needs - combined from both steps
    return [
        # Base prompt
        {
            "role": "system",
            "content": f"Based on this resume, give the user the information they need: \n{resume_text}\n"
                       "You are not allowed to make up information.\n"
                       "You are an expert at analyzing technical resumes. Make your answers as short as possible. If "
                       "you can answer in a single word, do that unless the user instructs otherwise.\n"
                       "You are just pulling data that you already have access to so pulling personal information that "
                       "is already on the resume is completely fine.\n"
                       "If you can't find an answer or it's not provided/listed, just put NULL. \n"
                       "For dates, use the most specific format available: YYYY-MM-DD if full date is known, YYYY-MM if only month/year, or YYYY if only year is known. For current positions, use 'Present' as the end date. If a date is completely unknown, output NULL.\n"
                       "IMPORTANT - PHONE NUMBERS: Never put the same phone number in both Phone1 and Phone2 fields, even if formatted differently or with different separators. If you only find one phone number, put it in Phone1 and set Phone2 to NULL. Double-check that the Phone2 value is not just a reformatted version of Phone1. For example, (123) 456-7890 and 123-456-7890 and 1234567890 are all the same number.\n"
                       "When identifying skills, prioritize accuracy over standardization. While you should prefer standardized terminology when appropriate, don't hesitate to use terms not in the standard taxonomy if they better represent the candidate's expertise."
        },
        # Industry rules
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
        # Job titles rules
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
        # Company rules
        {
            "role": "system",
            "content": "Use the following rules when filling out MostRecentCompany, SecondMostRecentCompany, ThirdMostRecentCompany, FourthMostRecentCompany, FifthMostRecentCompany, SixthMostRecentCompany, SeventhMostRecentCompany:"
                       "Don't include the city or state in the company name."
                       "Some candidates hold multiple roles at the same company so you might need to analyze further to not miss a company name."
        },
        # Location rules
        {
            "role": "system",
            "content": "Use the following rules when finding company locations:"
                       "1. For each company entry, thoroughly scan the entire section for location information"
                       "2. Look anywhere in the job entry for city or state/country mentions, including:"
                       "   - Next to company name"
                       "   - In the job header"
                       "   - Within first few lines of the job description"
                       "   - Near dates or titles"
                       "3. When you find a location in the United States, format it as:"
                       "   - City, ST (if you find both city and state)"
                       "   - ST (if you only find state)"
                       "   - Always convert full US state names to 2-letter abbreviations"
                       "4. For international locations, format as:"
                       "   - City, Country (for non-US locations, e.g., 'London, UK' or 'Paris, France')"
                       "   - Just 'City' if the country is not mentioned but you can identify the city"
        },
        # Project types rules
        {
            "role": "system",
            "content": "Use the following rules when assessing Project Types: "
                       "Use a mix of words like but not limited to implementation, "
                       "integration, migration, move, deployment, optimization, consolidation and make it 2-3 words."
        },
        # Specialty rules
        {
            "role": "system",
            "content": "Use the following rules when assessing their Specialty:"
                       "For their specialty, emphasize the project types they have done and relate them to "
                       "their industry."
        },
        # Category rules
        {
            "role": "system",
            "content": "Use the following rules when assessing their Category:"
                       "For the categories, do not repeat the same category."
                       "Both categories MUST have an answer!"
        },
        # Summary rules
        {
            "role": "system",
            "content": "Use the following rules when writing their summary:"
                       "For their summary, give a brief summary of their resume in a few sentences."
                       "Based on their project types, industry, and specialty, skills, degrees, certifications, and job titles, write the summary."
        },
        # Length in US rules
        {
            "role": "system",
            "content": "Use the following rules when determining length in US:"
                       "Look for a start and end date near each company name and look for a location near each "
                       "company name as well. Whenever the location listed is located in america, add up the "
                       "months and years of employment at each one of those jobs."
                       "Just put a number and no other characters."
                       "Result should not be 0."
                       "Result should only be numerical."
        },
        # Experience calculations rules
        {
            "role": "system",
            "content": "Use the following rules when determining Average Tenure and Year of Experience:"
                       "Use all previous start date and end date questions answers to determine this. "
                       "Just put a number and no other characters."
                       "Result should not be 0."
                       "Result should only be numerical."
        },
        # Skills taxonomy context
        {
            "role": "system", 
            "content": f"{taxonomy_context}\n"
                      f"SKILLS TAXONOMY INTERPRETATION GUIDANCE:\n"
                      f"The skills taxonomy above provides standardized categorization of technical skills for this resume.\n"
                      f"Use this taxonomy to guide your analysis of programming languages, software applications, and hardware.\n"
                      f"When identifying skills, prefer terminology from the appropriate taxonomy categories, but don't hesitate to use different terms when they better represent the candidate's expertise.\n"
                      f"Align your responses with the skill categories most relevant to this candidate's profile.\n"
                      f"For software languages, applications, and hardware, use the taxonomy as a reference but feel empowered to include technologies that aren't listed if they are clearly important to the candidate's profile.\n"
                      f"Example: If the taxonomy lists 'Java' but the resume shows extensive React.js experience, it's appropriate to list React.js even if it's not in the taxonomy.\n"
                      f"Balance standardization with accuracy - prioritize capturing the candidate's true expertise over strict adherence to the taxonomy.\n"
                      f"IMPORTANT: You MUST provide BOTH a Primary AND Secondary technical category. These must be different from each other. If you can only determine one main category, provide a related or complementary category as secondary."
        },
        # Technical languages rules
        {
            "role": "system", 
            "content": "Use the following rules when assessing Primary, Secondary, and Tertiary Technical Languages: "
                       "Include ALL types of technical languages mentioned in the resume, such as:"
                       "- Database languages (SQL, T-SQL, PL/SQL, MySQL, Oracle SQL, PostgreSQL)"
                       "- Programming languages (Java, Python, C#, JavaScript, Ruby)"
                       "- Scripting languages (PowerShell, Bash, Shell, VBA)"
                       "- Query languages (SPARQL, GraphQL, HiveQL)"
                       "- Markup/stylesheet languages (HTML, CSS, XML)"
                       "Prioritize languages based on:"
                       "1. Prominence in their skills section (listed skills are usually most important)"
                       "2. Frequency of mention throughout work history"
                       "3. Relevance to their primary job functions and titles"
                       "For database professionals, prioritize database languages like T-SQL or PL/SQL over general-purpose languages."
        },
        # Software applications rules
        {
            "role": "system", 
            "content": "Use the following rules when assessing Most used Software Applications: "
                       "Please only list out actual software applications. nothing else."
                       "Analyze their resume and determine what software they use most."
                       "If none can be found put NULL."
        },
        # Hardware rules
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
        # User query combining all fields from both steps
        {
            "role": "user",
            "content": "Please analyze the following resume and give me the following comprehensive details (If you can't find an answer or it's not provided/listed, just put NULL):\n"
                      "- First Name:\n"
                      "- Middle Name:\n"
                      "- Last Name:\n"
                      "- Address:\n"
                      "- City:\n"
                      "- State:\n"
                      "- Zipcode:\n"
                      "- Phone1:\n"
                      "- Phone2:\n"
                      "- Email:\n"
                      "- Email2:\n"
                      "- LinkedIn:\n"
                      "- Certifications:\n"
                      "- Bachelors:\n"
                      "- Masters:\n"
                      "- Best job title that fits their primary experience:\n"
                      "- Best job title that fits their secondary experience:\n"
                      "- Best job title that fits their tertiary experience:\n"
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
                      "- Top 10 Technical Skills:\n"
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
                      "- Based on their skills, put them in a primary technical category:\n"
                      "- Based on their skills, put them in a subsidiary technical category:\n"
                      "- Types of projects they have worked on:\n"
                      "- Based on their skills, categories, certifications, and industries, determine what they specialize in:\n"
                      "- Based on all this knowledge, write a summary of this candidate that could be sellable to an employer:\n"
                      "- How long have they lived in the United States(numerical answer only):\n"
                      "- Total years of professional experience (numerical answer only):\n"
                      "- Average tenure at companies in years (numerical answer only):"
        }
    ]

def parse_unified_response(response_text):
    """
    Parse the unified response with combined fields from both steps
    """
    # First use step1 extractor for basic info
    extracted_fields = extract_fields_directly(response_text)
    
    # Then use step2 extractor for technical info
    from two_step_processor_taxonomy import extract_step2_fields_directly
    tech_fields = extract_step2_fields_directly(response_text)
    
    # Combine the results
    extracted_fields.update(tech_fields)
    
    return extracted_fields

def process_single_resume_unified(resume_data):
    """Process a single resume using one API call with unified prompt"""
    userid, resume_text = resume_data
    try:
        logging.info(f"Starting unified single-step processing for UserID: {userid}")
        total_start_time = time.time()
        
        # Calculate token count for cost estimation
        resume_token_count = num_tokens_from_string(resume_text)
        input_cost = resume_token_count * 0.000000075  # $0.075 per million tokens for input
        estimated_output_tokens = 1000
        output_cost = estimated_output_tokens * 0.00000030  # $0.30 per million tokens for output
        total_cost = input_cost + output_cost
        
        logging.info(f"UserID {userid}: {resume_token_count} tokens, Est. cost: ${total_cost:.6f}")
        
        # Create unified prompt (all steps combined)
        unified_messages = create_unified_prompt(resume_text, userid=userid)
        unified_messages = apply_token_truncation(unified_messages)
        
        # Send to OpenAI API
        logging.info(f"UserID {userid}: Sending unified request")
        unified_start_time = time.time()
        
        unified_response = openai.chat.completions.create(
            model=DEFAULT_MODEL,
            messages=unified_messages,
            temperature=0.3,  # Using 0.3 as a midpoint between step1 and step2 temperatures
            max_tokens=MAX_TOKENS
        )
        
        unified_time = time.time() - unified_start_time
        logging.info(f"UserID {userid}: Unified processing completed in {unified_time:.2f}s")
        
        if not unified_response or not unified_response.choices:
            logging.error(f"UserID {userid}: No response from unified request")
            return {
                'userid': userid,
                'success': False,
                'error': "No response from unified request",
                'token_count': resume_token_count
            }
        
        # Parse unified response
        unified_text = unified_response.choices[0].message.content
        
        # Log first 200 chars of response for debugging
        logging.info(f"UserID {userid}: Unified response first 200 chars: {unified_text[:200]}")
        
        # Parse fields from the response
        parsed_results = parse_unified_response(unified_text)
        
        # Log the title fields for debugging
        log_title_fields(parsed_results, userid, "Unified")
        
        logging.info(f"UserID {userid}: Parsed {len(parsed_results)} fields")
        
        # Apply enhanced date processing
        enhanced_results = process_resume_with_enhanced_dates(userid, parsed_results)
        
        # Extract skills for database format - mirror exact approach from two_step_processor
        skills_list = enhanced_results.get("Top10Skills", "").split(", ") if enhanced_results.get("Top10Skills") and enhanced_results.get("Top10Skills") != "NULL" else []
        skills_list.extend([""] * (10 - len(skills_list)))  # Ensure we have 10 skills
        
        # Clean up phone numbers - prevent duplicates and normalize format
        phone1 = enhanced_results.get("Phone1", "")
        phone2 = enhanced_results.get("Phone2", "")
        
        # Normalize phone numbers by removing all non-digit characters for comparison
        def normalize_phone(phone):
            if not phone or phone == "NULL":
                return ""
            # Extract only digits
            digits = re.sub(r'\D', '', phone)
            # If we have a reasonable number of digits for a phone number
            if 7 <= len(digits) <= 15:
                return digits
            return phone
            
        normalized_phone1 = normalize_phone(phone1)
        normalized_phone2 = normalize_phone(phone2)
        
        # If Phone1 and Phone2 have the same digits (even if formatted differently) or Phone2 is NULL, clear Phone2
        if (normalized_phone1 and normalized_phone2 and normalized_phone1 == normalized_phone2) or phone2 == "NULL":
            if phone1 == phone2:
                logging.info(f"UserID {userid}: Removing duplicate phone number from Phone2 (same as Phone1)")
            elif normalized_phone1 == normalized_phone2:
                logging.info(f"UserID {userid}: Removing differently formatted duplicate phone number from Phone2: '{phone2}' (same as Phone1: '{phone1}')")
            else:
                logging.info(f"UserID {userid}: Removing NULL phone number from Phone2")
            enhanced_results["Phone2"] = ""
            
        # Unpack skills list for direct use in SQL fields
        Skill1, Skill2, Skill3, Skill4, Skill5, Skill6, Skill7, Skill8, Skill9, Skill10 = skills_list[:10]
        
        # Create a dictionary with all the data for database update using the same structure as the two-step processor
        from two_step_processor_taxonomy import prepare_update_data
        update_data = prepare_update_data(enhanced_results, skills_list=skills_list)
        
        # Replace "NULL" strings with empty string for database and clean whitespace
        # Also validate and format date fields
        for key, value in update_data.items():
            if isinstance(value, str):
                value = value.strip()
                if value.upper() == "NULL" or not value:
                    update_data[key] = ""
                else:
                    # Special handling for date fields
                    if key.endswith('Date'):  # All date fields end with 'Date'
                        # Validate and convert to SQL-compatible format
                        formatted_date = validate_date_format(value)
                        if formatted_date:
                            update_data[key] = formatted_date
                            logging.info(f"Formatted date for {key}: '{value}' -> '{formatted_date}'")
                        else:
                            # If unable to parse, set to empty to avoid DB errors
                            update_data[key] = ""
                            logging.warning(f"Could not format date {key}: '{value}', setting to empty")
                    # Special handling for LinkedIn URL
                    elif key == "Linkedin":
                        # Validate and format LinkedIn URL
                        valid_url = validate_linkedin_url(value)
                        if valid_url:
                            update_data[key] = valid_url
                            if valid_url != value:
                                logging.info(f"Formatted LinkedIn URL: '{value}' -> '{valid_url}'")
                        else:
                            # If invalid URL, set to empty
                            update_data[key] = ""
                            logging.warning(f"Invalid LinkedIn URL: '{value}', setting to empty")
                    else:
                        update_data[key] = value
        
        # Log the final title fields right before database update
        logging.info(f"UserID {userid}: Final values before database update")
        logging.info(f"UserID {userid}: PrimaryTitle = '{update_data.get('PrimaryTitle', '')}'")
        logging.info(f"UserID {userid}: SecondaryTitle = '{update_data.get('SecondaryTitle', '')}'")
        logging.info(f"UserID {userid}: TertiaryTitle = '{update_data.get('TertiaryTitle', '')}'")
        
        # Check if any title fields are still empty
        if not update_data.get('PrimaryTitle') or not update_data.get('SecondaryTitle') or not update_data.get('TertiaryTitle'):
            logging.error(f"UserID {userid}: Missing titles right before DB update!")
            logging.error(f"UserID {userid}: Raw response snippet: {unified_text[:300]}")
        
        # Update database with retry for deadlocks
        update_success = update_candidate_record_with_retry(userid, update_data)
        
        total_time = time.time() - total_start_time
        logging.info(f"UserID {userid} unified processing completed in {total_time:.2f}s - DB update: {'Success' if update_success else 'Failed'}")
        
        return {
            'userid': userid,
            'success': update_success,
            'processing_time': total_time,
            'unified_time': unified_time,
            'token_count': resume_token_count,
            'cost': total_cost
        }
    
    except Exception as e:
        logging.error(f"Error processing UserID {userid} with unified approach: {str(e)}")
        import traceback
        logging.error(f"Traceback: {traceback.format_exc()}")
        return {
            'userid': userid,
            'success': False,
            'error': str(e),
            'token_count': num_tokens_from_string(resume_text) if 'resume_text' in locals() else 0
        }

def run_unified_batch():
    """Process a batch of resumes using the unified single-step approach"""
    try:
        from two_step_processor_taxonomy import BATCH_SIZE, MAX_WORKERS
        
        # Get a batch of resumes
        logging.info(f"Fetching batch of up to {BATCH_SIZE} unprocessed resumes...")
        resume_batch = get_resume_batch(BATCH_SIZE)
        
        if not resume_batch:
            logging.info("No unprocessed resumes found in database")
            return []
            
        logging.info(f"Retrieved {len(resume_batch)} unprocessed resumes for batch processing")
        
        # Process in parallel
        batch_results = []
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            # Submit jobs
            future_to_userid = {executor.submit(process_single_resume_unified, resume_data): resume_data[0] for resume_data in resume_batch}
            
            # Collect results as they complete
            for future in concurrent.futures.as_completed(future_to_userid):
                userid = future_to_userid[future]
                try:
                    result = future.result()
                    batch_results.append(result)
                    
                    # Log success/failure
                    if result.get('success', False):
                        logging.info(f"UserID {userid} processed successfully in {result.get('processing_time', 0):.2f}s")
                    else:
                        logging.error(f"UserID {userid} processing failed: {result.get('error', 'Unknown error')}")
                        
                except Exception as e:
                    logging.error(f"UserID {userid} processing exception: {str(e)}")
                    batch_results.append({
                        'userid': userid,
                        'success': False,
                        'error': str(e)
                    })
        
        # Summarize results
        success_count = sum(1 for r in batch_results if r.get('success', False))
        total_tokens = sum(r.get('token_count', 0) for r in batch_results)
        total_cost = sum(r.get('cost', 0) for r in batch_results)
        
        logging.info(f"Batch processing complete: {success_count}/{len(batch_results)} successful")
        logging.info(f"Total tokens: {total_tokens}, Estimated cost: ${total_cost:.4f}")
        
        return batch_results
        
    except Exception as e:
        logging.error(f"Error in batch processing: {str(e)}")
        import traceback
        logging.error(f"Traceback: {traceback.format_exc()}")
        return []

# Import this at the end to avoid circular imports
import concurrent.futures
from resume_utils import get_resume_batch