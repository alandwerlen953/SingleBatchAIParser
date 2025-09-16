#!/usr/bin/env python3
"""Test script to debug degree extraction issues"""

import logging
import sys
from two_step_prompts_taxonomy import create_step1_prompt
from two_step_processor_taxonomy import parse_step1_response
from resume_utils import openai, DEFAULT_MODEL, DEFAULT_TEMPERATURE, MAX_TOKENS

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(message)s')

# Sample resume text with clear education information
test_resume = """
John Smith
Email: john.smith@email.com
Phone: 555-123-4567

EDUCATION
Bachelor of Science in Computer Science
University of California, Berkeley
Graduated: May 2018

Master of Science in Data Science
Stanford University
Graduated: June 2020

CERTIFICATIONS
AWS Certified Solutions Architect
Google Cloud Professional Data Engineer

WORK EXPERIENCE
Senior Software Engineer
Tech Company Inc. - San Francisco, CA
June 2020 - Present
- Developed scalable microservices using Python and Kubernetes
- Led team of 5 engineers on cloud migration project
- Implemented CI/CD pipelines using Jenkins and Docker

Software Engineer
Startup Corp - Palo Alto, CA
July 2018 - May 2020
- Built RESTful APIs using Python Flask
- Worked with PostgreSQL and Redis databases
- Developed front-end components using React

SKILLS
Python, Java, JavaScript, SQL, Docker, Kubernetes, AWS, React, Node.js, PostgreSQL
"""

def test_step1_extraction():
    """Test Step 1 extraction to see if degrees are captured"""

    logging.info("="*60)
    logging.info("TESTING DEGREE EXTRACTION FROM STEP 1")
    logging.info("="*60)

    # Create the prompt
    step1_messages = create_step1_prompt(test_resume, userid="TEST001")

    # Print the specific prompt parts related to education
    logging.info("\n--- EDUCATION-RELATED PROMPT INSTRUCTIONS ---")
    for msg in step1_messages:
        if "Bachelor" in msg["content"] or "Master" in msg["content"]:
            relevant_lines = [line for line in msg["content"].split('\n')
                            if "Bachelor" in line or "Master" in line]
            for line in relevant_lines:
                logging.info(f"  {line.strip()}")

    # Call OpenAI API
    logging.info("\n--- CALLING OPENAI API ---")
    try:
        response = openai.chat.completions.create(
            model=DEFAULT_MODEL,
            messages=step1_messages,
            temperature=DEFAULT_TEMPERATURE,
            max_tokens=MAX_TOKENS
        )

        if response and response.choices:
            response_text = response.choices[0].message.content

            # Print the raw response
            logging.info("\n--- RAW API RESPONSE ---")
            # Find and print only the education-related lines
            for line in response_text.split('\n'):
                if "Bachelor" in line or "Master" in line or "Degree" in line:
                    logging.info(f"  {line}")

            # Parse the response
            parsed_results = parse_step1_response(response_text)

            # Check what was extracted
            logging.info("\n--- PARSED RESULTS ---")
            education_fields = ["Bachelors", "Masters", "Bachelor's Degree", "Master's Degree"]
            for field in education_fields:
                if field in parsed_results:
                    logging.info(f"  {field}: {parsed_results[field]}")

            # Also check if the fields exist in the raw extraction
            from two_step_processor_taxonomy import extract_fields_directly
            direct_fields = extract_fields_directly(response_text)

            logging.info("\n--- DIRECT EXTRACTION RESULTS ---")
            for field in ["Bachelors", "Masters"]:
                if field in direct_fields:
                    logging.info(f"  {field}: {direct_fields[field]}")
                else:
                    logging.info(f"  {field}: NOT FOUND")

            # Check what the final mapped fields are
            logging.info("\n--- FINAL MAPPED FIELDS ---")
            final_fields = {}
            for key, value in parsed_results.items():
                if "bachelor" in key.lower() or "master" in key.lower():
                    final_fields[key] = value

            if final_fields:
                for field, value in final_fields.items():
                    logging.info(f"  {field}: {value}")
            else:
                logging.info("  No degree fields found in final mapping!")

            # Print the full response for debugging
            logging.info("\n--- FULL RAW RESPONSE (first 2000 chars) ---")
            logging.info(response_text[:2000])

        else:
            logging.error("No response from OpenAI API")

    except Exception as e:
        logging.error(f"Error calling OpenAI API: {str(e)}")
        return

    logging.info("\n" + "="*60)
    logging.info("TEST COMPLETE")
    logging.info("="*60)

if __name__ == "__main__":
    test_step1_extraction()