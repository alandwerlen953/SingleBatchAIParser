#!/usr/bin/env python3
"""Test single resume processing to see actual AI response"""

import os
import sys
import logging

# Set up logging to see everything
logging.basicConfig(level=logging.DEBUG, format='%(message)s')

# Set quiet mode to reduce noise
os.environ['QUIET_MODE'] = '0'

from single_step_processor import create_unified_prompt, parse_unified_response
from resume_utils import openai, DEFAULT_MODEL, MAX_TOKENS, apply_token_truncation

# Stephen Quesada's resume text
resume_text = """Stephen Quesada
Steve.Quesada1122@Gmail.com | 561-293-5228

Professional Summary
Dedicated technology support professional with proven desktop, mobile, and network knowledge and
skills. troubleshooting experience with Excellent problem solving and customer service skills. Strong
communication skills - enabling users to be more comfortable and productive with their technology.

Skills
Project Management & Hardware Installation:
• Installation and take-down of hardware: Windows 7, 10/11, Server 2012/2016/2019, MAC OSX
• Experience with VMware for desktop, laptop, and server environments
• Installation and support for modems, routers, printers, and Wi-Fi equipment
• Asset Management: Tracking and deployment of desktop/laptop/servers, routers, switches,
printers, and modems

Experience
IT Support Specialist
Embraer Executive Jets ~ Temp Assignment Jan 2024 to Oct 2024

IT Deskside/AV/Network Technician
Stryker ~ Contractual December 2021 - March 2024

IT Support Engineer Specialist
The Boca Raton Hotel and Resort | Boca Raton, FL November 2019 - October 2021

Network Engineer
Humana Inc | Delray, FL    January 2018 - May 2019

Technical Engineer Specialist
Humana Inc | Delray, FL    November 2011 October 2017

Education
Associate of Arts Financial Accounting
Sante Fe Community College, Gainesville, FL

PC Professor, Boca Raton, FL
Cloud Services: MS Azure/ AWS/Server 2019 SEC+ Linux
- MCITP Certification Program Microsoft Certified    April 2023

MCITP/MCP/A+/N+ Certification Program Microsoft Certified - Engineering    May 2011

Certifications
• MS Azure -/ MCITP Certification Program/ Microsoft Certified Professional/ N+, A+, SEC+/"""

def test_resume():
    """Test the resume processing to see actual AI response"""

    print("="*60)
    print("TESTING STEPHEN QUESADA'S RESUME")
    print("="*60)

    # Create the unified prompt
    userid = "1264743"
    messages = create_unified_prompt(resume_text, userid=userid)
    messages = apply_token_truncation(messages)

    print("\nPROMPT BEING SENT:")
    print("-"*40)
    # Show what fields we're asking for
    for msg in messages:
        if "- First Name:" in msg["content"]:
            # Just show the fields being requested
            lines = msg["content"].split('\n')
            for line in lines:
                if line.strip().startswith("-"):
                    print(line)
            break

    print("\nCALLING OPENAI API...")
    print("-"*40)

    try:
        # Call OpenAI
        response = openai.chat.completions.create(
            model=DEFAULT_MODEL,
            messages=messages,
            temperature=0.3,
            max_tokens=MAX_TOKENS
        )

        if response and response.choices:
            response_text = response.choices[0].message.content

            print("\nRAW AI RESPONSE:")
            print("-"*40)
            print(response_text)

            print("\n" + "="*60)
            print("PARSED FIELDS:")
            print("-"*40)

            # Parse the response
            parsed = parse_unified_response(response_text)

            # Show what was extracted for key fields
            important_fields = [
                "FirstName", "LastName", "Email", "Phone1",
                "City", "State", "Certifications",
                "Bachelors", "Masters", "LinkedIn",
                "PrimaryTitle", "SecondaryTitle",
                "MostRecentCompany", "PrimaryIndustry"
            ]

            for field in important_fields:
                value = parsed.get(field, "NOT FOUND")
                if value and value != "NULL":
                    print(f"{field}: {value}")
                else:
                    print(f"{field}: *** MISSING/NULL ***")

        else:
            print("No response from OpenAI")

    except Exception as e:
        print(f"Error calling OpenAI: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_resume()