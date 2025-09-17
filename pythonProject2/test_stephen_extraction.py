#!/usr/bin/env python3
"""Test extraction for Stephen Quesada's resume"""

import re

# Sample test - what the AI might respond with
test_response = """
- First Name: Stephen
- Middle Name: NULL
- Last Name: Quesada
- Address: NULL
- City: NULL
- State: FL
- Zipcode: NULL
- Phone1: 561-293-5228
- Phone2: NULL
- Email: Steve.Quesada1122@Gmail.com
- Email2: Puma112009@Yahoo.com
- LinkedIn: NULL
- Certifications: MS Azure, MCITP, MCP, A+, N+, SEC+
- Bachelors: NULL
- Masters: NULL
- Best job title that fits their primary experience: IT Support Specialist
- Best job title that fits their secondary experience: Network Engineer
- Best job title that fits their tertiary experience: Technical Support Engineer
"""

# Test the regex patterns from extract_fields_directly
def test_extraction():
    print("Testing extraction patterns:")
    print("="*60)

    # Test Phone1 pattern
    phone_patterns = [
        r"- Phone1:\s*(.+)",
        r"Phone1:\s*(.+)",
        r"Their Phone Number:\s*(.+)",
        r"Phone Number 1:\s*(.+)"
    ]

    for pattern in phone_patterns:
        match = re.search(pattern, test_response)
        if match:
            print(f"Phone1 matched with pattern '{pattern}': {match.group(1)}")
            break

    # Test Email pattern
    email_patterns = [
        r"- Email:\s*(.+)",
        r"Email:\s*(.+)",
        r"Their Email:\s*(.+)",
        r"Email 1:\s*(.+)"
    ]

    for pattern in email_patterns:
        match = re.search(pattern, test_response)
        if match:
            print(f"Email matched with pattern '{pattern}': {match.group(1)}")
            break

    # Test FirstName pattern
    firstname_patterns = [
        r"- First Name:\s*(.+)",
        r"First Name:\s*(.+)",
        r"Their First Name:\s*(.+)"
    ]

    for pattern in firstname_patterns:
        match = re.search(pattern, test_response)
        if match:
            print(f"FirstName matched with pattern '{pattern}': {match.group(1)}")
            break

    # Test Certifications pattern
    cert_patterns = [
        r"- Certifications:\s*(.+)",
        r"Certifications:\s*(.+)",
        r"Their Certifications Listed:\s*(.+)"
    ]

    for pattern in cert_patterns:
        match = re.search(pattern, test_response)
        if match:
            print(f"Certifications matched with pattern '{pattern}': {match.group(1)}")
            break

if __name__ == "__main__":
    test_extraction()