"""
Test script for date handling utilities
"""

import logging
import json
from date_utils import (
    parse_resume_date, 
    is_current_position, 
    calculate_tenure, 
    calculate_experience_metrics
)
from date_processor import enhance_resume_dates

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def test_date_parsing():
    """Test the date parsing functionality"""
    print("\n===== DATE PARSING TESTS =====")
    
    test_dates = [
        "2023-01-15",        # Complete date
        "January 2023",      # Month and year
        "Jan 2023",          # Abbreviated month and year
        "2023-01",           # ISO month and year
        "01/2023",           # Numeric month and year
        "2023",              # Only year
        "Present",           # Current position
        "Current",           # Current position
        "To date",           # Current position
        "2025-01-01",        # Future date
        "",                  # Empty
        "NULL"               # Explicit NULL
    ]
    
    print("Testing date parsing with different formats:")
    for date_string in test_dates:
        date_obj, confidence, text = parse_resume_date(date_string)
        current = is_current_position(date_string)
        
        status = "✓" if date_obj is not None or current else "✗"
        
        print(f"{status} {date_string:15} → {date_obj if date_obj else 'None':12} | Confidence: {confidence:.2f} | Current: {current}")

def test_tenure_calculation():
    """Test tenure calculation"""
    print("\n===== TENURE CALCULATION TESTS =====")
    
    test_cases = [
        {"start": "2020-01-01", "end": "2023-01-01", "name": "Complete dates"},
        {"start": "2020-01", "end": "2023-01", "name": "Month-year only"},
        {"start": "2020", "end": "2023", "name": "Year only"},
        {"start": "2020-01-01", "end": "Present", "name": "Current position"},
        {"start": "2020-01-01", "end": "", "name": "Empty end date"},
        {"start": "2020-01-01", "end": "NULL", "name": "NULL end date"},
        {"start": "2020-01-01", "end": "2025-01-01", "name": "Future end date"}
    ]
    
    print("Testing tenure calculation with different date combinations:")
    for case in test_cases:
        result = calculate_tenure(case["start"], case["end"])
        
        print(f"\nTest: {case['name']}")
        print(f"  Start: {case['start']}, End: {case['end']}")
        print(f"  Tenure: {result['tenure_years']:.2f} years")
        print(f"  Confidence: {result['confidence']:.2f}")
        print(f"  Current position: {result['is_current']}")

def test_experience_metrics():
    """Test experience metrics calculation"""
    print("\n===== EXPERIENCE METRICS TESTS =====")
    
    test_jobs = [
        {
            "company": "TechCorp",
            "start_date": "2020-01-01",
            "end_date": "Present",
            "location": "Seattle, WA"
        },
        {
            "company": "InnoSystems",
            "start_date": "2018-06",
            "end_date": "2019-12",
            "location": "San Francisco, CA"
        },
        {
            "company": "GlobalTech",
            "start_date": "2015-01",
            "end_date": "2018-05",
            "location": "New York, NY"
        },
        {
            "company": "Overseas Ltd",
            "start_date": "2012",
            "end_date": "2015",
            "location": "London, UK"
        }
    ]
    
    print("Testing experience metrics calculation:")
    metrics = calculate_experience_metrics(test_jobs)
    
    print(f"\nTotal Experience: {metrics['total_experience']:.1f} years")
    print(f"Average Tenure: {metrics['avg_tenure']:.1f} years")
    print(f"US Experience: {metrics['us_experience']:.1f} years")
    print(f"Overall Confidence: {metrics['confidence']:.2f}")
    
    print("\nDetailed job metrics:")
    for i, job in enumerate(metrics['job_metrics']):
        print(f"\nJob {i+1}: {job['company']}")
        print(f"  Location: {job['location']}")
        print(f"  Tenure: {job['tenure_years']:.2f} years")
        print(f"  Current: {'Yes' if job['is_current'] else 'No'}")
        print(f"  Confidence: {job['confidence']:.2f}")

def test_resume_enhancement():
    """Test resume enhancement with sample data"""
    print("\n===== RESUME ENHANCEMENT TEST =====")
    
    # Sample combined results from step1 and step2
    sample_results = {
        "PrimaryTitle": "Java Developer",
        "SecondaryTitle": "Backend Engineer",
        "TertiaryTitle": "DevOps Engineer",
        "MostRecentCompany": "Amazon Web Services",
        "MostRecentStartDate": "2022-01",
        "MostRecentEndDate": "Present",
        "MostRecentLocation": "Seattle, WA",
        "SecondMostRecentCompany": "Microsoft",
        "SecondMostRecentStartDate": "2019-05-01",
        "SecondMostRecentEndDate": "2021-12-01",
        "SecondMostRecentLocation": "Redmond, WA",
        "ThirdMostRecentCompany": "Google",
        "ThirdMostRecentStartDate": "2017",
        "ThirdMostRecentEndDate": "2019",
        "ThirdMostRecentLocation": "Mountain View, CA",
        "FourthMostRecentCompany": "Oracle",
        "FourthMostRecentStartDate": "2014-01",
        "FourthMostRecentEndDate": "2016-12",
        "FourthMostRecentLocation": "NULL",
        "YearsofExperience": "8",
        "AvgTenure": "2.5",
        "LengthinUS": "8",
        "Top10Skills": "Java, Python, AWS, Docker, Kubernetes"
    }
    
    try:
        # Enhance the resume data
        enhanced = enhance_resume_dates(sample_results)
        
        print("Original vs Enhanced Values:")
        for key in ["YearsofExperience", "AvgTenure", "LengthinUS"]:
            print(f"{key}: {sample_results.get(key, 'NULL')} → {enhanced.get(key, 'NULL')}")
        
        print("\nNew fields added:")
        for key in enhanced:
            if key not in sample_results:
                if key == "DateMetadata":
                    try:
                        metadata = json.loads(enhanced[key])
                        print(f"\nDate Metadata (decoded):")
                        print(f"  Metrics confidence: {metadata.get('metrics_confidence', 0):.2f}")
                        print(f"  Calculation date: {metadata.get('calculation_date', '')}")
                        print(f"  Job metrics: {len(metadata.get('job_metrics', []))} jobs analyzed")
                    except Exception as e:
                        print(f"{key}: [Error decoding JSON: {str(e)}]")
                else:
                    print(f"{key}: {enhanced[key]}")
    except Exception as e:
        print(f"Error during enhancement: {str(e)}")

if __name__ == "__main__":
    print("=== DATE UTILITIES TEST SCRIPT ===")
    
    test_date_parsing()
    test_tenure_calculation()
    test_experience_metrics()
    test_resume_enhancement()
    
    print("\nAll tests completed.")