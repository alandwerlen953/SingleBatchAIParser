"""
Date handling utilities for resume processing 

This module provides functions to:
1. Parse and validate dates from resumes
2. Calculate tenure and experience metrics
3. Handle "present" positions consistently
"""

import re
import datetime
from typing import Optional, Tuple, Dict, Union, List
import logging

def parse_resume_date(date_string: str, allow_future: bool = False) -> Tuple[Optional[datetime.date], float, str]:
    """
    Parse a date string from a resume with confidence score
    
    Args:
        date_string: String representation of a date
        allow_future: Whether to allow future dates (default: False)
    
    Returns:
        Tuple containing:
        - Parsed date object or None if parsing fails
        - Confidence score (0.0-1.0)
        - Original text
    """
    if not date_string or date_string.strip() == "":
        return None, 0.0, date_string
    
    # Handle special indicators for current positions
    date_lower = date_string.lower().strip() if date_string else ""
    if date_string.upper() == "NULL" or date_lower in ["present", "current", "now", "to date", "today", "ongoing", "to present", "currently"]:
        logging.debug(f"Recognized current position indicator: {date_string}")
        return None, 0.0, date_string
    
    # Clean the input
    cleaned_string = date_string.strip()
    
    # Get today's date for validation
    today = datetime.date.today()
    
    # Try different date formats with confidence scores
    formats_to_try = [
        # YYYY-MM-DD (highest confidence)
        {'pattern': r'^\d{4}-\d{2}-\d{2}$', 'format': '%Y-%m-%d', 'confidence': 1.0},
        # MM/DD/YYYY
        {'pattern': r'^\d{1,2}/\d{1,2}/\d{4}$', 'format': '%m/%d/%Y', 'confidence': 0.9},
        # MMM YYYY (e.g., "Jan 2020")
        {'pattern': r'^[A-Za-z]{3,9}\s+\d{4}$', 'format': '%b %Y', 'confidence': 0.7, 'day': 1},
        # Month YYYY (e.g., "January 2020")
        {'pattern': r'^[A-Za-z]{3,9}\s+\d{4}$', 'format': '%B %Y', 'confidence': 0.7, 'day': 1},
        # YYYY-MM
        {'pattern': r'^\d{4}-\d{2}$', 'format': '%Y-%m', 'confidence': 0.7, 'day': 1},
        # MM/YYYY
        {'pattern': r'^\d{1,2}/\d{4}$', 'format': '%m/%Y', 'confidence': 0.7, 'day': 1},
        # YYYY
        {'pattern': r'^\d{4}$', 'format': '%Y', 'confidence': 0.5, 'day': 1, 'month': 1}
    ]
    
    for format_info in formats_to_try:
        if re.match(format_info['pattern'], cleaned_string):
            try:
                # Handle incomplete dates (month or year only)
                if 'day' in format_info:
                    # For formats missing the day, we'll set it to the 1st
                    if format_info['format'] == '%Y':
                        # Year only
                        date_obj = datetime.datetime.strptime(cleaned_string, format_info['format']).date()
                        date_obj = date_obj.replace(month=format_info.get('month', date_obj.month), 
                                                   day=format_info.get('day', date_obj.day))
                    else:
                        # Month and year
                        date_obj = datetime.datetime.strptime(cleaned_string, format_info['format']).date()
                        date_obj = date_obj.replace(day=format_info.get('day', date_obj.day))
                else:
                    # Complete date
                    date_obj = datetime.datetime.strptime(cleaned_string, format_info['format']).date()
                
                # Validate the date (e.g., not in the future, unless allowed)
                if not allow_future and date_obj > today:
                    # Reject future dates unless explicitly allowed
                    logging.warning(f"Rejected future date: {date_obj} (today is {today})")
                    return None, 0.0, date_string
                
                return date_obj, format_info['confidence'], cleaned_string
                
            except ValueError as e:
                # Log parsing attempt failure at debug level
                logging.debug(f"Failed to parse '{cleaned_string}' with format '{format_info['format']}': {str(e)}")
                # If this format failed, try the next one
                continue
    
    # If we get here, none of the formats worked
    logging.warning(f"Could not parse date: {date_string}")
    return None, 0.0, date_string

def is_current_position(end_date_string: str) -> bool:
    """
    Determine if a position is current based on the end date string
    
    Args:
        end_date_string: String representation of end date
        
    Returns:
        True if position appears to be current, False otherwise
    """
    if not end_date_string:
        return True
        
    # Common indicators of current positions
    current_indicators = [
        "present", "current", "now", "to date", "today",
        "ongoing", "to present", "currently"
    ]
    
    end_date_lower = end_date_string.lower() if end_date_string else ""
    
    # Check for NULL values (may indicate current position)
    if end_date_lower == "null" or not end_date_lower.strip():
        return True
    
    # Check for text indicators of current position
    for indicator in current_indicators:
        if indicator in end_date_lower:
            return True
    
    # Check if date is in the future (may indicate current position)
    date_obj, confidence, _ = parse_resume_date(end_date_string, allow_future=True)
    if date_obj and date_obj > datetime.date.today():
        return True
        
    return False

def calculate_tenure(start_date_string: str, end_date_string: str) -> Dict[str, Union[float, str, bool]]:
    """
    Calculate tenure for a position with confidence metrics
    
    Args:
        start_date_string: String representation of start date
        end_date_string: String representation of end date
        
    Returns:
        Dictionary with:
        - 'tenure_years': Calculated tenure in years
        - 'confidence': Overall confidence score (0.0-1.0)
        - 'is_current': Whether this appears to be a current position
        - 'start_text': Original start date text
        - 'end_text': Original end date text
        - 'start_date': Parsed start date or None
        - 'end_date': Parsed end date or None
    """
    result = {
        'tenure_years': 0.0,
        'confidence': 0.0,
        'is_current': False,
        'start_text': start_date_string,
        'end_text': end_date_string,
        'start_date': None,
        'end_date': None
    }
    
    # Parse start date
    start_date, start_confidence, start_text = parse_resume_date(start_date_string)
    result['start_date'] = start_date
    
    # Check if position is current
    result['is_current'] = is_current_position(end_date_string)
    
    # Parse end date (allow future dates for verification)
    end_date, end_confidence, end_text = parse_resume_date(end_date_string, allow_future=True)
    result['end_date'] = end_date
    
    # Calculate tenure
    if start_date:
        # For current positions, use today's date for end_date
        if result['is_current']:
            end_date = datetime.date.today()
            # Adjust confidence for current positions
            end_confidence = 0.8
            
        # If we have a valid end date, calculate tenure
        if end_date and end_date >= start_date:
            # Calculate difference in days and convert to years
            days_diff = (end_date - start_date).days
            years_diff = days_diff / 365.25  # Account for leap years
            
            result['tenure_years'] = round(years_diff, 2)
            
            # Calculate confidence based on both dates
            # If both dates have high confidence, overall confidence is high
            # If one date has low confidence, overall confidence is lower
            result['confidence'] = (start_confidence + end_confidence) / 2.0
        else:
            # Can't calculate tenure with invalid end date
            result['confidence'] = start_confidence * 0.5  # Reduce confidence
    
    return result

def calculate_experience_metrics(jobs: List[Dict[str, str]]) -> Dict[str, Union[float, List[Dict]]]:
    """
    Calculate experience metrics for a collection of jobs
    
    Args:
        jobs: List of job dictionaries, each containing:
            - 'company': Company name
            - 'start_date': Start date string
            - 'end_date': End date string
            - 'location': Job location
            
    Returns:
        Dictionary with:
        - 'total_experience': Total years of professional experience
        - 'avg_tenure': Average tenure at companies in years
        - 'us_experience': Years of experience in the United States
        - 'confidence': Overall confidence score
        - 'job_metrics': Detailed metrics for each job
    """
    results = {
        'total_experience': 0.0,
        'avg_tenure': 0.0,
        'us_experience': 0.0,
        'confidence': 0.0,
        'job_metrics': []
    }
    
    # No jobs to process
    if not jobs:
        return results
    
    total_tenure = 0.0
    us_tenure = 0.0
    total_confidence = 0.0
    valid_job_count = 0
    us_job_count = 0
    
    # Process each job
    for job in jobs:
        # Skip jobs with missing company
        if not job.get('company') or job['company'] == 'NULL':
            continue
            
        # Calculate tenure for this job
        tenure_metrics = calculate_tenure(job.get('start_date', ''), job.get('end_date', ''))
        
        # Add to job metrics list
        job_detail = {
            'company': job.get('company', ''),
            'location': job.get('location', ''),
            'tenure_years': tenure_metrics['tenure_years'],
            'confidence': tenure_metrics['confidence'],
            'is_current': tenure_metrics['is_current'],
            'start_date': tenure_metrics['start_date'],
            'end_date': tenure_metrics['end_date']
        }
        results['job_metrics'].append(job_detail)
        
        # Only include jobs with valid tenure in calculations
        if tenure_metrics['tenure_years'] > 0 and tenure_metrics['confidence'] > 0:
            total_tenure += tenure_metrics['tenure_years']
            total_confidence += tenure_metrics['confidence']
            valid_job_count += 1
            
            # Check if job is in the US
            location = job.get('location', '').upper()
            us_indicators = [', AL', ', AK', ', AZ', ', AR', ', CA', ', CO', ', CT', ', DE', ', FL', 
                           ', GA', ', HI', ', ID', ', IL', ', IN', ', IA', ', KS', ', KY', ', LA', 
                           ', ME', ', MD', ', MA', ', MI', ', MN', ', MS', ', MO', ', MT', ', NE', 
                           ', NV', ', NH', ', NJ', ', NM', ', NY', ', NC', ', ND', ', OH', ', OK', 
                           ', OR', ', PA', ', RI', ', SC', ', SD', ', TN', ', TX', ', UT', ', VT', 
                           ', VA', ', WA', ', WV', ', WI', ', WY', 'UNITED STATES', 'USA', 'U.S.A']
            
            is_us_job = False
            for indicator in us_indicators:
                if indicator in location:
                    is_us_job = True
                    break
                    
            if is_us_job:
                us_tenure += tenure_metrics['tenure_years']
                us_job_count += 1
    
    # Calculate overall metrics
    if valid_job_count > 0:
        results['total_experience'] = round(total_tenure, 1)
        results['avg_tenure'] = round(total_tenure / valid_job_count, 1)
        results['confidence'] = total_confidence / valid_job_count
        
    if us_job_count > 0:
        results['us_experience'] = round(us_tenure, 1)
    
    return results

# Test function
if __name__ == "__main__":
    # Example usage
    test_jobs = [
        {
            'company': 'Acme Inc.',
            'start_date': '2020-01-01',
            'end_date': 'Present',
            'location': 'New York, NY'
        },
        {
            'company': 'Beta Corp',
            'start_date': '2015-07-01',
            'end_date': '2019-12-31',
            'location': 'San Francisco, CA'
        }
    ]
    
    metrics = calculate_experience_metrics(test_jobs)
    print(f"Total Experience: {metrics['total_experience']} years")
    print(f"Average Tenure: {metrics['avg_tenure']} years")
    print(f"US Experience: {metrics['us_experience']} years")
    print(f"Confidence: {metrics['confidence']:.2f}")
    
    for i, job in enumerate(metrics['job_metrics']):
        print(f"\nJob {i+1}: {job['company']}")
        print(f"  Tenure: {job['tenure_years']} years")
        print(f"  Current: {'Yes' if job['is_current'] else 'No'}")
        print(f"  Confidence: {job['confidence']:.2f}")