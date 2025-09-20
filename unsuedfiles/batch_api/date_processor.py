"""
Date processor module for enhancing resume date calculations

This module integrates with the existing processing pipeline to:
1. Convert date strings to proper date objects or NULL
2. Calculate enhanced experience metrics for existing fields
3. Maintain backward compatibility with the database schema
"""

import logging
from typing import Dict, Any, List, Optional, Union
import datetime
import json
from date_utils import (
    parse_resume_date, 
    is_current_position, 
    calculate_tenure, 
    calculate_experience_metrics
)

def enhance_resume_dates(combined_results: Dict[str, Any]) -> Dict[str, Any]:
    """
    Enhance resume dates with proper parsing and confidence metrics
    
    Args:
        combined_results: The combined results from step1 and step2 processing
        
    Returns:
        Enhanced results with improved metrics for existing fields only
    """
    # Extract company data in the same structure expected by calculate_experience_metrics
    jobs = []
    
    # Define the field mapping from combined_results to our jobs structure
    company_keys = ["MostRecentCompany", "SecondMostRecentCompany", "ThirdMostRecentCompany", 
                   "FourthMostRecentCompany", "FifthMostRecentCompany", "SixthMostRecentCompany", 
                   "SeventhMostRecentCompany"]
    
    start_date_keys = ["MostRecentStartDate", "SecondMostRecentStartDate", "ThirdMostRecentStartDate", 
                       "FourthMostRecentStartDate", "FifthMostRecentStartDate", "SixthMostRecentStartDate", 
                       "SeventhMostRecentStartDate"]
    
    end_date_keys = ["MostRecentEndDate", "SecondMostRecentEndDate", "ThirdMostRecentEndDate", 
                     "FourthMostRecentEndDate", "FifthMostRecentEndDate", "SixthMostRecentEndDate", 
                     "SeventhMostRecentEndDate"]
    
    location_keys = ["MostRecentLocation", "SecondMostRecentLocation", "ThirdMostRecentLocation", 
                     "FourthMostRecentLocation", "FifthMostRecentLocation", "SixthMostRecentLocation", 
                     "SeventhMostRecentLocation"]
    
    # Build the jobs list from the combined results
    for i in range(7):  # Up to 7 companies
        company = combined_results.get(company_keys[i], "NULL")
        if company != "NULL" and company:
            jobs.append({
                'company': company,
                'start_date': combined_results.get(start_date_keys[i], "NULL"),
                'end_date': combined_results.get(end_date_keys[i], "NULL"),
                'location': combined_results.get(location_keys[i], "NULL")
            })
    
    # Calculate experience metrics
    metrics = calculate_experience_metrics(jobs)
    
    # Store calculated metrics in the enhanced results
    enhanced_results = combined_results.copy()
    
    # Log the enhanced date metrics but don't store in database
    logging.info(f"Enhanced date metrics calculated - confidence: {metrics['confidence']:.2f}")
    
    # Log current positions
    current_positions = []
    for job in metrics['job_metrics']:
        if job['is_current']:
            current_positions.append(job['company'])
            logging.info(f"Detected current position: {job['company']} - tenure: {job['tenure_years']:.2f} years")
    
    if current_positions:
        logging.info(f"Current positions detected: {', '.join(current_positions)}")
    
    # Update only the existing experience calculations with more accurate values
    if metrics['total_experience'] > 0:
        enhanced_results["YearsofExperience"] = str(metrics['total_experience'])
        logging.info(f"Updated YearsofExperience: {enhanced_results['YearsofExperience']} (confidence: {metrics['confidence']:.2f})")
    
    if metrics['avg_tenure'] > 0:
        enhanced_results["AvgTenure"] = str(metrics['avg_tenure'])
        logging.info(f"Updated AvgTenure: {enhanced_results['AvgTenure']} (confidence: {metrics['confidence']:.2f})")
    
    if metrics['us_experience'] > 0:
        enhanced_results["LengthinUS"] = str(metrics['us_experience'])
        logging.info(f"Updated LengthinUS: {enhanced_results['LengthinUS']} (confidence: {metrics['confidence']:.2f})")
    
    return enhanced_results

def process_resume_with_enhanced_dates(userid: str, combined_results: Dict[str, Any]) -> Dict[str, Any]:
    """
    Process a resume with enhanced date handling
    
    Args:
        userid: The user ID
        combined_results: Combined results from step1 and step2
        
    Returns:
        Enhanced results with improved metrics for existing fields only
    """
    try:
        logging.info(f"UserID {userid}: Enhancing date handling")
        
        # Process dates and add metadata
        enhanced_results = enhance_resume_dates(combined_results)
        
        logging.info(f"UserID {userid}: Date enhancement complete")
        return enhanced_results
        
    except Exception as e:
        logging.error(f"Error enhancing dates for UserID {userid}: {str(e)}")
        return combined_results  # Return original results on error