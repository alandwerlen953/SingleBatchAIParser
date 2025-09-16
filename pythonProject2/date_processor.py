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
    
    # Update the experience calculations with more accurate values
    # For YearsofExperience, preserve the AI's answer if it's not NULL
    ai_years_of_experience = enhanced_results.get("YearsofExperience", "NULL")
    if ai_years_of_experience and ai_years_of_experience != "NULL":
        # Keep the AI's answer
        logging.info(f"Preserving AI's YearsofExperience value: {ai_years_of_experience}")
    elif metrics['total_experience'] > 0:
        # Only use calculated value if AI didn't provide one
        enhanced_results["YearsofExperience"] = str(metrics['total_experience'])
        logging.info(f"AI returned NULL for YearsofExperience, using calculated value: {enhanced_results['YearsofExperience']} (confidence: {metrics['confidence']:.2f})")
    else:
        # Last resort fallback
        if len(jobs) > 0:
            # Approximate as sum of job lengths
            default_experience = sum(1 for job in jobs if job.get('company'))
            enhanced_results["YearsofExperience"] = str(default_experience)
            logging.info(f"Using fallback YearsofExperience: {enhanced_results['YearsofExperience']}")
    
    # For AvgTenure, preserve the AI's answer if it's not NULL
    ai_avg_tenure = enhanced_results.get("AvgTenure", "NULL")
    if ai_avg_tenure and ai_avg_tenure != "NULL":
        # Keep the AI's answer
        logging.info(f"Preserving AI's AvgTenure value: {ai_avg_tenure}")
    elif metrics['avg_tenure'] > 0:
        # Only use calculated value if AI didn't provide one
        enhanced_results["AvgTenure"] = str(metrics['avg_tenure'])
        logging.info(f"AI returned NULL for AvgTenure, using calculated value: {enhanced_results['AvgTenure']} (confidence: {metrics['confidence']:.2f})")
    else:
        # Last resort fallback
        if len(jobs) > 0:
            default_tenure = round(metrics['total_experience'] / len(jobs), 1) if metrics['total_experience'] > 0 else 2.0
            enhanced_results["AvgTenure"] = str(default_tenure)
            logging.info(f"Using fallback AvgTenure: {enhanced_results['AvgTenure']}")
    
    # For LengthinUS, preserve the AI's answer if it's not NULL
    ai_length_in_us = enhanced_results.get("LengthinUS", "NULL")
    if ai_length_in_us and ai_length_in_us != "NULL":
        # Keep the AI's answer
        logging.info(f"Preserving AI's LengthinUS value: {ai_length_in_us}")
    elif metrics['us_experience'] > 0:
        # Only use calculated value if AI didn't provide one
        enhanced_results["LengthinUS"] = str(metrics['us_experience'])
        logging.info(f"AI returned NULL for LengthinUS, using calculated US experience: {enhanced_results['LengthinUS']} (confidence: {metrics['confidence']:.2f})")
    else:
        # Check location information for any US indicators
        has_us_indicators = False
        for job in jobs:
            location = job.get('location', '').upper()
            if 'USA' in location or 'UNITED STATES' in location or 'TX' in location or ', US' in location:
                has_us_indicators = True
                break
                
        # If we have US indicators and the AI returned NULL, use total experience as a fallback
        if has_us_indicators and enhanced_results.get("LengthinUS", "NULL") == "NULL":
            if metrics['total_experience'] > 0:
                enhanced_results["LengthinUS"] = str(metrics['total_experience'])
                logging.info(f"Using total experience for LengthinUS as fallback: {enhanced_results['LengthinUS']}")
    
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