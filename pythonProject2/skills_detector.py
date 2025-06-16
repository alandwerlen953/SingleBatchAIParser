"""
Skills taxonomy detector for resume processing

This module reads the newSkills.csv taxonomy and provides functions to:
1. Load and parse the skills taxonomy
2. Detect relevant skill categories in a resume
3. Generate targeted skill taxonomy context for prompts
"""

import csv
import re
import os
from collections import Counter, defaultdict
import logging

# Check if we're in quiet mode
if os.environ.get('QUIET_MODE', '').lower() in ('1', 'true', 'yes'):
    logging.disable(logging.CRITICAL)

# Path to skills taxonomy file
SKILLS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "Dictionary", "newSkills.csv")

# Data structures to hold the taxonomy
skill_categories = {}  # Maps category names to their row indices
category_jobs = defaultdict(list)  # Maps category names to job titles
category_skills = defaultdict(list)  # Maps category names to skills
all_skills_map = {}  # Maps individual skills to their categories

def load_skills_taxonomy():
    """
    Load and parse the skills taxonomy file
    Returns True if successful, False otherwise
    """
    try:
        if not os.path.exists(SKILLS_FILE):
            logging.error(f"Skills taxonomy file not found: {SKILLS_FILE}")
            return False
            
        with open(SKILLS_FILE, 'r', encoding='utf-8') as file:
            reader = csv.reader(file)
            current_category = None
            row_idx = 0
            
            for row in reader:
                if not row or not row[0].strip():
                    row_idx += 1
                    continue
                    
                content = row[0].strip()
                
                # Check if this is a category header
                if content.startswith('##'):
                    current_category = content[2:].strip()
                    skill_categories[current_category] = row_idx
                    row_idx += 1
                    continue
                
                if current_category:
                    # Second row after header contains job titles
                    if row_idx == skill_categories[current_category] + 1:
                        job_titles = content.split(',')
                        category_jobs[current_category] = [title.strip() for title in job_titles]
                    
                    # Third row after header contains skills
                    elif row_idx == skill_categories[current_category] + 2:
                        skills = content.split(',')
                        clean_skills = [skill.strip() for skill in skills]
                        category_skills[current_category] = clean_skills
                        
                        # Also map each skill to its category for reverse lookup
                        for skill in clean_skills:
                            if skill:
                                all_skills_map[skill.lower()] = current_category
                
                row_idx += 1
                
        logging.info(f"Loaded {len(skill_categories)} skill categories from taxonomy")
        return True
        
    except Exception as e:
        logging.error(f"Error loading skills taxonomy: {str(e)}")
        return False

def detect_skill_categories(resume_text):
    """
    Analyze resume text to identify the most relevant skill categories
    
    Args:
        resume_text: The full text of the resume
        
    Returns:
        List of (category_name, relevance_score) tuples, sorted by relevance
    """
    # Make sure taxonomy is loaded
    if not skill_categories and not load_skills_taxonomy():
        logging.error("Failed to load skills taxonomy")
        return []
    
    # Convert resume text to lowercase for case-insensitive matching
    resume_lower = resume_text.lower()
    
    # Count occurrences of skills from each category
    category_scores = Counter()
    
    # Try to identify the header section and current job title
    header_section = ""
    header_lines = resume_lower.split('\n')[:10]  # First 10 lines likely contain the header
    header_section = ' '.join(header_lines)
    
    # Try to extract work experience section for additional weighting
    work_exp_section = ""
    work_exp_pattern = r'(?i)(work experience|employment|professional experience).*?(\n\n|\Z)'
    work_exp_match = re.search(work_exp_pattern, resume_text, re.DOTALL)
    if work_exp_match:
        work_exp_section = work_exp_match.group(0).lower()
        
    # Extract first job (most recent) - typically right after the work experience heading
    first_job_section = ""
    if work_exp_section:
        job_sections = re.split(r'\n\n+', work_exp_section, maxsplit=2)
        if len(job_sections) > 1:
            first_job_section = job_sections[1].lower()  # Skip the header, take the first job entry
    
    # Check for job title matches with weighted importance
    for category, jobs in category_jobs.items():
        for job in jobs:
            job_lower = job.lower()
            # Look for job titles with word boundaries
            pattern = r'\b' + re.escape(job_lower) + r'\b'
            
            # Check in different sections with different weights
            header_matches = re.findall(pattern, header_section)
            first_job_matches = re.findall(pattern, first_job_section) 
            work_exp_matches = re.findall(pattern, work_exp_section)
            full_resume_matches = re.findall(pattern, resume_lower)
            
            # Calculate score with weighted importance
            header_score = len(header_matches) * 10       # Highest weight: job title in header
            first_job_score = len(first_job_matches) * 8  # High weight: most recent job
            work_exp_score = len(work_exp_matches) * 5    # Medium weight: any job in work history
            other_matches = max(0, len(full_resume_matches) - len(header_matches) - len(work_exp_matches))
            other_score = other_matches * 2               # Lowest weight: mentions elsewhere
            
            job_title_score = header_score + first_job_score + work_exp_score + other_score
            
            if job_title_score > 0:
                category_scores[category] += job_title_score
                if header_score > 0:
                    logging.debug(f"Job title in HEADER: '{job_lower}' - Added {header_score} to {category}")
                if first_job_score > 0:
                    logging.debug(f"Job title in MOST RECENT JOB: '{job_lower}' - Added {first_job_score} to {category}")
                if work_exp_score > 0:
                    logging.debug(f"Job title in WORK HISTORY: '{job_lower}' - Added {work_exp_score} to {category}")
    
    # Work experience section is already extracted above
    
    # Check for exact skill matches
    for skill, category in all_skills_map.items():
        # Use word boundary to match whole words, not substrings
        pattern = r'\b' + re.escape(skill) + r'\b'
        
        # Check in full resume
        full_matches = re.findall(pattern, resume_lower)
        
        # Check in work experience section if available
        work_exp_matches = []
        if work_exp_section:
            work_exp_matches = re.findall(pattern, work_exp_section)
        
        if full_matches:
            # Base score with slight boost for longer, more specific skills
            base_score = len(full_matches) * (1 + 0.1 * len(skill.split()))
            
            # Extra weight for skills mentioned in work experience
            work_exp_bonus = len(work_exp_matches) * 2
            
            total_score = base_score + work_exp_bonus
            category_scores[category] += total_score
            
            if work_exp_bonus > 0:
                logging.debug(f"Skill match in work exp: '{skill}' - Added {total_score} to {category}")
    
    # Return categories sorted by relevance score, highest first
    return sorted([(cat, score) for cat, score in category_scores.items()], 
                 key=lambda x: x[1], reverse=True)

def get_top_categories(resume_text, max_categories=3):
    """
    Get the most relevant skill categories for this resume using an adaptive threshold
    
    Args:
        resume_text: The full text of the resume
        max_categories: Maximum number of categories to return
        
    Returns:
        List of category names
    """
    categories = detect_skill_categories(resume_text)
    
    if not categories:
        return []
    
    # Always include the highest scoring category
    top_categories = [categories[0][0]]
    
    if len(categories) > 1:
        # Get highest score
        highest_score = categories[0][1]
        
        # Calculate threshold: highest score minus 20% of highest score
        threshold = highest_score - (0.2 * highest_score)
        
        # Include additional categories that meet the threshold, up to max_categories
        for cat, score in categories[1:]:
            if score >= threshold and len(top_categories) < max_categories:
                top_categories.append(cat)
    
    return top_categories

def get_taxonomy_context(resume_text, max_categories=2, userid=None):
    """
    Generate prompt context with the most relevant skills taxonomy sections
    
    Args:
        resume_text: The full text of the resume
        max_categories: Maximum number of categories to include
        userid: Optional user ID for logging purposes
        
    Returns:
        Formatted string with relevant skills taxonomy sections
    """
    top_categories = get_top_categories(resume_text, max_categories)
    
    if not top_categories:
        logging.warning("No relevant skill categories detected")
        return ""
    
    context = "SKILLS TAXONOMY REFERENCE:\n\n"
    
    # Use provided userid or try to get it from the calling function
    if userid is None:
        from inspect import currentframe, getouterframes
        # Try to get the userid from the calling function's variables
        caller_frame = getouterframes(currentframe(), 2)
        userid = "Unknown"
        for frame in caller_frame:
            if 'userid' in frame.frame.f_locals:
                userid = frame.frame.f_locals['userid']
                break
    
    categories_with_scores = detect_skill_categories(resume_text)
    # Calculate threshold for logging
    if categories_with_scores:
        highest_score = categories_with_scores[0][1]
        threshold = highest_score - (0.2 * highest_score)
        
        logging.info(f"********** TAXONOMY SELECTION - UserID {userid} **********")
        logging.info(f"UserID {userid}: TOP SELECTED CATEGORIES: {', '.join(top_categories)}")
        logging.info(f"UserID {userid}: HIGHEST CATEGORY SCORE: {highest_score:.1f}, THRESHOLD: {threshold:.1f}")
        logging.info(f"UserID {userid}: ALL CATEGORIES WITH SCORES: {categories_with_scores[:5]}")
        
        # Log detected job titles for top categories
        for category in top_categories[:1]:  # Just log for the top category to avoid log clutter
            matching_job_titles = []
            for job in category_jobs.get(category, []):
                job_lower = job.lower()
                pattern = r'\b' + re.escape(job_lower) + r'\b'
                if re.search(pattern, resume_text.lower()):
                    matching_job_titles.append(job)
            
            if matching_job_titles:
                logging.info(f"UserID {userid}: MATCHING JOB TITLES for {category}: {', '.join(matching_job_titles[:5])}")
                if len(matching_job_titles) > 5:
                    logging.info(f"UserID {userid}: ... and {len(matching_job_titles) - 5} more matching job titles")
        
        logging.info(f"*********************************************************")
    else:
        logging.info(f"********** TAXONOMY SELECTION - UserID {userid} **********")
        logging.info(f"UserID {userid}: NO CATEGORIES DETECTED")
        logging.info(f"*********************************************************")
    
    # Build the context with detailed logging
    included_jobs = {}
    included_skills = {}
    
    for category in top_categories:
        context += f"## {category}\n"
        logging.info(f"UserID {userid}: Adding category section: {category}")
        
        # Add job titles
        jobs = category_jobs.get(category, [])
        if jobs:
            job_sample = jobs[:10]  # Limit to first 10 for brevity
            context += "Relevant job titles: " + ", ".join(job_sample)
            if len(jobs) > 10:
                context += f", and {len(jobs)-10} more"
            context += "\n"
            included_jobs[category] = job_sample
        
        # Add skills
        skills = category_skills.get(category, [])
        if skills:
            skill_sample = skills[:20]  # Limit to first 20 for brevity
            context += "Skills in this category: " + ", ".join(skill_sample)
            if len(skills) > 20:
                context += f", and {len(skills)-20} more"
            context += "\n"
            included_skills[category] = skill_sample
        
        context += "\n"
    
    # Log a summary of what was included
    if categories_with_scores:
        # Get all detected categories and their scores
        all_categories = {cat: score for cat, score in categories_with_scores}
        
        logging.info(f"UserID {userid}: TAXONOMY SUMMARY: {len(top_categories)} sections added to prompt")
        for category in top_categories:
            num_jobs = len(included_jobs.get(category, []))
            num_skills = len(included_skills.get(category, []))
            score = all_categories.get(category, 0)
            logging.info(f"UserID {userid}: INCLUDED '{category}' - Score: {score:.1f}, Jobs: {num_jobs}, Skills: {num_skills}")
    else:
        logging.info(f"UserID {userid}: TAXONOMY SUMMARY: No sections added (no categories detected)")
    
    return context

# Test loading the taxonomy when module is imported
if __name__ != "__main__":
    # Suppress the initial load message if in quiet mode
    if os.environ.get('QUIET_MODE', '').lower() in ('1', 'true', 'yes'):
        # Temporarily disable logging for this load
        original_level = logging.root.level
        logging.disable(logging.CRITICAL)
        load_skills_taxonomy()
        logging.disable(original_level)
    else:
        load_skills_taxonomy()