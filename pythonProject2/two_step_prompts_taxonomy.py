"""
Enhanced prompts for two-step resume processing with skills taxonomy integration
"""

from skills_detector import get_taxonomy_context

def ordinal(n):
    """Convert number to ordinal string (1st, 2nd, 3rd, etc.)"""
    if 11 <= (n % 100) <= 13:
        suffix = 'th'
    else:
        suffix = ['th', 'st', 'nd', 'rd', 'th'][min(n % 10, 4)]
    return str(n) + suffix

def create_step1_prompt(resume_text, userid=None):
    """Create an enhanced prompt for step 1: Personal info, work history, and industry with skills taxonomy
    
    Args:
        resume_text: The full text of the resume
        userid: Optional user ID for logging purposes
    """
    
    # Get relevant skills taxonomy sections
    taxonomy_context = get_taxonomy_context(resume_text, max_categories=2, userid=userid)
    
    return [
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
                       "   - Just 'Country' if only the country is specified"
                       "5. Important: NEVER append 'NULL' to any location - if part of the location is unknown, just use what you know"
                       "6. Strip out any extra information (zip codes, postal codes, etc.)"
                       "7. Use NULL only if you truly cannot find ANY location information in that job entry"
                       "IMPORTANT: Be thorough - check the entire job section before deciding there's no location."
        },
        {
            "role": "system",
            "content": "Education/Degrees are typically located at the top or bottom of resumes in an EDUCATION section."
                       "For Bachelor's Degree: Look for bachelor's/undergraduate degrees including abbreviations like: BS, B.S., BA, B.A., BEng, BBA, etc."
                       "For Master's Degree: Look for master's/graduate degrees including abbreviations like: MS, M.S., MA, M.A., MBA, MEng, MSc, MFA, etc."
                       "Extract the complete degree information including field of study and institution when available."
        },
        {
            "role": "system",
            "content": "Abbreviate the state if it is not already done so."
                       "When needing to do a list, separate by commas."
                       "If there is no last name or the last name is one letter, look in their email for their last name."
        },
        {
            "role": "system",
            "content": f"{taxonomy_context}\n"
                       "Use the following rules when assessing Skills, and refer to the SKILLS TAXONOMY REFERENCE above:"
                       "Definition: Skills are a list of keywords from the resume that repeat and are consistent "
                       "throughout their project work."
                       "Skills should be 2 words max."
                       "Use the skills taxonomy reference above to help identify and categorize skills appropriately."
                       "Prioritize skills that match the taxonomy categories that are most relevant to this resume."
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
                       "- Their Zip Code:\n"
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
                       "- Most Recent Start Date (Most Recent Start Date (YYYY-MM-DD format, if unknown put NULL)):\n"
                       "- Most Recent End Date (Most Recent End Date (YYYY-MM-DD format, if unknown put NULL):\n"
                       "- Most Recent Job Location(City, State Abbreviation or just State):\n"
                       "- Second Most Recent Company Worked for:\n"
                       "- Second Most Recent Start Date (Most Recent Start Date (YYYY-MM-DD format, if unknown put NULL)):\n"
                       "- Second Most Recent End Date (Most Recent End Date (YYYY-MM-DD format, if unknown put NULL):\n"
                       "- Second Most Recent Job Location(City, State Abbreviation or just State):\n"
                       "- Third Most Recent Company Worked for:\n"
                       "- Third Most Recent Start Date (Most Recent Start Date (YYYY-MM-DD format, if unknown put NULL)):\n"
                       "- Third Most Recent End Date (Most Recent End Date (YYYY-MM-DD format, if unknown put NULL):\n"
                       "- Third Most Recent Job Location(City, State Abbreviation or just State):\n"
                       "- Fourth Most Recent Company Worked for:\n"
                       "- Fourth Most Recent Start Date (Most Recent Start Date (YYYY-MM-DD format, if unknown put NULL)):\n"
                       "- Fourth Most Recent End Date (Most Recent End Date (YYYY-MM-DD format, if unknown put NULL):\n"
                       "- Fourth Most Recent Job Location(City, State Abbreviation or just State):\n"
                       "- Fifth Most Recent Company Worked for:\n"
                       "- Fifth Most Recent Start Date (Most Recent Start Date (YYYY-MM-DD format, if unknown put NULL)):\n"
                       "- Fifth Most Recent End Date (Most Recent End Date (YYYY-MM-DD format, if unknown put NULL):\n"
                       "- Fifth Most Recent Job Location(City, State Abbreviation or just State):\n"
                       "- Sixth Most Recent Company Worked for:\n"
                       "- Sixth Most Recent Start Date (Most Recent Start Date (YYYY-MM-DD format, if unknown put NULL)):\n"
                       "- Sixth Most Recent End Date (Most Recent End Date (YYYY-MM-DD format, if unknown put NULL):\n"
                       "- Sixth Most Recent Job Location(City, State Abbreviation or just State):\n"
                       "- Seventh Most Recent Company Worked for:\n"
                       "- Seventh Most Recent Start Date (Most Recent Start Date (YYYY-MM-DD format, if unknown put NULL)):\n"
                       "- Seventh Most Recent End Date (Most Recent End Date (YYYY-MM-DD format, if unknown put NULL):\n"
                       "- Seventh Most Recent Job Location(City, State Abbreviation or just State):\n"
                       "- Without putting information technology and based on all 7 of their most recent companies above, what is the Primary industry they work in:\n"
                       "- Without putting information technology and based on all 7 of their most recent companies above, what is the Secondary industry they work in:\n"
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
                       "- Their Zip Code:\n"
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

def create_step2_prompt(resume_text, step1_results, userid=None):
    """Create an enhanced prompt for step 2: Skills, technical info, and experience calculations with skills taxonomy
    
    Args:
        resume_text: The original resume text
        step1_results: Results from step 1 (parsed into a dictionary)
        userid: Optional user ID for logging purposes
    """
    
    # Extract work history dates to help with calculations
    work_history = []
    
    # Define the correct mapped field names based on the field mapping in parse_step1_response
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
    
    for i in range(7):  # Up to 7 companies
        company = step1_results.get(company_keys[i], "NULL")
        start_date = step1_results.get(start_date_keys[i], "NULL")
        end_date = step1_results.get(end_date_keys[i], "NULL")
        location = step1_results.get(location_keys[i], "NULL")
        
        if company != "NULL":
            work_history.append({
                "company": company,
                "start_date": start_date,
                "end_date": end_date,
                "location": location
            })
    
    # Format work history for the prompt
    work_history_text = ""
    for i, job in enumerate(work_history):
        work_history_text += f"Job {i+1}:\n"
        work_history_text += f"Company: {job['company']}\n"
        work_history_text += f"Start Date: {job['start_date']}\n"
        work_history_text += f"End Date: {job['end_date']}\n"
        work_history_text += f"Location: {job['location']}\n\n"
    
    # Primary/Secondary industry - using mapped field names
    primary_industry = step1_results.get("PrimaryIndustry", "NULL")
    secondary_industry = step1_results.get("SecondaryIndustry", "NULL")
    
    # Build the job titles for context - using mapped field names
    primary_title = step1_results.get("PrimaryTitle", "NULL")
    secondary_title = step1_results.get("SecondaryTitle", "NULL")
    tertiary_title = step1_results.get("TertiaryTitle", "NULL")
    
    # Get top skills from step 1
    top_skills = step1_results.get("Top10Skills", "NULL")
    
    # Get skills taxonomy - use a more targeted approach for step 2
    # If we successfully got top skills from step 1, use those to enhance the taxonomy search
    combined_text = resume_text
    if top_skills and top_skills != "NULL":
        # Give extra weight to the skills already identified in step 1
        combined_text = resume_text + "\n\n" + top_skills * 3  # Repeat to give more weight
    
    # Get taxonomy context with the combined text to better target skill areas
    taxonomy_context = get_taxonomy_context(combined_text, max_categories=3, userid=userid)
    
    return [
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
                       "When identifying skills, software languages, applications, and hardware, prioritize accuracy over standardization. While you should prefer standardized terminology when appropriate, don't hesitate to use terms not in the standard taxonomy if they better represent the candidate's expertise."
        },
        {
            "role": "system",
            "content": f"BACKGROUND INFORMATION FROM FIRST ANALYSIS:\n\n"
                       f"Job Titles:\n"
                       f"- Primary: {primary_title}\n"
                       f"- Secondary: {secondary_title}\n"
                       f"- Tertiary: {tertiary_title}\n\n"
                       f"Industries:\n"
                       f"- Primary Industry: {primary_industry}\n"
                       f"- Secondary Industry: {secondary_industry}\n\n"
                       f"Technical Skills:\n"
                       f"- Top 10 Skills: {top_skills}\n\n"
                       f"Work History:\n{work_history_text}"
        },
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
        {
            "role": "system",
            "content": "Use the following rules when assessing Most used Software Applications: "
                       "Please only list out actual software applications. nothing else."
                       "Analyze their resume and determine what software they use most."
                       "If none can be found put NULL."
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
                       "For the categories, do not repeat the same category."
                       "Both categories MUST have an answer!"
        },
        {
            "role": "system",
            "content": "Use the following rules when writing their summary:"
                       "For their summary, give a brief summary of their resume in a few sentences."
                       "Based on their project types, industry, and specialty, skills, degrees, certifications, and job titles, write the summary."
        },
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
        {
            "role": "system",
            "content": "Use the following rules when determining Average Tenure and Year of Experience:"
                       "Use all previous start date and end date questions answers to determine this. "
                       "Just put a number and no other characters."
                       "Result should not be 0."
                       "Result should only be numerical."
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
                       "- Based on their skills, put them in a primary technical category:\n"
                       "- Based on their skills, put them in a subsidiary technical category:\n"
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