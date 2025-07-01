"""
One-time script to fix location entries containing NULL
"""

import os
import logging
import pyodbc
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    force=True
)

def fix_location_entries():
    """Fix location entries that contain NULL as part of the string"""
    server_ip = '172.19.115.25'
    database = 'BH_Mirror'
    username = 'silver'
    password = 'ltechmatlen'
    
    # Location fields to check
    location_fields = [
        "MostRecentLocation", 
        "SecondMostRecentLocation", 
        "ThirdMostRecentLocation",
        "FourthMostRecentLocation", 
        "FifthMostRecentLocation", 
        "SixthMostRecentLocation", 
        "SeventhMostRecentLocation"
    ]
    
    try:
        # Connect to the database
        connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server_ip};DATABASE={database};UID={username};PWD={password}'
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()
        
        total_fixes = 0
        
        # Process each location field
        for field in location_fields:
            # Find records with ", NULL" in the field
            query = f"SELECT userid, {field} FROM aicandidate WHERE {field} LIKE '%,% NULL%' OR {field} LIKE '%,%NULL%'"
            cursor.execute(query)
            rows = cursor.fetchall()
            
            if rows:
                logging.info(f"Found {len(rows)} records with NULL in {field}")
                
                for row in rows:
                    userid = row[0]
                    location = row[1]
                    
                    # Fix the location by removing ", NULL" or similar patterns
                    fixed_location = location.replace(", NULL", "").replace(",NULL", "")
                    
                    # Update the record
                    update_query = f"UPDATE aicandidate SET {field} = ? WHERE userid = ?"
                    cursor.execute(update_query, (fixed_location, userid))
                    
                    logging.info(f"Fixed {field} for userid {userid}: '{location}' -> '{fixed_location}'")
                    total_fixes += 1
            
                # Commit changes for this field
                conn.commit()
        
        logging.info(f"Total fixes completed: {total_fixes}")
        cursor.close()
        conn.close()
        
    except Exception as e:
        logging.error(f"Error fixing location entries: {str(e)}")

def update_location_prompt_instructions():
    """Create updated location prompt instructions to prevent NULL in international locations"""
    improved_instructions = """
IMPROVED LOCATION INSTRUCTIONS:

Replace the current location instructions in two_step_prompts_taxonomy.py with these:

```python
{
    "role": "system",
    "content": "Use the following rules when finding company locations:"
               "1. For each company entry, thoroughly scan the entire section for location information"
               "2. Look anywhere in the job entry for city or state mentions, including:"
               "   - Next to company name"
               "   - In the job header"
               "   - Within first few lines of the job description"
               "   - Near dates or titles"
               "3. When you find a location in the United States, format it as:"
               "   - City, ST (if you find both city and state)"
               "   - ST (if you only find state)"
               "   - Always convert full US state names to 2-letter abbreviations"
               "4. For international locations, format as:"
               "   - City, Country (if you find both)"
               "   - Country (if you only find country)"
               "   - NEVER append 'NULL' to international locations"
               "5. Strip out any extra information (zip codes, postal codes, etc.)"
               "6. Use NULL only if you truly cannot find any location information in that job entry"
               "IMPORTANT: Be thorough - check the entire job section before deciding there's no location."
},
```

This will properly handle international locations by:
1. Using "City, Country" format for international locations
2. Explicitly preventing the use of NULL as a substitute for state/province
3. Maintaining proper formatting for US locations
"""
    
    logging.info(improved_instructions)
    return improved_instructions

if __name__ == "__main__":
    logging.info("Location fix utility - choose an option:")
    logging.info("1. Fix existing location entries containing NULL")
    logging.info("2. Show improved location prompt instructions")
    logging.info("3. Run both options")
    
    choice = input("Enter your choice (1-3): ")
    
    if choice == "1":
        fix_location_entries()
    elif choice == "2":
        update_location_prompt_instructions()
    elif choice == "3":
        fix_location_entries()
        update_location_prompt_instructions()
    else:
        logging.error("Invalid choice")