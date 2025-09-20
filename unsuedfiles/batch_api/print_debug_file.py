#!/usr/bin/env python3
import json
import sys
import os

def main():
    # Get debug file path from command line or use default
    if len(sys.argv) > 1:
        debug_file = sys.argv[1]
    else:
        # Get most recent debug file
        debug_dir = os.path.dirname(os.path.abspath(__file__)) + "/debug_output"
        debug_files = [f for f in os.listdir(debug_dir) if f.endswith(".json")]
        
        if not debug_files:
            print("No debug files found in", debug_dir)
            return
            
        debug_files.sort(reverse=True)  # Latest first
        debug_file = os.path.join(debug_dir, debug_files[0])
    
    try:
        with open(debug_file, 'r') as f:
            data = json.load(f)
        
        # Add the API response (simulation since we don't save it to debug file)
        # Run another API call to get a sample response
        if 'userid' in data and 'success' in data and data['success']:
            import os
            import time
            from openai import OpenAI
            
            # Load API key from environment
            from dotenv import load_dotenv
            load_dotenv()
            
            api_key = os.getenv('OPENAI_API_KEY')
            client = OpenAI(api_key=api_key)
            
            # Get the resume text
            from db_connection import create_pyodbc_connection
            conn = create_pyodbc_connection()
            cursor = conn.cursor()
            
            userid = data['userid']
            cursor.execute("SELECT markdownResume FROM aicandidate WHERE userid = ?", userid)
            row = cursor.fetchone()
            
            if row and row[0]:
                resume_text = row[0]
                
                # Create the prompt
                from one_step_processor import create_unified_prompt
                messages = create_unified_prompt(resume_text)
                
                # Make API call
                response = client.chat.completions.create(
                    model="gpt-4o-mini",
                    messages=messages,
                    temperature=0,
                    max_tokens=4096
                )
                
                # Extract and print the response text
                completion_text = response.choices[0].message.content
                
                # Print the results
                print("\n=== API RESPONSE TEXT ===\n")
                print(completion_text)
                print("\n=== END API RESPONSE TEXT ===\n")
                
                # Print the parsed data we got
                print("\n=== PARSED DATA FROM DEBUG FILE ===\n")
                for key, value in data['parsed_data'].items():
                    print(f"{key}: {value}")
            else:
                print("No resume found for userid", userid)
        
    except Exception as e:
        print("Error parsing debug file:", str(e))

if __name__ == "__main__":
    main()