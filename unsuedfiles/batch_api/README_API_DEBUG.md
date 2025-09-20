# API Response Debugging Tools

This directory contains tools for debugging and analyzing raw API responses from the OpenAI API used in the resume parsing system.

## Overview

The resume parsing system uses OpenAI's API to extract structured information from resumes. These tools help debug and analyze the raw responses to understand how the model is interpreting the information and to identify potential issues.

## Tools Included

### 1. `test_user_response.py`

This script retrieves a resume for a specific user ID from the database and sends it to the OpenAI API using the same prompt structure as the main application. It then saves the raw response to a JSON file for further analysis.

**Usage:**
```bash
python test_user_response.py <userid> [--model MODEL]
```

**Example:**
```bash
python test_user_response.py 1234567 --model gpt-4o-mini
```

### 2. `parse_response.py`

This script takes a raw API response saved by `test_user_response.py` and parses it into a structured format matching the database schema. It analyzes the extraction quality, identifies missing fields, and provides statistics on the extraction process.

**Usage:**
```bash
python parse_response.py <response_file.json>
```

**Example:**
```bash
python parse_response.py debug_output/user_response_1234567_20250521123456.json
```

### 3. `analyze_user_response.sh`

This shell script combines the above tools to streamline the process of retrieving and analyzing a response for a specific user ID.

**Usage:**
```bash
./analyze_user_response.sh <userid> [model]
```

**Example:**
```bash
./analyze_user_response.sh 1234567 gpt-4o-mini
```

## Output

All tools save their output to the `debug_output/` directory, which includes:

1. The raw API response JSON files (`user_response_*.json`)
2. Processed analysis results (`analysis_*.json`)
3. Log files documenting the execution process

## Understanding the Results

The analysis output includes:

- **Field Extraction Analysis**: Statistics on how many fields were successfully extracted
- **Category-wise Extraction Rates**: How well each section (personal info, work history, etc.) was parsed
- **Structured Data**: The fully parsed and structured resume data
- **Raw Extraction**: The direct field-value mapping from the API response

## Troubleshooting

Common issues:

1. **Database Connection Issues**: Ensure the database credentials in the script are correct and the database is accessible.
2. **API Key Missing**: Make sure the `OPENAI_API_KEY` is set in your environment or `.env` file.
3. **User ID Not Found**: Verify the user ID exists in the database.
4. **Response Parsing Errors**: If fields are extracted incorrectly, check the raw response to understand the model's output format.

## Processing Status

If certain fields are consistently missing across multiple users, consider:

1. Reviewing the prompt structure in `one_step_processor.py`
2. Checking if the field mapping in `parse_response.py` matches the actual response format
3. Analyzing the resume text to see if the information is present but not being recognized

For any persistent issues, consider adjusting the prompts or field extraction methods in the main application.