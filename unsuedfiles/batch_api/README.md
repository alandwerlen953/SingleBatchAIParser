# Unified Batch API Resume Processor

This module implements resume processing using OpenAI's Batch API, offering a 50% cost reduction compared to the standard API by allowing up to 24 hours for processing. The new unified approach processes all resume information in a single step.

## Overview

The Batch API processor extracts comprehensive information from resumes in a single API call, including:
- Personal information (name, contact details, location)
- Education (degrees, certifications)
- Work history (up to 7 positions with companies, dates, locations)
- Skills and career summary
- Technical skills (languages, software, hardware, specializations)

## Usage

### 1. Submit a new batch job

```bash
python one_step_processor.py --submit --batch-size 500
```

This will:
- Query the database for up to 500 unprocessed resumes
- Create a unified batch request file
- Upload the file to OpenAI's Batch API
- Submit the batch job for processing

### 2. Check and process a completed batch

```bash
python one_step_processor.py --check-batch <batch_id>
```

This will:
- Check the status of the specified batch
- Download and process results if the batch is complete
- Update the database with extracted information
- Mark records as processed (LastProcessed field)

### 3. Run in continuous mode (NEW)

```bash
python one_step_processor.py --continuous --num-batches 5 --batch-size 200 --check-interval 1800
```

This will:
- Submit the first batch automatically
- Check status every 30 minutes (1800 seconds)
- Process completed batches and update the database
- Submit the next batch automatically
- Continue until all 5 batches are processed

## Detailed Processing Steps

The batch processing system follows these steps in sequence:

1. **Database Query**: 
   - Connects to SQL Server database
   - Retrieves unprocessed records where LastProcessed IS NULL
   - Extracts user IDs and resume text

2. **Prompt Generation**:
   - Creates a unified extraction prompt for each resume
   - Formats messages for OpenAI's chat completion API
   - Applies token count limits and truncation if needed

3. **Batch File Creation**:
   - Generates a JSONL file with one request per line
   - Each request includes a custom ID linking to the original user ID
   - Configures model parameters (gpt-4o-mini-2024-07-18, temp=0.2)

4. **API Submission**:
   - Uploads the JSONL file to OpenAI
   - Submits a batch job with 24-hour completion window
   - Records the batch ID and input file ID

5. **Status Tracking**:
   - Updates the aicandidateBatchStatus table
   - Records status, timestamps, and file IDs
   - (Continuous mode) Periodically checks for completion

6. **Results Processing**:
   - Downloads the output file when batch completes
   - Parses JSON responses for each resume
   - Extracts structured data from AI completions

7. **Date Standardization**:
   - Processes date fields into consistent YYYY-MM format
   - Handles "present" and "current" values appropriately

8. **Database Update**:
   - Maps extracted data to database fields
   - Creates parameterized SQL queries
   - Updates records with retry logic for deadlocks
   - Marks LastProcessed with current timestamp

9. **Cost Analysis**:
   - Estimates token usage for input and output
   - Calculates costs with 50% batch discount
   - Reports savings compared to standard API

## File Structure

- `one_step_processor.py`: Main unified processor with continuous mode
- `batch_api_utils.py`: Self-contained utilities for batch processing
- `test_config.py`: Configuration verification tool
- `run_batch.sh`: Helper script for command-line usage
- `README.md` & `CLAUDE.md`: Documentation

## Key Benefits

- **50% cost reduction** compared to standard API calls
- **Single-step processing** for efficiency (down from two steps)
- **Continuous operation mode** for automation
- **Self-contained code** with no external dependencies
- Support for processing up to **50,000 records per batch**
- **Explicit transaction management** for database reliability

## Requirements

- OpenAI API key with access to the Batch API
- SQL Server with `aicandidate` table
- Python 3.8+ with required packages: pyodbc, tiktoken, openai, dotenv

## Configuration

Set the following environment variables:
- `OPENAI_API_KEY`: Your OpenAI API key
- `server_ip`: Your SQL Server IP address (default: 172.19.115.25)
- `database`: Your database name (default: BH_Mirror)
- `username`: SQL Server username (default: silver)
- `password`: SQL Server password (default: ltechmatlen)

## Production Setup

For production use with continuous mode:

```
# Process 10 batches of 500 records each, checking every hour
python one_step_processor.py --continuous --num-batches 10 --batch-size 500 --check-interval 3600
```

## Error Handling

The implementation includes comprehensive error handling:
- Detailed logging in batch_api.log
- Database deadlock retry with exponential backoff
- Token limit management for large resumes
- JSON parsing error handling
- Connection error recovery