# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository Overview

SingleBatchAIParser is an AI-powered resume parser that uses OpenAI's GPT models to extract structured information from resumes and store it in a SQL Server database.

The system employs a two-step AI processing approach with taxonomy enhancement for better skill categorization, and supports both batch processing and individual resume analysis.

## Key Commands

### Running the Parser

**Process a batch of resumes:**
```bash
python pythonProject2/main.py
```

**Process a specific resume by user ID:**
```bash
python pythonProject2/main.py --userid 123456
```

**Continuous batch processing with specific settings:**
```bash
python pythonProject2/main.py --batch-size 25 --workers 4 --continuous --interval 300
```

**Process with OpenAI batch API enabled/disabled:**
```bash
python pythonProject2/main.py --use-batch-api
python pythonProject2/main.py --no-batch-api
```

### Testing

**Run date utilities tests:**
```bash
python pythonProject2/test_date_utils.py
```

**Run extraction verification tests:**
```bash
python pythonProject2/extraction_verification_test.py
```

**Run hardware extraction tests:**
```bash
python pythonProject2/hardware_extraction_test.py
```

**Test database connection:**
```bash
python pythonProject2/db_connection.py
```

**Test resume utilities:**
```bash
python pythonProject2/resume_utils.py
```

### Code Quality

**Run linting with Ruff:**
```bash
ruff check pythonProject2/
```

**Run type checking with MyPy:**
```bash
mypy pythonProject2/
```

## Architecture

The system is organized into several key components:

1. **Two-Step Processing Pipeline** (`two_step_processor_taxonomy.py`):
   - Step 1 extracts basic personal info, work history, and industry
   - Step 2 extracts skills, technical details, and calculates experience metrics
   - Uses taxonomy enhancement for better categorization

2. **Taxonomy Enhancement** (`skills_detector.py`):
   - Loads and analyzes skill taxonomies from CSV files
   - Matches resume content against industry-specific skill sets
   - Provides context to the AI for better field extraction

3. **Date Processing** (`date_processor.py`, `date_utils.py`):
   - Parses and validates dates in various formats
   - Calculates tenure and experience metrics
   - Handles special cases like "present" positions

4. **Single User Processing** (`process_single_user.py`):
   - Detailed processing with extensive logging
   - Creates field reports for quality assessment
   - Performs database operations with retry logic

5. **Database Operations** (`db_connection.py`):
   - Provides robust connectivity to SQL Server
   - Implements exponential backoff retry mechanisms
   - Handles connection pooling and transaction management

6. **Utilities** (`resume_utils.py`):
   - Database connection wrapper functions
   - OpenAI API integration
   - Token counting and validation functions

## Execution Flow

1. **Entry Point** (`main.py`):
   - Parses command-line arguments
   - Determines batch or single-user processing mode
   - Configures logging and environment variables

2. **Batch Processing**:
   - Fetches unprocessed resumes from database
   - Processes them concurrently using a worker pool
   - Updates the database with extracted information

3. **Single-User Processing**:
   - Performs detailed step-by-step processing with extensive logging
   - Combines results from multiple AI processing steps
   - Applies date enhancements and field validations

4. **Data Persistence**:
   - Validates fields before database insertion/update
   - Transforms data to match database schema
   - Handles NULL values and date formats

## Development Guidelines

1. **OpenAI Integration**:
   - All OpenAI API calls should use the configuration from `resume_utils.py`
   - Token limits are strictly enforced to prevent API errors
   - The default model is `gpt-4o-mini-2024-07-18` as defined in resume_utils.py

2. **Database Operations**:
   - Use `update_candidate_record_with_retry()` for database updates
   - Include field validation before database operations
   - Handle NULL values appropriately (empty strings for database)
   - Database operations use retry logic with exponential backoff

3. **Date Handling**:
   - Use consistent date format validation via `is_valid_sql_date()`
   - For current positions, always properly handle "Present" indicators
   - Experience calculations should fallback to AI values when date parsing fails
   - Dates must be in YYYY-MM-DD format for SQL Server compatibility

4. **Error Handling**:
   - Follow simple print error or step logging pattern
   - No complex fallback mechanisms, just error messages
   - Emphasize clean, straightforward code for error paths

5. **Taxonomy Integration**:
   - CSV taxonomy files in the Dictionary directory should be maintained
   - Skills taxonomy is loaded at module import time
   - The system references newSkills.csv and projecttypes.csv

## Database Schema

The system uses the `aicandidate` table in the SQL Server database with the following key fields:

- `userid`: Unique identifier for candidates
- Personal info: FirstName, MiddleName, LastName, Email, Phone1, etc.
- Work history: MostRecentCompany, MostRecentStartDate, MostRecentEndDate, etc.
- Skills: Top10Skills, Skill1 through Skill10, PrimarySoftwareLanguage, etc.
- Experience metrics: YearsofExperience, AvgTenure, LengthinUS
- Industry details: PrimaryIndustry, PrimaryCategory, ProjectTypes, etc.

## Configuration

The system uses environment variables loaded from a `.env` file:
- `OPENAI_API_KEY`: Your OpenAI API key
- Database credentials are configured in `db_connection.py` with these defaults:
  - Server: 172.19.115.25
  - Database: BH_Mirror
  - Username: silver
  - Password: ltechmatlen