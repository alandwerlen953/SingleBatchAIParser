# CLAUDE.md - Batch API Processing Guidelines

## Commands

### One-Step Processing
- **Submit new batch job**: `python batch_api/one_step_processor.py --submit`
- **Submit custom size batch**: `python batch_api/one_step_processor.py --submit --batch-size 100`
- **Check batch status**: `python batch_api/one_step_processor.py --check-batch <batch_id>`
- **Run in continuous mode**: `python batch_api/one_step_processor.py --continuous --num-batches 3 --batch-size 200 --check-interval 1800`

### Utilities
- **Run shell script**: `./batch_api/run_batch.sh submit` or `./batch_api/run_batch.sh check`
- **Run test config**: `python batch_api/test_config.py`
- **Run single test**: `python -m pytest batch_api/test_*.py::test_function_name -v`
- **Lint code**: `ruff check batch_api/*.py` or `flake8 batch_api/*.py`
- **Type check**: `mypy --strict batch_api/*.py`

## Code Style Guidelines
- **Imports**: Group standard → third-party → project-specific; sort alphabetically
- **Formatting**: PEP 8, 4 spaces indentation, 100 char line limit
- **Type Annotations**: Use type hints for all parameters/returns; Optional[] for nullable
- **Documentation**: Docstrings for all modules, classes, and functions with triple quotes
- **Naming**: snake_case (functions/vars), CamelCase (classes), UPPER_CASE (constants)
- **Error Handling**: Use specific exceptions with proper resource cleanup in finally blocks
- **Logging**: Maintain detailed logging with appropriate log levels
- **SQL Queries**: Use parameterized queries with proper escaping for user inputs
- **API Calls**: Add retry logic with exponential backoff for external API calls
- **Token Usage**: Track token usage for cost monitoring and optimization
- **Database Updates**: Always use explicit transaction management with commits

## Project Structure
- **one_step_processor.py**: Unified processor that handles extraction in a single step
- **batch_api_utils.py**: Self-contained utilities for batch processing
- **test_config.py**: Configuration verification tool
- **run_batch.sh**: Helper script for command-line usage
- **README.md**: Documentation for users

## Database Connection
The batch system connects to SQL Server database with the following parameters:
- Server: `172.19.115.25`
- Database: `BH_Mirror`
- Table: `dbo.aicandidate`
- Status Table: `aicandidateBatchStatus`

## Processing Steps (One-Step Approach)
1. **Data Retrieval**: The system queries the SQL database for unprocessed resumes (LastProcessed IS NULL)
2. **Batch Creation**: Generates a JSONL file with individual API requests for each resume
3. **File Upload**: Uploads the JSONL file to OpenAI's batch API service
4. **Batch Submission**: Submits the batch job with a 24-hour completion window for 50% discount
5. **Status Tracking**: Records batch information in the database for monitoring
6. **Result Processing**: When completed, downloads and processes the response data
7. **Data Extraction**: Parses the API responses to extract structured data from resumes
8. **Database Update**: Updates the database with extracted information and marks records as processed

## Continuous Mode Operation
The new continuous mode allows fully automated operation:
1. Submits the first batch to OpenAI
2. Periodically checks status at the specified interval
3. When a batch completes, processes results and updates the database
4. Automatically submits the next batch if more are requested
5. Continues until all requested batches are processed

## Important Notes
- The batch API system uses OpenAI's 24-hour batch processing, offering a 50% discount over standard API calls
- All code is now self-contained without dependencies on the parent project
- The unified processor combines extraction into a single API call for efficiency
- Always explicitly commit database transactions to ensure updates are saved
- SQL updates use the format: `UPDATE dbo.aicandidate SET field = ? WHERE userid = ?`
- Error handling includes retry logic for database connections and deadlock management