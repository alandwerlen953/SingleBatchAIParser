# SingleBatchAIParser

An AI-powered resume parsing system that uses OpenAI's GPT models to extract structured information from resumes. This system supports both batch processing and individual resume processing with detailed analysis.

## Features

- **Two-Step AI Processing**: Uses a sequential approach to extract information with higher accuracy
- **Taxonomy Enhancement**: Leverages industry taxonomies for better skill categorization
- **Batch Processing**: Efficiently processes multiple resumes in batches
- **Field Validation**: Validates and normalizes extracted fields
- **Database Integration**: Stores results in SQL Server
- **Detailed Logging**: Tracks processing steps and results

## Requirements

- Python 3.8+
- OpenAI API key
- SQL Server database
- ODBC Driver 17 for SQL Server

## Installation

1. Clone the repository:
   ```
   git clone https://github.com/alandwerlen953/SingleBatchAIParser.git
   cd SingleBatchAIParser
   ```

2. Install dependencies:
   ```
   pip install -r requirements.txt
   ```

3. Create a `.env` file with your configuration:
   ```
   OPENAI_API_KEY=your_openai_api_key
   ```

## Usage

### Batch Processing

To process resumes in batches (default mode):
```
python main.py
```

Options:
- `--batch-size 25`: Set the number of resumes to process in each batch
- `--workers 4`: Set the number of concurrent workers
- `--use-batch-api`: Use OpenAI's batch API for improved efficiency
- `--no-batch-api`: Disable OpenAI's batch API
- `--continuous`: Run continuously, processing new batches as they become available
- `--interval 300`: Set the interval in seconds between batch runs in continuous mode

### Single User Processing

To process a single resume:
```
python main.py --userid 123456
```

## Structure

- `main.py`: Entry point script
- `pythonProject2/`: Main code directory
  - `two_step_processor_taxonomy.py`: Main processing logic
  - `two_step_prompts_taxonomy.py`: AI prompting templates
  - `resume_utils.py`: Database and utility functions
  - `date_processor.py`: Enhanced date processing
  - `date_utils.py`: Date handling utilities
  - `skills_detector.py`: Skills taxonomy integration
  - `process_single_user.py`: Detailed single user processing
  - `Dictionary/`: Taxonomy CSV files

## Field Processing

The system extracts the following information:
- Personal information (name, contact, location)
- Work history (companies, positions, dates, locations)
- Education (degrees, certifications)
- Technical skills (programming languages, tools, technologies)
- Industry classification
- Experience metrics (years of experience, tenure)

## License

This project is proprietary software.

## Acknowledgments

This project utilizes OpenAI's GPT models for natural language processing.