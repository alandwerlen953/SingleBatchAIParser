#!/usr/bin/env python3
"""
AI Resume Parser - Main Entry Point

This script provides a command-line interface for running the AI-powered resume parsing system.
It supports both batch processing and single user processing modes.

Usage:
    python main.py                   - Runs in batch mode
    python main.py --userid [ID]     - Processes a single user
    python main.py --help            - Shows help information

The system uses a two-step AI approach with taxonomy enhancement to extract
information from resumes and store it in a SQL Server database.
"""

import os
import sys
import logging
import argparse
import time
from datetime import datetime
from dotenv import load_dotenv

# Configure basic logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"parser_{datetime.now().strftime('%Y%m%d')}.log"),
        logging.StreamHandler(sys.stdout)
    ]
)

def setup_parser():
    """Set up command line argument parser"""
    parser = argparse.ArgumentParser(description='AI Resume Parser')
    parser.add_argument('--userid', type=str, help='Process a single user by ID')
    parser.add_argument('--batch-size', type=int, default=25, 
                       help='Number of resumes to process in a batch (default: 25)')
    parser.add_argument('--workers', type=int, default=4,
                       help='Number of concurrent workers (default: 4)')
    parser.add_argument('--use-batch-api', action='store_true', 
                       help='Use OpenAI batch API for improved efficiency')
    parser.add_argument('--no-batch-api', action='store_false', dest='use_batch_api',
                       help='Disable OpenAI batch API')
    parser.add_argument('--continuous', action='store_true',
                       help='Run continuously, processing new batches as they become available')
    parser.add_argument('--interval', type=int, default=300,
                       help='Interval in seconds between batch runs when in continuous mode (default: 300)')
    
    return parser

def main():
    """Main entry point for the AI Resume Parser"""
    # Load environment variables
    load_dotenv()
    
    # Parse command line arguments
    parser = setup_parser()
    args = parser.parse_args()
    
    # Import modules that need environment variables
    from pythonProject2.two_step_processor_taxonomy import (
        process_single_resume_two_step, 
        run_taxonomy_enhanced_batch,
        BATCH_SIZE, MAX_WORKERS, USE_BATCH_API
    )
    from pythonProject2.resume_utils import get_resume_by_userid
    
    # Override global settings with command line arguments if provided
    if args.batch_size:
        # Using a global because the module uses it - not ideal but works with current structure
        global BATCH_SIZE
        BATCH_SIZE = args.batch_size
        logging.info(f"Setting batch size to {BATCH_SIZE}")
        
    if args.workers:
        global MAX_WORKERS
        MAX_WORKERS = args.workers
        logging.info(f"Setting worker count to {MAX_WORKERS}")
        
    # Use batch API setting
    global USE_BATCH_API
    USE_BATCH_API = args.use_batch_api
    
    try:
        # Single user mode
        if args.userid:
            logging.info(f"Processing single user with ID: {args.userid}")
            
            # Get the resume for this user
            from pythonProject2.process_single_user import process_with_detailed_logging
            
            # Fetch the resume
            resume_data = get_resume_by_userid(args.userid)
            
            if resume_data:
                userid, resume_text = resume_data
                # Process with detailed logging
                result = process_with_detailed_logging(userid, resume_text)
                
                if result.get('success', False):
                    logging.info(f"Successfully processed resume for user {args.userid}")
                else:
                    logging.warning(f"Processing completed with warnings for user {args.userid}")
                    if 'error' in result:
                        logging.error(f"Error: {result['error']}")
            else:
                logging.error(f"No resume found for user ID {args.userid}")
                
        # Batch mode
        else:
            if args.continuous:
                logging.info(f"Starting continuous batch processing (interval: {args.interval}s)")
                
                # Run continuously with the specified interval
                while True:
                    logging.info(f"Starting batch run at {datetime.now()}")
                    run_taxonomy_enhanced_batch()
                    
                    logging.info(f"Batch completed. Waiting {args.interval} seconds until next run...")
                    time.sleep(args.interval)
            else:
                # Run once
                logging.info("Starting batch processing")
                run_taxonomy_enhanced_batch()
                logging.info("Batch processing completed")
    
    except KeyboardInterrupt:
        logging.info("Process interrupted by user")
        sys.exit(0)
    except Exception as e:
        logging.error(f"Fatal error: {str(e)}")
        sys.exit(1)
        
if __name__ == "__main__":
    main()