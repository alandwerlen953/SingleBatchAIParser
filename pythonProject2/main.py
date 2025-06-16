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

# Don't configure logging here - will be done after parsing args

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
    parser.add_argument('--unified', action='store_true',
                       help='Use unified single-step processing (more token efficient)')
    parser.add_argument('--quiet', action='store_true',
                       help='Suppress all logging output')
    
    return parser

def main():
    """Main entry point for the AI Resume Parser"""
    # Load environment variables
    load_dotenv()
    
    # Parse command line arguments
    parser = setup_parser()
    args = parser.parse_args()
    
    # Configure logging based on --quiet flag
    if args.quiet:
        # Set logging to CRITICAL to suppress all output
        logging.basicConfig(
            level=logging.CRITICAL,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
    else:
        # Normal logging configuration
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(f"parser_{datetime.now().strftime('%Y%m%d')}.log"),
                logging.StreamHandler(sys.stdout)
            ]
        )
    
    # Import modules that need environment variables
    from two_step_processor_taxonomy import (
        process_single_resume_two_step, 
        run_taxonomy_enhanced_batch,
        BATCH_SIZE, MAX_WORKERS, USE_BATCH_API
    )
    from resume_utils import get_resume_by_userid
    
    # Import values first
    from two_step_processor_taxonomy import BATCH_SIZE as DEFAULT_BATCH_SIZE
    from two_step_processor_taxonomy import MAX_WORKERS as DEFAULT_WORKERS
    from two_step_processor_taxonomy import USE_BATCH_API as DEFAULT_USE_BATCH_API
    
    # Set module-level variables
    import two_step_processor_taxonomy
    
    # Import unified processor if needed
    if args.unified:
        logging.info("Using unified single-step processing (token efficient mode)")
        from single_step_processor import (
            process_single_resume_unified,
            run_unified_batch
        )
    
    # Override settings with command line arguments if provided
    if args.batch_size:
        two_step_processor_taxonomy.BATCH_SIZE = args.batch_size
        logging.info(f"Setting batch size to {args.batch_size}")
    else:
        two_step_processor_taxonomy.BATCH_SIZE = DEFAULT_BATCH_SIZE
        
    if args.workers:
        two_step_processor_taxonomy.MAX_WORKERS = args.workers
        logging.info(f"Setting worker count to {args.workers}")
    else:
        two_step_processor_taxonomy.MAX_WORKERS = DEFAULT_WORKERS
        
    # Use batch API setting
    two_step_processor_taxonomy.USE_BATCH_API = args.use_batch_api
    logging.info(f"Batch API setting: {args.use_batch_api}")
    
    try:
        # Single user mode
        if args.userid:
            logging.info(f"Processing single user with ID: {args.userid}")
            
            # Get the resume for this user
            from process_single_user import process_with_detailed_logging
            
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
            # Choose the processing function based on the unified flag
            batch_function = run_unified_batch if args.unified else run_taxonomy_enhanced_batch
            processor_type = "unified single-step" if args.unified else "two-step"
            
            if args.continuous:
                logging.info(f"Starting continuous {processor_type} batch processing (interval: {args.interval}s)")
                
                # Run continuously with the specified interval
                while True:
                    logging.info(f"Starting {processor_type} batch run at {datetime.now()}")
                    batch_function()
                    
                    logging.info(f"Batch completed. Waiting {args.interval} seconds until next run...")
                    time.sleep(args.interval)
            else:
                # Run once
                logging.info(f"Starting {processor_type} batch processing")
                batch_function()
                logging.info(f"Batch processing completed")
    
    except KeyboardInterrupt:
        logging.info("Process interrupted by user")
        sys.exit(0)
    except Exception as e:
        logging.error(f"Fatal error: {str(e)}")
        sys.exit(1)
        
if __name__ == "__main__":
    main()