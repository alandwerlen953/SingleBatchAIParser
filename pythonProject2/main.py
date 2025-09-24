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

# Check for --quiet flag early to suppress non-error logging
if '--quiet' in sys.argv:
    # Set root logger to ERROR level
    logging.getLogger().setLevel(logging.ERROR)
    # Set environment variable so imported modules know to be quiet
    os.environ['QUIET_MODE'] = '1'

try:
    from dotenv import load_dotenv
except ImportError:
    def load_dotenv():
        pass  # Skip if dotenv not available

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
                       help='Suppress all logging output except errors')
    parser.add_argument('--batch-api', action='store_true',
                       help='Use OpenAI Batch API for 50% cost savings (24hr processing)')
    parser.add_argument('--check-batch', type=str,
                       help='Check status of a specific batch job by ID')
    parser.add_argument('--submit-batch', action='store_true',
                       help='Submit a new batch job to OpenAI Batch API')
    parser.add_argument('--monitor-batches', action='store_true',
                       help='Continuously monitor all pending batch jobs')
    parser.add_argument('--check-interval', type=int, default=30,
                       help='Seconds between batch status checks (default: 30)')
    parser.add_argument('--num-batches', type=int, default=1,
                       help='Number of batches to submit (default: 1)')
    parser.add_argument('--streaming', action='store_true',
                       help='Use streaming batch submission (fetch and submit concurrently)')

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
        # Configure basic logging with ERROR level
        logging.basicConfig(
            level=logging.ERROR,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(f"parser_{datetime.now().strftime('%Y%m%d')}.log", encoding='utf-8'),
                logging.StreamHandler(sys.stdout)
            ]
        )
        # Also set all existing loggers to ERROR level
        for logger_name in logging.root.manager.loggerDict:
            logging.getLogger(logger_name).setLevel(logging.ERROR)
    else:
        # Normal logging configuration
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(f"parser_{datetime.now().strftime('%Y%m%d')}.log", encoding='utf-8'),
                logging.StreamHandler(sys.stdout)
            ]
        )
    
    # Import modules that need environment variables
    from resume_utils import get_resume_by_userid

    # Set default values
    DEFAULT_BATCH_SIZE = 25
    DEFAULT_WORKERS = 4
    
    # Import unified processor if needed
    if args.unified:
        logging.info("Using unified single-step processing (token efficient mode)")
        from single_step_processor import (
            process_single_resume_unified,
            run_unified_batch
        )
    
    # Override settings with command line arguments if provided
    batch_size = args.batch_size if args.batch_size else DEFAULT_BATCH_SIZE
    workers = args.workers if args.workers else DEFAULT_WORKERS

    logging.info(f"Using batch size: {batch_size}")
    logging.info(f"Using worker count: {workers}")
    logging.info(f"Batch API setting: {args.use_batch_api}")
    
    try:
        # Handle batch API operations first (separate flow)
        if args.monitor_batches and not args.submit_batch:
            logging.info(f"Starting batch monitoring mode (checking every {args.check_interval} seconds)")
            logging.info("Monitoring all active batches...")
            # TODO: Implement continuous monitoring for all active batches
            logging.error("Batch monitoring is not yet implemented")
            sys.exit(1)

        elif args.check_batch:
            logging.info(f"Checking batch job status: {args.check_batch}")
            from batch_operations import check_and_process_batch
            result = check_and_process_batch(args.check_batch)
            if result:
                if result['status'] == 'completed':
                    logging.info(f"Batch completed: {result['success_count']} success, {result['failure_count']} failed")
                    if 'successful_userids' in result:
                        logging.info(f"Updated UserIDs: {result['successful_userids']}")
                    if 'failed_userids' in result and result['failed_userids']:
                        logging.info(f"Failed UserIDs: {result['failed_userids']}")
                else:
                    logging.info(f"Batch status: {result['status']}")
            sys.exit(0)

        elif args.submit_batch:
            logging.info(f"Submitting {args.num_batches} batch job(s) to OpenAI Batch API")

            batch_size = args.batch_size if args.batch_size else 25
            submitted_batches = []

            # Check if streaming mode is enabled
            if args.streaming:
                logging.info("Using STREAMING batch submission (fetch and submit concurrently)")
                from batch_operations import submit_single_batch_streaming
                from db_connection import get_resume_batch_paginated
                import threading
                import queue
                import concurrent.futures

                # Queue to track submitted batches
                batch_queue = queue.Queue()
                pending_batches = []
                completed_batches = []

                # Monitoring thread function
                def monitor_batches():
                    from batch_operations import check_and_process_batch
                    while True:
                        # Check if we should stop
                        if batch_queue.empty() and len(pending_batches) == 0:
                            time.sleep(5)  # Wait a bit to see if more batches come
                            if batch_queue.empty() and len(pending_batches) == 0:
                                break

                        # Get new batches from queue
                        while not batch_queue.empty():
                            try:
                                batch_info = batch_queue.get_nowait()
                                pending_batches.append(batch_info['batch_id'])
                                logging.info(f"Monitor: Added batch {batch_info['batch_id']} to monitoring")
                            except queue.Empty:
                                break

                        # Check status of pending batches
                        for batch_id in pending_batches[:]:
                            result = check_and_process_batch(batch_id)
                            if result and result.get('status') in ['completed', 'failed', 'expired']:
                                logging.info(f"Monitor: Batch {batch_id} finished with status: {result['status']}")
                                pending_batches.remove(batch_id)
                                completed_batches.append(batch_id)

                        time.sleep(args.check_interval)

                # Start monitoring thread
                monitor_thread = threading.Thread(target=monitor_batches, daemon=True)
                monitor_thread.start()

                # Submit batches in streaming fashion
                # Always use offset 0 because processed records are excluded from query
                for i in range(args.num_batches):
                    logging.info(f"\nStreaming batch {i+1} of {args.num_batches}...")

                    # Fetch next batch of resumes (always at offset 0 since processed ones are excluded)
                    resume_batch = get_resume_batch_paginated(batch_size=batch_size, offset=0)

                    if not resume_batch:
                        logging.info(f"No more resumes found")
                        break

                    # Submit batch immediately
                    result = submit_single_batch_streaming(resume_batch, workers=args.workers)

                    if result:
                        submitted_batches.append(result['batch_id'])
                        batch_queue.put(result)
                        logging.info(f"Batch {i+1} submitted: {result['batch_id']}")
                        logging.info(f"  - Submitted {result['resume_count']} resumes")
                    else:
                        logging.error(f"Failed to submit batch {i+1}")

                # Wait for monitoring to complete
                logging.info("\nAll batches submitted. Waiting for processing to complete...")
                monitor_thread.join()
                logging.info(f"All {len(completed_batches)} batches have been processed!")

            else:
                # Original non-streaming mode
                logging.info("Using standard (non-streaming) batch submission")
                from batch_operations import submit_resume_batch

                # Submit multiple batches
                for i in range(args.num_batches):
                    logging.info(f"\nSubmitting batch {i+1} of {args.num_batches}...")
                    result = submit_resume_batch(batch_size=batch_size)

                    if result:
                        submitted_batches.append(result['batch_id'])
                        logging.info(f"Batch {i+1} submitted: {result['batch_id']}")
                        logging.info(f"  - Submitted {result['resume_count']} resumes")

                        # Add small delay between batch submissions to avoid rate limits
                        if i < args.num_batches - 1:
                            time.sleep(2)
                    else:
                        logging.error(f"Failed to submit batch {i+1}")

            # Summary
            if submitted_batches:
                logging.info(f"\n{'='*60}")
                logging.info(f"Successfully submitted {len(submitted_batches)} batches:")
                for batch_id in submitted_batches:
                    logging.info(f"  - {batch_id}")
                logging.info(f"\nCheck all batches with: python main.py --monitor-batches")
                logging.info(f"Check specific batch with: python main.py --check-batch BATCH_ID")

                # Start monitoring if requested
                if args.monitor_batches:
                    logging.info(f"\nStarting batch monitoring (checking every {args.check_interval} seconds)...")
                    from batch_operations import check_and_process_batch

                    # Monitor all submitted batches
                    pending_batches = submitted_batches.copy()
                    while pending_batches:
                        for batch_id in pending_batches[:]:  # Use slice to avoid modification during iteration
                            result = check_and_process_batch(batch_id)
                            if result and result.get('status') in ['completed', 'failed', 'expired']:
                                logging.info(f"Batch {batch_id} finished with status: {result['status']}")
                                pending_batches.remove(batch_id)

                        if pending_batches:
                            logging.info(f"Still monitoring {len(pending_batches)} batches...")
                            time.sleep(args.check_interval)

                    logging.info("All batches have been processed!")
            sys.exit(0)

        # Single user mode
        elif args.userid:
            logging.info(f"Processing single user with ID: {args.userid}")

            # Fetch the resume
            resume_data = get_resume_by_userid(args.userid)

            if resume_data:
                userid, resume_text = resume_data

                # Use unified processor if --unified flag is set
                if args.unified:
                    logging.info(f"Using unified single-step processor for user {args.userid}")
                    from single_step_processor import process_single_resume_unified
                    result = process_single_resume_unified((userid, resume_text))
                else:
                    logging.info(f"Using two-step processor for user {args.userid}")
                    from process_single_user import process_with_detailed_logging
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