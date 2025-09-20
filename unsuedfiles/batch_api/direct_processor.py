#!/usr/bin/env python3
"""
Direct processor for resume analysis with OpenAI synchronous API

This module replaces the batch API processing with direct API calls,
maintaining similar functionality while improving error handling and providing
real-time feedback on processing status.
"""

import os
import sys
import time
import json
import logging
import argparse
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Any, Optional
import concurrent.futures
# from tqdm import tqdm - removed dependency

# Add parent directory to path so we can import from the main project
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)

# Import our direct API utilities
from batch_api.direct_api_utils import (
    DEFAULT_MODEL,
    create_openai_client,
    get_resume_batch,
    process_resume_with_direct_api,
    update_candidate_record_with_retry,
    process_resumes_in_parallel
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(os.path.dirname(__file__), "direct_processor.log")),
        logging.StreamHandler()
    ]
)

def process_resume_batch(batch_size=50, max_workers=5, debug_mode=False, debug_limit=20):
    """
    Process a batch of resumes with direct API calls
    
    Args:
        batch_size: Number of resumes to process
        max_workers: Maximum number of concurrent workers for parallel processing
        debug_mode: Whether to generate debug files
        debug_limit: Maximum number of debug files to generate
        
    Returns:
        Dictionary with processing results and metrics
    """
    try:
        # Fetch a batch of resumes from the database
        batch_start_time = time.time()
        resume_batch = get_resume_batch(batch_size)
        
        if not resume_batch:
            logging.warning("No resumes found to process")
            return {
                "status": "completed",
                "message": "No resumes found to process",
                "total_records": 0,
                "success_count": 0,
                "failure_count": 0,
                "processing_time_seconds": time.time() - batch_start_time,
                "cost_estimates": {
                    "total_cost": 0,
                    "savings": 0,
                    "cost_per_record": 0
                }
            }
        
        # Log batch information
        batch_id = datetime.now().strftime("%Y%m%d%H%M%S")
        logging.info(f"Starting direct processing of {len(resume_batch)} resumes (Batch ID: {batch_id})")
        
        # Process resumes in parallel
        logging.info(f"Using {max_workers} parallel workers")
        results = process_resumes_in_parallel(
            resume_batch=resume_batch,
            max_workers=max_workers,
            model=DEFAULT_MODEL,
            temperature=0
        )
        
        # Generate debug files if requested
        if debug_mode:
            debug_count = 0
            debug_dir = os.path.join(os.path.dirname(__file__), "debug_output")
            os.makedirs(debug_dir, exist_ok=True)
            
            for result in results["results"]:
                if debug_count >= debug_limit:
                    break
                    
                if result["success"]:
                    userid = result["userid"]
                    debug_file = os.path.join(debug_dir, f"debug_{batch_id}_{userid}.json")
                    
                    with open(debug_file, 'w') as f:
                        json.dump(result, f, indent=2, default=str)
                        
                    logging.info(f"Created debug file for UserID {userid}: {debug_file}")
                    debug_count += 1
        
        # Calculate overall metrics for reporting
        total_records = len(resume_batch)
        success_count = results["metrics"]["successful_count"]
        failure_count = results["metrics"]["failed_count"]
        total_cost = results["metrics"]["total_cost"]
        processing_time = results["metrics"]["processing_time_seconds"]
        
        # Calculate savings compared to standard API (non-batch) - approximately 50% cheaper with batch
        standard_api_cost = total_cost * 2  # Approximate comparison
        savings = standard_api_cost - total_cost
        
        # Calculate cost per record
        cost_per_record = total_cost / total_records if total_records > 0 else 0
        
        return {
            "status": "completed",
            "batch_id": batch_id,
            "total_records": total_records,
            "success_count": success_count,
            "failure_count": failure_count,
            "processing_time_seconds": processing_time,
            "cost_estimates": {
                "total_cost": total_cost,
                "savings": savings,  # This is just for compatibility with the batch API reporting
                "cost_per_record": cost_per_record
            }
        }
        
    except Exception as e:
        logging.error(f"Error processing resume batch: {str(e)}")
        return {
            "status": "failed",
            "message": str(e),
            "total_records": 0,
            "success_count": 0,
            "failure_count": 0,
            "processing_time_seconds": 0,
            "cost_estimates": {
                "total_cost": 0,
                "savings": 0,
                "cost_per_record": 0
            }
        }

def run_continuous_processing(batch_size=50, num_batches=1, check_interval=20, max_workers=5, debug_mode=False, debug_limit=20):
    """
    Run continuous processing of multiple batches
    
    Args:
        batch_size: Number of resumes to process per batch
        num_batches: Number of batches to process (0 for unlimited)
        check_interval: Seconds to wait between batches
        max_workers: Maximum number of concurrent workers for parallel processing
        debug_mode: Whether to generate debug files
        debug_limit: Maximum number of debug files to generate
    """
    batch_count = 0
    total_records = 0
    total_success = 0
    total_failure = 0
    total_cost = 0
    unlimited = (num_batches == 0)
    
    print(f"\nStarting continuous processing with batch size {batch_size}")
    print(f"Using {max_workers} parallel workers")
    print(f"Will process {num_batches} batches" if not unlimited else "Will process unlimited batches")
    print(f"Waiting {check_interval} seconds between batches\n")
    
    try:
        start_time = time.time()
        
        # Process until reaching num_batches or interrupted
        while unlimited or batch_count < num_batches:
            batch_start = time.time()
            print(f"\n--- Starting Batch {batch_count + 1} at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ---")
            
            # Process this batch
            result = process_resume_batch(
                batch_size=batch_size,
                max_workers=max_workers,
                debug_mode=debug_mode,
                debug_limit=debug_limit
            )
            
            batch_count += 1
            total_records += result["total_records"]
            total_success += result["success_count"]
            total_failure += result["failure_count"]
            total_cost += result["cost_estimates"]["total_cost"]
            
            # Print batch results
            if result["status"] == "completed":
                if result["total_records"] > 0:
                    print(f"Processed {result['total_records']} records")
                    print(f"Success: {result['success_count']}, Failure: {result['failure_count']}")
                    print(f"Batch cost: ${result['cost_estimates']['total_cost']:.4f}")
                    print(f"Processing time: {result['processing_time_seconds']:.2f} seconds")
                else:
                    print("No records found to process in this batch")
            else:
                print(f"Batch failed: {result.get('message', 'Unknown error')}")
            
            # Print overall stats
            elapsed = time.time() - start_time
            print(f"\n=== Overall Statistics ===")
            print(f"Batches processed: {batch_count}")
            print(f"Total records: {total_records}")
            print(f"Total success: {total_success}")
            print(f"Total failure: {total_failure}")
            print(f"Total cost: ${total_cost:.4f}")
            print(f"Total time: {elapsed:.2f} seconds")
            print(f"Records per second: {total_records / elapsed:.2f}" if elapsed > 0 else "Records per second: N/A")
            
            # Check if we should continue
            if not unlimited and batch_count >= num_batches:
                print("\nReached target number of batches, stopping")
                break
                
            # If we processed records in this batch, wait before the next one
            if result["total_records"] > 0:
                next_time = datetime.now() + timedelta(seconds=check_interval)
                print(f"\nWaiting {check_interval} seconds before starting next batch")
                print(f"Next batch will start at approximately {next_time.strftime('%Y-%m-%d %H:%M:%S')}")
                time.sleep(check_interval)
            else:
                # If no records were found, wait longer to avoid hammering the database
                empty_wait = check_interval * 3
                next_time = datetime.now() + timedelta(seconds=empty_wait)
                print(f"\nNo records found, waiting {empty_wait} seconds before checking again")
                print(f"Next check at approximately {next_time.strftime('%Y-%m-%d %H:%M:%S')}")
                time.sleep(empty_wait)
    
    except KeyboardInterrupt:
        print("\nContinuous processing interrupted by user")
        print(f"Processed {batch_count} batches with {total_records} total records")
        print(f"Total cost: ${total_cost:.4f}")
    except Exception as e:
        print(f"\nContinuous processing stopped due to error: {str(e)}")
        logging.error(f"Error in continuous processing: {str(e)}")
        
    finally:
        # Print final summary
        elapsed = time.time() - start_time
        print(f"\n=== Final Summary ===")
        print(f"Batches processed: {batch_count}")
        print(f"Total records: {total_records}")
        print(f"Total success: {total_success}")
        print(f"Total failure: {total_failure}")
        print(f"Total cost: ${total_cost:.4f}")
        print(f"Total time: {elapsed:.2f} seconds")
        print(f"Average cost per record: ${total_cost / total_records:.6f}" if total_records > 0 else "Average cost per record: N/A")

def process_specific_resume(userid, debug_mode=False):
    """
    Process a specific resume by user ID
    
    Args:
        userid: The user ID to process
        
    Returns:
        Dictionary with processing results
    """
    try:
        # Connect to the database to get the resume
        from db_connection import create_pyodbc_connection
        conn = create_pyodbc_connection()
        cursor = conn.cursor()
        
        # Get the resume text
        query = "SELECT markdownResume FROM dbo.aicandidate WITH (NOLOCK) WHERE userid = ?"
        cursor.execute(query, userid)
        row = cursor.fetchone()
        
        if not row or not row[0]:
            logging.error(f"No resume found for UserID {userid}")
            return {
                "status": "failed",
                "message": f"No resume found for UserID {userid}"
            }
        
        resume_text = row[0]
        cursor.close()
        conn.close()
        
        # Process the resume
        print(f"Processing resume for UserID {userid}...")
        result = process_resume_with_direct_api(userid, resume_text, save_raw_response=debug_mode)
        
        if result["success"]:
            # Update the database with parsed data
            update_success = update_candidate_record_with_retry(userid, result["parsed_data"])
            
            if update_success:
                print(f"Successfully processed and updated UserID {userid}")
                return {
                    "status": "completed",
                    "userid": userid,
                    "cost": result["metrics"]["cost"]["total_cost"] if "metrics" in result and "cost" in result["metrics"] else 0,
                    "processing_time_seconds": result["metrics"]["processing_time_seconds"] if "metrics" in result else 0
                }
            else:
                print(f"Processing succeeded but database update failed for UserID {userid}")
                return {
                    "status": "partial",
                    "message": "Processing succeeded but database update failed",
                    "userid": userid
                }
        else:
            error = result.get("error", "Unknown error")
            print(f"Processing failed for UserID {userid}: {error}")
            return {
                "status": "failed",
                "message": error,
                "userid": userid
            }
            
    except Exception as e:
        logging.error(f"Error processing specific resume {userid}: {str(e)}")
        return {
            "status": "failed",
            "message": str(e),
            "userid": userid
        }

def main():
    """Main entry point with command-line argument parsing"""
    parser = argparse.ArgumentParser(description='Run direct API processor for resume analysis')
    
    # Add arguments similar to the batch processor for compatibility
    parser.add_argument('--batch', action='store_true', help='Process a batch of resumes')
    parser.add_argument('--process-user', type=str, help='Process a specific user by ID')
    parser.add_argument('--batch-size', type=int, default=50, help='Batch size (default: 50)')
    parser.add_argument('--workers', type=int, default=5, help='Number of parallel workers (default: 5)')
    parser.add_argument('--continuous', action='store_true', help='Run in continuous mode to process batches without manual intervention')
    parser.add_argument('--num-batches', type=int, default=1, help='Number of batches to process in continuous mode (default: 1, use 0 for unlimited)')
    parser.add_argument('--check-interval', type=int, default=20, help='Seconds between batches in continuous mode (default: 20)')
    parser.add_argument('--debug-mode', action='store_true', help='Enable debug file generation')
    parser.add_argument('--debug-limit', type=int, default=20, help='Maximum number of debug files to generate per batch (default: 20)')
    
    args = parser.parse_args()
    
    if args.continuous:
        # Run in continuous mode
        run_continuous_processing(
            batch_size=args.batch_size,
            num_batches=args.num_batches,
            check_interval=args.check_interval,
            max_workers=args.workers,
            debug_mode=args.debug_mode,
            debug_limit=args.debug_limit
        )
    elif args.batch:
        # Process a single batch
        result = process_resume_batch(
            batch_size=args.batch_size,
            max_workers=args.workers,
            debug_mode=args.debug_mode,
            debug_limit=args.debug_limit
        )
        
        if result["status"] == "completed":
            if result["total_records"] > 0:
                print(f"Processed {result['total_records']} records")
                print(f"Success: {result['success_count']}, Failure: {result['failure_count']}")
                print(f"Estimated cost: ${result['cost_estimates']['total_cost']:.4f}")
                print(f"Cost per record: ${result['cost_estimates']['cost_per_record']:.6f}")
            else:
                print("No records found to process")
        else:
            print(f"Batch processing failed: {result.get('message', 'Unknown error')}")
            
    elif args.process_user:
        # Process a specific user
        result = process_specific_resume(args.process_user, debug_mode=args.debug_mode)
        
        if result["status"] == "completed":
            print(f"Successfully processed UserID {result['userid']}")
            if "cost" in result:
                print(f"Cost: ${result['cost']:.4f}")
            if "processing_time_seconds" in result:
                print(f"Processing time: {result['processing_time_seconds']:.2f} seconds")
        else:
            print(f"Processing failed for UserID {result['userid']}: {result.get('message', 'Unknown error')}")
            
    else:
        parser.print_help()

if __name__ == "__main__":
    main()