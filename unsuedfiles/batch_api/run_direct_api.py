#!/usr/bin/env python3
"""
Simple wrapper script to run the direct API processor
"""

import sys
import os
import argparse

# Ensure the batch_api directory is in the Python path
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)

# Import from the direct processor
from direct_processor import process_resume_batch, process_specific_resume, run_continuous_processing

def main():
    parser = argparse.ArgumentParser(description='Run direct API processor')
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
        print(f"Running continuous processing with batch size {args.batch_size}, {args.workers} workers")
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
        print(f"Processing batch of size {args.batch_size} with {args.workers} workers")
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
        print(f"Processing specific user: {args.process_user}")
        result = process_specific_resume(args.process_user)
        
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