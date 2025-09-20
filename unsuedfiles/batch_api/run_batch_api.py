#!/usr/bin/env python3
"""
Simple wrapper script to run the batch API processor
"""

import sys
import os
import argparse

# Ensure the batch_api directory is in the Python path
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)

# Now import from the files directly
from one_step_processor import run_unified_processing, check_and_process_batch, run_continuous_processing

def main():
    parser = argparse.ArgumentParser(description='Run batch API processor')
    parser.add_argument('--submit', action='store_true', help='Submit a new unified batch job')
    parser.add_argument('--check-batch', type=str, help='Check a specific batch job by ID')
    parser.add_argument('--batch-size', type=int, default=50, help='Batch size (default: 50)')
    parser.add_argument('--continuous', action='store_true', help='Run in continuous mode to process batches without manual intervention')
    parser.add_argument('--num-batches', type=int, default=1, help='Number of batches to process in continuous mode (default: 1)')
    parser.add_argument('--check-interval', type=int, default=20, help='Seconds between status checks in continuous mode (default: 20)')
    parser.add_argument('--debug-mode', action='store_true', help='Enable debug file generation')
    parser.add_argument('--debug-limit', type=int, default=20, help='Maximum number of debug files to generate per batch (default: 20)')
    
    args = parser.parse_args()
    
    if args.continuous:
        # Run in continuous mode
        run_continuous_processing(
            batch_size=args.batch_size, 
            num_batches=args.num_batches, 
            check_interval=args.check_interval,
            debug_mode=args.debug_mode,
            debug_limit=args.debug_limit
        )
    elif args.submit:
        # Submit a new unified batch job
        result = run_unified_processing(
            batch_size=args.batch_size,
            debug_mode=args.debug_mode,
            debug_limit=args.debug_limit
        )
        if result:
            print(f"Submitted unified batch job with OpenAI batch ID: {result['openai_batch_id']}")
            print(f"Number of records: {result['request_count']}")
            print(f"Check back after: {result['next_check_time'].strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"Use this command to check status: python run_batch_api.py --check-batch {result['openai_batch_id']}")
    
    elif args.check_batch:
        # Check a specific batch job
        result = check_and_process_batch(args.check_batch)
        if result:
            if result['status'] == 'completed':
                print(f"Batch job {args.check_batch} completed")
                print(f"Processed {result['total_records']} records")
                print(f"Success: {result['success_count']}, Failure: {result['failure_count']}")
                print(f"Estimated cost: ${result['cost_estimates']['total_cost']:.4f}")
                print(f"Saved ${result['cost_estimates']['savings']:.4f} compared to standard API")
                print(f"Cost per record: ${result['cost_estimates']['cost_per_record']:.6f}")
            elif result['status'] == 'failed':
                print(f"Batch job {args.check_batch} failed: {result['message']}")
            else:
                print(f"Batch job {args.check_batch} status: {result['status']}")
                if 'hours_remaining' in result:
                    print(f"Estimated hours remaining: {result['hours_remaining']}")
    
    else:
        parser.print_help()

if __name__ == "__main__":
    main()