#!/bin/bash

# Run Batch API Processor
# Usage: ./run_batch.sh [submit|check] [batch_size]

# Get directory where the script is located
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PARENT_DIR="$(dirname "$DIR")"

# Make sure we're in the correct directory
cd "$PARENT_DIR"

ACTION="$1"
BATCH_SIZE="$2"

if [ "$ACTION" == "submit" ]; then
    if [ -n "$BATCH_SIZE" ]; then
        echo "Submitting batch with size: $BATCH_SIZE"
        python batch_api/processor.py --submit --batch-size "$BATCH_SIZE"
    else
        echo "Submitting batch with default size"
        python batch_api/processor.py --submit
    fi
elif [ "$ACTION" == "check" ]; then
    echo "Checking for completed batches"
    python batch_api/processor.py --check
else
    echo "Usage: ./run_batch.sh [submit|check] [batch_size]"
    echo ""
    echo "Examples:"
    echo "  ./run_batch.sh submit        # Submit batch with default size"
    echo "  ./run_batch.sh submit 10000  # Submit batch with size 10,000"
    echo "  ./run_batch.sh check         # Check for completed batches"
fi