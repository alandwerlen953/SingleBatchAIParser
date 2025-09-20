#!/bin/bash
# Script to get and analyze the raw API response for a specific user ID

# Check if a user ID was provided
if [ $# -lt 1 ]; then
    echo "Usage: $0 <userid> [model] [--save-prompts]"
    echo "Example: $0 1234567 gpt-4o-mini --save-prompts"
    exit 1
fi

# Get arguments
USERID=$1
MODEL=${2:-gpt-4o-mini}  # Default model if not provided
SAVE_PROMPTS=""

# Check if --save-prompts is specified
for arg in "$@"; do
    if [ "$arg" = "--save-prompts" ]; then
        SAVE_PROMPTS="--save-prompts"
        break
    fi
done

# Create debug_output directory if it doesn't exist
mkdir -p debug_output

# Set up path variables
DIR=$(dirname "$0")
TIMESTAMP=$(date +%Y%m%d%H%M%S)
OUTPUT_DIR="${DIR}/debug_output"
LOG_FILE="${OUTPUT_DIR}/analyze_${USERID}_${TIMESTAMP}.log"

# Echo commands to the log file
echo "Starting analysis for User ID ${USERID} with model ${MODEL}" | tee -a "$LOG_FILE"
echo "Current directory: $(pwd)" | tee -a "$LOG_FILE"
echo "Output directory: ${OUTPUT_DIR}" | tee -a "$LOG_FILE"
echo "Timestamp: ${TIMESTAMP}" | tee -a "$LOG_FILE"
echo "------------------------------------------------------" | tee -a "$LOG_FILE"

# Step 1: Get the raw API response
echo "Step 1: Fetching raw API response for user ID ${USERID}..." | tee -a "$LOG_FILE"
python3 "${DIR}/test_user_response.py" "$USERID" "--model" "$MODEL" $SAVE_PROMPTS | tee -a "$LOG_FILE"

# Check if the API response was fetched successfully
if [ $? -ne 0 ]; then
    echo "Error: Failed to fetch API response for user ID ${USERID}" | tee -a "$LOG_FILE"
    exit 1
fi

# Find the most recent response file for this user
RESPONSE_FILE=$(find "${OUTPUT_DIR}" -name "user_response_${USERID}_*.json" -type f -printf "%T@ %p\n" | sort -n | tail -1 | cut -f2- -d" ")

if [ -z "$RESPONSE_FILE" ]; then
    echo "Error: Could not find response file for user ID ${USERID}" | tee -a "$LOG_FILE"
    exit 1
fi

echo "Response file: ${RESPONSE_FILE}" | tee -a "$LOG_FILE"
echo "------------------------------------------------------" | tee -a "$LOG_FILE"

# Step 2: Parse and analyze the response
echo "Step 2: Parsing and analyzing response..." | tee -a "$LOG_FILE"
python3 "${DIR}/parse_response.py" "$RESPONSE_FILE" | tee -a "$LOG_FILE"

if [ $? -ne 0 ]; then
    echo "Error: Failed to parse and analyze response" | tee -a "$LOG_FILE"
    exit 1
fi

echo "------------------------------------------------------" | tee -a "$LOG_FILE"
echo "Analysis complete! Results saved to ${OUTPUT_DIR}" | tee -a "$LOG_FILE"
echo "Log file: ${LOG_FILE}" | tee -a "$LOG_FILE"

# Make the script executable
chmod +x "${DIR}/analyze_user_response.sh"

echo "------------------------------------------------------"
echo "You can now run this script with a user ID to analyze:"
echo "  ${DIR}/analyze_user_response.sh <userid> [model] [--save-prompts]"
echo "Examples:"
echo "  ${DIR}/analyze_user_response.sh 1234567 gpt-4o-mini"
echo "  ${DIR}/analyze_user_response.sh 1234567 gpt-4o-mini --save-prompts"
echo "------------------------------------------------------"