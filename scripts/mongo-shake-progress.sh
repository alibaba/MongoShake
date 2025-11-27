#!/usr/bin/env bash
set -e

#
# Script to fetch and format mongo-shake progress information for the FULL phase.
#
# Usage: mongo-shake-stats.sh <mongo_shake_url>
#

# Default URL if not provided
MONGO_SHAKE_URL=${1:-"http://localhost:9101"}

# Remove trailing slash if present
MONGO_SHAKE_URL=${MONGO_SHAKE_URL%/}

echo

# Fetch progress from the API
# Example JSON:
# {"progress":"26.14%","total_collection_number":88,"finished_collection_number":23,"processing_collection_number":4,"wait_collection_number":61,"collection_metric":{"ns.coll1":"100% (0/0)","ns.coll2":"-","ns.coll3":"50% (500/1000)"}}
progress_output=$(curl -s "$MONGO_SHAKE_URL/progress")

if [ -z "$progress_output" ]; then
    echo "Error: Failed to fetch progress information from $MONGO_SHAKE_URL/progress"
    exit 1
fi

# Format and display the progress
echo "Collections: Total=$(echo $progress_output | jq -r '.total_collection_number'), Finished=$(echo $progress_output | jq -r '.finished_collection_number') ($(echo $progress_output | jq -r '.progress')), Processing=$(echo $progress_output | jq -r '.processing_collection_number'), Waiting=$(echo $progress_output | jq -r '.wait_collection_number')"

echo
echo "In progress ($(echo $progress_output | jq -r '.processing_collection_number'))"
echo
(
  echo $progress_output | jq -r '.collection_metric | to_entries[] | select(.value | test("^[0-9][0-9]?(.[0-9]{2})?%")) | "\(.key) \(.value)"'
) | column -t -s ' '

# List finished collections
echo
echo "Finished ($(echo $progress_output | jq -r '.finished_collection_number'))"
echo
echo "$progress_output" | jq -r '.collection_metric | to_entries[] | select(.value | test("^100(.00)?%")) | .key' | pr -4 -t -w "$(tput cols)" | column -t

# List not started collections
echo
echo "Not started ($(echo $progress_output | jq -r '.wait_collection_number'))"
echo
echo "$progress_output" | jq -r '.collection_metric | to_entries[] | select(.value == "-") | .key' | pr -4 -t -w "$(tput cols)" | column -t