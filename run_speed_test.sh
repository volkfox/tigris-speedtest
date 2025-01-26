#!/bin/bash

# Default number of downloads if not specified
num_downloads=${1:-5}

echo "Starting speed test with ${num_downloads} large file downloads..."
echo "------------------------------------------------"

# Record start time
start_time=$(date +%s)

# Run speed test with specified number of downloads
python speed_test.py --download --large --times ${num_downloads}

# Calculate total time
end_time=$(date +%s)
total_time=$((end_time - start_time))

echo "------------------------------------------------"
echo "Total execution time: ${total_time} seconds"
echo "Average time per download: $((total_time / num_downloads)) seconds" 