#!/bin/bash

# Default number of downloads if not specified
num_downloads=${1:-5}

# S3 Configuration
#export AWS_ACCESS_KEY_ID="tid_"
#export AWS_SECRET_ACCESS_KEY="tsec_"
#export AWS_ENDPOINT_URL="https://fly.storage.tigris.dev"
#export AWS_REGION="auto"

BUCKET="dkh-test"
DATA_DIR="data/downloads"

# Cleanup function
cleanup() {
    echo "Cleaning up downloads..."
    rm -rf "$DATA_DIR"
    echo "Cleanup complete"
}

# Register cleanup on script exit
trap cleanup EXIT

# Create downloads directory if it doesn't exist
mkdir -p "$DATA_DIR"

function download_large_file() {
    local iteration=$1
    echo "Downloading large file (iteration $iteration/$num_downloads)..."
    start_time=$(date +%s)
    
    s5cmd --endpoint-url="$AWS_ENDPOINT_URL" \
          cp "s3://$BUCKET/large_file.dat" "$DATA_DIR/large_file_${iteration}.dat"
    
    end_time=$(date +%s)
    duration=$((end_time - start_time))
    
    # Get file size in bytes
    size=$(stat -f%z "$DATA_DIR/large_file_${iteration}.dat" 2>/dev/null || stat -c%s "$DATA_DIR/large_file_${iteration}.dat")
    speed=$(echo "scale=2; $size / (1024 * 1024 * $duration)" | bc)
    
    echo "Download Speed: ${speed} MB/s (Duration: ${duration}s)"
    
    # Clean up downloaded file
    rm "$DATA_DIR/large_file_${iteration}.dat"
}

function download_small_files() {
    echo "Downloading all small files..."
    start_time=$(date +%s)
    
    s5cmd --endpoint-url="$AWS_ENDPOINT_URL" \
          cp "s3://$BUCKET/small_file_*.txt" "$DATA_DIR/"
    
    end_time=$(date +%s)
    duration=$((end_time - start_time))
    
    # Calculate total size of downloaded files
    total_size=0
    for file in "$DATA_DIR"/small_file_*.txt; do
        size=$(stat -f%z "$file" 2>/dev/null || stat -c%s "$file")
        total_size=$((total_size + size))
    done
    
    speed=$(echo "scale=2; $total_size / (1024 * 1024 * $duration)" | bc)
    echo "Download Speed: ${speed} MB/s (Duration: ${duration}s)"
    
    # Clean up downloaded files
    rm "$DATA_DIR"/small_file_*.txt
}

# Main execution
echo "Starting s5cmd speed test..."
echo "------------------------------------------------"

if [ "$2" == "--small" ]; then
    download_small_files
else
    total_start_time=$(date +%s)
    
    for ((i=1; i<=num_downloads; i++)); do
        download_large_file $i
    done
    
    total_end_time=$(date +%s)
    total_time=$((total_end_time - total_start_time))
    
    echo "------------------------------------------------"
    echo "Total execution time: ${total_time} seconds"
    echo "Average time per download: $((total_time / num_downloads)) seconds"
fi 