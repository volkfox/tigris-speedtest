# S3 Speed Testing Tools

This package provides tools for testing S3 storage performance using both Python (boto3) and s5cmd.

## Files Overview

- `speed_test.py` - Main Python script for comprehensive S3 testing
- `s5cmd_test.sh` - Shell script for testing using s5cmd
- `run_speed_test.sh` - Helper script for automated multiple downloads
- `requirements.txt` - Python package dependencies
- `key.sh` - Template for S3 credentials (not included in repo)

## Prerequisites

1. Install required Python packages:
```bash
pip install -r requirements.txt
```

2. Install s5cmd (for s5cmd_test.sh):

MacOS
```bash
brew install s5cmd
```

Linux
```bash
curl -L https://github.com/peak/s5cmd/releases/download/v2.0.0/s5cmd_2.0.0_Linux-64bit.tar.gz | tar xz
sudo mv s5cmd /usr/local/bin/
```
3. Set up environment variables (create key.sh):

```bash
export AWS_ACCESS_KEY_ID="your_access_key"
export AWS_SECRET_ACCESS_KEY="your_secret_key"
export AWS_ENDPOINT_URL="https://fly.storage.tigris.dev"
export AWS_REGION="auto"
```
Then source it:

```bash
source key.sh
```

### Features of the Python script


- MD5 hash verification for downloads
- Progress bars for file operations
- Speed metrics in MB/s
- Automatic cleanup of downloaded files
- Connection pooling for small files
- Detailed metadata listing

#### Usage examples:

Create 1GB random file
```
python speed_test.py --create --large
```

Create with custom large file (e.g., 2GB)
```
python speed_test.py --create --large --size 2147483648
```

Create 10,000 small random files (2-512 bytes each)
```
python speed_test.py --create --small
```

Upload large file to S3 bucket
```
python speed_test.py --upload --large
```
Upload all small files
```
python speed_test.py --upload --small
```
Upload all files at once
```
python speed_test.py --upload --all
```
Download large file once
```
python speed_test.py --download --large
```
Download large file multiple times
```
python speed_test.py --download --large --times 10
```
Download small files
```
python speed_test.py --download --small
```
Download all files
```
python speed_test.py --download --all
```
List all objects
```
python speed_test.py --list
```
List with metadata query
```
python speed_test.py --list --query 'Content-Type = "binary/octet-stream"'
```
## S5cmd Script (s5cmd_test.sh)

### Basic Usage

1. Download large file multiple times (default: 5)

```
./s5cmd_test.sh 10
```
2. Download all small files
```
./s5cmd_test.sh 1 --small
```

### S5cmd script Features
- Speed metrics for each download
- Automatic cleanup
- Average speed calculation for multiple downloads
- Compatible with both Linux and macOS

## General Tips

1. For consistent testing:
   - Ensure stable network connection
   - Monitor system resources
   - Clear local cache between tests if needed

2. For large file operations:
   - Consider available disk space (need ~3x file size)
   - Use --times parameter for multiple downloads
   - Check MD5 hashes for integrity

3. For small file operations:
   - Uses connection pooling for better performance
   - Parallel uploads/downloads
   - Bulk integrity checking

4. Error handling:
   - All scripts handle interruptions gracefully
   - Automatic cleanup of temporary files
   - Clear error messages for missing prerequisites

## Common Issues

1. Environment variables not set:
   - Ensure all AWS variables are properly exported
   - Check for typos in credentials

2. Disk space:
   - Ensure enough space for test files
   - Downloads are automatically cleaned up

3. Network issues:
   - Scripts will show clear error messages
   - Check endpoint URL accessibility
