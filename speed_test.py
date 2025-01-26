# Testing Tigris S3 with boto3:
#
# Create files: single large file or multiple small 
# python speed_test.py --create --large
# python speed_test.py --create --large --size 2147483648
#
# Upload files: 
# python speed_test.py --upload --large

# Download small files:
# python speed_test.py --download --small
# Download large file 10 times: 
# python speed_test.py --download --large --times 10
#
# Advanced:
# List objects with specific metadata query example:
# python speed_test.py --list --query '`Content-Type` = "binary/octet-stream"'


import os
import time
import boto3
import random
import string
import argparse
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
import hashlib
import shutil

# Check required environment variables
required_vars = ['AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY', 'AWS_ENDPOINT_URL', 'AWS_REGION']
missing_vars = [var for var in required_vars if not os.getenv(var)]
if missing_vars:
    print("Error: Missing required environment variables:")
    for var in missing_vars:
        print(f"  {var}")
    print("Please ensure all required variables are set before running the script.")
    exit(1)

# S3 Configuration from environment
S3_CONFIG = {
    'aws_access_key_id': os.getenv('AWS_ACCESS_KEY_ID'),
    'aws_secret_access_key': os.getenv('AWS_SECRET_ACCESS_KEY'),
    'endpoint_url': os.getenv('AWS_ENDPOINT_URL'),
    'region_name': os.getenv('AWS_REGION')
}

BUCKET_NAME = 'dkh-test'
DATA_DIR = 'data'
LARGE_FILE_SIZE = 1 * 1024 * 1024 * 1024  # 1GB
SMALL_FILE_COUNT = 10000
SMALL_FILE_SIZE_MIN = 2
SMALL_FILE_SIZE_MAX = 512

def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='S3 Speed Test Tool')
    parser.add_argument('--create', action='store_true', help='Create test files')
    parser.add_argument('--upload', action='store_true', help='Upload files to S3')
    parser.add_argument('--download', action='store_true', help='Download files from S3')
    parser.add_argument('--all', action='store_true', help='Run all tests')
    parser.add_argument('--large', action='store_true', help='Test large file operations')
    parser.add_argument('--small', action='store_true', help='Test small files operations')
    parser.add_argument('--modified', action='store_true', help='Test modified large file upload')
    parser.add_argument('--size', type=int, default=LARGE_FILE_SIZE,
                       help='Size of large file in bytes (default: 1GB)')
    parser.add_argument('--times', type=int, default=1,
                       help='Number of times to repeat large file download (default: 1)')
    parser.add_argument('--list', action='store_true', help='List objects in bucket')
    parser.add_argument('--query', type=str, help='Optional metadata query for listing (e.g. "`Content-Type` = \'text/plain\'")')
    parser.add_argument('--replace-original', action='store_true', 
                       help='Replace original files with downloaded ones')
    return parser.parse_args()

def ensure_data_directory():
    """Create data directory if it doesn't exist"""
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)

def generate_random_content(size):
    """Generate random content of specified size"""
    return ''.join(random.choices(string.ascii_letters + string.digits, k=size)).encode()

def create_large_file(filename, size):
    """Create a large file with random content"""
    print(f"Generating large file {filename} ({size/1024/1024/1024:.2f} GB)...")
    filepath = os.path.join(DATA_DIR, filename)
    
    chunk_size = 1024 * 1024  # 1MB chunks
    written = 0
    
    with open(filepath, 'wb') as f:
        with tqdm(total=size, unit='B', unit_scale=True) as pbar:
            while written < size:
                remaining = min(chunk_size, size - written)
                f.write(generate_random_content(remaining))
                written += remaining
                pbar.update(remaining)
    
    return filepath

def create_small_files():
    """Create multiple small files with random content"""
    print(f"Generating {SMALL_FILE_COUNT} small files...")
    files = []
    for i in tqdm(range(SMALL_FILE_COUNT)):
        size = random.randint(SMALL_FILE_SIZE_MIN, SMALL_FILE_SIZE_MAX)
        filename = f'small_file_{i}.txt'
        filepath = os.path.join(DATA_DIR, filename)
        with open(filepath, 'wb') as f:
            f.write(generate_random_content(size))
        files.append(filepath)
    return files

def get_s3_client():
    """Create and return an S3 client"""
    return boto3.client('s3', **S3_CONFIG)

def upload_file(s3_client, filepath):
    """Upload a single file to S3"""
    filename = os.path.basename(filepath)
    s3_client.upload_file(filepath, BUCKET_NAME, filename)
    return filename

def calculate_md5(filepath, chunk_size=8192):
    """Calculate MD5 hash of a file"""
    md5 = hashlib.md5()
    with open(filepath, 'rb') as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            md5.update(chunk)
    return md5.hexdigest()

def verify_file_integrity(original_path, downloaded_path):
    """Verify file integrity by comparing MD5 hashes"""
    original_md5 = calculate_md5(original_path)
    downloaded_md5 = calculate_md5(downloaded_path)
    return original_md5 == downloaded_md5

def cleanup_downloads():
    """Remove downloaded files"""
    download_dir = os.path.join(DATA_DIR, 'downloads')
    if os.path.exists(download_dir):
        shutil.rmtree(download_dir)

def download_file(s3_client, filename, replace_original=False):
    """Download a single file from S3"""
    if replace_original:
        download_path = os.path.join(DATA_DIR, filename)
    else:
        download_dir = os.path.join(DATA_DIR, 'downloads')
        if not os.path.exists(download_dir):
            os.makedirs(download_dir)
        download_path = os.path.join(download_dir, filename)
    
    # Ensure parent directory exists
    os.makedirs(os.path.dirname(download_path), exist_ok=True)
    s3_client.download_file(BUCKET_NAME, filename, download_path)
    return download_path

def measure_transfer_speed(operation_name, start_time, total_size):
    """Calculate and return transfer speed metrics"""
    duration = time.time() - start_time
    speed_mbps = (total_size / 1024 / 1024) / duration
    return f"{operation_name} Speed: {speed_mbps:.2f} MB/s (Duration: {duration:.2f}s)"

def create_test_files(args):
    """Create test files based on arguments"""
    ensure_data_directory()
    
    if args.large or args.all:
        create_large_file('large_file.dat', args.size)
        if args.modified:
            create_large_file('large_file.dat', args.size)  # Creates a new random file with same name
    
    if args.small or args.all:
        create_small_files()

def upload_test_files(args):
    """Upload test files based on arguments"""
    s3_client = get_s3_client()
    
    if args.large or args.all:
        filepath = os.path.join(DATA_DIR, 'large_file.dat')
        if os.path.exists(filepath):
            file_size = os.path.getsize(filepath)
            print("\nUploading large file...")
            # Calculate and print hash before upload
            print(f"Source file MD5: {calculate_md5(filepath)}")
            start_time = time.time()
            upload_file(s3_client, filepath)
            print(measure_transfer_speed("Upload", start_time, file_size))
        else:
            print("Large file not found. Run with --create first.")
    
    if args.small or args.all:
        filepaths = [os.path.join(DATA_DIR, f'small_file_{i}.txt') for i in range(SMALL_FILE_COUNT)]
        if os.path.exists(filepaths[0]):
            total_size = sum(os.path.getsize(f) for f in filepaths if os.path.exists(f))
            print(f"\nUploading {SMALL_FILE_COUNT} small files...")
            start_time = time.time()
            with ThreadPoolExecutor(max_workers=10) as executor:
                list(tqdm(executor.map(lambda f: upload_file(s3_client, f), filepaths), total=len(filepaths)))
            print(measure_transfer_speed("Upload", start_time, total_size))
        else:
            print("Small files not found. Run with --create first.")

def download_test_files(args):
    """Download test files based on arguments"""
    s3_client = get_s3_client()
    integrity_failures = []
    
    try:
        if args.large or args.all:
            speeds = []
            for i in range(args.times):
                print(f"\nDownloading large file (iteration {i+1}/{args.times})...")
                try:
                    start_time = time.time()
                    original_path = os.path.join(DATA_DIR, 'large_file.dat')
                    if os.path.exists(original_path) and i == 0 and not args.replace_original:
                        print(f"Original file MD5: {calculate_md5(original_path)}")
                    
                    # Only replace original on last iteration if replace_original is True
                    replace_this_time = args.replace_original and (i == args.times - 1)
                    download_path = download_file(s3_client, 'large_file.dat', replace_this_time)
                    file_size = os.path.getsize(download_path)
                    duration = time.time() - start_time
                    speed_mbps = (file_size / 1024 / 1024) / duration
                    speeds.append(speed_mbps)
                    
                    print(f"Downloaded file MD5: {calculate_md5(download_path)}")
                    print(f"Download Speed: {speed_mbps:.2f} MB/s (Duration: {duration:.2f}s)")
                    
                    # Verify integrity if original exists and we're not replacing it
                    if os.path.exists(original_path) and not replace_this_time:
                        print("Verifying file integrity...")
                        if verify_file_integrity(original_path, download_path):
                            print("✓ Large file integrity verified")
                        else:
                            print("✗ Large file integrity check failed")
                            integrity_failures.append(f'large_file.dat (iteration {i+1})')
                    
                    # Cleanup after each iteration except the last one
                    if i < args.times - 1:
                        if not args.replace_original:
                            cleanup_downloads()
                
                except Exception as e:
                    print(f"Error downloading large file: {e}")
            
            # Print statistics for multiple downloads
            if args.times > 1:
                print("\nLarge file download statistics:")
                print(f"Average speed: {sum(speeds)/len(speeds):.2f} MB/s")
                print(f"Max speed: {max(speeds):.2f} MB/s")
                print(f"Min speed: {min(speeds):.2f} MB/s")
        
        if args.small or args.all:
            filenames = [f'small_file_{i}.txt' for i in range(SMALL_FILE_COUNT)]
            print(f"\nDownloading {SMALL_FILE_COUNT} small files...")
            try:
                start_time = time.time()
                with ThreadPoolExecutor(max_workers=10) as executor:
                    downloaded = list(tqdm(executor.map(
                        lambda f: download_file(s3_client, f, args.replace_original), 
                        filenames
                    ), total=len(filenames), desc="Downloading"))
                total_size = sum(os.path.getsize(f) for f in downloaded)
                print(measure_transfer_speed("Download", start_time, total_size))
                
                # Verify integrity only if not replacing originals
                if not args.replace_original:
                    print("Verifying files integrity...")
                    verified_count = 0
                    failed_count = 0
                    with tqdm(total=len(filenames), desc="Verifying") as pbar:
                        for i, filename in enumerate(filenames):
                            original_path = os.path.join(DATA_DIR, filename)
                            download_path = os.path.join(DATA_DIR, 'downloads', filename)
                            if os.path.exists(original_path):
                                if verify_file_integrity(original_path, download_path):
                                    verified_count += 1
                                else:
                                    failed_count += 1
                                    integrity_failures.append(filename)
                            pbar.update(1)
                    
                    print(f"✓ {verified_count} files verified successfully")
                    if failed_count > 0:
                        print(f"✗ {failed_count} files failed integrity check")
                
            except Exception as e:
                print(f"Error downloading small files: {e}")
    
    finally:
        # Print integrity summary if not replacing originals
        if integrity_failures and not args.replace_original:
            print("\nIntegrity check failed for the following files:")
            for filename in integrity_failures:
                print(f"- {filename}")
        
        # Cleanup downloaded files only if not replacing originals
        if not args.replace_original:
            print("\nCleaning up downloaded files...")
            cleanup_downloads()
            print("Cleanup complete")

def list_bucket_contents(args):
    """List contents of the bucket with optional metadata query"""
    print(f"\nListing contents of bucket '{BUCKET_NAME}'...")
    
    # Create S3 service client
    svc = boto3.client("s3", **S3_CONFIG)
    
    # Register metadata query if provided
    if args.query:
        def _x_tigris_query(request, query):
            request.headers.add_header('X-Tigris-Query', query.strip())
        svc.meta.events.register(
            "before-sign.s3.ListObjectsV2",
            lambda request, **kwargs: _x_tigris_query(request, args.query),
        )
    
    try:
        response = svc.list_objects_v2(Bucket=BUCKET_NAME)
        
        if 'Contents' in response:
            total_size = 0
            print("\nObjects:")
            for obj in response['Contents']:
                size_mb = obj['Size'] / (1024 * 1024)
                key = obj['Key']
                print(f"\n  {key} ({size_mb:.2f} MB)")
                
                # Get metadata using HeadObject
                try:
                    head = svc.head_object(Bucket=BUCKET_NAME, Key=key)
                    print("  Metadata:")
                    print(f"    Last Modified: {head.get('LastModified')}")
                    # print(f"    ETag: {head.get('ETag', '').strip('\"')}")
                    etag = head.get('ETag', '').strip('"')
                    print(f"    ETag: {etag}")
                    
                    print(f"    Content Type: {head.get('ContentType', 'not set')}")
                    if 'Metadata' in head:
                        for k, v in head['Metadata'].items():
                            print(f"    {k}: {v}")
                except Exception as e:
                    print(f"    Error getting metadata: {e}")
                
                total_size += obj['Size']
            
            print(f"\nTotal objects: {len(response['Contents'])}")
            print(f"Total size: {total_size / (1024 * 1024 * 1024):.2f} GB")
        else:
            print("Bucket is empty")
            
    except Exception as e:
        print(f"Error listing bucket contents: {e}")

def main():
    """Main function to run tests based on CLI arguments"""
    args = parse_args()
    
    try:
        if args.list:
            list_bucket_contents(args)
            return
            
        # If no specific operation is selected, show help
        if not (args.create or args.upload or args.download or args.all):
            print("Please specify an operation (--create, --upload, --download, --list, or --all)")
            return
        
        # If no file type is selected, default to both
        if not (args.large or args.small or args.all):
            args.large = True
            args.small = True
        
        if args.create or args.all:
            create_test_files(args)
        
        if args.upload or args.all:
            upload_test_files(args)
        
        if args.download or args.all:
            download_test_files(args)
    
    except KeyboardInterrupt:
        print("\nOperation interrupted by user")
    finally:
        # Ensure downloads are cleaned up even if there's an error
        cleanup_downloads()

if __name__ == "__main__":
    main()


