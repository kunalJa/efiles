#!/usr/bin/env python3
"""
Simple script to upload a file to an S3 bucket using boto3
"""

import boto3
import os
from pathlib import Path
from dotenv import load_dotenv

def upload_file_to_s3(bucket_name, local_file_path, s3_key=None):
    """
    Upload a file to an S3 bucket
    
    Args:
        bucket_name (str): Name of the S3 bucket
        local_file_path (str): Path to the local file to upload
        s3_key (str): S3 object key (path in bucket). If None, uses filename.
    """
    # If no S3 key provided, use the filename
    if s3_key is None:
        s3_key = Path(local_file_path).name
    
    # Create S3 client
    s3_client = boto3.client('s3')
    
    try:
        # Upload the file
        s3_client.upload_file(local_file_path, bucket_name, s3_key)
        print(f"Successfully uploaded {local_file_path} to s3://{bucket_name}/{s3_key}")
        return True
    except Exception as e:
        print(f"Error uploading file: {e}")
        return False

def list_bucket_contents(bucket_name):
    """List contents of an S3 bucket"""
    s3_client = boto3.client('s3')
    
    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name)
        if 'Contents' in response:
            print(f"\nContents of bucket '{bucket_name}':")
            for obj in response['Contents']:
                print(f"  - {obj['Key']} ({obj['Size']} bytes)")
        else:
            print(f"Bucket '{bucket_name}' is empty")
    except Exception as e:
        print(f"Error listing bucket contents: {e}")

if __name__ == "__main__":
    # 1. This command finds your .env file and loads the variables into memory
    load_dotenv()
    
    # Example usage
    bucket_name = os.getenv("AWS_S3_BUCKET_NAME")
    local_file = "example.txt"  # File to upload
    
    # Create a sample file to upload if it doesn't exist
    if not os.path.exists(local_file):
        with open(local_file, 'w') as f:
            f.write("Hello, S3! This is a test file uploaded via boto3.")
        print(f"Created sample file: {local_file}")
    
    # Upload the file
    success = upload_file_to_s3(bucket_name, local_file, 'test')
    
    if success:
        # List bucket contents to verify upload
        list_bucket_contents(bucket_name)
