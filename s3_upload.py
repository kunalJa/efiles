#!/usr/bin/env python3
"""
Bulk PDF uploader for E_Files volumes to S3.
Uses hash-based prefixes for partition distribution and multithreading for speed.

Usage:
    python s3_upload.py                     # Upload all volumes (1-12, skipping 9)
    python s3_upload.py --volume 3          # Upload only VOL00003
    python s3_upload.py --dry-run           # List files without uploading
    python s3_upload.py --workers 100       # Use 100 threads (default: 50)
"""

import argparse
import os
import re
import sys
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from threading import Lock

import boto3
from dotenv import load_dotenv

# Base path to E_Files (WSL mount of Windows path)
E_FILES_BASE = os.getenv("E_FILES_BASE")

# Volume numbers to process (1-12, skipping 9)
ALL_VOLUMES = [1, 2, 3, 4, 5, 6, 7, 8, 10, 11, 12]

# Thread-safe counters
stats_lock = Lock()
upload_stats = {"success": 0, "failed": 0, "skipped": 0}


def generate_s3_key(local_file_path: str) -> str:
    """
    Generate an S3 key: VOL00001/EFTA00003159.pdf
    
    Filenames are guaranteed unique in the dataset, so no hash prefix needed.
    """
    path = Path(local_file_path)
    filename = path.name
    
    # Extract volume from path (e.g., VOL00001)
    # Path structure: .../E_Files/VOL00001/IMAGES/0001/file.pdf
    for part in path.parts:
        if part.startswith("VOL"):
            return f"{part}/{filename}"
    
    # Fallback if VOL not found in path
    return filename


def collect_pdf_files(volume_num: int) -> list[str]:
    """
    Collect all PDF file paths from a specific volume's IMAGES directory.
    
    Args:
        volume_num: Volume number (e.g., 1 for VOL00001)
    
    Returns:
        List of absolute paths to PDF files
    """
    volume_dir = Path(E_FILES_BASE) / f"VOL{volume_num:05d}"
    images_dir = volume_dir / "IMAGES"
    
    if not images_dir.exists():
        print(f"Warning: IMAGES directory not found: {images_dir}")
        return []
    
    pdf_files = []
    for root, _, files in os.walk(images_dir):
        for filename in files:
            if filename.lower().endswith('.pdf'):
                pdf_files.append(os.path.join(root, filename))
    
    return pdf_files


def extract_file_number(filename: str) -> int | None:
    """
    Extract the numeric portion from a filename like EFTA00003159.pdf -> 3159
    """
    match = re.search(r'(\d+)', Path(filename).stem)
    if match:
        return int(match.group(1))
    return None


def analyze_page_distribution(file_paths: list[str]) -> dict:
    """
    Analyze page counts based on gaps between sequential file numbers.
    
    If EFTA00003159.pdf is followed by EFTA00003164.pdf, then
    EFTA00003159.pdf has (3164 - 3159) = 5 pages.
    
    Returns:
        Dict with page distribution statistics
    """
    # Extract (number, path) pairs and sort by number
    numbered_files = []
    skipped = 0
    
    for path in file_paths:
        filename = Path(path).name
        num = extract_file_number(filename)
        if num is not None:
            numbered_files.append((num, path, filename))
        else:
            skipped += 1
    
    if not numbered_files:
        print("No numbered files found to analyze.")
        return {}
    
    # Sort by the extracted number
    numbered_files.sort(key=lambda x: x[0])
    
    # Calculate page counts based on gaps
    page_counts = []  # List of (filename, page_count)
    
    for i in range(len(numbered_files) - 1):
        current_num, current_path, current_name = numbered_files[i]
        next_num, _, _ = numbered_files[i + 1]
        page_count = next_num - current_num
        page_counts.append((current_name, page_count, current_path))
    
    # Last file - unknown page count, assume 1
    last_name = numbered_files[-1][2]
    last_path = numbered_files[-1][1]
    page_counts.append((last_name, 1, last_path))  # Can't determine, assume 1
    
    # Build distribution buckets
    distribution = defaultdict(list)
    for filename, count, path in page_counts:
        if count == 1:
            distribution["1 page"].append((filename, count))
        elif count <= 4:
            distribution["2-4 pages"].append((filename, count))
        elif count <= 10:
            distribution["5-10 pages"].append((filename, count))
        else:
            distribution["10+ pages"].append((filename, count))
    
    # Find max
    max_pages = max(page_counts, key=lambda x: x[1])
    
    # Print results
    print("\n" + "=" * 60)
    print("PAGE DISTRIBUTION ANALYSIS")
    print("=" * 60)
    print(f"Total files analyzed: {len(page_counts)}")
    if skipped:
        print(f"Files skipped (no number found): {skipped}")
    print()
    
    buckets = ["1 page", "2-4 pages", "5-10 pages", "10+ pages"]
    for bucket in buckets:
        files_in_bucket = distribution[bucket]
        count = len(files_in_bucket)
        pct = 100 * count / len(page_counts) if page_counts else 0
        print(f"  {bucket:12}: {count:6} files ({pct:5.1f}%)")
    
    print()
    print(f"Maximum pages: {max_pages[1]} pages")
    print(f"  File: {max_pages[0]}")
    
    # Show some examples of large files
    large_files = [(f, c) for f, c, p in page_counts if c > 10]
    if large_files:
        large_files.sort(key=lambda x: -x[1])  # Sort descending
        print(f"\nTop 10 largest files (by page count):")
        for filename, count in large_files[:10]:
            print(f"  {filename}: {count} pages")
    
    print("=" * 60)
    
    return {
        "total": len(page_counts),
        "distribution": {k: len(v) for k, v in distribution.items()},
        "max_pages": max_pages[1],
        "max_file": max_pages[0],
    }


def upload_single_file(s3_client, bucket_name: str, local_path: str, s3_key: str) -> tuple[str, bool, str]:
    """
    Upload a single file to S3.
    
    Returns:
        Tuple of (local_path, success_bool, error_message)
    """
    try:
        s3_client.upload_file(local_path, bucket_name, s3_key)
        return (local_path, True, "")
    except Exception as e:
        return (local_path, False, str(e))


def upload_files_parallel(
    bucket_name: str,
    file_paths: list[str],
    max_workers: int = 50,
    dry_run: bool = False
) -> dict:
    """
    Upload multiple files to S3 using a thread pool.
    
    Args:
        bucket_name: S3 bucket name
        file_paths: List of local file paths to upload
        max_workers: Number of parallel upload threads
        dry_run: If True, only print what would be uploaded
    
    Returns:
        Stats dict with success/failed/skipped counts
    """
    global upload_stats
    upload_stats = {"success": 0, "failed": 0, "skipped": 0}
    
    if not file_paths:
        print("No files to upload.")
        return upload_stats
    
    total_files = len(file_paths)
    print(f"\n{'[DRY RUN] ' if dry_run else ''}Processing {total_files} PDF files...")
    print(f"Using {max_workers} worker threads\n")
    
    if dry_run:
        for path in file_paths:
            s3_key = generate_s3_key(path)
            print(f"  Would upload: {path}")
            print(f"            -> s3://{bucket_name}/{s3_key}")
        upload_stats["skipped"] = total_files
        return upload_stats
    
    # Create a single S3 client (thread-safe for upload_file)
    s3_client = boto3.client('s3')
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all upload tasks
        futures = {}
        for path in file_paths:
            s3_key = generate_s3_key(path)
            future = executor.submit(upload_single_file, s3_client, bucket_name, path, s3_key)
            futures[future] = path
        
        # Process results as they complete
        completed = 0
        for future in as_completed(futures):
            local_path, success, error = future.result()
            completed += 1
            
            with stats_lock:
                if success:
                    upload_stats["success"] += 1
                else:
                    upload_stats["failed"] += 1
                    print(f"  FAILED: {local_path} - {error}")
            
            # Progress update every 100 files or at the end
            if completed % 100 == 0 or completed == total_files:
                print(f"  Progress: {completed}/{total_files} ({100*completed//total_files}%)")
    
    return upload_stats


def main():
    load_dotenv()
    
    parser = argparse.ArgumentParser(
        description="Upload E_Files PDFs to S3 with hash-based prefixes and multithreading."
    )
    parser.add_argument(
        "--volume", "-v",
        type=int,
        help="Specific volume number to upload (e.g., 3 for VOL00003). If omitted, uploads all volumes."
    )
    parser.add_argument(
        "--workers", "-w",
        type=int,
        default=50,
        help="Number of parallel upload threads (default: 50)"
    )
    parser.add_argument(
        "--dry-run", "-n",
        action="store_true",
        help="List files that would be uploaded without actually uploading"
    )
    parser.add_argument(
        "--analyze", "-a",
        action="store_true",
        help="Analyze page distribution without uploading (no S3 credentials needed)"
    )
    args = parser.parse_args()
    
    # S3 bucket not required for analyze mode
    bucket_name = os.getenv("AWS_S3_BUCKET_NAME")
    if not bucket_name and not args.analyze:
        print("Error: AWS_S3_BUCKET_NAME not set in environment or .env file")
        sys.exit(1)
    
    # Determine which volumes to process
    if args.volume is not None:
        if args.volume == 9:
            print("Error: Volume 9 does not exist")
            sys.exit(1)
        volumes = [args.volume]
    else:
        volumes = ALL_VOLUMES
    
    print(f"E_Files base path: {E_FILES_BASE}")
    print(f"Target S3 bucket: {bucket_name}")
    print(f"Volumes to process: {volumes}")
    
    # Collect all PDF files from specified volumes
    all_pdf_files = []
    for vol_num in volumes:
        print(f"\nScanning VOL{vol_num:05d}...")
        pdf_files = collect_pdf_files(vol_num)
        print(f"  Found {len(pdf_files)} PDF files")
        all_pdf_files.extend(pdf_files)
    
    print(f"\nTotal PDF files found: {len(all_pdf_files)}")
    
    if not all_pdf_files:
        print("No PDF files found.")
        sys.exit(0)
    
    # Analyze mode - just show page distribution stats
    if args.analyze:
        analyze_page_distribution(all_pdf_files)
        sys.exit(0)
    
    # Upload files
    stats = upload_files_parallel(
        bucket_name=bucket_name,
        file_paths=all_pdf_files,
        max_workers=args.workers,
        dry_run=args.dry_run
    )
    
    # Print summary
    print("\n" + "=" * 50)
    print("UPLOAD SUMMARY")
    print("=" * 50)
    print(f"  Successful: {stats['success']}")
    print(f"  Failed:     {stats['failed']}")
    print(f"  Skipped:    {stats['skipped']}")
    print("=" * 50)


if __name__ == "__main__":
    main()
