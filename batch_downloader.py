import os
import argparse
import subprocess
import sys
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pathlib import Path


def parse_month(month_str):
    """Parse month string YYYY_MM to datetime object"""
    try:
        year, month = month_str.split('_')
        return datetime(int(year), int(month), 1)
    except Exception as e:
        raise ValueError(f"Invalid month format: {month_str}. Expected format: YYYY_MM (e.g., 2020_01)")


def format_month(dt):
    """Format datetime to YYYY_MM string"""
    return dt.strftime("%Y_%m")


def generate_month_range(start_month, end_month):
    """Generate list of months between start and end (inclusive)"""
    start = parse_month(start_month)
    end = parse_month(end_month)
    
    if start > end:
        raise ValueError(f"Start month ({start_month}) must be before or equal to end month ({end_month})")
    
    months = []
    current = start
    while current <= end:
        months.append(format_month(current))
        current += relativedelta(months=1)
    
    return months


def run_downloader(aoi_path, month, api_key, zoom, save_dir, output_name=None, dry_run=False):
    """Run planet_downloader.py for a single month"""
    cmd = [
        sys.executable,  # Use same Python interpreter
        "planet_downloader.py",
        "--aoi", aoi_path,
        "--month", month,
        "--api-key", api_key,
        "--zoom", str(zoom),
        "--save-dir", save_dir
    ]
    
    if output_name:
        cmd.extend(["--output-name", output_name])
    
    if dry_run:
        print(f"[DRY RUN] Would execute: {' '.join(cmd)}")
        return True, "Dry run - not executed"
    
    print(f"\n{'='*60}")
    print(f"Downloading: {month}")
    print(f"{'='*60}")
    
    try:
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        print(result.stdout)
        return True, None
    except subprocess.CalledProcessError as e:
        error_msg = f"Error downloading {month}:\n{e.stderr}"
        print(error_msg, file=sys.stderr)
        return False, error_msg


def main():
    parser = argparse.ArgumentParser(
        description="Batch download PlanetScope tiles for multiple months",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Download from Jan 2020 to Dec 2020
  python batch_downloader.py --aoi area.geojson --start 2020_01 --end 2020_12 --api-key YOUR_KEY

  # Download with custom zoom and save directory
  python batch_downloader.py --aoi area.geojson --start 2023_01 --end 2023_06 \\
    --api-key YOUR_KEY --zoom 16 --save-dir ./output

  # Dry run to see what would be downloaded
  python batch_downloader.py --aoi area.geojson --start 2020_01 --end 2020_03 \\
    --api-key YOUR_KEY --dry-run

  # Continue on error (don't stop if one month fails)
  python batch_downloader.py --aoi area.geojson --start 2020_01 --end 2020_12 \\
    --api-key YOUR_KEY --continue-on-error
        """
    )
    
    parser.add_argument("--aoi", type=str, required=True, 
                       help="Path to AOI GeoJSON file")
    parser.add_argument("--start", type=str, required=True, 
                       help="Start month in format YYYY_MM (e.g., 2020_01)")
    parser.add_argument("--end", type=str, required=True, 
                       help="End month in format YYYY_MM (e.g., 2025_12)")
    parser.add_argument("--api-key", type=str, required=True, 
                       help="Planet API key")
    parser.add_argument("--zoom", type=int, default=15, 
                       help="Zoom level for tiles (default: 15)")
    parser.add_argument("--save-dir", type=str, default="./data", 
                       help="Directory to save output GeoTIFFs (default: ./data)")
    parser.add_argument("--dry-run", action="store_true", 
                       help="Print commands without executing")
    parser.add_argument("--continue-on-error", action="store_true", 
                       help="Continue downloading even if some months fail")
    parser.add_argument("--skip-existing", action="store_true", 
                       help="Skip months where output file already exists")
    
    args = parser.parse_args()
    
    # Validate AOI file exists
    if not os.path.exists(args.aoi):
        print(f"Error: AOI file not found: {args.aoi}", file=sys.stderr)
        sys.exit(1)
    
    # Generate month range
    try:
        months = generate_month_range(args.start, args.end)
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    
    # Create save directory
    os.makedirs(args.save_dir, exist_ok=True)
    
    # Get AOI filename for output naming
    aoi_basename = os.path.splitext(os.path.basename(args.aoi))[0]
    
    print(f"\n{'='*60}")
    print(f"Batch Planet Downloader")
    print(f"{'='*60}")
    print(f"AOI: {args.aoi}")
    print(f"Period: {args.start} to {args.end}")
    print(f"Total months: {len(months)}")
    print(f"Zoom level: {args.zoom}")
    print(f"Save directory: {args.save_dir}")
    print(f"Dry run: {args.dry_run}")
    print(f"Continue on error: {args.continue_on_error}")
    print(f"Skip existing: {args.skip_existing}")
    print(f"\nMonths to download:")
    for i, month in enumerate(months, 1):
        print(f"  {i}. {month}")
    print(f"{'='*60}\n")
    
    if not args.dry_run:
        response = input("Proceed with download? (y/n): ")
        if response.lower() != 'y':
            print("Aborted by user.")
            sys.exit(0)
    
    # Track results
    results = {
        "success": [],
        "failed": [],
        "skipped": []
    }
    
    # Download each month
    for i, month in enumerate(months, 1):
        # Check if output already exists
        expected_output = os.path.join(args.save_dir, f"{aoi_basename}_{month}.tif")
        
        if args.skip_existing and os.path.exists(expected_output):
            print(f"\n[{i}/{len(months)}] Skipping {month} - output already exists: {expected_output}")
            results["skipped"].append(month)
            continue
        
        print(f"\n[{i}/{len(months)}] Processing month: {month}")
        
        success, error = run_downloader(
            aoi_path=args.aoi,
            month=month,
            api_key=args.api_key,
            zoom=args.zoom,
            save_dir=args.save_dir,
            dry_run=args.dry_run
        )
        
        if success:
            results["success"].append(month)
        else:
            results["failed"].append((month, error))
            if not args.continue_on_error and not args.dry_run:
                print(f"\nStopping due to error. Use --continue-on-error to continue on failures.")
                break
    
    # Print summary
    print(f"\n{'='*60}")
    print(f"Batch Download Summary")
    print(f"{'='*60}")
    print(f"✅ Successful: {len(results['success'])}/{len(months)}")
    if results['success']:
        for month in results['success']:
            print(f"   - {month}")
    
    if results['skipped']:
        print(f"\n⏭️  Skipped: {len(results['skipped'])}")
        for month in results['skipped']:
            print(f"   - {month}")
    
    if results['failed']:
        print(f"\n❌ Failed: {len(results['failed'])}")
        for month, error in results['failed']:
            print(f"   - {month}")
    
    print(f"{'='*60}\n")
    
    # Exit with error code if any failed and not continuing on error
    if results['failed'] and not args.continue_on_error:
        sys.exit(1)


if __name__ == "__main__":
    main()