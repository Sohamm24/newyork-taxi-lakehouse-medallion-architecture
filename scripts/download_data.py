import os
import sys
import argparse
import requests
from pathlib import Path

# Base URLs for official NYC Taxi and Limousine Commission (TLC) public data hosted on CloudFront
BASE_TRIP_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{month}.parquet"
ZONE_LOOKUP_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

def download_file(url: str, dest_path: Path, force: bool = False) -> None:
    """
    Downloads a remote file with streaming chunks to keep memory footprint minimal.
    Skips download if file already exists locally and has non-zero size.
    """
    if dest_path.exists() and dest_path.stat().st_size > 0 and not force:
        size_mb = dest_path.stat().st_size / (1024 * 1024)
        print(f"Skipping {dest_path.name}: already exists ({size_mb:.2f} MB).")
        return

    print(f"Downloading from {url} -> {dest_path}...")
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Use temporary file during download to ensure atomic writes
    temp_path = dest_path.with_suffix(dest_path.suffix + ".tmp")
    try:
        with requests.get(url, stream=True, timeout=60) as response:
            response.raise_for_status()
            total_size = int(response.headers.get("content-length", 0))
            downloaded = 0
            
            with open(temp_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=1024 * 1024):  # 1 MB chunks
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)
                        if total_size > 0:
                            percent = (downloaded / total_size) * 100
                            print(f"\r  Progress: {downloaded / (1024*1024):.1f}/{total_size / (1024*1024):.1f} MB ({percent:.1f}%)", end="", flush=True)
                        else:
                            print(f"\r  Downloaded: {downloaded / (1024*1024):.1f} MB", end="", flush=True)

        print()  # Newline after progress bar
        # Atomically rename temporary file to destination
        temp_path.replace(dest_path)
        print(f"Successfully saved {dest_path.name} ({dest_path.stat().st_size / (1024*1024):.2f} MB)")
    except Exception as e:
        if temp_path.exists():
            temp_path.unlink()
        print(f"\nFailed to download {url}: {e}", file=sys.stderr)
        raise

def main() -> None:
    parser = argparse.ArgumentParser(description="Download NYC Yellow Taxi raw data files")
    parser.add_argument(
        "--months",
        type=str,
        default=os.getenv("NYC_TAXI_MONTHS", "2024-01,2024-02"),
        help="Comma-separated YYYY-MM strings to download (default: 2024-01,2024-02)",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=os.getenv("RAW_DATA_DIR", "data/raw"),
        help="Directory to save downloaded files (default: data/raw)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force redownload even if files exist locally",
    )
    args = parser.parse_args()

    raw_dir = Path(args.output_dir).resolve()
    raw_dir.mkdir(parents=True, exist_ok=True)
    months = [m.strip() for m in args.months.split(",") if m.strip()]

    print(f"=== NYC Taxi Data Downloader ===")
    print(f"Destination: {raw_dir}")
    print(f"Target Months: {months}")

    # 1. Download Taxi Zone Lookup CSV
    zone_file = raw_dir / "taxi_zone_lookup.csv"
    download_file(ZONE_LOOKUP_URL, zone_file, force=args.force)

    # 2. Download Yellow Taxi Monthly Parquet files
    for month in months:
        url = BASE_TRIP_URL.format(month=month)
        dest_file = raw_dir / f"yellow_tripdata_{month}.parquet"
        download_file(url, dest_file, force=args.force)

    print("All raw files downloaded successfully!")

if __name__ == "__main__":
    main()
