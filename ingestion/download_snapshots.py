"""
Downloads Airbnb snapshot files from Inside Airbnb.
Only downloads snapshots not already in the registry.
"""

import os
import gzip
import shutil
import requests
from pathlib import Path
from loguru import logger
from dotenv import load_dotenv

from config import CITIES, SNAPSHOT_FILES, INSIDE_AIRBNB_BASE_URL, LOCAL_TEMP_DIR, CityConfig
from snapshot_registry import SnapshotRegistry

load_dotenv()


def get_snapshot_urls(city: CityConfig, snapshot_date: str) -> dict[str, str]:
    """
    Constructs download URLs for a given city and snapshot date.
    Inside Airbnb URL pattern:
    https://data.insideairbnb.com/{url_country}/{state}/{city_id}/{date}/data/{file}
    """
    urls = {}
    for filename in SNAPSHOT_FILES:
        url = (
            f"{INSIDE_AIRBNB_BASE_URL}/"
            f"{city.url_country}/"
            f"{city.state}/"
            f"{city.inside_airbnb_id}/"
            f"{snapshot_date}/data/{filename}"
        )
        urls[filename] = url
    return urls


def download_file(url: str, dest_path: Path) -> bool:
    """
    Downloads a single file from a URL to a local path.
    Returns True if successful, False otherwise.
    """
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    try:
        logger.info(f"Downloading {url}")
        response = requests.get(url, stream=True, timeout=60, headers=headers)

        if response.status_code == 404:
            logger.warning(f"File not found (404): {url}")
            return False

        response.raise_for_status()

        dest_path.parent.mkdir(parents=True, exist_ok=True)

        with open(dest_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

        logger.info(f"Downloaded to {dest_path}")
        return True

    except requests.RequestException as e:
        logger.error(f"Failed to download {url}: {e}")
        return False


def decompress_gz(gz_path: Path) -> Path:
    """
    Decompresses a .gz file and returns the path to the decompressed file.
    """
    csv_path = gz_path.with_suffix("")  # removes .gz → keeps .csv
    with gzip.open(gz_path, "rb") as f_in:
        with open(csv_path, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)
    gz_path.unlink()  # delete the .gz file after decompression
    logger.info(f"Decompressed to {csv_path}")
    return csv_path


def download_city_snapshot(
    city: CityConfig,
    snapshot_date: str,
    temp_dir: Path,
) -> dict[str, Path]:
    """
    Downloads all files for a single city + snapshot date.
    Returns dict of {filename: local_path} for successful downloads.
    """
    urls = get_snapshot_urls(city, snapshot_date)
    downloaded = {}

    for filename, url in urls.items():
        dest_path = temp_dir / city.name / snapshot_date / filename
        success = download_file(url, dest_path)

        if success:
            if filename.endswith(".gz"):
                csv_path = decompress_gz(dest_path)
                csv_filename = filename.replace(".gz", "")
                downloaded[csv_filename] = csv_path
            else:
                downloaded[filename] = dest_path

    return downloaded


def get_available_snapshots(city: CityConfig) -> list[str]:
    """
    Scrapes Inside Airbnb to find available snapshot dates for a city.
    Returns list of date strings in YYYY-MM-DD format.
    """
    import re
    url = "https://insideairbnb.com/get-the-data/"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    try:
        response = requests.get(url, timeout=30, headers=headers)
        pattern = rf"{city.url_country}/{city.state}/{city.inside_airbnb_id}/(\d{{4}}-\d{{2}}-\d{{2}})"
        dates = list(set(re.findall(pattern, response.text)))
        dates.sort()
        logger.info(f"Found {len(dates)} snapshots for {city.name}: {dates}")
        return dates
    except Exception as e:
        logger.error(f"Failed to get snapshots for {city.name}: {e}")
        return []


def run_ingestion(snapshot_dates: list[str] | None = None) -> None:
    """
    Main ingestion function.
    Downloads all unprocessed snapshots for all cities.
    """
    registry = SnapshotRegistry()
    temp_dir = Path(LOCAL_TEMP_DIR)
    temp_dir.mkdir(parents=True, exist_ok=True)

    logger.info("Starting ingestion run")

    for city in CITIES:
        logger.info(f"Processing city: {city.name}")

        dates = snapshot_dates or get_available_snapshots(city)

        if not dates:
            logger.warning(f"No snapshots found for {city.name}, skipping")
            continue

        for snapshot_date in dates:
            if registry.is_processed(city.name, snapshot_date):
                logger.info(
                    f"Skipping {city.name}/{snapshot_date} — already in registry"
                )
                continue

            logger.info(f"Downloading {city.name}/{snapshot_date}")

            downloaded = download_city_snapshot(
                city=city,
                snapshot_date=snapshot_date,
                temp_dir=temp_dir,
            )

            if downloaded:
                logger.info(
                    f"Downloaded {len(downloaded)} files for "
                    f"{city.name}/{snapshot_date}"
                )
                registry.mark_downloaded(city.name, snapshot_date)
            else:
                logger.error(
                    f"No files downloaded for {city.name}/{snapshot_date}"
                )

    logger.info("Ingestion run complete")


if __name__ == "__main__":
    run_ingestion()