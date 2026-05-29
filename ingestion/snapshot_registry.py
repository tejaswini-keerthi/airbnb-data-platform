"""
Snapshot registry — tracks which city/snapshot combinations
have been downloaded and uploaded to S3 Bronze.
Prevents reprocessing on every pipeline run (incremental loading).
"""

import json
import os
from datetime import datetime
from pathlib import Path
from loguru import logger


# Registry file lives locally during dev
# In production this would be a Snowflake table
REGISTRY_PATH = Path("ingestion/registry.json")


class SnapshotRegistry:
    """
    Tracks processing state for each city + snapshot_date combination.

    States:
        downloaded  — files downloaded locally, not yet uploaded to S3
        uploaded    — files uploaded to S3 Bronze
        processed   — Bronze → Silver transformation complete
        complete    — Silver → Gold transformation complete
    """

    def __init__(self, registry_path: Path = REGISTRY_PATH):
        self.registry_path = registry_path
        self._data = self._load()

    def _load(self) -> dict:
        """Loads registry from JSON file. Creates empty registry if not found."""
        if self.registry_path.exists():
            with open(self.registry_path, "r") as f:
                return json.load(f)
        return {}

    def _save(self) -> None:
        """Persists registry to JSON file."""
        self.registry_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.registry_path, "w") as f:
            json.dump(self._data, f, indent=2)

    def _key(self, city: str, snapshot_date: str) -> str:
        """Generates registry key from city + snapshot_date."""
        return f"{city}::{snapshot_date}"

    def is_processed(self, city: str, snapshot_date: str) -> bool:
        """Returns True if this snapshot has already been uploaded to S3."""
        key = self._key(city, snapshot_date)
        entry = self._data.get(key, {})
        return entry.get("status") in ("uploaded", "processed", "complete")

    def mark_downloaded(self, city: str, snapshot_date: str) -> None:
        """Marks a snapshot as downloaded locally."""
        key = self._key(city, snapshot_date)
        self._data[key] = {
            "city": city,
            "snapshot_date": snapshot_date,
            "status": "downloaded",
            "downloaded_at": datetime.utcnow().isoformat(),
        }
        self._save()
        logger.info(f"Registry: marked {city}/{snapshot_date} as downloaded")

    def mark_uploaded(self, city: str, snapshot_date: str) -> None:
        """Marks a snapshot as uploaded to S3 Bronze."""
        key = self._key(city, snapshot_date)
        if key in self._data:
            self._data[key]["status"] = "uploaded"
            self._data[key]["uploaded_at"] = datetime.utcnow().isoformat()
        else:
            self._data[key] = {
                "city": city,
                "snapshot_date": snapshot_date,
                "status": "uploaded",
                "uploaded_at": datetime.utcnow().isoformat(),
            }
        self._save()
        logger.info(f"Registry: marked {city}/{snapshot_date} as uploaded")

    def mark_processed(self, city: str, snapshot_date: str) -> None:
        """Marks a snapshot as Bronze → Silver processed."""
        key = self._key(city, snapshot_date)
        if key in self._data:
            self._data[key]["status"] = "processed"
            self._data[key]["processed_at"] = datetime.utcnow().isoformat()
        self._save()
        logger.info(f"Registry: marked {city}/{snapshot_date} as processed")

    def mark_complete(self, city: str, snapshot_date: str) -> None:
        """Marks a snapshot as fully complete (Silver → Gold done)."""
        key = self._key(city, snapshot_date)
        if key in self._data:
            self._data[key]["status"] = "complete"
            self._data[key]["completed_at"] = datetime.utcnow().isoformat()
        self._save()
        logger.info(f"Registry: marked {city}/{snapshot_date} as complete")

    def get_pending_upload(self, city: str) -> list[str]:
        """Returns snapshot dates that are downloaded but not yet uploaded."""
        pending = []
        for key, entry in self._data.items():
            if entry["city"] == city and entry["status"] == "downloaded":
                pending.append(entry["snapshot_date"])
        return pending

    def get_all_entries(self) -> list[dict]:
        """Returns all registry entries — useful for debugging."""
        return list(self._data.values())

    def summary(self) -> None:
        """Prints a summary of registry state."""
        from collections import Counter
        statuses = Counter(e["status"] for e in self._data.values())
        logger.info(f"Registry summary: {dict(statuses)}")