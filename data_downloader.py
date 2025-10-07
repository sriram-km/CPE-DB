"""Data downloader and extractor for NVD CPE feed."""

import os
import requests
import tarfile
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


class NVDDataDownloader:
    """Downloads and extracts NVD CPE feed data."""

    def __init__(self, config):
        self.config = config
        self.feed_url = config.nvd_feed_url
        self.extract_dir = Path(config.nvd_extract_dir)
        self.download_file = self.extract_dir / "nvdcpe-2.0.tar.gz"

    def download_feed(self, force_download=False):
        """Download the NVD CPE feed."""
        if self.download_file.exists() and not force_download:
            logger.info(f"Feed file already exists: {self.download_file}")
            return True

        # Create directory if it doesn't exist
        self.extract_dir.mkdir(parents=True, exist_ok=True)

        try:
            logger.info(f"Downloading NVD CPE feed from {self.feed_url}")
            response = requests.get(self.feed_url, stream=True)
            response.raise_for_status()

            total_size = int(response.headers.get('content-length', 0))
            downloaded = 0

            with open(self.download_file, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)
                        if total_size > 0:
                            percent = (downloaded / total_size) * 100
                            logger.info(f"Download progress: {percent:.1f}%")

            logger.info(f"Downloaded feed to {self.download_file}")
            return True
        except Exception as e:
            logger.error(f"Failed to download feed: {e}")
            return False

    def extract_feed(self):
        """Extract the downloaded tar.gz file."""
        if not self.download_file.exists():
            logger.error(f"Download file not found: {self.download_file}")
            return False

        try:
            logger.info(f"Extracting {self.download_file}")
            with tarfile.open(self.download_file, 'r:gz') as tar:
                tar.extractall(path=self.extract_dir)

            logger.info(f"Extracted feed to {self.extract_dir}")
            return True
        except Exception as e:
            logger.error(f"Failed to extract feed: {e}")
            return False

    def get_json_files(self):
        """Get list of JSON chunk files from extracted data."""
        chunks_dir = self.extract_dir / "nvdcpe-2.0-chunks"
        if not chunks_dir.exists():
            logger.error(f"Chunks directory not found: {chunks_dir}")
            return []

        json_files = list(chunks_dir.glob("nvdcpe-2.0-chunk-*.json"))
        json_files.sort()  # Ensure consistent order

        logger.info(f"Found {len(json_files)} JSON chunk files")
        return json_files

    def cleanup_download(self):
        """Remove the downloaded tar.gz file to save space."""
        if self.download_file.exists():
            self.download_file.unlink()
            logger.info(f"Cleaned up download file: {self.download_file}")

    def download_and_extract(self, force_download=False, cleanup=True):
        """Complete download and extraction process."""
        success = self.download_feed(force_download)
        if not success:
            return False

        success = self.extract_feed()
        if not success:
            return False

        if cleanup:
            self.cleanup_download()

        return True
