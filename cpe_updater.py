"""CPE Database Updater - Handles updates and creates diffs with old data."""

import json
import csv
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Set, Tuple
from dataclasses import dataclass, asdict
import hashlib

from config_parser import Config
from elasticsearch_manager import ElasticsearchManager
from data_downloader import NVDDataDownloader
from data_parser import CPEDataParser
from cpe_search_client import CPESearchClient

logger = logging.getLogger(__name__)


@dataclass
class CPEEntry:
    """Represents a CPE entry for comparison."""
    cpeName: str
    cpeNameId: str
    created: str
    lastModified: str
    deprecated: bool
    titles: List[Dict]
    refs: List[Dict]

    def __hash__(self):
        return hash(self.cpeName)

    def to_dict(self):
        return asdict(self)


@dataclass
class UpdateStats:
    """Statistics for an update operation."""
    total_old: int = 0
    total_new: int = 0
    added: int = 0
    modified: int = 0
    deprecated: int = 0
    unchanged: int = 0

    def to_dict(self):
        return asdict(self)


class CPEUpdater:
    """Handles CPE database updates with diff generation."""

    def __init__(self, config):
        self.config = config
        self.es_manager = ElasticsearchManager(config)
        self.search_client = CPESearchClient(self.es_manager)
        self.downloader = NVDDataDownloader(config)
        self.parser = CPEDataParser(self.es_manager)

        # Backup and diff directories
        self.backup_dir = Path("./backups")
        self.diff_dir = Path("./diffs")
        self.backup_dir.mkdir(exist_ok=True)
        self.diff_dir.mkdir(exist_ok=True)

    def create_backup(self) -> str:
        """Create a backup of current CPE data from Elasticsearch."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_file = self.backup_dir / f"cpe_backup_{timestamp}.json"

        logger.info(f"Creating backup of current CPE data: {backup_file}")

        try:
            # Get all documents from Elasticsearch
            all_docs = self.search_client.get_all_documents()

            with open(backup_file, 'w') as f:
                json.dump(all_docs, f, indent=2, default=str)

            logger.info(f"Backup created successfully: {backup_file}")
            return str(backup_file)

        except Exception as e:
            logger.error(f"Failed to create backup: {e}")
            return None

    def load_cpe_entries_from_backup(self, backup_file: str) -> Dict[str, CPEEntry]:
        """Load CPE entries from backup file."""
        entries = {}

        try:
            with open(backup_file, 'r') as f:
                data = json.load(f)

            for doc in data:
                entry = CPEEntry(
                    cpeName=doc.get('cpeName', ''),
                    cpeNameId=doc.get('cpeNameId', ''),
                    created=doc.get('created', ''),
                    lastModified=doc.get('lastModified', ''),
                    deprecated=doc.get('deprecated', False),
                    titles=doc.get('titles', []),
                    refs=doc.get('refs', [])
                )
                entries[entry.cpeName] = entry

        except Exception as e:
            logger.error(f"Failed to load backup file: {e}")

        return entries

    def load_cpe_entries_from_json_files(self, json_files: List[Path]) -> Dict[str, CPEEntry]:
        """Load CPE entries from JSON chunk files."""
        entries = {}

        for json_file in json_files:
            try:
                with open(json_file, 'r') as f:
                    data = json.load(f)

                for product in data.get('products', []):
                    cpe_data = product.get('cpe', {})

                    entry = CPEEntry(
                        cpeName=cpe_data.get('cpeName', ''),
                        cpeNameId=cpe_data.get('cpeNameId', ''),
                        created=cpe_data.get('created', ''),
                        lastModified=cpe_data.get('lastModified', ''),
                        deprecated=cpe_data.get('deprecated', False),
                        titles=cpe_data.get('titles', []),
                        refs=cpe_data.get('refs', [])
                    )

                    if entry.cpeName:
                        entries[entry.cpeName] = entry

            except Exception as e:
                logger.error(f"Failed to process file {json_file}: {e}")

        return entries

    def generate_diff(self, old_entries: Dict[str, CPEEntry],
                     new_entries: Dict[str, CPEEntry]) -> Tuple[Dict, UpdateStats]:
        """Generate diff between old and new CPE entries."""

        old_cpes = set(old_entries.keys())
        new_cpes = set(new_entries.keys())

        added_cpes = new_cpes - old_cpes
        removed_cpes = old_cpes - new_cpes
        common_cpes = old_cpes & new_cpes

        diff_data = {
            'added': [],
            'modified': [],
            'deprecated': [],
            'removed': [],
            'unchanged': []
        }

        stats = UpdateStats()
        stats.total_old = len(old_entries)
        stats.total_new = len(new_entries)

        # Process added entries
        for cpe_name in added_cpes:
            entry = new_entries[cpe_name]
            diff_data['added'].append(entry.to_dict())
            stats.added += 1

        # Process removed entries
        for cpe_name in removed_cpes:
            entry = old_entries[cpe_name]
            diff_data['removed'].append(entry.to_dict())

        # Process common entries for modifications
        for cpe_name in common_cpes:
            old_entry = old_entries[cpe_name]
            new_entry = new_entries[cpe_name]

            # Check for modifications
            if (old_entry.lastModified != new_entry.lastModified or
                old_entry.deprecated != new_entry.deprecated or
                old_entry.titles != new_entry.titles or
                old_entry.refs != new_entry.refs):

                modification = {
                    'cpeName': cpe_name,
                    'old': old_entry.to_dict(),
                    'new': new_entry.to_dict(),
                    'changes': self._get_field_changes(old_entry, new_entry)
                }
                diff_data['modified'].append(modification)
                stats.modified += 1

                # Check if newly deprecated
                if not old_entry.deprecated and new_entry.deprecated:
                    diff_data['deprecated'].append(new_entry.to_dict())
                    stats.deprecated += 1
            else:
                diff_data['unchanged'].append(new_entry.to_dict())
                stats.unchanged += 1

        return diff_data, stats

    def _get_field_changes(self, old_entry: CPEEntry, new_entry: CPEEntry) -> Dict:
        """Get specific field changes between entries."""
        changes = {}

        if old_entry.lastModified != new_entry.lastModified:
            changes['lastModified'] = {
                'old': old_entry.lastModified,
                'new': new_entry.lastModified
            }

        if old_entry.deprecated != new_entry.deprecated:
            changes['deprecated'] = {
                'old': old_entry.deprecated,
                'new': new_entry.deprecated
            }

        if old_entry.titles != new_entry.titles:
            changes['titles'] = {
                'old': old_entry.titles,
                'new': new_entry.titles
            }

        if old_entry.refs != new_entry.refs:
            changes['refs'] = {
                'old': old_entry.refs,
                'new': new_entry.refs
            }

        return changes

    def save_diff_report(self, diff_data: Dict, stats: UpdateStats,
                        timestamp: str) -> str:
        """Save diff report to file."""
        diff_file = self.diff_dir / f"cpe_diff_{timestamp}.json"

        report = {
            'timestamp': timestamp,
            'statistics': stats.to_dict(),
            'changes': diff_data
        }

        with open(diff_file, 'w') as f:
            json.dump(report, f, indent=2, default=str)

        logger.info(f"Diff report saved: {diff_file}")
        return str(diff_file)

    def create_csv_diff_report(self, diff_data: Dict, stats: UpdateStats,
                              timestamp: str) -> str:
        """Create a CSV summary of the diff."""
        csv_file = self.diff_dir / f"cpe_diff_summary_{timestamp}.csv"

        with open(csv_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)

            # Write header
            writer.writerow(['Change Type', 'CPE Name', 'Product Title', 'Vendor',
                           'Product', 'Version', 'References', 'Deprecated'])

            # Write added entries
            for entry in diff_data['added']:
                title = entry['titles'][0]['title'] if entry['titles'] else ''
                cpe_parts = entry['cpeName'].split(':')
                vendor = cpe_parts[3] if len(cpe_parts) > 3 else ''
                product = cpe_parts[4] if len(cpe_parts) > 4 else ''
                version = cpe_parts[5] if len(cpe_parts) > 5 else ''
                refs = '; '.join([ref['ref'] for ref in entry.get('refs', [])])

                writer.writerow(['ADDED', entry['cpeName'], title, vendor,
                               product, version, refs, entry['deprecated']])

            # Write modified entries
            for mod in diff_data['modified']:
                entry = mod['new']
                title = entry['titles'][0]['title'] if entry['titles'] else ''
                cpe_parts = entry['cpeName'].split(':')
                vendor = cpe_parts[3] if len(cpe_parts) > 3 else ''
                product = cpe_parts[4] if len(cpe_parts) > 4 else ''
                version = cpe_parts[5] if len(cpe_parts) > 5 else ''
                refs = '; '.join([ref['ref'] for ref in entry.get('refs', [])])

                writer.writerow(['MODIFIED', entry['cpeName'], title, vendor,
                               product, version, refs, entry['deprecated']])

            # Write deprecated entries
            for entry in diff_data['deprecated']:
                title = entry['titles'][0]['title'] if entry['titles'] else ''
                cpe_parts = entry['cpeName'].split(':')
                vendor = cpe_parts[3] if len(cpe_parts) > 3 else ''
                product = cpe_parts[4] if len(cpe_parts) > 4 else ''
                version = cpe_parts[5] if len(cpe_parts) > 5 else ''
                refs = '; '.join([ref['ref'] for ref in entry.get('refs', [])])

                writer.writerow(['DEPRECATED', entry['cpeName'], title, vendor,
                               product, version, refs, True])

        logger.info(f"CSV diff summary saved: {csv_file}")
        return str(csv_file)

    def update_database(self, force_download=False, create_diff=True) -> Dict:
        """
        Update the CPE database with latest data and optionally create diff.

        Returns:
            Dict with update results including file paths and statistics
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        logger.info("Starting CPE database update...")

        # Step 1: Create backup of current data
        backup_file = None
        old_entries = {}

        if create_diff:
            backup_file = self.create_backup()
            if backup_file:
                old_entries = self.load_cpe_entries_from_backup(backup_file)
                logger.info(f"Loaded {len(old_entries)} entries from current database")

        # Step 2: Download latest data
        logger.info("Downloading latest NVD CPE feed...")
        if not self.downloader.download_and_extract(force_download=force_download):
            return {
                'success': False,
                'error': 'Failed to download latest data'
            }

        # Step 3: Load new data
        json_files = self.downloader.get_json_files()
        if not json_files:
            return {
                'success': False,
                'error': 'No JSON files found after download'
            }

        new_entries = self.load_cpe_entries_from_json_files(json_files)
        logger.info(f"Loaded {len(new_entries)} entries from new data")

        # Step 4: Generate diff if requested
        diff_file = None
        csv_diff_file = None
        stats = None

        if create_diff and old_entries:
            logger.info("Generating diff between old and new data...")
            diff_data, stats = self.generate_diff(old_entries, new_entries)

            # Save diff reports
            diff_file = self.save_diff_report(diff_data, stats, timestamp)
            csv_diff_file = self.create_csv_diff_report(diff_data, stats, timestamp)

            logger.info(f"Diff generated - Added: {stats.added}, Modified: {stats.modified}, "
                       f"Deprecated: {stats.deprecated}, Unchanged: {stats.unchanged}")

        # Step 5: Update Elasticsearch index
        logger.info("Updating Elasticsearch index...")

        # Clear existing index
        if not self.es_manager.delete_index():
            logger.warning("Failed to delete existing index")

        if not self.es_manager.create_index():
            return {
                'success': False,
                'error': 'Failed to recreate index'
            }

        # Index new data
        total_indexed = self.parser.process_and_index_files(json_files)

        if total_indexed == 0:
            return {
                'success': False,
                'error': 'Failed to index any documents'
            }

        # Return results
        result = {
            'success': True,
            'timestamp': timestamp,
            'total_indexed': total_indexed,
            'backup_file': backup_file,
            'diff_file': diff_file,
            'csv_diff_file': csv_diff_file
        }

        if stats:
            result['statistics'] = stats.to_dict()

        logger.info(f"Update completed successfully - {total_indexed} documents indexed")
        return result

    def get_update_summary(self, result: Dict) -> str:
        """Generate a human-readable summary of the update."""
        if not result.get('success'):
            return f"❌ Update failed: {result.get('error', 'Unknown error')}"

        summary = [
            f"✅ CPE Database updated successfully!",
            f"📅 Update timestamp: {result['timestamp']}",
            f"📊 Total documents indexed: {result['total_indexed']}"
        ]

        if 'statistics' in result:
            stats = result['statistics']
            summary.extend([
                f"",
                f"📈 Update Statistics:",
                f"   • Added entries: {stats['added']}",
                f"   • Modified entries: {stats['modified']}",
                f"   • Newly deprecated: {stats['deprecated']}",
                f"   • Unchanged entries: {stats['unchanged']}",
                f"   • Total old entries: {stats['total_old']}",
                f"   • Total new entries: {stats['total_new']}"
            ])

        if result.get('backup_file'):
            summary.append(f"💾 Backup saved: {result['backup_file']}")

        if result.get('diff_file'):
            summary.append(f"📄 Diff report: {result['diff_file']}")

        if result.get('csv_diff_file'):
            summary.append(f"📊 CSV diff summary: {result['csv_diff_file']}")

        return "\n".join(summary)
