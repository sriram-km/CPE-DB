"""Data parser and indexer for NVD CPE feed JSON files."""

import json
import logging
from datetime import datetime
from dateutil.parser import parse as parse_date

logger = logging.getLogger(__name__)


class CPEDataParser:
    """Parses CPE JSON data and prepares it for Elasticsearch indexing."""

    def __init__(self, elasticsearch_manager):
        self.es_manager = elasticsearch_manager

    def parse_json_file(self, json_file):
        """Parse a single JSON chunk file and extract CPE products."""
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)

            if data.get('format') != 'NVD_CPE':
                logger.warning(f"Unexpected format in {json_file}: {data.get('format')}")
                return []

            products = data.get('products', [])
            logger.info(f"Found {len(products)} products in {json_file.name}")

            parsed_products = []
            for product in products:
                parsed_product = self._parse_product(product)
                if parsed_product:
                    parsed_products.append(parsed_product)

            return parsed_products

        except Exception as e:
            logger.error(f"Failed to parse {json_file}: {e}")
            return []

    def _parse_product(self, product):
        """Parse a single product entry."""
        try:
            cpe_data = product.get('cpe', {})

            # Extract basic fields
            parsed = {
                'cpeName': cpe_data.get('cpeName'),
                'cpeNameId': cpe_data.get('cpeNameId'),
                'deprecated': cpe_data.get('deprecated', False)
            }

            # Parse dates
            if 'created' in cpe_data:
                parsed['created'] = self._parse_date(cpe_data['created'])

            if 'lastModified' in cpe_data:
                parsed['lastModified'] = self._parse_date(cpe_data['lastModified'])

            # Parse references
            refs = cpe_data.get('refs', [])
            if refs:
                parsed['refs'] = [
                    {
                        'ref': ref.get('ref'),
                        'type': ref.get('type')
                    }
                    for ref in refs if ref.get('ref')
                ]

            # Parse titles
            titles = cpe_data.get('titles', [])
            if titles:
                parsed['titles'] = [
                    {
                        'lang': title.get('lang'),
                        'title': title.get('title')
                    }
                    for title in titles if title.get('title')
                ]

            return parsed

        except Exception as e:
            logger.error(f"Failed to parse product: {e}")
            return None

    def _parse_date(self, date_string):
        """Parse date string to ISO format."""
        try:
            if date_string:
                # Parse the date and convert to ISO format
                parsed_date = parse_date(date_string)
                return parsed_date.isoformat()
        except Exception as e:
            logger.warning(f"Failed to parse date {date_string}: {e}")
        return None

    def process_and_index_files(self, json_files, batch_size=1000):
        """Process multiple JSON files and index them in batches."""
        total_indexed = 0

        for json_file in json_files:
            logger.info(f"Processing {json_file.name}")
            products = self.parse_json_file(json_file)

            if not products:
                continue

            # Index in batches
            for i in range(0, len(products), batch_size):
                batch = products[i:i + batch_size]
                success = self.es_manager.bulk_index_documents(batch)

                if success:
                    total_indexed += len(batch)
                    logger.info(f"Indexed batch of {len(batch)} documents. Total: {total_indexed}")
                else:
                    logger.error(f"Failed to index batch from {json_file.name}")

        logger.info(f"Total documents indexed: {total_indexed}")
        return total_indexed
