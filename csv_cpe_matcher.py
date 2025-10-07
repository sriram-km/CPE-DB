"""CSV to CPE Matcher - Matches tools from CSV against CPE database."""

import csv
import re
import logging
from urllib.parse import urlparse
from pathlib import Path
from config_parser import Config
from elasticsearch_manager import ElasticsearchManager
from cpe_search_client import CPESearchClient

logger = logging.getLogger(__name__)


class CSVCPEMatcher:
    """Matches CSV tool data against CPE database."""

    def __init__(self, config):
        self.config = config
        self.es_manager = ElasticsearchManager(config)
        self.search_client = CPESearchClient(self.es_manager)

    def clean_website_url(self, website):
        """Clean website URL by removing protocol and trailing paths."""
        if not website:
            return ""

        # Remove http:// or https://
        cleaned = re.sub(r'^https?://', '', website.strip())

        # Parse the URL to get domain and path
        try:
            # Add protocol back for parsing
            if not cleaned.startswith('http'):
                parsed = urlparse(f'http://{cleaned}')
            else:
                parsed = urlparse(cleaned)

            # Get domain
            domain = parsed.netloc or parsed.path.split('/')[0]

            # Include path if it exists but remove trailing slashes and fragments
            path = parsed.path.rstrip('/')
            if path and path != '/':
                return f"{domain}{path}"
            else:
                return domain

        except Exception as e:
            logger.warning(f"Failed to parse URL {website}: {e}")
            return cleaned.split('/')[0]  # Fallback to just domain

    def extract_cpe_components(self, cpe_name):
        """Extract vendor, product, and version from CPE name."""
        # CPE format: cpe:2.3:part:vendor:product:version:update:edition:language:sw_edition:target_sw:target_hw:other
        try:
            parts = cpe_name.split(':')
            if len(parts) >= 6:
                return {
                    'vendor': parts[3],
                    'product': parts[4],
                    'version': parts[5],
                    'full_cpe': cpe_name
                }
        except Exception as e:
            logger.warning(f"Failed to parse CPE {cpe_name}: {e}")

        return None

    def normalize_cpe_for_comparison(self, cpe_name):
        """Create a normalized CPE with version set to * for comparison."""
        try:
            parts = cpe_name.split(':')
            if len(parts) >= 6:
                # Set version to *
                parts[5] = '*'
                return ':'.join(parts)
        except Exception:
            pass
        return cpe_name

    def group_cpe_variants(self, cpe_results):
        """Group CPE results by vendor/product, handling version differences."""
        if not cpe_results or not cpe_results.get('hits'):
            return []

        # Filter out deprecated CPEs
        active_cpes = [hit for hit in cpe_results['hits'] if not hit.get('deprecated', False)]

        if not active_cpes:
            return []

        # Group by vendor/product combination
        groups = {}

        for cpe_hit in active_cpes:
            cpe_name = cpe_hit.get('cpeName', '')
            components = self.extract_cpe_components(cpe_name)

            if components:
                # Create key for grouping (vendor:product)
                group_key = f"{components['vendor']}:{components['product']}"

                if group_key not in groups:
                    groups[group_key] = {
                        'vendor': components['vendor'],
                        'product': components['product'],
                        'versions': set(),
                        'cpes': [],
                        'sample_cpe': cpe_hit
                    }

                groups[group_key]['versions'].add(components['version'])
                groups[group_key]['cpes'].append(cpe_hit)

        # Convert to list and create normalized CPEs
        result_groups = []
        for group_key, group_data in groups.items():
            # If multiple versions exist, create a version-agnostic CPE
            if len(group_data['versions']) > 1 or '*' in group_data['versions']:
                # Use the first CPE as template and set version to *
                sample_cpe = group_data['sample_cpe']['cpeName']
                normalized_cpe = self.normalize_cpe_for_comparison(sample_cpe)

                result_groups.append({
                    'normalized_cpe': normalized_cpe,
                    'vendor': group_data['vendor'],
                    'product': group_data['product'],
                    'version_count': len(group_data['versions']),
                    'versions': sorted(list(group_data['versions'])),
                    'all_cpes': [cpe['cpeName'] for cpe in group_data['cpes']],
                    'sample_data': group_data['sample_cpe']
                })
            else:
                # Single version, keep as is
                cpe_data = group_data['cpes'][0]
                result_groups.append({
                    'normalized_cpe': cpe_data['cpeName'],
                    'vendor': group_data['vendor'],
                    'product': group_data['product'],
                    'version_count': 1,
                    'versions': list(group_data['versions']),
                    'all_cpes': [cpe_data['cpeName']],
                    'sample_data': cpe_data
                })

        return result_groups

    def search_cpe_for_tool(self, tool_name, website):
        """Search CPE database for a tool - prioritize website search over name search."""
        results = {'by_name': [], 'by_website': [], 'combined': []}

        # First priority: Search by website if available
        if website:
            cleaned_website = self.clean_website_url(website)
            logger.info(f"Searching by website (priority 1): {cleaned_website}")
            website_results = self.search_client.search_by_website(cleaned_website, size=50)
            if website_results and website_results.get('hits'):
                results['by_website'] = self.group_cpe_variants(website_results)

                # If website search found results, use only those and skip name search
                if results['by_website']:
                    logger.info(f"Found {len(results['by_website'])} matches by website - skipping name search")
                    for result in results['by_website']:
                        result['found_by'] = 'website'
                    results['combined'] = results['by_website']
                    return results

        # Second priority: Search by tool name only if website search failed or no website provided
        if tool_name:
            logger.info(f"Searching by tool name (priority 2): {tool_name}")
            name_results = self.search_client.search_by_tool_name(tool_name, size=50)
            if name_results and name_results.get('hits'):
                results['by_name'] = self.group_cpe_variants(name_results)

                # Mark results as found by name
                for result in results['by_name']:
                    result['found_by'] = 'name'
                results['combined'] = results['by_name']

        # If no results found by either method
        if not results['combined']:
            logger.info(f"No CPE matches found for tool: {tool_name}, website: {website}")

        return results

    def process_csv_file(self, csv_file_path, tool_name_col, website_col, output_file_path):
        """Process CSV file and match tools against CPE database."""

        # Validate inputs
        csv_path = Path(csv_file_path)
        if not csv_path.exists():
            raise FileNotFoundError(f"CSV file not found: {csv_file_path}")

        # Check if Elasticsearch has data
        doc_count = self.es_manager.get_index_stats()
        if doc_count == 0:
            raise ValueError("CPE database is empty. Please run 'python main.py parse-and-index' first.")

        logger.info(f"Processing CSV file: {csv_file_path}")
        logger.info(f"CPE database contains {doc_count} documents")

        results = []
        stats = {
            'total_rows': 0,
            'tools_with_cpe': 0,
            'tools_without_cpe': 0,
            'total_cpe_matches': 0,
            'matches_by_name': 0,
            'matches_by_website': 0,
            'matches_by_both': 0
        }

        # Read and process CSV
        with open(csv_path, 'r', encoding='utf-8') as csvfile:
            # Detect delimiter
            sample = csvfile.read(1024)
            csvfile.seek(0)
            sniffer = csv.Sniffer()
            delimiter = sniffer.sniff(sample).delimiter

            reader = csv.reader(csvfile, delimiter=delimiter)

            # Skip header if exists
            header = next(reader, None)

            for row_num, row in enumerate(reader, start=1):
                stats['total_rows'] += 1

                # Extract tool data
                tool_name = row[tool_name_col] if len(row) > tool_name_col else ""
                website = row[website_col] if len(row) > website_col else ""

                logger.info(f"Processing row {row_num}: {tool_name}")

                # Search for CPE matches
                cpe_matches = self.search_cpe_for_tool(tool_name, website)

                # Prepare result row
                result_row = {
                    'original_row': row,
                    'row_number': row_num,
                    'tool_name': tool_name,
                    'website': website,
                    'cpe_matches': cpe_matches['combined'],
                    'match_count': len(cpe_matches['combined'])
                }

                # Update statistics
                if result_row['match_count'] > 0:
                    stats['tools_with_cpe'] += 1
                    stats['total_cpe_matches'] += result_row['match_count']

                    # Count by search method
                    for match in cpe_matches['combined']:
                        if match['found_by'] == 'name':
                            stats['matches_by_name'] += 1
                        elif match['found_by'] == 'website':
                            stats['matches_by_website'] += 1
                        elif match['found_by'] == 'both':
                            stats['matches_by_both'] += 1
                else:
                    stats['tools_without_cpe'] += 1

                results.append(result_row)

        # Write results to output file
        self.write_results_to_csv(results, output_file_path, header)

        # Print statistics
        self.print_statistics(stats, output_file_path)

        return results, stats

    def write_results_to_csv(self, results, output_file_path, original_header):
        """Write matching results to CSV file."""
        output_path = Path(output_file_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        with open(output_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)

            # Write header
            extended_header = list(original_header) if original_header else []
            extended_header.extend([
                'cpe_match_count',
                'cpe_1', 'vendor_1', 'product_1', 'versions_1', 'found_by_1',
                'cpe_2', 'vendor_2', 'product_2', 'versions_2', 'found_by_2',
                'cpe_3', 'vendor_3', 'product_3', 'versions_3', 'found_by_3',
                'cpe_4', 'vendor_4', 'product_4', 'versions_4', 'found_by_4',
                'cpe_5', 'vendor_5', 'product_5', 'versions_5', 'found_by_5'
            ])
            writer.writerow(extended_header)

            # Write data rows
            for result in results:
                row = list(result['original_row'])
                row.append(result['match_count'])

                # Add up to 5 CPE matches
                for i in range(5):
                    if i < len(result['cpe_matches']):
                        match = result['cpe_matches'][i]
                        row.extend([
                            match['normalized_cpe'],
                            match['vendor'],
                            match['product'],
                            '|'.join(match['versions']),
                            match['found_by']
                        ])
                    else:
                        row.extend(['', '', '', '', ''])

                writer.writerow(row)

        logger.info(f"Results written to: {output_path}")

    def print_statistics(self, stats, output_file):
        """Print processing statistics."""
        print("\n" + "="*60)
        print("🎯 CPE MATCHING RESULTS")
        print("="*60)
        print(f"📄 Input processed: {stats['total_rows']} tools")
        print(f"✅ Tools with CPE matches: {stats['tools_with_cpe']}")
        print(f"❌ Tools without CPE matches: {stats['tools_without_cpe']}")
        print(f"📊 Success rate: {(stats['tools_with_cpe']/stats['total_rows']*100):.1f}%")
        print()
        print(f"🔍 Total CPE matches found: {stats['total_cpe_matches']}")
        print(f"   📝 Found by name: {stats['matches_by_name']}")
        print(f"   🌐 Found by website: {stats['matches_by_website']}")
        print(f"   🎯 Found by both: {stats['matches_by_both']}")
        print()
        print(f"💾 Results saved to: {output_file}")
        print("="*60)
