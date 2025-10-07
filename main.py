#!/usr/bin/env python3
"""Main application for CPE Database management and search."""

import argparse
import logging
import sys
from pathlib import Path

from config_parser import Config
from elasticsearch_manager import ElasticsearchManager
from data_downloader import NVDDataDownloader
from data_parser import CPEDataParser
from cpe_search_client import CPESearchClient
from cpe_updater import CPEUpdater


def setup_logging(verbose=False):
    """Setup logging configuration."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )


def setup_project(config, force_recreate=False):
    """Setup the project: test Elasticsearch and create index."""
    print("🔧 Setting up CPE Database project...")

    # Initialize Elasticsearch manager
    es_manager = ElasticsearchManager(config)

    # Test connection
    print("📡 Testing Elasticsearch connection...")
    if not es_manager.test_connection():
        print("❌ Failed to connect to Elasticsearch. Please ensure it's running.")
        return False

    print("✅ Connected to Elasticsearch successfully!")

    # Check if index already exists
    if es_manager.es.indices.exists(index=es_manager.index_name):
        if not force_recreate:
            print(f"⚠️  Index '{es_manager.index_name}' already exists with data.")
            doc_count = es_manager.get_index_stats()
            print(f"📊 Current index contains {doc_count} documents.")
            print("🚨 Proceeding will DELETE ALL existing data and create a new index.")

            while True:
                response = input("Do you want to continue? (yes/no): ").strip().lower()
                if response in ['yes', 'y']:
                    break
                elif response in ['no', 'n']:
                    print("❌ Operation cancelled by user.")
                    return False
                else:
                    print("Please enter 'yes' or 'no'.")

        # Delete existing index
        print("🗑️  Deleting existing index...")
        if not es_manager.delete_index():
            print("❌ Failed to delete existing index.")
            return False

    # Create index
    print("📚 Creating CPE index...")
    if es_manager.create_index():
        print("✅ CPE index created successfully!")
    else:
        print("❌ Failed to create CPE index.")
        return False

    return True


def download_data(config, force=False):
    """Download and extract NVD CPE feed data."""
    print("📥 Downloading NVD CPE feed data...")

    downloader = NVDDataDownloader(config)

    if downloader.download_and_extract(force_download=force):
        print("✅ Data downloaded and extracted successfully!")

        # List available files
        json_files = downloader.get_json_files()
        print(f"📄 Found {len(json_files)} JSON chunk files ready for processing.")
        return True
    else:
        print("❌ Failed to download or extract data.")
        return False


def parse_and_index(config):
    """Parse JSON files and index data into Elasticsearch."""
    print("🔄 Parsing and indexing CPE data...")

    # Initialize components
    es_manager = ElasticsearchManager(config)
    downloader = NVDDataDownloader(config)
    parser = CPEDataParser(es_manager)

    # Get JSON files
    json_files = downloader.get_json_files()
    if not json_files:
        print("❌ No JSON files found. Please download data first.")
        return False

    print(f"📄 Processing {len(json_files)} JSON files...")

    # Process and index
    total_indexed = parser.process_and_index_files(json_files)

    if total_indexed > 0:
        print(f"✅ Successfully indexed {total_indexed} CPE entries!")

        # Show final stats
        final_count = es_manager.get_index_stats()
        print(f"📊 Total documents in index: {final_count}")
        return True
    else:
        print("❌ Failed to index any documents.")
        return False


def search_demo(config):
    """Demonstrate search capabilities."""
    print("🔍 Demonstrating search capabilities...")

    # Initialize components
    es_manager = ElasticsearchManager(config)
    search_client = CPESearchClient(es_manager)

    # Check if index has data
    doc_count = es_manager.get_index_stats()
    if doc_count == 0:
        print("❌ No data in index. Please run parse-and-index first.")
        return False

    print(f"📊 Index contains {doc_count} documents.")

    # Demo searches
    print("\n🔍 Example Search 1: Tool name 'apache'")
    results = search_client.search_by_tool_name("apache", size=3)
    if results:
        print(f"   Found {results['total']} results. Showing first 3:")
        for i, hit in enumerate(results['hits'], 1):
            print(f"   {i}. {hit.get('cpeName', 'N/A')}")
            if hit.get('titles'):
                print(f"      Title: {hit['titles'][0].get('title', 'N/A')}")

    print("\n🔍 Example Search 2: Website 'github.com'")
    results = search_client.search_by_website("github.com", size=3)
    if results:
        print(f"   Found {results['total']} results. Showing first 3:")
        for i, hit in enumerate(results['hits'], 1):
            print(f"   {i}. {hit.get('cpeName', 'N/A')}")
            if hit.get('refs'):
                print(f"      References:")
                for ref in hit['refs']:
                    ref_url = ref.get('ref', 'N/A')
                    ref_type = ref.get('type', 'N/A')
                    print(f"        - {ref_url} (Type: {ref_type})")

    print("\n🔍 Example Search 3: Vendor/Product search")
    results = search_client.search_by_vendor_product(vendor="apache", size=3)
    if results:
        print(f"   Found {results['total']} results for vendor 'apache'. Showing first 3:")
        for i, hit in enumerate(results['hits'], 1):
            print(f"   {i}. {hit.get('cpeName', 'N/A')}")

    # Show statistics
    print("\n📈 Index Statistics:")
    stats = search_client.get_statistics()
    if stats:
        print(f"   Total documents: {stats['total_documents']}")
        if stats['deprecated_breakdown']:
            for status, count in stats['deprecated_breakdown'].items():
                status_text = "deprecated" if status else "active"
                print(f"   {status_text.capitalize()}: {count}")

    return True


def interactive_search(config):
    """Interactive search mode."""
    print("🔍 Interactive Search Mode")
    print("Available commands:")
    print("  1. tool <name>     - Search by tool name")
    print("  2. website <url>   - Search by website")
    print("  3. cpe <pattern>   - Search by CPE pattern")
    print("  4. vendor <name>   - Search by vendor")
    print("  5. stats           - Show statistics")
    print("  6. quit            - Exit")

    # Initialize components
    es_manager = ElasticsearchManager(config)
    search_client = CPESearchClient(es_manager)

    while True:
        try:
            command = input("\n> ").strip().lower()

            if command == "quit" or command == "exit":
                break

            parts = command.split(maxsplit=1)
            if len(parts) < 1:
                continue

            cmd = parts[0]
            query = parts[1] if len(parts) > 1 else ""

            if cmd == "tool" and query:
                results = search_client.search_by_tool_name(query, size=5)
                print_search_results(results)

            elif cmd == "website" and query:
                results = search_client.search_by_website(query, size=5)
                print_search_results(results)

            elif cmd == "cpe" and query:
                results = search_client.search_by_cpe_pattern(query, size=5)
                print_search_results(results)

            elif cmd == "vendor" and query:
                results = search_client.search_by_vendor_product(vendor=query, size=5)
                print_search_results(results)

            elif cmd == "stats":
                stats = search_client.get_statistics()
                if stats:
                    print(f"Total documents: {stats['total_documents']}")
                    for status, count in stats.get('deprecated_breakdown', {}).items():
                        status_text = "deprecated" if status else "active"
                        print(f"{status_text.capitalize()}: {count}")

            else:
                print("Invalid command. Type 'quit' to exit.")

        except KeyboardInterrupt:
            break
        except Exception as e:
            print(f"Error: {e}")

    print("Goodbye! 👋")


def print_search_results(results):
    """Print search results in a formatted way."""
    if not results:
        print("❌ Search failed.")
        return

    print(f"Found {results['total']} results:")
    for i, hit in enumerate(results['hits'], 1):
        print(f"{i}. {hit.get('cpeName', 'N/A')}")
        if hit.get('titles'):
            print(f"   Title: {hit['titles'][0].get('title', 'N/A')}")
        if hit.get('refs'):
            print(f"   References:")
            for ref in hit['refs']:
                ref_url = ref.get('ref', 'N/A')
                ref_type = ref.get('type', 'N/A')
                print(f"     - {ref_url} (Type: {ref_type})")
        print()


def recreate_index(config):
    """Recreate the index with updated mapping for enhanced search."""
    print("🔄 Recreating index with enhanced mapping...")

    # Initialize Elasticsearch manager
    es_manager = ElasticsearchManager(config)

    # Test connection first
    if not es_manager.test_connection():
        print("❌ Failed to connect to Elasticsearch. Please ensure it's running.")
        return False

    # Recreate index
    if es_manager.recreate_index():
        print("✅ Index recreated successfully with enhanced mapping!")
        print("📝 Now supports exact matching and improved search ranking.")
        return True
    else:
        print("❌ Failed to recreate index.")
        return False


def update_cpe_database(config, force_download=False, no_diff=False):
    """Update CPE database with latest data and create diff reports."""
    print("🔄 Updating CPE database with latest data...")

    try:
        # Initialize updater
        updater = CPEUpdater(config)

        # Perform update
        result = updater.update_database(
            force_download=force_download,
            create_diff=not no_diff
        )

        # Print summary
        summary = updater.get_update_summary(result)
        print(summary)

        return result.get('success', False)

    except Exception as e:
        print(f"❌ Update failed: {e}")
        return False


def match_csv_cpes(config, csv_file, tool_col, website_col, output_file):
    """Match tools from CSV against CPE database."""
    print("🔍 Matching CSV tools against CPE database...")

    try:
        from csv_cpe_matcher import CSVCPEMatcher

        # Initialize matcher
        matcher = CSVCPEMatcher(config)

        # Process the CSV file
        results, stats = matcher.process_csv_file(csv_file, tool_col, website_col, output_file)

        return True

    except FileNotFoundError as e:
        print(f"❌ File not found: {e}")
        return False
    except ValueError as e:
        print(f"❌ Error: {e}")
        return False
    except Exception as e:
        print(f"❌ Failed to process CSV: {e}")
        return False


def main():
    """Main application entry point."""
    parser = argparse.ArgumentParser(description="CPE Database Management Tool")
    parser.add_argument("--config", default="config.properties", help="Configuration file path")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose logging")

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Setup command
    subparsers.add_parser("setup", help="Setup project and create Elasticsearch index")

    # Download command
    download_parser = subparsers.add_parser("download", help="Download NVD CPE feed data")
    download_parser.add_argument("--force", action="store_true", help="Force re-download")

    # Parse and index command
    subparsers.add_parser("parse-and-index", help="Parse JSON files and index into Elasticsearch")

    # Recreate index command
    subparsers.add_parser("recreate-index", help="Delete and recreate index with updated mapping")

    # Search demo command
    subparsers.add_parser("search-demo", help="Demonstrate search capabilities")

    # Interactive search command
    subparsers.add_parser("search", help="Interactive search mode")

    # Full pipeline command
    full_parser = subparsers.add_parser("full-pipeline", help="Run complete pipeline: setup + download + index")
    full_parser.add_argument("--force-download", action="store_true", help="Force re-download of data")

    # CSV CPE matching command
    csv_parser = subparsers.add_parser("match-csv", help="Match tools from CSV against CPE database")
    csv_parser.add_argument("csv_file", help="Path to CSV file containing tools")
    csv_parser.add_argument("--tool-col", type=int, default=1, help="Tool name column index (0-based, default: 1)")
    csv_parser.add_argument("--website-col", type=int, default=2, help="Website column index (0-based, default: 2)")
    csv_parser.add_argument("--output", default="cpe_matches.csv", help="Output file path (default: cpe_matches.csv)")

    # Update command
    update_parser = subparsers.add_parser("update", help="Update CPE database with latest data and create diff")
    update_parser.add_argument("--force-download", action="store_true", help="Force re-download of latest data")
    update_parser.add_argument("--no-diff", action="store_true", help="Skip diff generation (faster update)")

    args = parser.parse_args()

    # Setup logging
    setup_logging(args.verbose)

    # Load configuration
    try:
        config = Config(args.config)
    except Exception as e:
        print(f"❌ Failed to load configuration: {e}")
        return 1

    # Execute command
    if args.command == "setup":
        success = setup_project(config)

    elif args.command == "download":
        success = download_data(config, force=args.force)

    elif args.command == "parse-and-index":
        success = parse_and_index(config)

    elif args.command == "search-demo":
        success = search_demo(config)

    elif args.command == "search":
        success = interactive_search(config)
        return 0  # Interactive mode always succeeds

    elif args.command == "full-pipeline":
        print("🚀 Running full pipeline...")
        success = (setup_project(config) and
                  download_data(config, force=args.force_download) and
                  parse_and_index(config))
        if success:
            print("🎉 Full pipeline completed successfully!")
            search_demo(config)

    elif args.command == "recreate-index":
        success = recreate_index(config)

    elif args.command == "match-csv":
        # For CSV matching, directly call the function
        success = match_csv_cpes(config, args.csv_file, args.tool_col, args.website_col, args.output)

    elif args.command == "update":
        # For update, directly call the function
        success = update_cpe_database(config, force_download=args.force_download, no_diff=args.no_diff)

    else:
        parser.print_help()
        return 1

    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
