# CPE Database Tool

A comprehensive tool for downloading, parsing, and indexing **CPE (Common Platform Enumeration)** product data from the NVD (National Vulnerability Database) into Elasticsearch for fast searching.

## Features

- 📥 **Download** NVD CPE 2.0 feed data automatically
- 🔄 **Parse** JSON chunk files and extract CPE information
- 📚 **Index** data into Elasticsearch with optimized mapping
- 🔍 **Search** by tool name, website, CPE pattern, vendor, and more
- 📊 **Statistics** and aggregations on the indexed data
- 🖥️ **Interactive** command-line search interface
- 🔄 **Update** database with latest data and generate diff reports
- 📄 **CSV matching** for bulk tool analysis

## Prerequisites

- Python 3.7+
- Elasticsearch 7.x or 8.x running locally
- Internet connection for downloading NVD feed

## Installation

1. Clone or download this project
2. Create a virtual environment (recommended):
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   ```
3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## Configuration

Edit `config.properties` to match your setup:

```properties
# Elasticsearch connection
es.host=localhost
es.port=9200
es.scheme=http

# Index configuration
es.index=cpe-index

# Feed configuration
nvd.feed.url=https://nvd.nist.gov/feeds/json/cpe/2.0/nvdcpe-2.0.tar.gz
nvd.feed.extract.dir=./data/nvd-cpe
```

## Usage

### Command Line Interface

```bash
python main.py [-h] [--config CONFIG] [--verbose] {setup,download,parse-and-index,recreate-index,search-demo,search,full-pipeline,match-csv,update} ...
```

**Global Options:**
- `--config CONFIG`: Configuration file path (default: config.properties)
- `--verbose, -v`: Enable verbose logging

**Available Commands:**

#### 1. Setup Project
```bash
python main.py setup
```
Setup project and create Elasticsearch index.

#### 2. Download Data
```bash
python main.py download [--force]
```
Download NVD CPE feed data.

**Options:**
- `--force`: Force re-download even if data exists

#### 3. Parse and Index Data
```bash
python main.py parse-and-index
```
Parse JSON files and index into Elasticsearch.

#### 4. Recreate Index
```bash
python main.py recreate-index
```
Delete and recreate index with updated mapping.

#### 5. Search Demo
```bash
python main.py search-demo
```
Demonstrate search capabilities with example queries.

#### 6. Interactive Search
```bash
python main.py search
```
Interactive search mode with command-line interface.

**Available search commands in interactive mode:**
- `tool <name>` - Search by tool name
- `website <url>` - Search by website
- `cpe <pattern>` - Search by CPE pattern
- `vendor <name>` - Search by vendor
- `stats` - Show database statistics
- `quit` - Exit interactive mode

#### 7. Full Pipeline
```bash
python main.py full-pipeline [--force-download]
```
Run complete pipeline: setup + download + index + search demo.

**Options:**
- `--force-download`: Force re-download of data during pipeline

**Important Note:** If an Elasticsearch index already exists, the tool will prompt for confirmation before proceeding, as the operation will delete all existing data and create a new index.

#### 8. CSV Tool Matching
```bash
python main.py match-csv CSV_FILE [--tool-col COL] [--website-col COL] [--output OUTPUT]
```
Match tools from a CSV file against the CPE database.

**Arguments:**
- `CSV_FILE`: Path to CSV file containing tools

**Options:**
- `--tool-col COL`: Tool name column index (0-based, default: 1)
- `--website-col COL`: Website column index (0-based, default: 2)
- `--output OUTPUT`: Output file path (default: cpe_matches.csv)

#### 9. Update Database
```bash
python main.py update [--force-download] [--no-diff]
```
Update CPE database with latest data and create diff reports.

**Options:**
- `--force-download`: Force re-download of latest data
- `--no-diff`: Skip diff generation for faster updates

### Quick Start (Full Pipeline)

Run the complete pipeline in one command:
```bash
python main.py full-pipeline
```

This will:
1. Setup Elasticsearch index
2. Download NVD CPE feed
3. Parse and index all data
4. Show search examples

### CSV Tool Matching

Match tools from a CSV file against the CPE database:
```bash
python main.py match-csv your_tools.csv --tool-col 1 --website-col 2 --output results.csv
```

**CSV Format Requirements:**
- Tool name column (default: column 1, 0-based indexing)
- Website column (default: column 2, 0-based indexing)

**Example CSV:**
```csv
rank,tool_name,website,popularity
1,apache,https://httpd.apache.org,1000000
2,nginx,https://nginx.org,800000
```

**What the tool does:**
- Searches CPE database by tool name and website
- Removes http/https prefixes from URLs
- Filters out deprecated CPE entries
- Groups CPE variants by vendor/product (handles version differences)
- Sets version to `*` when multiple versions exist
- Outputs up to 5 CPE matches per tool with full details

### Database Updates

Update your CPE database with the latest data:
```bash
python main.py update
```

This command will:
- Download the latest NVD CPE feed
- Compare with existing database
- Generate diff reports showing changes
- Update the database with new entries
- Create backup of previous state

**Update Options:**
- Use `--force-download` to force fresh download
- Use `--no-diff` to skip diff generation for faster updates

### Step-by-Step Usage

#### 1. Setup Project
```bash
python main.py setup
```

#### 2. Download Data
```bash
python main.py download
```

Force re-download:
```bash
python main.py download --force
```

#### 3. Parse and Index Data
```bash
python main.py parse-and-index
```

#### 4. Search Data

Demo searches:
```bash
python main.py search-demo
```

Interactive search:
```bash
python main.py search
```

### Search Examples

The tool supports various search types:

- **Tool name search**: `tool apache`
- **Website search**: `website github.com`
- **CPE pattern search**: `cpe *apache*`
- **Vendor search**: `vendor microsoft`
- **Statistics**: `stats`

## Search Capabilities

### 1. Search by Tool Name (Fuzzy)
```python
search_client.search_by_tool_name("apache")
```

### 2. Search by Website Reference
```python
search_client.search_by_website("github.com")
```

### 3. Search by Exact CPE
```python
search_client.search_by_exact_cpe("cpe:2.3:a:apache:http_server:2.4.41:*:*:*:*:*:*:*")
```

### 4. Search by Vendor/Product/Version
```python
search_client.search_by_vendor_product(vendor="apache", product="tomcat")
```

### 5. Search Deprecated Entries
```python
search_client.search_deprecated(deprecated=True)
```

### 6. Search by Date Range
```python
search_client.search_by_date_range(start_date="2023-01-01", end_date="2023-12-31")
```

## Data Structure

Each CPE entry contains:
- **cpeName**: Full CPE identifier
- **cpeNameId**: Unique UUID
- **created/lastModified**: Timestamps
- **deprecated**: Boolean status
- **refs**: Array of reference URLs
- **titles**: Array of human-readable titles

## Project Structure

```
├── main.py                    # Main application
├── config_parser.py           # Configuration handling
├── elasticsearch_manager.py   # Elasticsearch operations
├── data_downloader.py         # NVD feed download/extraction
├── data_parser.py            # JSON parsing and indexing
├── cpe_search_client.py      # Search functionality
├── config.properties         # Configuration file
├── requirements.txt          # Python dependencies
└── data/                     # Downloaded and extracted data
    └── nvd-cpe/
        └── nvdcpematch-2.0-chunks/
```

## Troubleshooting

### Elasticsearch Connection Issues
- Ensure Elasticsearch is running: `curl http://localhost:9200`
- Check configuration in `config.properties`
- Verify firewall settings

### Download Issues
- Check internet connection
- Verify NVD feed URL in configuration
- Try force re-download: `python main.py download --force`

### Memory Issues
- The tool processes data in batches (default: 1000 documents)
- Adjust batch size in `data_parser.py` if needed
- Ensure sufficient disk space for extracted data

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## License

This project is open source. Please check the repository for license details.
