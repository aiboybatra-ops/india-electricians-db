# India Electricians Scraper

A comprehensive web scraping system to collect electrician and electrical lineman data from various Indian business directories and forums.

## Features

- **Multiple Data Sources**: Scrapes from Google Maps, JustDial, IndiaMART, Sulekha, and Urban Company
- **Pan-India Coverage**: Covers all 28 states, 8 union territories, and 200+ major cities
- **Data Deduplication**: Automatically removes duplicate entries based on phone numbers
- **Multiple Export Formats**: Exports to CSV, JSON, Excel, and SQLite database
- **Anti-Detection**: User agent rotation, request delays, and proxy support
- **Parallel Processing**: Optional parallel scraping for faster data collection
- **Resume Support**: Data is saved incrementally, so you can resume interrupted scrapes

## Data Collected

For each electrician/lineman, the system collects:

- **Name** and Business Name
- **Phone Number** (primary identifier)
- **City** and **State**
- **Full Address** and Pincode
- **Email** and Website (if available)
- **Services Offered**
- **Years of Experience**
- **Rating** and **Review Count**
- **Verification Status**
- **Source URL**

## Installation

### Prerequisites

- Python 3.8 or higher
- pip (Python package manager)

### Setup

1. Clone or download this project:
```bash
cd india_electricians_scraper
```

2. Create a virtual environment (recommended):
```bash
python -m venv venv
source venv/bin/activate  # On macOS/Linux
# or
venv\Scripts\activate  # On Windows
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Configure environment variables (optional):
```bash
cp .env.example .env
# Edit .env with your settings
```

## Usage

### Basic Usage

Scrape all cities from all sources:
```bash
python main.py
```

### Specific States

Scrape only specific states:
```bash
python main.py --states "Maharashtra" "Gujarat" "Karnataka"
```

### Specific Cities

Scrape only specific cities:
```bash
python main.py --cities "Mumbai" "Pune" "Bangalore"
```

### Specific Scrapers

Use only specific data sources:
```bash
python main.py --scrapers justdial indiamart
```

Available scrapers: `google`, `justdial`, `indiamart`, `sulekha`, `urbancompany`

### Parallel Scraping

For faster scraping (use with caution):
```bash
python main.py --parallel --workers 4
```

### Export Options

Export in specific format:
```bash
python main.py --export csv
```

Options: `csv`, `json`, `excel`, `all`

### View Statistics

Check database statistics without scraping:
```bash
python main.py --stats-only
```

### Verbose Mode

Enable detailed logging:
```bash
python main.py --verbose
```

## Configuration

### Environment Variables

Create a `.env` file with:

```env
# Google Places API (optional, for better Google results)
GOOGLE_PLACES_API_KEY=your_api_key_here

# Proxy Configuration (optional)
PROXY_HOST=proxy.example.com
PROXY_PORT=8080
PROXY_USERNAME=user
PROXY_PASSWORD=pass

# Scraper Settings
REQUEST_DELAY_MIN=2
REQUEST_DELAY_MAX=5
MAX_RETRIES=3

# Output
OUTPUT_DIR=./output
LOG_DIR=./logs
```

### Adding Custom Locations

Edit `src/config.py` to add or modify locations:

```python
INDIAN_LOCATIONS = {
    "Your State": ["City1", "City2", "City3"],
    ...
}
```

## Output

### CSV Format

```csv
name,phone,city,state,address,rating,source
"ABC Electricians","9876543210","Mumbai","Maharashtra","Shop 123, ...",4.5,"justdial"
```

### JSON Format

```json
[
  {
    "name": "ABC Electricians",
    "phone": "9876543210",
    "city": "Mumbai",
    "state": "Maharashtra",
    "address": "Shop 123, ...",
    "rating": 4.5,
    "source": "justdial"
  }
]
```

### SQLite Database

The data is also stored in `electricians_data.db` for querying:

```python
from src.storage import DataStorage

storage = DataStorage()

# Get all electricians from Mumbai
mumbai_electricians = storage.load_from_database(city="Mumbai")

# Get statistics
stats = storage.get_statistics()
print(stats)
```

## Project Structure

```
india_electricians_scraper/
├── main.py                    # Main orchestrator script
├── requirements.txt           # Python dependencies
├── .env.example              # Environment variables template
├── src/
│   ├── __init__.py
│   ├── config.py             # Configuration and settings
│   ├── models.py             # Data models
│   ├── storage.py            # Data storage (CSV, JSON, SQLite)
│   └── scrapers/
│       ├── __init__.py       # Base scraper class
│       ├── google_scraper.py
│       ├── justdial_scraper.py
│       ├── indiamart_scraper.py
│       ├── sulekha_scraper.py
│       └── urbancompany_scraper.py
├── output/                   # Generated data files
└── logs/                     # Log files
```

## Legal & Ethical Considerations

⚠️ **Important**: Web scraping may be subject to legal restrictions. Please:

1. **Respect robots.txt**: Check each website's robots.txt before scraping
2. **Rate Limiting**: Use appropriate delays between requests (configured in `.env`)
3. **Terms of Service**: Review each website's terms of service
4. **Personal Data**: Handle collected personal data responsibly
5. **Commercial Use**: Some websites prohibit commercial use of scraped data

This tool is provided for educational purposes. Users are responsible for ensuring their use complies with applicable laws and website terms of service.

## Troubleshooting

### Common Issues

1. **No data collected from JustDial**
   - JustDial has strong anti-bot measures
   - Try using proxies or reducing request frequency

2. **Google API quota exceeded**
   - Use a valid Google Places API key
   - Or use web scraping mode (no API key)

3. **Connection errors**
   - Check your internet connection
   - Some websites may block your IP
   - Consider using rotating proxies

4. **Import errors**
   - Make sure you're in the project directory
   - Ensure virtual environment is activated
   - Run `pip install -r requirements.txt` again

### Logs

Check logs in the `logs/` directory for detailed error information.

## Contributing

Feel free to submit issues and pull requests to improve the scrapers or add new data sources.

## License

This project is for educational purposes. Use responsibly and ethically.
