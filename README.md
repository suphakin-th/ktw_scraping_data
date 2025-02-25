# KTW Scraper: Zero to Hero Guide

Author : BabylVoob

Version: 0.1.0

Last Modified: 2025/02/10

------

This guide will walk you through setting up, configuring, and running the KTW web scraper, including how to automate it with cron jobs.

## Table of Contents

- [Introduction](#introduction)
- [System Requirements](#system-requirements)
- [Installation](#installation)
- [Configuration](#configuration)
- [Running the Scraper](#running-the-scraper)
- [Python Integration](#python-integration)
- [Automation with Cron](#automation-with-cron)
- [Troubleshooting](#troubleshooting)
- [Advanced Usage](#advanced-usage)

## Introduction

The KTW Scraper is a Rust application that extracts product information from the KTW website, tracking product details, prices, and stock information. It stores the data in CSV format and can notify you of changes via Telegram.

Key features:

- Scrapes product details from all pages
- Tracks stock changes
- Logs in to access restricted information
- Sends notifications via Telegram
- Integrates with Python for additional data processing

## System Requirements

- Rust (latest stable version)
- Cargo package manager
- Python 3.x (for the send_data.py script)
- Internet connection
- Linux/macOS/Windows OS

## Installation

### 1. Clone the repository or create the project

```bash
# Create a new Rust project
cd ktw_scraper
```

### 2. Set up the project structure

Create the following files in your project:

- `src/main.rs` (the main scraper code)
- `src/config.rs` (configuration module)
- `src/config.json` (configuration file)
- `send_data.py` (Python script for additional data processing)

### 3. Install dependencies And Run (Rust auto install package after run)

```bash
cargo run
```

## Configuration

### 1. Create the config.rs file

Create `src/config.rs` with the following content:

```rust
use serde::{Deserialize, Serialize};
use std::error::Error as StdError;
use std::fs::File;
use std::io::BufReader;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AppSettings {
    pub base_url: String,
    pub all_p_page: String,
    pub shop_url: String,
    pub user_name: String,
    pub password: String,
    pub csv_path: String,
    pub telegram_token: String, 
    pub chat_id: String,
    pub chunk_size: u32,
}

impl AppSettings {
    pub fn from_file(path: &str) -> Result<Self, Box<dyn StdError>> {
        let file = File::open(path)?;
        let reader = BufReader::new(file);
        let settings = serde_json::from_reader(reader)?;
        Ok(settings)
    }
    
    pub fn default() -> Self {
        Self {
            base_url: "https://ktw.co.th".to_string(),
            all_p_page: "/all-products".to_string(),
            shop_url: "https://shop.ktw.co.th".to_string(),
            user_name: "your_username".to_string(),
            password: "your_password".to_string(),
            csv_path: "products.csv".to_string(),
            telegram_token: "your_telegram_token".to_string(),
            chat_id: "your_chat_id".to_string(),
            chunk_size: 100,
        }
    }
}
```

### 2. Create the config.json file

Create `src/config.json` with your actual configuration:

```json
{
  "base_url": "https://ktw.co.th",
  "all_p_page": "/all-products",
  "shop_url": "https://shop.ktw.co.th",
  "user_name": "your_username",
  "password": "your_password",
  "csv_path": "products.csv",
  "telegram_token": "your_telegram_token",
  "chat_id": "your_chat_id",
  "chunk_size": 100
}
```

Make sure to replace:

- `your_username` and `your_password` with your KTW login credentials
- `your_telegram_token` with your Telegram bot token
- `your_chat_id` with your Telegram chat ID

### 3. Create a simple Python script for data processing

Create `send_data.py` in your project root (customize as needed):

```python
#!/usr/bin/env python3
import csv
import json
import requests

def detect_delimiter(file_path):
 with open(file_path, mode='r', encoding='utf-8') as file:
  first_line = file.readline()
  if ',' in first_line:
   return ','
  elif '\t' in first_line:
   return '\t'
  return ','

def read_csv(file_path):
    delimiter = detect_delimiter(file_path)
    with open(file_path, mode='r', encoding='utf-8') as file:
        reader = csv.reader(file, delimiter=delimiter)
        headers = [h.strip() for h in next(reader)]  # Strip spaces from headers
        
        all_rows = [dict(zip(headers, row)) for row in reader]
        return headers, all_rows, delimiter

def send_bulk_update(data_chunk):
 print(f"Sending bulk update for {len(data_chunk)} products")
 url = "https://www.exogro.co.th/wp-json/v2/product/api"
 headers = {
  "Content-Type": "application/json",
  "Authorization": "Basic c2hvcF9tYW5hZ2VyMTooTFBySW0mKWdJcTk1N1VoMUQxdTF3d28=",
  "Cookie": "PHPSESSID=deteolss5gt31sb4qhl2l6pciu"
 }
 payload = {"bulk": data_chunk}
 response = requests.post(url, headers=headers, data=json.dumps(payload))
 print(f"Response: {response.status_code}")
 return response.status_code

def process_csv(file_path):
    headers, all_rows, delimiter = read_csv(file_path)

    # Filter rows with "pending" result
    pending_rows = [
        {
            "sku": row["sku"],
            "regular_price": row["regular_price"],
            "sale_price": row["sale_price"],
            "stock_status": row["stock_status"],
            "stock_qty": row["stock_quantity"]
        }
        for row in all_rows if row.get("result", "").strip().lower() == "pending"
    ]

    if not pending_rows:
        print("No 'pending' rows found.")
        return

    status_code = send_bulk_update(pending_rows)
    print(f"API Response: {status_code}")

    if status_code == 200:
        # Update 'result' column in all_rows
        for row in all_rows:
            if row.get("result", "").strip().lower() == "pending":
                row["result"] = "updated"

        # Write back the updated CSV
        with open(file_path, mode='w', encoding='utf-8', newline='') as file:
            writer = csv.writer(file, delimiter=delimiter)
            writer.writerow(headers)
            for row in all_rows:
                writer.writerow([row[h] for h in headers])

        print("CSV updated successfully.")

if __name__ == "__main__":
 print("Start Send Data")
 csv_file_path = "ktw_products.csv"
 process_csv(csv_file_path)
```

## Running the Scraper

### 1. Build the project

```bash
cargo build --release
```

### 2. Run the scraper

```bash
./target/release/ktw_scraper
```

The scraper will:

1. Log in to the KTW website
2. Determine the total number of pages
3. Scrape product information from all pages
4. Save data to CSV
5. Check stock for all products
6. Run the Python script for additional processing
7. Send notifications via Telegram

## Python Integration

The Rust code executes `python3 send_data.py` at the end of the scraping process. The Python script can:

- Process the CSV data
- Send data to external APIs
- Apply additional transformations
- Update the status of processed items

Customize the Python script based on your specific requirements.

## Automation with Cron

### Setting up a Cron Job on Linux/macOS

1. Open the crontab editor:

```bash
crontab -e
```

2.Add a cron job to run the scraper at specific times:

```BASH
# Run daily at 2:00 AM
0 2 * * * cd /path/to/ktw_scraper && /path/to/ktw_scraper/target/release/ktw_scraper >> /path/to/ktw_scraper/logs/scraper.log 2>&1
```

Explanation of cron syntax: `minute hour day-of-month month day-of-week command`

Common schedule examples:

- `0 */6 * * *` - Run every 6 hours
- `0 2 * * *` - Run daily at 2:00 AM
- `0 2 * * 1-5` - Run at 2:00 AM Monday through Friday
- `0 2 1 * *` - Run at 2:00 AM on the first day of every month

### Setting up a Systemd Timer (Alternative to Cron)

1. Create a service file at `/etc/systemd/system/ktw-scraper.service`:

```
[Unit]
Description=KTW Scraper Service
After=network.target

[Service]
Type=oneshot
User=your_username
WorkingDirectory=/path/to/ktw_scraper
ExecStart=/path/to/ktw_scraper/target/release/ktw_scraper
Environment=RUST_LOG=info

[Install]
WantedBy=multi-user.target
```

2. Create a timer file at `/etc/systemd/system/ktw-scraper.timer`:

```
[Unit]
Description=Run KTW Scraper daily

[Timer]
OnCalendar=*-*-* 02:00:00
Persistent=true

[Install]
WantedBy=timers.target
```

3. Enable and start the timer:

```bash
sudo systemctl enable ktw-scraper.timer
sudo systemctl start ktw-scraper.timer
```

4. Check status:

```bash
sudo systemctl list-timers
```

## Troubleshooting

### Common Issues and Solutions

1. **Authentication failures**
   - Verify username and password in config.json
   - Check for CSRF token extraction issues
   - Ensure the website structure hasn't changed

2. **Network errors**
   - Check your internet connection
   - Verify the base URLs are correct
   - Consider adding retry logic for failed requests

3. **Memory usage issues**
   - Adjust the chunk size in config.json to process fewer products at once
   - Monitor memory usage with the built-in tracking

4. **Telegram notifications not working**
   - Verify your bot token and chat ID
   - Ensure your bot has permission to send messages to the chat

5. **Python script errors**
   - Check Python version and required packages
   - Verify the Python script is in the correct location
   - Add error handling in the Rust code for Python execution failures

## Advanced Usage

### Optimizing Performance

- Adjust the `chunk_size` and `concurrent_requests` parameters based on your server capabilities
- Consider using a proxy rotation service for large-scale scraping
- Implement exponential backoff for request retries

### Extending Functionality

1. **Additional data sources**
   - Implement multiple scrapers for different websites
   - Combine data from various sources

2. **Enhanced notifications**
   - Add email notifications
   - Create custom alerts based on specific conditions

3. **Data visualization**
   - Integrate with data visualization tools
   - Generate reports with the Python script

4. **Database integration**
   - Store data in a database instead of CSV
   - Implement historical tracking and analytics

---

This guide should help you get up and running with the KTW Scraper. Remember to respect website terms of service and avoid excessive requests that might impact server performance.
