// src/lib.rs or src/main.rs
mod config; // Declare the config module
pub use config::AppSettings; // Re-export AppSettings
use teloxide::prelude::*;

use csv::{ReaderBuilder, WriterBuilder};
use futures::stream::{self, StreamExt};
use reqwest::{
    header::{self, HeaderValue},
    Client,
};
use std::process::Command;

use scraper::{Html, Selector};
use serde::{Deserialize, Serialize};
use std::error::Error as StdError;
use std::fs::File;
use std::path::Path;
use std::time::Instant;
use std::{collections::HashMap, fs::OpenOptions};
use sysinfo::{Pid, System};
use tracing::instrument;
use tracing_subscriber::{self, fmt::format::FmtSpan};
use url::Url;

#[derive(Debug, Serialize, Deserialize, Clone)]
struct Product {
    sku: String,
    brand: String,
    stock_status: i32,
    stock_quantity: i32,
    sale_price: String,
    regular_price: String,
    result: String,
}

struct KTWScraper {
    client: Client,
    settings: AppSettings,
}

impl Product {
    // Update existing function to mark changes
    fn update_if_changed(&mut self, other: &Product) -> bool {
        let mut changed = false;

        if self.brand != other.brand
            || self.sale_price != other.sale_price
            || self.regular_price != other.regular_price
        {
            self.brand = other.brand.clone();
            self.sale_price = other.sale_price.clone();
            self.regular_price = other.regular_price.clone();
            self.stock_quantity = other.stock_quantity;
            self.result = "pending".to_string();
            changed = true;
        }
        changed
    }
}

impl KTWScraper {
    #[instrument]
    async fn new(settings: AppSettings) -> Result<Self, Box<dyn StdError>> {
        tracing::info!("Initializing KTW scraper");
        let client = Client::builder().cookie_store(true).build()?;
        Ok(Self { client, settings })
    }

    async fn get_csrf_token(&self) -> Result<String, Box<dyn StdError>> {
        let login_page_url = "https://shop.ktw.co.th/ktw/th/THB/login";

        let response = self.client.get(login_page_url).send().await?;

        if !response.status().is_success() {
            return Err("Failed to get login page".into());
        }

        let text = response.text().await?;
        let document = Html::parse_document(&text);
        let selector = Selector::parse("input[name='CSRFToken']").unwrap();

        if let Some(csrf_input) = document.select(&selector).next() {
            if let Some(token) = csrf_input.value().attr("value") {
                return Ok(token.to_string());
            }
        }

        Err("CSRF token not found".into())
    }

    #[instrument(skip(self))]
    async fn login(&self) -> Result<String, Box<dyn StdError>> {
        let login_post_url = format!(
            "{}/ktw/th/THB/j_spring_security_check",
            self.settings.shop_url
        );

        // Get CSRF token
        let csrf_token = self.get_csrf_token().await?;
        tracing::info!("Retrieved CSRF token: {}", csrf_token);

        // Prepare form data
        let mut form_data = HashMap::new();
        form_data.insert("j_username", self.settings.user_name.clone());
        form_data.insert("j_password", self.settings.password.clone());
        form_data.insert("CSRFToken", csrf_token.clone());
        form_data.insert("_csrf", csrf_token); // Add both token variants

        // Setup headers
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(header::ACCEPT, HeaderValue::from_static("text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8"));
        headers.insert(
            header::ACCEPT_LANGUAGE,
            HeaderValue::from_static("en-US,en;q=0.5"),
        );
        headers.insert(
            header::ACCEPT_ENCODING,
            HeaderValue::from_static("gzip, deflate, br, zstd"),
        );
        headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("max-age=0"));
        headers.insert(header::HOST, HeaderValue::from_static("shop.ktw.co.th"));
        headers.insert(header::HOST, HeaderValue::from_static("shop.ktw.co.th"));
        headers.insert(
            header::ORIGIN,
            HeaderValue::from_str(&self.settings.shop_url)?,
        );

        // Perform login with automatic redirect handling
        let login_response = self
            .client
            .post(login_post_url.clone())
            .headers(headers)
            .form(&form_data)
            .send()
            .await?;

        // Get cookies from response
        let cookies: Vec<_> = login_response.cookies().collect();

        let jsessionid = cookies
            .iter()
            .find(|c| c.name() == "JSESSIONID")
            .ok_or("No JSESSIONID cookie found")?
            .value()
            .to_string();

        // Verify login success by checking the account page
        if !self.verify_login().await? {
            tracing::error!("Login verification failed");
            return Err("Login verification failed".into());
        }

        tracing::info!("Login successful, obtained JSESSIONID: {}", jsessionid);
        Ok(jsessionid)
    }

    #[instrument(skip(self))]
    async fn verify_login(&self) -> Result<bool, Box<dyn StdError>> {
        let account_url = format!(
            "{}/ktw/th/THB/my-account/update-profile",
            self.settings.shop_url
        );

        let response = self.client.get(&account_url).send().await?;

        // Check if we got redirected to login page
        if response.url().path().contains("/login") {
            return Ok(false);
        }

        if !response.status().is_success() {
            return Ok(false);
        }

        let text = response.text().await?;

        // Save response for debugging
        std::fs::write("login_verification.html", &text)?;

        let document = Html::parse_document(&text);

        // Check for multiple indicators of successful login
        let profile_selector = Selector::parse("form#updateProfileForm").unwrap();
        let username_selector = Selector::parse("input#profile.email").unwrap();
        let logout_selector = Selector::parse("a[href*='logout']").unwrap();

        Ok(document.select(&profile_selector).next().is_some()
            || document.select(&username_selector).next().is_some()
            || document.select(&logout_selector).next().is_some())
    }

    // Helper function to check if still logged in
    async fn is_logged_in(&self) -> Result<bool, Box<dyn StdError>> {
        let home_url = format!("{}/ktw/th/THB", self.settings.shop_url);
        let response = self.client.get(&home_url).send().await?;

        if !response.status().is_success() {
            return Ok(false);
        }

        let text = response.text().await?;
        let document = Html::parse_document(&text);

        let profile_selector =
            Selector::parse("a[href='/ktw/th/THB/my-account/update-profile']").unwrap();
        let username_selector = Selector::parse("span.header__user-name").unwrap();

        Ok(document.select(&profile_selector).next().is_some()
            || document.select(&username_selector).next().is_some())
    }

    async fn check_stock(&self, sku: &str) -> Result<i32, Box<dyn StdError>> {
        // Setup headers with cookie
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(header::ACCEPT, HeaderValue::from_static("text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8"));
        headers.insert(
            header::ACCEPT_LANGUAGE,
            HeaderValue::from_static("en-US,en;q=0.5"),
        );
        headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-cache"));
        headers.insert(header::CONNECTION, HeaderValue::from_static("keep-alive"));
        headers.insert(header::PRAGMA, HeaderValue::from_static("no-cache"));
        headers.insert("sec-fetch-dest", HeaderValue::from_static("document"));
        headers.insert("sec-fetch-mode", HeaderValue::from_static("navigate"));
        headers.insert("sec-fetch-site", HeaderValue::from_static("same-origin"));
        headers.insert("sec-fetch-user", HeaderValue::from_static("?1"));
        headers.insert("sec-gpc", HeaderValue::from_static("1"));
        headers.insert(
            header::UPGRADE_INSECURE_REQUESTS,
            HeaderValue::from_static("1"),
        );
        headers.insert(header::USER_AGENT, HeaderValue::from_static("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36"));
        headers.insert(
            "sec-ch-ua",
            HeaderValue::from_static(
                "\"Not(A:Brand\";v=\"99\", \"Brave\";v=\"133\", \"Chromium\";v=\"133\"",
            ),
        );
        headers.insert("sec-ch-ua-mobile", HeaderValue::from_static("?0"));
        headers.insert(
            "sec-ch-ua-platform",
            HeaderValue::from_static("\"Windows\""),
        );
    
        let url = format!(
            "https://shop.ktw.co.th/ktw/th/THB/p/{}",
            sku
        );
    
        let response = self.client.get(&url).headers(headers).send().await?;
    
        tracing::info!(
            "Checking stock for SKU: {} Status Call: {}",
            sku,
            response.status().as_str()
        );
        if !response.status().is_success() {
            tracing::error!("Failed to get product page for SKU: {}", sku);
            return Ok(0);
        }
    
        let text = response.text().await?;
        let document = Html::parse_document(&text);
    
        // Find the stock table
        let table_selector = Selector::parse("div.table-responsive.stock-striped table").unwrap();
        let table_header_selector = Selector::parse("th").unwrap();
        let table_row_selector = Selector::parse("tr").unwrap();
        let table_data_selector = Selector::parse("td").unwrap();
    
        let mut stock_index = 1; // Default to second column
        let mut total_stock = 0;
    
        if let Some(table) = document.select(&table_selector).next() {
            // Find the correct stock column index
            for (index, header) in table.select(&table_header_selector).enumerate() {
                if header.text().any(|t| t.trim() == "ในสต๊อก") {
                    stock_index = index;
                    break;
                }
            }
    
            // Parse stock rows
            for row in table.select(&table_row_selector) {
                let cells: Vec<_> = row.select(&table_data_selector).collect();
                
                if cells.len() > stock_index {
                    let stock_cell = &cells[stock_index];
                    let stock_text = stock_cell.text().collect::<String>();
                    
                    // Extract the last part of the text (number)
                    if let Some(stock_num_str) = stock_text.split_whitespace().last() {
                        if let Ok(stock_num) = stock_num_str.parse::<i32>() {
                            total_stock += stock_num;
                        }
                    }
                }
            }
        }
    
        tracing::info!("Total stock for SKU {}: {}", sku, total_stock);
        Ok(total_stock)
    }
    

    fn load_csv_products(&self) -> Result<HashMap<String, Product>, Box<dyn StdError>> {
        if !Path::new(&self.settings.csv_path).exists() {
            return Ok(HashMap::new());
        }

        let file = File::open(&self.settings.csv_path)?;
        let mut reader = ReaderBuilder::new().has_headers(true).from_reader(file);

        let mut products = HashMap::new();
        for result in reader.deserialize() {
            let product: Product = result?;
            products.insert(product.sku.clone(), product);
        }

        Ok(products)
    }

    fn save_csv_products(
        &self,
        products: &HashMap<String, Product>,
    ) -> Result<(), Box<dyn StdError>> {
        let file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .create(true)
            .open(&self.settings.csv_path)?;

        tracing::info!("Save CSV and Writing");
        let mut writer = WriterBuilder::new().has_headers(true).from_writer(file);

        for product in products.values() {
            writer.serialize(product)?;
        }

        writer.flush()?;
        tracing::info!(
            "Saved {} products to {}",
            products.len(),
            self.settings.csv_path
        );
        let bot = Bot::new(self.settings.telegram_token.clone());
        let _ = bot.send_message(
            self.settings.chat_id.clone(),
            format!(
                "Saved {} products to {}",
                products.len(),
                self.settings.csv_path
            ),
        );
        Ok(())
    }

    // Modified version of update_product_stocks method
    #[instrument(skip(self))]
    async fn update_product_stocks(
        &self,
        chunk_size: usize,
        save_interval: usize,
    ) -> Result<(), Box<dyn StdError>> {
        // Login first
        self.login().await?;

        // Load existing products
        let mut products = self.load_csv_products()?;
        let product_keys: Vec<String> = products.keys().cloned().collect();
        let total_products = product_keys.len();

        let bot = Bot::new(self.settings.telegram_token.clone());
        bot.send_message(
            self.settings.chat_id.clone(),
            format!(
                "Starting Update Data Stock CSV - Processing {} products in chunks of {}",
                total_products, chunk_size
            ),
        )
        .await?;

        // Create a temp directory for chunk files
        let temp_dir = "temp_chunks";
        std::fs::create_dir_all(&temp_dir)?;
        tracing::info!("Created temporary directory for chunk files: {}", temp_dir);

        // Process in chunks
        let concurrent_requests = 50;
        let mut processed_count = 0;
        let mut changes_count = 0;

        for (chunk_index, keys_chunk) in product_keys.chunks(chunk_size).enumerate() {
            let chunk_start = Instant::now();
            tracing::info!(
                "Processing chunk {}/{} ({} products)",
                chunk_index + 1,
                (total_products + chunk_size - 1) / chunk_size,
                keys_chunk.len()
            );

            // Send notification for chunk start
            bot.send_message(
                self.settings.chat_id.clone(),
                format!(
                    "Processing chunk {}/{} - Products {}-{} of {}",
                    chunk_index + 1,
                    (total_products + chunk_size - 1) / chunk_size,
                    processed_count + 1,
                    processed_count + keys_chunk.len(),
                    total_products
                ),
            )
            .await?;

            // For each product in the chunk, check stock concurrently
            let mut chunk_items = Vec::new();
            for key in keys_chunk {
                if let Some(product) = products.get(key) {
                    chunk_items.push((product.sku.clone(), product.stock_quantity));
                }
            }

            // Create a new HashMap for this chunk to avoid race conditions
            let mut chunk_products = HashMap::new();
            for key in keys_chunk {
                if let Some(product) = products.get(key) {
                    chunk_products.insert(key.clone(), product.clone());
                }
            }

            // Store the length before moving chunk_items
            let chunk_items_len = chunk_items.len();

            let results = stream::iter(chunk_items)
                .map(|(sku, current_stock)| {
                    let scraper = self;
                    async move {
                        let stock_result = scraper.check_stock(&sku).await;
                        (sku, current_stock, stock_result)
                    }
                })
                .buffer_unordered(concurrent_requests)
                .collect::<Vec<_>>()
                .await;

            // Update the chunk products with stock information
            let mut chunk_changes = 0;
            for (sku, current_stock, stock_result) in results {
                if let Some(product) = chunk_products.get_mut(&sku) {
                    if let Ok(new_stock) = stock_result {
                        // Only mark as "pending" if the stock actually changed
                        if current_stock != new_stock {
                            product.stock_quantity = new_stock;
                            product.stock_status = if new_stock > 0 { 1 } else { 0 };
                            product.result = "pending".to_string();
                            chunk_changes += 1;

                            tracing::info!(
                                sku = %sku,
                                old_stock = current_stock,
                                new_stock = new_stock,
                                "Stock changed - marked as pending"
                            );
                        }
                    }
                }
            }

            changes_count += chunk_changes;
            processed_count += chunk_items_len; // Use the stored length instead of chunk_items.len()

            // Save this chunk to a separate file
            let chunk_file = format!("{}/chunk_{}.csv", temp_dir, chunk_index);
            self.save_chunk_to_csv(&chunk_products, &chunk_file)?;

            tracing::info!(
                "Saved chunk {} with {} products ({} changes) to {}",
                chunk_index,
                chunk_products.len(),
                chunk_changes,
                chunk_file
            );

            // Log progress
            tracing::info!(
                "Processed {} of {} products, {} changes detected overall",
                processed_count,
                total_products,
                changes_count
            );

            // Save periodically by merging chunks
            if chunk_index % save_interval == save_interval - 1
                || chunk_index == product_keys.chunks(chunk_size).len() - 1
            {
                tracing::info!(
                    "Merging intermediate results after processing {} products",
                    processed_count
                );

                // Merge all chunk files into the main products HashMap
                self.merge_chunk_files(&temp_dir, &mut products)?;

                // Save merged products
                self.save_csv_products(&products)?;

                bot.send_message(
					self.settings.chat_id.clone(),
					format!("Progress update: Processed {}/{} products ({}%). {} changes detected. Saved intermediate results.", 
						processed_count,
						total_products,
						(processed_count as f64 / total_products as f64 * 100.0) as u32,
						changes_count
					)
				).await?;
            }

            tracing::info!(
                "Completed chunk {}/{} in {:?}",
                chunk_index + 1,
                (total_products + chunk_size - 1) / chunk_size,
                chunk_start.elapsed()
            );
        }

        // Final merge of all chunks
        self.merge_chunk_files(&temp_dir, &mut products)?;

        // Final save of all products
        self.save_csv_products(&products)?;

        // Clean up temp directory
        std::fs::remove_dir_all(&temp_dir)?;
        tracing::info!("Removed temporary directory: {}", temp_dir);

        // Send completion notification
        bot.send_message(
            self.settings.chat_id.clone(),
            format!(
                "Completed stock update for all {} products. {} products marked as pending.",
                total_products, changes_count
            ),
        )
        .await?;

        Ok(())
    }

    // New method to save a chunk of products to a CSV file
    fn save_chunk_to_csv(
        &self,
        chunk_products: &HashMap<String, Product>,
        filename: &str,
    ) -> Result<(), Box<dyn StdError>> {
        let file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .create(true)
            .open(filename)?;

        tracing::info!("Saving chunk to {}", filename);
        let mut writer = WriterBuilder::new().has_headers(true).from_writer(file);

        for product in chunk_products.values() {
            writer.serialize(product)?;
        }

        writer.flush()?;
        Ok(())
    }

    // New method to merge chunk files into the main products HashMap
    fn merge_chunk_files(
        &self,
        temp_dir: &str,
        products: &mut HashMap<String, Product>,
    ) -> Result<(), Box<dyn StdError>> {
        let paths = std::fs::read_dir(temp_dir)?;

        for path in paths {
            let path = path?.path();
            if path.is_file() && path.extension().map_or(false, |ext| ext == "csv") {
                tracing::info!("Merging chunk file: {:?}", path);

                let file = File::open(&path)?;
                let mut reader = ReaderBuilder::new().has_headers(true).from_reader(file);

                for result in reader.deserialize() {
                    let chunk_product: Product = result?;
                    products.insert(chunk_product.sku.clone(), chunk_product);
                }

                // Optionally remove the chunk file after merging
                std::fs::remove_file(&path)?;
            }
        }

        tracing::info!("Merged all chunk files into main products HashMap");
        Ok(())
    }

    #[instrument(skip(self))]
    async fn scrape_and_update(
        &self,
        chunk_size: usize,
        save_interval: usize,
    ) -> Result<(), Box<dyn StdError>> {
        // Load existing products
        let mut existing_products = self.load_csv_products()?;
        tracing::info!(
            "Loaded {} existing products from CSV",
            existing_products.len()
        );

        // First, scrape new products with our enhanced logic
        let new_products = self.scrape_all_products().await?;

        // Update existing products or add new ones
        let mut updates_count = 0;
        let mut new_count = 0;

        for product in new_products {
            let sku = product.sku.clone(); // Clone the SKU to use for HashMap lookup

            if let Some(existing) = existing_products.get_mut(&sku) {
                // Create a copy of the product to avoid borrow issues
                let new_product = product.clone();

                if existing.update_if_changed(&new_product) {
                    tracing::info!("Updated product: {}", sku);
                    updates_count += 1;
                }
            } else {
                tracing::info!("Added new product: {}", product.sku);
                // For new products, set result to "pending"
                let mut new_product = product;
                new_product.result = "pending".to_string();
                existing_products.insert(sku, new_product);
                new_count += 1;
            }
        }

        tracing::info!(
            "Updated {} products, added {} new products",
            updates_count,
            new_count
        );

        // Save to CSV
        tracing::info!("Saving data to CSV");
        self.save_csv_products(&existing_products)?;

        // Now update stock quantities with chunking
        self.update_product_stocks(chunk_size, save_interval)
            .await?;

        Ok(())
    }
    #[instrument(skip(self))]
    async fn get_total_pages(&self) -> Result<u32, Box<dyn StdError>> {
        let start = Instant::now();
        let url = format!("{}{}", &self.settings.base_url, &self.settings.all_p_page);
        tracing::info!(url = %url, "Fetching main page to determine total pages");

        let response = self.client.get(url).send().await?.text().await?;
        let document = Html::parse_document(&response);

        // Find the pagination-desktop div
        let pagination_selector = Selector::parse("div.pagination-desktop").unwrap();
        let last_link_selector = Selector::parse("a.page-link.last").unwrap();

        if let Some(pagination_div) = document.select(&pagination_selector).next() {
            if let Some(last_link) = pagination_div.select(&last_link_selector).next() {
                if let Some(href) = last_link.value().attr("href") {
                    // Parse the URL to extract the page parameter
                    if let Ok(parsed_url) = Url::parse(&format!("https://ktw.co.th{}", href)) {
                        let pairs = parsed_url.query_pairs();
                        for (key, value) in pairs {
                            if key == "page" {
                                if let Ok(page_num) = value.parse::<u32>() {
                                    tracing::info!(
                                        total_pages = page_num + 1,
                                        duration = ?start.elapsed(),
                                        "Found total number of pages"
                                    );
                                    return Ok(page_num + 1);
                                }
                            }
                        }
                    }
                }
            }
        }
        tracing::error!("Could not find pagination information");
        Err("Could not determine total pages".into())
    }

    #[instrument(skip(self), fields(page = %page))]
    async fn scrape_page(&self, page: u32) -> Result<Vec<Product>, Box<dyn StdError>> {
        let start = Instant::now();
        let url = if page == 1 {
            format!("{}{}", self.settings.base_url, self.settings.all_p_page)
        } else {
            format!(
                "{}&page={}{}",
                self.settings.base_url,
                page - 1,
                self.settings.all_p_page
            )
        };
        tracing::info!(url = %url, "Starting page scrape");

        let response = self.client.get(&url).send().await?.text().await?;

        // Debug: Save HTML content to file for inspection
        if page == 1 {
            std::fs::write("debug_page.html", &response)?;
            tracing::info!("Saved first page HTML to debug_page.html");
        }

        let document = Html::parse_document(&response);

        // Updated selectors based on actual HTML structure
        let grid_selector = Selector::parse(".zproduct-grid").unwrap();
        let grids = document.select(&grid_selector);

        let sku_selector = Selector::parse(".grid-item__sku").unwrap();
        let brand_selector = Selector::parse(".grid-item__brand").unwrap();
        let sale_price_selector = Selector::parse(".grid-item__saleprice").unwrap();
        let regular_price_selector = Selector::parse(".grid-item__wasprice").unwrap();

        let mut products = Vec::new();

        // Find all product containers within the document
        // for container in document.select(&container_selector) {
        for grid in grids {
            let item_selector = Selector::parse(".grid-item").unwrap();
            let items = grid.select(&item_selector);
            for item in items {
                // Extract the SKU for each product
                let sku = item
                    .select(&sku_selector)
                    .next()
                    .map(|el| el.text().collect::<String>())
                    .unwrap_or_default();

                if !sku.is_empty() {
                    // Extract other product details
                    let brand = item
                        .select(&brand_selector)
                        .next()
                        .map(|el| el.text().collect::<String>())
                        .unwrap_or_default();

                    let sale_price = item
                        .select(&sale_price_selector)
                        .next()
                        .map(|el| el.text().collect::<String>())
                        .unwrap_or_default();

                    let regular_price = item
                        .select(&regular_price_selector)
                        .next()
                        .map(|el| el.text().collect::<String>())
                        .unwrap_or_default();

                    tracing::info!(
                        sku = %sku,
                        brand = %brand,
                        "Found product"
                    );

                    products.push(Product {
                        sku: sku.trim().to_string(),
                        brand: brand.trim().to_string(),
                        stock_status: 0,
                        stock_quantity: 0,
                        sale_price: sale_price.trim().to_string(),
                        regular_price: regular_price.trim().to_string(),
                        result: "-".to_string(),
                    });
                }
            }
        }

        tracing::info!(
            products_found = products.len(),
            duration = ?start.elapsed(),
            "Completed page scrape"
        );
        Ok(products)
    }

    // For scrape_all_products method, we can apply a similar approach:
    #[instrument(skip(self))]
    async fn scrape_all_products(&self) -> Result<Vec<Product>, Box<dyn StdError>> {
        let start = Instant::now();
        let total_pages = self.get_total_pages().await?;
        tracing::info!(total_pages = total_pages, "Starting full scrape");

        // Load existing products from CSV for comparison
        let existing_products = std::sync::Arc::new(self.load_csv_products()?);
        tracing::info!(
            existing_products = existing_products.len(),
            "Loaded existing products for comparison"
        );

        // Create a temp directory for page results
        let temp_dir = "temp_pages";
        std::fs::create_dir_all(&temp_dir)?;
        tracing::info!("Created temporary directory for page files: {}", temp_dir);

        let mut skipped_count = 0;
        let mut updated_count = 0;
        let mut new_count = 0;
        let concurrent_requests = 50;

        let pages = stream::iter(1..=total_pages);
        let mut results = pages
            .map(|page| {
                let scraper = self;
                let temp_dir = temp_dir;
                let products_ref = std::sync::Arc::clone(&existing_products);

                async move {
                    match scraper.scrape_page(page).await {
                        Ok(products) => {
                            tracing::info!(
                                page = page,
                                products = products.len(),
                                "Page scrape successful"
                            );

                            // Immediately save this page's products to a separate file
                            let page_file = format!("{}/page_{}.csv", temp_dir, page);
                            let file = OpenOptions::new()
                                .write(true)
                                .create(true)
                                .truncate(true)
                                .open(&page_file)
                                .expect("Failed to create page file");

                            let mut writer =
                                WriterBuilder::new().has_headers(true).from_writer(file);

                            let mut page_new = 0;
                            let mut page_updated = 0;
                            let mut page_skipped = 0;

                            for mut product in products {
                                // Compare with existing products
                                if let Some(existing_product) = products_ref.get(&product.sku) {
                                    let stock_matches =
                                        product.stock_quantity == existing_product.stock_quantity;
                                    let sale_price_matches =
                                        product.sale_price == existing_product.sale_price;
                                    let regular_price_matches =
                                        product.regular_price == existing_product.regular_price;

                                    if stock_matches && sale_price_matches && regular_price_matches
                                    {
                                        page_skipped += 1;
                                        continue; // Skip writing this product
                                    } else {
                                        product.result = "pending".to_string();
                                        page_updated += 1;
                                    }
                                } else {
                                    page_new += 1;
                                }

                                // Write the product to the page file
                                writer
                                    .serialize(&product)
                                    .expect("Failed to write product to page file");
                            }

                            writer.flush().expect("Failed to flush page file");

                            Ok((page, page_new, page_updated, page_skipped))
                        }
                        Err(e) => {
                            tracing::error!(page = page, error = %e, "Page scrape failed");
                            Err(e)
                        }
                    }
                }
            })
            .buffer_unordered(concurrent_requests);

        while let Some(result) = results.next().await {
            match result {
                Ok((page, page_new, page_updated, page_skipped)) => {
                    tracing::info!(
                        page = page,
                        new = page_new,
                        updated = page_updated,
                        skipped = page_skipped,
                        "Processed page results"
                    );
                    new_count += page_new;
                    updated_count += page_updated;
                    skipped_count += page_skipped;
                }
                Err(e) => tracing::error!(error = %e, "Error processing page"),
            }
        }

        // Now merge all page files into a single result
        let mut all_products = Vec::new();
        let paths = std::fs::read_dir(&temp_dir)?;

        for path in paths {
            let path = path?.path();
            if path.is_file() && path.extension().map_or(false, |ext| ext == "csv") {
                tracing::info!("Merging page file: {:?}", path);

                let file = File::open(&path)?;
                let mut reader = ReaderBuilder::new().has_headers(true).from_reader(file);

                for result in reader.deserialize() {
                    let product: Product = result?;
                    all_products.push(product);
                }

                // Remove the page file after merging
                std::fs::remove_file(&path)?;
            }
        }

        // Clean up temp directory
        std::fs::remove_dir_all(&temp_dir)?;

        tracing::info!(
            total_products = all_products.len(),
            new_products = new_count,
            updated_products = updated_count,
            skipped_products = skipped_count,
            duration = ?start.elapsed(),
            "Completed full scrape"
        );

        // Report via Telegram as well
        let bot = Bot::new(self.settings.telegram_token.clone());
        let _ = bot
            .send_message(
                self.settings.chat_id.clone(),
                format!(
				"Scrape completed:\n- Total: {}\n- New: {}\n- Updated: {}\n- Skipped: {}\n- Duration: {:?}",
				all_products.len(), new_count, updated_count, skipped_count, start.elapsed()
			),
            )
            .await;

        Ok(all_products)
    }

    #[instrument(skip(self, products))]
    async fn save_to_csv(
        &self,
        products: Vec<Product>,
        filename: &str,
    ) -> Result<(), Box<dyn StdError>> {
        let start = Instant::now();
        tracing::info!(filename = %filename, products = products.len(), "Starting CSV save");

        let file = File::create(filename)?;
        let mut writer = WriterBuilder::new().has_headers(true).from_writer(file);

        for product in products {
            if let Err(e) = writer.serialize(&product) {
                tracing::error!(sku = %product.sku, error = %e, "Failed to write product to CSV");
            }
        }

        writer.flush()?;
        tracing::info!(
            duration = ?start.elapsed(),
            "Completed CSV save"
        );
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn StdError>> {
    // Initialize settings
    let settings = match AppSettings::from_file("src/config.json") {
        Ok(settings) => settings,
        Err(e) => {
            tracing::warn!("Failed to load config file: {}. Using default settings.", e);
            AppSettings::default()
        }
    };

    // Initialize tracing
    tracing_subscriber::fmt()
        .with_target(false)
        .with_thread_ids(true)
        .with_thread_names(true)
        .with_file(true)
        .with_line_number(true)
        .with_level(true)
        .with_span_events(FmtSpan::CLOSE)
        .init();

    let mut sys = System::new_all();
    sys.refresh_all();

    let pid = Pid::from(std::process::id() as usize);
    let mut max_mem_usage = 0; // Track maximum memory usage in KB

    // Configure chunking parameters
    let chunk_size = settings.chunk_size; // Process 100 products at a time
    let save_interval = 1; // Save after each chunk

    // Start scraping
    let start = Instant::now();
    tracing::info!("Starting KTW scraping process");
    let bot = Bot::new(settings.telegram_token.clone());
    bot.send_message(settings.chat_id.clone(), "Starting KTW scraping process")
        .await?;

    // Create scraper with settings
    let scraper: KTWScraper = KTWScraper::new(settings.clone()).await?;
    scraper
        .scrape_and_update(chunk_size.try_into().unwrap(), save_interval)
        .await?;

    // Check max memory usage
    sys.refresh_processes();
    if let Some(process) = sys.process(pid) {
        max_mem_usage = process.memory();
    }

    // Execute Python script and wait for it to complete
    tracing::info!("Executing Python script: send_data.py");
    let python_status = Command::new("python")
        .arg("src/send_data.py")
        .status()
        .expect("Failed to execute Python script");
    tracing::info!("{:?}", python_status);

    let txt_stat: String = if python_status.success() {
        tracing::info!("Python script executed successfully");
        "Python script executed successfully".to_string()
    } else {
        tracing::error!(
            "Python script failed with exit code: {:?}",
            python_status.code()
        );
        format!(
            "Python script send_data.py failed with exit code: {:?}",
            python_status.code()
        ).to_string()
    };

    bot.send_message(settings.chat_id.clone(), txt_stat).await?;

    tracing::info!(
        total_duration = ?start.elapsed(),
        max_memory_kb = max_mem_usage as f64 / 1024.0,
        max_memory_mb = max_mem_usage as f64 / (1024.0 * 1024.0),
        max_memory_gb = max_mem_usage as f64 / (1024.0 * 1024.0 * 1024.0),
        "Scraping process completed"
    );
    bot.send_message(
        settings.chat_id.clone(),
        format!(
            "Duration : {:?}\nMax Memory Usage: {:.2} MB",
            start.elapsed(),
            max_mem_usage as f64 / (1024.0 * 1024.0)
        ),
    )
    .await?;
    Ok(())
}
