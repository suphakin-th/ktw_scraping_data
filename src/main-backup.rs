use chrono::Utc;
use csv::{ReaderBuilder, WriterBuilder};
use futures::stream::{self, StreamExt};
use regex::Regex;
use reqwest::{Client, header::{self, HeaderValue}};
use scraper::{Html, Selector};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, fs::OpenOptions};
use tracing::instrument;
use std::error::Error as StdError;
use std::fs::File;
use std::time::Instant;
use std::path::Path;
use sysinfo::{Pid, System};
use tracing_subscriber::{self, fmt::format::FmtSpan};
use url::Url;

#[derive(Debug, Serialize, Deserialize)]
struct Product {
    sku: String,
    brand: String,
    stock_status: i32,
    stock_quantity: i32,
    sale_price: String,
    regular_price: String,
    result: String,
}

#[derive(Clone, Debug)]
struct LoginCredentials {
    auth_url: String,
    user_name: String,
    password: String,
}

struct KTWScraper {
    client: Client,
    base_url: String,
    all_p_page: String,
    shop_url: String,
    credentials: LoginCredentials,
    csv_path: String,
}

impl Product {
    fn new(
        sku: String,
        brand: String,
        sale_price: String,
        regular_price: String,
    ) -> Self {
        Self {
            sku,
            brand,
            stock_status: 0,
            stock_quantity: 0,
            sale_price,
            regular_price,
            result: "-".to_string(),
        }
    }

    fn update_if_changed(&mut self, other: &Product) -> bool {
        let mut changed = false;

        if self.brand != other.brand ||
           self.sale_price != other.sale_price ||
           self.regular_price != other.regular_price {
            self.brand = other.brand.clone();
            self.sale_price = other.sale_price.clone();
            self.regular_price = other.regular_price.clone();
            changed = true;
        }
        changed
    }

    fn update_stock(&mut self, quantity: i32) {
        if self.stock_quantity != quantity {
            self.stock_quantity = quantity;
            self.stock_status = if quantity > 0 { 1 } else { 0 };
        }
    }
}



impl KTWScraper {
    #[instrument]
    async fn new(credentials: LoginCredentials) -> Result<Self, Box<dyn StdError>> {
        tracing::info!("Initializing KTW scraper");
        let client = Client::builder()
            .cookie_store(true)
            .build()?;

        Ok(Self {
            client,
            base_url: "https://ktw.co.th/search?pageSize=108".to_string(),
            all_p_page: "&q=%3Aprice-desc&altText=&viewType=grid#".to_string(),
            shop_url: "https://shop.ktw.co.th".to_string(),
            credentials,
            csv_path: "ktw_products.csv".to_string(),
        })
    }

    #[instrument(skip(self))]
    async fn login(&self) -> Result<(), Box<dyn StdError>> {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            header::ACCEPT,
            HeaderValue::from_static(
                "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
            ),
        );
        headers.insert(header::CONNECTION, HeaderValue::from_static("keep-alive"));
        headers.insert(
            "Content-Type",
            HeaderValue::from_static("application/x-www-form-urlencoded"),
        );
        headers.insert(header::ORIGIN, HeaderValue::from_static("https://shop.ktw.co.th"));
        headers.insert(
            header::REFERER,
            HeaderValue::from_static("https://shop.ktw.co.th/ktw/th/THB/login"),
        );
        headers.insert(
            header::COOKIE,
            HeaderValue::from_static(
                "JSESSIONID=397AA10DAF88A01EB582ED3D9730F705.node1; JSESSIONID=397AA10DAF88A01EB582ED3D9730F705.node1; acceleratorSecureGUID=9fcdf2a19fd23bbb1c60d58005a926cb7e838581",
            ),
        );

        let mut params = HashMap::new();
        params.insert("j_username", self.credentials.user_name.clone());
        params.insert("j_password", self.credentials.password.clone());

        let response = self.client
            .post(&self.credentials.auth_url)
            .headers(headers)
            .form(&params)
            .send()
            .await?;

        if response.status().is_success() {
            tracing::info!("Login successful");
            Ok(())
        } else {
            tracing::error!("Login failed: {}", response.status());
            Err("Login failed".into())
        }
    }

    async fn check_stock(&self, sku: &str) -> Result<i32, Box<dyn StdError>> {
        let url = format!("{}/ktw/th/THB/p/{}", self.shop_url.replace("/", "@"), sku);
        let response = self.client.get(&url).send().await?;
    
        tracing::info!(
            "Checking stock for SKU: {} Status Call: {}",
            sku,
            response.status().as_str()
        );
    
        if !response.status().is_success() {
            return Ok(0);
        }
    
        let text = response.text().await?;
        let document = Html::parse_document(&text);
    
        // Check if "สต๊อก" exists before scraping
        let stock_header_selector = Selector::parse("h4").unwrap();
        let stock_header_exists = document
            .select(&stock_header_selector)
            .any(|e| e.text().any(|t| t.trim() == "สต๊อก"));
    
        if !stock_header_exists {
            tracing::info!("No stock section found for SKU: {}", sku);
            return Ok(0);
        }
    
        // Select the stock table area
        let table_container_selector = Selector::parse("div.table-responsive.stock-striped").unwrap();
        let stock_td_selector = Selector::parse("td.text-right").unwrap();
        let number_pattern = Regex::new(r"\d+").unwrap();
        let mut total_stock = 0;
    
        if let Some(table_container) = document.select(&table_container_selector).next() {
            for td in table_container.select(&stock_td_selector) {
                let text = td.text().collect::<String>().trim().to_string();
                if let Some(capture) = number_pattern.find(&text) {
                    if let Ok(num) = capture.as_str().parse::<i32>() {
                        tracing::info!("Checking stock for SKU: {} Found stock {}", sku, num);
                        total_stock += num;
                    }
                }
            }
        }
    
        Ok(total_stock)
    }
    

    fn load_csv_products(&self) -> Result<HashMap<String, Product>, Box<dyn StdError>> {
        if !Path::new(&self.csv_path).exists() {
            return Ok(HashMap::new());
        }

        let file = File::open(&self.csv_path)?;
        let mut reader = ReaderBuilder::new()
            .has_headers(true)
            .from_reader(file);

        let mut products = HashMap::new();
        for result in reader.deserialize() {
            let product: Product = result?;
            products.insert(product.sku.clone(), product);
        }

        Ok(products)
    }

    fn save_csv_products(&self, products: &HashMap<String, Product>) -> Result<(), Box<dyn StdError>> {
        let file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .create(true)
            .open(&self.csv_path)?;

        let mut writer = WriterBuilder::new()
            .has_headers(true)
            .from_writer(file);

        for product in products.values() {
            writer.serialize(product)?;
        }

        writer.flush()?;
        Ok(())
    }

    #[instrument(skip(self))]
async fn update_product_stocks(&self) -> Result<(), Box<dyn StdError>> {
    // Login first
    self.login().await?;

    // Load existing products
    let mut products = self.load_csv_products()?;
    let concurrent_requests = 100;

    // Process stock updates concurrently
    let _ = stream::iter(products.values_mut())
        .map(|product| async {
            if let Ok(stock) = self.check_stock(&product.sku).await {
                product.update_stock(stock);
            }
        })
        .buffer_unordered(concurrent_requests) // Adjust concurrency level as needed
        .collect::<Vec<_>>()
        .await;

    // Save updated products
    self.save_csv_products(&products)?;

    Ok(())
}

    #[instrument(skip(self))]
    async fn scrape_and_update(&self) -> Result<(), Box<dyn StdError>> {
        // First, scrape new products
        let mut existing_products = self.load_csv_products()?;
        let new_products = self.scrape_all_products().await?;

        // Update existing products or add new ones
        for product in new_products {
            if let Some(existing) = existing_products.get_mut(&product.sku) {
                if existing.update_if_changed(&product) {
                    tracing::info!("Updated product: {}", product.sku);
                }
            } else {
                tracing::info!("Added new product: {}", product.sku);
                existing_products.insert(product.sku.clone(), product);
            }
        }

        // Save to CSV
        self.save_csv_products(&existing_products)?;

        // Now update stock quantities
        self.update_product_stocks().await?;

        Ok(())
    }

    #[instrument(skip(self))]
    async fn get_total_pages(&self) -> Result<u32, Box<dyn StdError>> {
        let start = Instant::now();
        let url = format!("{}{}", &self.base_url, &self.all_p_page);
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
            format!("{}{}", self.base_url, self.all_p_page)
        } else {
            format!("{}&page={}{}", self.base_url, page - 1 , self.all_p_page)
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
        // let container_selector = Selector::parse(
        //     ".yCmsComponent search-list-page-right-result-list-component",
        // ).unwrap();
        // let container = document.select(&container_selector).next().unwrap();
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

    #[instrument(skip(self))]
    async fn scrape_all_products(&self) -> Result<Vec<Product>, Box<dyn StdError>> {
        let start = Instant::now();
        let total_pages = self.get_total_pages().await?;
        tracing::info!(total_pages = total_pages, "Starting full scrape");

        let mut all_products = Vec::new();
        let concurrent_requests = 100;

        let pages = stream::iter(1..=total_pages);
        let mut results = pages
            .map(|page| {
                let scraper = self;
                async move {
                    match scraper.scrape_page(page).await {
                        Ok(products) => {
                            tracing::info!(
                                page = page,
                                products = products.len(),
                                "Page scrape successful"
                            );
                            Ok(products)
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
                Ok(products) => all_products.extend(products),
                Err(e) => tracing::error!(error = %e, "Error processing page"),
            }
        }

        tracing::info!(
            total_products = all_products.len(),
            duration = ?start.elapsed(),
            "Completed full scrape"
        );
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

    let start = Instant::now();
    tracing::info!("Starting KTW scraping process");

    let credentials = LoginCredentials {
        auth_url: "https://shop.ktw.co.th/ktw/th/THB/j_spring_security_check".to_string(),
        user_name: "jureeporn.b@exogro.co.th".to_string(),
        password: "Zaq12wsx".to_string(),
    };

    let scraper = KTWScraper::new(credentials).await?;
    scraper.scrape_and_update().await?;

    // Check max memory usage
    sys.refresh_processes();
    if let Some(process) = sys.process(pid) {
        max_mem_usage = process.memory();
    }

    tracing::info!(
        total_duration = ?start.elapsed(),
        max_memory_kb = max_mem_usage as f64 / 1024.0,
        max_memory_mb = max_mem_usage as f64 / (1024.0 * 1024.0),
        max_memory_gb = max_mem_usage as f64 / (1024.0 * 1024.0 * 1024.0),
        "Scraping process completed"
    );
    Ok(())
}
