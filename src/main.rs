use csv::WriterBuilder;
use futures::stream::{self, StreamExt};
use reqwest::Client;
use scraper::{Html, Selector};
use serde::{Deserialize, Serialize};
use tracing::instrument;
use std::error::Error as StdError;
use std::fs::File;
use std::time::Instant;
use sysinfo::{Pid, System};
use tracing_subscriber::{self, fmt::format::FmtSpan};
use url::Url;

#[derive(Debug, Serialize, Deserialize)]
struct Product {
    sku: String,
    brand: String,
    stock_status: i32,
    sale_price: String,
    regular_price: String,
    rate: f64,
    result: String,
    link: String,
    stock_quantity: i32,
}

struct KTWScraper {
    client: Client,
    base_url: String,
    max_page: String,
}

impl KTWScraper {
    #[instrument]
    async fn new() -> Result<Self, Box<dyn StdError>> {
        tracing::info!("Initializing KTW scraper");
        let client = Client::builder().cookie_store(true).build()?;

        Ok(KTWScraper {
            client,
            base_url: "https://ktw.co.th".to_string(),
            max_page: "?pageSize=108".to_string(),
        })
    }

    fn extract_page_number(&self, href: &str) -> Option<u32> {
        let parsed_url = Url::parse(&format!("{}{}", self.base_url, href)).ok()?;
        let pairs = parsed_url.query_pairs();
        for (key, value) in pairs {
            if key == "page" {
                return value.parse().ok();
            }
        }
        None
    }

    #[instrument(skip(self))]
    async fn get_total_pages(&self) -> Result<u32, Box<dyn StdError>> {
        let start = Instant::now();
        let url = format!("{}/c/cat_c05000000{}", self.base_url, self.max_page);
        tracing::info!(url = %url, "Fetching main page to determine total pages");

        let response = self.client.get(&url).send().await?.text().await?;
        let document = Html::parse_document(&response);

        // Find the pagination-desktop div
        let pagination_desktop = Selector::parse(".pagination-desktop").unwrap();
        let pagination = Selector::parse("ul.pagination").unwrap();
        let li_selector = Selector::parse("li").unwrap();
        let a_selector = Selector::parse("a").unwrap();

        if let Some(pagination_div) = document.select(&pagination_desktop).next() {
            if let Some(ul) = pagination_div.select(&pagination).next() {
                let li_elements: Vec<_> = ul.select(&li_selector).collect();

                // Get the last li element
                if let Some(last_li) = li_elements.last() {
                    if let Some(a_tag) = last_li.select(&a_selector).next() {
                        if let Some(href) = a_tag.value().attr("href") {
                            if let Some(page_num) = self.extract_page_number(href) {
                                tracing::info!(
                                    total_pages = page_num,
                                    duration = ?start.elapsed(),
                                    "Found total number of pages"
                                );
                                return Ok(page_num);
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
        let url = format!(
            "{}/c/cat_c05000000?pageSize=108&page={}",
            self.base_url, page
        );
        tracing::info!(url = %url, "Starting page scrape");

        let response = self.client.get(&url).send().await?.text().await?;

        // Debug: Save HTML content to file for inspection
        if page == 1 {
            std::fs::write("debug_page.html", &response)?;
            tracing::info!("Saved first page HTML to debug_page.html");
        }

        let document = Html::parse_document(&response);

        // Updated selectors based on actual HTML structure
        let container_selector = Selector::parse(
            ".yCmsComponent.product__list--wrapper.yComponentWrapper.product-list-right-component",
        )
        .unwrap();
        let container = document.select(&container_selector).next().unwrap();
        let grid_selector = Selector::parse(".zproduct-grid").unwrap();
        let grids = container.select(&grid_selector);

        let sku_selector = Selector::parse(".grid-item__sku").unwrap();
        let brand_selector = Selector::parse(".grid-item__brand").unwrap();
        let sale_price_selector = Selector::parse(".grid-item__saleprice").unwrap();
        let regular_price_selector = Selector::parse(".grid-item__wasprice").unwrap();
        let link_selector = Selector::parse(".grid-item__description a").unwrap();

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

                    let link = item
                        .select(&link_selector)
                        .next()
                        .and_then(|el| el.value().attr("href"))
                        .map(|href| format!("{}{}", self.base_url, href))
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
                        sale_price: sale_price.trim().to_string(),
                        regular_price: regular_price.trim().to_string(),
                        rate: 0.99,
                        result: "-".to_string(),
                        link,
                        stock_quantity: 0,
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
                let scraper = self.clone();
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

impl Clone for KTWScraper {
    fn clone(&self) -> Self {
        KTWScraper {
            client: self.client.clone(),
            base_url: self.base_url.clone(),
            max_page: self.max_page.clone(),
        }
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

    let scraper = KTWScraper::new().await?;
    let products = scraper.scrape_all_products().await?;

    scraper.save_to_csv(products, "ktw_products.csv").await?;

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
