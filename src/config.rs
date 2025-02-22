use serde::Deserialize;
use std::fs::File;
use std::io::Read;
use std::path::Path;

#[derive(Deserialize, Clone, Debug)]
pub struct AppSettings {
    pub auth_url: String,
    pub user_name: String,
    pub password: String,
    pub auth_key: String,
    pub base_url: String,
    pub all_p_page: String,
    pub shop_url: String,
    pub shop_url_login: String,
    pub csv_path: String,
}

impl AppSettings {
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self, Box<dyn std::error::Error>> {
        let mut file = File::open(path)?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)?;

        let settings: AppSettings = serde_json::from_str(&contents)?;
        Ok(settings)
    }

    pub fn default() -> Self {
        Self {
            auth_url: "https://shop.ktw.co.th/ktw/th/THB/j_spring_security_check".to_string(),
            user_name: "default@example.com".to_string(),
            password: "default_password".to_string(),
            auth_key: "default_key".to_string(),
            base_url: "https://ktw.co.th/search?pageSize=108".to_string(),
            all_p_page: "&q=%3Aprice-desc&altText=&viewType=grid#".to_string(),
            shop_url: "https://shop.ktw.co.th".to_string(),
            shop_url_login: "https://shop.ktw.co.th/login".to_string(),
            csv_path: "ktw_products.csv".to_string(),
        }
    }
}

