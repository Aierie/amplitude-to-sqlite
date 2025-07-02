use crate::amplitude_types::{BatchUploadRequest, BatchUploadResponse, Event};
use crate::config::AmplitudeConfig;
use chrono::{DateTime, Utc};
use reqwest::StatusCode;
use std::error::Error;
use std::env;

/// The default Amplitude batch endpoint
const DEFAULT_ENDPOINT: &str = "https://api2.amplitude.com/batch";

/// The default Amplitude export endpoint
const DEFAULT_EXPORT_ENDPOINT: &str = "https://amplitude.com/api/2/export";

/// Client for uploading events to Amplitude via the Batch Event Upload API.
pub struct AmplitudeClient {
    api_key: String,
    endpoint: String,
    export_endpoint: String,
    secret_key: String,
    transfer_project_api_key: Option<String>,
    transfer_project_secret_key: Option<String>,
    client: reqwest::Client,
}

impl AmplitudeClient {
    /// Create a new AmplitudeClient with the given API key and optional endpoint.
    pub fn new<S: Into<String>>(api_key: S, endpoint: Option<String>) -> Self {
        AmplitudeClient {
            api_key: api_key.into(),
            endpoint: endpoint.unwrap_or_else(|| DEFAULT_ENDPOINT.to_string()),
            export_endpoint: DEFAULT_EXPORT_ENDPOINT.to_string(),
            secret_key: String::new(), // Will be loaded from config when needed
            transfer_project_api_key: None,
            transfer_project_secret_key: None,
            client: reqwest::Client::new(),
        }
    }

    /// Create a new AmplitudeClient from configuration.
    pub fn from_config(config: AmplitudeConfig) -> Self {
        AmplitudeClient {
            api_key: config.api_key,
            endpoint: config.endpoint,
            export_endpoint: config.export_endpoint,
            secret_key: config.secret_key,
            transfer_project_api_key: config.transfer_project_api_key,
            transfer_project_secret_key: config.transfer_project_secret_key,
            client: reqwest::Client::new(),
        }
    }

    /// Upload a batch of events to Amplitude.
    /// 
    /// This method requires transfer project API credentials to be configured.
    /// The transfer_project_api_key must be set in the configuration file.
    ///
    /// # Example
    /// ```no_run
    /// use amplitude_to_sqlite::amplitude_types::Event;
    /// use amplitude_to_sqlite::amplitude_sdk::AmplitudeClient;
    /// use amplitude_to_sqlite::config::AmplitudeConfig;
    /// # tokio_test::block_on(async {
    /// let config = AmplitudeConfig::load().unwrap();
    /// let client = AmplitudeClient::from_config(config);
    /// let event = Event {
    ///     user_id: Some("user@example.com".to_string()),
    ///     device_id: None,
    ///     event_type: "test_event".to_string(),
    ///     time: 1640995200000,
    ///     event_properties: None,
    ///     user_properties: None,
    ///     groups: None,
    ///     app_version: None,
    ///     platform: None,
    ///     os_name: None,
    ///     os_version: None,
    ///     device_brand: None,
    ///     device_manufacturer: None,
    ///     device_model: None,
    ///     carrier: None,
    ///     country: None,
    ///     region: None,
    ///     city: None,
    ///     dma: None,
    ///     language: None,
    ///     price: None,
    ///     quantity: None,
    ///     revenue: None,
    ///     product_id: None,
    ///     revenue_type: None,
    ///     location_lat: None,
    ///     location_lng: None,
    ///     ip: None,
    ///     idfa: None,
    ///     idfv: None,
    ///     adid: None,
    ///     android_id: None,
    ///     event_id: None,
    ///     session_id: None,
    ///     insert_id: None,
    /// };
    /// let res = client.send_events(vec![event]).await;
    /// # });
    /// ```
    pub async fn send_events(&self, events: Vec<Event>) -> Result<BatchUploadResponse, Box<dyn Error>> {
        // Require transfer project API key for batch uploads
        let api_key = self.transfer_project_api_key.as_ref()
            .ok_or("Transfer project API key is required for batch uploads. Please configure transfer_project_api_key in your config file.")?;
        
        let req_body = BatchUploadRequest {
            api_key: api_key.clone(),
            events,
            options: None,
        };
        let resp = self
            .client
            .post(&self.endpoint)
            .json(&req_body)
            .send()
            .await?;

        let status = resp.status();
        let text = resp.text().await?;
        if status == StatusCode::OK {
            let parsed: BatchUploadResponse = serde_json::from_str(&text)?;
            Ok(parsed)
        } else {
            Err(format!("Amplitude API error ({}): {}", status, text).into())
        }
    }

    /// Export events from Amplitude for a given date range.
    /// 
    /// This method uses the configuration loaded from config file or environment variables.
    ///
    /// # Example
    /// ```no_run
    /// use amplitude_to_sqlite::amplitude_sdk::AmplitudeClient;
    /// use chrono::{DateTime, Utc};
    /// # tokio_test::block_on(async {
    /// let config = AmplitudeConfig::load().unwrap();
    /// let client = AmplitudeClient::from_config(config);
    /// let start = DateTime::parse_from_rfc3339("2024-12-01T00:00:00Z").unwrap().with_timezone(&Utc);
    /// let end = DateTime::parse_from_rfc3339("2025-05-26T23:59:59Z").unwrap().with_timezone(&Utc);
    /// let data = client.export_events(start, end).await.unwrap();
    /// # });
    /// ```
    pub async fn export_events(&self, start: DateTime<Utc>, end: DateTime<Utc>) -> Result<Vec<u8>, Box<dyn Error>> {
        // Use credentials from config if available, otherwise fall back to environment variables
        let api_key = if !self.api_key.is_empty() {
            self.api_key.clone()
        } else {
            env::var("AMPLITUDE_PROJECT_API_KEY")
                .map_err(|_| "AMPLITUDE_PROJECT_API_KEY environment variable not set")?
        };
        
        let secret_key = if !self.secret_key.is_empty() {
            self.secret_key.clone()
        } else {
            env::var("AMPLITUDE_PROJECT_SECRET_KEY")
                .map_err(|_| "AMPLITUDE_PROJECT_SECRET_KEY environment variable not set")?
        };

        // Format dates in the required format: YYYYMMDDTHH
        let start_str = start.format("%Y%m%dT%H").to_string();
        let end_str = end.format("%Y%m%dT%H").to_string();

        // Build the URL with query parameters
        let url = format!("{}?start={}&end={}", self.export_endpoint, start_str, end_str);

        // Make the request with basic auth
        let resp = self
            .client
            .get(&url)
            .basic_auth(&api_key, Some(&secret_key))
            .send()
            .await?;

        let status = resp.status();
        if status == StatusCode::OK {
            let data = resp.bytes().await?;
            Ok(data.to_vec())
        } else {
            let text = resp.text().await?;
            Err(format!("Amplitude export API error ({}): {}", status, text).into())
        }
    }
} 