use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Serialize, Deserialize)]
pub struct AmplitudeConfig {
    pub api_key: String,
    pub secret_key: String,
    #[serde(default = "default_endpoint")]
    pub endpoint: String,
    #[serde(default = "default_export_endpoint")]
    pub export_endpoint: String,
    #[serde(default)]
    pub transfer_project_api_key: Option<String>,
    #[serde(default)]
    pub transfer_project_secret_key: Option<String>,
}

fn default_endpoint() -> String {
    "https://api2.amplitude.com/batch".to_string()
}

fn default_export_endpoint() -> String {
    "https://amplitude.com/api/2/export".to_string()
}

impl AmplitudeConfig {
    /// Load configuration from a file
    pub fn from_file(path: &PathBuf) -> Result<Self, Box<dyn std::error::Error>> {
        let settings = config::Config::builder()
            .add_source(config::File::from(path.as_path()))
            .build()?;
        
        let config: AmplitudeConfig = settings.try_deserialize()?;
        Ok(config)
    }

    /// Load configuration from default locations
    pub fn load() -> Result<Self, Box<dyn std::error::Error>> {
        // Try to load from config file first
        let config_paths = vec![
            PathBuf::from("./amplitude.toml"),
            PathBuf::from("./amplitude.json"),
            PathBuf::from("./config/amplitude.toml"),
            PathBuf::from("./config/amplitude.json"),
            dirs::config_dir()
                .map(|p| p.join("amplitude-cli").join("config.toml"))
                .unwrap_or_else(|| PathBuf::from("./amplitude.toml")),
        ];

        for path in config_paths {
            if path.exists() {
                return Self::from_file(&path);
            }
        }

        // Fall back to environment variables
        let api_key = std::env::var("AMPLITUDE_PROJECT_API_KEY")
            .map_err(|_| "No config file found and AMPLITUDE_PROJECT_API_KEY environment variable not set")?;
        let secret_key = std::env::var("AMPLITUDE_PROJECT_SECRET_KEY")
            .map_err(|_| "No config file found and AMPLITUDE_PROJECT_SECRET_KEY environment variable not set")?;

        Ok(AmplitudeConfig {
            api_key,
            secret_key,
            endpoint: default_endpoint(),
            export_endpoint: default_export_endpoint(),
            transfer_project_api_key: None,
            transfer_project_secret_key: None,
        })
    }

    /// Create a sample configuration file
    pub fn create_sample_config(path: &PathBuf) -> Result<(), Box<dyn std::error::Error>> {
        let sample_config = AmplitudeConfig {
            api_key: "your_amplitude_project_api_key_here".to_string(),
            secret_key: "your_amplitude_project_secret_key_here".to_string(),
            endpoint: default_endpoint(),
            export_endpoint: default_export_endpoint(),
            transfer_project_api_key: Some("your_transfer_project_api_key_here".to_string()),
            transfer_project_secret_key: Some("your_transfer_project_secret_key_here".to_string()),
        };

        // Create parent directory if it doesn't exist
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        // Write as TOML
        if path.extension().and_then(|s| s.to_str()) == Some("toml") {
            let toml_string = toml::to_string_pretty(&sample_config)?;
            std::fs::write(path, toml_string)?;
        } else {
            // Default to JSON
            let json_string = serde_json::to_string_pretty(&sample_config)?;
            std::fs::write(path, json_string)?;
        }

        println!("Sample configuration file created at: {:?}", path);
        println!("Please edit the file and add your actual Amplitude API credentials.");
        println!("Note: transfer_project_api_key and transfer_project_secret_key are required for batch upload operations.");
        
        Ok(())
    }
} 