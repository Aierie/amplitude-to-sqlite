use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::collections::HashMap;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AmplitudeProjectSecrets {
    pub api_key: String,
    pub secret_key: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MultiProjectConfig {
    #[serde(default)]
    pub projects: HashMap<String, AmplitudeProjectSecrets>,
}

impl MultiProjectConfig {
    /// Load configuration from a file
    pub fn from_file(path: &std::path::Path) -> Result<Self, Box<dyn std::error::Error>> {
        let settings = config::Config::builder()
            .add_source(config::File::from(path))
            .build()?;
        
        let config: MultiProjectConfig = settings.try_deserialize()?;
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

        // Fall back to environment variables (legacy support)
        let api_key = std::env::var("AMPLITUDE_PROJECT_API_KEY")
            .map_err(|_| "No config file found and AMPLITUDE_PROJECT_API_KEY environment variable not set")?;
        let secret_key = std::env::var("AMPLITUDE_PROJECT_SECRET_KEY")
            .map_err(|_| "No config file found and AMPLITUDE_PROJECT_SECRET_KEY environment variable not set")?;

        let mut projects = HashMap::new();
        projects.insert("default".to_string(), AmplitudeProjectSecrets {
            api_key,
            secret_key,
        });

        Ok(MultiProjectConfig {
            projects,
        })
    }

    /// Get a specific project configuration
    pub fn get_project(&self, project_name: &str) -> Option<&AmplitudeProjectSecrets> {
        self.projects.get(project_name)
    }

    /// List all available project names
    pub fn list_projects(&self) -> Vec<&String> {
        self.projects.keys().collect()
    }

    /// Create a sample configuration file
    pub fn create_sample_config(path: &std::path::Path) -> Result<(), Box<dyn std::error::Error>> {
        let mut projects = HashMap::new();
        
        // Add sample project
        projects.insert("my_project".to_string(), AmplitudeProjectSecrets {
            api_key: "your_amplitude_project_api_key_here".to_string(),
            secret_key: "your_amplitude_project_secret_key_here".to_string(),
        });

        let sample_config = MultiProjectConfig {
            projects,
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
        println!("You can add multiple projects by adding more entries to the 'projects' section.");
        
        Ok(())
    }
}

// Legacy compatibility - keep the old AmplitudeProjectSecrets methods for backward compatibility
impl AmplitudeProjectSecrets {
} 