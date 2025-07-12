use crate::config::config::{AmplitudeProjectSecrets, MultiProjectConfig};
use dialoguer::{Select, Input};

pub struct ProjectSelector {
    pub config: MultiProjectConfig,
}

impl ProjectSelector {
    pub fn new() -> Result<Self, Box<dyn std::error::Error>> {
        let config = MultiProjectConfig::load()?;
        Ok(ProjectSelector { config })
    }

    /// Select a project interactively or use the provided project name
    pub fn select_project(&self, project_name: Option<&str>) -> Result<&AmplitudeProjectSecrets, Box<dyn std::error::Error>> {
        if let Some(name) = project_name {
            println!("Using project configuration: {}", name);
            // Use the specified project name
            self.config.get_project(name)
                .ok_or_else(|| format!("Project '{}' not found in configuration", name).into())
        } else {
            // Interactive selection
            self.select_project_interactive()
        }
    }

    /// Interactive project selection
    fn select_project_interactive(&self) -> Result<&AmplitudeProjectSecrets, Box<dyn std::error::Error>> {
        let projects: Vec<&String> = self.config.list_projects();
        
        if projects.is_empty() {
            return Err("No projects found in configuration".into());
        }

        // Multiple projects, let user choose
        println!("Available projects:");
        for (i, project_name) in projects.iter().enumerate() {
            println!("  {}. {}", i + 1, project_name);
        }

        let selection = Select::new()
            .with_prompt("Select a project")
            .items(&projects)
            .default(0)
            .interact()?;

        let selected_project = projects[selection];
        println!("Using project configuration: {}", projects[selection]);
        self.config.get_project(selected_project)
            .ok_or_else(|| "Failed to get selected project configuration".into())
    }

    /// List all available project names
    pub fn list_projects(&self) -> Vec<&String> {
        self.config.list_projects()
    }

    /// Add a new project interactively
    pub fn add_project_interactive(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let project_name: String = Input::new()
            .with_prompt("Enter project name")
            .interact_text()?;

        if self.config.projects.contains_key(&project_name) {
            let overwrite = dialoguer::Confirm::new()
                .with_prompt(format!("Project '{}' already exists. Overwrite?", project_name))
                .interact()?;
            
            if !overwrite {
                println!("Project creation cancelled.");
                return Ok(());
            }
        }

        let api_key: String = Input::new()
            .with_prompt("Enter API key")
            .interact_text()?;

        let secret_key: String = Input::new()
            .with_prompt("Enter secret key")
            .interact_text()?;

        let project_config = AmplitudeProjectSecrets {
            api_key,
            secret_key,
        };

        self.config.projects.insert(project_name.clone(), project_config);

        println!("Project '{}' added successfully!", project_name);
        Ok(())
    }

    /// Save the configuration to file
    pub fn save_config(&self, path: Option<&std::path::Path>) -> Result<(), Box<dyn std::error::Error>> {
        let default_path = std::path::PathBuf::from("./amplitude.toml");
        let config_path = path.unwrap_or(&default_path);
        
        // Create parent directory if it doesn't exist
        if let Some(parent) = config_path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        // Write as TOML
        if config_path.extension().and_then(|s| s.to_str()) == Some("toml") {
            let toml_string = toml::to_string_pretty(&self.config)?;
            std::fs::write(config_path, toml_string)?;
        } else {
            // Default to JSON
            let json_string = serde_json::to_string_pretty(&self.config)?;
            std::fs::write(config_path, json_string)?;
        }

        println!("Configuration saved to: {:?}", config_path);
        Ok(())
    }
} 