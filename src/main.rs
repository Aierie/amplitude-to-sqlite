use clap::{arg, command, Parser, Subcommand};
use std::path::PathBuf;

mod amplitude_types;
mod amplitude_sdk;
mod converter;
mod config;
mod verifier;
mod project_selector;

#[derive(Parser)]
#[command(name = "amplitude-cli")]
#[command(about = "A CLI tool for Amplitude data export and conversion")]
#[command(version)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Export events from Amplitude to JSON files
    Export {
        /// Start date for export (YYYY-MM-DD format)
        #[arg(long)]
        start_date: String,
        
        /// End date for export (YYYY-MM-DD format)
        #[arg(long)]
        end_date: String,
        
        /// Output directory for exported files
        #[arg(long, default_value = "./export")]
        output_dir: PathBuf,

        /// Project name to use (if not specified, will prompt for selection)
        #[arg(long)]
        project: Option<String>,
    },
    
    /// Convert exported Amplitude JSON files to SQLite database
    Convert {
        /// Input directory containing exported JSON files
        #[arg(long)]
        input_dir: PathBuf,
        
        /// Output SQLite database file
        #[arg(long, default_value = "amplitude_data.sqlite")]
        output_db: PathBuf,
    },

    /// Create a sample configuration file
    Init {
        /// Output path for the configuration file
        #[arg(long, default_value = "./amplitude.toml")]
        config_path: PathBuf,
    },

    /// Verify round-trip deserialization of JSON files
    VerifyDeserialization {
        /// Directory containing JSON files to verify
        #[arg(long)]
        input_dir: PathBuf,
    },

    /// Process JSON files and upload events via batch API
    Upload {
        /// Input directory containing JSON files with ExportEvents
        #[arg(long)]
        input_dir: PathBuf,
        
        /// Batch size for uploads (default: 1000)
        #[arg(long, default_value = "1000")]
        batch_size: usize,

        /// Project name to use (if not specified, will prompt for selection)
        #[arg(long)]
        project: Option<String>,
    },

    /// End-to-end round-trip: export from one project and upload to another
    RoundTripE2E {
        /// Start date for export (YYYY-MM-DD format)
        #[arg(long)]
        start_date: String,
        
        /// End date for export (YYYY-MM-DD format)
        #[arg(long)]
        end_date: String,
        
        /// Output directory for exported files
        #[arg(long, default_value = "./e2e-test-export")]
        output_dir: PathBuf,

        /// Project name to export from (if not specified, will prompt for selection)
        #[arg(long)]
        export_from: Option<String>,

        /// Project name to upload to (if not specified, will prompt for selection)
        #[arg(long)]
        upload_to: Option<String>,
    },

    /// Compare export events between original and comparison directories
    Compare {
        /// Directory containing original export events
        #[arg(long)]
        original_dir: PathBuf,
        
        /// Directory containing comparison export events
        #[arg(long)]
        comparison_dir: PathBuf,
        
        /// Output directory for comparison results
        #[arg(long, default_value = "./comparison-results")]
        output_dir: PathBuf,
    },

    /// Check for duplicate insert IDs across events in a directory
    CheckForDuplicates {
        /// Input directory containing exported JSON files
        #[arg(long)]
        input_dir: PathBuf,
        
        /// Output directory for duplicate event files
        #[arg(long, default_value = "./duplicate-results")]
        output_dir: PathBuf,
    },

    /// Manage projects in configuration
    Projects {
        #[command(subcommand)]
        subcommand: ProjectCommands,
    },
}

#[derive(Subcommand)]
enum ProjectCommands {
    /// List all configured projects
    List,
    
    // TODO: I have not checked this manually
    /// Add a new project interactively
    Add,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    match &cli.command {
        Commands::Export { start_date, end_date, output_dir, project } => {
            converter::export_amplitude_data_with_project(start_date, end_date, output_dir, project.as_deref()).await?;
        }
        Commands::Convert { input_dir, output_db } => {
            converter::convert_json_to_sqlite(input_dir, output_db)?;
        }
        Commands::Init { config_path } => {
            config::MultiProjectConfig::create_sample_config(config_path)?;
        }
        Commands::VerifyDeserialization { input_dir } => {
            println!("Verifying JSON files in: {}", input_dir.display());
            let results = verifier::verify_directory(input_dir)?;
            verifier::print_verification_summary(&results);
        }
        Commands::Upload { input_dir, batch_size, project } => {
            converter::process_and_upload_events_with_project(input_dir, *batch_size, project.as_deref()).await?;
        }
        Commands::RoundTripE2E { start_date, end_date, output_dir, export_from, upload_to } => {
            converter::round_trip_e2e(start_date, end_date, output_dir, export_from.as_deref(), upload_to.as_deref()).await?;
        }
        Commands::Compare { original_dir, comparison_dir, output_dir } => {
            converter::compare_export_events(original_dir, comparison_dir, output_dir)?;
        }
        Commands::CheckForDuplicates { input_dir, output_dir } => {
            converter::check_for_duplicate_insert_ids(input_dir, output_dir)?;
        }
        Commands::Projects { subcommand } => {
            match subcommand {
                ProjectCommands::List => {
                    let selector = project_selector::ProjectSelector::new()?;
                    let projects = selector.list_projects();
                    
                    if projects.is_empty() {
                        println!("No projects configured.");
                    } else {
                        println!("Configured projects:");
                        for project_name in projects {
                            println!("  {}", project_name);
                        }
                    }
                }
                ProjectCommands::Add => {
                    let mut selector = project_selector::ProjectSelector::new()?;
                    selector.add_project_interactive()?;
                    selector.save_config(None)?;
                }
            }
        }
    }

    Ok(())
}
