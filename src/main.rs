use clap::{arg, command, Parser, Subcommand};
use std::path::PathBuf;

mod amplitude_types;
mod amplitude_sdk;
mod converter;
mod config;
mod verifier;
mod project_selector;
mod difference_cleaner;
mod exporter;
mod uploader;

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
    /// Create a sample configuration file
    Init {
        /// Output path for the configuration file
        #[arg(long, default_value = "./amplitude.toml")]
        config_path: PathBuf,
    },

    /// Manage projects and perform project-specific operations
    Project {
        #[command(subcommand)]
        subcommand: ProjectCommands,
    },

    /// Transform and process Amplitude data
    Transform {
        #[command(subcommand)]
        subcommand: TransformCommands,
    },
}

#[derive(Subcommand)]
enum ProjectCommands {
    /// List all configured projects
    List,
    
    /// Add a new project interactively
    Add,

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
}

#[derive(Subcommand)]
enum TransformCommands {
    /// Convert exported Amplitude JSON files to SQLite database
    Convert {
        /// Input directory containing exported JSON files
        #[arg(long)]
        input_dir: PathBuf,
        
        /// Output SQLite database file
        #[arg(long, default_value = "amplitude_data.sqlite")]
        output_db: PathBuf,
    },

    /// Verify round-trip deserialization of JSON files
    VerifySerde {
        /// Directory containing JSON files to verify
        #[arg(long)]
        input_dir: PathBuf,
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

    /// Filter events based on criteria and output remaining/removed items
    FilterEvents {
        /// Input directory containing exported JSON files
        #[arg(long)]
        input_dir: PathBuf,
        
        /// Output directory for filtered results
        #[arg(long, default_value = "./filter-results")]
        output_dir: PathBuf,

        /// Filter by event type (exact match)
        #[arg(long)]
        event_type: Option<String>,

        /// Filter by user ID (exact match)
        #[arg(long)]
        user_id: Option<String>,

        /// Filter by device ID (exact match)
        #[arg(long)]
        device_id: Option<String>,

        /// Filter by insert ID (exact match)
        #[arg(long)]
        insert_id: Option<String>,

        /// Filter by UUID (exact match)
        #[arg(long)]
        uuid: Option<String>,

        /// Start time filter (YYYY-MM-DD HH:MM:SS format)
        #[arg(long)]
        start_time: Option<String>,

        /// End time filter (YYYY-MM-DD HH:MM:SS format)
        #[arg(long)]
        end_time: Option<String>,

        /// Invert the filter (keep items that don't match criteria)
        #[arg(long, default_value = "false")]
        invert: bool,
    },

    /// Clean up differences in comparison results where property names are the only difference
    CleanDifferences {
        /// Directory containing comparison difference files
        #[arg(long)]
        differences_dir: PathBuf,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    match &cli.command {
        Commands::Init { config_path } => {
            config::MultiProjectConfig::create_sample_config(config_path)?;
        }
        Commands::Project { subcommand } => {
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
                ProjectCommands::Export { start_date, end_date, output_dir, project } => {
                    // Select project first
                    let selector = project_selector::ProjectSelector::new()?;
                    let project_config = selector.select_project(project.as_deref())?;
                    
                    // Call the core export function with the selected project config
                    exporter::export_amplitude_data(start_date, end_date, output_dir, project_config).await?;
                }
                ProjectCommands::Upload { input_dir, batch_size: _, project } => {
                    // Select project first
                    let selector = project_selector::ProjectSelector::new()?;
                    let project_config = selector.select_project(project.as_deref())?;
                    
                    // Call the core upload function with the selected project config
                    uploader::process_and_upload_events(input_dir, project_config).await?;
                }
            }
        }
        Commands::Transform { subcommand } => {
            match subcommand {
                // This can move out maybe
                TransformCommands::Convert { input_dir, output_db } => {
                    converter::convert_json_to_sqlite(input_dir, output_db)?;
                }
                TransformCommands::VerifySerde { input_dir } => {
                    println!("Verifying JSON files in: {}", input_dir.display());
                    let results = verifier::verify_directory(input_dir)?;
                    verifier::print_verification_summary(&results);
                }
                TransformCommands::Compare { original_dir, comparison_dir, output_dir } => {
                    converter::compare_export_events(original_dir, comparison_dir, output_dir)?;
                }
                TransformCommands::CheckForDuplicates { input_dir, output_dir } => {
                    converter::check_for_duplicate_insert_ids(input_dir, output_dir)?;
                }
                TransformCommands::FilterEvents { input_dir, output_dir, event_type, user_id, device_id, insert_id, uuid, start_time, end_time, invert } => {
                    converter::filter_events(input_dir, output_dir, event_type.as_deref(), user_id.as_deref(), device_id.as_deref(), insert_id.as_deref(), uuid.as_deref(), start_time.as_deref(), end_time.as_deref(), *invert)?;
                }
                TransformCommands::CleanDifferences { differences_dir } => {
                    difference_cleaner::clean_property_name_differences(differences_dir)?;
                }
            }
        }
    }

    Ok(())
}
