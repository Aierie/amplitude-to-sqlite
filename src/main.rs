use clap::{arg, command, Parser, Subcommand};
use std::path::PathBuf;

mod common;
mod config;
mod project;
mod transform;

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
        #[arg(long, default_value = "./output/export")]
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
        #[arg(long, default_value = "./output/comparison-results")]
        output_dir: PathBuf,
    },

    /// Clean duplicates based on insert_id, determine DupeTypes, and write results to JSON files
    CleanDuplicates {
        /// Input directory containing exported JSON files
        #[arg(long)]
        input_dir: PathBuf,
        
        /// Output directory for dupe analysis files
        #[arg(long, default_value = "./output/dupe-analysis-results")]
        output_dir: PathBuf,

        /// Output mode: analyze (default), debug, or full
        #[arg(long, default_value = "analyze")]
        output_mode: String,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    match &cli.command {
        Commands::Init { config_path } => {
            config::config::MultiProjectConfig::create_sample_config(config_path)?;
        }
        Commands::Project { subcommand } => {
            match subcommand {
                ProjectCommands::List => {
                    let selector = project::project_selector::ProjectSelector::new()?;
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
                    let mut selector = project::project_selector::ProjectSelector::new()?;
                    selector.add_project_interactive()?;
                    selector.save_config(None)?;
                }
                ProjectCommands::Export { start_date, end_date, output_dir, project } => {
                    // Select project first
                    let selector = project::project_selector::ProjectSelector::new()?;
                    let project_config = selector.select_project(project.as_deref())?;
                    
                    // Call the core export function with the selected project config
                    project::exporter::export_amplitude_data(start_date, end_date, output_dir, project_config).await?;
                }
                ProjectCommands::Upload { input_dir, batch_size: _, project } => {
                    // Select project first
                    let selector = project::project_selector::ProjectSelector::new()?;
                    let project_config = selector.select_project(project.as_deref())?;
                    
                    // Call the core upload function with the selected project config
                    project::uploader::process_and_upload_events(input_dir, project_config).await?;
                }
            }
        }
        Commands::Transform { subcommand } => {
            match subcommand {
                TransformCommands::VerifySerde { input_dir } => {
                    println!("Verifying JSON files in: {}", input_dir.display());
                    let results = transform::verifier::verify_directory(input_dir)?;
                    transform::verifier::print_verification_summary(&results);
                }
                TransformCommands::Compare { original_dir, comparison_dir, output_dir } => {
                    transform::compare::compare_export_events(original_dir, comparison_dir, output_dir)?;
                }
                TransformCommands::CleanDuplicates { input_dir, output_dir, output_mode } => {
                    let mode = match output_mode.as_str() {
                        "analyze" => transform::OutputMode::Analyze,
                        "debug" => transform::OutputMode::Debug,
                        "full" => transform::OutputMode::Full,
                        _ => {
                            eprintln!("Invalid output mode: {}. Valid options are: analyze, debug, full", output_mode);
                            std::process::exit(1);
                        }
                    };
                    transform::clean_duplicates_and_types(input_dir, output_dir, mode)?;
                }
            }
        }
    }

    Ok(())
}
