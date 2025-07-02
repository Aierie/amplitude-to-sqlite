use clap::{arg, command, Parser, Subcommand};
use std::path::PathBuf;

mod amplitude_types;
mod amplitude_sdk;
mod converter;
mod config;

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
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    match &cli.command {
        Commands::Export { start_date, end_date, output_dir } => {
            converter::export_amplitude_data(start_date, end_date, output_dir).await?;
        }
        Commands::Convert { input_dir, output_db } => {
            converter::convert_json_to_sqlite(input_dir, output_db)?;
        }
        Commands::Init { config_path } => {
            config::AmplitudeConfig::create_sample_config(config_path)?;
        }
    }

    Ok(())
}
