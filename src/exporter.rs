use crate::amplitude_sdk::AmplitudeClient;
use crate::config::AmplitudeProjectSecrets;
use chrono::{DateTime, Utc};
use std::fs::{self, File};
use std::io::{self, BufReader, BufWriter};
use std::path::Path;
use zip::ZipArchive;



/// Export events from Amplitude for a given date range using a specific project configuration
pub async fn export_amplitude_data(
    start_date: &str,
    end_date: &str,
    output_dir: &std::path::Path,
    project_config: &AmplitudeProjectSecrets,
) -> Result<(), Box<dyn std::error::Error>> {
    // Parse dates
    let start = DateTime::parse_from_rfc3339(&format!("{}T00:00:00Z", start_date))?.with_timezone(&Utc);
    let end = DateTime::parse_from_rfc3339(&format!("{}T23:00:00Z", end_date))?.with_timezone(&Utc);
    
    // Clean up output directory if it exists
    if output_dir.exists() {
        println!("Cleaning up existing export directory: {:?}", output_dir);
        fs::remove_dir_all(output_dir)?;
        println!("Successfully cleaned up export directory");
    }
    
    // Create output directory
    fs::create_dir_all(output_dir)?;
    
    // Create client with provided project config
    let client = AmplitudeClient::from_project_config(project_config);
    let export_data = client.export_events(start, end).await?;
    
    // Save the zip file
    let zip_path = output_dir.join("amplitude-export.zip");
    fs::write(&zip_path, export_data)?;
    println!("Export saved to: {:?}", zip_path);
    
    // Extract the zip file
    let zip_file = File::open(&zip_path)?;
    let mut archive = ZipArchive::new(zip_file)?;
    
    for i in 0..archive.len() {
        let mut file = archive.by_index(i)?;
        let outpath = output_dir.join(file.name());
        
        if file.name().ends_with('/') {
            fs::create_dir_all(&outpath)?;
        } else {
            if let Some(p) = outpath.parent() {
                if !p.exists() {
                    fs::create_dir_all(p)?;
                }
            }
            let mut outfile = File::create(&outpath)?;
            io::copy(&mut file, &mut outfile)?;
        }
    }
    
    println!("Export extracted to: {:?}", output_dir);
    
    // Unzip all gzipped files in the extracted directory tree
    let unzipped_files = unzip_gz_files_recursive(output_dir)?;
    if !unzipped_files.is_empty() {
        println!("Unzipped {} gzipped files", unzipped_files.len());
    }
    
    Ok(())
}

// Recursively unzips all `.gz` files in a directory tree, replacing the original files
fn unzip_gz_files_recursive(dir: &Path) -> io::Result<Vec<String>> {
    let mut processed_files = Vec::new();

    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();

        if path.is_dir() {
            // Recursively process subdirectories
            let sub_processed = unzip_gz_files_recursive(&path)?;
            processed_files.extend(sub_processed);
        } else if path.extension().and_then(|s| s.to_str()) == Some("gz") {
            // Unzip the file in place
            let file_name = path.file_name().unwrap().to_string_lossy().to_string();
            let output_name = path.file_stem().unwrap().to_string_lossy().to_string();
            let dst_file_path = path.with_file_name(&output_name);

            let input_file = File::open(&path)?;
            let mut decoder = flate2::read::GzDecoder::new(BufReader::new(input_file));
            let output_file = File::create(&dst_file_path)?;
            let mut writer = BufWriter::new(output_file);

            io::copy(&mut decoder, &mut writer)?;
            
            // Remove the original .gz file
            fs::remove_file(&path)?;
            
            processed_files.push(file_name);
        }
    }

    Ok(processed_files)
} 