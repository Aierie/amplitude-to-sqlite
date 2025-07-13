//! Uploader module with resumability support
//! 
//! This module provides functionality to upload Amplitude events with automatic resumability.
//! 
//! ## Resumability Features
//! 
//! The uploader automatically tracks progress to enable resuming interrupted uploads:
//! 
//! 1. **Progress File**: A deterministic progress file is created based on the input directory
//!    and project configuration. The filename format is `upload_progress_{hash}.txt` where
//!    the hash is derived from the input directory path and API key. Progress files are stored
//!    in `./output/upload-progress/{hash}/` directory.
//! 
//! 2. **Progress Tracking**: After each successful batch upload, the `insert_id`s of uploaded
//!    events are written to the progress file. The file is kept open and flushed after each
//!    batch to ensure progress is saved even if the process is interrupted.
//! 
//! 3. **Resume on Restart**: When the uploader starts, it reads the progress file and skips
//!    any events that have already been uploaded based on their `insert_id`.
//! 
//! 4. **Deterministic Naming**: The progress filename is deterministic, so running the same
//!    upload command multiple times will use the same progress file.
//! 
//! 5. **Hash-Scoped Output**: All upload-related files (progress files, failed uploads) are stored
//!    in a hash-scoped directory `./output/upload-progress/{hash}/` to keep different upload
//!    operations separate and prevent conflicts.
//! 
//! ## Usage Example
//! 
//! ```rust
//! use crate::project::uploader::process_and_upload_events;
//! use crate::config::config::AmplitudeProjectSecrets;
//! 
//! let project_config = AmplitudeProjectSecrets {
//!     api_key: "your-api-key".to_string(),
//!     secret_key: "your-secret-key".to_string(),
//! };
//! 
//! // This will create/use a progress file like: ./output/upload-progress/a1b2c3d4/upload_progress.txt
//! process_and_upload_events(&std::path::Path::new("./data"), &project_config).await?;
//! 
//! // If interrupted and run again with the same arguments, it will resume from where it left off
//! process_and_upload_events(&std::path::Path::new("./data"), &project_config).await?;
//! ```
//! 
//! ## Progress File Format
//! 
//! The progress file contains one `insert_id` per line:
//! ```
//! insert_id_1
//! insert_id_2
//! insert_id_3
//! ...
//! ```
//! 
//! ## Output Files
//! 
//! The uploader creates the following files in `./output/upload-progress/{hash}/`:
//! 
//! - `upload_progress.txt` - Progress tracking file with uploaded insert_ids
//! - `failed_batch_*.json` - Individual files for each failed batch (if any)
//! 
//! Where `{hash}` is a deterministic hash generated from the input directory path and API key.
//! 
//! Failed batch files are named with the format: `failed_batch_{timestamp}_{batch}_{count}.json`
//! and contain all events from the failed batch along with error information and batch context.

use crate::common::amplitude_sdk::AmplitudeClient;
use crate::config::config::AmplitudeProjectSecrets;
use crate::common::parser;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use tokio::time::{sleep, Duration};
use std::collections::HashSet;

/// Generate a deterministic hash based on input directory and project config
fn generate_upload_hash(input_dir: &Path, project_config: &AmplitudeProjectSecrets) -> String {
    // Create a hash of the input directory path and project API key for deterministic naming
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    
    let mut hasher = DefaultHasher::new();
    input_dir.hash(&mut hasher);
    project_config.api_key.hash(&mut hasher);
    
    format!("{:x}", hasher.finish())
}

/// Get the upload-specific output directory based on input directory and project config
fn get_upload_output_dir(input_dir: &Path, project_config: &AmplitudeProjectSecrets) -> std::path::PathBuf {
    let hash = generate_upload_hash(input_dir, project_config);
    std::path::PathBuf::from("./output/upload-progress").join(hash)
}

/// Read existing progress from file
fn read_progress_file(progress_path: &Path) -> Result<HashSet<String>, Box<dyn std::error::Error>> {
    let mut completed_insert_ids = HashSet::new();
    
    if progress_path.exists() {
        let file = File::open(progress_path)?;
        let reader = BufReader::new(file);
        
        for line in reader.lines() {
            let line = line?;
            let insert_id = line.trim();
            if !insert_id.is_empty() {
                completed_insert_ids.insert(insert_id.to_string());
            }
        }
        
        println!("Found {} previously uploaded events in progress file", completed_insert_ids.len());
    }
    
    Ok(completed_insert_ids)
}

/// Write insert_ids to progress file
fn write_progress_batch(
    progress_file: &mut File,
    insert_ids: &[String],
) -> Result<(), Box<dyn std::error::Error>> {
    for insert_id in insert_ids {
        writeln!(progress_file, "{}", insert_id)?;
    }
    progress_file.flush()?;
    Ok(())
}

/// Process JSON files and upload events via batch API using a specific project configuration
pub async fn process_and_upload_events(
    input_dir: &std::path::Path,
    project_config: &AmplitudeProjectSecrets,
) -> Result<(), Box<dyn std::error::Error>> {
    // Create client with provided project config
    let client = AmplitudeClient::from_project_config(project_config);
    
    // Create upload-specific output directory
    let output_dir = get_upload_output_dir(input_dir, project_config);
    std::fs::create_dir_all(&output_dir)?;
    
    // Generate progress filename and path
    let progress_path = output_dir.join("upload_progress.txt");
    
    // Read existing progress
    let completed_insert_ids = read_progress_file(&progress_path)?;
    
    // Open progress file for appending
    let mut progress_file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&progress_path)?;
    
    // Parse all ExportEvents from JSON files
    let export_events = parser::parse_export_events_from_directory(input_dir)?;
    println!("Parsed {} export events", export_events.len());
    
    // Filter out already uploaded events
    let mut events_to_upload = Vec::new();
    let mut skipped_events = 0;
    
    for export_event in export_events {
        if let Some(insert_id) = &export_event.insert_id {
            if completed_insert_ids.contains(insert_id) {
                skipped_events += 1;
                continue;
            }
        }
        
        match export_event.to_batch_event() {
            Ok(event) => events_to_upload.push(event),
            Err(e) => {
                eprintln!("Failed to convert export event to batch event: {}", e);
                continue;
            }
        }
    }
    
    println!("Skipped {} already uploaded events", skipped_events);
    let total_events = events_to_upload.len();
    println!("Events to upload: {}", total_events);
    
    if total_events == 0 {
        println!("No new events to upload!");
        return Ok(());
    }
    
    // Sort events by time
    events_to_upload.sort_by_key(|event| event.time);
    println!("Sorted events by timestamp");
    
    // Group events by user_id (or device_id if user_id is None)
    let mut user_events: std::collections::HashMap<String, Vec<crate::common::amplitude_types::Event>> = std::collections::HashMap::new();
    
    for event in events_to_upload {
        let key = event.user_id.as_ref()
            .map(|uid| format!("user:{}", uid))
            .or_else(|| event.device_id.as_ref().map(|did| format!("device:{}", did)))
            .unwrap_or_else(|| "unknown".to_string());
        
        user_events.entry(key).or_insert_with(Vec::new).push(event);
    }
    
    println!("Grouped events by {} users/devices", user_events.len());
    
    // Upload events in batches of 65 per user/device
    let mut total_uploaded = 0;
    let mut total_batches = 0;
    let mut failed_uploads_count = 0;
    let user_batch_size = 65;
    // Use a static timestamp for the entire run
    let run_timestamp: i64 = chrono::Utc::now().timestamp_millis();
    
    for (user_key, user_event_list) in user_events {
        println!("Processing {} events for {}", user_event_list.len(), user_key);
        
        let total_batches_for_user = (user_event_list.len() + user_batch_size - 1) / user_batch_size;
        
        // Create chunks and collect them to avoid borrow checker issues
        let chunks: Vec<_> = user_event_list.chunks(user_batch_size).collect();
        
        // Upload events in batches of 65 for this user/device
        for (batch_index, chunk) in chunks.into_iter().enumerate() {
            // Print progress before uploading
            println!(
                "Uploading batch {} for {} ({} events) | Progress: {}/{} uploaded",
                batch_index + 1,
                user_key,
                chunk.len(),
                total_uploaded,
                total_events
            );
            
            // Extract insert_ids for this batch
            let batch_insert_ids: Vec<String> = chunk
                .iter()
                .filter_map(|event| event.insert_id.clone())
                .collect();
            
            match client.send_events(chunk.to_vec()).await {
                Ok(response) => {
                    total_uploaded += chunk.len();
                    total_batches += 1;
                    println!(
                        "Batch {} for {} uploaded successfully | Progress: {}/{} uploaded",
                        batch_index + 1,
                        user_key,
                        total_uploaded,
                        total_events
                    );
                    
                    // Write successful insert_ids to progress file
                    if let Err(e) = write_progress_batch(&mut progress_file, &batch_insert_ids) {
                        eprintln!("Warning: Failed to write progress for batch {}: {}", batch_index + 1, e);
                    }
                    
                    // Log any warnings or issues from the response
                    if let Some(error) = &response.error {
                        eprintln!("Warning: {}", error);
                    }
                    if let Some(missing_field) = &response.missing_field {
                        eprintln!("Warning: Missing field: {}", missing_field);
                    }
                }
                Err(e) => {
                    eprintln!("Failed to upload batch {} for {}: {}", batch_index + 1, user_key, e);
                    
                    // Write the entire failed batch to a single error report file
                    let failed_batch_data = serde_json::json!({
                        "batch_info": {
                            "user_key": user_key,
                            "batch_index": batch_index + 1,
                            "batch_size": chunk.len(),
                            "timestamp": chrono::Utc::now().to_rfc3339()
                        },
                        "error": format!("Upload error: {}", e),
                        "events": chunk
                    });
                    
                    // Create a unique filename for the failed batch
                    let failed_filename = format!("failed_batch_{}_{}_{}.json", 
                        run_timestamp, 
                        batch_index + 1,
                        failed_uploads_count + 1
                    );
                    let failed_path = output_dir.join(failed_filename);
                    
                    // Write the failed batch to its own file
                    if let Ok(mut failed_file) = File::create(&failed_path) {
                        if let Err(write_err) = serde_json::to_writer_pretty(&mut failed_file, &failed_batch_data) {
                            eprintln!("Warning: Failed to write failed batch file {}: {}", failed_path.display(), write_err);
                            // TODO: should this panic? A: probably yes...?
                        } else {
                            failed_uploads_count += 1;
                        }
                    } else {
                        eprintln!("Warning: Failed to create failed batch file: {}", failed_path.display());
                    }
                }
            }

            // Add 1 second delay between upload calls (except after the last batch)
            if batch_index + 1 < total_batches_for_user {
                println!("Waiting 1 second before next batch...");
                sleep(Duration::from_millis(1000)).await;
            }
        }
    }
    
    if failed_uploads_count > 0 {
        println!("Saved {} failed batches to individual files in {:?}", failed_uploads_count, output_dir);
    }
    
    println!("Upload completed successfully!");
    println!("Total events uploaded: {}", total_uploaded);
    println!("Total batches: {}", total_batches);
    println!("Total failed batches: {}", failed_uploads_count);
    println!("Progress saved to: {:?}", progress_path);
    
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn test_generate_upload_hash() {
        let temp_dir = TempDir::new().unwrap();
        let project_config = AmplitudeProjectSecrets {
            api_key: "test-api-key".to_string(),
            secret_key: "test-secret-key".to_string(),
        };
        
        let hash1 = generate_upload_hash(temp_dir.path(), &project_config);
        let hash2 = generate_upload_hash(temp_dir.path(), &project_config);
        
        // Should be deterministic
        assert_eq!(hash1, hash2);
        
        // Should be a hex string (typically 16 characters for a 64-bit hash)
        assert!(!hash1.is_empty());
        assert!(hash1.chars().all(|c| c.is_ascii_hexdigit()));
    }


    #[test]
    fn test_get_upload_output_dir() {
        let temp_dir = TempDir::new().unwrap();
        let project_config = AmplitudeProjectSecrets {
            api_key: "test-api-key".to_string(),
            secret_key: "test-secret-key".to_string(),
        };
        
        let output_dir = get_upload_output_dir(temp_dir.path(), &project_config);
        let expected_hash = generate_upload_hash(temp_dir.path(), &project_config);
        let expected_path = std::path::PathBuf::from("./output/upload-progress").join(expected_hash);
        
        assert_eq!(output_dir, expected_path);
        
        // Should be deterministic
        let output_dir2 = get_upload_output_dir(temp_dir.path(), &project_config);
        assert_eq!(output_dir, output_dir2);
    }

    #[test]
    fn test_read_progress_file() {
        let temp_dir = TempDir::new().unwrap();
        let progress_path = temp_dir.path().join("test_progress.txt");
        
        // Test reading non-existent file
        let result = read_progress_file(&progress_path).unwrap();
        assert_eq!(result.len(), 0);
        
        // Test reading existing file
        let content = "insert_id_1\ninsert_id_2\ninsert_id_3\n";
        fs::write(&progress_path, content).unwrap();
        
        let result = read_progress_file(&progress_path).unwrap();
        assert_eq!(result.len(), 3);
        assert!(result.contains("insert_id_1"));
        assert!(result.contains("insert_id_2"));
        assert!(result.contains("insert_id_3"));
    }

    #[test]
    fn test_write_progress_batch() {
        let temp_dir = TempDir::new().unwrap();
        let progress_path = temp_dir.path().join("test_progress.txt");
        
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .open(&progress_path)
            .unwrap();
        
        let insert_ids = vec!["id1".to_string(), "id2".to_string(), "id3".to_string()];
        write_progress_batch(&mut file, &insert_ids).unwrap();
        
        let content = fs::read_to_string(&progress_path).unwrap();
        let lines: Vec<&str> = content.lines().collect();
        
        assert_eq!(lines.len(), 3);
        assert_eq!(lines[0], "id1");
        assert_eq!(lines[1], "id2");
        assert_eq!(lines[2], "id3");
    }
}

 