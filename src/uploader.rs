use crate::amplitude_sdk::AmplitudeClient;
use crate::amplitude_types::ExportEvent;
use crate::config::AmplitudeProjectSecrets;
use std::fs::{self, File};
use std::io::{self, BufRead, BufReader};
use std::path::Path;
use tokio::time::{sleep, Duration};

/// Process JSON files and upload events via batch API using a specific project configuration
pub async fn process_and_upload_events(
    input_dir: &std::path::Path,
    project_config: &AmplitudeProjectSecrets,
) -> Result<(), Box<dyn std::error::Error>> {
    // Create client with provided project config
    let client = AmplitudeClient::from_project_config(project_config);
    
    // Parse all ExportEvents from JSON files
    let export_events = parse_export_events_from_directory(input_dir)?;
    println!("Parsed {} export events", export_events.len());
    
    // Convert ExportEvents to Events
    let mut events = Vec::new();
    let mut failed_conversions = Vec::new();
    for export_event in export_events {
        match export_event.to_batch_event() {
            Ok(event) => events.push(event),
            Err(e) => {
                eprintln!("Failed to convert export event to batch event: {}", e);
                failed_conversions.push(export_event);
                continue;
            }
        }
    }
    println!("Successfully converted {} events", events.len());
    if !failed_conversions.is_empty() {
        println!("Failed to convert {} events", failed_conversions.len());
    }
    
    // Sort events by time
    events.sort_by_key(|event| event.time);
    println!("Sorted events by timestamp");
    
    // Group events by user_id (or device_id if user_id is None)
    let mut user_events: std::collections::HashMap<String, Vec<crate::amplitude_types::Event>> = std::collections::HashMap::new();
    
    for event in events {
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
    let mut failed_uploads = Vec::new();
    let user_batch_size = 65;
    
    for (user_key, user_event_list) in user_events {
        println!("Processing {} events for {}", user_event_list.len(), user_key);
        
        let total_batches_for_user = (user_event_list.len() + user_batch_size - 1) / user_batch_size;
        
        // Create chunks and collect them to avoid borrow checker issues
        let chunks: Vec<_> = user_event_list.chunks(user_batch_size).collect();
        
        // Upload events in batches of 65 for this user/device
        for (batch_index, chunk) in chunks.into_iter().enumerate() {
            println!("Uploading batch {} for {} ({} events)", batch_index + 1, user_key, chunk.len());
            
            match client.send_events(chunk.to_vec()).await {
                Ok(response) => {
                    total_uploaded += chunk.len();
                    total_batches += 1;
                    println!("Batch {} for {} uploaded successfully", batch_index + 1, user_key);
                    
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
                    // Store failed events for later saving
                    for event in chunk {
                        failed_uploads.push((event.clone(), format!("Upload error: {}", e)));
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
    
    // Save failed conversions to JSON file
    if !failed_conversions.is_empty() {
        let failed_conversions_path = input_dir.join("failed_conversions.json");
        let failed_conversions_file = File::create(&failed_conversions_path)?;
        serde_json::to_writer_pretty(failed_conversions_file, &failed_conversions)?;
        println!("Saved {} failed conversions to {:?}", failed_conversions.len(), failed_conversions_path);
    }
    
    // Save failed uploads to JSON file
    if !failed_uploads.is_empty() {
        let failed_uploads_path = input_dir.join("failed_uploads.json");
        let failed_uploads_file = File::create(&failed_uploads_path)?;
        
        // Create a structured format for failed uploads
        let failed_uploads_data: Vec<serde_json::Value> = failed_uploads
            .iter()
            .map(|(event, error)| {
                serde_json::json!({
                    "event": event,
                    "error": error,
                    "timestamp": chrono::Utc::now().to_rfc3339()
                })
            })
            .collect();
        
        serde_json::to_writer_pretty(failed_uploads_file, &failed_uploads_data)?;
        println!("Saved {} failed uploads to {:?}", failed_uploads.len(), failed_uploads_path);
    }
    
    println!("Upload completed successfully!");
    println!("Total events uploaded: {}", total_uploaded);
    println!("Total batches: {}", total_batches);
    println!("Total failed conversions: {}", failed_conversions.len());
    println!("Total failed uploads: {}", failed_uploads.len());
    
    Ok(())
}

/// Parse all ExportEvents from JSON files in a directory (recursively)
fn parse_export_events_from_directory(dir: &Path) -> io::Result<Vec<ExportEvent>> {
    let mut events = Vec::new();
    parse_export_events_recursive(dir, &mut events)?;
    Ok(events)
}

/// Recursively parse ExportEvents from JSON files in a directory tree
fn parse_export_events_recursive(dir: &Path, events: &mut Vec<ExportEvent>) -> io::Result<()> {
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();

        if path.is_dir() {
            // Recursively process subdirectories
            parse_export_events_recursive(&path, events)?;
        } else if path.is_file() {
            let file_name = path.file_name().unwrap().to_string_lossy().to_string();
            
            // Check if it's a JSON file
            if let Some(extension) = path.extension() {
                if extension != "json" {
                    continue;
                }
            }
            
            println!("Processing file: {}", file_name);
            let file = File::open(&path)?;
            let reader = BufReader::new(file);

            for (line_number, line_result) in reader.lines().enumerate() {
                let line = line_result?;
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }

                let export_event: ExportEvent = match serde_json::from_str(trimmed) {
                    Ok(event) => event,
                    Err(e) => {
                        eprintln!("Failed to parse JSON in {} line {}: {}", file_name, line_number + 1, e);
                        continue;
                    }
                };

                events.push(export_event);
            }
        }
    }

    Ok(())
} 