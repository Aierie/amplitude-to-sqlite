use crate::amplitude_types::ExportEvent;
use std::fs;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use walkdir::WalkDir;

#[derive(Debug)]
pub struct VerificationResult {
    pub file_path: PathBuf,
    pub total_events: usize,
    pub successful_events: usize,
    pub failed_events: Vec<EventError>,
}

#[derive(Debug)]
pub struct EventError {
    pub line_number: usize,
    pub error: String,
    pub original_json: String,
}

pub fn verify_directory(directory: &Path) -> Result<Vec<VerificationResult>, Box<dyn std::error::Error>> {
    let mut results = Vec::new();
    
    // Walk through all files in the directory
    for entry in WalkDir::new(directory)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_file())
    {
        let file_path = entry.path();
        
        // Check if it's a JSON file
        if let Some(extension) = file_path.extension() {
            if extension == "json" {
                println!("Verifying file: {}", file_path.display());
                let result = verify_file(file_path)?;
                results.push(result);
            }
        }
    }
    
    Ok(results)
}

pub fn verify_file(file_path: &Path) -> Result<VerificationResult, Box<dyn std::error::Error>> {
    let file_content = fs::read_to_string(file_path)
        .map_err(|e| format!("Failed to read file {}: {}", file_path.display(), e))?;
    
    let reader = BufReader::new(file_content.as_bytes());
    let mut total_events = 0;
    let mut successful_events = 0;
    let mut failed_events = Vec::new();
    
    // Parse each line as a separate JSON object
    for (line_number, line_result) in reader.lines().enumerate() {
        let line_number = line_number + 1; // Convert to 1-based indexing
        let line = line_result.map_err(|e| format!("Failed to read line {}: {}", line_number, e))?;
        
        if line.trim().is_empty() {
            continue;
        }
        
        total_events += 1;
        
        // Try to deserialize the export event
        let export_event: ExportEvent = match serde_json::from_str(&line) {
            Ok(event) => event,
            Err(e) => {
                failed_events.push(EventError {
                    line_number,
                    error: format!("Failed to deserialize: {}", e),
                    original_json: line.clone(),
                });
                continue;
            }
        };

        if export_event.event_type.is_none() {
            failed_events.push(EventError {
                line_number,
                error: "Event type is empty".to_string(),
                original_json: line.clone(),
            });
            continue;
        }

        // Assert that insert_id is not empty
        if export_event.insert_id.is_none() {
            failed_events.push(EventError {
                line_number,
                error: "Insert ID is empty".to_string(),
                original_json: line.clone(),
            });
            continue;
        }

        
        // Try to serialize back to JSON
        let round_trip_json = match serde_json::to_string(&export_event) {
            Ok(json) => json,
            Err(e) => {
                failed_events.push(EventError {
                    line_number,
                    error: format!("Failed to serialize: {}", e),
                    original_json: line.clone(),
                });
                continue;
            }
        };
        
        // Normalize both JSONs for comparison by parsing and re-serializing
        let original_normalized: serde_json::Value = match serde_json::from_str(&line) {
            Ok(value) => value,
            Err(e) => {
                failed_events.push(EventError {
                    line_number,
                    error: format!("Failed to parse original JSON: {}", e),
                    original_json: line.clone(),
                });
                continue;
            }
        };
        
        let round_trip_normalized: serde_json::Value = match serde_json::from_str(&round_trip_json) {
            Ok(value) => value,
            Err(e) => {
                failed_events.push(EventError {
                    line_number,
                    error: format!("Failed to parse round-trip JSON: {}", e),
                    original_json: line.clone(),
                });
                continue;
            }
        };
        
        // Compare the normalized JSONs
        if original_normalized != round_trip_normalized {
            let differences = find_json_differences(&original_normalized, &round_trip_normalized);
            failed_events.push(EventError {
                line_number,
                error: format!("JSON mismatch:\n{}", differences.join("\n")),
                original_json: line.clone(),
            });
        } else {
            successful_events += 1;
        }
    }
    
    Ok(VerificationResult {
        file_path: file_path.to_path_buf(),
        total_events,
        successful_events,
        failed_events,
    })
}

fn find_json_differences(original: &serde_json::Value, round_trip: &serde_json::Value) -> Vec<String> {
    let mut differences = Vec::new();
    
    if let (Some(original_obj), Some(round_trip_obj)) = (original.as_object(), round_trip.as_object()) {
        // Check for fields present in original but missing or different in round-trip
        for (key, original_value) in original_obj {
            match round_trip_obj.get(key) {
                Some(round_trip_value) => {
                    if original_value != round_trip_value {
                        differences.push(format!(
                            "Field '{}': original = {:?}, round_trip = {:?}",
                            key, original_value, round_trip_value
                        ));
                    }
                }
                None => {
                    differences.push(format!(
                        "Field '{}': present in original ({:?}) but missing in round_trip",
                        key, original_value
                    ));
                }
            }
        }
        
        // Check for fields present in round-trip but missing in original
        for (key, round_trip_value) in round_trip_obj {
            if !original_obj.contains_key(key) {
                differences.push(format!(
                    "Field '{}': missing in original but present in round_trip ({:?})",
                    key, round_trip_value
                ));
            }
        }
    } else {
        differences.push("JSON structure is not an object".to_string());
    }
    
    differences
}

pub fn print_verification_summary(results: &[VerificationResult]) {
    let total_files = results.len();
    let mut total_events = 0;
    let mut total_successful = 0;
    let mut total_failed = 0;
    let mut files_with_errors = 0;
    
    println!("\n=== Verification Summary ===");
    
    for result in results {
        total_events += result.total_events;
        total_successful += result.successful_events;
        total_failed += result.failed_events.len();
        
        if !result.failed_events.is_empty() {
            files_with_errors += 1;
            println!("\n❌ {} ({} events, {} failed)", 
                result.file_path.display(), 
                result.total_events, 
                result.failed_events.len()
            );
            
            for error in &result.failed_events {
                println!("  Line {}: {}", error.line_number, error.error);
            }
        } else {
            println!("✅ {} ({} events)", 
                result.file_path.display(), 
                result.total_events
            );
        }
    }
    
    println!("\n=== Overall Statistics ===");
    println!("Files processed: {}", total_files);
    println!("Files with errors: {}", files_with_errors);
    println!("Total events: {}", total_events);
    println!("Successful events: {}", total_successful);
    println!("Failed events: {}", total_failed);
    
    if total_events > 0 {
        let success_rate = (total_successful as f64 / total_events as f64) * 100.0;
        println!("Success rate: {:.2}%", success_rate);
    }
    
    if total_failed == 0 {
        println!("\n🎉 All files passed verification!");
    } else {
        println!("\n⚠️  {} files have verification errors", files_with_errors);
    }
} 