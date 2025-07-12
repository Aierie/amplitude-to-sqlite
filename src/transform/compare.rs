use crate::common::amplitude_types::ExportEvent;
use crate::common::parser;
use std::collections::HashMap;
use std::fs::{self, File};

pub fn compare_export_events(
    original_dir: &std::path::Path,
    comparison_dir: &std::path::Path,
    output_dir: &std::path::Path,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("Comparing export events between:");
    println!("  Original: {:?}", original_dir);
    println!("  Comparison: {:?}", comparison_dir);
    println!("  Output: {:?}", output_dir);
    
    // Create output directory if it doesn't exist
    fs::create_dir_all(output_dir)?;
    
    // Parse events from both directories
    println!("Parsing events from original directory...");
    let original_events = parser::parse_export_events_from_directory(original_dir)?;
    let original_event_count = original_events.len();
    println!("Found {} events in original directory", original_event_count);
    
    println!("Parsing events from comparison directory...");
    let comparison_events = parser::parse_export_events_from_directory(comparison_dir)?;
    let comparison_event_count = comparison_events.len();
    println!("Found {} events in comparison directory", comparison_event_count);
    
    // Create maps keyed by insert_id for efficient lookup
    let mut original_map: HashMap<String, ExportEvent> = HashMap::new();
    let mut comparison_map: HashMap<String, ExportEvent> = HashMap::new();
    
    for event in original_events {
        if let Some(insert_id) = &event.insert_id {
            original_map.insert(insert_id.clone(), event);
        }
    }
    
    for event in comparison_events {
        if let Some(insert_id) = &event.insert_id {
            comparison_map.insert(insert_id.clone(), event);
        }
    }
    
    println!("Created maps with {} original and {} comparison events", original_map.len(), comparison_map.len());
    
    // Find differences
    let mut only_in_original = Vec::new();
    let mut only_in_comparison = Vec::new();
    let mut different_events = Vec::new();
    let mut identical_events = Vec::new();
    
    // Check for events only in original
    for (insert_id, original_event) in &original_map {
        match comparison_map.get(insert_id) {
            Some(comparison_event) => {
                if events_are_identical(original_event, comparison_event) {
                    identical_events.push(insert_id.clone());
                } else {
                    different_events.push((insert_id.clone(), original_event.clone(), comparison_event.clone()));
                }
            }
            None => {
                only_in_original.push((insert_id.clone(), original_event.clone()));
            }
        }
    }
    
    // Check for events only in comparison
    for (insert_id, comparison_event) in &comparison_map {
        if !original_map.contains_key(insert_id) {
            only_in_comparison.push((insert_id.clone(), comparison_event.clone()));
        }
    }
    
    // Collect IDs for summary before moving the data
    let different_event_ids: Vec<String> = different_events.iter().map(|(id, _, _)| id.clone()).collect();
    let only_original_ids: Vec<String> = only_in_original.iter().map(|(id, _)| id.clone()).collect();
    let only_comparison_ids: Vec<String> = only_in_comparison.iter().map(|(id, _)| id.clone()).collect();
    
    // Clone the vectors to avoid borrow checker issues
    let different_events_clone = different_events.clone();
    let only_in_original_clone = only_in_original.clone();
    let only_in_comparison_clone = only_in_comparison.clone();
    
    // Write summary report
    let summary_path = output_dir.join("comparison_summary.json");
    let summary = serde_json::json!({
        "summary": {
            "total_original_events": original_event_count,
            "total_comparison_events": comparison_event_count,
            "identical_events": identical_events.len(),
            "different_events": different_events.len(),
            "only_in_original": only_in_original.len(),
            "only_in_comparison": only_in_comparison.len()
        },
        "identical_events": identical_events,
        "different_events": different_event_ids,
        "only_in_original": only_original_ids,
        "only_in_comparison": only_comparison_ids
    });
    
    let summary_file = File::create(&summary_path)?;
    serde_json::to_writer_pretty(summary_file, &summary)?;
    println!("Summary written to: {:?}", summary_path);
    
    // Write detailed diff for different events
    if !different_events_clone.is_empty() {
        let diff_dir = output_dir.join("differences");
        fs::create_dir_all(&diff_dir)?;
        
        for (insert_id, original_event, comparison_event) in different_events_clone {
            let diff_path = diff_dir.join(format!("{}.json", sanitize_filename(&insert_id)));
            let diff_file = File::create(&diff_path)?;
            
            let diff_data = serde_json::json!({
                "insert_id": insert_id,
                "original_event": original_event,
                "comparison_event": comparison_event,
                "differences": find_event_differences(&original_event, &comparison_event)
            });
            
            serde_json::to_writer_pretty(diff_file, &diff_data)?;
        }
        
        println!("Detailed differences written to: {:?}", diff_dir);
    }
    
    // Write events only in original
    if !only_in_original_clone.is_empty() {
        let only_original_dir = output_dir.join("only_in_original");
        fs::create_dir_all(&only_original_dir)?;
        
        for (insert_id, event) in only_in_original_clone {
            let event_path = only_original_dir.join(format!("{}.json", sanitize_filename(&insert_id)));
            let event_file = File::create(&event_path)?;
            serde_json::to_writer_pretty(event_file, &event)?;
        }
        
        println!("Events only in original written to: {:?}", only_original_dir);
    }
    
    // Write events only in comparison
    if !only_in_comparison_clone.is_empty() {
        let only_comparison_dir = output_dir.join("only_in_comparison");
        fs::create_dir_all(&only_comparison_dir)?;
        
        for (insert_id, event) in only_in_comparison_clone {
            let event_path = only_comparison_dir.join(format!("{}.json", sanitize_filename(&insert_id)));
            let event_file = File::create(&event_path)?;
            serde_json::to_writer_pretty(event_file, &event)?;
        }
        
        println!("Events only in comparison written to: {:?}", only_comparison_dir);
    }
    
    // Print final summary
    println!("\nComparison completed!");
    println!("  Identical events: {}", identical_events.len());
    println!("  Different events: {}", different_events.len());
    println!("  Only in original: {}", only_in_original.len());
    println!("  Only in comparison: {}", only_in_comparison.len());
    
    Ok(())
}

/// Check if two ExportEvents are identical
fn events_are_identical(event1: &ExportEvent, event2: &ExportEvent) -> bool {
    // Compare key fields that should be identical
    event1.insert_id == event2.insert_id &&
    event1.event_type == event2.event_type &&
    event1.user_id == event2.user_id &&
    event1.device_id == event2.device_id &&
    event1.event_time == event2.event_time &&
    event1.event_properties == event2.event_properties &&
    // We may want to ignore user properties, since they don't matter too much
    event1.user_properties == event2.user_properties &&
    event1.groups == event2.groups &&
    event1.group_properties == event2.group_properties
}

/// Find differences between two ExportEvents
fn find_event_differences(event1: &ExportEvent, event2: &ExportEvent) -> serde_json::Value {
    let mut differences = serde_json::Map::new();
    
    // Compare each field individually to handle different types
    if event1.insert_id != event2.insert_id {
        differences.insert("insert_id".to_string(), serde_json::json!({
            "original": event1.insert_id,
            "comparison": event2.insert_id
        }));
    }
    
    if event1.event_type != event2.event_type {
        differences.insert("event_type".to_string(), serde_json::json!({
            "original": event1.event_type,
            "comparison": event2.event_type
        }));
    }
    
    if event1.user_id != event2.user_id {
        differences.insert("user_id".to_string(), serde_json::json!({
            "original": event1.user_id,
            "comparison": event2.user_id
        }));
    }
    
    if event1.device_id != event2.device_id {
        differences.insert("device_id".to_string(), serde_json::json!({
            "original": event1.device_id,
            "comparison": event2.device_id
        }));
    }
    
    if event1.event_time != event2.event_time {
        differences.insert("event_time".to_string(), serde_json::json!({
            "original": event1.event_time,
            "comparison": event2.event_time
        }));
    }
    
    if event1.event_properties != event2.event_properties {
        differences.insert("event_properties".to_string(), serde_json::json!({
            "original": event1.event_properties,
            "comparison": event2.event_properties
        }));
    }
    
    if event1.user_properties != event2.user_properties {
        differences.insert("user_properties".to_string(), serde_json::json!({
            "original": event1.user_properties,
            "comparison": event2.user_properties
        }));
    }
    
    if event1.groups != event2.groups {
        differences.insert("groups".to_string(), serde_json::json!({
            "original": event1.groups,
            "comparison": event2.groups
        }));
    }
    
    if event1.group_properties != event2.group_properties {
        differences.insert("group_properties".to_string(), serde_json::json!({
            "original": event1.group_properties,
            "comparison": event2.group_properties
        }));
    }
    
    if event1.uuid != event2.uuid {
        differences.insert("uuid".to_string(), serde_json::json!({
            "original": event1.uuid,
            "comparison": event2.uuid
        }));
    }
    
    if event1.session_id != event2.session_id {
        differences.insert("session_id".to_string(), serde_json::json!({
            "original": event1.session_id,
            "comparison": event2.session_id
        }));
    }
    
    if event1.app != event2.app {
        differences.insert("app".to_string(), serde_json::json!({
            "original": event1.app,
            "comparison": event2.app
        }));
    }
    
    if event1.amplitude_id != event2.amplitude_id {
        differences.insert("amplitude_id".to_string(), serde_json::json!({
            "original": event1.amplitude_id,
            "comparison": event2.amplitude_id
        }));
    }
    
    if event1.event_id != event2.event_id {
        differences.insert("event_id".to_string(), serde_json::json!({
            "original": event1.event_id,
            "comparison": event2.event_id
        }));
    }
    
    if event1.client_event_time != event2.client_event_time {
        differences.insert("client_event_time".to_string(), serde_json::json!({
            "original": event1.client_event_time,
            "comparison": event2.client_event_time
        }));
    }
    
    if event1.client_upload_time != event2.client_upload_time {
        differences.insert("client_upload_time".to_string(), serde_json::json!({
            "original": event1.client_upload_time,
            "comparison": event2.client_upload_time
        }));
    }
    
    if event1.server_received_time != event2.server_received_time {
        differences.insert("server_received_time".to_string(), serde_json::json!({
            "original": event1.server_received_time,
            "comparison": event2.server_received_time
        }));
    }
    
    if event1.server_upload_time != event2.server_upload_time {
        differences.insert("server_upload_time".to_string(), serde_json::json!({
            "original": event1.server_upload_time,
            "comparison": event2.server_upload_time
        }));
    }
    
    if event1.processed_time != event2.processed_time {
        differences.insert("processed_time".to_string(), serde_json::json!({
            "original": event1.processed_time,
            "comparison": event2.processed_time
        }));
    }
    
    serde_json::Value::Object(differences)
}

/// Sanitize filename by replacing invalid characters
fn sanitize_filename(filename: &str) -> String {
    filename
        .chars()
        .map(|c| if c.is_alphanumeric() || c == '-' || c == '_' { c } else { '_' })
        .collect()
} 