use crate::common::amplitude_types::{ExportEvent, ExportEventFilter, MultiCriteriaFilter};
use crate::common::parser;
use chrono::{DateTime, Utc};
use std::fs::{self, File};
use std::io::Write;

/// Filter events based on criteria and output remaining/removed items
pub fn filter_events(
    input_dir: &std::path::Path,
    output_dir: &std::path::Path,
    event_type: Option<&str>,
    user_id: Option<&str>,
    device_id: Option<&str>,
    insert_id: Option<&str>,
    uuid: Option<&str>,
    start_time: Option<&str>,
    end_time: Option<&str>,
    invert: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    // Parse time filters if provided
    let start_time_filter = if let Some(start_str) = start_time {
        Some(DateTime::parse_from_str(start_str, "%Y-%m-%d %H:%M:%S")?.with_timezone(&Utc))
    } else {
        None
    };
    
    let end_time_filter = if let Some(end_str) = end_time {
        Some(DateTime::parse_from_str(end_str, "%Y-%m-%d %H:%M:%S")?.with_timezone(&Utc))
    } else {
        None
    };
    
    // Create the filter using the trait-based approach
    let mut filter = MultiCriteriaFilter::new(
        event_type.map(|s| s.to_string()),
        user_id.map(|s| s.to_string()),
        device_id.map(|s| s.to_string()),
        insert_id.map(|s| s.to_string()),
        uuid.map(|s| s.to_string()),
        start_time_filter,
        end_time_filter,
        invert,
    );
    
    filter_events_with_filter(input_dir, output_dir, &mut filter)
}

/// Filter events using a trait-based filter
pub fn filter_events_with_filter<F: ExportEventFilter>(
    input_dir: &std::path::Path,
    output_dir: &std::path::Path,
    filter: &mut F,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("Filtering events in: {} using {}", input_dir.display(), filter.description());
    
    // Create output directory if it doesn't exist
    fs::create_dir_all(output_dir)?;
    
    // Parse all export events from the input directory
    let events = parser::parse_export_events_from_directory(input_dir)?;
    println!("Found {} total events", events.len());
    
    // Filter events based on the provided filter
    let (remaining_events, removed_events): (Vec<&ExportEvent>, Vec<&ExportEvent>) = events
        .iter()
        .partition(|event| filter.should_include(event));
    
    println!("Filtered {} events remaining, {} events removed", remaining_events.len(), removed_events.len());
    
    // Create summary file
    let summary_path = output_dir.join("filter_summary.json");
    let summary = serde_json::json!({
        "total_events": events.len(),
        "remaining_events": remaining_events.len(),
        "removed_events": removed_events.len(),
        "filter_description": filter.description()
    });
    
    let summary_file = File::create(&summary_path)?;
    serde_json::to_writer_pretty(summary_file, &summary)?;
    println!("Summary written to: {}", summary_path.display());
    
    // Write remaining events to JSON file
    if !remaining_events.is_empty() {
        let remaining_path = output_dir.join("remaining_events.json");
        let remaining_data = serde_json::json!({
            "count": remaining_events.len(),
            "events": remaining_events.iter().map(|event| {
                serde_json::to_value(event).unwrap()
            }).collect::<Vec<_>>()
        });
        
        let remaining_file = File::create(&remaining_path)?;
        serde_json::to_writer_pretty(remaining_file, &remaining_data)?;
        println!("Remaining events written to: {}", remaining_path.display());
    }
    
    // Write removed events to JSON file
    if !removed_events.is_empty() {
        let removed_path = output_dir.join("removed_events.json");
        let removed_data = serde_json::json!({
            "count": removed_events.len(),
            "events": removed_events.iter().map(|event| {
                serde_json::to_value(event).unwrap()
            }).collect::<Vec<_>>()
        });
        
        let removed_file = File::create(&removed_path)?;
        serde_json::to_writer_pretty(removed_file, &removed_data)?;
        println!("Removed events written to: {}", removed_path.display());
    }
    
    // Also write individual JSONL files for easier processing
    if !remaining_events.is_empty() {
        let remaining_jsonl_path = output_dir.join("remaining_events.jsonl");
        let mut remaining_jsonl_file = File::create(&remaining_jsonl_path)?;
        for event in remaining_events {
            let event_json = serde_json::to_string(event)?;
            writeln!(remaining_jsonl_file, "{}", event_json)?;
        }
        println!("Remaining events (JSONL) written to: {}", remaining_jsonl_path.display());
    }
    
    if !removed_events.is_empty() {
        let removed_jsonl_path = output_dir.join("removed_events.jsonl");
        let mut removed_jsonl_file = File::create(&removed_jsonl_path)?;
        for event in removed_events {
            let event_json = serde_json::to_string(event)?;
            writeln!(removed_jsonl_file, "{}", event_json)?;
        }
        println!("Removed events (JSONL) written to: {}", removed_jsonl_path.display());
    }
    
    println!("Event filtering completed successfully!");
    Ok(())
}

/// Filter events using UUID-based deduplication
/// 
/// This function demonstrates how to use the UUIDDeduplicationFilter to:
/// 1. Always include events with UUID insert_ids
/// 2. Only include the first occurrence of events with non-UUID insert_ids
pub fn filter_events_uuid_deduplication(
    input_dir: &std::path::Path,
    output_dir: &std::path::Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut filter = crate::common::amplitude_types::UUIDDeduplicationFilter::new();
    filter_events_with_filter(input_dir, output_dir, &mut filter)
} 