use crate::amplitude_types::ExportEvent;
use crate::parser;
use std::collections::HashMap;
use std::fs::{self, File};

/// Check for duplicate insert IDs across events in a directory
pub fn check_for_duplicate_insert_ids(
    input_dir: &std::path::Path,
    output_dir: &std::path::Path,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("Checking for duplicate insert IDs in: {}", input_dir.display());
    
    // Create output directory if it doesn't exist
    fs::create_dir_all(output_dir)?;
    
    // Parse all export events from the input directory
    let events = parser::parse_export_events_from_directory(input_dir)?;
    println!("Found {} total events", events.len());
    
    // Group events by insert_id
    let mut insert_id_groups: HashMap<String, Vec<&ExportEvent>> = HashMap::new();
    
    for event in &events {
        if let Some(insert_id) = &event.insert_id {
            insert_id_groups.entry(insert_id.clone()).or_default().push(event);
        }
    }
    
    // Find duplicates (insert_ids with more than one event)
    let duplicates: HashMap<String, Vec<&ExportEvent>> = insert_id_groups
        .into_iter()
        .filter(|(_, events)| events.len() > 1)
        .collect();
    
    println!("Found {} insert IDs with duplicates", duplicates.len());
    
    if duplicates.is_empty() {
        println!("No duplicate insert IDs found!");
        return Ok(());
    }
    
    // Create summary file
    let summary_path = output_dir.join("duplicate_summary.json");
    let summary = serde_json::json!({
        "total_events": events.len(),
        "unique_insert_ids": events.iter().filter(|e| e.insert_id.is_some()).count(),
        "duplicate_insert_ids_count": duplicates.len(),
        "duplicate_insert_ids": duplicates.keys().collect::<Vec<_>>(),
        "duplicate_counts": duplicates.iter().map(|(id, events)| (id, events.len())).collect::<HashMap<_, _>>()
    });
    
    let summary_file = File::create(&summary_path)?;
    serde_json::to_writer_pretty(summary_file, &summary)?;
    println!("Summary written to: {}", summary_path.display());
    
    // Create individual files for each duplicate insert_id
    for (insert_id, duplicate_events) in &duplicates {
        let filename = sanitize_filename(insert_id);
        let file_path = output_dir.join(format!("duplicate_{}.json", filename));
        
        let duplicate_data = serde_json::json!({
            "insert_id": insert_id,
            "duplicate_count": duplicate_events.len(),
            "events": duplicate_events.iter().map(|event| {
                serde_json::to_value(event).unwrap()
            }).collect::<Vec<_>>()
        });
        
        let file = File::create(&file_path)?;
        serde_json::to_writer_pretty(file, &duplicate_data)?;
        println!("Duplicate events for insert_id '{}' written to: {}", insert_id, file_path.display());
    }
    
    // Create a consolidated file with all duplicates
    let consolidated_path = output_dir.join("all_duplicates.json");
    let consolidated_data = serde_json::json!({
        "summary": {
            "total_events": events.len(),
            "unique_insert_ids": events.iter().filter(|e| e.insert_id.is_some()).count(),
            "duplicate_insert_ids_count": duplicates.len()
        },
        "duplicates": duplicates.iter().map(|(insert_id, events)| {
            serde_json::json!({
                "insert_id": insert_id,
                "count": events.len(),
                "events": events.iter().map(|event| {
                    serde_json::to_value(event).unwrap()
                }).collect::<Vec<_>>()
            })
        }).collect::<Vec<_>>()
    });
    
    let consolidated_file = File::create(&consolidated_path)?;
    serde_json::to_writer_pretty(consolidated_file, &consolidated_data)?;
    println!("All duplicates consolidated in: {}", consolidated_path.display());
    
    println!("Duplicate checking completed successfully!");
    Ok(())
}

/// Sanitize filename by replacing invalid characters
fn sanitize_filename(filename: &str) -> String {
    filename
        .chars()
        .map(|c| if c.is_alphanumeric() || c == '-' || c == '_' { c } else { '_' })
        .collect()
} 