use crate::amplitude_sdk::AmplitudeClient;
use crate::amplitude_types::{ExportEvent, ExportEventFilter, MultiCriteriaFilter};
use crate::project_selector::ProjectSelector;
use chrono::{DateTime, Utc};
use flate2::read::GzDecoder;
use rusqlite::{params, Connection, Result};
use serde_json::Value;
use std::fs::{self, File};
use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::path::Path;
use zip::ZipArchive;

#[derive(Debug)]
pub struct ParsedItem {
    pub user_id: Option<String>,
    pub screen_name: Option<String>,
    pub event_name: String,
    pub server_event: bool,
    pub event_time: chrono::DateTime<Utc>,
    pub uuid: String,
    pub raw_json: String,
    pub source_file: String,
    pub session_id: Option<u64>,
}

/// Export events from Amplitude for a given date range with project selection
pub async fn export_amplitude_data_with_project(
    start_date: &str,
    end_date: &str,
    output_dir: &std::path::Path,
    project_name: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    // Select project
    let selector = ProjectSelector::new()?;
    let project_config = selector.select_project(project_name)?;
    
    println!("Using project configuration");
    
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
    
    // Create client with selected project config
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

/// Convert exported Amplitude JSON files to SQLite database
pub fn convert_json_to_sqlite(
    input_dir: &std::path::Path,
    output_db: &std::path::Path,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("Converting JSON files from {:?} to SQLite database: {:?}", input_dir, output_db);
    
    // Unzip all .gz files in the input directory
    let unzipped_dir = tempfile::tempdir()?;
    let processed_files = unzip_gz_files(input_dir, unzipped_dir.path())?;
    
    if processed_files.is_empty() {
        println!("No .gz files found in input directory");
        return Ok(());
    }
    
    // Parse all JSON lines from unzipped files
    let parsed_items = parse_json_objects_in_dir(unzipped_dir.path())?;
    
    // Write parsed items to SQLite
    write_parsed_items_to_sqlite(output_db, &parsed_items, &processed_files)?;
    
    println!("Conversion completed successfully!");
    Ok(())
}

// Unzips all `.gz` files in a source directory into a destination directory
fn unzip_gz_files(src_dir: &Path, dst_dir: &Path) -> io::Result<Vec<String>> {
    fs::create_dir_all(dst_dir)?;
    let mut processed_files = Vec::new();

    for entry in fs::read_dir(src_dir)? {
        let entry = entry?;
        let path = entry.path();

        if path.extension().and_then(|s| s.to_str()) == Some("gz") {
            let file_name = path.file_name().unwrap().to_string_lossy().to_string();
            let output_name = path.file_stem().unwrap().to_string_lossy().to_string();
            let dst_file_path = dst_dir.join(&output_name);

            let input_file = File::open(&path)?;
            let mut decoder = GzDecoder::new(BufReader::new(input_file));
            let output_file = File::create(dst_file_path)?;
            let mut writer = BufWriter::new(output_file);

            io::copy(&mut decoder, &mut writer)?;
            processed_files.push(file_name);
        }
    }

    Ok(processed_files)
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
            let mut decoder = GzDecoder::new(BufReader::new(input_file));
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

// Parses all JSON lines from files in a directory
fn parse_json_objects_in_dir(dir: &Path) -> io::Result<Vec<ParsedItem>> {
    let mut results = Vec::new();

    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();

        if path.is_file() {
            let file_name = path.file_name().unwrap().to_string_lossy().to_string();
            let file = File::open(&path)?;
            let reader = BufReader::new(file);

            for line_result in reader.lines() {
                let line = line_result?;
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }

                let json: Value = match serde_json::from_str(trimmed) {
                    Ok(v) => v,
                    Err(e) => {
                        eprintln!("Failed to parse JSON in {}: {}", file_name, e);
                        continue;
                    }
                };

                let user_id = json
                    .get("user_id")
                    .and_then(|v| v.as_str().map(|s| s.to_string()));

                let uuid = json
                    .get("uuid")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "Missing uuid"))?
                    .to_string();

                let server_event: bool = json
                    .get("data")
                    .unwrap()
                    .get("path")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| {
                        io::Error::new(
                            io::ErrorKind::InvalidData,
                            "Missing data/path for server_event",
                        )
                    })?
                    != "/";
                let event_time: chrono::DateTime<Utc> = json
                    .get("event_time")
                    .map(|v| {
                        chrono::DateTime::parse_from_str(
                            &format!("{} +0000", v.as_str().unwrap().to_owned()),
                            "%Y-%m-%d %H:%M:%S%.6f %z",
                        )
                        .unwrap()
                        .to_utc()
                    })
                    .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "Missing event time"))
                    .unwrap();
                let event_name: String = json
                    .get("event_type")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| {
                        io::Error::new(io::ErrorKind::InvalidData, "Missing event name")
                    })?
                    .to_string();
                let session_id: Option<u64> = json
                    .get("session_id")
                    .and_then(|v| {
                        match v {
                            Value::Null => None,
                            Value::Bool(_) => None,
                            Value::Number(number) => number.as_u64(),
                            Value::String(_) => None,
                            Value::Array(_values) => None,
                            Value::Object(_map) => None,
                        }
                    });
                let screen_name: Option<String> = None;
                results.push(ParsedItem {
                    user_id,
                    uuid,
                    event_name,
                    server_event,
                    event_time,
                    screen_name,
                    session_id,
                    raw_json: trimmed.to_string(),
                    source_file: file_name.clone(),
                });
            }
        }
    }

    Ok(results)
}

// Writes parsed items to a SQLite DB, avoiding duplicates and tracking import metadata
fn write_parsed_items_to_sqlite<P: AsRef<Path>>(
    db_path: P,
    items: &[ParsedItem],
    processed_files: &[String],
) -> Result<()> {
    let mut conn = Connection::open(db_path)?;

    // Ensure required tables exist
    conn.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS amplitude_events (
            uuid TEXT PRIMARY KEY,
            user_id TEXT,
            event_screen TEXT,
            server_event INTEGER,
            event_time DATETIME NOT NULL,
            event_name TEXT NOT NULL,
            session_id INTEGER,
            raw_json TEXT NOT NULL,
            source_file TEXT NOT NULL,
            created_at DATETIME NOT NULL
        );

        CREATE TABLE IF NOT EXISTS imported_files (
            filename TEXT PRIMARY KEY,
            imported_at DATETIME DEFAULT CURRENT_TIMESTAMP
        );
        ",
    )?;

    let tx = conn.transaction()?;

    // Mark files as imported
    {
        let mut stmt = tx.prepare("INSERT OR IGNORE INTO imported_files (filename) VALUES (?1)")?;
        for filename in processed_files {
            stmt.execute(params![filename])?;
        }
    }

    let mut inserted = 0;
    {
        // Insert parsed items
        let mut stmt = tx.prepare(
            "INSERT OR IGNORE INTO amplitude_events (uuid, user_id, raw_json, source_file, created_at, event_screen, server_event, event_time, event_name, session_id)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
        )?;

        for item in items {
            let rows = stmt.execute(params![
                item.uuid,
                item.user_id.as_deref(),
                item.raw_json,
                item.source_file,
                Utc::now().to_rfc3339(),
                item.screen_name,
                if item.server_event { 1 } else { 0 },
                item.event_time.to_rfc3339(),
                item.event_name,
                item.session_id,
            ])?;
            inserted += rows;
        }
    }

    tx.commit()?;

    println!(
        "Inserted {} new items. Skipped {} duplicates.",
        inserted,
        items.len() - inserted
    );

    Ok(())
}

/// Process JSON files and upload events via batch API with project selection
pub async fn process_and_upload_events_with_project(
    input_dir: &std::path::Path,
    batch_size: usize,
    project_name: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    // Select project
    let selector = ProjectSelector::new()?;
    let project_config = selector.select_project(project_name)?;
    
    println!("Using project configuration");
    
    // Create client with selected project config
    let client = AmplitudeClient::from_project_config(project_config);
    
    // Parse all ExportEvents from JSON files
    let export_events = parse_export_events_from_directory(input_dir)?;
    println!("Parsed {} export events", export_events.len());
    
    // Convert ExportEvents to Events
    let mut events = Vec::new();
    for export_event in export_events {
        match export_event.to_batch_event() {
            Ok(event) => events.push(event),
            Err(e) => {
                eprintln!("Failed to convert export event to batch event: {}", e);
                continue;
            }
        }
    }
    println!("Successfully converted {} events", events.len());
    
    // Sort events by time
    events.sort_by_key(|event| event.time);
    println!("Sorted events by timestamp");
    
    // Upload events in batches
    let mut total_uploaded = 0;
    let mut total_batches = 0;
    
    for (batch_index, chunk) in events.chunks(batch_size).enumerate() {
        println!("Uploading batch {} ({} events)", batch_index + 1, chunk.len());
        
        match client.send_events(chunk.to_vec()).await {
            Ok(response) => {
                total_uploaded += chunk.len();
                total_batches += 1;
                println!("Batch {} uploaded successfully", batch_index + 1);
                
                // Log any warnings or issues from the response
                if let Some(error) = &response.error {
                    eprintln!("Warning: {}", error);
                }
                if let Some(missing_field) = &response.missing_field {
                    eprintln!("Warning: Missing field: {}", missing_field);
                }
            }
            Err(e) => {
                eprintln!("Failed to upload batch {}: {}", batch_index + 1, e);
                return Err(e);
            }
        }
    }
    
    println!("Upload completed successfully!");
    println!("Total events uploaded: {}", total_uploaded);
    println!("Total batches: {}", total_batches);
    
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

/// End-to-end round-trip: export from one project and upload to another
pub async fn round_trip_e2e(
    start_date: &str,
    end_date: &str,
    output_dir: &std::path::Path,
    export_from: Option<&str>,
    upload_to: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    // Load project selector
    let selector = ProjectSelector::new()?;
    
    println!("Select project to export from");
    // Select export project
    let export_project = selector.select_project(export_from)?;
    
    // Get export project name
    let export_project_name = if let Some(name) = export_from {
        name
    } else {
        // Find the name of the selected project
        let mut found_name = "unknown";
        for (name, config) in &selector.config.projects {
            if std::ptr::eq(config, export_project) {
                found_name = name;
                break;
            }
        }
        found_name
    };

    println!("Exporting from: {}", export_project_name);
    
    // Select upload project (must be different from export project)
    let upload_project_name = if let Some(upload_name) = upload_to {
        if upload_name == export_project_name {
            return Err("Export and upload projects must be different".into());
        }
        let _upload_project = selector.select_project(Some(upload_name))?;
        upload_name
    } else {
        // Interactive selection - ensure it's different from export project
        let projects: Vec<&String> = selector.config.list_projects();
        let available_projects: Vec<&String> = projects
            .iter()
            .filter(|&&name| name != export_project_name)
            .copied()
            .collect();
        
        if available_projects.is_empty() {
            return Err("No other projects available for upload. You need at least 2 projects configured.".into());
        }
        
        println!("Available projects for upload (excluding '{}'):", export_project_name);
        for (i, project_name) in available_projects.iter().enumerate() {
            println!("  {}. {}", i + 1, project_name);
        }
        
        let selection = dialoguer::Select::new()
            .with_prompt("Select project to upload to")
            .items(&available_projects)
            .default(0)
            .interact()?;
        
        available_projects[selection]
    };
    
    // Display the selected options
    println!("Round-trip E2E configuration:");
    println!("  Export from: {}", export_project_name);
    println!("  Upload to: {}", upload_project_name);
    println!("  Date range: {} to {}", start_date, end_date);
    println!("  Output directory: {}", output_dir.display());
    
    // Perform the export from the export_from project
    println!("\nStarting export from project: {}", export_project_name);
    let original_export_dir = output_dir.join("original");
    export_amplitude_data_with_project(start_date, end_date, &original_export_dir, Some(export_project_name)).await?;
    println!("Export completed successfully!");
    
    // Perform the upload to the upload_to project
    println!("\nStarting upload to project: {}", upload_project_name);
    process_and_upload_events_with_project(&original_export_dir, 1000, Some(upload_project_name)).await?;
    println!("Upload completed successfully!");
    
    // Export from the upload_to project to a different directory for comparison
    let comparison_dir = output_dir.join("comparison");
    println!("\nStarting export from upload_to project for comparison: {}", upload_project_name);
    export_amplitude_data_with_project(start_date, end_date, &comparison_dir, Some(upload_project_name)).await?;
    println!("Comparison export completed successfully!");
    
    // Display final summary
    println!("\nRound-trip E2E completed successfully!");
    println!("  Exported from: {}", export_project_name);
    println!("  Uploaded to: {}", upload_project_name);
    println!("  Date range: {} to {}", start_date, end_date);
    println!("  Original export directory: {}", original_export_dir.display());
    println!("  Comparison export directory: {}", comparison_dir.display());
    
    Ok(())
}

/// Compare export events between original and comparison directories
/// Creates a diff report keyed by insert_id and writes it to the filesystem
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
    let original_events = parse_export_events_from_directory(original_dir)?;
    println!("Found {} events in original directory", original_events.len());
    
    println!("Parsing events from comparison directory...");
    let comparison_events = parse_export_events_from_directory(comparison_dir)?;
    println!("Found {} events in comparison directory", comparison_events.len());
    
    // Create maps keyed by insert_id for efficient lookup
    let mut original_map: std::collections::HashMap<String, ExportEvent> = std::collections::HashMap::new();
    let mut comparison_map: std::collections::HashMap<String, ExportEvent> = std::collections::HashMap::new();
    
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
            "total_original_events": original_map.len(),
            "total_comparison_events": comparison_map.len(),
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

/// Check for duplicate insert IDs across events in a directory
pub fn check_for_duplicate_insert_ids(
    input_dir: &std::path::Path,
    output_dir: &std::path::Path,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("Checking for duplicate insert IDs in: {}", input_dir.display());
    
    // Create output directory if it doesn't exist
    fs::create_dir_all(output_dir)?;
    
    // Parse all export events from the input directory
    let events = parse_export_events_from_directory(input_dir)?;
    println!("Found {} total events", events.len());
    
    // Group events by insert_id
    let mut insert_id_groups: std::collections::HashMap<String, Vec<&ExportEvent>> = std::collections::HashMap::new();
    
    for event in &events {
        if let Some(insert_id) = &event.insert_id {
            insert_id_groups.entry(insert_id.clone()).or_default().push(event);
        }
    }
    
    // Find duplicates (insert_ids with more than one event)
    let duplicates: std::collections::HashMap<String, Vec<&ExportEvent>> = insert_id_groups
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
        "duplicate_counts": duplicates.iter().map(|(id, events)| (id, events.len())).collect::<std::collections::HashMap<_, _>>()
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
    let events = parse_export_events_from_directory(input_dir)?;
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
    let mut filter = crate::amplitude_types::UUIDDeduplicationFilter::new();
    filter_events_with_filter(input_dir, output_dir, &mut filter)
}

/// Sanitize filename to be filesystem-safe
fn sanitize_filename(filename: &str) -> String {
    filename
        .chars()
        .map(|c| match c {
            'a'..='z' | 'A'..='Z' | '0'..='9' | '-' | '_' => c,
            _ => '_',
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;
    use std::fs::File;
    use std::io::Write;
    use tempfile::tempdir;

    #[test]
    fn test_parse_export_events_from_directory() {
        let test_dir = tempdir().unwrap();
        let test_file = test_dir.path().join("test.json");
        
        // Create a test JSON file with export events
        let test_data = r#"{"$insert_id":"test-1","event_type":"test_event","event_time":"2025-07-01 16:34:54.837000","user_id":"test-user","device_id":"test-device","event_properties":{},"user_properties":{},"groups":{},"group_properties":{},"uuid":"test-uuid-1"}
{"$insert_id":"test-2","event_type":"test_event_2","event_time":"2025-07-01 16:34:55.837000","user_id":"test-user-2","device_id":"test-device-2","event_properties":{},"user_properties":{},"groups":{},"group_properties":{},"uuid":"test-uuid-2"}"#;
        
        let mut file = File::create(&test_file).unwrap();
        file.write_all(test_data.as_bytes()).unwrap();
        
        // Parse the events
        let events = parse_export_events_from_directory(test_dir.path()).unwrap();
        
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].insert_id, Some("test-1".to_string()));
        assert_eq!(events[0].event_type, Some("test_event".to_string()));
        assert_eq!(events[1].insert_id, Some("test-2".to_string()));
        assert_eq!(events[1].event_type, Some("test_event_2".to_string()));
    }

    #[test]
    fn test_parse_export_events_from_directory_recursive() {
        let test_dir = tempdir().unwrap();
        
        // Create a subdirectory
        let subdir = test_dir.path().join("subdir");
        fs::create_dir(&subdir).unwrap();
        
        // Create JSON files in both root and subdirectory
        let root_file = test_dir.path().join("root.json");
        let subdir_file = subdir.join("subdir.json");
        
        let root_data = r#"{"$insert_id":"root-1","event_type":"root_event","event_time":"2025-07-01 16:34:54.837000","user_id":"root-user","device_id":"root-device","event_properties":{},"user_properties":{},"groups":{},"group_properties":{},"uuid":"root-uuid-1"}"#;
        let subdir_data = r#"{"$insert_id":"subdir-1","event_type":"subdir_event","event_time":"2025-07-01 16:34:55.837000","user_id":"subdir-user","device_id":"subdir-device","event_properties":{},"user_properties":{},"groups":{},"group_properties":{},"uuid":"subdir-uuid-1"}"#;
        
        let mut root_file_handle = File::create(&root_file).unwrap();
        root_file_handle.write_all(root_data.as_bytes()).unwrap();
        
        let mut subdir_file_handle = File::create(&subdir_file).unwrap();
        subdir_file_handle.write_all(subdir_data.as_bytes()).unwrap();
        
        // Parse the events recursively
        let events = parse_export_events_from_directory(test_dir.path()).unwrap();
        
        assert_eq!(events.len(), 2);
        
        // Check that both events are found (order may vary)
        let root_event = events.iter().find(|e| e.insert_id.as_deref() == Some("root-1")).unwrap();
        let subdir_event = events.iter().find(|e| e.insert_id.as_deref() == Some("subdir-1")).unwrap();
        
        assert_eq!(root_event.event_type, Some("root_event".to_string()));
        assert_eq!(subdir_event.event_type, Some("subdir_event".to_string()));
    }

    #[test]
    fn test_export_event_conversion_to_batch_event() {
        let export_event = ExportEvent {
            insert_id: Some("test-insert-id".to_string()),
            event_type: Some("test_event".to_string()),
            event_time: Some(DateTime::parse_from_str("2025-07-01 16:34:54.837000 +0000", "%Y-%m-%d %H:%M:%S%.6f %z").unwrap().with_timezone(&Utc)),
            user_id: Some("test-user".to_string()),
            device_id: Some("test-device".to_string()),
            event_properties: Some(std::collections::HashMap::new()),
            user_properties: Some(std::collections::HashMap::new()),
            groups: Some(std::collections::HashMap::new()),
            group_properties: Some(std::collections::HashMap::new()),
            uuid: Some("test-uuid".to_string()),
            ..Default::default()
        };
        
        let batch_event = export_event.to_batch_event().unwrap();
        
        assert_eq!(batch_event.insert_id, Some("test-insert-id".to_string()));
        assert_eq!(batch_event.event_type, "test_event");
        assert_eq!(batch_event.user_id, Some("test-user".to_string()));
        assert_eq!(batch_event.device_id, Some("test-device".to_string()));
        assert_eq!(batch_event.skip_user_properties_sync, Some(true));
    }

    #[test]
    fn test_end_to_end_multiple_files_and_rows() {
        fn create_gzipped_fixture(dir: &Path, name: &str, contents: &str) -> std::io::Result<()> {
            let path = dir.join(name);
            let file = File::create(path)?;
            let encoder = flate2::write::GzEncoder::new(file, flate2::Compression::default());
            let mut writer = BufWriter::new(encoder);
            writer.write_all(contents.as_bytes())?;
            writer.flush()?;
            Ok(())
        }

        let compressed_dir = tempdir().unwrap();
        let unzipped_dir = tempdir().unwrap();
        let db_path = compressed_dir.path().join("test_multiple.sqlite");

        // Two gzip files, each with 2 JSON objects
        let fixture1 = r#"
{ "user_id": "abc", "uuid": "uuid-0001", "data": {"path": "/test"}, "event_time": "2024-01-01 12:00:00.000000", "event_type": "test_event" }
{ "user_id": null, "uuid": "uuid-0002", "data": {"path": "/"}, "event_time": "2024-01-01 12:01:00.000000", "event_type": "test_event" }
"#;

        let fixture2 = r#"
{ "user_id": "def", "uuid": "uuid-0003", "data": {"path": "/test"}, "event_time": "2024-01-01 12:02:00.000000", "event_type": "test_event" }
{ "user_id": "ghi", "uuid": "uuid-0004", "data": {"path": "/"}, "event_time": "2024-01-01 12:03:00.000000", "event_type": "test_event" }
"#;

        create_gzipped_fixture(compressed_dir.path(), "fixture1.gz", fixture1)
            .expect("Failed fixture1");
        create_gzipped_fixture(compressed_dir.path(), "fixture2.gz", fixture2)
            .expect("Failed fixture2");

        // Unzip all .gz files
        let processed_files = unzip_gz_files(compressed_dir.path(), unzipped_dir.path())
            .expect("Failed to unzip files");

        // Parse all JSON lines from unzipped files
        let parsed_items = parse_json_objects_in_dir(unzipped_dir.path()).expect("Failed to parse");

        // Write parsed data to SQLite
        write_parsed_items_to_sqlite(&db_path, &parsed_items, &processed_files)
            .expect("Failed to write to SQLite");

        // Verify SQLite contents
        let conn = Connection::open(&db_path).unwrap();
        let mut stmt = conn
            .prepare("SELECT uuid, user_id, raw_json, source_file FROM amplitude_events ORDER BY uuid")
            .unwrap();

        let rows = stmt
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                ))
            })
            .unwrap();

        let results: Vec<_> = rows.map(|r| r.unwrap()).collect();

        // Expect 4 rows total
        assert_eq!(results.len(), 4);

        // Check some values for correctness and ordering by uuid
        assert_eq!(results[0].0, "uuid-0001");
        assert_eq!(results[0].1.as_deref(), Some("abc"));
        assert!(results[0].2.contains("\"data\": {\"path\": \"/test\"}"));
        assert!(results[0].3.contains("fixture1"));

        assert_eq!(results[1].0, "uuid-0002");
        assert_eq!(results[1].1, None);
        assert!(results[1].2.contains("\"data\": {\"path\": \"/\"}"));
        assert!(results[1].3.contains("fixture1"));

        assert_eq!(results[2].0, "uuid-0003");
        assert_eq!(results[2].1.as_deref(), Some("def"));
        assert!(results[2].2.contains("\"data\": {\"path\": \"/test\"}"));
        assert!(results[2].3.contains("fixture2"));

        assert_eq!(results[3].0, "uuid-0004");
        assert_eq!(results[3].1.as_deref(), Some("ghi"));
        assert!(results[3].2.contains("\"data\": {\"path\": \"/\"}"));
        assert!(results[3].3.contains("fixture2"));
    }

    #[test]
    fn test_check_for_duplicate_insert_ids() {
        let test_dir = tempdir().unwrap();
        let output_dir = tempdir().unwrap();
        
        // Create test JSON files with duplicate insert IDs
        let test_file1 = test_dir.path().join("file1.json");
        let test_file2 = test_dir.path().join("file2.json");
        
        // File 1: Has a duplicate insert_id "dup-1"
        let test_data1 = r#"{"$insert_id":"dup-1","event_type":"test_event_1","event_time":"2025-07-01 16:34:54.837000","user_id":"user-1","device_id":"device-1","event_properties":{},"user_properties":{},"groups":{},"group_properties":{},"uuid":"uuid-1"}
{"$insert_id":"unique-1","event_type":"test_event_2","event_time":"2025-07-01 16:34:55.837000","user_id":"user-2","device_id":"device-2","event_properties":{},"user_properties":{},"groups":{},"group_properties":{},"uuid":"uuid-2"}"#;
        
        // File 2: Has the same duplicate insert_id "dup-1" and another duplicate "dup-2"
        let test_data2 = r#"{"$insert_id":"dup-1","event_type":"test_event_3","event_time":"2025-07-01 16:34:56.837000","user_id":"user-3","device_id":"device-3","event_properties":{},"user_properties":{},"groups":{},"group_properties":{},"uuid":"uuid-3"}
{"$insert_id":"dup-2","event_type":"test_event_4","event_time":"2025-07-01 16:34:57.837000","user_id":"user-4","device_id":"device-4","event_properties":{},"user_properties":{},"groups":{},"group_properties":{},"uuid":"uuid-4"}
{"$insert_id":"dup-2","event_type":"test_event_5","event_time":"2025-07-01 16:34:58.837000","user_id":"user-5","device_id":"device-5","event_properties":{},"user_properties":{},"groups":{},"group_properties":{},"uuid":"uuid-5"}"#;
        
        let mut file1 = File::create(&test_file1).unwrap();
        file1.write_all(test_data1.as_bytes()).unwrap();
        
        let mut file2 = File::create(&test_file2).unwrap();
        file2.write_all(test_data2.as_bytes()).unwrap();
        
        // Run the duplicate check
        check_for_duplicate_insert_ids(test_dir.path(), output_dir.path()).unwrap();
        
        // Verify the summary file was created
        let summary_path = output_dir.path().join("duplicate_summary.json");
        assert!(summary_path.exists());
        
        // Read and verify summary content
        let summary_content = fs::read_to_string(&summary_path).unwrap();
        let summary: serde_json::Value = serde_json::from_str(&summary_content).unwrap();
        
        assert_eq!(summary["total_events"], 5);
        assert_eq!(summary["duplicate_insert_ids_count"], 2);
        
        let duplicate_ids = summary["duplicate_insert_ids"].as_array().unwrap();
        assert_eq!(duplicate_ids.len(), 2);
        assert!(duplicate_ids.contains(&serde_json::json!("dup-1")));
        assert!(duplicate_ids.contains(&serde_json::json!("dup-2")));
        
        let duplicate_counts = summary["duplicate_counts"].as_object().unwrap();
        assert_eq!(duplicate_counts["dup-1"], 2);
        assert_eq!(duplicate_counts["dup-2"], 2);
        
        // Verify individual duplicate files were created
        let dup1_file = output_dir.path().join("duplicate_dup-1.json");
        let dup2_file = output_dir.path().join("duplicate_dup-2.json");
        
        assert!(dup1_file.exists());
        assert!(dup2_file.exists());
        
        // Verify consolidated file was created
        let consolidated_file = output_dir.path().join("all_duplicates.json");
        assert!(consolidated_file.exists());
        
        // Read and verify consolidated content
        let consolidated_content = fs::read_to_string(&consolidated_file).unwrap();
        let consolidated: serde_json::Value = serde_json::from_str(&consolidated_content).unwrap();
        
        assert_eq!(consolidated["summary"]["total_events"], 5);
        assert_eq!(consolidated["summary"]["duplicate_insert_ids_count"], 2);
        
        let duplicates = consolidated["duplicates"].as_array().unwrap();
        assert_eq!(duplicates.len(), 2);
        
        // Check that dup-1 has 2 events
        let dup1_data = duplicates.iter().find(|d| d["insert_id"] == "dup-1").unwrap();
        assert_eq!(dup1_data["count"], 2);
        assert_eq!(dup1_data["events"].as_array().unwrap().len(), 2);
        
        // Check that dup-2 has 2 events
        let dup2_data = duplicates.iter().find(|d| d["insert_id"] == "dup-2").unwrap();
        assert_eq!(dup2_data["count"], 2);
        assert_eq!(dup2_data["events"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn test_filter_events() {
        let test_dir = tempdir().unwrap();
        let output_dir = tempdir().unwrap();
        
        // Create test JSON files with various events
        let test_file = test_dir.path().join("test.json");
        
        let test_data = r#"{"$insert_id":"id-1","event_type":"page_view","event_time":"2025-07-01 16:34:54.837000","user_id":"user-1","device_id":"device-1","event_properties":{},"user_properties":{},"groups":{},"group_properties":{},"uuid":"uuid-1"}
{"$insert_id":"id-2","event_type":"button_click","event_time":"2025-07-01 16:35:54.837000","user_id":"user-2","device_id":"device-2","event_properties":{},"user_properties":{},"groups":{},"group_properties":{},"uuid":"uuid-2"}
{"$insert_id":"id-3","event_type":"page_view","event_time":"2025-07-01 16:36:54.837000","user_id":"user-1","device_id":"device-3","event_properties":{},"user_properties":{},"groups":{},"group_properties":{},"uuid":"uuid-3"}
{"$insert_id":"id-4","event_type":"form_submit","event_time":"2025-07-01 16:37:54.837000","user_id":"user-3","device_id":"device-1","event_properties":{},"user_properties":{},"groups":{},"group_properties":{},"uuid":"uuid-4"}"#;
        
        let mut file = File::create(&test_file).unwrap();
        file.write_all(test_data.as_bytes()).unwrap();
        
        // Test filtering by event_type
        filter_events(test_dir.path(), output_dir.path(), Some("page_view"), None, None, None, None, None, None, false).unwrap();
        
        // Verify the summary file was created
        let summary_path = output_dir.path().join("filter_summary.json");
        assert!(summary_path.exists());
        
        // Read and verify summary content
        let summary_content = fs::read_to_string(&summary_path).unwrap();
        let summary: serde_json::Value = serde_json::from_str(&summary_content).unwrap();
        
        assert_eq!(summary["total_events"], 4);
        assert_eq!(summary["remaining_events"], 2);
        assert_eq!(summary["removed_events"], 2);
        
        // Verify remaining events file was created
        let remaining_path = output_dir.path().join("remaining_events.json");
        assert!(remaining_path.exists());
        
        let remaining_content = fs::read_to_string(&remaining_path).unwrap();
        let remaining: serde_json::Value = serde_json::from_str(&remaining_content).unwrap();
        
        assert_eq!(remaining["count"], 2);
        let remaining_events = remaining["events"].as_array().unwrap();
        assert_eq!(remaining_events.len(), 2);
        
        // Check that both remaining events are page_view events
        for event in remaining_events {
            assert_eq!(event["event_type"], "page_view");
        }
        
        // Verify removed events file was created
        let removed_path = output_dir.path().join("removed_events.json");
        assert!(removed_path.exists());
        
        let removed_content = fs::read_to_string(&removed_path).unwrap();
        let removed: serde_json::Value = serde_json::from_str(&removed_content).unwrap();
        
        assert_eq!(removed["count"], 2);
        let removed_events = removed["events"].as_array().unwrap();
        assert_eq!(removed_events.len(), 2);
        
        // Check that removed events are not page_view events
        for event in removed_events {
            assert_ne!(event["event_type"], "page_view");
        }
        
        // Verify JSONL files were created
        let remaining_jsonl_path = output_dir.path().join("remaining_events.jsonl");
        let removed_jsonl_path = output_dir.path().join("removed_events.jsonl");
        
        assert!(remaining_jsonl_path.exists());
        assert!(removed_jsonl_path.exists());
        
        // Test with multiple filters
        let output_dir2 = tempdir().unwrap();
        filter_events(test_dir.path(), output_dir2.path(), Some("page_view"), Some("user-1"), None, None, None, None, None, false).unwrap();
        
        let summary_path2 = output_dir2.path().join("filter_summary.json");
        let summary_content2 = fs::read_to_string(&summary_path2).unwrap();
        let summary2: serde_json::Value = serde_json::from_str(&summary_content2).unwrap();
        
        assert_eq!(summary2["remaining_events"], 2); // Both page_view events from user-1
        assert_eq!(summary2["removed_events"], 2); // The other 2 events
    }
} 