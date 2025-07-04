use crate::amplitude_sdk::AmplitudeClient;
use crate::amplitude_types::ExportEvent;
use crate::config::AmplitudeConfig;
use chrono::{DateTime, Utc};
use flate2::read::GzDecoder;
use rusqlite::{params, Connection, Result};
use serde_json::Value;
use std::fs::{self, File};
use std::io::{self, BufRead, BufReader, BufWriter};
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

/// Export events from Amplitude for a given date range
pub async fn export_amplitude_data(
    start_date: &str,
    end_date: &str,
    output_dir: &std::path::Path,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("Exporting Amplitude data from {} to {}", start_date, end_date);
    
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
    
    // Load configuration and create client
    let config = AmplitudeConfig::load()?;
    let client = AmplitudeClient::from_config(config);
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
                    .to_string()
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

/// Process JSON files containing ExportEvents, convert them to Events, order by time, and upload via batch API
pub async fn process_and_upload_events(
    input_dir: &std::path::Path,
    batch_size: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("Processing JSON files from {:?} with batch size {}", input_dir, batch_size);
    
    // Load configuration and create client
    let config = AmplitudeConfig::load()?;
    let client = AmplitudeClient::from_config(config);
    
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

/// Parse all ExportEvents from JSON files in a directory
fn parse_export_events_from_directory(dir: &Path) -> io::Result<Vec<ExportEvent>> {
    let mut events = Vec::new();

    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();

        if path.is_file() {
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

    Ok(events)
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
} 