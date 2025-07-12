use crate::common::amplitude_types::ExportEvent;

use std::fs::{self, File};
use std::io::{self, BufRead, BufReader};
use std::path::Path;

/// Parse export events from a directory recursively
pub fn parse_export_events_from_directory(dir: &Path) -> io::Result<Vec<ExportEvent>> {
    let mut events = Vec::new();
    parse_export_events_recursive(dir, &mut events)?;
    Ok(events)
}

/// Recursively parse export events from a directory
fn parse_export_events_recursive(dir: &Path, events: &mut Vec<ExportEvent>) -> io::Result<()> {
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();

        if path.is_dir() {
            parse_export_events_recursive(&path, events)?;
        } else if path.is_file() {
            // Check if it's a JSON file
            if let Some(extension) = path.extension() {
                if extension == "json" {
                    let file = File::open(&path)?;
                    let reader = BufReader::new(file);

                    for line_result in reader.lines() {
                        let line = line_result?;
                        let trimmed = line.trim();
                        if trimmed.is_empty() {
                            continue;
                        }

                        match serde_json::from_str::<ExportEvent>(trimmed) {
                            Ok(event) => events.push(event),
                            Err(e) => {
                                return Err(io::Error::new(
                                    io::ErrorKind::InvalidData,
                                    format!("Failed to parse event in {}: {}", path.display(), e)
                                ));
                            }
                        }
                    }
                }
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
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
        
        // Parse the events
        let events = parse_export_events_from_directory(test_dir.path()).unwrap();
        
        assert_eq!(events.len(), 2);
        
        // Check that we have events from both locations
        let root_events: Vec<_> = events.iter().filter(|e| e.insert_id == Some("root-1".to_string())).collect();
        let subdir_events: Vec<_> = events.iter().filter(|e| e.insert_id == Some("subdir-1".to_string())).collect();
        
        assert_eq!(root_events.len(), 1);
        assert_eq!(subdir_events.len(), 1);
        assert_eq!(root_events[0].event_type, Some("root_event".to_string()));
        assert_eq!(subdir_events[0].event_type, Some("subdir_event".to_string()));
    }

    #[test]
    fn test_export_event_conversion_to_batch_event() {
        use std::collections::HashMap;
        
        let mut export_event = ExportEvent::default();
        export_event.insert_id = Some("test-insert-id".to_string());
        export_event.event_type = Some("test_event".to_string());
        export_event.user_id = Some("test-user".to_string());
        export_event.device_id = Some("test-device".to_string());
        export_event.event_time = Some(chrono::Utc::now());
        export_event.event_properties = Some(HashMap::new());
        export_event.user_properties = Some(HashMap::new());
        export_event.groups = Some(HashMap::new());
        export_event.group_properties = Some(HashMap::new());
        export_event.uuid = Some("test-uuid".to_string());
        export_event.session_id = Some(12345);
        export_event.app = Some(67890);
        export_event.amplitude_id = Some(11111);
        export_event.event_id = Some(22222);
        export_event.client_event_time = Some("2025-07-01 16:34:54.837000".to_string());
        export_event.client_upload_time = Some("2025-07-01 16:34:55.837000".to_string());
        export_event.server_received_time = Some("2025-07-01 16:34:56.837000".to_string());
        export_event.server_upload_time = Some("2025-07-01 16:34:57.837000".to_string());
        export_event.processed_time = Some("2025-07-01 16:34:58.837000".to_string());
        
        let batch_event = export_event.to_batch_event().unwrap();
        
        assert_eq!(batch_event.insert_id, Some("test-insert-id".to_string()));
        assert_eq!(batch_event.event_type, "test_event");
        assert_eq!(batch_event.user_id, Some("test-user".to_string()));
        assert_eq!(batch_event.device_id, Some("test-device".to_string()));
    }

    #[test]
    fn test_parse_export_events_with_invalid_json() {
        let test_dir = tempdir().unwrap();
        let test_file = test_dir.path().join("invalid.json");
        
        // Create a test JSON file with invalid JSON
        let invalid_data = r#"{"$insert_id":"test-1","event_type":"test_event","event_time":"2025-07-01 16:34:54.837000","user_id":"test-user","device_id":"test-device","event_properties":{},"user_properties":{},"groups":{},"group_properties":{},"uuid":"test-uuid-1"}
{"invalid": json, "missing": quotes}"#;
        
        let mut file = File::create(&test_file).unwrap();
        file.write_all(invalid_data.as_bytes()).unwrap();
        
        // Parse the events should fail
        let result = parse_export_events_from_directory(test_dir.path());
        assert!(result.is_err());
        
        let error = result.unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("Failed to parse event"));
        assert!(error.to_string().contains("invalid.json"));
    }
} 