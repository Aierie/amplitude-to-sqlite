use crate::common::amplitude_types::ExportEvent;
use crate::common::parser;
use crate::transform::model::{Dupe, DataCorrection};
use itertools::Itertools;
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::Write;

#[derive(Debug, Clone, Copy)]
pub enum OutputMode {
    Analyze,
    Debug,
    Full,
}

pub fn clean_duplicates_and_types(
    input_dir: &std::path::Path,
    output_dir: &std::path::Path,
    _output_mode: OutputMode,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("Cleaning duplicates in: {}", input_dir.display());

    // Create output directory if it doesn't exist
    if output_dir.try_exists().is_ok_and(|v| v) {
        fs::remove_dir_all(output_dir)?;
    }
    fs::create_dir_all(output_dir)?;

    // Parse all export events from the input directory
    let events = parser::parse_export_events_from_directory(input_dir)?;
    let total_events = events.len();
    println!("Found {} total events", total_events);

    // Group events by insert_id
    let mut insert_id_groups: HashMap<String, Vec<ExportEvent>> = HashMap::new();

    for event in events {
        if let Some(insert_id) = &event.insert_id {
            insert_id_groups
                .entry(insert_id.clone())
                .or_default()
                .push(event);
        } else {
            // TODO: better way to deal with this?
            panic!("Missing insert id");
        }
    }

    // Find duplicates (insert_ids with more than one event)
    // let duplicates: HashMap<String, Vec<ExportEvent>> =

    let data: (
        HashMap<String, Vec<ExportEvent>>,
        HashMap<String, Vec<ExportEvent>>,
    ) = insert_id_groups
        .into_iter()
        .partition(|(_, events)| events.len() > 1);
    let duplicates = data.0;
    let non_duplicates = data.1;

    println!("Found {} insert IDs with duplicates", duplicates.len());

    if duplicates.is_empty() {
        println!("No duplicate insert IDs found!");
        return Ok(());
    }

    if matches!(_output_mode, OutputMode::Full) {
        non_duplicates
            .iter()
            .flat_map(|v| v.1)
            .chunks(1000)
            .into_iter()
            .enumerate()
            .for_each(|(idx, chunk)| {
                let mut file =
                    File::create(output_dir.join(format!("non_duplicate_chunk_{idx}.json")))
                        .unwrap();
                chunk.for_each(|item| {
                    file.write(serde_json::to_string(item).unwrap().as_bytes())
                        .unwrap();
                    file.write("\n".as_bytes()).unwrap();
                });
            });
        duplicates
            .iter()
            .map(|d| {
                let dupe = Dupe::from_events(d.1);
                dupe.resolution()
            })
            .flat_map(|r| match r {
                DataCorrection::KeepOne(export_event) => vec![export_event],
                DataCorrection::KeepMany(export_events) => export_events,
                DataCorrection::Error(dupe) => panic!("Error. Please analyze"),
            })
            .chunks(1000)
            .into_iter()
            .enumerate()
            .for_each(|(idx, chunk)| {
                let mut file =
                    File::create(output_dir.join(format!("duplicate_chunk_{idx}.json")))
                        .unwrap();
                chunk.for_each(|item| {
                    file.write(serde_json::to_string(&item).unwrap().as_bytes())
                        .unwrap();
                    file.write("\n".as_bytes()).unwrap();
                });
            });
    } else {
        let mut created_dirs = HashMap::<String, bool>::new();
        let mut handle_resolution = |insert_id: &String,
                                     dupe_type: Dupe,
                                     resolution: DataCorrection|
         -> Result<(), Box<dyn std::error::Error>> {
            let dupe_type_str = dupe_type.to_str();
            let type_dir = output_dir.join(&dupe_type_str);
            if !created_dirs.contains_key(&dupe_type_str) {
                fs::create_dir_all(&type_dir)?;
                created_dirs.insert(dupe_type_str.clone(), true);
            }
            let filename = sanitize_filename(insert_id);
            let file_path = type_dir.join(format!("dupe_analysis_{}.json", filename));
            let file = File::create(&file_path)?;
            let json_dupe = serde_json::json!({
                "insert_id": &insert_id,
                "dupe": dupe_type,
                "resolution": resolution,
            });
            serde_json::to_writer_pretty(file, &json_dupe)?;

            Ok(())
        };
        for (insert_id, duplicate_events) in &duplicates {
            let dupe_type = Dupe::from_events(duplicate_events);
            let resolution = dupe_type.clone().resolution();
            handle_resolution(insert_id, dupe_type, resolution)?;
        }
    }

    Ok(())
}


/// Sanitize filename by replacing invalid characters
fn sanitize_filename(filename: &str) -> String {
    filename
        .chars()
        .map(|c| {
            if c.is_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_output_mode_enum() {
        // Test that all output modes can be created and compared
        assert!(matches!(OutputMode::Analyze, OutputMode::Analyze));
        assert!(matches!(OutputMode::Debug, OutputMode::Debug));
        assert!(matches!(OutputMode::Full, OutputMode::Full));
    }

    #[test]
    fn test_sanitize_filename() {
        assert_eq!(sanitize_filename("normal-name"), "normal-name");
        assert_eq!(sanitize_filename("name with spaces"), "name_with_spaces");
        assert_eq!(
            sanitize_filename("name@with#special$chars"),
            "name_with_special_chars"
        );
        assert_eq!(
            sanitize_filename("name/with\\path:chars"),
            "name_with_path_chars"
        );
    }
}
