use crate::common::amplitude_types::ExportEvent;
use crate::common::parser;
use crate::transform::model::{DupeResolution, DupeType};
use serde_json::Value;
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::Write;

#[derive(Debug, Clone, Copy)]
pub enum OutputMode {
    Analyze,
    Debug,
    Full,
}

/// Clean duplicates based on insert_id, determine DupeTypes, and write results to JSON files
pub fn clean_duplicates_and_types(
    input_dir: &std::path::Path,
    output_dir: &std::path::Path,
    output_mode: OutputMode,
) -> Result<(), Box<dyn std::error::Error>> {
    println!(
        "Cleaning duplicates and determining DupeTypes in: {}",
        input_dir.display()
    );

    // Create output directory if it doesn't exist
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
        }
    }

    // Calculate counts before moving data
    let unique_insert_ids_count = insert_id_groups.len();

    // Find duplicates (insert_ids with more than one event)
    let duplicates: HashMap<String, Vec<ExportEvent>> = insert_id_groups
        .into_iter()
        .filter(|(_, events)| events.len() > 1)
        .collect();

    println!("Found {} insert IDs with duplicates", duplicates.len());

    if duplicates.is_empty() {
        println!("No duplicate insert IDs found!");
        return Ok(());
    }

    let mut all_diff_fields = std::collections::BTreeSet::new();
    let mut all_event_properties_diff_fields = std::collections::BTreeSet::new();
    // Analyze each duplicate group and determine DupeType
    let mut dupe_analysis: HashMap<String, DupeAnalysis> = HashMap::new();
    let mut dupe_type_counts: HashMap<String, usize> = HashMap::new();
    let mut dupe_type_groups: HashMap<String, Vec<String>> = HashMap::new();

    for (insert_id, duplicate_events) in &duplicates {
        let dupe_type = DupeType::from_events(duplicate_events);
        let resolution = dupe_type.clone().resolution();
        let dupe_type_str = dupe_type.to_str();
        
        // Group insert_ids by dupe type
        dupe_type_groups
            .entry(dupe_type_str.clone())
            .or_default()
            .push(insert_id.clone());
        
        // Collect diff fields for UnknownPropDiff
        if let DupeType::UnknownPropDiff(events) = &dupe_type {
            if events.len() >= 2 {
                let differences = find_event_differences(&events[0], &events[1]);
                for field in differences.keys() {
                    all_diff_fields.insert(field.clone());
                }
                
                // Collect event_properties differences specifically
                let event_props_differences = find_event_properties_differences(&events[0], &events[1]);
                for field in event_props_differences.keys() {
                    all_event_properties_diff_fields.insert(field.clone());
                }
            }
        }
        let analysis = DupeAnalysis {
            insert_id: insert_id.clone(),
            duplicate_count: duplicate_events.len(),
            dupe_type: dupe_type.clone(),
            resolution: resolution.clone(),
            events: duplicate_events.clone(),
        };
        dupe_analysis.insert(insert_id.clone(), analysis);
        // Count dupe types
        *dupe_type_counts.entry(dupe_type_str).or_default() += 1;
    }

    // Create subdirectories for each DupeType
    for dupe_type_str in dupe_type_groups.keys() {
        let type_dir = output_dir.join(dupe_type_str);
        fs::create_dir_all(&type_dir)?;
        println!("Created directory for {}: {}", dupe_type_str, type_dir.display());
    }

    // Create summary file in the main output directory
    let summary_path = output_dir.join("dupe_analysis_summary.json");
    let event_props_diff_fields_vec: Vec<String> = all_event_properties_diff_fields.iter().cloned().collect();
    let summary = serde_json::json!({
        "total_events": total_events,
        "unique_insert_ids": unique_insert_ids_count,
        "duplicate_insert_ids_count": duplicates.len(),
        "dupe_type_counts": dupe_type_counts,
        "duplicate_insert_ids": duplicates.keys().collect::<Vec<_>>(),
        "all_diff_fields": all_diff_fields.into_iter().filter(|f| {
            !["city",
    "country",
    "device_carrier",
    "device_family",
    "device_type",
    "event_id",
    "ip_address",
    "os_name",
    "os_version",
    "platform",
    "client_upload_time",
    "processed_time",
    "server_received_time",
    "server_upload_time",
    "user_properties",
    "uuid",
    "language",
    "region",
    "dma",
    "data",].contains(&f.as_ref())
        }).collect::<Vec<_>>(),
        "all_event_properties_diff_fields": event_props_diff_fields_vec
    });

    let summary_file = File::create(&summary_path)?;
    serde_json::to_writer_pretty(summary_file, &summary)?;
    println!("Summary written to: {}", summary_path.display());

    // Create individual files for each duplicate insert_id with DupeType analysis
    for (insert_id, analysis) in &dupe_analysis {
        let dupe_type_str = analysis.dupe_type.to_str();
        let type_dir = output_dir.join(&dupe_type_str);
        let filename = sanitize_filename(insert_id);
        let file_path = type_dir.join(format!("dupe_analysis_{}.json", filename));

        // Prepare additional analysis data for UnknownPropDiff and Multi cases
        let mut additional_data = serde_json::Map::new();

        match &analysis.dupe_type {
            DupeType::UnknownPropDiff(events) => {
                if events.len() >= 2 {
                    let differences = find_event_differences(&events[0], &events[1]);
                    if !differences.is_empty() {
                        additional_data.insert(
                            "field_differences".to_string(),
                            serde_json::to_value(differences).unwrap(),
                        );
                    }
                    
                    let event_props_differences = find_event_properties_differences(&events[0], &events[1]);
                    if !event_props_differences.is_empty() {
                        additional_data.insert(
                            "event_properties_differences".to_string(),
                            serde_json::to_value(event_props_differences).unwrap(),
                        );
                    }
                }
            },
            DupeType::Multi(events, dupe_types) => {
                let dupe_type_strings: Vec<String> = dupe_types.iter().map(|dt| dt.to_str()).collect();
                additional_data.insert(
                    "dupe_types".to_string(),
                    serde_json::to_value(dupe_type_strings).unwrap(),
                );

                if events.len() >= 2 {
                    let differences = find_event_differences(&events[0], &events[1]);
                    if !differences.is_empty() {
                        additional_data.insert(
                            "field_differences".to_string(),
                            serde_json::to_value(differences).unwrap(),
                        );
                    }
                    
                    let event_props_differences = find_event_properties_differences(&events[0], &events[1]);
                    if !event_props_differences.is_empty() {
                        additional_data.insert(
                            "event_properties_differences".to_string(),
                            serde_json::to_value(event_props_differences).unwrap(),
                        );
                    }
                }
            },
            _ => {}
        }

        let mut analysis_data = serde_json::json!({
            "insert_id": analysis.insert_id,
            "duplicate_count": analysis.duplicate_count,
            "dupe_type": analysis.dupe_type.to_str(),
            "resolution": match &analysis.resolution {
                DupeResolution::KeepOne(event) => {
                    serde_json::json!({
                        "type": "KeepOne",
                        "kept_event": serde_json::to_value(event).unwrap()
                    })
                },
                DupeResolution::KeepNone(event) => {
                    serde_json::json!({
                        "type": "KeepNone",
                        "discarded_event": serde_json::to_value(event).unwrap()
                    })
                },
                DupeResolution::KeepMany(events) => {
                    serde_json::json!({
                        "type": "KeepMany",
                        "kept_events": events.iter().map(|e| serde_json::to_value(e).unwrap()).collect::<Vec<_>>()
                    })
                },
                DupeResolution::Error(dupe_type) => {
                    serde_json::json!({
                        "type": "Error",
                        "error_type": dupe_type.to_str(),
                    })
                },
            },
            "events": analysis.events.iter().map(|event| {
                serde_json::to_value(event).unwrap()
            }).collect::<Vec<_>>()
        });

        // Add additional data if present
        if let Value::Object(ref mut obj) = analysis_data {
            for (key, value) in additional_data {
                obj.insert(key, value);
            }
        }

        let file = File::create(&file_path)?;
        serde_json::to_writer_pretty(file, &analysis_data)?;
        println!(
            "Dupe analysis for insert_id '{}' written to: {}",
            insert_id,
            file_path.display()
        );
    }

    // // Create a consolidated file for each DupeType
    // for (dupe_type_str, insert_ids) in &dupe_type_groups {
    //     let type_dir = output_dir.join(dupe_type_str);
    //     let consolidated_path = type_dir.join("consolidated_analyses.json");
        
    //     let mut type_analyses = Vec::new();
        
    //     for insert_id in insert_ids {
    //         if let Some(analysis) = dupe_analysis.get(insert_id) {
    //             // Prepare additional analysis data for UnknownPropDiff cases
    //             let mut additional_data = serde_json::Map::new();

    //             if let DupeType::UnknownPropDiff(events) = &analysis.dupe_type {
    //                 if events.len() >= 2 {
    //                     let differences = find_event_differences(&events[0], &events[1]);
    //                     if !differences.is_empty() {
    //                         additional_data.insert("field_differences".to_string(), serde_json::to_value(differences).unwrap());
    //                     }
                        
    //                     let event_props_differences = find_event_properties_differences(&events[0], &events[1]);
    //                     if !event_props_differences.is_empty() {
    //                         additional_data.insert("event_properties_differences".to_string(), serde_json::to_value(event_props_differences).unwrap());
    //                     }
    //                 }
    //             }

    //             let mut analysis_json = serde_json::json!({
    //                 "insert_id": insert_id,
    //                 "duplicate_count": analysis.duplicate_count,
    //                 "dupe_type": analysis.dupe_type.to_str(),
    //                 "resolution": match &analysis.resolution {
    //                     DupeResolution::KeepOne(event) => {
    //                         serde_json::json!({
    //                             "type": "KeepOne",
    //                             "kept_event": serde_json::to_value(event).unwrap()
    //                         })
    //                     },
    //                     DupeResolution::KeepNone(event) => {
    //                         serde_json::json!({
    //                             "type": "KeepNone",
    //                             "discarded_event": serde_json::to_value(event).unwrap()
    //                         })
    //                     },
    //                     DupeResolution::KeepMany(events) => {
    //                         serde_json::json!({
    //                             "type": "KeepMany",
    //                             "kept_events": events.iter().map(|e| serde_json::to_value(e).unwrap()).collect::<Vec<_>>()
    //                         })
    //                     },
    //                     DupeResolution::Error(dupe_type) => {
    //                         serde_json::json!({
    //                             "type": "Error",
    //                             "error_type": match dupe_type {
    //                                 DupeType::Unknown(_) => "Unknown",
    //                                 DupeType::UnknownPropDiff(_) => "UnknownPropDiff",
    //                                 DupeType::TooMany(_) => "TooMany",
    //                                 DupeType::Multi(_) => "Multi",
    //                                 DupeType::EventPropsIncompatible(_) => "EventPropsIncompatible",
    //                                 _ => "Unexpected",
    //                             }
    //                         })
    //                     },
    //                 },
    //                 "events": analysis.events.iter().map(|event| {
    //                     serde_json::to_value(event).unwrap()
    //                 }).collect::<Vec<_>>()
    //             });

    //             // Add additional data if present
    //             if let Value::Object(ref mut obj) = analysis_json {
    //                 for (key, value) in additional_data {
    //                     obj.insert(key, value);
    //                 }
    //             }

    //             type_analyses.push(analysis_json);
    //         }
    //     }

    //     let consolidated_data = serde_json::json!({
    //         "dupe_type": dupe_type_str,
    //         "count": insert_ids.len(),
    //         "analyses": type_analyses
    //     });

    //     let consolidated_file = File::create(&consolidated_path)?;
    //     serde_json::to_writer_pretty(consolidated_file, &consolidated_data)?;
    //     println!(
    //         "Consolidated analyses for {} written to: {}",
    //         dupe_type_str,
    //         consolidated_path.display()
    //     );
    // }

    // Create a consolidated file with all duplicate analyses in the main output directory
    let consolidated_path = output_dir.join("all_dupe_analyses.json");
    let consolidated_data = serde_json::json!({
        "summary": {
            "total_events": total_events,
            "unique_insert_ids": unique_insert_ids_count,
            "duplicate_insert_ids_count": duplicates.len(),
            "dupe_type_counts": dupe_type_counts,
            "all_event_properties_diff_fields": event_props_diff_fields_vec
        },
        "dupe_analyses": dupe_analysis.iter().map(|(insert_id, analysis)| {
            // Prepare additional analysis data for UnknownPropDiff and Multi cases
            let mut additional_data = serde_json::Map::new();

            match &analysis.dupe_type {
                DupeType::UnknownPropDiff(events) => {
                    if events.len() >= 2 {
                        let differences = find_event_differences(&events[0], &events[1]);
                        if !differences.is_empty() {
                            additional_data.insert("field_differences".to_string(), serde_json::to_value(differences).unwrap());
                        }
                        
                        let event_props_differences = find_event_properties_differences(&events[0], &events[1]);
                        if !event_props_differences.is_empty() {
                            additional_data.insert("event_properties_differences".to_string(), serde_json::to_value(event_props_differences).unwrap());
                        }
                    }
                },
                DupeType::Multi(events, dupe_types) => {
                    let dupe_type_strings: Vec<String> = dupe_types.iter().map(|dt| dt.to_str()).collect();
                    additional_data.insert(
                        "dupe_types".to_string(),
                        serde_json::to_value(dupe_type_strings).unwrap(),
                    );
                    if events.len() >= 2 {
                        let differences = find_event_differences(&events[0], &events[1]);
                        if !differences.is_empty() {
                            additional_data.insert("field_differences".to_string(), serde_json::to_value(differences).unwrap());
                        }
                        
                        let event_props_differences = find_event_properties_differences(&events[0], &events[1]);
                        if !event_props_differences.is_empty() {
                            additional_data.insert("event_properties_differences".to_string(), serde_json::to_value(event_props_differences).unwrap());
                        }
                    }
                },
                _ => {}
            }

            let mut analysis_json = serde_json::json!({
                "insert_id": insert_id,
                "duplicate_count": analysis.duplicate_count,
                "dupe_type": analysis.dupe_type.to_str(),
                "resolution": match &analysis.resolution {
                    DupeResolution::KeepOne(event) => {
                        serde_json::json!({
                            "type": "KeepOne",
                            "kept_event": serde_json::to_value(event).unwrap()
                        })
                    },
                    DupeResolution::KeepNone(event) => {
                        serde_json::json!({
                            "type": "KeepNone",
                            "discarded_event": serde_json::to_value(event).unwrap()
                        })
                    },
                    DupeResolution::KeepMany(events) => {
                        serde_json::json!({
                            "type": "KeepMany",
                            "kept_events": events.iter().map(|e| serde_json::to_value(e).unwrap()).collect::<Vec<_>>()
                        })
                    },
                    DupeResolution::Error(dupe_type) => {
                        serde_json::json!({
                            "type": "Error",
                            "error_type": dupe_type.to_str()
                        })
                    },
                },
                "events": analysis.events.iter().map(|event| {
                    serde_json::to_value(event).unwrap()
                }).collect::<Vec<_>>()
            });

            // Add additional data if present
            if let Value::Object(ref mut obj) = analysis_json {
                for (key, value) in additional_data {
                    obj.insert(key, value);
                }
            }

            analysis_json
        }).collect::<Vec<_>>()
    });

    let consolidated_file = File::create(&consolidated_path)?;
    serde_json::to_writer_pretty(consolidated_file, &consolidated_data)?;
    println!(
        "All dupe analyses consolidated in: {}",
        consolidated_path.display()
    );

    // Handle different output modes
    match output_mode {
        OutputMode::Analyze => {
            // Current logic - just print summary
            println!("\n=== Dupe Analysis Summary ===");
            println!("Total events: {}", total_events);
            println!("Duplicate insert IDs: {}", duplicates.len());
            println!("\nDupe type breakdown:");
            for (dupe_type, count) in &dupe_type_counts {
                println!("  {}: {}", dupe_type, count);
            }

            println!("\nOutput organized by DupeType in subdirectories:");
            for dupe_type_str in dupe_type_groups.keys() {
                let type_dir = output_dir.join(dupe_type_str);
                println!("  {}: {}", dupe_type_str, type_dir.display());
            }
        },
        OutputMode::Debug => {
            // Debug mode: include resolution processing
            println!("\n=== Dupe Analysis Summary (Debug Mode) ===");
            println!("Total events: {}", total_events);
            println!("Duplicate insert IDs: {}", duplicates.len());
            println!("\nDupe type breakdown:");
            for (dupe_type, count) in &dupe_type_counts {
                println!("  {}: {}", dupe_type, count);
            }

            // Process resolutions and create resolved events
            let mut resolved_events = Vec::new();
            let mut resolution_summary = HashMap::new();
            
            for (insert_id, analysis) in &dupe_analysis {
                match &analysis.resolution {
                    DupeResolution::KeepOne(event) => {
                        resolved_events.push(event.clone());
                        *resolution_summary.entry("KeepOne").or_insert(0) += 1;
                    },
                    DupeResolution::KeepMany(events) => {
                        resolved_events.extend(events.clone());
                        *resolution_summary.entry("KeepMany").or_insert(0) += events.len();
                    },
                    DupeResolution::KeepNone(_) => {
                        *resolution_summary.entry("KeepNone").or_insert(0) += 1;
                    },
                    DupeResolution::Error(_) => {
                        *resolution_summary.entry("Error").or_insert(0) += 1;
                    },
                }
            }

            println!("\nResolution summary:");
            for (resolution_type, count) in &resolution_summary {
                println!("  {}: {}", resolution_type, count);
            }

            // Write resolved events to a debug file
            let debug_path = output_dir.join("debug_resolved_events.json");
            let mut debug_file = File::create(&debug_path)?;
            for event in &resolved_events {
                serde_json::to_writer(&mut debug_file, &event)?;
                writeln!(debug_file)?;
            }
            println!("Debug resolved events written to: {}", debug_path.display());

            println!("\nOutput organized by DupeType in subdirectories:");
            for dupe_type_str in dupe_type_groups.keys() {
                let type_dir = output_dir.join(dupe_type_str);
                println!("  {}: {}", dupe_type_str, type_dir.display());
            }
        },
        OutputMode::Full => {
            // Full mode: output ALL data including non-duplicates
            println!("\n=== Full Export Mode ===");
            println!("Total events: {}", total_events);
            println!("Duplicate insert IDs: {}", duplicates.len());
            println!("\nDupe type breakdown:");
            for (dupe_type, count) in &dupe_type_counts {
                println!("  {}: {}", dupe_type, count);
            }

            // Process resolutions for duplicates
            let mut resolved_events = Vec::new();
            let mut resolution_summary = HashMap::new();
            
            for (insert_id, analysis) in &dupe_analysis {
                match &analysis.resolution {
                    DupeResolution::KeepOne(event) => {
                        resolved_events.push(event.clone());
                        *resolution_summary.entry("KeepOne").or_insert(0) += 1;
                    },
                    DupeResolution::KeepMany(events) => {
                        resolved_events.extend(events.clone());
                        *resolution_summary.entry("KeepMany").or_insert(0) += events.len();
                    },
                    DupeResolution::KeepNone(_) => {
                        *resolution_summary.entry("KeepNone").or_insert(0) += 1;
                    },
                    DupeResolution::Error(_) => {
                        *resolution_summary.entry("Error").or_insert(0) += 1;
                    },
                }
            }

            println!("\nResolution summary:");
            for (resolution_type, count) in &resolution_summary {
                println!("  {}: {}", resolution_type, count);
            }

            // Create a map of resolved events by insert_id for easy lookup
            let mut resolved_map: HashMap<String, ExportEvent> = HashMap::new();
            for event in &resolved_events {
                if let Some(insert_id) = &event.insert_id {
                    resolved_map.insert(insert_id.clone(), event.clone());
                }
            }

            // Re-parse all events to get the complete dataset
            let all_events = parser::parse_export_events_from_directory(input_dir)?;
            
            // Create final events list: resolved duplicates + non-duplicates
            let mut final_events = Vec::new();
            let mut non_duplicate_count = 0;
            
            for event in all_events {
                if let Some(insert_id) = &event.insert_id {
                    if let Some(resolved_event) = resolved_map.get(insert_id) {
                        // This is a duplicate that was resolved
                        final_events.push(resolved_event.clone());
                    } else {
                        // This is a non-duplicate event
                        final_events.push(event);
                        non_duplicate_count += 1;
                    }
                } else {
                    // Event without insert_id, keep as-is
                    final_events.push(event);
                    non_duplicate_count += 1;
                }
            }

            println!("Non-duplicate events: {}", non_duplicate_count);
            println!("Final events after resolution: {}", final_events.len());

            // Write all events in Amplitude export format
            let full_export_path = output_dir.join("full_export_events.json");
            let mut full_export_file = File::create(&full_export_path)?;
            for event in &final_events {
                serde_json::to_writer(&mut full_export_file, &event)?;
                writeln!(full_export_file)?;
            }
            println!("Full export written to: {}", full_export_path.display());

            // Also write a summary of what was done
            let full_summary_path = output_dir.join("full_export_summary.json");
            let full_summary = serde_json::json!({
                "total_original_events": total_events,
                "duplicate_insert_ids_count": duplicates.len(),
                "non_duplicate_events": non_duplicate_count,
                "final_events_count": final_events.len(),
                "dupe_type_counts": dupe_type_counts,
                "resolution_summary": resolution_summary,
                "duplicate_insert_ids": duplicates.keys().collect::<Vec<_>>()
            });
            let full_summary_file = File::create(&full_summary_path)?;
            serde_json::to_writer_pretty(full_summary_file, &full_summary)?;
            println!("Full export summary written to: {}", full_summary_path.display());

            println!("\nOutput organized by DupeType in subdirectories:");
            for dupe_type_str in dupe_type_groups.keys() {
                let type_dir = output_dir.join(dupe_type_str);
                println!("  {}: {}", dupe_type_str, type_dir.display());
            }
        }
    }

    println!("\nDupe analysis completed successfully!");
    Ok(())
}

/// Structure to hold analysis results for a duplicate group
#[derive(Debug)]
struct DupeAnalysis {
    insert_id: String,
    duplicate_count: usize,
    dupe_type: DupeType,
    resolution: DupeResolution,
    events: Vec<ExportEvent>,
}

/// Find differences between two serialized events
fn find_event_differences(
    event1: &ExportEvent,
    event2: &ExportEvent,
) -> HashMap<String, serde_json::Value> {
    let mut differences = HashMap::new();

    // Serialize both events to JSON
    let json1 = serde_json::to_value(event1).unwrap();
    let json2 = serde_json::to_value(event2).unwrap();

    if let (Value::Object(obj1), Value::Object(obj2)) = (json1, json2) {
        // Get all unique keys from both objects
        let all_keys: std::collections::HashSet<_> = obj1.keys().chain(obj2.keys()).collect();

        for key in all_keys {
            let val1 = obj1.get(key);
            let val2 = obj2.get(key);

            if val1 != val2 {
                differences.insert(
                    key.clone(),
                    serde_json::json!({
                        "event1_value": val1,
                        "event2_value": val2
                    }),
                );
            }
        }
    }

    differences
}

/// Find differences specifically in event_properties
fn find_event_properties_differences(
    event1: &ExportEvent,
    event2: &ExportEvent,
) -> HashMap<String, serde_json::Value> {
    let mut differences = HashMap::new();

    // Get event_properties from both events
    let props1 = event1.event_properties.as_ref();
    let props2 = event2.event_properties.as_ref();

    // If both are None, no differences
    if props1.is_none() && props2.is_none() {
        return differences;
    }

    // If one is None and the other isn't, all keys in the non-None one are differences
    if props1.is_none() || props2.is_none() {
        let non_none_props = props1.or(props2).unwrap();
        for key in non_none_props.keys() {
            differences.insert(
                key.clone(),
                serde_json::json!({
                    "event1_value": props1.and_then(|p| p.get(key)),
                    "event2_value": props2.and_then(|p| p.get(key))
                }),
            );
        }
        return differences;
    }

    // Both are Some, compare their contents
    let props1 = props1.unwrap();
    let props2 = props2.unwrap();

    // Get all unique keys from both event_properties
    let all_keys: std::collections::HashSet<_> = props1.keys().chain(props2.keys()).collect();

    for key in all_keys {
        let val1 = props1.get(key);
        let val2 = props2.get(key);

        if val1 != val2 {
            differences.insert(
                key.clone(),
                serde_json::json!({
                    "event1_value": val1,
                    "event2_value": val2
                }),
            );
        }
    }

    differences
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
        assert_eq!(sanitize_filename("name@with#special$chars"), "name_with_special_chars");
        assert_eq!(sanitize_filename("name/with\\path:chars"), "name_with_path_chars");
    }
}
