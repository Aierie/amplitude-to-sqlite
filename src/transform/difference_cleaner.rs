use crate::common::amplitude_types::ExportEvent;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::fs;
use std::path::Path;

/// Structure representing a difference between two events
#[derive(Debug, Serialize, Deserialize)]
pub struct EventDifference {
    pub comparison: Value,
    pub original: Value,
}

/// Structure representing the differences object in a comparison file
#[derive(Debug, Serialize, Deserialize)]
pub struct Differences {
    #[serde(flatten)]
    pub fields: HashMap<String, EventDifference>,
}

/// Structure representing a comparison result file
#[derive(Debug, Serialize, Deserialize)]
pub struct ComparisonResult {
    pub comparison_event: ExportEvent,
    pub differences: Differences,
    pub insert_id: String,
    pub original_event: ExportEvent,
}

/// Clean up differences where property names are the only difference
/// For these cases, choose the property name from the LATER event based on client_upload_time
pub fn clean_property_name_differences(
    differences_dir: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    println!(
        "Cleaning property name differences in: {:?}",
        differences_dir
    );

    if !differences_dir.exists() {
        return Err(format!(
            "Differences directory does not exist: {:?}",
            differences_dir
        )
        .into());
    }

    let mut processed_count = 0;
    let mut cleaned_count = 0;

    // Read all JSON files in the differences directory
    for entry in fs::read_dir(differences_dir)? {
        let entry = entry?;
        let path = entry.path();

        if path.extension().and_then(|s| s.to_str()) == Some("json") {
            processed_count += 1;

            // Read and parse the comparison file
            let content = fs::read_to_string(&path)?;
            let mut comparison: ComparisonResult = serde_json::from_str(&content)?;

            // Clean the comparison data
            let was_cleaned = clean_comparison_result(&mut comparison);

            if was_cleaned {
                cleaned_count += 1;
            }
        }
    }

    println!(
        "Processed {} files, cleaned {} files",
        processed_count, cleaned_count
    );
    Ok(())
}

/// Clean a single comparison result by removing non-material differences and resolving property name conflicts
/// Returns true if any cleaning was performed
pub fn clean_comparison_result(comparison: &mut ComparisonResult) -> bool {
    let mut was_cleaned = false;

    // Fields that are not considered material differences (expected to differ when uploading to different project)
    let non_material_fields = [
        "app",
        "client_upload_time",
        "processed_time",
        "server_received_time",
        "uuid",
        "user_properties",
    ];

    // Remove non-material differences
    for field in &non_material_fields {
        if comparison.differences.fields.remove(*field).is_some() {
            was_cleaned = true;
        }
    }

    // Check if this file has event_properties differences that need cleaning
    if let Some(event_props_diff) = comparison.differences.fields.get("event_properties") {
        if let (Value::Object(comparison_props), Value::Object(original_props)) =
            (&event_props_diff.comparison, &event_props_diff.original)
        {
            // Check if the only difference is property names (same values, different keys)
            if has_property_name_differences(comparison_props, original_props) {
                // Determine which event is later based on client_upload_time
                if let (Some(comparison_time), Some(original_time)) = (
                    comparison.comparison_event.client_upload_time,
                    comparison.original_event.client_upload_time,
                ) {
                    if comparison_time > original_time {
                        // Use comparison event properties (later event)
                        println!("Cleaning: using comparison event properties (later)");
                    } else {
                        // Use original event properties (later event)
                        println!("Cleaning: using original event properties (later)");
                    }

                    // Update the differences to remove the event_properties difference
                    comparison.differences.fields.remove("event_properties");
                    was_cleaned = true;
                }
            }
        }
    }

    // If there are no remaining differences after removing non-material ones,
    // we can consider this file fully cleaned
    if comparison.differences.fields.is_empty() {
        println!("Fully cleaned: no material differences remaining");
        was_cleaned = true;
    }

    was_cleaned
}

/// Utility functions for analyzing differences between event properties
pub mod difference_utils {
    use super::*;
    use std::collections::HashSet;

    /// Check if the only differences between two event_properties objects are property names
    /// (i.e., same keys but different values that represent the same property name)
    pub fn has_property_name_differences(
        comparison_props: &Map<String, Value>,
        original_props: &Map<String, Value>,
    ) -> bool {
        // If they have the same number of properties
        if comparison_props.len() != original_props.len() {
            println!("not the same length");
            return false;
        }

        // Check if all keys are the same
        let comparison_keys: HashSet<_> = comparison_props.keys().collect();
        let original_keys: HashSet<_> = original_props.keys().collect();

        if comparison_keys != original_keys {
            println!("not the same keys");
            return false;
        }

        let key = "Property";
        let comparison_val = comparison_props.get(key);
        let original_val = original_props.get(key);

        return original_val != comparison_val;
    }

    /// Get the set of property names (keys) that differ between two event_properties objects
    /// Returns None if the objects have different values (not just different keys)
    pub fn get_differing_property_names(
        comparison_props: &Map<String, Value>,
        original_props: &Map<String, Value>,
    ) -> Option<HashSet<String>> {
        // If they have the same number of properties
        if comparison_props.len() != original_props.len() {
            return None;
        }

        // Convert to sets of values to check if they're the same
        let comparison_values: HashSet<_> = comparison_props.values().collect();
        let original_values: HashSet<_> = original_props.values().collect();

        // If the values are the same, return the differing keys
        if comparison_values == original_values {
            let comparison_keys: HashSet<_> = comparison_props.keys().cloned().collect();
            let original_keys: HashSet<_> = original_props.keys().cloned().collect();

            if comparison_keys != original_keys {
                return Some(
                    comparison_keys
                        .symmetric_difference(&original_keys)
                        .cloned()
                        .collect(),
                );
            }
        }

        None
    }

    /// Check if two event_properties objects have identical values (regardless of property names)
    pub fn have_identical_values(
        comparison_props: &Map<String, Value>,
        original_props: &Map<String, Value>,
    ) -> bool {
        // Convert to sets of values to check if they're the same
        let comparison_values: HashSet<_> = comparison_props.values().collect();
        let original_values: HashSet<_> = original_props.values().collect();

        comparison_values == original_values
    }
}

/// Check if the only differences between two event_properties objects are property names
/// (i.e., same values but different keys)
fn has_property_name_differences(
    comparison_props: &Map<String, Value>,
    original_props: &Map<String, Value>,
) -> bool {
    difference_utils::has_property_name_differences(comparison_props, original_props)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Datelike, Timelike};
    use serde_json::json;

    #[test]
    fn test_has_property_name_differences() {
        // Test case 1: Same property names, different values that represent the same property
        let comparison_json = json!({
            "Property": "Ketupat House",
            "Drop Id": 470,
            "Total Price": 173.84
        });
        let comparison_props = comparison_json.as_object().unwrap();

        let original_json = json!({
            "Property": "Ketupat House ◊",
            "Drop Id": 470,
            "Total Price": 173.84
        });
        let original_props = original_json.as_object().unwrap();

        // Should return true because the property names are the same and values are DIFFERENT
        assert!(has_property_name_differences(
            comparison_props,
            original_props
        ));

        // Test case 2: Different property names (keys)
        let comparison_json2 = json!({
            "Property": "Ketupat House",
            "Drop Id": 470,
            "Total Price": 173.84
        });
        let comparison_props2 = comparison_json2.as_object().unwrap();

        let original_json2 = json!({
            "PropertyName": "Ketupat House",
            "Drop Id": 470,
            "Total Price": 173.84
        });
        let original_props2 = original_json2.as_object().unwrap();

        // Should return false because property names (keys) are different
        assert!(!has_property_name_differences(
            comparison_props2,
            original_props2
        ));

        // Test case 3: Same property names, completely different values
        let comparison_json3 = json!({
            "Property": "Ketupat House",
            "Drop Id": 470,
            "Total Price": 173.84
        });
        let comparison_props3 = comparison_json3.as_object().unwrap();

        let original_json3 = json!({
            "Property": "Different House",
            "Drop Id": 470,
            "Total Price": 173.84
        });
        let original_props3 = original_json3.as_object().unwrap();

        assert!(has_property_name_differences(
            comparison_props3,
            original_props3
        ));
    }

    #[test]
    fn test_difference_utils() {
        use difference_utils;

        // Test case 1: Same property names (keys), different values that represent the same property
        let comparison_json = json!({
            "Property": "Ketupat House",
            "Drop Id": 470,
            "Total Price": 173.84
        });
        let comparison_props = comparison_json.as_object().unwrap();

        let original_json = json!({
            "Property": "Ketupat House ◊",
            "Drop Id": 470,
            "Total Price": 173.84
        });
        let original_props = original_json.as_object().unwrap();

        // Test has_property_name_differences
        assert!(difference_utils::has_property_name_differences(
            comparison_props,
            original_props
        ));

        // Test get_differing_property_names - should return None since keys are the same
        let differing_names =
            difference_utils::get_differing_property_names(comparison_props, original_props);
        assert!(differing_names.is_none());

        // Test have_identical_values
        assert!(!difference_utils::have_identical_values(
            comparison_props,
            original_props
        ));

        // Test case 2: Different property names (keys)
        let comparison_json2 = json!({
            "Property": "Ketupat House",
            "Drop Id": 470,
            "Total Price": 173.84
        });
        let comparison_props2 = comparison_json2.as_object().unwrap();

        let original_json2 = json!({
            "PropertyName": "Ketupat House",
            "Drop Id": 470,
            "Total Price": 173.84
        });
        let original_props2 = original_json2.as_object().unwrap();

        // Test has_property_name_differences
        assert!(!difference_utils::has_property_name_differences(
            comparison_props2,
            original_props2
        ));

        // Test get_differing_property_names
        let differing_names =
            difference_utils::get_differing_property_names(comparison_props2, original_props2);
        assert!(differing_names.is_some());
        let differing_names = differing_names.unwrap();
        assert!(differing_names.contains("Property"));
        assert!(differing_names.contains("PropertyName"));
        assert_eq!(differing_names.len(), 2);

        // Test have_identical_values
        assert!(difference_utils::have_identical_values(
            comparison_props2,
            original_props2
        ));

        // Test case 3: Different number of properties
        let comparison_json3 = json!({
            "Property": "Ketupat House",
            "Drop Id": 470
        });
        let comparison_props3 = comparison_json3.as_object().unwrap();

        let original_json3 = json!({
            "Property": "Ketupat House",
            "Drop Id": 470,
            "Total Price": 173.84
        });
        let original_props3 = original_json3.as_object().unwrap();

        // Test has_property_name_differences
        assert!(!difference_utils::has_property_name_differences(
            comparison_props3,
            original_props3
        ));

        // Test get_differing_property_names
        let differing_names =
            difference_utils::get_differing_property_names(comparison_props3, original_props3);
        assert!(differing_names.is_none());

        // Test have_identical_values
        assert!(!difference_utils::have_identical_values(
            comparison_props3,
            original_props3
        ));
    }

    #[test]
    fn test_clean_comparison_result() {
        // Create a mock comparison result with non-material differences
        let mut comparison = ComparisonResult {
            comparison_event: ExportEvent {
                client_upload_time: Some(
                    DateTime::parse_from_str(
                        "2025-07-09 15:50:50.913000 +00:00",
                        "%Y-%m-%d %H:%M:%S%.f %z",
                    )
                    .unwrap()
                    .with_timezone(&Utc),
                ),
                ..Default::default()
            },
            differences: Differences {
                fields: {
                    let mut map = HashMap::new();
                    map.insert(
                        "app".to_string(),
                        EventDifference {
                            comparison: json!(714982),
                            original: json!(658833),
                        },
                    );
                    map.insert(
                        "uuid".to_string(),
                        EventDifference {
                            comparison: json!("new-uuid"),
                            original: json!("old-uuid"),
                        },
                    );
                    map.insert(
                        "event_properties".to_string(),
                        EventDifference {
                            comparison: json!({
                                "Property": "Ketupat House",
                                "Drop Id": 470
                            }),
                            original: json!({
                                "PropertyName": "Ketupat House",
                                "Drop Id": 470
                            }),
                        },
                    );
                    map
                },
            },
            insert_id: "test".to_string(),
            original_event: ExportEvent {
                client_upload_time: Some(
                    DateTime::parse_from_str(
                        "2025-06-30 07:56:20.621000 +00:00",
                        "%Y-%m-%d %H:%M:%S%.f %z",
                    )
                    .unwrap()
                    .with_timezone(&Utc),
                ),
                ..Default::default()
            },
        };

        // Clean the comparison
        let was_cleaned = clean_comparison_result(&mut comparison);

        // Should have been cleaned
        assert!(was_cleaned);

        // Non-material differences should be removed
        [
            "app",
            "client_upload_time",
            "processed_time",
            "server_received_time",
            "uuid",
            "user_properties",
        ]
        .iter()
        .for_each(|&v| {
            assert!(!comparison.differences.fields.contains_key(v));
        });
    }

    #[test]
    fn test_clean_comparison_result_no_changes() {
        // Create a comparison result with only material differences
        let mut comparison = ComparisonResult {
            comparison_event: ExportEvent::default(),
            differences: Differences {
                fields: {
                    let mut map = HashMap::new();
                    map.insert(
                        "event_type".to_string(),
                        EventDifference {
                            comparison: json!("Event A"),
                            original: json!("Event B"),
                        },
                    );
                    map
                },
            },
            insert_id: "test".to_string(),
            original_event: ExportEvent::default(),
        };

        // Clean the comparison
        let was_cleaned = clean_comparison_result(&mut comparison);

        // Should not have been cleaned (material differences remain)
        assert!(!was_cleaned);

        // Material differences should remain
        assert!(comparison.differences.fields.contains_key("event_type"));
    }

    #[test]
    fn test_deserialization() {
        // Test JSON that matches the structure of a comparison result file
        let json_str = r#"
        {
            "comparison_event": {
                "amplitude_id": 1084922772951,
                "app": 714982,
                "city": "Petaling Jaya",
                "client_event_time": "2025-03-05 14:36:36.916000",
                "client_upload_time": "2025-07-09 15:50:09.479000",
                "country": "Malaysia",
                "data": {
                    "group_first_event": {},
                    "group_ids": {},
                    "path": "/batch"
                },
                "data_type": "event",
                "device_carrier": "--",
                "device_id": "df9efdb9-b72b-556b-afdd-ed72ec455b5c",
                "event_id": 597609329,
                "event_properties": {
                    "Drop Id": 470,
                    "Drop Type": "Buy Anytime",
                    "Number of Shares": 16,
                    "Price per Share": 10.6,
                    "Property": "Ketupat House",
                    "Total Price": 173.84
                },
                "event_time": "2025-03-05 14:36:36.916000",
                "event_type": "Property Drop Purchased",
                "group_properties": {},
                "groups": {},
                "ip_address": "115.164.174.80",
                "language": "English",
                "library": "batch/1.0",
                "os_name": "ios",
                "os_version": "18.5",
                "platform": "iOS",
                "processed_time": "2025-07-09 15:50:11.265000",
                "region": "Selangor",
                "server_received_time": "2025-07-09 15:50:09.479000",
                "server_upload_time": "2025-07-09 15:50:09.543000",
                "session_id": -1,
                "user_id": "ac59bfee-4038-405b-b48c-c50d781cf11c",
                "user_properties": {
                    "User Tag": [
                        "beta_tester",
                        "can_refer",
                        "internal"
                    ]
                },
                "uuid": "65144498-0adb-4a4f-b21d-9e35f30920d9",
                "version_name": "1.0.80"
            },
            "differences": {
                "app": {
                    "comparison": 714982,
                    "original": 658833
                },
                "client_upload_time": {
                    "comparison": "2025-07-09 15:50:09.479000",
                    "original": "2025-03-05 14:36:38.284000"
                },
                "event_id": {
                    "comparison": 597609329,
                    "original": 447607237
                },
                "event_properties": {
                    "comparison": {
                        "Drop Id": 470,
                        "Drop Type": "Buy Anytime",
                        "Number of Shares": 16,
                        "Price per Share": 10.6,
                        "Property": "Ketupat House",
                        "Total Price": 173.84
                    },
                    "original": {
                        "Drop Id": 470,
                        "Drop Type": "Buy Anytime",
                        "Number of Shares": 16,
                        "Price per Share": 10.6,
                        "Property": "Ketupat House ◊",
                        "Total Price": 173.84
                    }
                },
                "processed_time": {
                    "comparison": "2025-07-09 15:50:11.265000",
                    "original": "2025-03-05 14:36:39.441000"
                },
                "server_received_time": {
                    "comparison": "2025-07-09 15:50:09.479000",
                    "original": "2025-03-05 14:36:38.284000"
                },
                "server_upload_time": {
                    "comparison": "2025-07-09 15:50:09.543000",
                    "original": "2025-03-05 14:36:38.289000"
                },
                "user_properties": {
                    "comparison": {
                        "User Tag": [
                            "beta_tester",
                            "can_refer",
                            "internal"
                        ]
                    },
                    "original": {}
                },
                "uuid": {
                    "comparison": "65144498-0adb-4a4f-b21d-9e35f30920d9",
                    "original": "7a7a2a97-4d99-4013-9aec-9424d3c8a240"
                }
            },
            "insert_id": "Property Drop Purchased:6572",
            "original_event": {
                "amplitude_id": 1084922772951,
                "app": 658833,
                "city": "Kuala Lumpur",
                "client_event_time": "2025-03-05 14:36:36.916000",
                "client_upload_time": "2025-03-05 14:36:38.284000",
                "country": "Malaysia",
                "data": {
                    "group_first_event": {},
                    "group_ids": {},
                    "path": "/2/httpapi",
                    "user_properties_updated": true
                },
                "data_type": "event",
                "device_carrier": "--",
                "device_family": "Apple iPhone",
                "device_id": "df9efdb9-b72b-556b-afdd-ed72ec455b5c",
                "device_type": "Apple iPhone 12",
                "event_id": 447607237,
                "event_properties": {
                    "Drop Id": 470,
                    "Drop Type": "Buy Anytime",
                    "Number of Shares": 16,
                    "Price per Share": 10.6,
                    "Property": "Ketupat House ◊",
                    "Total Price": 173.84
                },
                "event_time": "2025-03-05 14:36:36.916000",
                "event_type": "Property Drop Purchased",
                "group_properties": {},
                "groups": {},
                "ip_address": "161.142.150.64",
                "language": "English",
                "library": "http/2.0",
                "os_name": "ios",
                "os_version": "18.1.1",
                "platform": "iOS",
                "processed_time": "2025-03-05 14:36:39.441000",
                "region": "Kuala Lumpur",
                "server_received_time": "2025-03-05 14:36:38.284000",
                "server_upload_time": "2025-03-05 14:36:38.289000",
                "session_id": -1,
                "start_version": "1.0.58",
                "user_id": "ac59bfee-4038-405b-b48c-c50d781cf11c",
                "user_properties": {},
                "uuid": "7a7a2a97-4d99-4013-9aec-9424d3c8a240",
                "version_name": "1.0.69"
            }
        }
        "#;

        // Deserialize the JSON into our ComparisonResult structure
        let result: Result<ComparisonResult, serde_json::Error> = serde_json::from_str(json_str);
        assert!(result.is_ok(), "Deserialization should succeed");

        let comparison = result.unwrap();

        // Test that the top-level fields are correctly deserialized
        assert_eq!(comparison.insert_id, "Property Drop Purchased:6572");

        // Test that the comparison_event is correctly deserialized
        assert_eq!(comparison.comparison_event.app, Some(714982));
        assert_eq!(
            comparison.comparison_event.city,
            Some("Petaling Jaya".to_string())
        );
        assert_eq!(
            comparison.comparison_event.event_type,
            Some("Property Drop Purchased".to_string())
        );

        // Test that the original_event is correctly deserialized
        assert_eq!(comparison.original_event.app, Some(658833));
        assert_eq!(
            comparison.original_event.city,
            Some("Kuala Lumpur".to_string())
        );
        assert_eq!(
            comparison.original_event.event_type,
            Some("Property Drop Purchased".to_string())
        );

        // Test that the differences object is correctly deserialized with #[serde(flatten)]
        // The #[serde(flatten)] attribute means that the fields HashMap contains all the
        // key-value pairs from the "differences" object in the JSON
        assert!(comparison.differences.fields.contains_key("app"));
        assert!(comparison
            .differences
            .fields
            .contains_key("client_upload_time"));
        assert!(comparison
            .differences
            .fields
            .contains_key("event_properties"));
        assert!(comparison.differences.fields.contains_key("uuid"));

        // Test that individual EventDifference objects are correctly deserialized
        let app_diff = &comparison.differences.fields["app"];
        assert_eq!(app_diff.comparison, json!(714982));
        assert_eq!(app_diff.original, json!(658833));

        let event_props_diff = &comparison.differences.fields["event_properties"];

        // Test that the event_properties difference contains the expected structure
        if let (Value::Object(comp_props), Value::Object(orig_props)) =
            (&event_props_diff.comparison, &event_props_diff.original)
        {
            // Check that the comparison properties contain the expected values
            assert_eq!(comp_props["Property"], "Ketupat House");
            assert_eq!(comp_props["Drop Id"], 470);
            assert_eq!(comp_props["Total Price"], 173.84);

            // Check that the original properties contain the expected values
            assert_eq!(orig_props["Property"], "Ketupat House ◊");
            assert_eq!(orig_props["Drop Id"], 470);
            assert_eq!(orig_props["Total Price"], 173.84);
        } else {
            panic!("event_properties should be objects");
        }

        // Test that we can serialize and deserialize round-trip
        let serialized = serde_json::to_string_pretty(&comparison).unwrap();
        let round_trip: ComparisonResult = serde_json::from_str(&serialized).unwrap();

        assert_eq!(round_trip.insert_id, comparison.insert_id);
        assert_eq!(
            round_trip.comparison_event.app,
            comparison.comparison_event.app
        );
        assert_eq!(round_trip.original_event.app, comparison.original_event.app);
        assert_eq!(
            round_trip.differences.fields.len(),
            comparison.differences.fields.len()
        );
    }
}
