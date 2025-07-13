use serde::{Deserialize, Serialize};

use crate::common::amplitude_types::ExportEvent;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Dupe {
    PreOrderDropCompletedMistake(Vec<ExportEvent>),
    PropertyNameChange(Vec<ExportEvent>),
    PropertyDropPriceChange(Vec<ExportEvent>),
    DropTypeChange(Vec<ExportEvent>),
    TrueDuplicate(Vec<ExportEvent>), // this one might not matter because Amplitude might deduplicate these if they are EXACTLY the same
    UnknownPropDiff(Vec<ExportEvent>),
    Unknown(Vec<ExportEvent>),
    TooMany(Vec<ExportEvent>),
    Multi(Vec<ExportEvent>, Vec<Dupe>),
    EventPropsIncompatible(Vec<ExportEvent>),
}

impl Dupe {
    pub fn resolution(self) -> DupeResolution {
        match self {
            Dupe::PreOrderDropCompletedMistake(items) => {
                let mut submitted_event = None;
                let mut completed_event = None;
                
                for item in items {
                    if let Some(event_type) = &item.event_type {
                        match event_type.as_str() {
                            "Property Pre-Order Submitted" => submitted_event = Some(item.clone()),
                            "Property Pre-Order Completed" => completed_event = Some(item.clone()),
                            _ => {}
                        }
                    }
                }
                
                let mut result_events = Vec::new();
                
                // Add the submitted event as-is
                if let Some(submitted) = submitted_event {
                    result_events.push(submitted);
                }
                
                // Add the completed event with modified insert_id
                if let Some(mut completed) = completed_event {
                    // Modify the insert_id to reflect that this is a completed event
                    if let Some(insert_id) = &completed.insert_id {
                        // Replace "Submitted" with "Completed" in the insert_id
                        let new_insert_id = insert_id.replace("Submitted", "Completed");
                        completed.insert_id = Some(new_insert_id);
                    }
                    result_events.push(completed);
                }
                
                DupeResolution::KeepMany(result_events)
            }
            Dupe::DropTypeChange(items)
            | Dupe::PropertyNameChange(items)
            | Dupe::PropertyDropPriceChange(items) => {
                // Keep ALL properties from the event that had an earlier upload time
                // EXCEPT event_properties, which should be from the second event
                let (earlier_event, later_event) =
                    if items[0].client_upload_time < items[1].client_upload_time {
                        (&items[0], &items[1])
                    } else {
                        (&items[1], &items[0])
                    };

                // Create a new event with all properties from the earlier event, but event_properties from the later event
                let mut merged_event = earlier_event.clone();
                merged_event.event_properties = later_event.event_properties.clone();

                DupeResolution::KeepOne(merged_event)
            }
            Dupe::TrueDuplicate(items) => {
                let kept = items
                    .iter()
                    .min_by(|v1, v2| {
                        return v1.client_upload_time.cmp(&v2.client_upload_time);
                    })
                    .unwrap();
                DupeResolution::KeepOne(kept.clone())
            }
            Dupe::Unknown(_) => DupeResolution::Error(self),
            Dupe::UnknownPropDiff(_) => DupeResolution::Error(self),
            Dupe::TooMany(_) => DupeResolution::Error(self),
            Dupe::EventPropsIncompatible(_) => DupeResolution::Error(self),
            Dupe::Multi(_, _) => DupeResolution::Error(self),
        }
    }

    pub fn from_events(events: &Vec<ExportEvent>) -> Self {
        if events.len() > 2 {
            return Dupe::TooMany(events.clone());
        }

        // We skip diff checking if we have confirmed that this is truly a server-side event. It is expected
        // that there are differences. For re-sending server-side events, what we should do is
        // to take all the metadata of the first event, and the properties of the second event
        let mut skip_diff_check = false;

        let mut tentative: Option<Dupe> = None;
        let mut set_tentative = |v| {
            if tentative.is_none() {
                tentative = Some(v);
            } else {
                let prev = tentative.clone().unwrap();
                let mut current_col = match prev {
                    Dupe::Multi(items, types) => types,
                    _ => vec![prev],
                };
                current_col.push(v);
                tentative = Some(Dupe::Multi(events.clone(), current_col));
            }
        };

        let submitted = Some("Property Pre-Order Submitted".to_owned());
        let completed = Some("Property Pre-Order Completed".to_owned());
        if events.iter().any(|e| e.event_type == submitted)
            && events.iter().any(|e| e.event_type == completed)
        {
            // This is a server-sent event that was mistakenly labelled with the same insert_id
            // Therefore it only makes sense that we have significant diffs in various fields
            // Hence we should take BOTH the events, but modify the one with event name "completed"
            // to have an insert id that matches "completed"
            set_tentative(Dupe::PreOrderDropCompletedMistake(events.clone()));
            skip_diff_check = true;
        }

        let first = events[0].clone();
        let second = events[1].clone();
        if first.event_properties != second.event_properties {
            match (first.event_properties, second.event_properties) {
                (Some(first_props), Some(second_props)) => {
                    // uuids only for client-side events
                    if uuid::Uuid::parse_str(&first.insert_id.unwrap().to_string()).is_ok() {
                        set_tentative(Dupe::Unknown(events.clone()));
                    } else {
                        // These are server-sent events where we modified something before backfill added a duplicate
                        // so we should NOT care about properties that Amplitude added on
                        if first_props.get("Property") != second_props.get("Property") {
                            set_tentative(Dupe::PropertyNameChange(events.clone()));
                            skip_diff_check = true;
                        }

                        if first_props.get("Drop Type") != second_props.get("Drop Type") {
                            set_tentative(Dupe::DropTypeChange(events.clone()));
                            skip_diff_check = true;
                        }

                        if first_props.get("Price per Share") != second_props.get("Price per Share")
                        {
                            set_tentative(Dupe::PropertyDropPriceChange(events.clone()));
                            skip_diff_check = true;
                        }
                    }
                }
                (None, Some(_)) => set_tentative(Dupe::EventPropsIncompatible(events.clone())),
                (Some(_), None) => set_tentative(Dupe::EventPropsIncompatible(events.clone())),
                (None, None) => panic!("Impossible condition"),
            };
        }

        if !skip_diff_check {
            let first = events[0].clone();
            let second = events[1].clone();
            if first == second {
                set_tentative(Dupe::TrueDuplicate(events.clone()));
            } else {
                set_tentative(Dupe::UnknownPropDiff(events.clone()));
            }
        }

        if tentative.is_some() {
            tentative.unwrap()
        } else {
            Dupe::Unknown(events.clone())
        }
    }

    pub(crate) fn to_str(&self) -> String {
        match &self {
            Dupe::PreOrderDropCompletedMistake(_) => "PreOrderDropCompletedMistake",
            Dupe::PropertyNameChange(_) => "PropertyNameChange",
            Dupe::DropTypeChange(_) => "DropTypeChange",
            Dupe::PropertyDropPriceChange(_) => "PropertyDropPriceChange",
            Dupe::TrueDuplicate(_) => "TrueDuplicate",
            Dupe::Unknown(_) => "Unknown",
            Dupe::TooMany(_) => "TooMany",
            Dupe::Multi(_, _) => "Multi",
            Dupe::EventPropsIncompatible(_) => "EventPropsIncompatible",
            Dupe::UnknownPropDiff(export_events) => "UnknownPropDiff",
        }
        .to_string()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DupeResolution {
    KeepOne(ExportEvent),
    KeepMany(Vec<ExportEvent>),
    Error(Dupe),
}
