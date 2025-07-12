use chrono::{DateTime, Utc};
use impl_tools::autoimpl;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use uuid::Uuid;

/// Request body for the Batch Event Upload API
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BatchUploadRequest {
    /// Required. Amplitude project API key
    pub api_key: String,
    /// Required. Array of events to upload
    pub events: Vec<Event>,
    /// Optional. Options for the request
    #[serde(skip_serializing_if = "Option::is_none")]
    pub options: Option<UploadOptions>,
}

/// Options for the batch upload request
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UploadOptions {
    /// Minimum length for device IDs and user IDs (default: 5)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_id_length: Option<usize>,
}

// TODO: how would I verify that this and the exported event are the same?
// My A: list the fields that matter and check if they are the same
// Follow-up: list the fields that are the same between a round-trip export-upload-export event. This should be most, and should be verifiable through a manual run with small sample size.
// TODO: alternatives to the above?
// TODO: get bot to check https://amplitude.com/docs/apis/analytics/batch-event-upload for the TODOs where fields are missing
/// Individual event in the batch upload
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct Event {
    /// Required. A readable ID specified by you. Must have a minimum length of 5 characters.
    /// Required unless device_id is present.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_id: Option<String>,

    /// Required. A device-specific identifier. Must have a minimum length of 5 characters.
    /// Required unless user_id is present.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub device_id: Option<String>,

    /// Required. The name of the event being tracked.
    pub event_type: String,

    /// Required. The timestamp of the event in milliseconds since epoch.
    pub time: i64,

    // TODO: check if event properties makes sense to EVER be none?
    /// Optional. A map of event properties.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event_properties: Option<HashMap<String, Value>>,

    /// Optional. A map of user properties.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_properties: Option<HashMap<String, Value>>,

    // This is also only for customers who have purchased the Accounts add-on
    /// Optional. A map of group properties.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub groups: Option<HashMap<String, Value>>,

    // This is also only for customers who have purchased the Accounts add-on
    /// Optional. A map of group properties.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub group_properties: Option<HashMap<String, Value>>,

    // Possible that it is important to set this to true because
    // it actually tries to read data from the user properties table and sync it with the event
    // However, we are backfilling historical data. Therefore we should probably not use syncs
    // Yes. should set to true
    /// Optional. When set to true, user properties will not be synced to the user profile.
    #[serde(
        skip_serializing_if = "Option::is_none",
        rename = "$skip_user_properties_sync"
    )]
    pub skip_user_properties_sync: Option<bool>,

    /// Optional. The version of the app.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub app_version: Option<String>,

    /// Optional. The platform of the device.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub platform: Option<String>,

    /// Optional. The operating system name.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub os_name: Option<String>,

    /// Optional. The operating system version.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub os_version: Option<String>,

    /// Optional. The device brand.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub device_brand: Option<String>,

    /// Optional. The device manufacturer.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub device_manufacturer: Option<String>,

    /// Optional. The device model.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub device_model: Option<String>,

    /// Optional. The carrier.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub carrier: Option<String>,

    /// Optional. The country.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub country: Option<String>,

    /// Optional. The region.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub region: Option<String>,

    /// Optional. The city.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub city: Option<String>,

    /// Optional. The DMA (Designated Market Area).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dma: Option<String>,

    /// Optional. The language.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub language: Option<String>,

    /// Optional. The price.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub price: Option<f64>,

    /// Optional. The quantity.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quantity: Option<i32>,

    /// Optional. The revenue.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub revenue: Option<f64>,

    /// Optional. The product ID.
    #[serde(skip_serializing_if = "Option::is_none", rename = "productId")]
    pub product_id: Option<String>,

    /// Optional. The revenue type.
    #[serde(skip_serializing_if = "Option::is_none", rename = "revenueType")]
    pub revenue_type: Option<String>,

    /// Optional. The latitude.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub location_lat: Option<f64>,

    /// Optional. The longitude.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub location_lng: Option<f64>,

    /// Optional. The IP address.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ip: Option<String>,

    /// Optional. (iOS only) The IDFA (Identifier for Advertisers).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub idfa: Option<String>,

    /// Optional. (iOS only) The IDFV (Identifier for Vendors).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub idfv: Option<String>,

    /// Optional. The ADID (Android Advertising ID).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub adid: Option<String>,

    /// Optional. The Android ID.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub android_id: Option<String>,

    /// Optional. The event ID.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event_id: Option<i64>,

    /// Optional. The session ID.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<i64>,

    /// Optional. The insert ID.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub insert_id: Option<String>,

    /// Optional. Plan information containing branch, source, and version.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub plan: Option<HashMap<String, Value>>,
}

/// Response from the Batch Event Upload API
#[derive(Debug, Serialize, Deserialize)]
pub struct BatchUploadResponse {
    /// The HTTP status code
    pub code: i32,
    /// Error description
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// Missing field description
    #[serde(skip_serializing_if = "Option::is_none")]
    pub missing_field: Option<String>,
    /// Events with invalid fields
    #[serde(skip_serializing_if = "Option::is_none")]
    pub events_with_invalid_fields: Option<HashMap<String, Vec<usize>>>,
    /// Events with missing fields
    #[serde(skip_serializing_if = "Option::is_none")]
    pub events_with_missing_fields: Option<HashMap<String, Vec<usize>>>,
    /// EPS threshold
    #[serde(skip_serializing_if = "Option::is_none")]
    pub eps_threshold: Option<i32>,
    /// Exceeded daily quota devices
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exceeded_daily_quota_devices: Option<HashMap<String, i64>>,
    /// Silenced devices
    #[serde(skip_serializing_if = "Option::is_none")]
    pub silenced_devices: Option<Vec<String>>,
    /// Silenced events
    #[serde(skip_serializing_if = "Option::is_none")]
    pub silenced_events: Option<Vec<usize>>,
    /// Throttled devices
    #[serde(skip_serializing_if = "Option::is_none")]
    pub throttled_devices: Option<HashMap<String, i32>>,
    /// Throttled events
    #[serde(skip_serializing_if = "Option::is_none")]
    pub throttled_events: Option<Vec<usize>>,
    /// Throttled users
    #[serde(skip_serializing_if = "Option::is_none")]
    pub throttled_users: Option<HashMap<String, i32>>,
    /// Exceeded daily quota users
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exceeded_daily_quota_users: Option<HashMap<String, i64>>,
}

/// Error response for payload too large
#[derive(Debug, Serialize, Deserialize)]
pub struct PayloadTooLargeError {
    /// 413 error code
    pub code: i32,
    /// Error description
    pub error: String,
}

/// Error response for too many requests
#[derive(Debug, Serialize, Deserialize)]
pub struct TooManyRequestsError {
    /// 429 error code
    pub code: i32,
    /// Error description
    pub error: String,
    /// EPS threshold
    pub eps_threshold: i32,
    /// Throttled devices
    #[serde(skip_serializing_if = "Option::is_none")]
    pub throttled_devices: Option<HashMap<String, i32>>,
    /// Throttled users
    #[serde(skip_serializing_if = "Option::is_none")]
    pub throttled_users: Option<HashMap<String, i32>>,
    /// Exceeded daily quota users
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exceeded_daily_quota_users: Option<HashMap<String, i64>>,
    /// Exceeded daily quota devices
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exceeded_daily_quota_devices: Option<HashMap<String, i64>>,
    /// Throttled events
    #[serde(skip_serializing_if = "Option::is_none")]
    pub throttled_events: Option<Vec<usize>>,
}

/// Error response for silenced device IDs
#[derive(Debug, Serialize, Deserialize)]
pub struct SilencedDeviceIDError {
    /// 400 error code
    pub code: i32,
    /// Error description
    pub error: String,
    /// EPS threshold
    pub eps_threshold: i32,
    /// Exceeded daily quota devices
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exceeded_daily_quota_devices: Option<HashMap<String, i64>>,
    /// Silenced devices
    #[serde(skip_serializing_if = "Option::is_none")]
    pub silenced_devices: Option<Vec<String>>,
    /// Silenced events
    #[serde(skip_serializing_if = "Option::is_none")]
    pub silenced_events: Option<Vec<usize>>,
    /// Throttled devices
    #[serde(skip_serializing_if = "Option::is_none")]
    pub throttled_devices: Option<HashMap<String, i32>>,
    /// Throttled events
    #[serde(skip_serializing_if = "Option::is_none")]
    pub throttled_events: Option<Vec<usize>>,
}

/// Custom deserializer for export API time format
pub fn deserialize_amplitude_timestamp<'de, D>(deserializer: D) -> Result<Option<DateTime<Utc>>, D::Error>
where
    D: Deserializer<'de>,
{
    let opt = Option::<String>::deserialize(deserializer)?;
    match opt {
        Some(s) => {
            let dt = DateTime::parse_from_str(&format!("{} +0000", s), "%Y-%m-%d %H:%M:%S%.6f %z")
                .map_err(serde::de::Error::custom)?;
            Ok(Some(dt.with_timezone(&Utc)))
        }
        None => Ok(None),
    }
}

/// Custom serializer for export API time format
pub fn serialize_amplitude_timestamp<S>(time: &Option<DateTime<Utc>>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    match time {
        Some(dt) => {
            let formatted = dt.format("%Y-%m-%d %H:%M:%S%.6f").to_string();
            serializer.serialize_str(&formatted)
        }
        None => serializer.serialize_none(),
    }
}

/// Event structure from Amplitude Export API
#[autoimpl(PartialEq ignore 
    self.city,
self.country,
self.device_carrier,
self.device_family,
self.device_type,
self.event_id,
self.ip_address,
self.os_name,
self.os_version,
self.platform,
self.client_upload_time,
self.processed_time,
self.server_received_time,
self.server_upload_time,
self.user_properties,
self.uuid,
self.language,
self.region,
self.dma,
self.data,
self.start_version,
self.version_name
)]
#[derive(Default, Serialize, Deserialize, Debug, Clone)]
pub struct ExportEvent {
    #[serde(rename = "$insert_id")]
    pub insert_id: Option<String>,
    #[serde(rename = "$insert_key")]
    pub insert_key: Option<Value>,
    #[serde(rename = "$schema")]
    pub schema: Option<Value>,
    pub adid: Option<String>,
    pub amplitude_attribution_ids: Option<Value>,
    pub amplitude_event_type: Option<Value>,
    pub amplitude_id: Option<i64>,
    pub app: Option<i64>,
    pub city: Option<String>,
    #[serde(
        deserialize_with = "deserialize_amplitude_timestamp",
        serialize_with = "serialize_amplitude_timestamp"
    )]
    pub client_event_time: Option<DateTime<Utc>>,
    #[serde(
        deserialize_with = "deserialize_amplitude_timestamp",
        serialize_with = "serialize_amplitude_timestamp"
    )]
    pub client_upload_time: Option<DateTime<Utc>>,
    pub country: Option<String>,
    pub data: Option<HashMap<String, Value>>,
    pub data_type: Option<String>,
    pub device_brand: Option<String>,
    pub device_carrier: Option<String>,
    pub device_family: Option<String>,
    pub device_id: Option<String>,
    pub device_manufacturer: Option<String>,
    pub device_model: Option<String>,
    pub device_type: Option<String>,
    pub dma: Option<String>,
    pub event_id: Option<i64>,
    pub event_properties: Option<HashMap<String, Value>>,
    #[serde(
        deserialize_with = "deserialize_amplitude_timestamp",
        serialize_with = "serialize_amplitude_timestamp"
    )]
    pub event_time: Option<DateTime<Utc>>,
    pub event_type: Option<String>,
    pub global_user_properties: Option<Value>,
    pub group_properties: Option<HashMap<String, Value>>,
    pub groups: Option<HashMap<String, Value>>,
    pub idfa: Option<String>,
    pub ip_address: Option<String>,
    pub is_attribution_event: Option<Value>,
    pub language: Option<String>,
    pub library: Option<String>,
    pub location_lat: Option<f64>,
    pub location_lng: Option<f64>,
    pub os_name: Option<String>,
    pub os_version: Option<String>,
    pub partner_id: Option<Value>,
    pub paying: Option<Value>,
    pub plan: Option<HashMap<String, Value>>,
    pub platform: Option<String>,
    pub processed_time: Option<String>,
    pub region: Option<String>,
    pub sample_rate: Option<Value>,
    #[serde(
        deserialize_with = "deserialize_amplitude_timestamp",
        serialize_with = "serialize_amplitude_timestamp"
    )]
    pub server_received_time: Option<DateTime<Utc>>,
    #[serde(
        deserialize_with = "deserialize_amplitude_timestamp",
        serialize_with = "serialize_amplitude_timestamp"
    )]
    pub server_upload_time: Option<DateTime<Utc>>,
    pub session_id: Option<i64>,
    pub source_id: Option<Value>,
    pub start_version: Option<Value>,
    pub user_creation_time: Option<Value>,
    pub user_id: Option<String>,
    pub user_properties: Option<HashMap<String, Value>>,
    pub uuid: Option<String>,
    pub version_name: Option<Value>,
}

impl ExportEvent {
    /// Convert an ExportEvent to an Event for batch upload
    pub fn to_batch_event(&self) -> Result<Event, Box<dyn std::error::Error>> {
        // Get event time in milliseconds since epoch
        let time = self
            .event_time
            .ok_or("Missing event_time")?
            .timestamp_millis();

        // Ensure we have either user_id or device_id
        if self.user_id.is_none() && self.device_id.is_none() {
            return Err("Either user_id or device_id is required".into());
        }

        // Ensure we have event_type
        let event_type = self
            .event_type
            .as_ref()
            .ok_or("Missing event_type")?
            .clone();
        if event_type.is_empty() {
            return Err("Event type is empty".into());
        }

        let insert_id = self.insert_id.as_ref().ok_or("Missing insert_id")?.clone();
        if insert_id.is_empty() {
            return Err("Insert ID is empty".into());
        }

        Ok(Event {
            user_id: self.user_id.clone(),
            device_id: self.device_id.clone(),
            event_type,
            time,
            event_properties: self.event_properties.clone(),
            user_properties: self.user_properties.clone(),
            groups: self.groups.clone(),
            group_properties: self.group_properties.clone(),
            skip_user_properties_sync: Some(true), // Hardcode to true because we are backfilling historical data and don't want to take latest user properties and merge
            app_version: self
                .version_name
                .as_ref()
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            platform: self.platform.clone(),
            os_name: self.os_name.clone(),
            os_version: self.os_version.clone(),
            device_brand: self.device_brand.clone(),
            device_manufacturer: self.device_manufacturer.clone(),
            device_model: self.device_model.clone(),
            carrier: self.device_carrier.clone(),
            country: self.country.clone(),
            region: self.region.clone(),
            city: self.city.clone(),
            dma: self.dma.clone(),
            language: self.language.clone(),
            price: None,        // Not directly mapped from export event
            quantity: None,     // Not directly mapped from export event
            revenue: None,      // Not directly mapped from export event
            product_id: None,   // Not directly mapped from export event
            revenue_type: None, // Not directly mapped from export event
            location_lat: self.location_lat,
            location_lng: self.location_lng,
            ip: self.ip_address.clone(),
            idfa: self.idfa.clone(),
            idfv: None, // Not available in export event
            adid: self.adid.clone(),
            android_id: None, // Not available in export event
            event_id: self.event_id,
            session_id: self.session_id,
            insert_id: self.insert_id.clone(),
            plan: self.plan.clone(),
        })
    }
}

/// Trait for filtering ExportEvent instances
pub trait ExportEventFilter {
    /// Determine whether an event should be included (true) or filtered out (false)
    fn should_include(&mut self, event: &ExportEvent) -> bool;
    
    /// Get a description of the filter for logging/debugging purposes
    fn description(&self) -> &str;
}

/// Default implementation of ExportEventFilter that includes all events
pub struct DefaultFilter;

impl ExportEventFilter for DefaultFilter {
    fn should_include(&mut self, _event: &ExportEvent) -> bool {
        true
    }
    
    fn description(&self) -> &str {
        "Default filter (includes all events)"
    }
}

/// Filter that matches events based on multiple criteria
pub struct MultiCriteriaFilter {
    event_type: Option<String>,
    user_id: Option<String>,
    device_id: Option<String>,
    insert_id: Option<String>,
    uuid: Option<String>,
    start_time: Option<DateTime<Utc>>,
    end_time: Option<DateTime<Utc>>,
    invert: bool,
}

impl MultiCriteriaFilter {
    /// Create a new MultiCriteriaFilter with the specified criteria
    pub fn new(
        event_type: Option<String>,
        user_id: Option<String>,
        device_id: Option<String>,
        insert_id: Option<String>,
        uuid: Option<String>,
        start_time: Option<DateTime<Utc>>,
        end_time: Option<DateTime<Utc>>,
        invert: bool,
    ) -> Self {
        Self {
            event_type,
            user_id,
            device_id,
            insert_id,
            uuid,
            start_time,
            end_time,
            invert,
        }
    }
    
    /// Builder method to set event type filter
    pub fn event_type(mut self, event_type: Option<String>) -> Self {
        self.event_type = event_type;
        self
    }
    
    /// Builder method to set user ID filter
    pub fn user_id(mut self, user_id: Option<String>) -> Self {
        self.user_id = user_id;
        self
    }
    
    /// Builder method to set device ID filter
    pub fn device_id(mut self, device_id: Option<String>) -> Self {
        self.device_id = device_id;
        self
    }
    
    /// Builder method to set insert ID filter
    pub fn insert_id(mut self, insert_id: Option<String>) -> Self {
        self.insert_id = insert_id;
        self
    }
    
    /// Builder method to set UUID filter
    pub fn uuid(mut self, uuid: Option<String>) -> Self {
        self.uuid = uuid;
        self
    }
    
    /// Builder method to set start time filter
    pub fn start_time(mut self, start_time: Option<DateTime<Utc>>) -> Self {
        self.start_time = start_time;
        self
    }
    
    /// Builder method to set end time filter
    pub fn end_time(mut self, end_time: Option<DateTime<Utc>>) -> Self {
        self.end_time = end_time;
        self
    }
    
    /// Builder method to set invert flag
    pub fn invert(mut self, invert: bool) -> Self {
        self.invert = invert;
        self
    }
}

impl ExportEventFilter for MultiCriteriaFilter {
    fn should_include(&mut self, event: &ExportEvent) -> bool {
        let mut matches = true;
        
        // Check event_type filter
        if let Some(ref filter_event_type) = self.event_type {
            matches = matches && event.event_type.as_deref() == Some(filter_event_type);
        }
        
        // Check user_id filter
        if let Some(ref filter_user_id) = self.user_id {
            matches = matches && event.user_id.as_deref() == Some(filter_user_id);
        }
        
        // Check device_id filter
        if let Some(ref filter_device_id) = self.device_id {
            matches = matches && event.device_id.as_deref() == Some(filter_device_id);
        }
        
        // Check insert_id filter
        if let Some(ref filter_insert_id) = self.insert_id {
            matches = matches && event.insert_id.as_deref() == Some(filter_insert_id);
        }
        
        // Check uuid filter
        if let Some(ref filter_uuid) = self.uuid {
            matches = matches && event.uuid.as_deref() == Some(filter_uuid);
        }
        
        // Check time filters
        if let Some(start_filter) = self.start_time {
            if let Some(event_time) = event.event_time {
                matches = matches && event_time >= start_filter;
            } else {
                matches = false;
            }
        }
        
        if let Some(end_filter) = self.end_time {
            if let Some(event_time) = event.event_time {
                matches = matches && event_time <= end_filter;
            } else {
                matches = false;
            }
        }
        
        // Invert the result if requested
        if self.invert {
            !matches
        } else {
            matches
        }
    }
    
    fn description(&self) -> &str {
        "Multi-criteria filter"
    }
}

/// Filter that deduplicates events based on UUID vs non-UUID insert_ids
/// 
/// This filter works as follows:
/// 1. If an event's insert_id is a valid UUID, it is always included
/// 2. If an event's insert_id is not a UUID, it is only included if it's the first
///    event with that insert_id encountered
pub struct UUIDDeduplicationFilter {
    non_uuid_events: std::collections::HashMap<String, Vec<ExportEvent>>,
}

impl UUIDDeduplicationFilter {
    /// Create a new UUIDDeduplicationFilter
    pub fn new() -> Self {
        Self {
            non_uuid_events: std::collections::HashMap::new(),
        }
    }
    
    /// Check if a string is a valid UUID
    fn is_uuid(s: &str) -> bool {
        Uuid::parse_str(s).is_ok()
    }
    
    /// Get statistics about the filter's operation
    pub fn get_stats(&self) -> (usize, usize) {
        let total_non_uuid_events: usize = self.non_uuid_events.values().map(|v| v.len()).sum();
        let unique_insert_ids = self.non_uuid_events.len();
        (total_non_uuid_events, unique_insert_ids)
    }
}

impl ExportEventFilter for UUIDDeduplicationFilter {
    fn should_include(&mut self, event: &ExportEvent) -> bool {
        // Get the insert_id, if it doesn't exist, treat as non-UUID
        let insert_id = match &event.insert_id {
            Some(id) if !id.is_empty() => id,
            _ => return false, // No insert_id, filter out
        };
        
        // Check if insert_id is a UUID
        if Self::is_uuid(insert_id) {
            // UUID events are always included
            return true;
        }
        
        // Non-UUID event - check if this is the first one with this insert_id
        let events_with_this_id = self.non_uuid_events.entry(insert_id.clone()).or_insert_with(Vec::new);
        
        if events_with_this_id.is_empty() {
            // First event with this insert_id, include it and add to tracking
            events_with_this_id.push(event.clone());
            true
        } else {
            // Not the first event with this insert_id, filter it out
            events_with_this_id.push(event.clone());
            false
        }
    }
    
    fn description(&self) -> &str {
        "UUID-based deduplication filter"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    

    #[test]
    fn test_event_serialization() {
        let event = Event {
            user_id: Some("test_user".to_string()),
            device_id: Some("test_device".to_string()),
            event_type: "test_event".to_string(),
            time: 1640995200000,
            event_properties: Some(HashMap::new()),
            user_properties: None,
            groups: None,
            group_properties: None,
            skip_user_properties_sync: None,
            app_version: Some("1.0.0".to_string()),
            platform: Some("iOS".to_string()),
            os_name: Some("iOS".to_string()),
            os_version: Some("15.0".to_string()),
            device_brand: Some("Apple".to_string()),
            device_manufacturer: Some("Apple".to_string()),
            device_model: Some("iPhone 13".to_string()),
            carrier: Some("Verizon".to_string()),
            country: Some("United States".to_string()),
            region: Some("California".to_string()),
            city: Some("San Francisco".to_string()),
            dma: Some("San Francisco-Oakland-San Jose, CA".to_string()),
            language: Some("English".to_string()),
            price: Some(4.99),
            quantity: Some(1),
            revenue: Some(4.99),
            product_id: Some("test_product".to_string()),
            revenue_type: Some("Purchase".to_string()),
            location_lat: Some(37.7749),
            location_lng: Some(-122.4194),
            ip: Some("127.0.0.1".to_string()),
            idfa: Some("AEBE52E7-03EE-455A-B3C4-E57283966239".to_string()),
            idfv: Some("BCCE52E7-03EE-321A-B3D4-E57123966239".to_string()),
            adid: None,
            android_id: None,
            event_id: Some(123),
            session_id: Some(1640995200000),
            insert_id: Some("5f0adeff-6668-4427-8d02-57d803a2b841".to_string()),
            plan: None,
        };

        let json = serde_json::to_string(&event).unwrap();
        let deserialized: Event = serde_json::from_str(&json).unwrap();

        assert_eq!(event.user_id, deserialized.user_id);
        assert_eq!(event.event_type, deserialized.event_type);
        assert_eq!(event.time, deserialized.time);
    }

    #[test]
    fn test_batch_upload_request_serialization() {
        let event = Event {
            user_id: Some("test_user".to_string()),
            device_id: None,
            event_type: "test_event".to_string(),
            time: 1640995200000,
            event_properties: None,
            user_properties: None,
            groups: None,
            group_properties: None,
            skip_user_properties_sync: None,
            app_version: None,
            platform: None,
            os_name: None,
            os_version: None,
            device_brand: None,
            device_manufacturer: None,
            device_model: None,
            carrier: None,
            country: None,
            region: None,
            city: None,
            dma: None,
            language: None,
            price: None,
            quantity: None,
            revenue: None,
            product_id: None,
            revenue_type: None,
            location_lat: None,
            location_lng: None,
            ip: None,
            idfa: None,
            idfv: None,
            adid: None,
            android_id: None,
            event_id: None,
            session_id: None,
            insert_id: None,
            plan: None,
        };

        let request = BatchUploadRequest {
            api_key: "test_api_key".to_string(),
            events: vec![event],
            options: Some(UploadOptions {
                min_id_length: Some(5),
            }),
        };

        let json = serde_json::to_string(&request).unwrap();
        let deserialized: BatchUploadRequest = serde_json::from_str(&json).unwrap();

        assert_eq!(request.api_key, deserialized.api_key);
        assert_eq!(request.events.len(), deserialized.events.len());
    }

    #[test]
    fn test_error_response_deserialization() {
        let error_json = r#"{
            "code": 429,
            "error": "Too many requests for some devices and users",
            "eps_threshold": 1000,
            "throttled_devices": {
                "C8F9E604-F01A-4BD9-95C6-8E5357DF265D": 4000
            },
            "throttled_events": [3, 4, 7]
        }"#;

        let error: TooManyRequestsError = serde_json::from_str(error_json).unwrap();

        assert_eq!(error.code, 429);
        assert_eq!(error.eps_threshold, 1000);
        assert!(error.throttled_devices.is_some());
        assert!(error.throttled_events.is_some());
    }

    #[test]
    fn test_export_event_conversion() {
        let export_json = r#"{"$insert_id":"Property Drop Purchased:2","$insert_key":null,"$schema":null,"adid":null,"amplitude_attribution_ids":null,"amplitude_event_type":null,"amplitude_id":1263130950579,"app":636686,"city":null,"client_event_time":"2025-07-01 16:34:54.837000","client_upload_time":"2025-07-01 17:14:33.145000","country":null,"data":{"path":"/2/httpapi","user_properties_updated":true,"group_first_event":{},"group_ids":{}},"data_type":"event","device_brand":null,"device_carrier":null,"device_family":null,"device_id":"f48913e6-c106-5429-a296-9f9588859c3b","device_manufacturer":null,"device_model":null,"device_type":null,"dma":null,"event_id":991179682,"event_properties":{"Total Price":6396.0,"Drop Id":2,"Number of Shares":20,"Drop Type":"Sale","Price per Share":312.0,"Property":"Property 0"},"event_time":"2025-07-01 16:34:54.837000","event_type":"Property Drop Purchased","global_user_properties":null,"group_properties":{},"groups":{},"idfa":null,"ip_address":null,"is_attribution_event":null,"language":null,"library":"http/2.0","location_lat":null,"location_lng":null,"os_name":null,"os_version":null,"partner_id":null,"paying":null,"plan":{},"platform":null,"processed_time":"2025-07-01 17:14:33.693000","region":null,"sample_rate":null,"server_received_time":"2025-07-01 17:14:33.145000","server_upload_time":"2025-07-01 17:14:33.149000","session_id":-1,"source_id":null,"start_version":null,"user_creation_time":null,"user_id":"2b59c518-dc2b-43f6-9444-85a84d5b1e1f","user_properties":{},"uuid":"a6ec45f6-0016-4907-b433-5bf4e4a56908","version_name":null}"#;

        let export_event: ExportEvent = serde_json::from_str(export_json).unwrap();
        let batch_event = export_event.to_batch_event().unwrap();

        assert_eq!(
            batch_event.user_id,
            Some("2b59c518-dc2b-43f6-9444-85a84d5b1e1f".to_string())
        );
        assert_eq!(
            batch_event.device_id,
            Some("f48913e6-c106-5429-a296-9f9588859c3b".to_string())
        );
        assert_eq!(batch_event.event_type, "Property Drop Purchased");
        assert_eq!(batch_event.event_id, Some(991179682));
        assert_eq!(batch_event.session_id, Some(-1));
        assert_eq!(
            batch_event.insert_id,
            Some("Property Drop Purchased:2".to_string())
        );

        // Check that event properties are preserved
        let event_props = batch_event.event_properties.as_ref().unwrap();
        assert_eq!(event_props.get("Total Price"), Some(&Value::from(6396.0)));
        assert_eq!(event_props.get("Drop Id"), Some(&Value::from(2)));
        assert_eq!(event_props.get("Number of Shares"), Some(&Value::from(20)));
        assert_eq!(event_props.get("Drop Type"), Some(&Value::from("Sale")));
        assert_eq!(
            event_props.get("Price per Share"),
            Some(&Value::from(312.0))
        );
        assert_eq!(
            event_props.get("Property"),
            Some(&Value::from("Property 0"))
        );

        // Check that time is converted to milliseconds since epoch
        // The event_time is now already a DateTime<Utc> from serde deserialization
        let expected_time = export_event.event_time.unwrap().timestamp_millis();
        assert_eq!(
            batch_event.time, expected_time,
            "Timestamps do not match: batch_event.time = {}, expected_time = {}",
            batch_event.time, expected_time
        );
    }

    #[test]
    fn test_round_trip_json_conversion() {
        use std::fs;
        use std::io::{BufRead, BufReader};

        // Read the test-round-trip.json file
        let file_content = fs::read_to_string("test-round-trip.json")
            .expect("Failed to read test-round-trip.json");

        let reader = BufReader::new(file_content.as_bytes());
        let mut original_lines = Vec::new();
        let mut export_events = Vec::new();

        // Parse each line as a separate JSON object
        for line in reader.lines() {
            let line = line.expect("Failed to read line");
            if line.trim().is_empty() {
                continue;
            }

            original_lines.push(line.clone());

            // Deserialize the export event
            let export_event: ExportEvent = serde_json::from_str(&line)
                .unwrap_or_else(|_| panic!("Failed to deserialize export event: {}", line));

            export_events.push(export_event);
        }

        // Verify we parsed the expected number of events
        assert_eq!(export_events.len(), 13, "Expected 13 events in test file");

        // Test round-trip conversion for each event: JSON -> ExportEvent -> JSON
        for (i, (original_line, export_event)) in
            original_lines.iter().zip(export_events.iter()).enumerate()
        {
            // Serialize the export event back to JSON
            let round_trip_json = serde_json::to_string(export_event)
                .unwrap_or_else(|_| panic!("Failed to serialize export event {}", i));

            // Normalize both JSONs for comparison by parsing and re-serializing
            let original_normalized: serde_json::Value = serde_json::from_str(original_line)
                .unwrap_or_else(|_| panic!("Failed to parse original JSON for event {}", i));
            let round_trip_normalized: serde_json::Value = serde_json::from_str(&round_trip_json)
                .unwrap_or_else(|_| panic!("Failed to parse round-trip JSON for event {}", i));

            // Compare the normalized JSONs
            if original_normalized != round_trip_normalized {
                // Create a detailed diff for better debugging
                let original_obj = original_normalized.as_object().unwrap();
                let round_trip_obj = round_trip_normalized.as_object().unwrap();

                let mut differences = Vec::new();

                // Check for fields present in original but missing or different in round-trip
                for (key, original_value) in original_obj {
                    match round_trip_obj.get(key) {
                        Some(round_trip_value) => {
                            if original_value != round_trip_value {
                                differences.push(format!(
                                    "Field '{}': original = {:?}, round_trip = {:?}",
                                    key, original_value, round_trip_value
                                ));
                            }
                        }
                        None => {
                            differences.push(format!(
                                "Field '{}': present in original ({:?}) but missing in round_trip",
                                key, original_value
                            ));
                        }
                    }
                }

                // Check for fields present in round-trip but missing in original
                for (key, round_trip_value) in round_trip_obj {
                    if !original_obj.contains_key(key) {
                        differences.push(format!(
                            "Field '{}': missing in original but present in round_trip ({:?})",
                            key, round_trip_value
                        ));
                    }
                }

                // Create a detailed error message
                let error_msg = format!(
                    "JSON mismatch for event {}:\nDifferences:\n{}\n\nOriginal JSON: {}\n\nRound-trip JSON: {}",
                    i,
                    differences.join("\n"),
                    serde_json::to_string_pretty(&original_normalized).unwrap(),
                    serde_json::to_string_pretty(&round_trip_normalized).unwrap()
                );

                panic!("{}", error_msg);
            }
        }

        println!(
            "Successfully completed round-trip JSON conversion test for {} events",
            export_events.len()
        );
    }

    #[test]
    fn test_uuid_deduplication_filter() {
        let mut filter = UUIDDeduplicationFilter::new();
        
        // Create test events
        let uuid_event = ExportEvent {
            insert_id: Some("550e8400-e29b-41d4-a716-446655440000".to_string()), // Valid UUID
            event_type: Some("test_event".to_string()),
            ..Default::default()
        };
        
        let non_uuid_event1 = ExportEvent {
            insert_id: Some("non-uuid-id-1".to_string()), // Not a UUID
            event_type: Some("test_event".to_string()),
            ..Default::default()
        };
        
        let non_uuid_event2 = ExportEvent {
            insert_id: Some("non-uuid-id-1".to_string()), // Same non-UUID ID
            event_type: Some("test_event".to_string()),
            ..Default::default()
        };
        
        let non_uuid_event3 = ExportEvent {
            insert_id: Some("non-uuid-id-2".to_string()), // Different non-UUID ID
            event_type: Some("test_event".to_string()),
            ..Default::default()
        };
        
        // Test UUID event - should always be included
        assert!(filter.should_include(&uuid_event));
        assert!(filter.should_include(&uuid_event)); // Even if called multiple times
        
        // Test first non-UUID event - should be included
        assert!(filter.should_include(&non_uuid_event1));
        
        // Test second non-UUID event with same ID - should be filtered out
        assert!(!filter.should_include(&non_uuid_event2));
        
        // Test third non-UUID event with different ID - should be included
        assert!(filter.should_include(&non_uuid_event3));
        
        // Test events without insert_id - should be filtered out
        let no_insert_id_event = ExportEvent {
            insert_id: None,
            event_type: Some("test_event".to_string()),
            ..Default::default()
        };
        assert!(!filter.should_include(&no_insert_id_event));
        
        let empty_insert_id_event = ExportEvent {
            insert_id: Some("".to_string()),
            event_type: Some("test_event".to_string()),
            ..Default::default()
        };
        assert!(!filter.should_include(&empty_insert_id_event));
        
        // Check stats
        let (total_non_uuid, unique_ids) = filter.get_stats();
        assert_eq!(total_non_uuid, 3); // non_uuid_event1, non_uuid_event2, non_uuid_event3
        assert_eq!(unique_ids, 2); // "non-uuid-id-1", "non-uuid-id-2"
    }

    #[test]
    fn test_export_event_partial_eq_ignores_specified_fields() {
        // Create two ExportEvent instances that are identical except for the fields
        // that should be ignored by PartialEq
        let base_event = ExportEvent {
            insert_id: Some("test-insert-id".to_string()),
            event_type: Some("test_event".to_string()),
            user_id: Some("test_user".to_string()),
            device_id: Some("test_device".to_string()),
            event_time: Some(DateTime::parse_from_rfc3339("2023-01-01T00:00:00Z").unwrap().with_timezone(&Utc)),
            event_properties: Some({
                let mut map = HashMap::new();
                map.insert("key1".to_string(), Value::String("value1".to_string()));
                map
            }),
            ..Default::default()
        };

        // Create a modified event with different values for the ignored fields
        let mut modified_event = base_event.clone();
        modified_event.app = Some(999); // Different app ID
        modified_event.client_upload_time = Some(DateTime::parse_from_rfc3339("2023-01-02T00:00:00Z").unwrap().with_timezone(&Utc)); // Different upload time
        modified_event.processed_time = Some("2023-01-02T00:00:00Z".to_string()); // Different processed time
        modified_event.server_received_time = Some(DateTime::parse_from_rfc3339("2023-01-03T00:00:00Z").unwrap().with_timezone(&Utc)); // Different server time
        modified_event.uuid = Some("different-uuid".to_string()); // Different UUID
        modified_event.user_properties = Some({
            let mut map = HashMap::new();
            map.insert("different_key".to_string(), Value::String("different_value".to_string()));
            map
        }); // Different user properties

        // The events should be equal despite the differences in ignored fields
        assert_eq!(base_event, modified_event, "ExportEvent instances should be equal when only ignored fields differ");

        // Now test that changing a non-ignored field makes them unequal
        let mut different_event = base_event.clone();
        different_event.event_type = Some("different_event".to_string()); // Change a non-ignored field
        
        assert_ne!(base_event, different_event, "ExportEvent instances should be unequal when non-ignored fields differ");

        // Test that changing insert_id (a non-ignored field) makes them unequal
        let mut different_insert_id = base_event.clone();
        different_insert_id.insert_id = Some("different-insert-id".to_string());
        
        assert_ne!(base_event, different_insert_id, "ExportEvent instances should be unequal when insert_id differs");

        // Test that changing user_id (a non-ignored field) makes them unequal
        let mut different_user_id = base_event.clone();
        different_user_id.user_id = Some("different_user".to_string());
        
        assert_ne!(base_event, different_user_id, "ExportEvent instances should be unequal when user_id differs");
    }
}
