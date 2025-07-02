use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

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

/// Individual event in the batch upload
#[derive(Debug, Serialize, Deserialize)]
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
    
    /// Optional. A map of event properties.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event_properties: Option<HashMap<String, Value>>,
    
    /// Optional. A map of user properties.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_properties: Option<HashMap<String, Value>>,
    
    /// Optional. A map of group properties.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub groups: Option<HashMap<String, Value>>,
    
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
    
    /// Optional. The IDFA (Identifier for Advertisers).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub idfa: Option<String>,
    
    /// Optional. The IDFV (Identifier for Vendors).
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json;

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
} 