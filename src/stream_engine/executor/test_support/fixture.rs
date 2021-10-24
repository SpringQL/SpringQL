use serde_json::json;

use crate::{
    stream_engine::executor::data::foreign_input_row::format::json::JsonObject,
    timestamp::Timestamp,
};

impl JsonObject {
    pub fn fx_tokyo(ts: Timestamp) -> Self {
        JsonObject::new(json!({
            "timestamp": ts.to_string(),
            "city": "Tokyo",
            "temparture": 21,
        }))
    }

    pub fn fx_osaka(ts: Timestamp) -> Self {
        JsonObject::new(json!({
            "timestamp": ts.to_string(),
            "city": "Osaka",
            "temparture": 23,
        }))
    }

    pub fn fx_london(ts: Timestamp) -> Self {
        JsonObject::new(json!({
            "timestamp": ts.to_string(),
            "city": "London",
            "temparture": 13,
        }))
    }
}
