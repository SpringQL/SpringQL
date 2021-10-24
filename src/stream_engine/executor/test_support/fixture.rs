use serde_json::json;

use crate::stream_engine::executor::data::{
    foreign_input_row::format::json::JsonObject, timestamp::Timestamp,
};

impl Timestamp {
    pub fn fx_ts1() -> Self {
        "2021-01-01 13:00:00.000000001".parse().unwrap()
    }
    pub fn fx_ts2() -> Self {
        "2021-01-01 13:00:00.000000002".parse().unwrap()
    }
    pub fn fx_ts3() -> Self {
        "2021-01-01 13:00:00.000000003".parse().unwrap()
    }
}

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
