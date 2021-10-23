#[derive(Clone, Eq, PartialEq, Debug)]
pub(in crate::stream_engine) struct JsonObject(serde_json::Value);

impl ToString for JsonObject {
    fn to_string(&self) -> String {
        self.0.to_string()
    }
}

impl JsonObject {
    pub(in crate::stream_engine) fn new(value: serde_json::Value) -> Self {
        Self(value)
    }
}
