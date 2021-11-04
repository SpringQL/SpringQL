use crate::model::name::{PumpName, StreamName};

impl StreamName {
    pub(crate) fn factory(name: &str) -> Self {
        Self::new(name.to_string())
    }
}

impl PumpName {
    pub(crate) fn factory(name: &str) -> Self {
        Self::new(name.to_string())
    }
}
