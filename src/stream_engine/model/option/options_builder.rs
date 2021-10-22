use std::collections::HashMap;

use super::Options;

#[derive(Default, Debug)]
pub(in crate::stream_engine) struct OptionsBuilder(HashMap<String, String>);

impl OptionsBuilder {
    pub(in crate::stream_engine) fn add<SK, SV>(mut self, key: SK, value: SV) -> Self
    where
        SK: Into<String>,
        SV: Into<String>,
    {
        let k = key.into();
        let v = value.into();
        self.0.insert(k, v);
        Self(self.0)
    }

    pub(in crate::stream_engine) fn build(self) -> Options {
        Options(self.0)
    }
}
