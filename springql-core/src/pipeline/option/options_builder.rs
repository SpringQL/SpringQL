// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use std::collections::HashMap;

use crate::pipeline::option::Options;

#[derive(Default, Debug)]
pub(crate) struct OptionsBuilder(HashMap<String, String>);

impl OptionsBuilder {
    pub(crate) fn add<SK, SV>(mut self, key: SK, value: SV) -> Self
    where
        SK: Into<String>,
        SV: Into<String>,
    {
        let k = key.into();
        let v = value.into();
        self.0.insert(k, v);
        Self(self.0)
    }

    pub(crate) fn build(self) -> Options {
        Options(self.0)
    }
}
