mod repository;

use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub(in crate::stream_engine) struct StreamModelRef;

impl StreamModelRef {
    fn get(&self) -> &StreamModel {
        unimplemented!()
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub(in crate::stream_engine) struct StreamModel;
