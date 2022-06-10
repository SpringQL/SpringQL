// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use parking_lot::RwLock;

use crate::{
    api::{error::Result, SpringSourceReaderConfig},
    pipeline::{SourceReaderModel, SourceReaderName},
    stream_engine::autonomous_executor::task::source_task::source_reader::{
        source_reader_factory::SourceReaderFactory, SourceReader,
    },
};

#[allow(clippy::type_complexity)]
#[derive(Debug)]
pub(in crate::stream_engine) struct SourceReaderRepository {
    config: SpringSourceReaderConfig,

    sources: RwLock<HashMap<SourceReaderName, Arc<Mutex<Box<dyn SourceReader>>>>>,
}

impl SourceReaderRepository {
    pub(in crate::stream_engine::autonomous_executor) fn new(
        config: SpringSourceReaderConfig,
    ) -> Self {
        Self {
            config,
            sources: RwLock::default(),
        }
    }

    /// Do nothing if a source reader with the same name already exists.
    ///
    /// # Failures
    ///
    /// - `SpringError::ForeignIo` when:
    ///   - failed to start subtask.
    pub(in crate::stream_engine::autonomous_executor) fn register(
        &self,
        source_reader: &SourceReaderModel,
    ) -> Result<()> {
        let mut sources = self.sources.write();

        if sources.get(source_reader.name()).is_some() {
            Ok(())
        } else {
            let subtask = SourceReaderFactory::source(
                source_reader.source_reader_type(),
                source_reader.options(),
                &self.config,
            )?;
            let subtask = Arc::new(Mutex::new(subtask as Box<dyn SourceReader>));
            let _ = sources.insert(source_reader.name().clone(), subtask);
            log::debug!(
                "[SourceReaderRepository] registered source subtask: {}",
                source_reader.name()
            );
            Ok(())
        }
    }

    /// # Panics
    ///
    /// `name` is not registered yet
    pub(in crate::stream_engine::autonomous_executor) fn get_source_reader(
        &self,
        name: &SourceReaderName,
    ) -> Arc<Mutex<Box<dyn SourceReader>>> {
        self.sources
            .read()
            .get(name)
            .unwrap_or_else(|| panic!("source reader name ({}) not registered yet", name))
            .clone()
    }
}
