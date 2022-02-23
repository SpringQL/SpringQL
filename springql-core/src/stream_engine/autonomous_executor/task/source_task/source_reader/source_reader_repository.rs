// Copyright (c) 2022 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex, RwLock},
};

use crate::{
    error::Result,
    low_level_rs::SpringSourceReaderConfig,
    pipeline::{name::SourceReaderName, source_reader_model::SourceReaderModel},
    stream_engine::autonomous_executor::task::source_task::source_reader::source_reader_factory::SourceReaderFactory,
};

use super::SourceReader;

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
    /// - [SpringError::ForeignIo](crate::error::SpringError::ForeignIo) when:
    ///   - failed to start subtask.
    pub(in crate::stream_engine::autonomous_executor) fn register(
        &self,
        source_reader: &SourceReaderModel,
    ) -> Result<()> {
        let mut sources = self
            .sources
            .write()
            .expect("another thread sharing the same internal got panic");

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
            .expect("another thread sharing the same internal got panic")
            .get(name)
            .unwrap_or_else(|| panic!("source reader name ({}) not registered yet", name))
            .clone()
    }
}
