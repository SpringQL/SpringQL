// TODO remove (each source task should hold ownership)

// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex, RwLock},
};

use crate::{
    error::Result,
    pipeline::{name::SourceReaderName, source_reader_model::SourceReaderModel},
    stream_engine::autonomous_executor::task::source_task::source_subtask::source_subtask_factory::SourceSubtaskFactory,
};

use super::SourceSubtask;

#[allow(clippy::type_complexity)]
#[derive(Debug, Default)]
pub(in crate::stream_engine) struct SourceSubtaskRepository {
    sources: RwLock<HashMap<SourceReaderName, Arc<Mutex<Box<dyn SourceSubtask>>>>>,
}

impl SourceSubtaskRepository {
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
            let subtask = SourceSubtaskFactory::source(
                source_reader.source_reader_type(),
                source_reader.options(),
            )?;
            let subtask = Arc::new(Mutex::new(subtask as Box<dyn SourceSubtask>));
            let _ = sources.insert(source_reader.name().clone(), subtask);
            log::debug!(
                "[SourceSubtaskRepository] registered source subtask: {}",
                source_reader.name()
            );
            Ok(())
        }
    }

    /// # Panics
    ///
    /// `name` is not registered yet
    pub(in crate::stream_engine::autonomous_executor) fn get_source_subtask(
        &self,
        name: &SourceReaderName,
    ) -> Arc<Mutex<Box<dyn SourceSubtask>>> {
        self.sources
            .read()
            .expect("another thread sharing the same internal got panic")
            .get(name)
            .unwrap_or_else(|| panic!("source reader name ({}) not registered yet", name))
            .clone()
    }
}
