// TODO remove (each source task should hold ownership)

// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex, RwLock},
};

use crate::{
    error::Result,
    pipeline::{name::SourceReaderName, source_reader::SourceReader},
    stream_engine::autonomous_executor::task::source_task::source_subtask::source_subtask_factory::SourceSubtaskFactory,
};

use super::SourceSubtask;

#[allow(clippy::type_complexity)]
#[derive(Debug, Default)]
pub(in crate::stream_engine) struct SourceSubtaskRepository {
    sources: RwLock<HashMap<SourceReaderName, Arc<Mutex<Box<dyn SourceSubtask>>>>>,
}

impl SourceSubtaskRepository {
    /// Do nothing if a server with the same name already exists.
    ///
    /// # Failures
    ///
    /// - [SpringError::ForeignIo](crate::error::SpringError::ForeignIo) when:
    ///   - failed to start server.
    pub(in crate::stream_engine::autonomous_executor) fn register(
        &self,
        source_reader: &SourceReader,
    ) -> Result<()> {
        let mut sources = self
            .sources
            .write()
            .expect("another thread sharing the same internal got panic");

        if sources.get(source_reader.name()).is_some() {
            Ok(())
        } else {
            let server =
                SourceSubtaskFactory::source(source_reader.server_type(), source_reader.options())?;
            let server = Arc::new(Mutex::new(server as Box<dyn SourceSubtask>));
            let _ = sources.insert(source_reader.name().clone(), server);
            log::debug!(
                "[SourceSubtaskRepository] registered source server: {}",
                source_reader.name()
            );
            Ok(())
        }
    }

    /// # Panics
    ///
    /// `server_name` is not registered yet
    pub(in crate::stream_engine::autonomous_executor) fn get_source_server(
        &self,
        server_name: &SourceReaderName,
    ) -> Arc<Mutex<Box<dyn SourceSubtask>>> {
        self.sources
            .read()
            .expect("another thread sharing the same internal got panic")
            .get(server_name)
            .unwrap_or_else(|| panic!("server name ({}) not registered yet", server_name))
            .clone()
    }
}
