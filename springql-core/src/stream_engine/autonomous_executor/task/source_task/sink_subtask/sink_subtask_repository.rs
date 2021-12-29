// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex, RwLock},
};

use crate::{error::Result, pipeline::name::SinkWriterName};
use crate::{
    pipeline::sink_writer::SinkWriter,
    stream_engine::autonomous_executor::task::source_task::sink_subtask::sink_subtask_factory::SinkSubtaskFactory,
};

use super::SinkSubtask;

#[allow(clippy::type_complexity)]
#[derive(Debug, Default)]
pub(in crate::stream_engine) struct SinkSubtaskRepository {
    sinks: RwLock<HashMap<SinkWriterName, Arc<Mutex<Box<dyn SinkSubtask>>>>>,
}

impl SinkSubtaskRepository {
    /// Do nothing if a sink writer with the same name already exists.
    ///
    /// # Failures
    ///
    /// - [SpringError::ForeignIo](crate::error::SpringError::ForeignIo) when:
    ///   - failed to start subtask.
    pub(in crate::stream_engine::autonomous_executor) fn register(
        &self,
        sink_writer: &SinkWriter,
    ) -> Result<()> {
        let mut sinks = self
            .sinks
            .write()
            .expect("another thread sharing the same internal got panic");

        if sinks.get(sink_writer.name()).is_some() {
            Ok(())
        } else {
            let subtask =
                SinkSubtaskFactory::sink(sink_writer.sink_writer_type(), sink_writer.options())?;
            let subtask = Arc::new(Mutex::new(subtask as Box<dyn SinkSubtask>));
            let _ = sinks.insert(sink_writer.name().clone(), subtask);
            log::debug!(
                "[SinkSubtaskRepository] registered sink subtask: {}",
                sink_writer.name()
            );
            Ok(())
        }
    }

    /// # Panics
    ///
    /// `name` is not registered yet
    pub(in crate::stream_engine::autonomous_executor) fn get_sink_subtask(
        &self,
        name: &SinkWriterName,
    ) -> Arc<Mutex<Box<dyn SinkSubtask>>> {
        self.sinks
            .read()
            .expect("another thread sharing the same internal got panic")
            .get(name)
            .unwrap_or_else(|| panic!("sink name ({}) not registered yet", name))
            .clone()
    }
}
