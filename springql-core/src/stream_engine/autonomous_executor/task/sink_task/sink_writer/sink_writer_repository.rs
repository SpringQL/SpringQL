// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex, RwLock},
};

use crate::{pipeline::sink_writer_model::SinkWriterModel, stream_engine::autonomous_executor::task::sink_task::sink_writer::sink_writer_factory::SinkWriterFactory};
use crate::{error::Result, pipeline::name::SinkWriterName};

use super::SinkWriter;

#[allow(clippy::type_complexity)]
#[derive(Debug, Default)]
pub(in crate::stream_engine) struct SinkWriterRepository {
    sinks: RwLock<HashMap<SinkWriterName, Arc<Mutex<Box<dyn SinkWriter>>>>>,
}

impl SinkWriterRepository {
    /// Do nothing if a sink writer with the same name already exists.
    ///
    /// # Failures
    ///
    /// - [SpringError::ForeignIo](crate::error::SpringError::ForeignIo) when:
    ///   - failed to start subtask.
    pub(in crate::stream_engine::autonomous_executor) fn register(
        &self,
        sink_writer: &SinkWriterModel,
    ) -> Result<()> {
        let mut sinks = self
            .sinks
            .write()
            .expect("another thread sharing the same internal got panic");

        if sinks.get(sink_writer.name()).is_some() {
            Ok(())
        } else {
            let subtask =
                SinkWriterFactory::sink(sink_writer.sink_writer_type(), sink_writer.options())?;
            let subtask = Arc::new(Mutex::new(subtask as Box<dyn SinkWriter>));
            let _ = sinks.insert(sink_writer.name().clone(), subtask);
            log::debug!(
                "[SinkWriterRepository] registered sink subtask: {}",
                sink_writer.name()
            );
            Ok(())
        }
    }

    /// # Panics
    ///
    /// `name` is not registered yet
    pub(in crate::stream_engine::autonomous_executor) fn get_sink_writer(
        &self,
        name: &SinkWriterName,
    ) -> Arc<Mutex<Box<dyn SinkWriter>>> {
        self.sinks
            .read()
            .expect("another thread sharing the same internal got panic")
            .get(name)
            .unwrap_or_else(|| panic!("sink name ({}) not registered yet", name))
            .clone()
    }
}
