// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use parking_lot::RwLock;

use crate::{api::error::Result, pipeline::name::SinkWriterName};
use crate::{
    api::low_level_rs::SpringSinkWriterConfig, pipeline::sink_writer_model::SinkWriterModel,
    stream_engine::autonomous_executor::task::sink_task::sink_writer::sink_writer_factory::SinkWriterFactory,
};

use super::SinkWriter;

#[allow(clippy::type_complexity)]
#[derive(Debug)]
pub(in crate::stream_engine) struct SinkWriterRepository {
    config: SpringSinkWriterConfig,

    sinks: RwLock<HashMap<SinkWriterName, Arc<Mutex<Box<dyn SinkWriter>>>>>,
}

impl SinkWriterRepository {
    pub(in crate::stream_engine::autonomous_executor) fn new(
        config: SpringSinkWriterConfig,
    ) -> Self {
        Self {
            config,
            sinks: RwLock::default(),
        }
    }

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
        let mut sinks = self.sinks.write();

        if sinks.get(sink_writer.name()).is_some() {
            Ok(())
        } else {
            let subtask = SinkWriterFactory::sink(
                sink_writer.sink_writer_type(),
                sink_writer.options(),
                &self.config,
            )?;
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
            .get(name)
            .unwrap_or_else(|| panic!("sink name ({}) not registered yet", name))
            .clone()
    }
}
