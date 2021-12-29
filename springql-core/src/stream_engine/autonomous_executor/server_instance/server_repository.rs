// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex, RwLock},
};

use super::{sink::SinkWriterInstance, source::SourceServerInstance};
use crate::{error::Result, pipeline::name::ServerName};
use crate::{
    pipeline::server_model::ServerModel,
    stream_engine::autonomous_executor::server_instance::server_instance_factory::ServerInstanceFactory,
};

#[allow(clippy::type_complexity)]
#[derive(Debug, Default)]
pub(in crate::stream_engine) struct ServerRepository {
    sources: RwLock<HashMap<ServerName, Arc<Mutex<Box<dyn SourceServerInstance>>>>>,
    sinks: RwLock<HashMap<ServerName, Arc<Mutex<Box<dyn SinkWriterInstance>>>>>,
}

impl ServerRepository {
    /// Do nothing if a server with the same name already exists.
    ///
    /// # Failures
    ///
    /// - [SpringError::ForeignIo](crate::error::SpringError::ForeignIo) when:
    ///   - failed to start server.
    pub(in crate::stream_engine::autonomous_executor) fn register(
        &self,
        server_model: &ServerModel,
    ) -> Result<()> {
        let mut sources = self
            .sources
            .write()
            .expect("another thread sharing the same internal got panic");
        let mut sinks = self
            .sinks
            .write()
            .expect("another thread sharing the same internal got panic");

        if server_model.server_type().is_source() {
            if sources.get(server_model.name()).is_some() {
                Ok(())
            } else {
                let server = ServerInstanceFactory::source(
                    server_model.server_type(),
                    server_model.options(),
                )?;
                let server = Arc::new(Mutex::new(server as Box<dyn SourceServerInstance>));
                let _ = sources.insert(server_model.name().clone(), server);
                log::debug!(
                    "[ServerRepository] registered source server: {}",
                    server_model.name()
                );
                Ok(())
            }
        } else if server_model.server_type().is_sink() {
            if sinks.get(server_model.name()).is_some() {
                Ok(())
            } else {
                let server = ServerInstanceFactory::sink(
                    server_model.server_type(),
                    server_model.options(),
                )?;
                let server = Arc::new(Mutex::new(server as Box<dyn SinkWriterInstance>));
                let _ = sinks.insert(server_model.name().clone(), server);
                log::debug!(
                    "[ServerRepository] registered sink server: {}",
                    server_model.name()
                );
                Ok(())
            }
        } else {
            unreachable!()
        }
    }

    /// # Panics
    ///
    /// `server_name` is not registered yet
    pub(in crate::stream_engine::autonomous_executor) fn get_source_server(
        &self,
        server_name: &ServerName,
    ) -> Arc<Mutex<Box<dyn SourceServerInstance>>> {
        self.sources
            .read()
            .expect("another thread sharing the same internal got panic")
            .get(server_name)
            .unwrap_or_else(|| panic!("server name ({}) not registered yet", server_name))
            .clone()
    }

    /// # Panics
    ///
    /// `server_name` is not registered yet
    pub(in crate::stream_engine::autonomous_executor) fn get_sink_server(
        &self,
        server_name: &ServerName,
    ) -> Arc<Mutex<Box<dyn SinkWriterInstance>>> {
        self.sinks
            .read()
            .expect("another thread sharing the same internal got panic")
            .get(server_name)
            .unwrap_or_else(|| panic!("server name ({}) not registered yet", server_name))
            .clone()
    }
}
