use std::{
    collections::HashMap,
    sync::{Arc, Mutex, RwLock},
};

use super::{
    sink::{net::NetSinkServerStandby, SinkServerActive, SinkServerStandby},
    source::{SourceServerActive, SourceServerStandby},
};
use crate::error::Result;
use crate::{
    model::name::ServerName,
    stream_engine::{
        autonomous_executor::server::source::net::NetSourceServerStandby,
        pipeline::server_model::{server_type::ServerType, ServerModel},
    },
};

#[derive(Debug, Default)]
pub(in crate::stream_engine) struct ServerRepository {
    sources: RwLock<HashMap<ServerName, Arc<Mutex<Box<dyn SourceServerActive>>>>>,
    sinks: RwLock<HashMap<ServerName, Arc<Mutex<Box<dyn SinkServerActive>>>>>,
}

impl ServerRepository {
    /// # Failures
    ///
    /// - [SpringError::ForeignIo](crate::error::SpringError::ForeignIo) whem:
    ///   - failed to start server.
    ///
    /// # Panics
    ///
    /// Same named server_model is already registered
    pub(in crate::stream_engine::autonomous_executor) fn register(
        &self,
        server_model: &ServerModel,
    ) -> Result<()> {
        match server_model.server_type() {
            ServerType::SourceNet => {
                let server = NetSourceServerStandby::new(server_model.options())?;
                let server = server.start()?;
                let server = Box::new(server);
                let server = Arc::new(Mutex::new(server as Box<dyn SourceServerActive>));
                if self
                    .sources
                    .write()
                    .expect("another thread sharing the same internal got panic")
                    .insert(server_model.name().clone(), server)
                    .is_some()
                {
                    panic!("server_model with same name is already registered");
                } else {
                    Ok(())
                }
            }
            ServerType::SinkNet => {
                let server = NetSinkServerStandby::new(server_model.options())?;
                let server = server.start()?;
                let server = Box::new(server);
                let server = Arc::new(Mutex::new(server as Box<dyn SinkServerActive>));
                if self
                    .sinks
                    .write()
                    .expect("another thread sharing the same internal got panic")
                    .insert(server_model.name().clone(), server)
                    .is_some()
                {
                    panic!("server_model with same name is already registered");
                } else {
                    Ok(())
                }
            }
        }
    }

    /// # Panics
    ///
    /// `server_name` is not registered yet
    pub(in crate::stream_engine::autonomous_executor) fn get_source_server(
        &self,
        server_name: &ServerName,
    ) -> Arc<Mutex<Box<dyn SourceServerActive>>> {
        todo!()
    }

    /// # Panics
    ///
    /// `server_name` is not registered yet
    pub(in crate::stream_engine::autonomous_executor) fn get_sink_server(
        &self,
        server_name: &ServerName,
    ) -> Arc<Mutex<Box<dyn SinkServerActive>>> {
        todo!()
    }
}
