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

#[allow(clippy::type_complexity)]
#[derive(Debug, Default)]
pub(in crate::stream_engine) struct ServerRepository {
    sources: RwLock<HashMap<ServerName, Arc<Mutex<Box<dyn SourceServerActive>>>>>,
    sinks: RwLock<HashMap<ServerName, Arc<Mutex<Box<dyn SinkServerActive>>>>>,
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

        match server_model.server_type() {
            ServerType::SourceNet => {
                if sources.get(server_model.name()).is_some() {
                    Ok(())
                } else {
                    let server = NetSourceServerStandby::new(server_model.options())?;
                    let server = server.start()?;
                    let server = Box::new(server);
                    let server = Arc::new(Mutex::new(server as Box<dyn SourceServerActive>));
                    let _ = sources.insert(server_model.name().clone(), server);
                    log::debug!(
                        "[ServerRepository] registered source server: {}",
                        server_model.name()
                    );
                    Ok(())
                }
            }
            ServerType::SinkNet => {
                if sinks.get(server_model.name()).is_some() {
                    Ok(())
                } else {
                    let server = NetSinkServerStandby::new(server_model.options())?;
                    let server = server.start()?;
                    let server = Box::new(server);
                    let server = Arc::new(Mutex::new(server as Box<dyn SinkServerActive>));
                    let _ = sinks.insert(server_model.name().clone(), server);
                    log::debug!(
                        "[ServerRepository] registered sink server: {}",
                        server_model.name()
                    );
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
    ) -> Arc<Mutex<Box<dyn SinkServerActive>>> {
        self.sinks
            .read()
            .expect("another thread sharing the same internal got panic")
            .get(server_name)
            .unwrap_or_else(|| panic!("server name ({}) not registered yet", server_name))
            .clone()
    }
}
