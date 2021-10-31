//! - Pipeline
//!   - Stream
//!     - (native) Stream
//!     - Source/Sink Foreign Stream
//!   - Source/Sink Server
//!   - Pump

pub(crate) mod foreign_stream_model;
pub(crate) mod pump_model;
pub(crate) mod server_model;
pub(crate) mod stream_model;

use anyhow::anyhow;
use std::collections::{HashMap, HashSet};

use crate::{
    error::{Result, SpringError},
    model::name::{PumpName, StreamName},
};
use serde::{Deserialize, Serialize};

use self::{
    foreign_stream_model::ForeignStreamModel, pump_model::PumpModel, server_model::ServerModel,
};

#[derive(Eq, PartialEq, Debug, Default, Serialize, Deserialize)]
pub(super) struct Pipeline {
    object_names: HashSet<String>,
    pumps: HashMap<PumpName, PumpModel>,
    foreign_streams: HashMap<StreamName, ForeignStreamModel>,
    servers_serving_to: HashMap<StreamName, ServerModel>,
}

impl Pipeline {
    /// # Failure
    ///
    /// - [SpringError::Sql](crate::error::SpringError::Sql) when:
    ///   - Name of pump is already used in the same pipeline
    ///   - Name of upstream stream is not found in pipeline
    ///   - Name of downstream stream is not found in pipeline
    pub(super) fn add_pump(&mut self, pump: PumpModel) -> Result<()> {
        self.register_name(pump.name().as_ref())?;

        self.object_names
            .get(pump.upstream().as_ref())
            .ok_or_else(|| {
                SpringError::Sql(anyhow!(
                    r#"upstream "{}" does not exist in pipeline"#,
                    pump.upstream()
                ))
            })?;
        self.object_names
            .get(pump.downstream().as_ref())
            .ok_or_else(|| {
                SpringError::Sql(anyhow!(
                    r#"downstream "{}" does not exist in pipeline"#,
                    pump.downstream()
                ))
            })?;

        let _ = self.pumps.insert(pump.name().clone(), pump);
        Ok(())
    }

    /// # Failure
    ///
    /// - [SpringError::Sql](crate::error::SpringError::Sql) when:
    ///   - Name of foreign stream is already used in the same pipeline
    pub(super) fn add_foreign_stream(&mut self, foreign_stream: ForeignStreamModel) -> Result<()> {
        self.register_name(foreign_stream.name().as_ref())?;

        let _ = self
            .foreign_streams
            .insert(foreign_stream.name().clone(), foreign_stream);
        Ok(())
    }

    /// # Failure
    ///
    /// TODO
    pub(super) fn add_server(&mut self, server: ServerModel) -> Result<()> {
        let serving_to = server.serving_foreign_stream();
        let _ = self.servers_serving_to.insert(serving_to.clone(), server);
        Ok(())
    }

    /// # Failure
    ///
    /// - [SpringError::Sql](crate::error::SpringError::Sql) when:
    ///   - Name is already used in the same pipeline
    fn register_name(&mut self, name: &str) -> Result<()> {
        if !self.object_names.insert(name.to_string()) {
            Err(SpringError::Sql(anyhow!(
                r#"name "{}" already exists in pipeline"#,
                name
            )))
        } else {
            Ok(())
        }
    }
}
