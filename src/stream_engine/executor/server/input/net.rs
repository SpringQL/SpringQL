use std::net::IpAddr;

use anyhow::Context;

use crate::{
    error::{Result, SpringError},
    stream_engine::{
        executor::foreign_input_row::foreign_input_row_chunk::ForeignInputRowChunk,
        model::{option::Options, server_model::ServerModel},
    },
};

use super::{InputServerActive, InputServerStandby};

#[derive(Debug)]
enum Protocol {
    Tcp,
}

#[derive(Debug)]
pub(in crate::stream_engine::executor) struct NetInputServerStandby {
    protocol: Protocol,
    remote_host: IpAddr,
    remote_port: u16,
}

#[derive(Debug)]
pub(in crate::stream_engine::executor) struct NetInputServerActive {}

impl InputServerStandby<NetInputServerActive> for NetInputServerStandby {
    fn new(options: Options) -> Result<Self>
    where
        Self: Sized,
    {
        Ok(Self {
            protocol: options.get("PROTOCOL", |protocol_str| {
                (protocol_str == "TCP")
                    .then(|| Protocol::Tcp)
                    .context("unsupported protocol")
            })?,
            remote_host: options.get("REMOTE_HOST", |remote_host_str| {
                remote_host_str.parse().context("invalid remote host")
            })?,
            remote_port: options.get("REMOTE_PORT", |remote_port_str| {
                remote_port_str.parse().context("invalid remote port")
            })?,
        })
    }

    fn start(self) -> Result<NetInputServerActive> {
        todo!()
    }
}

impl InputServerActive for NetInputServerActive {
    fn next_chunk(&self) -> Result<ForeignInputRowChunk> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stream_engine::executor::foreign_input_row::ForeignInputRow;
    use crate::stream_engine::model::{
        option::options_builder::OptionsBuilder,
        server_model::{server_type::ServerType, ServerModel},
    };

    const REMOTE_PORT: u16 = 17890;

    // TODO JSON to socket

    #[test]
    fn test_input_server_tcp() -> crate::error::Result<()> {
        let options = OptionsBuilder::default()
            .add("PROTOCOL", "TCP")
            .add("REMOTE_HOST", "127.0.0.1")
            .add("REMOTE_PORT", REMOTE_PORT.to_string())
            .build();

        let server = NetInputServerStandby::new(options)?;
        let server = server.start()?;

        let mut row_chunk = server.next_chunk()?;
        assert_eq!(row_chunk.next(), Some(ForeignInputRow::fx_tokyo()));
        assert_eq!(row_chunk.next(), Some(ForeignInputRow::fx_osaka()));
        assert_eq!(row_chunk.next(), Some(ForeignInputRow::fx_london()));
        assert_eq!(row_chunk.next(), None);

        Ok(())
    }
}
