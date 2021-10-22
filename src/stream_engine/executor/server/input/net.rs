use std::{
    net::{IpAddr, SocketAddr, TcpStream},
    time::Duration,
};

use anyhow::Context;

use crate::{
    error::Result,
    stream_engine::{
        executor::foreign_input_row::foreign_input_row_chunk::ForeignInputRowChunk,
        model::option::Options,
    },
};

use super::{InputServerActive, InputServerStandby};

#[derive(Debug)]
enum Protocol {
    Tcp,
}

// TODO config
const CONNECT_TIMEOUT_SECS: u64 = 1;

#[derive(Debug)]
pub(in crate::stream_engine::executor) struct NetInputServerStandby {
    protocol: Protocol,
    remote_host: IpAddr,
    remote_port: u16,
}

#[derive(Debug)]
pub(in crate::stream_engine::executor) struct NetInputServerActive {
    conn_stream: TcpStream, // TODO UDP
}

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
        let sock_addr = SocketAddr::new(self.remote_host, self.remote_port);
        let conn_stream =
            TcpStream::connect_timeout(&sock_addr, Duration::from_secs(CONNECT_TIMEOUT_SECS))?;
        Ok(NetInputServerActive { conn_stream })
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
    use crate::stream_engine::executor::foreign_input_row::format::json::JsonObject;
    use crate::stream_engine::executor::foreign_input_row::ForeignInputRow;
    use crate::stream_engine::executor::test_support::foreign::source::TestSource;
    use crate::stream_engine::model::option::options_builder::OptionsBuilder;
    use crate::timestamp::Timestamp;

    #[test]
    fn test_input_server_tcp() -> crate::error::Result<()> {
        let j1 = JsonObject::fx_tokyo(Timestamp::fx_ts1());
        let j2 = JsonObject::fx_osaka(Timestamp::fx_ts2());
        let j3 = JsonObject::fx_london(Timestamp::fx_ts3());

        let source = TestSource::start(vec![j2.clone(), j3.clone(), j1.clone()])?;

        let options = OptionsBuilder::default()
            .add("PROTOCOL", "TCP")
            .add("REMOTE_HOST", "127.0.0.1")
            .add("REMOTE_PORT", source.port().to_string())
            .build();

        let server = NetInputServerStandby::new(options)?;
        let server = server.start()?;

        let mut row_chunk = server.next_chunk()?;
        assert_eq!(row_chunk.next(), Some(ForeignInputRow::from_json(j1)));
        assert_eq!(row_chunk.next(), Some(ForeignInputRow::from_json(j2)));
        assert_eq!(row_chunk.next(), Some(ForeignInputRow::from_json(j3)));
        assert_eq!(row_chunk.next(), None);

        Ok(())
    }
}
