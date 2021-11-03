use std::{
    io::{self, BufRead, BufReader},
    net::{SocketAddr, TcpStream},
    time::Duration,
};

use anyhow::Context;

use crate::{
    error::{foreign_info::ForeignInfo, Result, SpringError},
    model::option::{server_options::NetServerOptions, Options},
    stream_engine::autonomous_executor::data::foreign_row::{
        foreign_source_row::ForeignSourceRow, format::json::JsonObject,
    },
};

use super::{SourceServerActive, SourceServerStandby};

// TODO config
const CONNECT_TIMEOUT_SECS: u64 = 1;
const READ_TIMEOUT_MSECS: u64 = 100;

#[derive(Debug)]
enum Protocol {
    Tcp,
}

#[derive(Debug)]
pub(in crate::stream_engine) struct NetSourceServerStandby {
    options: NetServerOptions,
}

#[derive(Debug)]
pub(in crate::stream_engine) struct NetSourceServerActive {
    foreign_addr: SocketAddr,
    tcp_stream_reader: BufReader<TcpStream>, // TODO UDP
    options: NetServerOptions,
}

impl SourceServerStandby for NetSourceServerStandby {
    type Act = NetSourceServerActive;

    fn new(options: &Options) -> Result<Self>
    where
        Self: Sized,
    {
        Ok(Self {
            options: NetServerOptions::try_from(options)?,
        })
    }

    /// # Failure
    ///
    /// - [SpringError::ForeignIo](crate::error::SpringError::ForeignIo)
    fn start(self) -> Result<NetSourceServerActive> {
        let options = self.options.clone();
        let sock_addr = SocketAddr::new(self.options.remote_host, self.options.remote_port);

        let tcp_stream =
            TcpStream::connect_timeout(&sock_addr, Duration::from_secs(CONNECT_TIMEOUT_SECS))
                .context("failed to connect to remote host")
                .map_err(|e| SpringError::ForeignIo {
                    source: e,
                    foreign_info: ForeignInfo::GenericTcp(sock_addr),
                })?;
        tcp_stream
            .set_read_timeout(Some(Duration::from_millis(READ_TIMEOUT_MSECS)))
            .context("failed to set timeout to remote host")
            .map_err(|e| SpringError::ForeignIo {
                source: e,
                foreign_info: ForeignInfo::GenericTcp(sock_addr),
            })?;

        let tcp_stream_reader = BufReader::new(tcp_stream);

        log::info!("[NetSourceServerActive] Ready to read from {}", sock_addr);

        Ok(NetSourceServerActive {
            tcp_stream_reader,
            foreign_addr: sock_addr,
            options,
        })
    }
}

impl SourceServerActive for NetSourceServerActive {
    fn next_row(&mut self) -> Result<ForeignSourceRow> {
        let mut json_s = String::new();

        self.tcp_stream_reader
            .read_line(&mut json_s)
            .map_err(|io_err| {
                if let io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock = io_err.kind() {
                    SpringError::ForeignSourceTimeout {
                        source: anyhow::Error::from(io_err),
                        foreign_info: ForeignInfo::GenericTcp(self.foreign_addr),
                    }
                } else {
                    SpringError::ForeignIo {
                        source: anyhow::Error::from(io_err),
                        foreign_info: ForeignInfo::GenericTcp(self.foreign_addr),
                    }
                }
            })?;

        self.parse_resp(&json_s)
    }
}

impl NetSourceServerActive {
    fn parse_resp(&self, json_s: &str) -> Result<ForeignSourceRow> {
        let json_v = serde_json::from_str(json_s)
            .with_context(|| {
                format!(
                    r#"failed to parse message from foreign stream as JSON ("{}")"#,
                    json_s,
                )
            })
            .map_err(|e| SpringError::ForeignIo {
                source: e,
                foreign_info: ForeignInfo::GenericTcp(self.foreign_addr),
            })?;

        let json_obj = JsonObject::new(json_v);

        Ok(ForeignSourceRow::from_json(json_obj))
    }
}

impl NetSourceServerActive {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::option::options_builder::OptionsBuilder;
    use crate::stream_engine::autonomous_executor::data::foreign_row::format::json::JsonObject;
    use crate::stream_engine::autonomous_executor::test_support::foreign::source::TestSource;

    #[test]
    fn test_source_server_tcp() -> crate::error::Result<()> {
        let j1 = JsonObject::fx_city_temperature_tokyo();
        let j2 = JsonObject::fx_city_temperature_osaka();
        let j3 = JsonObject::fx_city_temperature_london();

        let source = TestSource::start(vec![j2.clone(), j3.clone(), j1.clone()])?;

        let options = OptionsBuilder::default()
            .add("PROTOCOL", "TCP")
            .add("REMOTE_HOST", source.host_ip().to_string())
            .add("REMOTE_PORT", source.port().to_string())
            .build();

        let server = NetSourceServerStandby::new(&options)?;
        let mut server = server.start()?;

        assert_eq!(server.next_row()?, ForeignSourceRow::from_json(j2));
        assert_eq!(server.next_row()?, ForeignSourceRow::from_json(j3));
        assert_eq!(server.next_row()?, ForeignSourceRow::from_json(j1));
        assert!(matches!(
            server.next_row().unwrap_err(),
            SpringError::ForeignSourceTimeout { .. }
        ));

        Ok(())
    }
}
