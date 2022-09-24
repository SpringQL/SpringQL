// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use std::{
    io::{self, BufRead, BufReader},
    net::{SocketAddr, TcpStream},
    time::Duration,
};

use anyhow::Context;

use crate::{
    api::error::{foreign_info::ForeignInfo, Result, SpringError},
    api::SpringSourceReaderConfig,
    pipeline::{NetClientOptions, Options},
    stream_engine::autonomous_executor::{
        row::SourceRow, task::source_task::source_reader::SourceReader,
    },
};

#[derive(Debug)]
pub struct NetClientSourceReader {
    foreign_addr: SocketAddr,
    tcp_stream_reader: BufReader<TcpStream>, // TODO UDP
}

impl SourceReader for NetClientSourceReader {
    /// # Failure
    ///
    /// - `SpringError::ForeignIo`
    /// - `SpringError::InvalidOption`
    fn start(options: &Options, config: &SpringSourceReaderConfig) -> Result<Self> {
        let options = NetClientOptions::try_from(options)?;
        let sock_addr = SocketAddr::new(options.remote_host, options.remote_port);

        let tcp_stream = TcpStream::connect_timeout(
            &sock_addr,
            Duration::from_millis(config.net_connect_timeout_msec as u64),
        )
        .context("failed to connect to remote host")
        .map_err(|e| SpringError::ForeignIo {
            source: e,
            foreign_info: ForeignInfo::GenericTcp(sock_addr),
        })?;
        tcp_stream
            .set_read_timeout(Some(Duration::from_millis(
                config.net_read_timeout_msec as u64,
            )))
            .context("failed to set timeout to remote host")
            .map_err(|e| SpringError::ForeignIo {
                source: e,
                foreign_info: ForeignInfo::GenericTcp(sock_addr),
            })?;

        let tcp_stream_reader = BufReader::new(tcp_stream);

        log::info!("[NetSourceReader] Ready to read from {}", sock_addr);

        Ok(Self {
            tcp_stream_reader,
            foreign_addr: sock_addr,
        })
    }

    fn next_row(&mut self) -> Result<SourceRow> {
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

impl NetClientSourceReader {
    // TODO other formats than JSON
    fn parse_resp(&self, json_s: &str) -> Result<SourceRow> {
        SourceRow::from_json(json_s)
    }
}

impl NetClientSourceReader {}

#[cfg(test)]
mod tests {
    use springql_foreign_service::source::ForeignSource;
    use springql_foreign_service::source::ForeignSourceInput;

    use super::*;
    use crate::pipeline::test_support::fixture::default_source_reader_config;
    use crate::pipeline::OptionsBuilder;
    use crate::stream_engine::autonomous_executor::row::JsonObject;
    use crate::stream_engine::autonomous_executor::row::JsonSourceRow;

    #[test]
    fn test_source_tcp() -> crate::api::error::Result<()> {
        let j1 = JsonObject::fx_city_temperature_tokyo();
        let j2 = JsonObject::fx_city_temperature_osaka();
        let j3 = JsonObject::fx_city_temperature_london();

        let source = ForeignSource::new().unwrap();

        let options = OptionsBuilder::default()
            .add("PROTOCOL", "TCP")
            .add("REMOTE_HOST", source.host_ip().to_string())
            .add("REMOTE_PORT", source.port().to_string())
            .build();

        source.start(ForeignSourceInput::new_fifo_batch(vec![
            serde_json::Value::from(j2.clone()),
            serde_json::Value::from(j3.clone()),
            serde_json::Value::from(j1.clone()),
        ]));

        let mut subtask = NetClientSourceReader::start(&options, &default_source_reader_config())?;

        assert_eq!(
            subtask.next_row()?,
            SourceRow::Json(JsonSourceRow::from_json(j2))
        );
        assert_eq!(
            subtask.next_row()?,
            SourceRow::Json(JsonSourceRow::from_json(j3))
        );
        assert_eq!(
            subtask.next_row()?,
            SourceRow::Json(JsonSourceRow::from_json(j1))
        );
        assert!(matches!(
            subtask.next_row().unwrap_err(),
            SpringError::ForeignSourceTimeout { .. }
        ));

        Ok(())
    }
}
