use std::{
    io::{BufWriter, Write},
    net::{SocketAddr, TcpStream},
    time::Duration,
};

use anyhow::Context;

use super::SinkServerInstance;
use crate::{
    error::{foreign_info::ForeignInfo, Result, SpringError},
    pipeline::option::{net_server_options::NetServerOptions, Options},
    stream_engine::autonomous_executor::row::foreign_row::{
        foreign_sink_row::ForeignSinkRow, format::json::JsonObject,
    },
};

// TODO config
const CONNECT_TIMEOUT_SECS: u64 = 1;
const WRITE_TIMEOUT_MSECS: u64 = 100;

#[derive(Debug)]
pub(in crate::stream_engine) struct NetSinkServerInstance {
    foreign_addr: SocketAddr,
    tcp_stream_writer: BufWriter<TcpStream>, // TODO UDP
}

impl SinkServerInstance for NetSinkServerInstance {
    fn start(options: &Options) -> Result<Self> {
        let options = NetServerOptions::try_from(options)?;
        let sock_addr = SocketAddr::new(options.remote_host, options.remote_port);

        let tcp_stream =
            TcpStream::connect_timeout(&sock_addr, Duration::from_secs(CONNECT_TIMEOUT_SECS))
                .context("failed to connect to remote host")
                .map_err(|e| SpringError::ForeignIo {
                    source: e,
                    foreign_info: ForeignInfo::GenericTcp(sock_addr),
                })?;
        tcp_stream
            .set_write_timeout(Some(Duration::from_millis(WRITE_TIMEOUT_MSECS)))
            .context("failed to set timeout to remote host")
            .map_err(|e| SpringError::ForeignIo {
                source: e,
                foreign_info: ForeignInfo::GenericTcp(sock_addr),
            })?;

        let tcp_stream_writer = BufWriter::new(tcp_stream);

        log::info!("[NetSinkServerInstance] Ready to write into {}", sock_addr);

        Ok(Self {
            tcp_stream_writer,
            foreign_addr: sock_addr,
        })
    }

    fn send_row(&mut self, row: ForeignSinkRow) -> Result<()> {
        let mut json_s = JsonObject::from(row).to_string();
        json_s.push('\n');

        log::info!(
            "[NetSinkServerInstance] Writing message to remote: {}",
            json_s
        );

        self.tcp_stream_writer
            .write_all(json_s.as_bytes())
            .with_context(|| format!("failed to write JSON row to remote sink: {}", json_s))
            .map_err(|e| SpringError::ForeignIo {
                source: e,
                foreign_info: ForeignInfo::GenericTcp(self.foreign_addr),
            })?;
        self.tcp_stream_writer
            .flush()
            .with_context(|| format!("failed to flush JSON row to remote sink: {}", json_s))
            .map_err(|e| SpringError::ForeignIo {
                source: e,
                foreign_info: ForeignInfo::GenericTcp(self.foreign_addr),
            })?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use test_foreign_service::sink::TestForeignSink;

    use super::*;
    use crate::{
        pipeline::option::options_builder::OptionsBuilder,
        stream_engine::autonomous_executor::row::foreign_row::format::json::JsonObject,
    };

    #[test]
    fn test_sink_server_tcp() {
        let sink = TestForeignSink::start().unwrap();

        let options = OptionsBuilder::default()
            .add("PROTOCOL", "TCP")
            .add("REMOTE_HOST", sink.host_ip().to_string())
            .add("REMOTE_PORT", sink.port().to_string())
            .build();

        let mut server = NetSinkServerInstance::start(&options).unwrap();

        server
            .send_row(ForeignSinkRow::fx_city_temperature_tokyo())
            .unwrap();
        server
            .send_row(ForeignSinkRow::fx_city_temperature_osaka())
            .unwrap();
        server
            .send_row(ForeignSinkRow::fx_city_temperature_london())
            .unwrap();

        assert_eq!(
            JsonObject::new(sink.receive().unwrap()),
            JsonObject::fx_city_temperature_tokyo()
        );
        assert_eq!(
            JsonObject::new(sink.receive().unwrap()),
            JsonObject::fx_city_temperature_osaka()
        );
        assert_eq!(
            JsonObject::new(sink.receive().unwrap()),
            JsonObject::fx_city_temperature_london()
        );
        assert!(sink.receive().is_err());
    }
}
