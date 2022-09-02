// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use std::{
    io::{BufWriter, Write},
    net::{SocketAddr, TcpStream},
    time::Duration,
};

use anyhow::Context;

use crate::{
    api::error::{foreign_info::ForeignInfo, Result, SpringError},
    api::SpringSinkWriterConfig,
    pipeline::{NetClientOptions, Options},
    stream_engine::autonomous_executor::{
        row::JsonObject, task::sink_task::sink_writer::SinkWriter, SchemalessRow,
    },
};

#[derive(Debug)]
pub struct NetSinkWriter {
    foreign_addr: SocketAddr,
    tcp_stream_writer: BufWriter<TcpStream>, // TODO UDP
}

impl SinkWriter for NetSinkWriter {
    fn start(options: &Options, config: &SpringSinkWriterConfig) -> Result<Self> {
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
            .set_write_timeout(Some(Duration::from_millis(
                config.net_write_timeout_msec as u64,
            )))
            .context("failed to set timeout to remote host")
            .map_err(|e| SpringError::ForeignIo {
                source: e,
                foreign_info: ForeignInfo::GenericTcp(sock_addr),
            })?;

        let tcp_stream_writer = BufWriter::new(tcp_stream);

        log::info!("[NetSinkWriter] Ready to write into {}", sock_addr);

        Ok(Self {
            tcp_stream_writer,
            foreign_addr: sock_addr,
        })
    }

    fn send_row(&mut self, row: SchemalessRow) -> Result<()> {
        let mut json_s = JsonObject::from(row).to_string();
        json_s.push('\n');

        log::debug!("[NetSinkWriter] Writing message to remote: {}", json_s);
        self.write_row(json_s.as_bytes())
    }
}

impl NetSinkWriter {
    fn write_row(&mut self, content: &[u8]) -> Result<()> {
        self.tcp_stream_writer
            .write_all(content)
            .with_context(|| {
                format!(
                    "failed to write row's content to remote sink: {}",
                    String::from_utf8_lossy(content)
                )
            })
            .map_err(|e| SpringError::ForeignIo {
                source: e,
                foreign_info: ForeignInfo::GenericTcp(self.foreign_addr),
            })?;
        self.tcp_stream_writer
            .flush()
            .with_context(|| "failed to flush row to remote sink")
            .map_err(|e| SpringError::ForeignIo {
                source: e,
                foreign_info: ForeignInfo::GenericTcp(self.foreign_addr),
            })?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use springql_configloader::SpringConfig;
    use springql_foreign_service::sink::ForeignSink;

    use super::*;
    use crate::{pipeline::OptionsBuilder, stream_engine::autonomous_executor::row::JsonObject};

    #[test]
    fn test_sink_writer_tcp() {
        let sink = ForeignSink::start().unwrap();

        let options = OptionsBuilder::default()
            .add("PROTOCOL", "TCP")
            .add("REMOTE_HOST", sink.host_ip().to_string())
            .add("REMOTE_PORT", sink.port().to_string())
            .build();

        let mut sink_writer =
            NetSinkWriter::start(&options, &SpringConfig::default().sink_writer).unwrap();

        sink_writer
            .send_row(SchemalessRow::fx_city_temperature_tokyo())
            .unwrap();
        sink_writer
            .send_row(SchemalessRow::fx_city_temperature_osaka())
            .unwrap();
        sink_writer
            .send_row(SchemalessRow::fx_city_temperature_london())
            .unwrap();

        const TIMEOUT: Duration = Duration::from_secs(1);
        assert_eq!(
            JsonObject::new(sink.try_receive(TIMEOUT).unwrap()),
            JsonObject::fx_city_temperature_tokyo()
        );
        assert_eq!(
            JsonObject::new(sink.try_receive(TIMEOUT).unwrap()),
            JsonObject::fx_city_temperature_osaka()
        );
        assert_eq!(
            JsonObject::new(sink.try_receive(TIMEOUT).unwrap()),
            JsonObject::fx_city_temperature_london()
        );
        assert!(sink.try_receive(TIMEOUT).is_none());
    }
}
