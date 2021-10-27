use std::{
    io::{BufWriter, Write},
    net::{IpAddr, SocketAddr, TcpStream},
    time::Duration,
};

use anyhow::Context;

use super::{OutputServerActive, OutputServerStandby};
use crate::{
    error::{foreign_info::ForeignInfo, Result, SpringError},
    model::option::Options,
    stream_engine::executor::data::foreign_row::{
        foreign_output_row::ForeignOutputRow, format::json::JsonObject,
    },
};

// TODO config
const CONNECT_TIMEOUT_SECS: u64 = 1;
const WRITE_TIMEOUT_MSECS: u64 = 100;

#[derive(Debug)]
enum Protocol {
    Tcp,
}

#[derive(Debug)]
pub(in crate::stream_engine::executor) struct NetOutputServerStandby {
    protocol: Protocol,
    remote_host: IpAddr,
    remote_port: u16,
}

#[derive(Debug)]
pub(in crate::stream_engine::executor) struct NetOutputServerActive {
    foreign_addr: SocketAddr,
    tcp_stream_writer: BufWriter<TcpStream>, // TODO UDP
}

impl OutputServerStandby<NetOutputServerActive> for NetOutputServerStandby {
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

    fn start(self) -> Result<NetOutputServerActive> {
        let sock_addr = SocketAddr::new(self.remote_host, self.remote_port);

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

        Ok(NetOutputServerActive {
            tcp_stream_writer,
            foreign_addr: sock_addr,
        })
    }
}

impl OutputServerActive for NetOutputServerActive {
    fn send_row(&mut self, row: ForeignOutputRow) -> Result<()> {
        let mut json_s = JsonObject::from(row).to_string();
        json_s.push('\n');

        self.tcp_stream_writer
            .write_all(json_s.as_bytes())
            .with_context(|| format!("failed to write JSON row to remote sink: {}", json_s))
            .map_err(|e| SpringError::ForeignIo {
                source: e,
                foreign_info: ForeignInfo::GenericTcp(self.foreign_addr),
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        model::option::options_builder::OptionsBuilder,
        stream_engine::executor::{
            data::foreign_row::format::json::JsonObject, test_support::foreign::sink::TestSink,
        },
    };

    #[test]
    fn test_output_server_tcp() {
        let sink = TestSink::start().unwrap();

        let options = OptionsBuilder::default()
            .add("PROTOCOL", "TCP")
            .add("REMOTE_HOST", sink.host_ip().to_string())
            .add("REMOTE_PORT", sink.port().to_string())
            .build();

        let server = NetOutputServerStandby::new(options).unwrap();
        let mut server = server.start().unwrap();

        server
            .send_row(ForeignOutputRow::fx_city_temperature_tokyo())
            .unwrap();
        server
            .send_row(ForeignOutputRow::fx_city_temperature_osaka())
            .unwrap();
        server
            .send_row(ForeignOutputRow::fx_city_temperature_london())
            .unwrap();

        sink.expect_receive(vec![
            JsonObject::fx_city_temperature_tokyo().into(),
            JsonObject::fx_city_temperature_osaka().into(),
            JsonObject::fx_city_temperature_london().into(),
        ]);
    }
}
