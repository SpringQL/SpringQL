// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use std::{
    io::{BufRead, BufReader},
    net::{Shutdown, SocketAddr, TcpListener, TcpStream},
    sync::{mpsc, Mutex, MutexGuard},
    thread,
    time::Duration,
};

use crate::{
    api::error::{foreign_info::ForeignInfo, Result, SpringError},
    api::SpringSourceReaderConfig,
    pipeline::{NetProtocol, NetServerOptions, Options},
    stream_engine::autonomous_executor::{
        row::foreign_row::{
            format::json::JsonObject,
            source_row::{json_source_row::JsonSourceRow, SourceRow},
        },
        task::source_task::source_reader::SourceReader,
    },
};

#[derive(Debug)]
pub struct NetServerSourceReader {
    my_addr: SocketAddr,

    /// FIXME this source reader does not scale
    rx: Mutex<mpsc::Receiver<serde_json::Value>>,

    timeout: Duration,
}

impl SourceReader for NetServerSourceReader {
    /// # Failure
    ///
    /// - `SpringError::ForeignIo`
    /// - `SpringError::InvalidOption`
    fn start(options: &Options, config: &SpringSourceReaderConfig) -> Result<Self> {
        let options = NetServerOptions::try_from(options)?;
        assert!(
            matches!(options.protocol, NetProtocol::Tcp),
            "unsupported protocol"
        );

        let listener = TcpListener::bind(("127.0.0.1", options.port)).unwrap();
        let my_addr = listener.local_addr().unwrap();

        let (tx, rx) = mpsc::channel();

        let timeout = Duration::from_millis(config.net_read_timeout_msec as u64);

        let _ = thread::Builder::new()
            .name("NetServerSourceReader".into())
            .spawn(move || {
                for stream in listener.incoming() {
                    let stream = stream.unwrap();
                    stream.shutdown(Shutdown::Write).unwrap();
                    Self::stream_handler(stream, tx.clone());
                }
            });

        log::info!(
            "[NetServerSourceReader] Ready to accept rows at {}",
            my_addr
        );

        Ok(Self {
            my_addr,
            rx: Mutex::new(rx),
            timeout,
        })
    }

    fn next_row(&mut self) -> Result<SourceRow> {
        let rx = self.rx();

        rx.try_recv()
            .or_else(|_| {
                thread::sleep(self.timeout);
                rx.try_recv()
            })
            .map(|json| {
                let json_obj = JsonObject::new(json);
                SourceRow::Json(JsonSourceRow::from_json(json_obj))
            })
            .map_err(|e| SpringError::ForeignSourceTimeout {
                source: anyhow::Error::from(e),
                foreign_info: ForeignInfo::GenericTcp(self.my_addr),
            })
    }
}

impl NetServerSourceReader {
    fn rx(&self) -> MutexGuard<mpsc::Receiver<serde_json::Value>> {
        self.rx.lock().expect("failed to lock mutex")
    }

    fn stream_handler(stream: TcpStream, tx: mpsc::Sender<serde_json::Value>) {
        log::info!(
            "[NetServerSourceReader] Connection from {}",
            stream.peer_addr().unwrap()
        );

        let mut tcp_reader = BufReader::new(stream);

        loop {
            let mut buf_read = String::new();
            loop {
                log::info!("[NetServerSourceReader] waiting for next row message...");

                let n = tcp_reader
                    .read_line(&mut buf_read)
                    .expect("failed to read from the socket");

                if n == 0 {
                    log::info!("[NetServerSourceReader] Got EOF. Stop stream_handler.");
                    return;
                }

                log::info!("[NetServerSourceReader] read: {}", buf_read);

                let received_json: serde_json::Value = buf_read.parse().unwrap();
                tx.send(received_json).unwrap();

                buf_read.clear();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        api::SpringSinkWriterConfig,
        pipeline::OptionsBuilder,
        stream_engine::{
            autonomous_executor::task::sink_task::sink_writer::{net::NetSinkWriter, SinkWriter},
            Row,
        },
    };

    fn ephemeral_port() -> u16 {
        let addr = TcpListener::bind("127.0.0.1:0").unwrap();
        addr.local_addr().unwrap().port()
    }

    fn tcp_writer(remote_port: u16) -> NetSinkWriter {
        let options = OptionsBuilder::default()
            .add("PROTOCOL", "TCP")
            .add("REMOTE_HOST", "127.0.0.1")
            .add("REMOTE_PORT", remote_port.to_string())
            .build();
        NetSinkWriter::start(&options, &SpringSinkWriterConfig::fx_default()).unwrap()
    }

    #[test]
    fn test_source_tcp() -> crate::api::error::Result<()> {
        let port = ephemeral_port();
        let options = OptionsBuilder::default()
            .add("PROTOCOL", "TCP")
            .add("PORT", port.to_string())
            .build();
        let mut reader =
            NetServerSourceReader::start(&options, &SpringSourceReaderConfig::fx_default())?;

        let mut writer = tcp_writer(port);

        writer.send_row(Row::fx_city_temperature_tokyo()).unwrap();
        writer.send_row(Row::fx_city_temperature_osaka()).unwrap();
        writer.send_row(Row::fx_city_temperature_london()).unwrap();

        assert_eq!(
            reader.next_row()?,
            SourceRow::Json(JsonSourceRow::fx_city_temperature_tokyo())
        );
        assert_eq!(
            reader.next_row()?,
            SourceRow::Json(JsonSourceRow::fx_city_temperature_osaka())
        );
        assert_eq!(
            reader.next_row()?,
            SourceRow::Json(JsonSourceRow::fx_city_temperature_london())
        );
        assert!(matches!(
            reader.next_row().unwrap_err(),
            SpringError::ForeignSourceTimeout { .. }
        ));

        Ok(())
    }
}
