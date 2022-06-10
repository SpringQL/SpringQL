// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

pub mod source_input;

use std::{
    io::Write,
    net::{IpAddr, Shutdown, SocketAddr, TcpListener, TcpStream},
    thread,
};

use anyhow::Result;
use chrono::Duration;

use crate::source::source_input::ForeignSourceInput;

/// Runs as a TCP server and write(2)s foreign rows to socket.
pub struct ForeignSource {
    listener: TcpListener,
    my_addr: SocketAddr,
}

impl ForeignSource {
    pub fn new() -> Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let my_addr = listener.local_addr()?;
        Ok(Self { listener, my_addr })
    }

    pub fn start(self, input: ForeignSourceInput) {
        let listener = self.listener;

        let _ = thread::Builder::new()
            .name("ForeignSource".into())
            .spawn(move || {
                let (stream, _sock) = listener.accept().unwrap();
                stream.shutdown(Shutdown::Read).unwrap();
                Self::stream_handler(stream, input).unwrap();
            });
    }

    pub fn host_ip(&self) -> IpAddr {
        self.my_addr.ip()
    }

    pub fn port(&self) -> u16 {
        self.my_addr.port()
    }

    fn stream_handler(mut stream: TcpStream, input: ForeignSourceInput) -> Result<()> {
        log::info!(
            "[ForeignSource] Connection from {}",
            stream.peer_addr().unwrap()
        );

        for res_v in input {
            let v = res_v.unwrap();
            let mut json_s = v.to_string();
            json_s.push('\n');
            stream.write_all(json_s.as_bytes()).unwrap();

            log::info!("[ForeignSource] Sent: {}", json_s);
        }

        log::info!("[ForeignSource] No message left. Wait forever...");
        thread::sleep(Duration::hours(1).to_std().unwrap());

        Ok(())
    }
}
