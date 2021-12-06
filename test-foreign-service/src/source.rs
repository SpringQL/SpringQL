// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

pub mod source_input;

use anyhow::Result;
use chrono::Duration;
use std::io::Write;
use std::net::{IpAddr, Shutdown, SocketAddr, TcpListener, TcpStream};
use std::thread;

use self::source_input::TestForeignSourceInput;

/// Runs as a TCP server and write(2)s foreign rows to socket.
pub struct TestForeignSource {
    my_addr: SocketAddr,
}

impl TestForeignSource {
    pub fn start(input: TestForeignSourceInput) -> Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let my_addr = listener.local_addr().unwrap();

        let _ = thread::spawn(move || {
            let (stream, _sock) = listener.accept().unwrap();
            stream.shutdown(Shutdown::Read).unwrap();
            Self::stream_handler(stream, input).unwrap();
        });

        Ok(Self { my_addr })
    }

    pub fn host_ip(&self) -> IpAddr {
        self.my_addr.ip()
    }

    pub fn port(&self) -> u16 {
        self.my_addr.port()
    }

    fn stream_handler(mut stream: TcpStream, input: TestForeignSourceInput) -> Result<()> {
        log::info!(
            "[TestForeignSource] Connection from {}",
            stream.peer_addr().unwrap()
        );

        for v in input {
            let mut json_s = v.to_string();
            json_s.push('\n');
            stream.write_all(json_s.as_bytes()).unwrap();

            log::info!("[TestForeignSource] Sent: {}", json_s);
        }

        log::info!("[TestForeignSource] No message left. Wait forever...");
        thread::sleep(Duration::hours(1).to_std().unwrap());

        Ok(())
    }
}
