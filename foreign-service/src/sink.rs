// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

use anyhow::Result;
use std::io::{BufRead, BufReader};
use std::net::{IpAddr, Shutdown, SocketAddr, TcpListener, TcpStream};
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

const TIMEOUT: Duration = Duration::from_millis(500);

/// TCP server to receive JSON text, and returns serialized one.
pub struct ForeignSink {
    my_addr: SocketAddr,

    rx: mpsc::Receiver<serde_json::Value>,
}

impl ForeignSink {
    pub fn start() -> Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let my_addr = listener.local_addr().unwrap();

        let (tx, rx) = mpsc::channel();

        let _ = thread::spawn(move || {
            for stream in listener.incoming() {
                let stream = stream.unwrap();
                stream.shutdown(Shutdown::Write).unwrap();
                Self::stream_handler(stream, tx.clone());
            }
        });

        Ok(Self { my_addr, rx })
    }

    pub fn host_ip(&self) -> IpAddr {
        self.my_addr.ip()
    }

    pub fn port(&self) -> u16 {
        self.my_addr.port()
    }

    /// Non-blocking call.
    ///
    /// # Returns
    ///
    /// `None` when sink subtask has not sent new row yet.
    pub fn try_receive(&self) -> Option<serde_json::Value> {
        self.rx
            .try_recv()
            .or_else(|_| {
                thread::sleep(TIMEOUT);
                self.rx.try_recv()
            })
            .ok()
    }

    /// Blocking call.
    pub fn receive(&self) -> serde_json::Value {
        self.rx.recv().expect("failed to receive JSON text")
    }

    fn stream_handler(stream: TcpStream, tx: mpsc::Sender<serde_json::Value>) {
        log::info!(
            "[ForeignSink] Connection from {}",
            stream.peer_addr().unwrap()
        );

        let mut tcp_reader = BufReader::new(stream);

        loop {
            let mut buf_read = String::new();
            loop {
                log::info!("[ForeignSink] waiting for next row message...");

                let n = tcp_reader
                    .read_line(&mut buf_read)
                    .expect("failed to read from the socket");

                if n == 0 {
                    log::info!("[ForeignSink] Got EOF. Stop stream_handler.");
                    return;
                }

                log::info!("[ForeignSink] read: {}", buf_read);

                let received_json: serde_json::Value = buf_read.parse().unwrap();
                tx.send(received_json).unwrap();

                buf_read.clear();
            }
        }
    }
}
