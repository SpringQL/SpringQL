use anyhow::Context;

use crate::error::{Result, SpringError};
use std::io::{BufRead, BufReader};
use std::net::{IpAddr, Shutdown, SocketAddr, TcpListener, TcpStream};
use std::sync::mpsc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

pub struct TestSink {
    my_addr: SocketAddr,
    conn_thread: JoinHandle<()>,

    rx: mpsc::Receiver<serde_json::Value>,
}

impl TestSink {
    pub(in crate::stream_engine::autonomous_executor) fn start() -> Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let my_addr = listener.local_addr().unwrap();

        let (tx, rx) = mpsc::channel();

        let conn_thread = thread::spawn(move || {
            for stream in listener.incoming() {
                let stream = stream.unwrap();
                stream.shutdown(Shutdown::Write).unwrap();
                Self::stream_handler(stream, tx.clone());
            }
        });

        Ok(Self {
            my_addr,
            conn_thread,
            rx,
        })
    }

    pub fn host_ip(&self) -> IpAddr {
        self.my_addr.ip()
    }

    pub fn port(&self) -> u16 {
        self.my_addr.port()
    }

    /// Non-blocking call.
    ///
    /// # Failures
    ///
    /// [SpringError::Unavailable](crate::error::SpringError::Unavailable) when sink server has not sent new row yet.
    pub(in crate::stream_engine::autonomous_executor) fn receive(
        &self,
    ) -> Result<serde_json::Value> {
        let timeout = Duration::from_millis(500);

        let received = self
            .rx
            .try_recv()
            .or_else(|_| {
                thread::sleep(timeout);
                self.rx.try_recv()
            })
            .context("sink server has not sent new row yet")
            .map_err(|e| SpringError::Unavailable {
                resource: "foreign row (sink)".to_string(),
                source: e,
            })?;
        Ok(received)
    }

    fn stream_handler(stream: TcpStream, tx: mpsc::Sender<serde_json::Value>) {
        eprintln!("[TestSink] Connection from {}", stream.peer_addr().unwrap());

        let mut tcp_reader = BufReader::new(stream);

        loop {
            let mut buf_read = String::new();
            loop {
                eprintln!("[TestSink] waiting for next row message...");

                let n = tcp_reader
                    .read_line(&mut buf_read)
                    .expect("failed to read from the socket");

                if n == 0 {
                    eprintln!("[TestSink] Got EOF. Stop stream_handler.");
                    return;
                }

                eprintln!("[TestSink] read: {}", buf_read);

                let received_json: serde_json::Value = buf_read.parse().unwrap();
                tx.send(received_json).unwrap();

                buf_read.clear();
            }
        }
    }
}
