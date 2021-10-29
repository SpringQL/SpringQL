use crate::error::Result;
use std::io::{BufRead, BufReader};
use std::net::{IpAddr, Shutdown, SocketAddr, TcpListener, TcpStream};
use std::sync::mpsc;
use std::thread::{self, JoinHandle};

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

    /// # Panics
    ///
    /// If expectation and received rows are different
    pub(in crate::stream_engine::autonomous_executor) fn expect_receive(
        self,
        expected: Vec<serde_json::Value>,
    ) {
        for expected_row in expected {
            let received = self.rx.recv().unwrap();
            assert_eq!(received, expected_row);
        }
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
