use crate::error::Result;
use crate::stream_engine::executor::data::foreign_input_row::format::json::JsonObject;
use chrono::Duration;
use std::io::Write;
use std::net::{IpAddr, Shutdown, SocketAddr, TcpListener, TcpStream};
use std::thread::{self, JoinHandle};

pub struct TestSource {
    my_addr: SocketAddr,
    conn_thread: JoinHandle<()>,
}

impl TestSource {
    pub(in crate::stream_engine::executor) fn start(inputs: Vec<JsonObject>) -> Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let my_addr = listener.local_addr().unwrap();

        let conn_thread = thread::spawn(move || {
            for stream in listener.incoming() {
                let stream = stream.unwrap();
                stream.shutdown(Shutdown::Read).unwrap();
                Self::stream_handler(stream, inputs.clone()).unwrap();
            }
        });

        Ok(Self {
            my_addr,
            conn_thread,
        })
    }

    pub fn host_ip(&self) -> IpAddr {
        self.my_addr.ip()
    }

    pub fn port(&self) -> u16 {
        self.my_addr.port()
    }

    fn stream_handler(mut stream: TcpStream, inputs: Vec<JsonObject>) -> Result<()> {
        eprintln!(
            "[TestSource] Connection from {}",
            stream.peer_addr().unwrap()
        );

        for input in inputs {
            let mut json_s = input.to_string();
            json_s.push('\n');
            stream.write_all(json_s.as_bytes()).unwrap();

            eprint!("[TestSource] Sent: {}", json_s);
        }

        eprintln!("[TestSource] No message left. Wait forever...");
        thread::sleep(Duration::hours(1).to_std().unwrap());

        Ok(())
    }
}
