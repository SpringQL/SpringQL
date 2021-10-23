use crate::error::Result;
use crate::stream_engine::executor::foreign_input_row::format::json::JsonObject;
use std::io::Write;
use std::net::{TcpListener, TcpStream};
use std::thread::{self, JoinHandle};

pub struct TestSource {
    port: u16,
    conn_thread: JoinHandle<()>,
}

impl TestSource {
    pub(in crate::stream_engine::executor) fn start(inputs: Vec<JsonObject>) -> Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();

        let conn_thread = thread::spawn(move || {
            for stream in listener.incoming() {
                let stream = stream.unwrap();
                Self::stream_handler(stream, inputs.clone()).unwrap();
            }
        });

        Ok(Self { port, conn_thread })
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    fn stream_handler(mut stream: TcpStream, inputs: Vec<JsonObject>) -> Result<()> {
        println!("Connection from {}", stream.peer_addr().unwrap());

        for input in inputs {
            let mut json_s = input.to_string();
            json_s.push('\n');
            stream.write_all(json_s.as_bytes()).unwrap();
        }

        Ok(())
    }
}
