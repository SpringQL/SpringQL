pub mod builder;
pub mod request_body;

mod request_handler;

use std::{
    net::{SocketAddr, TcpListener},
    thread,
    time::Duration,
};

use builder::WebConsoleMockBuilder;
use simple_server::{Method, Server};

use crate::request_handler::RequestHandler;

pub struct WebConsoleMock {
    listener_sock: TcpListener,
    server: Server,
}

impl WebConsoleMock {
    fn new(builder: WebConsoleMockBuilder) -> Self {
        let cb_post_pipeline = builder.cb_post_pipeline;

        let listener_sock =
            TcpListener::bind(("127.0.0.1", 0)).expect("Error starting the server.");

        let server = Server::new(move |request, response| {
            let method = request.method().clone();
            let uri = request.uri().clone();

            let mut h = RequestHandler::new(request, response);

            match (method, uri.path()) {
                (Method::GET, "/health") => h.get_health(),
                (Method::POST, "/task-graph") => h.post_task_graph(cb_post_pipeline.clone()),
                (_, _) => h.not_found(),
            }
        });

        Self {
            listener_sock,
            server,
        }
    }

    pub fn sock_addr(&self) -> SocketAddr {
        self.listener_sock.local_addr().unwrap()
    }

    /// Returns when health-check endpoint starts working.
    pub fn start(self) {
        let addr = self.sock_addr();

        thread::spawn(move || {
            self.server.listen_on_socket(self.listener_sock);
        });
        Self::wait_for_health_check(addr);
    }

    fn wait_for_health_check(addr: SocketAddr) {
        let client = reqwest::blocking::Client::builder()
            .timeout(Some(Duration::from_secs(1)))
            .build()
            .expect("failed to build a reqwest client");

        let url = format!("http://{}:{}/health", addr.ip(), addr.port());

        loop {
            match client.get(&url).send() {
                Ok(resp) => {
                    let res_status = resp.error_for_status_ref();
                    match res_status {
                        Ok(_) => {
                            log::info!("Got response to healthcheck endpoint.");
                            break;
                        }
                        Err(e) => {
                            panic!("healthcheck endpoint responsed with error status: {:?}", e)
                        }
                    }
                }
                Err(_) => thread::sleep(Duration::from_secs(1)),
            }
        }
    }
}
