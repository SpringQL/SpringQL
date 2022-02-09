pub mod builder;

mod handlers;

use std::{
    net::{SocketAddr, TcpListener},
    thread,
};

use builder::WebConsoleMockBuilder;
use simple_server::{Method, Server, StatusCode};

use crate::handlers::RequestHandler;

#[derive(Debug)]
pub struct WebConsoleMock {
    listener_sock: TcpListener,
    server: Server,
}

impl WebConsoleMock {
    fn new(builder: WebConsoleMockBuilder) -> Self {
        // let cb_post_pipeline = builder.cb_post_pipeline.clone();

        let listener_sock =
            TcpListener::bind(("127.0.0.1", 0)).expect("Error starting the server.");

        let server = Server::new(|request, response| {
            let method = request.method().clone();
            let uri = request.uri().clone();

            let mut h = RequestHandler::new(request, response);

            match (method, uri.path()) {
                (Method::GET, "/health") => h.get_health(),
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
    pub fn start(self) -> ! {
        // thread::spawn(move || {
        //     self.server.listen_on_socket(self.listener_sock);
        // });
        self.server.listen_on_socket(self.listener_sock)
    }
}
