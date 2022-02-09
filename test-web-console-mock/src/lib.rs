pub mod builder;

mod handlers;

use std::net::SocketAddr;

use builder::WebConsoleMockBuilder;
use warp::Filter;

pub struct WebConsoleMock {
    sock_addr: SocketAddr,
}

impl WebConsoleMock {
    async fn new(builder: WebConsoleMockBuilder) -> Self {
        let cb_post_pipeline = builder.cb_post_pipeline.clone();

        let filter = warp::path!("pipeline")
            .and(warp::post())
            .and(warp::body::json())
            .and_then(move |body| handlers::post_pipeline(body, cb_post_pipeline.clone()));

        let (sock_addr, server) = warp::serve(filter).bind_ephemeral(([127, 0, 0, 1], 0));

        let _ = tokio::spawn(server);

        Self { sock_addr }
    }

    pub fn sock_addr(&self) -> SocketAddr {
        self.sock_addr
    }
}
