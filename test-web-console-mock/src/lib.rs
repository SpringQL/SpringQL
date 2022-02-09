pub mod builder;

mod handlers;

use builder::WebConsoleMockBuilder;
use warp::Filter;

pub struct WebConsoleMock;

impl WebConsoleMock {
    async fn new(builder: WebConsoleMockBuilder) -> Self {
        let cb_post_pipeline = builder.cb_post_pipeline.clone();

        let filter = warp::path!("pipeline")
            .and(warp::post())
            .and(warp::body::json())
            .and_then(move |body| handlers::post_pipeline(body, cb_post_pipeline.clone()));

        warp::serve(filter).run((builder.addr, builder.port)).await;

        Self
    }
}
