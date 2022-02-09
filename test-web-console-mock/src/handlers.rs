use std::sync::Arc;

use serde_derive::{Deserialize, Serialize};
use simple_server::{Builder, Request, ResponseResult, StatusCode};

#[derive(Debug)]
pub(super) struct RequestHandler {
    request: Request<Vec<u8>>,
    response: Builder,
}

impl RequestHandler {
    pub(super) fn new(request: Request<Vec<u8>>, response: Builder) -> Self {
        Self { request, response }
    }

    pub(super) fn get_health(&mut self) -> ResponseResult {
        Ok(self.response.body("ok".as_bytes().to_vec())?)
    }

    pub(super) fn not_found(&mut self) -> ResponseResult {
        self.response.status(StatusCode::NOT_FOUND);
        Ok(self.response.body("404 Not found".as_bytes().to_vec())?)
    }

    pub(super) fn post_task_graph(
        &mut self,
        callback: Option<Arc<dyn Fn() + Sync + Send>>,
    ) -> ResponseResult {
        if let Some(cb) = callback {
            cb();
        }
        Ok(self.response.body("ok".as_bytes().to_vec())?)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(super) struct PostPipelineBody {
    data: String,
}

// pub(super) async fn post_pipeline(
//     body: PostPipelineBody,
//     callback: Option<Arc<dyn Fn() + Sync + Send>>,
// ) -> Result<impl warp::Reply, Infallible> {
//     if let Some(cb) = callback {
//         cb();
//     }
//     Ok(warp::reply::json(&body))
// }
