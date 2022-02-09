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
        callback: Option<Arc<dyn Fn(PostTaskGraphBody) + Sync + Send>>,
    ) -> ResponseResult {
        let req_body = self.request.body();
        let body: PostTaskGraphBody = serde_json::from_slice(req_body.as_slice()).unwrap();

        if let Some(cb) = callback {
            cb(body);
        }
        Ok(self.response.body("ok".as_bytes().to_vec())?)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PostTaskGraphBody {
    data: String,
}
