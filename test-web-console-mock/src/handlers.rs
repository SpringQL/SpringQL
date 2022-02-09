use serde_derive::{Deserialize, Serialize};
use std::{convert::Infallible, sync::Arc};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(super) struct PostPipelineBody {
    data: String,
}

pub(super) async fn post_pipeline(
    body: PostPipelineBody,
    callback: Option<Arc<dyn Fn() + Sync + Send>>,
) -> Result<impl warp::Reply, Infallible> {
    if let Some(cb) = callback {
        cb();
    }
    Ok(warp::reply::json(&body))
}
