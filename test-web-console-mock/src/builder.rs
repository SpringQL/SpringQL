use std::sync::Arc;

use crate::WebConsoleMock;

#[derive(Default)]
pub struct WebConsoleMockBuilder {
    pub(crate) cb_post_pipeline: Option<Arc<dyn Fn() + Sync + Send>>,
}

impl WebConsoleMockBuilder {
    pub fn add_callback_post_pipeline<F: Fn() + Sync + Send + 'static>(self, callback: F) -> Self {
        let mut me = self;
        me.cb_post_pipeline = Some(Arc::new(callback));
        me
    }

    pub fn start(self) -> WebConsoleMock {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(async move { WebConsoleMock::new(self).await })
    }
}
