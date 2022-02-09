use std::{net::IpAddr, sync::Arc};

use crate::WebConsoleMock;

pub struct WebConsoleMockBuilder {
    pub(crate) addr: IpAddr,
    pub(crate) port: u16,

    pub(crate) cb_post_pipeline: Option<Arc<dyn Fn() + Sync + Send>>,
}

impl WebConsoleMockBuilder {
    pub fn new(addr: IpAddr, port: u16) -> Self {
        Self {
            addr,
            port,
            cb_post_pipeline: None,
        }
    }

    pub fn add_callback_post_pipeline<F: Fn() + Sync + Send + 'static>(self, callback: F) -> Self {
        let mut me = self;
        me.cb_post_pipeline = Some(Arc::new(callback));
        me
    }

    pub async fn start(self) -> WebConsoleMock {
        WebConsoleMock::new(self).await
    }
}
