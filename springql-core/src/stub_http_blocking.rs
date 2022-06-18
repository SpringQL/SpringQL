use std::{sync::RwLock, time::Duration};

use once_cell::sync::OnceCell;
use reqwest::StatusCode;
use serde_json::Value;

#[derive(Debug)]
pub struct ReqwestClient;
#[derive(Debug)]
pub struct Response;

#[derive(Debug)]
pub struct ReqwestError;

#[derive(Debug)]
pub struct RequestBuilder {
    json_value: Option<serde_json::Value>,
}

impl ReqwestClient {
    pub fn with_timeout(_timeout: Duration) -> Self {
        Self
    }

    pub fn post(&self, _url: &String) -> RequestBuilder {
        RequestBuilder::new()
    }
}

static REQUESTS: OnceCell<RwLock<Vec<Value>>> = OnceCell::new();

/// gets stubed request JsonValues
pub fn stubed_requests() -> Vec<Value> {
    let req_store = REQUESTS.get_or_init(|| RwLock::new(Vec::new()));
    let mut store = req_store.write().unwrap();
    let result = store.clone();
    store.clear();
    result
}

impl RequestBuilder {
    fn new() -> Self {
        Self { json_value: None }
    }
    pub fn json(self, json: Value) -> Self {
        Self {
            json_value: Some(json),
        }
    }

    pub fn send(self) -> Result<Response, ReqwestError> {
        if let Some(json_value) = self.json_value {
            let req_store = REQUESTS.get_or_init(|| RwLock::new(Vec::new()));
            let mut store = req_store.write().unwrap();
            store.push(json_value);

            Ok(Response)
        } else {
            unreachable!()
        }
    }
}

impl Response {
    pub fn error_for_status_ref(&self) -> Result<&Response, ReqwestError> {
        Ok(self)
    }

    pub fn text(self) -> Result<String, ReqwestError> {
        Ok("stubbed request".to_string())
    }
}

impl ReqwestError {
    pub fn status(&self) -> Option<StatusCode> {
        Some(StatusCode::INTERNAL_SERVER_ERROR)
    }
}

impl From<reqwest::Error> for ReqwestError {
    fn from(_e: reqwest::Error) -> Self {
        Self
    }
}
