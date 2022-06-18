use std::time::Duration;

use reqwest::StatusCode;

#[derive(Debug)]
pub struct ReqwestClient(reqwest::blocking::Client);
#[derive(Debug)]
pub struct Response(reqwest::blocking::Response);

#[derive(Debug)]
pub struct ReqwestError(reqwest::Error);

#[derive(Debug)]
pub struct RequestBuilder {
    raw_builder: reqwest::blocking::RequestBuilder,
    json_value: Option<serde_json::Value>,
}

impl ReqwestClient {
    pub fn with_timeout(timeout: Duration) -> Self {
        Self(
            reqwest::blocking::Client::builder()
                .timeout(Some(timeout))
                .build()
                .expect("failed to build a reqwest client"),
        )
    }

    pub fn post(&self, url: &String) -> RequestBuilder {
        RequestBuilder::new(self.0.post(url))
    }
}

impl RequestBuilder {
    fn new(builder: reqwest::blocking::RequestBuilder) -> Self {
        Self {
            raw_builder: builder,
            json_value: None,
        }
    }
    pub fn json(self, json: serde_json::Value) -> Self {
        Self {
            raw_builder: self.raw_builder,
            json_value: Some(json),
        }
    }

    pub fn send(self) -> Result<Response, ReqwestError> {
        if let Some(json_value) = self.json_value {
            Ok(Response(self.raw_builder.json(&json_value).send()?))
        } else {
            unreachable!()
        }
    }
}

impl Response {
    pub fn error_for_status_ref(&self) -> Result<&Response, ReqwestError> {
        match self.0.error_for_status_ref() {
            Ok(_) => Ok(self),
            Err(e) => Err(ReqwestError::from(e)),
        }
    }

    pub fn text(self) -> Result<String, ReqwestError> {
        Ok(self.0.text()?)
    }
}

impl ReqwestError {
    pub fn status(&self) -> Option<StatusCode> {
        self.0.status()
    }
}

impl From<reqwest::Error> for ReqwestError {
    fn from(e: reqwest::Error) -> Self {
        Self(e)
    }
}
