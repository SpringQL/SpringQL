// Copyright (c) 2022 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

use serde_derive::Deserialize;

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct PostTaskGraphBody {
    pub tasks: Vec<TaskRequest>,
    pub queues: Vec<QueueRequest>,
}

#[derive(Clone, PartialEq, Debug, Deserialize)]
pub struct TaskRequest {
    pub id: String,
    #[serde(rename = "type")]
    pub type_: String,
    #[serde(rename = "avg-gain-bytes-per-sec")]
    pub avg_gain_bytes_per_sec: f32,
}

#[derive(Clone, PartialEq, Debug, Deserialize)]
pub struct QueueRequest {
    pub id: String,
    #[serde(rename = "upstream-task-id")]
    pub upstream_task_id: String,
    #[serde(rename = "downstream-task-id")]
    pub downstream_task_id: String,

    #[serde(rename = "row-queue")]
    pub row_queue: Option<RowQueueRequest>,
    #[serde(rename = "window-queue")]
    pub window_queue: Option<WindowQueueRequest>,
}

#[derive(Clone, PartialEq, Debug, Deserialize)]
pub struct RowQueueRequest {
    #[serde(rename = "num-rows")]
    pub num_rows: u64,
    #[serde(rename = "total-bytes")]
    pub total_bytes: u64,
}

#[derive(Clone, PartialEq, Debug, Deserialize)]
pub struct WindowQueueRequest {
    #[serde(rename = "num-rows-waiting")]
    pub num_rows_waiting: u64,
    #[serde(rename = "total-bytes")]
    pub total_bytes: u64,
}
