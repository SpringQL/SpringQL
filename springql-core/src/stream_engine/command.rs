// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

pub mod alter_pipeline_command;
pub mod insert_plan;
pub mod query_plan;

use crate::stream_engine::command::alter_pipeline_command::AlterPipelineCommand;

#[derive(Clone, PartialEq, Debug)]
pub enum Command {
    AlterPipeline(AlterPipelineCommand),
}
