// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

use self::alter_pipeline_command::AlterPipelineCommand;

pub(crate) mod alter_pipeline_command;
pub(crate) mod insert_plan;
pub(crate) mod query_plan;

#[derive(Clone, Eq, PartialEq, Debug)]
pub(crate) enum Command {
    AlterPipeline(AlterPipelineCommand),
}
