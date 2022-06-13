// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

mod alter_pipeline_command;
mod insert_plan;
mod query_plan;

pub use crate::stream_engine::command::alter_pipeline_command::AlterPipelineCommand;
pub use insert_plan::InsertPlan;
pub use query_plan::{
    CollectOp, GroupAggregateWindowOp, JoinOp, JoinWindowOp, LowerOps, ProjectionOp, QueryPlan,
    UpperOps,
};

#[derive(Clone, PartialEq, Debug)]
pub enum Command {
    AlterPipeline(AlterPipelineCommand),
}
