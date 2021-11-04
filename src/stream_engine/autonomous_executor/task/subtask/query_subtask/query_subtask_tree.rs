mod node_executor;

use std::rc::Rc;
use std::sync::Arc;

use self::node_executor::collect_executor::CollectExecutor;
use self::node_executor::{CollectNodeExecutor, NodeExecutor};

use super::final_row::FinalRow;
use super::interm_row::NewRow;
use crate::error::Result;
use crate::model::query_plan::query_plan_node::operation::LeafOperation;
use crate::model::query_plan::query_plan_node::QueryPlanNode;
use crate::model::query_plan::QueryPlan;
use crate::stream_engine::autonomous_executor::row::Row;
use crate::stream_engine::autonomous_executor::task::task_context::TaskContext;
use crate::stream_engine::dependency_injection::DependencyInjection;

#[derive(Debug)]
pub(super) struct QuerySubtaskTree {
    root: NodeExecutor,

    /// Some(_) means: Output of the query plan is this NewRow.
    /// None means: Output of the query plan is the input of it.
    latest_new_row: Option<NewRow>,
}

impl QuerySubtaskTree {
    pub(super) fn compile(query_plan: QueryPlan) -> Self {
        let plan_root = query_plan.root();
        let root = Self::compile_node(plan_root);

        Self {
            root,
            latest_new_row: None,
        }
    }

    fn compile_node(plan_node: Rc<QueryPlanNode>) -> NodeExecutor {
        match plan_node.as_ref() {
            QueryPlanNode::Leaf(leaf_node) => match &leaf_node.op {
                LeafOperation::Collect => {
                    NodeExecutor::Collect(CollectNodeExecutor::Collect(CollectExecutor::new()))
                }
            },
        }
    }

    /// # Failure
    ///
    /// - [SpringError::InputTimeout](crate::error::SpringError::InputTimeout) when:
    ///   - Input from a source stream is not available within timeout period.
    pub(super) fn run<DI: DependencyInjection>(
        &mut self,
        context: &TaskContext<DI>,
    ) -> Result<FinalRow> {
        let row = Self::run_dfs_post_order::<DI>(&self.root, &mut self.latest_new_row, context)?;

        if let Some(new_row) = self.latest_new_row.take() {
            Ok(FinalRow::NewlyCreated(new_row.into()))
        } else {
            Ok(FinalRow::Preserved(row))
        }
    }

    fn run_dfs_post_order<DI: DependencyInjection>(
        executor: &NodeExecutor,
        latest_new_row: &mut Option<NewRow>,
        context: &TaskContext<DI>,
    ) -> Result<Arc<Row>> {
        match executor {
            NodeExecutor::Collect(e) => match e {
                CollectNodeExecutor::Collect(executor) => executor.run::<DI>(context),
            },
            NodeExecutor::Stream(_) => todo!(),
            NodeExecutor::Window(_) => todo!(),
        }
    }
}
