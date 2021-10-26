mod node_executor;

use std::rc::Rc;

use self::node_executor::collect_executor::CollectExecutor;
use self::node_executor::{CollectNodeExecutor, NodeExecutor};

use super::final_row::FinalRow;
use super::interm_row::NewRow;
use crate::dependency_injection::DependencyInjection;
use crate::error::Result;
use crate::model::query_plan::query_plan_node::operation::LeafOperation;
use crate::model::query_plan::query_plan_node::QueryPlanNode;
use crate::model::query_plan::QueryPlan;
use crate::stream_engine::executor::data::row::Row;

#[derive(Debug)]
pub(super) struct NodeExecutorTree<DI>
where
    DI: DependencyInjection,
{
    root: NodeExecutor<DI>,

    /// Some(_) means: Output of the query plan is this NewRow.
    /// None means: Output of the query plan is the input of it.
    latest_new_row: Option<NewRow>,
}

impl<DI> NodeExecutorTree<DI>
where
    DI: DependencyInjection,
{
    pub(super) fn compile(di: Rc<DI>, query_plan: QueryPlan) -> Self {
        let plan_root = query_plan.root();
        let root = Self::compile_node(di, plan_root);

        Self {
            root,
            latest_new_row: None,
        }
    }

    fn compile_node(di: Rc<DI>, plan_node: Rc<QueryPlanNode>) -> NodeExecutor<DI> {
        match plan_node.as_ref() {
            QueryPlanNode::Leaf(leaf_node) => match &leaf_node.op {
                LeafOperation::Collect { pump } => NodeExecutor::Collect(
                    CollectNodeExecutor::Collect(CollectExecutor::new(di, pump.clone())),
                ),
            },
        }
    }

    /// # Failure
    ///
    /// - [SpringError::InputTimeout](crate::error::SpringError::InputTimeout) when:
    ///   - Input from a source stream is not available within timeout period.
    pub(super) fn run(&mut self) -> Result<FinalRow> {
        let row = Self::run_dfs_post_order(&self.root, &mut self.latest_new_row)?;

        if let Some(new_row) = self.latest_new_row.take() {
            Ok(FinalRow::NewlyCreated(new_row.into()))
        } else {
            Ok(FinalRow::Preserved(row))
        }
    }

    fn run_dfs_post_order(
        executor: &NodeExecutor<DI>,
        latest_new_row: &mut Option<NewRow>,
    ) -> Result<Rc<Row>> {
        match executor {
            NodeExecutor::Collect(e) => match e {
                CollectNodeExecutor::Collect(executor) => executor.run(),
            },
            NodeExecutor::Stream(_) => todo!(),
            NodeExecutor::Window(_) => todo!(),
        }
    }
}
