// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

use serde::{Deserialize, Serialize};

use crate::stream_engine::command::{insert_plan::InsertPlan, query_plan::QueryPlan};

use super::name::{PumpName, StreamName};

#[derive(Clone, Eq, PartialEq, Debug, Serialize, Deserialize, new)]
pub(crate) struct PumpModel {
    name: PumpName,
    query_plan: QueryPlan,
    insert_plan: InsertPlan,
}

impl PumpModel {
    pub(crate) fn name(&self) -> &PumpName {
        &self.name
    }

    pub(crate) fn query_plan(&self) -> &QueryPlan {
        &self.query_plan
    }

    pub(crate) fn insert_plan(&self) -> &InsertPlan {
        &self.insert_plan
    }

    /// Has more than 1 upstreams on JOIN, for example.
    pub(crate) fn upstreams(&self) -> Vec<&StreamName> {
        self.query_plan.upstreams()
    }

    pub(crate) fn downstream(&self) -> &StreamName {
        self.insert_plan.stream()
    }
}
