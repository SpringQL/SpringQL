use std::sync::Arc;

use crate::{
    model::{
        name::{ColumnName, PumpName, StreamName},
        option::options_builder::OptionsBuilder,
        query_plan::query_plan_node::{operation::LeafOperation, QueryPlanNodeLeaf},
    },
    stream_engine::{
        autonomous_executor::{
            data::{
                column::stream_column::StreamColumns,
                column_values::ColumnValues,
                foreign_row::format::json::JsonObject,
                row::Row,
                value::sql_value::{nn_sql_value::NnSqlValue, SqlValue},
            },
            server_instance::{
                server_repository::ServerRepository,
                source::{net::NetSourceServerInstance, SourceServerInstance},
            },
            test_support::foreign::source::TestSource,
        },
        pipeline::stream_model::stream_shape::StreamShape,
        RowRepository,
    },
    stream_engine::{
        autonomous_executor::{
            task::{task_context::TaskContext, task_id::TaskId},
            Timestamp,
        },
        dependency_injection::{test_di::TestDI, DependencyInjection},
    },
};

impl NetSourceServerInstance {
    pub(in crate::stream_engine) fn factory_with_test_source(inputs: Vec<JsonObject>) -> Self {
        let source = TestSource::start(inputs).unwrap();

        let options = OptionsBuilder::default()
            .add("PROTOCOL", "TCP")
            .add("REMOTE_HOST", source.host_ip().to_string())
            .add("REMOTE_PORT", source.port().to_string())
            .build();

        NetSourceServerInstance::start(&options).unwrap()
    }
}

impl StreamColumns {
    pub(in crate::stream_engine) fn factory_city_temperature(
        timestamp: Timestamp,
        city: &str,
        temperature: i32,
    ) -> Self {
        let mut column_values = ColumnValues::default();
        column_values
            .insert(
                ColumnName::new("timestamp".to_string()),
                SqlValue::NotNull(NnSqlValue::Timestamp(timestamp)),
            )
            .unwrap();
        column_values
            .insert(
                ColumnName::new("city".to_string()),
                SqlValue::NotNull(NnSqlValue::Text(city.to_string())),
            )
            .unwrap();
        column_values
            .insert(
                ColumnName::new("temperature".to_string()),
                SqlValue::NotNull(NnSqlValue::Integer(temperature)),
            )
            .unwrap();

        Self::new(Arc::new(StreamShape::fx_city_temperature()), column_values).unwrap()
    }

    pub(in crate::stream_engine) fn factory_trade(
        timestamp: Timestamp,
        ticker: &str,
        amount: i16,
    ) -> Self {
        let mut column_values = ColumnValues::default();
        column_values
            .insert(
                ColumnName::new("timestamp".to_string()),
                SqlValue::NotNull(NnSqlValue::Timestamp(timestamp)),
            )
            .unwrap();
        column_values
            .insert(
                ColumnName::new("ticker".to_string()),
                SqlValue::NotNull(NnSqlValue::Text(ticker.to_string())),
            )
            .unwrap();
        column_values
            .insert(
                ColumnName::new("amount".to_string()),
                SqlValue::NotNull(NnSqlValue::SmallInt(amount)),
            )
            .unwrap();

        Self::new(Arc::new(StreamShape::fx_trade()), column_values).unwrap()
    }

    pub(in crate::stream_engine) fn factory_no_promoted_rowtime(amount: i32) -> Self {
        let mut column_values = ColumnValues::default();
        column_values
            .insert(
                ColumnName::new("amount".to_string()),
                SqlValue::NotNull(NnSqlValue::Integer(amount)),
            )
            .unwrap();

        Self::new(
            Arc::new(StreamShape::fx_no_promoted_rowtime()),
            column_values,
        )
        .unwrap()
    }
}

impl Row {
    pub(in crate::stream_engine) fn factory_city_temperature(
        timestamp: Timestamp,
        city: &str,
        temperature: i32,
    ) -> Self {
        Self::new::<TestDI>(StreamColumns::factory_city_temperature(
            timestamp,
            city,
            temperature,
        ))
    }
    pub(in crate::stream_engine) fn factory_trade(
        timestamp: Timestamp,
        ticker: &str,
        amount: i16,
    ) -> Self {
        Self::new::<TestDI>(StreamColumns::factory_trade(timestamp, ticker, amount))
    }
}

impl QueryPlanNodeLeaf {
    pub(in crate::stream_engine) fn factory_with_task_in<DI>(
        input: Vec<Row>,
        context: &TaskContext<DI>,
    ) -> Self
    where
        DI: DependencyInjection,
    {
        for row in input {
            context
                .row_repository()
                .emit_owned(row, &[context.task()])
                .unwrap();
        }

        Self {
            op: LeafOperation::Collect,
        }
    }
}

impl StreamName {
    pub(in crate::stream_engine) fn factory(name: &str) -> Self {
        Self::new(name.to_string())
    }
}

impl PumpName {
    pub(in crate::stream_engine) fn factory(name: &str) -> Self {
        Self::new(name.to_string())
    }
}

impl<DI: DependencyInjection> TaskContext<DI> {
    pub(in crate::stream_engine) fn factory_with_1_level_downstreams(
        task: TaskId,
        downstream_tasks: Vec<TaskId>,
    ) -> Self {
        let row_repo = DI::RowRepositoryType::default();
        let server_repo = ServerRepository::default();

        let mut tasks = downstream_tasks.clone();
        tasks.push(task.clone());
        row_repo.reset(tasks);

        Self::_test_factory(
            task,
            downstream_tasks,
            Arc::new(row_repo),
            Arc::new(server_repo),
        )
    }
}
