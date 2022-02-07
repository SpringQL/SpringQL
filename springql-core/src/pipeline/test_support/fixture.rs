// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

use std::{net::IpAddr, sync::Arc};

use crate::{
    low_level_rs::{SpringConfig, SpringSinkWriterConfig, SpringSourceReaderConfig},
    pipeline::{
        field::field_name::ColumnReference,
        name::{ColumnName, PumpName, SinkWriterName, SourceReaderName, StreamName},
        option::{options_builder::OptionsBuilder, Options},
        pipeline_version::PipelineVersion,
        pump_model::PumpModel,
        relation::{
            column::{
                column_constraint::ColumnConstraint, column_data_type::ColumnDataType,
                column_definition::ColumnDefinition,
            },
            sql_type::SqlType,
        },
        sink_writer_model::{sink_writer_type::SinkWriterType, SinkWriterModel},
        source_reader_model::{source_reader_type::SourceReaderType, SourceReaderModel},
        stream_model::{stream_shape::StreamShape, StreamModel},
        Pipeline,
    },
    stream_engine::command::{
        insert_plan::InsertPlan,
        query_plan::{query_plan_operation::QueryPlanOperation, QueryPlan},
    },
};

impl SpringConfig {
    pub(crate) fn fx_default() -> Self {
        Self::new("").unwrap()
    }
}

impl SpringSourceReaderConfig {
    pub(crate) fn fx_default() -> Self {
        let c = SpringConfig::fx_default();
        c.source_reader
    }
}

impl SpringSinkWriterConfig {
    pub(crate) fn fx_default() -> Self {
        let c = SpringConfig::fx_default();
        c.sink_writer
    }
}

impl Pipeline {
    /// ```text
    /// (0)--a-->[1]
    /// ```
    pub(crate) fn fx_source_only() -> Self {
        let st_1 = Arc::new(StreamModel::fx_trade_with_name(StreamName::factory("st_1")));

        let mut pipeline = Pipeline::new(PipelineVersion::new());
        pipeline.add_stream(st_1).unwrap();
        pipeline
    }

    pub(crate) fn fx_sink_only() -> Self {
        let sink_1 = Arc::new(StreamModel::fx_trade_with_name(StreamName::factory(
            "sink_1",
        )));

        let mut pipeline = Pipeline::new(PipelineVersion::new());
        pipeline.add_stream(sink_1).unwrap();
        pipeline
    }
}

impl StreamShape {
    pub(crate) fn fx_city_temperature() -> Self {
        Self::new(vec![
            ColumnDefinition::fx_timestamp(),
            ColumnDefinition::fx_city(),
            ColumnDefinition::fx_temperature(),
        ])
        .unwrap()
    }
    pub(crate) fn fx_trade() -> Self {
        Self::new(vec![
            ColumnDefinition::fx_timestamp(),
            ColumnDefinition::fx_ticker(),
            ColumnDefinition::fx_amount(),
        ])
        .unwrap()
    }

    pub(crate) fn fx_no_promoted_rowtime() -> Self {
        Self::new(vec![ColumnDefinition::fx_amount()]).unwrap()
    }
}

impl StreamModel {
    pub(crate) fn fx_city_temperature() -> Self {
        Self::new(
            StreamName::fx_city_temperature(),
            StreamShape::fx_city_temperature(),
        )
    }

    pub(crate) fn fx_trade() -> Self {
        Self::new(StreamName::fx_trade(), StreamShape::fx_trade())
    }

    pub(crate) fn fx_trade_with_name(name: StreamName) -> Self {
        Self::new(name, StreamShape::fx_trade())
    }
}

impl SourceReaderModel {
    pub(crate) fn fx_net(stream_name: StreamName, remote_host: IpAddr, remote_port: u16) -> Self {
        Self::new(
            SourceReaderName::fx_tcp_trade(),
            SourceReaderType::Net,
            stream_name,
            Options::fx_net(remote_host, remote_port),
        )
    }
}
impl SinkWriterModel {
    pub(crate) fn fx_net(stream_name: StreamName, remote_host: IpAddr, remote_port: u16) -> Self {
        Self::new(
            SinkWriterName::fx_tcp_trade(),
            SinkWriterType::Net,
            stream_name,
            Options::fx_net(remote_host, remote_port),
        )
    }
}

impl PumpModel {
    pub(crate) fn fx_trade(name: PumpName, upstream: StreamName, downstream: StreamName) -> Self {
        let select_columns = vec![
            ColumnName::fx_timestamp(),
            ColumnName::fx_ticker(),
            ColumnName::fx_amount(),
        ];
        Self::new(
            name,
            QueryPlan::fx_collect_projection(upstream, select_columns),
            InsertPlan::fx_trade(downstream),
        )
    }
}

impl QueryPlan {
    pub(crate) fn fx_collect_projection(
        upstream: StreamName,
        column_names: Vec<ColumnName>,
    ) -> Self {
        let mut query_plan = QueryPlan::default();

        let colrefs = column_names
            .into_iter()
            .map(|column_name| ColumnReference::new(upstream.clone(), column_name))
            .collect::<Vec<ColumnReference>>();

        let collection_op = QueryPlanOperation::fx_collect(upstream);
        let projection_op = QueryPlanOperation::fx_projection(colrefs);

        query_plan.add_root(projection_op.clone());
        query_plan.add_left(&projection_op, collection_op);
        query_plan
    }
}

impl QueryPlanOperation {
    pub(crate) fn fx_collect(upstream: StreamName) -> Self {
        Self::Collect { stream: upstream }
    }

    pub(crate) fn fx_projection(column_references: Vec<ColumnReference>) -> Self {
        Self::Projection { column_references }
    }
}

impl InsertPlan {
    pub(crate) fn fx_trade(downstream: StreamName) -> Self {
        Self::new(
            downstream,
            vec![
                ColumnName::fx_timestamp(),
                ColumnName::fx_ticker(),
                ColumnName::fx_amount(),
            ],
        )
    }
}

impl ColumnDefinition {
    pub(crate) fn fx_timestamp() -> Self {
        Self::new(
            ColumnDataType::fx_timestamp(),
            vec![ColumnConstraint::Rowtime],
        )
    }

    pub(crate) fn fx_city() -> Self {
        Self::new(ColumnDataType::fx_city(), vec![])
    }

    pub(crate) fn fx_temperature() -> Self {
        Self::new(ColumnDataType::fx_temperature(), vec![])
    }

    pub(crate) fn fx_ticker() -> Self {
        Self::new(ColumnDataType::fx_ticker(), vec![])
    }

    pub(crate) fn fx_amount() -> Self {
        Self::new(ColumnDataType::fx_amount(), vec![])
    }
}

impl ColumnDataType {
    pub(crate) fn fx_timestamp() -> Self {
        Self::new(ColumnName::fx_timestamp(), SqlType::timestamp(), false)
    }

    pub(crate) fn fx_city() -> Self {
        Self::new(ColumnName::new("city".to_string()), SqlType::text(), false)
    }

    pub(crate) fn fx_temperature() -> Self {
        Self::new(
            ColumnName::new("temperature".to_string()),
            SqlType::integer(),
            false,
        )
    }

    pub(crate) fn fx_ticker() -> Self {
        Self::new(ColumnName::fx_ticker(), SqlType::text(), false)
    }

    pub(crate) fn fx_amount() -> Self {
        Self::new(ColumnName::fx_amount(), SqlType::integer(), false)
    }
}

impl StreamName {
    pub(crate) fn fx_trade() -> Self {
        Self::new("trade".to_string())
    }
    pub(crate) fn fx_city_temperature() -> Self {
        Self::new("city_temperature".to_string())
    }
}

impl ColumnName {
    pub(crate) fn fx_timestamp() -> Self {
        Self::new("ts".to_string())
    }
    pub(crate) fn fx_ticker() -> Self {
        Self::new("ticker".to_string())
    }
    pub(crate) fn fx_amount() -> Self {
        Self::new("amount".to_string())
    }
}

impl Options {
    pub(crate) fn fx_net(remote_host: IpAddr, remote_port: u16) -> Self {
        OptionsBuilder::default()
            .add("PROTOCOL", "TCP")
            .add("REMOTE_HOST", remote_host.to_string())
            .add("REMOTE_PORT", remote_port.to_string())
            .build()
    }
}

impl SourceReaderName {
    pub(crate) fn fx_tcp_trade() -> Self {
        Self::new("tcp_source_trade".to_string())
    }
}

impl SinkWriterName {
    pub(crate) fn fx_tcp_trade() -> Self {
        Self::new("tcp_sink_trade".to_string())
    }
}
