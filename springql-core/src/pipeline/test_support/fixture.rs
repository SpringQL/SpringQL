// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

use std::{net::IpAddr, sync::Arc};

use crate::{
    pipeline::{
        name::{ColumnName, PumpName, SinkWriterName, SourceReaderName, StreamName},
        option::{options_builder::OptionsBuilder, Options},
        pump_model::PumpModel,
        relation::{
            column::{
                column_constraint::ColumnConstraint, column_data_type::ColumnDataType,
                column_definition::ColumnDefinition,
            },
            sql_type::SqlType,
        },
        sink_stream_model::SinkStreamModel,
        sink_writer_model::{sink_writer_type::SinkWriterType, SinkWriterModel},
        source_reader_model::{source_reader_type::SourceReaderType, SourceReaderModel},
        source_stream_model::SourceStreamModel,
        stream_model::{stream_shape::StreamShape, StreamModel},
        Pipeline,
    },
    stream_engine::command::{
        insert_plan::InsertPlan,
        query_plan::{query_plan_operation::QueryPlanOperation, QueryPlan},
    },
};

impl Pipeline {
    /// ```text
    /// (0)--a-->[1]
    /// ```
    pub(crate) fn fx_source_only() -> Self {
        let st_1 = Arc::new(SourceStreamModel::fx_trade_with_name(StreamName::factory(
            "st_1",
        )));

        let mut pipeline = Pipeline::default();
        pipeline.add_source_stream(st_1).unwrap();
        pipeline
    }

    /// ```text
    /// (0)--a-->[1]--b-->[2]--c-->
    /// ```
    pub(crate) fn fx_linear(
        source_remote_host: IpAddr,
        source_remote_port: u16,
        sink_remote_host: IpAddr,
        sink_remote_port: u16,
    ) -> Self {
        let st_1 = Arc::new(SourceStreamModel::fx_trade_with_name(StreamName::factory(
            "st_1",
        )));
        let st_2 = Arc::new(SinkStreamModel::fx_trade_with_name(StreamName::factory(
            "st_2",
        )));

        let source_a =
            SourceReaderModel::fx_net(st_1.name().clone(), source_remote_host, source_remote_port);
        let sink_c =
            SinkWriterModel::fx_net(st_2.name().clone(), sink_remote_host, sink_remote_port);

        let pu_b = PumpModel::fx_trade(
            PumpName::factory("pu_b"),
            st_1.name().clone(),
            st_2.name().clone(),
        );

        let mut pipeline = Pipeline::default();

        pipeline.add_source_stream(st_1).unwrap();
        pipeline.add_sink_stream(st_2).unwrap();

        pipeline.add_source_reader(source_a).unwrap();
        pipeline.add_sink_writer(sink_c).unwrap();

        pipeline.add_pump(pu_b).unwrap();

        pipeline
    }

    /// ```text
    /// (0)--a-->[1]--c-->[3]--e-->
    ///  |
    ///  +---b-->[2]--d-->[4]--f-->
    /// ```
    pub(crate) fn fx_split(
        source_remote_host: IpAddr,
        source_remote_port: u16,
        sink1_remote_host: IpAddr,
        sink1_remote_port: u16,
        sink2_remote_host: IpAddr,
        sink2_remote_port: u16,
    ) -> Self {
        let st_1 = Arc::new(SourceStreamModel::fx_trade_with_name(StreamName::factory(
            "st_1",
        )));
        let st_2 = Arc::new(SourceStreamModel::fx_trade_with_name(StreamName::factory(
            "st_2",
        )));

        let st_3 = Arc::new(SinkStreamModel::fx_trade_with_name(StreamName::factory(
            "st_3",
        )));
        let st_4 = Arc::new(SinkStreamModel::fx_trade_with_name(StreamName::factory(
            "st_4",
        )));

        let source_a =
            SourceReaderModel::fx_net(st_1.name().clone(), source_remote_host, source_remote_port);
        let source_b =
            SourceReaderModel::fx_net(st_2.name().clone(), source_remote_host, source_remote_port);

        let sink_e =
            SinkWriterModel::fx_net(st_3.name().clone(), sink1_remote_host, sink1_remote_port);
        let sink_f =
            SinkWriterModel::fx_net(st_4.name().clone(), sink2_remote_host, sink2_remote_port);

        let pu_c = PumpModel::fx_trade(
            PumpName::factory("pu_c"),
            st_1.name().clone(),
            st_3.name().clone(),
        );
        let pu_d = PumpModel::fx_trade(
            PumpName::factory("pu_d"),
            st_2.name().clone(),
            st_4.name().clone(),
        );

        let mut pipeline = Pipeline::default();

        pipeline.add_source_stream(st_1).unwrap();
        pipeline.add_source_stream(st_2).unwrap();

        pipeline.add_sink_stream(st_3).unwrap();
        pipeline.add_sink_stream(st_4).unwrap();

        pipeline.add_source_reader(source_a).unwrap();
        pipeline.add_source_reader(source_b).unwrap();

        pipeline.add_sink_writer(sink_e).unwrap();
        pipeline.add_sink_writer(sink_f).unwrap();

        pipeline.add_pump(pu_c).unwrap();
        pipeline.add_pump(pu_d).unwrap();

        pipeline
    }

    /// ```text
    /// (0)--a-->[1]--c-->[3]--e-->
    ///  |                 ^
    ///  |                 |
    ///  +---b-->[2]--d----+
    /// ```
    pub(crate) fn fx_split_merge(
        source_remote_host: IpAddr,
        source_remote_port: u16,
        sink_remote_host: IpAddr,
        sink_remote_port: u16,
    ) -> Self {
        let st_1 = Arc::new(SourceStreamModel::fx_trade_with_name(StreamName::factory(
            "st_1",
        )));
        let st_2 = Arc::new(SourceStreamModel::fx_trade_with_name(StreamName::factory(
            "st_2",
        )));

        let st_3 = Arc::new(SinkStreamModel::fx_trade_with_name(StreamName::factory(
            "st_3",
        )));

        let source_a =
            SourceReaderModel::fx_net(st_1.name().clone(), source_remote_host, source_remote_port);
        let source_b =
            SourceReaderModel::fx_net(st_2.name().clone(), source_remote_host, source_remote_port);

        let sink_e =
            SinkWriterModel::fx_net(st_3.name().clone(), sink_remote_host, sink_remote_port);

        let pu_c = PumpModel::fx_trade(
            PumpName::factory("pu_c"),
            st_1.name().clone(),
            st_3.name().clone(),
        );
        let pu_d = PumpModel::fx_trade(
            PumpName::factory("pu_d"),
            st_2.name().clone(),
            st_3.name().clone(),
        );

        let mut pipeline = Pipeline::default();

        pipeline.add_source_stream(st_1).unwrap();
        pipeline.add_source_stream(st_2).unwrap();

        pipeline.add_sink_stream(st_3).unwrap();

        pipeline.add_source_reader(source_a).unwrap();
        pipeline.add_source_reader(source_b).unwrap();

        pipeline.add_sink_writer(sink_e).unwrap();

        pipeline.add_pump(pu_c).unwrap();
        pipeline.add_pump(pu_d).unwrap();

        pipeline
    }

    /// ```text
    /// (0)--a-->[1]--c-->[3]--f-->[4]--g-->[5]--h-->[6]--j-->[8]--l-->
    ///  |                          ^       ^ |
    ///  |                          |       | |
    ///  +---b-->[2]-------d--------+       | +--i-->[7]--k-->[9]--m-->
    ///           |                         |
    ///           +--------------e----------+
    /// ```
    pub(crate) fn fx_complex(
        source_remote_host: IpAddr,
        source_remote_port: u16,
        sink1_remote_host: IpAddr,
        sink1_remote_port: u16,
        sink2_remote_host: IpAddr,
        sink2_remote_port: u16,
    ) -> Self {
        let st_1 = Arc::new(SourceStreamModel::fx_trade_with_name(StreamName::factory(
            "st_1",
        )));
        let st_2 = Arc::new(SourceStreamModel::fx_trade_with_name(StreamName::factory(
            "st_2",
        )));
        let st_3 = Arc::new(StreamModel::fx_trade_with_name(StreamName::factory("st_3")));
        let st_4 = Arc::new(StreamModel::fx_trade_with_name(StreamName::factory("st_4")));
        let st_5 = Arc::new(StreamModel::fx_trade_with_name(StreamName::factory("st_5")));
        let st_6 = Arc::new(StreamModel::fx_trade_with_name(StreamName::factory("st_6")));
        let st_7 = Arc::new(StreamModel::fx_trade_with_name(StreamName::factory("st_7")));
        let st_8 = Arc::new(SinkStreamModel::fx_trade_with_name(StreamName::factory(
            "st_8",
        )));
        let st_9 = Arc::new(SinkStreamModel::fx_trade_with_name(StreamName::factory(
            "st_9",
        )));

        let source_a =
            SourceReaderModel::fx_net(st_1.name().clone(), source_remote_host, source_remote_port);
        let source_b =
            SourceReaderModel::fx_net(st_2.name().clone(), source_remote_host, source_remote_port);

        let sink_l =
            SinkWriterModel::fx_net(st_8.name().clone(), sink1_remote_host, sink1_remote_port);
        let sink_m =
            SinkWriterModel::fx_net(st_9.name().clone(), sink2_remote_host, sink2_remote_port);

        let pu_c = PumpModel::fx_trade(
            PumpName::factory("pu_c"),
            st_1.name().clone(),
            st_3.name().clone(),
        );
        let pu_d = PumpModel::fx_trade(
            PumpName::factory("pu_d"),
            st_2.name().clone(),
            st_4.name().clone(),
        );
        let pu_e = PumpModel::fx_trade(
            PumpName::factory("pu_e"),
            st_2.name().clone(),
            st_5.name().clone(),
        );
        let pu_f = PumpModel::fx_trade(
            PumpName::factory("pu_f"),
            st_3.name().clone(),
            st_4.name().clone(),
        );
        let pu_g = PumpModel::fx_trade(
            PumpName::factory("pu_g"),
            st_4.name().clone(),
            st_5.name().clone(),
        );
        let pu_h = PumpModel::fx_trade(
            PumpName::factory("pu_h"),
            st_5.name().clone(),
            st_6.name().clone(),
        );
        let pu_i = PumpModel::fx_trade(
            PumpName::factory("pu_i"),
            st_5.name().clone(),
            st_7.name().clone(),
        );
        let pu_j = PumpModel::fx_trade(
            PumpName::factory("pu_j"),
            st_6.name().clone(),
            st_8.name().clone(),
        );
        let pu_k = PumpModel::fx_trade(
            PumpName::factory("pu_k"),
            st_7.name().clone(),
            st_9.name().clone(),
        );

        let mut pipeline = Pipeline::default();

        pipeline.add_source_stream(st_1).unwrap();
        pipeline.add_source_stream(st_2).unwrap();

        pipeline.add_sink_stream(st_8).unwrap();
        pipeline.add_sink_stream(st_9).unwrap();

        pipeline.add_source_reader(source_a).unwrap();
        pipeline.add_source_reader(source_b).unwrap();

        pipeline.add_sink_writer(sink_l).unwrap();
        pipeline.add_sink_writer(sink_m).unwrap();

        pipeline.add_stream(st_3).unwrap();
        pipeline.add_stream(st_4).unwrap();
        pipeline.add_stream(st_5).unwrap();
        pipeline.add_stream(st_6).unwrap();
        pipeline.add_stream(st_7).unwrap();

        pipeline.add_pump(pu_c).unwrap();
        pipeline.add_pump(pu_d).unwrap();
        pipeline.add_pump(pu_e).unwrap();
        pipeline.add_pump(pu_f).unwrap();
        pipeline.add_pump(pu_g).unwrap();
        pipeline.add_pump(pu_h).unwrap();
        pipeline.add_pump(pu_i).unwrap();
        pipeline.add_pump(pu_j).unwrap();
        pipeline.add_pump(pu_k).unwrap();

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
    pub(crate) fn fx_trade_with_name(name: StreamName) -> Self {
        Self::new(name, Arc::new(StreamShape::fx_trade()))
    }
}

impl SourceStreamModel {
    pub(crate) fn fx_trade_with_name(name: StreamName) -> Self {
        Self::new(StreamModel::new(name, Arc::new(StreamShape::fx_trade())))
    }
}

impl SinkStreamModel {
    pub(crate) fn fx_trade_with_name(name: StreamName) -> Self {
        Self::new(StreamModel::new(name, Arc::new(StreamShape::fx_trade())))
    }
}

impl SourceReaderModel {
    pub(crate) fn fx_net(
        source_stream_name: StreamName,
        remote_host: IpAddr,
        remote_port: u16,
    ) -> Self {
        Self::new(
            SourceReaderName::fx_tcp_trade(),
            SourceReaderType::Net,
            source_stream_name,
            Options::fx_net(remote_host, remote_port),
        )
    }
}
impl SinkWriterModel {
    pub(crate) fn fx_net(
        sink_stream_name: StreamName,
        remote_host: IpAddr,
        remote_port: u16,
    ) -> Self {
        Self::new(
            SinkWriterName::fx_tcp_trade(),
            SinkWriterType::Net,
            sink_stream_name,
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

        let collection_op = QueryPlanOperation::fx_collect(upstream);
        let projection_op = QueryPlanOperation::fx_projection(column_names);

        query_plan.add_root(projection_op.clone());
        query_plan.add_left(&projection_op, collection_op);
        query_plan
    }
}

impl QueryPlanOperation {
    pub(crate) fn fx_collect(upstream: StreamName) -> Self {
        Self::Collect { stream: upstream }
    }

    pub(crate) fn fx_projection(column_names: Vec<ColumnName>) -> Self {
        Self::Projection { column_names }
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
