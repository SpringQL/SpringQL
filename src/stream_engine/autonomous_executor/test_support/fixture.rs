use std::{net::IpAddr, sync::Arc};

use serde_json::json;

use crate::{
    model::{
        column::{column_data_type::ColumnDataType, column_definition::ColumnDefinition},
        name::{ColumnName, PumpName, StreamName},
        option::{options_builder::OptionsBuilder, Options},
        sql_type::SqlType,
    },
    stream_engine::{
        autonomous_executor::{
            data::{
                column::stream_column::StreamColumns,
                foreign_row::{
                    foreign_sink_row::ForeignSinkRow, foreign_source_row::ForeignSourceRow,
                    format::json::JsonObject,
                },
                row::Row,
                timestamp::Timestamp,
            },
            task::{task_graph::TaskGraph, task_id::TaskId},
            test_support::foreign::sink::TestSink,
        },
        command::alter_pipeline_command::{
            alter_pump_command::AlterPumpCommand, AlterPipelineCommand,
        },
        pipeline::{
            pipeline_graph::PipelineGraph,
            pump_model::pump_state::PumpState,
            stream_model::{stream_shape::StreamShape, StreamModel},
            Pipeline,
        },
    },
    stream_engine::{
        dependency_injection::test_di::TestDI,
        pipeline::{
            foreign_stream_model::ForeignStreamModel,
            pump_model::PumpModel,
            server_model::{server_type::ServerType, ServerModel},
        },
    },
};

use super::foreign::source::TestSource;

impl Timestamp {
    pub(in crate::stream_engine) fn fx_now() -> Self {
        "2000-01-01 12:00:00.123456789".parse().unwrap()
    }

    pub(in crate::stream_engine) fn fx_ts1() -> Self {
        "2021-01-01 13:00:00.000000001".parse().unwrap()
    }
    pub(in crate::stream_engine) fn fx_ts2() -> Self {
        "2021-01-01 13:00:00.000000002".parse().unwrap()
    }
    pub(in crate::stream_engine) fn fx_ts3() -> Self {
        "2021-01-01 13:00:00.000000003".parse().unwrap()
    }
}

impl JsonObject {
    pub(in crate::stream_engine) fn fx_city_temperature_tokyo() -> Self {
        Self::new(json!({
            "timestamp": Timestamp::fx_ts1().to_string(),
            "city": "Tokyo",
            "temperature": 21,
        }))
    }

    pub(in crate::stream_engine) fn fx_city_temperature_osaka() -> Self {
        Self::new(json!({
            "timestamp": Timestamp::fx_ts2().to_string(),
            "city": "Osaka",
            "temperature": 23,
        }))
    }

    pub(in crate::stream_engine) fn fx_city_temperature_london() -> Self {
        Self::new(json!({
            "timestamp": Timestamp::fx_ts3().to_string(),
            "city": "London",
            "temperature": 13,
        }))
    }

    pub(in crate::stream_engine) fn fx_trade_oracle() -> Self {
        Self::new(json!({
            "timestamp": Timestamp::fx_ts1().to_string(),
            "ticker": "ORCL",
            "amount": 20,
        }))
    }

    pub(in crate::stream_engine) fn fx_trade_ibm() -> Self {
        Self::new(json!({
            "timestamp": Timestamp::fx_ts2().to_string(),
            "ticker": "IBM",
            "amount": 30,
        }))
    }

    pub(in crate::stream_engine) fn fx_trade_google() -> Self {
        Self::new(json!({
            "timestamp": Timestamp::fx_ts3().to_string(),
            "ticker": "GOOGL",
            "amount": 100,
        }))
    }
}

impl ForeignSourceRow {
    pub(in crate::stream_engine) fn fx_city_temperature_tokyo() -> Self {
        Self::from_json(JsonObject::fx_city_temperature_tokyo())
    }
    pub(in crate::stream_engine) fn fx_city_temperature_osaka() -> Self {
        Self::from_json(JsonObject::fx_city_temperature_osaka())
    }
    pub(in crate::stream_engine) fn fx_city_temperature_london() -> Self {
        Self::from_json(JsonObject::fx_city_temperature_london())
    }

    pub(in crate::stream_engine) fn fx_trade_oracle() -> Self {
        Self::from_json(JsonObject::fx_trade_oracle())
    }
    pub(in crate::stream_engine) fn fx_trade_ibm() -> Self {
        Self::from_json(JsonObject::fx_trade_ibm())
    }
    pub(in crate::stream_engine) fn fx_trade_google() -> Self {
        Self::from_json(JsonObject::fx_trade_google())
    }
}

impl ForeignSinkRow {
    pub(in crate::stream_engine) fn fx_city_temperature_tokyo() -> Self {
        Row::fx_city_temperature_tokyo().into()
    }
    pub(in crate::stream_engine) fn fx_city_temperature_osaka() -> Self {
        Row::fx_city_temperature_osaka().into()
    }
    pub(in crate::stream_engine) fn fx_city_temperature_london() -> Self {
        Row::fx_city_temperature_london().into()
    }

    pub(in crate::stream_engine) fn fx_trade_oracle() -> Self {
        Row::fx_trade_oracle().into()
    }
    pub(in crate::stream_engine) fn fx_trade_ibm() -> Self {
        Row::fx_trade_ibm().into()
    }
    pub(in crate::stream_engine) fn fx_trade_google() -> Self {
        Row::fx_trade_google().into()
    }
}

impl Pipeline {
    /// ```text
    /// (0)--a-->[1]
    /// ```
    pub(in crate::stream_engine) fn fx_source_only() -> Self {
        let test_source = TestSource::start(vec![]).unwrap();

        let fst_1 = Arc::new(ForeignStreamModel::fx_trade_with_name(StreamName::factory(
            "fst_1",
        )));

        let server_a =
            ServerModel::fx_net_source(fst_1.clone(), test_source.host_ip(), test_source.port());

        let mut pipeline = Pipeline::default();
        pipeline.add_foreign_stream(fst_1).unwrap();
        pipeline.add_server(server_a).unwrap();
        pipeline
    }

    /// ```text
    /// (0)--a-->[1]--b(STOPPED)-->[2]--c-->
    /// ```
    pub(in crate::stream_engine) fn fx_linear_stopped() -> Self {
        let test_source = TestSource::start(vec![]).unwrap();
        let test_sink = TestSink::start().unwrap();

        let fst_1 = Arc::new(ForeignStreamModel::fx_trade_with_name(StreamName::factory(
            "fst_1",
        )));
        let fst_2 = Arc::new(ForeignStreamModel::fx_trade_with_name(StreamName::factory(
            "fst_2",
        )));

        let server_a =
            ServerModel::fx_net_source(fst_1.clone(), test_source.host_ip(), test_source.port());
        let server_c =
            ServerModel::fx_net_sink(fst_2.clone(), test_sink.host_ip(), test_sink.port());

        let pu_b = PumpModel::fx_passthrough_trade_stopped(
            PumpName::factory("pu_b"),
            fst_1.name().clone(),
            fst_2.name().clone(),
        );

        let mut pipeline = Pipeline::default();

        pipeline.add_foreign_stream(fst_1).unwrap();
        pipeline.add_foreign_stream(fst_2).unwrap();

        pipeline.add_server(server_a).unwrap();
        pipeline.add_server(server_c).unwrap();

        pipeline.add_pump(pu_b).unwrap();

        pipeline
    }

    /// ```text
    /// (0)--a-->[1]--b-->[2]--c-->
    /// ```
    pub(in crate::stream_engine) fn fx_linear() -> Self {
        let test_source = TestSource::start(vec![]).unwrap();
        let test_sink = TestSink::start().unwrap();

        let fst_1 = Arc::new(ForeignStreamModel::fx_trade_with_name(StreamName::factory(
            "fst_1",
        )));
        let fst_2 = Arc::new(ForeignStreamModel::fx_trade_with_name(StreamName::factory(
            "fst_2",
        )));

        let server_a =
            ServerModel::fx_net_source(fst_1.clone(), test_source.host_ip(), test_source.port());
        let server_c =
            ServerModel::fx_net_sink(fst_2.clone(), test_sink.host_ip(), test_sink.port());

        let pu_b = PumpModel::fx_passthrough_trade(
            PumpName::factory("pu_b"),
            fst_1.name().clone(),
            fst_2.name().clone(),
        );

        let mut pipeline = Pipeline::default();

        pipeline.add_foreign_stream(fst_1).unwrap();
        pipeline.add_foreign_stream(fst_2).unwrap();

        pipeline.add_server(server_a).unwrap();
        pipeline.add_server(server_c).unwrap();

        pipeline.add_pump(pu_b).unwrap();

        pipeline
    }

    /// ```text
    /// (0)--a-->[1]--c-->[3]--e-->
    ///  |
    ///  +---b-->[2]--d-->[4]--f-->
    /// ```
    pub(in crate::stream_engine) fn fx_split() -> Self {
        let test_source = TestSource::start(vec![]).unwrap();
        let test_sink1 = TestSink::start().unwrap();
        let test_sink2 = TestSink::start().unwrap();

        let fst_1 = Arc::new(ForeignStreamModel::fx_trade_with_name(StreamName::factory(
            "fst_1",
        )));
        let fst_2 = Arc::new(ForeignStreamModel::fx_trade_with_name(StreamName::factory(
            "fst_2",
        )));

        let fst_3 = Arc::new(ForeignStreamModel::fx_trade_with_name(StreamName::factory(
            "fst_3",
        )));
        let fst_4 = Arc::new(ForeignStreamModel::fx_trade_with_name(StreamName::factory(
            "fst_4",
        )));

        let server_a =
            ServerModel::fx_net_source(fst_1.clone(), test_source.host_ip(), test_source.port());
        let server_b =
            ServerModel::fx_net_source(fst_2.clone(), test_source.host_ip(), test_source.port());

        let server_e =
            ServerModel::fx_net_sink(fst_3.clone(), test_sink1.host_ip(), test_sink1.port());
        let server_f =
            ServerModel::fx_net_sink(fst_4.clone(), test_sink2.host_ip(), test_sink2.port());

        let pu_c = PumpModel::fx_passthrough_trade(
            PumpName::factory("pu_c"),
            fst_1.name().clone(),
            fst_3.name().clone(),
        );
        let pu_d = PumpModel::fx_passthrough_trade(
            PumpName::factory("pu_d"),
            fst_2.name().clone(),
            fst_4.name().clone(),
        );

        let mut pipeline = Pipeline::default();

        pipeline.add_foreign_stream(fst_1).unwrap();
        pipeline.add_foreign_stream(fst_2).unwrap();

        pipeline.add_foreign_stream(fst_3).unwrap();
        pipeline.add_foreign_stream(fst_4).unwrap();

        pipeline.add_server(server_a).unwrap();
        pipeline.add_server(server_b).unwrap();

        pipeline.add_server(server_e).unwrap();
        pipeline.add_server(server_f).unwrap();

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
    pub(in crate::stream_engine) fn fx_split_merge() -> Self {
        let test_source = TestSource::start(vec![]).unwrap();
        let test_sink = TestSink::start().unwrap();

        let fst_1 = Arc::new(ForeignStreamModel::fx_trade_with_name(StreamName::factory(
            "fst_1",
        )));
        let fst_2 = Arc::new(ForeignStreamModel::fx_trade_with_name(StreamName::factory(
            "fst_2",
        )));

        let fst_3 = Arc::new(ForeignStreamModel::fx_trade_with_name(StreamName::factory(
            "fst_3",
        )));

        let server_a =
            ServerModel::fx_net_source(fst_1.clone(), test_source.host_ip(), test_source.port());
        let server_b =
            ServerModel::fx_net_source(fst_2.clone(), test_source.host_ip(), test_source.port());

        let server_e =
            ServerModel::fx_net_sink(fst_3.clone(), test_sink.host_ip(), test_sink.port());

        let pu_c = PumpModel::fx_passthrough_trade(
            PumpName::factory("pu_c"),
            fst_1.name().clone(),
            fst_3.name().clone(),
        );
        let pu_d = PumpModel::fx_passthrough_trade(
            PumpName::factory("pu_d"),
            fst_2.name().clone(),
            fst_3.name().clone(),
        );

        let mut pipeline = Pipeline::default();

        pipeline.add_foreign_stream(fst_1).unwrap();
        pipeline.add_foreign_stream(fst_2).unwrap();

        pipeline.add_foreign_stream(fst_3).unwrap();

        pipeline.add_server(server_a).unwrap();
        pipeline.add_server(server_b).unwrap();

        pipeline.add_server(server_e).unwrap();

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
    pub(in crate::stream_engine) fn fx_complex() -> Self {
        let test_source = TestSource::start(vec![]).unwrap();
        let test_sink = TestSink::start().unwrap();
        let test_sink2 = TestSink::start().unwrap();

        let fst_1 = Arc::new(ForeignStreamModel::fx_trade_with_name(StreamName::factory(
            "fst_1",
        )));
        let fst_2 = Arc::new(ForeignStreamModel::fx_trade_with_name(StreamName::factory(
            "fst_2",
        )));
        let st_3 = Arc::new(StreamModel::fx_trade_with_name(StreamName::factory("st_3")));
        let st_4 = Arc::new(StreamModel::fx_trade_with_name(StreamName::factory("st_4")));
        let st_5 = Arc::new(StreamModel::fx_trade_with_name(StreamName::factory("st_5")));
        let st_6 = Arc::new(StreamModel::fx_trade_with_name(StreamName::factory("st_6")));
        let st_7 = Arc::new(StreamModel::fx_trade_with_name(StreamName::factory("st_7")));
        let fst_8 = Arc::new(ForeignStreamModel::fx_trade_with_name(StreamName::factory(
            "fst_8",
        )));
        let fst_9 = Arc::new(ForeignStreamModel::fx_trade_with_name(StreamName::factory(
            "fst_9",
        )));

        let server_a =
            ServerModel::fx_net_source(fst_1.clone(), test_source.host_ip(), test_source.port());
        let server_b =
            ServerModel::fx_net_source(fst_2.clone(), test_source.host_ip(), test_source.port());

        let server_l =
            ServerModel::fx_net_sink(fst_8.clone(), test_sink.host_ip(), test_sink.port());
        let server_m =
            ServerModel::fx_net_sink(fst_9.clone(), test_sink2.host_ip(), test_sink2.port());

        let pu_c = PumpModel::fx_passthrough_trade(
            PumpName::factory("pu_c"),
            fst_1.name().clone(),
            st_3.name().clone(),
        );
        let pu_d = PumpModel::fx_passthrough_trade(
            PumpName::factory("pu_d"),
            fst_2.name().clone(),
            st_4.name().clone(),
        );
        let pu_e = PumpModel::fx_passthrough_trade(
            PumpName::factory("pu_e"),
            fst_2.name().clone(),
            st_5.name().clone(),
        );
        let pu_f = PumpModel::fx_passthrough_trade(
            PumpName::factory("pu_f"),
            st_3.name().clone(),
            st_4.name().clone(),
        );
        let pu_g = PumpModel::fx_passthrough_trade(
            PumpName::factory("pu_g"),
            st_4.name().clone(),
            st_5.name().clone(),
        );
        let pu_h = PumpModel::fx_passthrough_trade(
            PumpName::factory("pu_h"),
            st_5.name().clone(),
            st_6.name().clone(),
        );
        let pu_i = PumpModel::fx_passthrough_trade(
            PumpName::factory("pu_i"),
            st_5.name().clone(),
            st_7.name().clone(),
        );
        let pu_j = PumpModel::fx_passthrough_trade(
            PumpName::factory("pu_j"),
            st_6.name().clone(),
            fst_8.name().clone(),
        );
        let pu_k = PumpModel::fx_passthrough_trade(
            PumpName::factory("pu_k"),
            st_7.name().clone(),
            fst_9.name().clone(),
        );

        let mut pipeline = Pipeline::default();

        pipeline.add_foreign_stream(fst_1).unwrap();
        pipeline.add_foreign_stream(fst_2).unwrap();

        pipeline.add_foreign_stream(fst_8).unwrap();
        pipeline.add_foreign_stream(fst_9).unwrap();

        pipeline.add_server(server_a).unwrap();
        pipeline.add_server(server_b).unwrap();

        pipeline.add_server(server_l).unwrap();
        pipeline.add_server(server_m).unwrap();

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
    pub(in crate::stream_engine) fn fx_city_temperature() -> Self {
        Self::new(
            vec![
                ColumnDefinition::fx_timestamp(),
                ColumnDefinition::fx_city(),
                ColumnDefinition::fx_temperature(),
            ],
            Some(ColumnName::new("timestamp".to_string())),
        )
        .unwrap()
    }
    pub(in crate::stream_engine) fn fx_trade() -> Self {
        Self::new(
            vec![
                ColumnDefinition::fx_timestamp(),
                ColumnDefinition::fx_ticker(),
                ColumnDefinition::fx_amount(),
            ],
            Some(ColumnName::new("timestamp".to_string())),
        )
        .unwrap()
    }

    pub(in crate::stream_engine) fn fx_no_promoted_rowtime() -> Self {
        Self::new(vec![ColumnDefinition::fx_amount()], None).unwrap()
    }
}

impl StreamModel {
    pub(in crate::stream_engine) fn fx_city_temperature() -> Self {
        Self::new(
            StreamName::fx_city_temperature(),
            Arc::new(StreamShape::fx_city_temperature()),
            Options::fx_empty(),
        )
    }

    pub(in crate::stream_engine) fn fx_trade() -> Self {
        Self::new(
            StreamName::fx_trade(),
            Arc::new(StreamShape::fx_trade()),
            Options::fx_empty(),
        )
    }

    pub(in crate::stream_engine) fn fx_trade_with_name(name: StreamName) -> Self {
        Self::new(name, Arc::new(StreamShape::fx_trade()), Options::fx_empty())
    }
}

impl ForeignStreamModel {
    pub(in crate::stream_engine) fn fx_city_temperature_source() -> Self {
        Self::new(StreamModel::new(
            StreamName::fx_city_temperature_source(),
            Arc::new(StreamShape::fx_city_temperature()),
            Options::fx_empty(),
        ))
    }

    pub(in crate::stream_engine) fn fx_trade_source() -> Self {
        Self::new(StreamModel::new(
            StreamName::fx_trade_source(),
            Arc::new(StreamShape::fx_trade()),
            Options::fx_empty(),
        ))
    }

    pub(in crate::stream_engine) fn fx_trade_sink() -> Self {
        Self::new(StreamModel::new(
            StreamName::fx_trade_sink(),
            Arc::new(StreamShape::fx_trade()),
            Options::fx_empty(),
        ))
    }

    pub(in crate::stream_engine) fn fx_trade_with_name(name: StreamName) -> Self {
        Self::new(StreamModel::new(
            name,
            Arc::new(StreamShape::fx_trade()),
            Options::fx_empty(),
        ))
    }
}

impl ServerModel {
    pub(in crate::stream_engine) fn fx_net_source(
        serving_foreign_stream: Arc<ForeignStreamModel>,
        remote_host: IpAddr,
        remote_port: u16,
    ) -> Self {
        Self::new(
            ServerType::SourceNet,
            serving_foreign_stream,
            Options::fx_net_source_server(remote_host, remote_port),
        )
    }
    pub(in crate::stream_engine) fn fx_net_sink(
        serving_foreign_stream: Arc<ForeignStreamModel>,
        remote_host: IpAddr,
        remote_port: u16,
    ) -> Self {
        Self::new(
            ServerType::SinkNet,
            serving_foreign_stream,
            Options::fx_net_sink_server(remote_host, remote_port),
        )
    }
}

impl PumpModel {
    pub(in crate::stream_engine) fn fx_passthrough_trade(
        name: PumpName,
        upstream: StreamName,
        downstream: StreamName,
    ) -> Self {
        Self::new(name, PumpState::Started, upstream, downstream)
    }
    pub(in crate::stream_engine) fn fx_passthrough_trade_stopped(
        name: PumpName,
        upstream: StreamName,
        downstream: StreamName,
    ) -> Self {
        Self::new(name, PumpState::Stopped, upstream, downstream)
    }
}

impl StreamName {
    pub(in crate::stream_engine) fn fx_city_temperature() -> Self {
        StreamName::new("st_city_temperature".to_string())
    }
    pub(in crate::stream_engine) fn fx_city_temperature_source() -> Self {
        StreamName::new("st_city_temperature_source".to_string())
    }
    pub(in crate::stream_engine) fn fx_city_temperature_sink() -> Self {
        StreamName::new("st_city_temperature_sink".to_string())
    }

    pub(in crate::stream_engine) fn fx_trade() -> Self {
        StreamName::new("st_trade".to_string())
    }
    pub(in crate::stream_engine) fn fx_trade_source() -> Self {
        StreamName::new("fst_trade_source".to_string())
    }
    pub(in crate::stream_engine) fn fx_trade_sink() -> Self {
        StreamName::new("fst_trade_sink".to_string())
    }
}

impl PumpName {
    pub(in crate::stream_engine) fn fx_trade_p1() -> Self {
        Self::new("pu_trade_p1".to_string())
    }
    pub(in crate::stream_engine) fn fx_trade2_p1() -> Self {
        Self::new("pu_trade2_p1".to_string())
    }
    pub(in crate::stream_engine) fn fx_trade_window() -> Self {
        Self::new("pu_trade_window".to_string())
    }
}

impl Options {
    pub(in crate::stream_engine) fn fx_empty() -> Self {
        OptionsBuilder::default().build()
    }

    pub(in crate::stream_engine) fn fx_net_source_server(
        remote_host: IpAddr,
        remote_port: u16,
    ) -> Self {
        OptionsBuilder::default()
            .add("PROTOCOL", "TCP")
            .add("REMOTE_HOST", remote_host.to_string())
            .add("REMOTE_PORT", remote_port.to_string())
            .build()
    }

    pub(in crate::stream_engine) fn fx_net_sink_server(
        remote_host: IpAddr,
        remote_port: u16,
    ) -> Self {
        OptionsBuilder::default()
            .add("PROTOCOL", "TCP")
            .add("REMOTE_HOST", remote_host.to_string())
            .add("REMOTE_PORT", remote_port.to_string())
            .build()
    }
}

impl ColumnDefinition {
    pub(in crate::stream_engine) fn fx_timestamp() -> Self {
        Self::new(ColumnDataType::fx_timestamp())
    }

    pub(in crate::stream_engine) fn fx_city() -> Self {
        Self::new(ColumnDataType::fx_city())
    }

    pub(in crate::stream_engine) fn fx_temperature() -> Self {
        Self::new(ColumnDataType::fx_temperature())
    }

    pub(in crate::stream_engine) fn fx_ticker() -> Self {
        Self::new(ColumnDataType::fx_ticker())
    }

    pub(in crate::stream_engine) fn fx_amount() -> Self {
        Self::new(ColumnDataType::fx_amount())
    }
}

impl ColumnDataType {
    pub(in crate::stream_engine) fn fx_timestamp() -> Self {
        Self::new(
            ColumnName::new("timestamp".to_string()),
            SqlType::timestamp(),
            false,
        )
    }

    pub(in crate::stream_engine) fn fx_city() -> Self {
        Self::new(ColumnName::new("city".to_string()), SqlType::text(), false)
    }

    pub(in crate::stream_engine) fn fx_temperature() -> Self {
        Self::new(
            ColumnName::new("temperature".to_string()),
            SqlType::integer(),
            false,
        )
    }

    pub(in crate::stream_engine) fn fx_ticker() -> Self {
        Self::new(
            ColumnName::new("ticker".to_string()),
            SqlType::text(),
            false,
        )
    }

    pub(in crate::stream_engine) fn fx_amount() -> Self {
        Self::new(
            ColumnName::new("amount".to_string()),
            SqlType::small_int(),
            false,
        )
    }
}

impl Row {
    pub(in crate::stream_engine) fn fx_city_temperature_tokyo() -> Self {
        Self::new::<TestDI>(StreamColumns::fx_city_temperature_tokyo())
    }
    pub(in crate::stream_engine) fn fx_city_temperature_osaka() -> Self {
        Self::new::<TestDI>(StreamColumns::fx_city_temperature_osaka())
    }
    pub(in crate::stream_engine) fn fx_city_temperature_london() -> Self {
        Self::new::<TestDI>(StreamColumns::fx_city_temperature_london())
    }

    pub(in crate::stream_engine) fn fx_trade_oracle() -> Self {
        Self::new::<TestDI>(StreamColumns::fx_trade_oracle())
    }
    pub(in crate::stream_engine) fn fx_trade_ibm() -> Self {
        Self::new::<TestDI>(StreamColumns::fx_trade_ibm())
    }
    pub(in crate::stream_engine) fn fx_trade_google() -> Self {
        Self::new::<TestDI>(StreamColumns::fx_trade_google())
    }

    pub(in crate::stream_engine) fn fx_no_promoted_rowtime() -> Self {
        Self::new::<TestDI>(StreamColumns::fx_no_promoted_rowtime())
    }
}

impl StreamColumns {
    pub(in crate::stream_engine) fn fx_city_temperature_tokyo() -> Self {
        Self::factory_city_temperature(Timestamp::fx_ts1(), "Tokyo", 21)
    }
    pub(in crate::stream_engine) fn fx_city_temperature_osaka() -> Self {
        Self::factory_city_temperature(Timestamp::fx_ts2(), "Osaka", 23)
    }
    pub(in crate::stream_engine) fn fx_city_temperature_london() -> Self {
        Self::factory_city_temperature(Timestamp::fx_ts3(), "London", 13)
    }

    pub(in crate::stream_engine) fn fx_trade_oracle() -> Self {
        Self::factory_trade(Timestamp::fx_ts1(), "ORCL", 20)
    }
    pub(in crate::stream_engine) fn fx_trade_ibm() -> Self {
        Self::factory_trade(Timestamp::fx_ts2(), "IBM", 30)
    }
    pub(in crate::stream_engine) fn fx_trade_google() -> Self {
        Self::factory_trade(Timestamp::fx_ts3(), "GOOGL", 100)
    }

    pub(in crate::stream_engine) fn fx_no_promoted_rowtime() -> Self {
        Self::factory_no_promoted_rowtime(12345)
    }
}

impl AlterPipelineCommand {
    pub(in crate::stream_engine) fn fx_create_foreign_stream_trade_with_source_server(
        stream_name: StreamName,
        source_server_host: IpAddr,
        source_server_port: u16,
    ) -> Self {
        let stream = Arc::new(ForeignStreamModel::fx_trade_with_name(stream_name));
        let server = ServerModel::fx_net_source(stream, source_server_host, source_server_port);
        Self::CreateForeignStream(server)
    }

    pub(in crate::stream_engine) fn fx_create_foreign_stream_trade_with_sink_server(
        stream_name: StreamName,
        sink_server_host: IpAddr,
        sink_server_port: u16,
    ) -> Self {
        let stream = Arc::new(ForeignStreamModel::fx_trade_with_name(stream_name));
        let server = ServerModel::fx_net_sink(stream, sink_server_host, sink_server_port);
        Self::CreateForeignStream(server)
    }

    pub(in crate::stream_engine) fn fx_create_pump(
        pump_name: PumpName,
        upstream: StreamName,
        downstream: StreamName,
    ) -> Self {
        let pump = PumpModel::fx_passthrough_trade(pump_name, upstream, downstream);
        Self::CreatePump(pump)
    }

    pub(in crate::stream_engine) fn fx_alter_pump_start(pump_name: PumpName) -> Self {
        Self::AlterPump {
            name: pump_name,
            state: PumpState::Started,
        }
    }
}

impl TaskGraph {
    pub(in crate::stream_engine) fn fx_empty() -> Self {
        Self::from(&PipelineGraph::default())
    }
}
