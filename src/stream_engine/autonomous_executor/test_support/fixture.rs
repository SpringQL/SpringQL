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
            task::task_id::TaskId,
            test_support::foreign::sink::TestSink,
        },
        pipeline::{
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
    /// (0)--a-->[1]--b-->[2]--c-->
    /// ```
    pub(in crate::stream_engine) fn fx_linear() -> Self {
        let test_source = TestSource::start(vec![]).unwrap();
        let test_sink = TestSink::start().unwrap();

        let fst_trade_source = Arc::new(ForeignStreamModel::fx_trade_source());
        let fst_trade_sink = Arc::new(ForeignStreamModel::fx_trade_sink());

        let server_trade_source = ServerModel::fx_net_source(
            fst_trade_source.clone(),
            test_source.host_ip(),
            test_source.port(),
        );
        let server_trade_sink = ServerModel::fx_net_sink(
            fst_trade_sink.clone(),
            test_sink.host_ip(),
            test_sink.port(),
        );

        let pump_trade_source_p1 = PumpModel::fx_passthrough_trade(
            PumpName::fx_trade_p1(),
            fst_trade_source.name().clone(),
            fst_trade_sink.name().clone(),
        );

        let mut pipeline = Pipeline::default();
        pipeline.add_foreign_stream(fst_trade_source).unwrap();
        pipeline.add_server(server_trade_source).unwrap();

        pipeline.add_foreign_stream(fst_trade_sink).unwrap();
        pipeline.add_server(server_trade_sink).unwrap();

        pipeline.add_pump(pump_trade_source_p1).unwrap();

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

        let fst_trade_source1 = Arc::new(ForeignStreamModel::fx_trade_source());
        let fst_trade_source2 = Arc::new(ForeignStreamModel::fx_trade_source2());

        let fst_trade_sink1 = Arc::new(ForeignStreamModel::fx_trade_sink());
        let fst_trade_sink2 = Arc::new(ForeignStreamModel::fx_trade_sink2());

        let server_trade_source1 = ServerModel::fx_net_source(
            fst_trade_source1.clone(),
            test_source.host_ip(),
            test_source.port(),
        );
        let server_trade_source2 = ServerModel::fx_net_source(
            fst_trade_source2.clone(),
            test_source.host_ip(),
            test_source.port(),
        );

        let server_trade_sink1 = ServerModel::fx_net_sink(
            fst_trade_sink1.clone(),
            test_sink1.host_ip(),
            test_sink1.port(),
        );
        let server_trade_sink2 = ServerModel::fx_net_sink(
            fst_trade_sink1.clone(),
            test_sink2.host_ip(),
            test_sink2.port(),
        );

        let pump_trade_source1_p1 = PumpModel::fx_passthrough_trade(
            PumpName::fx_trade_p1(),
            fst_trade_source1.name().clone(),
            fst_trade_sink1.name().clone(),
        );
        let pump_trade_source2_p1 = PumpModel::fx_passthrough_trade(
            PumpName::fx_trade2_p1(),
            fst_trade_source2.name().clone(),
            fst_trade_sink2.name().clone(),
        );

        let mut pipeline = Pipeline::default();
        pipeline.add_foreign_stream(fst_trade_source1).unwrap();
        pipeline.add_server(server_trade_source1).unwrap();

        pipeline.add_foreign_stream(fst_trade_source2).unwrap();
        pipeline.add_server(server_trade_source2).unwrap();

        pipeline.add_foreign_stream(fst_trade_sink1).unwrap();
        pipeline.add_server(server_trade_sink1).unwrap();

        pipeline.add_foreign_stream(fst_trade_sink2).unwrap();
        pipeline.add_server(server_trade_sink2).unwrap();

        pipeline.add_pump(pump_trade_source1_p1).unwrap();
        pipeline.add_pump(pump_trade_source2_p1).unwrap();

        pipeline
    }

    /// ```text
    /// (0)--a-->[1]--c-->[3]--e-->
    ///  |                 ^
    ///  |                 |
    ///  +---b-->[2]--d----+
    /// ```
    pub(in crate::stream_engine) fn fx_split_merge() -> Self {
        todo!()
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
        todo!()
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
    pub(in crate::stream_engine) fn fx_trade_source2() -> Self {
        Self::new(StreamModel::new(
            StreamName::fx_trade_source2(),
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
    pub(in crate::stream_engine) fn fx_trade_sink2() -> Self {
        Self::new(StreamModel::new(
            StreamName::fx_trade_sink2(),
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
        Self::new(name, upstream, downstream)
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
    pub(in crate::stream_engine) fn fx_trade_source2() -> Self {
        StreamName::new("fst_trade_source2".to_string())
    }
    pub(in crate::stream_engine) fn fx_trade_sink() -> Self {
        StreamName::new("fst_trade_sink".to_string())
    }
    pub(in crate::stream_engine) fn fx_trade_sink2() -> Self {
        StreamName::new("fst_trade_sink2".to_string())
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

impl TaskId {
    pub(in crate::stream_engine) fn fx_a() -> Self {
        Self::new("task-a".to_string())
    }
    pub(in crate::stream_engine) fn fx_b() -> Self {
        Self::new("task-b".to_string())
    }
    pub(in crate::stream_engine) fn fx_c() -> Self {
        Self::new("task-c".to_string())
    }
    pub(in crate::stream_engine) fn fx_d() -> Self {
        Self::new("task-d".to_string())
    }
    pub(in crate::stream_engine) fn fx_e() -> Self {
        Self::new("task-e".to_string())
    }
    pub(in crate::stream_engine) fn fx_f() -> Self {
        Self::new("task-f".to_string())
    }
    pub(in crate::stream_engine) fn fx_g() -> Self {
        Self::new("task-g".to_string())
    }
    pub(in crate::stream_engine) fn fx_h() -> Self {
        Self::new("task-h".to_string())
    }
    pub(in crate::stream_engine) fn fx_i() -> Self {
        Self::new("task-i".to_string())
    }
    pub(in crate::stream_engine) fn fx_j() -> Self {
        Self::new("task-j".to_string())
    }
    pub(in crate::stream_engine) fn fx_k() -> Self {
        Self::new("task-k".to_string())
    }
    pub(in crate::stream_engine) fn fx_l() -> Self {
        Self::new("task-l".to_string())
    }
    pub(in crate::stream_engine) fn fx_m() -> Self {
        Self::new("task-m".to_string())
    }
}
