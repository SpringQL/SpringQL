// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

use std::{net::IpAddr, sync::Arc};

use serde_json::json;

use crate::{
    pipeline::{
        foreign_stream_model::ForeignStreamModel,
        name::{PumpName, StreamName},
        pipeline_graph::PipelineGraph,
        pump_model::{pump_state::PumpState, PumpModel},
        sink_writer_model::SinkWriterModel,
        source_reader_model::SourceReaderModel,
    },
    stream_engine::dependency_injection::test_di::TestDI,
    stream_engine::{
        autonomous_executor::{
            row::{
                column::stream_column::StreamColumns,
                foreign_row::{
                    foreign_sink_row::ForeignSinkRow, foreign_source_row::ForeignSourceRow,
                    format::json::JsonObject,
                },
                timestamp::Timestamp,
                Row,
            },
            task::task_graph::TaskGraph,
        },
        command::alter_pipeline_command::AlterPipelineCommand,
    },
};

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
            "ts": Timestamp::fx_ts1().to_string(),
            "city": "Tokyo",
            "temperature": 21,
        }))
    }

    pub(in crate::stream_engine) fn fx_city_temperature_osaka() -> Self {
        Self::new(json!({
            "ts": Timestamp::fx_ts2().to_string(),
            "city": "Osaka",
            "temperature": 23,
        }))
    }

    pub(in crate::stream_engine) fn fx_city_temperature_london() -> Self {
        Self::new(json!({
            "ts": Timestamp::fx_ts3().to_string(),
            "city": "London",
            "temperature": 13,
        }))
    }

    pub(in crate::stream_engine) fn fx_trade_oracle() -> Self {
        Self::new(json!({
            "ts": Timestamp::fx_ts1().to_string(),
            "ticker": "ORCL",
            "amount": 20,
        }))
    }

    pub(in crate::stream_engine) fn fx_trade_ibm() -> Self {
        Self::new(json!({
            "ts": Timestamp::fx_ts2().to_string(),
            "ticker": "IBM",
            "amount": 30,
        }))
    }

    pub(in crate::stream_engine) fn fx_trade_google() -> Self {
        Self::new(json!({
            "ts": Timestamp::fx_ts3().to_string(),
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
    pub(in crate::stream_engine) fn fx_create_foreign_stream_trade_with_source(
        stream_name: StreamName,
        source_server_host: IpAddr,
        source_server_port: u16,
    ) -> Self {
        let stream = Arc::new(ForeignStreamModel::fx_trade_with_name(stream_name));
        let source =
            SourceReaderModel::fx_net_started(stream, source_server_host, source_server_port);
        Self::CreateForeignSourceStream(source)
    }

    pub(in crate::stream_engine) fn fx_create_foreign_stream_trade_with_sink(
        stream_name: StreamName,
        sink_server_host: IpAddr,
        sink_server_port: u16,
    ) -> Self {
        let stream = Arc::new(ForeignStreamModel::fx_trade_with_name(stream_name));
        let sink = SinkWriterModel::fx_net(stream, sink_server_host, sink_server_port);
        Self::CreateForeignSinkStream(sink)
    }

    pub(in crate::stream_engine) fn fx_create_pump(
        pump_name: PumpName,
        upstream: StreamName,
        downstream: StreamName,
    ) -> Self {
        let pump = PumpModel::fx_trade_stopped(pump_name, upstream, downstream);
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
