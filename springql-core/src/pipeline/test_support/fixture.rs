// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use std::{net::IpAddr, sync::Arc};

use crate::{
    api::{SpringConfig, SpringSinkWriterConfig, SpringSourceReaderConfig},
    pipeline::{
        field::ColumnReference,
        name::{ColumnName, SinkWriterName, SourceReaderName, StreamName},
        option::{Options, OptionsBuilder},
        pipeline_version::PipelineVersion,
        relation::{ColumnConstraint, ColumnDataType, ColumnDefinition, SqlType},
        sink_writer_model::{SinkWriterModel, SinkWriterType},
        source_reader_model::{SourceReaderModel, SourceReaderType},
        stream_model::{StreamModel, StreamShape},
        Pipeline,
    },
};

impl SpringConfig {
    pub fn fx_default() -> Self {
        Self::default()
    }
}

impl SpringSourceReaderConfig {
    pub fn fx_default() -> Self {
        let c = SpringConfig::fx_default();
        c.source_reader
    }
}

impl SpringSinkWriterConfig {
    pub fn fx_default() -> Self {
        let c = SpringConfig::fx_default();
        c.sink_writer
    }
}

impl Pipeline {
    /// ```text
    /// (0)--a-->[1]
    /// ```
    pub fn fx_source_only() -> Self {
        let st_1 = Arc::new(StreamModel::fx_trade_with_name(StreamName::factory("st_1")));

        let mut pipeline = Pipeline::new(PipelineVersion::new());
        pipeline.add_stream(st_1).unwrap();
        pipeline
    }

    pub fn fx_sink_only() -> Self {
        let sink_1 = Arc::new(StreamModel::fx_trade_with_name(StreamName::factory(
            "sink_1",
        )));

        let mut pipeline = Pipeline::new(PipelineVersion::new());
        pipeline.add_stream(sink_1).unwrap();
        pipeline
    }
}

impl StreamShape {
    pub fn fx_city_temperature() -> Self {
        Self::new(vec![
            ColumnDefinition::fx_timestamp(),
            ColumnDefinition::fx_city(),
            ColumnDefinition::fx_temperature(),
        ])
        .unwrap()
    }
    pub fn fx_trade() -> Self {
        Self::new(vec![
            ColumnDefinition::fx_timestamp(),
            ColumnDefinition::fx_ticker(),
            ColumnDefinition::fx_amount(),
        ])
        .unwrap()
    }

    pub fn fx_no_promoted_rowtime() -> Self {
        Self::new(vec![ColumnDefinition::fx_amount()]).unwrap()
    }
}

impl StreamModel {
    pub fn fx_city_temperature() -> Self {
        Self::new(
            StreamName::fx_city_temperature(),
            StreamShape::fx_city_temperature(),
        )
    }

    pub fn fx_trade() -> Self {
        Self::new(StreamName::fx_trade(), StreamShape::fx_trade())
    }

    pub fn fx_trade_with_name(name: StreamName) -> Self {
        Self::new(name, StreamShape::fx_trade())
    }
}

impl SourceReaderModel {
    pub fn fx_net(stream_name: StreamName, remote_host: IpAddr, remote_port: u16) -> Self {
        Self::new(
            SourceReaderName::fx_tcp_trade(),
            SourceReaderType::NetClient,
            stream_name,
            Options::fx_net(remote_host, remote_port),
        )
    }
}
impl SinkWriterModel {
    pub fn fx_net(stream_name: StreamName, remote_host: IpAddr, remote_port: u16) -> Self {
        Self::new(
            SinkWriterName::fx_tcp_trade(),
            SinkWriterType::Net,
            stream_name,
            Options::fx_net(remote_host, remote_port),
        )
    }
}

impl ColumnReference {
    pub fn fx_trade_timestamp() -> Self {
        Self::Column {
            stream_name: StreamName::fx_trade(),
            column_name: ColumnName::fx_timestamp(),
        }
    }
    pub fn fx_trade_ticker() -> Self {
        Self::Column {
            stream_name: StreamName::fx_trade(),
            column_name: ColumnName::fx_ticker(),
        }
    }
    pub fn fx_trade_amount() -> Self {
        Self::Column {
            stream_name: StreamName::fx_trade(),
            column_name: ColumnName::fx_amount(),
        }
    }

    pub fn fx_city_temperature_timestamp() -> Self {
        Self::Column {
            stream_name: StreamName::fx_city_temperature(),
            column_name: ColumnName::fx_timestamp(),
        }
    }
    pub fn fx_city_temperature_city() -> Self {
        Self::Column {
            stream_name: StreamName::fx_city_temperature(),
            column_name: ColumnName::fx_city(),
        }
    }
    pub fn fx_city_temperature_temperature() -> Self {
        Self::Column {
            stream_name: StreamName::fx_city_temperature(),
            column_name: ColumnName::fx_temperature(),
        }
    }
}

impl ColumnDefinition {
    pub fn fx_timestamp() -> Self {
        Self::new(
            ColumnDataType::fx_timestamp(),
            vec![ColumnConstraint::Rowtime],
        )
    }

    pub fn fx_city() -> Self {
        Self::new(ColumnDataType::fx_city(), vec![])
    }

    pub fn fx_temperature() -> Self {
        Self::new(ColumnDataType::fx_temperature(), vec![])
    }

    pub fn fx_ticker() -> Self {
        Self::new(ColumnDataType::fx_ticker(), vec![])
    }

    pub fn fx_amount() -> Self {
        Self::new(ColumnDataType::fx_amount(), vec![])
    }
}

impl ColumnDataType {
    pub fn fx_timestamp() -> Self {
        Self::new(ColumnName::fx_timestamp(), SqlType::timestamp(), false)
    }

    pub fn fx_city() -> Self {
        Self::new(ColumnName::new("city".to_string()), SqlType::text(), false)
    }

    pub fn fx_temperature() -> Self {
        Self::new(
            ColumnName::new("temperature".to_string()),
            SqlType::integer(),
            false,
        )
    }

    pub fn fx_ticker() -> Self {
        Self::new(ColumnName::fx_ticker(), SqlType::text(), false)
    }

    pub fn fx_amount() -> Self {
        Self::new(ColumnName::fx_amount(), SqlType::integer(), false)
    }
}

impl StreamName {
    pub fn fx_trade() -> Self {
        Self::new("trade".to_string())
    }
    pub fn fx_city_temperature() -> Self {
        Self::new("city_temperature".to_string())
    }
}

impl ColumnName {
    pub fn fx_timestamp() -> Self {
        Self::new("ts".to_string())
    }
    pub fn fx_ticker() -> Self {
        Self::new("ticker".to_string())
    }
    pub fn fx_amount() -> Self {
        Self::new("amount".to_string())
    }

    pub fn fx_city() -> Self {
        Self::new("city".to_string())
    }
    pub fn fx_temperature() -> Self {
        Self::new("temperature".to_string())
    }
}

impl Options {
    pub fn fx_net(remote_host: IpAddr, remote_port: u16) -> Self {
        OptionsBuilder::default()
            .add("PROTOCOL", "TCP")
            .add("REMOTE_HOST", remote_host.to_string())
            .add("REMOTE_PORT", remote_port.to_string())
            .build()
    }
}

impl SourceReaderName {
    pub fn fx_tcp_trade() -> Self {
        Self::new("tcp_source_trade".to_string())
    }
}

impl SinkWriterName {
    pub fn fx_tcp_trade() -> Self {
        Self::new("tcp_sink_trade".to_string())
    }
}
