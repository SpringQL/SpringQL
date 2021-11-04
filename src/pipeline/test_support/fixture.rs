use std::{net::IpAddr, sync::Arc};

use crate::{
    model::{
        column::{column_data_type::ColumnDataType, column_definition::ColumnDefinition},
        sql_type::SqlType,
    },
    pipeline::{
        foreign_stream_model::ForeignStreamModel,
        name::{ColumnName, PumpName, StreamName},
        option::{options_builder::OptionsBuilder, Options},
        pump_model::pump_state::PumpState,
        pump_model::PumpModel,
        server_model::{server_type::ServerType, ServerModel},
        stream_model::{stream_shape::StreamShape, StreamModel},
        Pipeline,
    },
};

impl Pipeline {
    /// ```text
    /// (0)--a-->[1]
    /// ```
    pub(crate) fn fx_source_only(source_remote_host: IpAddr, source_remote_port: u16) -> Self {
        let fst_1 = Arc::new(ForeignStreamModel::fx_trade_with_name(StreamName::factory(
            "fst_1",
        )));

        let server_a = ServerModel::fx_net_source_started(
            fst_1.clone(),
            source_remote_host,
            source_remote_port,
        );

        let mut pipeline = Pipeline::default();
        pipeline.add_foreign_stream(fst_1).unwrap();
        pipeline.add_server(server_a).unwrap();
        pipeline
    }

    /// ```text
    /// (0)--a-->[1]--b(STOPPED)-->[2]--c-->
    /// ```
    pub(crate) fn fx_linear_stopped(
        source_remote_host: IpAddr,
        source_remote_port: u16,
        sink_remote_host: IpAddr,
        sink_remote_port: u16,
    ) -> Self {
        let fst_1 = Arc::new(ForeignStreamModel::fx_trade_with_name(StreamName::factory(
            "fst_1",
        )));
        let fst_2 = Arc::new(ForeignStreamModel::fx_trade_with_name(StreamName::factory(
            "fst_2",
        )));

        let server_a = ServerModel::fx_net_source_started(
            fst_1.clone(),
            source_remote_host,
            source_remote_port,
        );
        let server_c = ServerModel::fx_net_sink(fst_2.clone(), sink_remote_host, sink_remote_port);

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
    pub(crate) fn fx_linear(
        source_remote_host: IpAddr,
        source_remote_port: u16,
        sink_remote_host: IpAddr,
        sink_remote_port: u16,
    ) -> Self {
        let fst_1 = Arc::new(ForeignStreamModel::fx_trade_with_name(StreamName::factory(
            "fst_1",
        )));
        let fst_2 = Arc::new(ForeignStreamModel::fx_trade_with_name(StreamName::factory(
            "fst_2",
        )));

        let server_a = ServerModel::fx_net_source_started(
            fst_1.clone(),
            source_remote_host,
            source_remote_port,
        );
        let server_c = ServerModel::fx_net_sink(fst_2.clone(), sink_remote_host, sink_remote_port);

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
    pub(crate) fn fx_split(
        source_remote_host: IpAddr,
        source_remote_port: u16,
        sink1_remote_host: IpAddr,
        sink1_remote_port: u16,
        sink2_remote_host: IpAddr,
        sink2_remote_port: u16,
    ) -> Self {
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

        let server_a = ServerModel::fx_net_source_started(
            fst_1.clone(),
            source_remote_host,
            source_remote_port,
        );
        let server_b = ServerModel::fx_net_source_started(
            fst_2.clone(),
            source_remote_host,
            source_remote_port,
        );

        let server_e =
            ServerModel::fx_net_sink(fst_3.clone(), sink1_remote_host, sink1_remote_port);
        let server_f =
            ServerModel::fx_net_sink(fst_4.clone(), sink2_remote_host, sink2_remote_port);

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
    pub(crate) fn fx_split_merge(
        source_remote_host: IpAddr,
        source_remote_port: u16,
        sink_remote_host: IpAddr,
        sink_remote_port: u16,
    ) -> Self {
        let fst_1 = Arc::new(ForeignStreamModel::fx_trade_with_name(StreamName::factory(
            "fst_1",
        )));
        let fst_2 = Arc::new(ForeignStreamModel::fx_trade_with_name(StreamName::factory(
            "fst_2",
        )));

        let fst_3 = Arc::new(ForeignStreamModel::fx_trade_with_name(StreamName::factory(
            "fst_3",
        )));

        let server_a = ServerModel::fx_net_source_started(
            fst_1.clone(),
            source_remote_host,
            source_remote_port,
        );
        let server_b = ServerModel::fx_net_source_started(
            fst_2.clone(),
            source_remote_host,
            source_remote_port,
        );

        let server_e = ServerModel::fx_net_sink(fst_3.clone(), sink_remote_host, sink_remote_port);

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
    pub(crate) fn fx_complex(
        source_remote_host: IpAddr,
        source_remote_port: u16,
        sink1_remote_host: IpAddr,
        sink1_remote_port: u16,
        sink2_remote_host: IpAddr,
        sink2_remote_port: u16,
    ) -> Self {
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

        let server_a = ServerModel::fx_net_source_started(
            fst_1.clone(),
            source_remote_host,
            source_remote_port,
        );
        let server_b = ServerModel::fx_net_source_started(
            fst_2.clone(),
            source_remote_host,
            source_remote_port,
        );

        let server_l =
            ServerModel::fx_net_sink(fst_8.clone(), sink1_remote_host, sink1_remote_port);
        let server_m =
            ServerModel::fx_net_sink(fst_9.clone(), sink2_remote_host, sink2_remote_port);

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
    pub(crate) fn fx_city_temperature() -> Self {
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
    pub(crate) fn fx_trade() -> Self {
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

    pub(crate) fn fx_no_promoted_rowtime() -> Self {
        Self::new(vec![ColumnDefinition::fx_amount()], None).unwrap()
    }
}

impl StreamModel {
    pub(crate) fn fx_city_temperature() -> Self {
        Self::new(
            StreamName::fx_city_temperature(),
            Arc::new(StreamShape::fx_city_temperature()),
            Options::fx_empty(),
        )
    }

    pub(crate) fn fx_trade() -> Self {
        Self::new(
            StreamName::fx_trade(),
            Arc::new(StreamShape::fx_trade()),
            Options::fx_empty(),
        )
    }

    pub(crate) fn fx_trade_with_name(name: StreamName) -> Self {
        Self::new(name, Arc::new(StreamShape::fx_trade()), Options::fx_empty())
    }
}

impl ForeignStreamModel {
    pub(crate) fn fx_city_temperature_source() -> Self {
        Self::new(StreamModel::new(
            StreamName::fx_city_temperature_source(),
            Arc::new(StreamShape::fx_city_temperature()),
            Options::fx_empty(),
        ))
    }

    pub(crate) fn fx_trade_source() -> Self {
        Self::new(StreamModel::new(
            StreamName::fx_trade_source(),
            Arc::new(StreamShape::fx_trade()),
            Options::fx_empty(),
        ))
    }

    pub(crate) fn fx_trade_sink() -> Self {
        Self::new(StreamModel::new(
            StreamName::fx_trade_sink(),
            Arc::new(StreamShape::fx_trade()),
            Options::fx_empty(),
        ))
    }

    pub(crate) fn fx_trade_with_name(name: StreamName) -> Self {
        Self::new(StreamModel::new(
            name,
            Arc::new(StreamShape::fx_trade()),
            Options::fx_empty(),
        ))
    }
}

impl ServerModel {
    pub(crate) fn fx_net_source_started(
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
    pub(crate) fn fx_net_sink(
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
    pub(crate) fn fx_passthrough_trade(
        name: PumpName,
        upstream: StreamName,
        downstream: StreamName,
    ) -> Self {
        Self::new(name, PumpState::Started, upstream, downstream)
    }
    pub(crate) fn fx_passthrough_trade_stopped(
        name: PumpName,
        upstream: StreamName,
        downstream: StreamName,
    ) -> Self {
        Self::new(name, PumpState::Stopped, upstream, downstream)
    }
}

impl ColumnDefinition {
    pub(crate) fn fx_timestamp() -> Self {
        Self::new(ColumnDataType::fx_timestamp())
    }

    pub(crate) fn fx_city() -> Self {
        Self::new(ColumnDataType::fx_city())
    }

    pub(crate) fn fx_temperature() -> Self {
        Self::new(ColumnDataType::fx_temperature())
    }

    pub(crate) fn fx_ticker() -> Self {
        Self::new(ColumnDataType::fx_ticker())
    }

    pub(crate) fn fx_amount() -> Self {
        Self::new(ColumnDataType::fx_amount())
    }
}

impl ColumnDataType {
    pub(crate) fn fx_timestamp() -> Self {
        Self::new(
            ColumnName::new("timestamp".to_string()),
            SqlType::timestamp(),
            false,
        )
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
        Self::new(
            ColumnName::new("ticker".to_string()),
            SqlType::text(),
            false,
        )
    }

    pub(crate) fn fx_amount() -> Self {
        Self::new(
            ColumnName::new("amount".to_string()),
            SqlType::small_int(),
            false,
        )
    }
}

impl Options {
    pub(crate) fn fx_empty() -> Self {
        OptionsBuilder::default().build()
    }

    pub(crate) fn fx_net_source_server(remote_host: IpAddr, remote_port: u16) -> Self {
        OptionsBuilder::default()
            .add("PROTOCOL", "TCP")
            .add("REMOTE_HOST", remote_host.to_string())
            .add("REMOTE_PORT", remote_port.to_string())
            .build()
    }

    pub(crate) fn fx_net_sink_server(remote_host: IpAddr, remote_port: u16) -> Self {
        OptionsBuilder::default()
            .add("PROTOCOL", "TCP")
            .add("REMOTE_HOST", remote_host.to_string())
            .add("REMOTE_PORT", remote_port.to_string())
            .build()
    }
}

impl StreamName {
    pub(crate) fn fx_city_temperature() -> Self {
        StreamName::new("st_city_temperature".to_string())
    }
    pub(crate) fn fx_city_temperature_source() -> Self {
        StreamName::new("st_city_temperature_source".to_string())
    }
    pub(crate) fn fx_city_temperature_sink() -> Self {
        StreamName::new("st_city_temperature_sink".to_string())
    }

    pub(crate) fn fx_trade() -> Self {
        StreamName::new("st_trade".to_string())
    }
    pub(crate) fn fx_trade_source() -> Self {
        StreamName::new("fst_trade_source".to_string())
    }
    pub(crate) fn fx_trade_sink() -> Self {
        StreamName::new("fst_trade_sink".to_string())
    }
}

impl PumpName {
    pub(crate) fn fx_city_temperature_p1() -> Self {
        Self::new("pu_city_temperature_p1".to_string())
    }

    pub(crate) fn fx_trade_p1() -> Self {
        Self::new("pu_trade_p1".to_string())
    }
    pub(crate) fn fx_trade2_p1() -> Self {
        Self::new("pu_trade2_p1".to_string())
    }
    pub(crate) fn fx_trade_window() -> Self {
        Self::new("pu_trade_window".to_string())
    }
}
