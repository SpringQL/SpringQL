use std::rc::Rc;

use serde_json::json;

use crate::{
    model::{
        column::{column_data_type::ColumnDataType, column_definition::ColumnDefinition},
        name::{ColumnName, PumpName, StreamName},
        option::{options_builder::OptionsBuilder, Options},
        sql_type::SqlType,
    },
    stream_engine::{
        autonomous_executor::worker_pool::worker::worker_id::WorkerId,
        dependency_injection::test_di::TestDI,
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
        },
        pipeline::{
            stream_model::{stream_shape::StreamShape, StreamModel},
            Pipeline,
        },
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
    pub(in crate::stream_engine) fn fx_linear() -> Self {
        todo!()
    }

    pub(in crate::stream_engine) fn fx_split() -> Self {
        todo!()
    }

    pub(in crate::stream_engine) fn fx_split_merge() -> Self {
        todo!()
    }

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
    pub(in crate::stream_engine) fn fx_ticker() -> Self {
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
            Rc::new(StreamShape::fx_city_temperature()),
            Options::empty(),
        )
    }

    pub(in crate::stream_engine) fn fx_trade() -> Self {
        Self::new(
            StreamName::fx_trade(),
            Rc::new(StreamShape::fx_ticker()),
            Options::empty(),
        )
    }
}

impl StreamName {
    pub(in crate::stream_engine) fn fx_city_temperature() -> Self {
        StreamName::new("city_temperature".to_string())
    }
    pub(in crate::stream_engine) fn fx_trade() -> Self {
        StreamName::new("trade".to_string())
    }
}

impl PumpName {
    pub(in crate::stream_engine) fn fx_trade_p1() -> Self {
        Self::new("trade_p1".to_string())
    }
    pub(in crate::stream_engine) fn fx_trade_window() -> Self {
        Self::new("trade_window".to_string())
    }
}

impl Options {
    pub(in crate::stream_engine) fn empty() -> Self {
        OptionsBuilder::default().build()
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

impl WorkerId {
    pub(in crate::stream_engine) fn fx_main_thread() -> Self {
        Self::new(0)
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
