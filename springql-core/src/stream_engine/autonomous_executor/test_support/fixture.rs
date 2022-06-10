// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use std::net::IpAddr;

use serde_json::json;

use crate::{
    pipeline::{
        PipelineVersion, PumpInputType, SinkWriterModel, SourceReaderModel, StreamModel, StreamName,
    },
    stream_engine::{
        autonomous_executor::{
            performance_metrics::metrics_update_command::{
                InQueueMetricsUpdateByCollect, InQueueMetricsUpdateByTask,
                OutQueueMetricsUpdateByTask, TaskMetricsUpdateByTask, WindowInFlowByWindowTask,
            },
            row::{
                column::stream_column::StreamColumns, foreign_row::format::JsonObject,
                foreign_row::source_row::json_source_row::JsonSourceRow, Row,
            },
            task::tuple::Tuple,
            task_graph::{
                queue_id::{row_queue_id::RowQueueId, window_queue_id::WindowQueueId, QueueId},
                task_id::TaskId,
                QueueIdWithUpstream,
            },
        },
        command::alter_pipeline_command::AlterPipelineCommand,
        time::duration::{wall_clock_duration::WallClockDuration, SpringDuration},
    },
    stream_engine::{
        autonomous_executor::{
            performance_metrics::{
                metrics_update_command::MetricsUpdateByTaskExecution, PerformanceMetrics,
            },
            task_graph::TaskGraph,
        },
        time::timestamp::SpringTimestamp,
    },
};

impl SpringTimestamp {
    pub fn fx_ts1() -> Self {
        "2021-01-01 13:00:00.000000001".parse().unwrap()
    }
    pub fn fx_ts2() -> Self {
        "2021-01-01 13:00:00.000000002".parse().unwrap()
    }
    pub fn fx_ts3() -> Self {
        "2021-01-01 13:00:00.000000003".parse().unwrap()
    }
}

impl JsonObject {
    pub fn fx_city_temperature_tokyo() -> Self {
        Self::new(json!({
            "ts": SpringTimestamp::fx_ts1().to_string(),
            "city": "Tokyo",
            "temperature": 21,
        }))
    }

    pub fn fx_city_temperature_osaka() -> Self {
        Self::new(json!({
            "ts": SpringTimestamp::fx_ts2().to_string(),
            "city": "Osaka",
            "temperature": 23,
        }))
    }

    pub fn fx_city_temperature_london() -> Self {
        Self::new(json!({
            "ts": SpringTimestamp::fx_ts3().to_string(),
            "city": "London",
            "temperature": 13,
        }))
    }

    pub fn fx_trade_oracle() -> Self {
        Self::new(json!({
            "ts": SpringTimestamp::fx_ts1().to_string(),
            "ticker": "ORCL",
            "amount": 20,
        }))
    }

    pub fn fx_trade_ibm() -> Self {
        Self::new(json!({
            "ts": SpringTimestamp::fx_ts2().to_string(),
            "ticker": "IBM",
            "amount": 30,
        }))
    }

    pub fn fx_trade_google() -> Self {
        Self::new(json!({
            "ts": SpringTimestamp::fx_ts3().to_string(),
            "ticker": "GOOGL",
            "amount": 100,
        }))
    }
}

impl JsonSourceRow {
    pub(in crate::stream_engine) fn fx_city_temperature_tokyo() -> Self {
        Self::from_json(JsonObject::fx_city_temperature_tokyo())
    }
    pub fn fx_city_temperature_osaka() -> Self {
        Self::from_json(JsonObject::fx_city_temperature_osaka())
    }
    pub fn fx_city_temperature_london() -> Self {
        Self::from_json(JsonObject::fx_city_temperature_london())
    }

    pub fn fx_trade_oracle() -> Self {
        Self::from_json(JsonObject::fx_trade_oracle())
    }
    pub fn fx_trade_ibm() -> Self {
        Self::from_json(JsonObject::fx_trade_ibm())
    }
    pub fn fx_trade_google() -> Self {
        Self::from_json(JsonObject::fx_trade_google())
    }
}

impl Row {
    pub fn fx_city_temperature_tokyo() -> Self {
        Self::new(StreamColumns::fx_city_temperature_tokyo())
    }
    pub fn fx_city_temperature_osaka() -> Self {
        Self::new(StreamColumns::fx_city_temperature_osaka())
    }
    pub fn fx_city_temperature_london() -> Self {
        Self::new(StreamColumns::fx_city_temperature_london())
    }

    pub fn fx_trade_oracle() -> Self {
        Self::new(StreamColumns::fx_trade_oracle())
    }
    pub fn fx_trade_ibm() -> Self {
        Self::new(StreamColumns::fx_trade_ibm())
    }
    pub fn fx_trade_google() -> Self {
        Self::new(StreamColumns::fx_trade_google())
    }

    pub fn fx_no_promoted_rowtime() -> Self {
        Self::new(StreamColumns::fx_no_promoted_rowtime())
    }
}

impl Tuple {
    pub fn fx_trade_oracle() -> Self {
        let row = Row::fx_trade_oracle();
        Self::from_row(row)
    }
}

impl StreamColumns {
    pub fn fx_city_temperature_tokyo() -> Self {
        Self::factory_city_temperature(SpringTimestamp::fx_ts1(), "Tokyo", 21)
    }
    pub fn fx_city_temperature_osaka() -> Self {
        Self::factory_city_temperature(SpringTimestamp::fx_ts2(), "Osaka", 23)
    }
    pub fn fx_city_temperature_london() -> Self {
        Self::factory_city_temperature(SpringTimestamp::fx_ts3(), "London", 13)
    }

    pub fn fx_trade_oracle() -> Self {
        Self::factory_trade(SpringTimestamp::fx_ts1(), "ORCL", 20)
    }
    pub fn fx_trade_ibm() -> Self {
        Self::factory_trade(SpringTimestamp::fx_ts2(), "IBM", 30)
    }
    pub fn fx_trade_google() -> Self {
        Self::factory_trade(SpringTimestamp::fx_ts3(), "GOOGL", 100)
    }

    pub fn fx_no_promoted_rowtime() -> Self {
        Self::factory_no_promoted_rowtime(12345)
    }
}

impl AlterPipelineCommand {
    pub fn fx_create_source_stream_trade(stream_name: StreamName) -> Self {
        let stream = StreamModel::fx_trade_with_name(stream_name);
        Self::CreateSourceStream(stream)
    }

    pub fn fx_create_source_reader_trade(
        stream_name: StreamName,
        source_server_host: IpAddr,
        source_server_port: u16,
    ) -> Self {
        let source = SourceReaderModel::fx_net(stream_name, source_server_host, source_server_port);
        Self::CreateSourceReader(source)
    }

    pub fn fx_create_sink_stream_trade(stream_name: StreamName) -> Self {
        let stream = StreamModel::fx_trade_with_name(stream_name);
        Self::CreateSinkStream(stream)
    }

    pub fn fx_create_sink_writer_trade(
        stream_name: StreamName,
        sink_server_host: IpAddr,
        sink_server_port: u16,
    ) -> Self {
        let sink = SinkWriterModel::fx_net(stream_name, sink_server_host, sink_server_port);
        Self::CreateSinkWriter(sink)
    }
}

impl TaskGraph {
    pub fn fx_split_join() -> Self {
        let mut g = TaskGraph::new(PipelineVersion::new());

        g.add_task(TaskId::fx_split_join_t1());
        g.add_task(TaskId::fx_split_join_t2());
        g.add_task(TaskId::fx_split_join_t3());
        g.add_task(TaskId::fx_split_join_t4());
        g.add_task(TaskId::fx_split_join_t5());
        g.add_task(TaskId::fx_split_join_t6());
        g.add_task(TaskId::fx_split_join_t7());
        g.add_task(TaskId::fx_split_join_t8());
        g.add_task(TaskId::fx_split_join_t9());
        g.add_task(TaskId::fx_split_join_t10());

        g.add_queue(
            QueueIdWithUpstream::fx_split_join_q2(),
            TaskId::fx_split_join_t1(),
            TaskId::fx_split_join_t2(),
        );
        g.add_queue(
            QueueIdWithUpstream::fx_split_join_q3_1(),
            TaskId::fx_split_join_t2(),
            TaskId::fx_split_join_t3(),
        );
        g.add_queue(
            QueueIdWithUpstream::fx_split_join_q3_2(),
            TaskId::fx_split_join_t7(),
            TaskId::fx_split_join_t3(),
        );
        g.add_queue(
            QueueIdWithUpstream::fx_split_join_q4(),
            TaskId::fx_split_join_t3(),
            TaskId::fx_split_join_t4(),
        );
        g.add_queue(
            QueueIdWithUpstream::fx_split_join_q5(),
            TaskId::fx_split_join_t4(),
            TaskId::fx_split_join_t5(),
        );
        g.add_queue(
            QueueIdWithUpstream::fx_split_join_q7(),
            TaskId::fx_split_join_t6(),
            TaskId::fx_split_join_t7(),
        );
        g.add_queue(
            QueueIdWithUpstream::fx_split_join_q8(),
            TaskId::fx_split_join_t7(),
            TaskId::fx_split_join_t8(),
        );
        g.add_queue(
            QueueIdWithUpstream::fx_split_join_q9(),
            TaskId::fx_split_join_t8(),
            TaskId::fx_split_join_t9(),
        );
        g.add_queue(
            QueueIdWithUpstream::fx_split_join_q10(),
            TaskId::fx_split_join_t9(),
            TaskId::fx_split_join_t10(),
        );

        g
    }
}

impl TaskId {
    pub fn fx_split_join_t1() -> Self {
        TaskId::Source {
            id: "source_task1".to_string(),
        }
    }
    pub fn fx_split_join_t2() -> Self {
        TaskId::Pump {
            id: "pump_task2".to_string(),
            input_type: PumpInputType::Row,
        }
    }
    pub fn fx_split_join_t3() -> Self {
        TaskId::Pump {
            id: "pump_task3".to_string(),
            input_type: PumpInputType::Window,
        }
    }
    pub fn fx_split_join_t4() -> Self {
        TaskId::Pump {
            id: "pump_task4".to_string(),
            input_type: PumpInputType::Row,
        }
    }
    pub fn fx_split_join_t5() -> Self {
        TaskId::Sink {
            id: "sink_task5".to_string(),
        }
    }
    pub fn fx_split_join_t6() -> Self {
        TaskId::Source {
            id: "source_task6".to_string(),
        }
    }
    pub fn fx_split_join_t7() -> Self {
        TaskId::Pump {
            id: "pump_task7".to_string(),
            input_type: PumpInputType::Row,
        }
    }
    pub fn fx_split_join_t8() -> Self {
        TaskId::Pump {
            id: "pump_task8".to_string(),
            input_type: PumpInputType::Row,
        }
    }
    pub fn fx_split_join_t9() -> Self {
        TaskId::Pump {
            id: "pump_task9".to_string(),
            input_type: PumpInputType::Row,
        }
    }
    pub fn fx_split_join_t10() -> Self {
        TaskId::Sink {
            id: "sink_task10".to_string(),
        }
    }
}

impl QueueIdWithUpstream {
    pub fn fx_split_join_q2() -> Self {
        Self::new(QueueId::fx_split_join_q2(), StreamName::factory("ss1"))
    }
    pub fn fx_split_join_q3_1() -> Self {
        Self::new(QueueId::fx_split_join_q3_1(), StreamName::factory("s1"))
    }
    pub fn fx_split_join_q3_2() -> Self {
        Self::new(QueueId::fx_split_join_q3_2(), StreamName::factory("s3"))
    }
    pub fn fx_split_join_q4() -> Self {
        Self::new(QueueId::fx_split_join_q4(), StreamName::factory("s2"))
    }
    pub fn fx_split_join_q5() -> Self {
        Self::new(QueueId::fx_split_join_q5(), StreamName::factory("ss2"))
    }
    pub fn fx_split_join_q7() -> Self {
        Self::new(QueueId::fx_split_join_q7(), StreamName::factory("ss3"))
    }
    pub fn fx_split_join_q8() -> Self {
        Self::new(QueueId::fx_split_join_q8(), StreamName::factory("s3"))
    }
    pub fn fx_split_join_q9() -> Self {
        Self::new(QueueId::fx_split_join_q9(), StreamName::factory("s4"))
    }
    pub fn fx_split_join_q10() -> Self {
        Self::new(QueueId::fx_split_join_q10(), StreamName::factory("ss4"))
    }
}
impl QueueId {
    pub fn fx_split_join_q2() -> Self {
        Self::Row(RowQueueId::fx_q2())
    }
    pub fn fx_split_join_q3_1() -> Self {
        Self::Window(WindowQueueId::fx_q3_1())
    }
    pub fn fx_split_join_q3_2() -> Self {
        Self::Window(WindowQueueId::fx_q3_2())
    }
    pub fn fx_split_join_q4() -> Self {
        Self::Row(RowQueueId::fx_q4())
    }
    pub fn fx_split_join_q5() -> Self {
        Self::Row(RowQueueId::fx_q5())
    }
    pub fn fx_split_join_q7() -> Self {
        Self::Row(RowQueueId::fx_q7())
    }
    pub fn fx_split_join_q8() -> Self {
        Self::Row(RowQueueId::fx_q8())
    }
    pub fn fx_split_join_q9() -> Self {
        Self::Row(RowQueueId::fx_q9())
    }
    pub fn fx_split_join_q10() -> Self {
        Self::Row(RowQueueId::fx_q10())
    }
}
impl RowQueueId {
    pub fn fx_q2() -> Self {
        Self::new("q2".to_string())
    }
    pub fn fx_q4() -> Self {
        Self::new("q4".to_string())
    }
    pub fn fx_q5() -> Self {
        Self::new("q5".to_string())
    }
    pub fn fx_q7() -> Self {
        Self::new("q7".to_string())
    }
    pub fn fx_q8() -> Self {
        Self::new("q8".to_string())
    }
    pub fn fx_q9() -> Self {
        Self::new("q9".to_string())
    }
    pub fn fx_q10() -> Self {
        Self::new("q10".to_string())
    }
}
impl WindowQueueId {
    pub fn fx_q3_1() -> Self {
        Self::new("q3_1".to_string())
    }
    pub fn fx_q3_2() -> Self {
        Self::new("q3_2".to_string())
    }
}

impl PerformanceMetrics {
    pub fn fx_split_join() -> Self {
        let graph = TaskGraph::fx_split_join();
        let metrics = PerformanceMetrics::from_task_graph(&graph);

        for _ in 0..10 {
            metrics.update_by_task_execution(&MetricsUpdateByTaskExecution::fx_split_join_t1());
        }
        for _ in 0..3 {
            metrics.update_by_task_execution(&MetricsUpdateByTaskExecution::fx_split_join_t6());
        }

        for _ in 0..5 {
            metrics.update_by_task_execution(&MetricsUpdateByTaskExecution::fx_split_join_t2());
            metrics.update_by_task_execution(&MetricsUpdateByTaskExecution::fx_split_join_t3());
        }
        for _ in 0..1 {
            metrics.update_by_task_execution(&MetricsUpdateByTaskExecution::fx_split_join_t7());
            metrics.update_by_task_execution(&MetricsUpdateByTaskExecution::fx_split_join_t8());
            metrics.update_by_task_execution(&MetricsUpdateByTaskExecution::fx_split_join_t9());
            metrics.update_by_task_execution(&MetricsUpdateByTaskExecution::fx_split_join_t10());
        }

        metrics
    }
}

impl MetricsUpdateByTaskExecution {
    pub fn fx_split_join_t1() -> Self {
        let task = TaskMetricsUpdateByTask::new(
            TaskId::fx_split_join_t1(),
            WallClockDuration::from_micros(200),
        );
        let out_queues = vec![OutQueueMetricsUpdateByTask::new(
            QueueId::fx_split_join_q2(),
            1,
            100,
        )];
        Self::new(task, vec![], out_queues)
    }
    pub fn fx_split_join_t2() -> Self {
        let task = TaskMetricsUpdateByTask::new(
            TaskId::fx_split_join_t2(),
            WallClockDuration::from_micros(50),
        );
        let in_queues = vec![InQueueMetricsUpdateByTask::new(
            InQueueMetricsUpdateByCollect::Row {
                queue_id: RowQueueId::fx_q2(),
                rows_used: 1,
                bytes_used: 100,
            },
            None,
        )];
        let out_queues = vec![OutQueueMetricsUpdateByTask::new(
            QueueId::fx_split_join_q3_1(),
            1,
            80,
        )];
        Self::new(task, in_queues, out_queues)
    }
    pub fn fx_split_join_t3() -> Self {
        let task = TaskMetricsUpdateByTask::new(
            TaskId::fx_split_join_t3(),
            WallClockDuration::from_micros(90),
        );
        let in_queues = vec![InQueueMetricsUpdateByTask::new(
            InQueueMetricsUpdateByCollect::Window {
                queue_id: WindowQueueId::fx_q3_1(),
                waiting_bytes_dispatched: 80,
                waiting_rows_dispatched: 1,
            },
            Some(WindowInFlowByWindowTask::new(80, 0)),
        )];
        let out_queues = vec![];
        Self::new(task, in_queues, out_queues)
    }
    pub fn fx_split_join_t6() -> Self {
        let task = TaskMetricsUpdateByTask::new(
            TaskId::fx_split_join_t6(),
            WallClockDuration::from_micros(800),
        );
        let out_queues = vec![OutQueueMetricsUpdateByTask::new(
            QueueId::fx_split_join_q7(),
            1,
            800,
        )];
        Self::new(task, vec![], out_queues)
    }
    pub fn fx_split_join_t7() -> Self {
        let task = TaskMetricsUpdateByTask::new(
            TaskId::fx_split_join_t7(),
            WallClockDuration::from_micros(50),
        );
        let in_queues = vec![InQueueMetricsUpdateByTask::new(
            InQueueMetricsUpdateByCollect::Row {
                queue_id: RowQueueId::fx_q7(),
                rows_used: 1,
                bytes_used: 800,
            },
            None,
        )];
        let out_queues = vec![
            OutQueueMetricsUpdateByTask::new(QueueId::fx_split_join_q3_2(), 1, 150),
            OutQueueMetricsUpdateByTask::new(QueueId::fx_split_join_q8(), 1, 150),
        ];
        Self::new(task, in_queues, out_queues)
    }
    pub fn fx_split_join_t8() -> Self {
        let task = TaskMetricsUpdateByTask::new(
            TaskId::fx_split_join_t8(),
            WallClockDuration::from_micros(50),
        );
        let in_queues = vec![InQueueMetricsUpdateByTask::new(
            InQueueMetricsUpdateByCollect::Row {
                queue_id: RowQueueId::fx_q8(),
                rows_used: 1,
                bytes_used: 150,
            },
            None,
        )];
        let out_queues = vec![OutQueueMetricsUpdateByTask::new(
            QueueId::fx_split_join_q9(),
            1,
            120,
        )];
        Self::new(task, in_queues, out_queues)
    }
    pub fn fx_split_join_t9() -> Self {
        let task = TaskMetricsUpdateByTask::new(
            TaskId::fx_split_join_t9(),
            WallClockDuration::from_micros(50),
        );
        let in_queues = vec![InQueueMetricsUpdateByTask::new(
            InQueueMetricsUpdateByCollect::Row {
                queue_id: RowQueueId::fx_q9(),
                rows_used: 1,
                bytes_used: 120,
            },
            None,
        )];
        let out_queues = vec![OutQueueMetricsUpdateByTask::new(
            QueueId::fx_split_join_q10(),
            1,
            70,
        )];
        Self::new(task, in_queues, out_queues)
    }
    pub fn fx_split_join_t10() -> Self {
        let task = TaskMetricsUpdateByTask::new(
            TaskId::fx_split_join_t10(),
            WallClockDuration::from_micros(200),
        );
        let in_queues = vec![InQueueMetricsUpdateByTask::new(
            InQueueMetricsUpdateByCollect::Row {
                queue_id: RowQueueId::fx_q10(),
                rows_used: 1,
                bytes_used: 70,
            },
            None,
        )];
        let out_queues = vec![];
        Self::new(task, in_queues, out_queues)
    }
}
