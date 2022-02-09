// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

use std::net::IpAddr;

use serde_json::json;

use crate::{
    pipeline::{
        name::StreamName, pipeline_version::PipelineVersion,
        pump_model::pump_input_type::PumpInputType, sink_writer_model::SinkWriterModel,
        source_reader_model::SourceReaderModel, stream_model::StreamModel,
    },
    stream_engine::{
        autonomous_executor::{
            performance_metrics::metrics_update_command::metrics_update_by_task_execution::{
                InQueueMetricsUpdateByCollect, OutQueueMetricsUpdateByTask,
                TaskMetricsUpdateByTask,
            },
            row::{
                column::stream_column::StreamColumns, foreign_row::format::json::JsonObject, Row,
            },
            task::tuple::Tuple,
            task_graph::{
                queue_id::{row_queue_id::RowQueueId, window_queue_id::WindowQueueId, QueueId},
                task_id::TaskId,
            },
        },
        command::alter_pipeline_command::AlterPipelineCommand,
        time::duration::{wall_clock_duration::WallClockDuration, SpringDuration},
    },
    stream_engine::{
        autonomous_executor::{
            performance_metrics::{
                metrics_update_command::metrics_update_by_task_execution::MetricsUpdateByTaskExecution,
                PerformanceMetrics,
            },
            row::foreign_row::source_row::SourceRow,
            task_graph::TaskGraph,
        },
        time::timestamp::Timestamp,
        SinkRow,
    },
};

impl Timestamp {
    pub(crate) fn fx_ts1() -> Self {
        "2021-01-01 13:00:00.000000001".parse().unwrap()
    }
    pub(crate) fn fx_ts2() -> Self {
        "2021-01-01 13:00:00.000000002".parse().unwrap()
    }
    pub(crate) fn fx_ts3() -> Self {
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

impl SourceRow {
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

impl SinkRow {
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
        Self::new(StreamColumns::fx_city_temperature_tokyo())
    }
    pub(in crate::stream_engine) fn fx_city_temperature_osaka() -> Self {
        Self::new(StreamColumns::fx_city_temperature_osaka())
    }
    pub(in crate::stream_engine) fn fx_city_temperature_london() -> Self {
        Self::new(StreamColumns::fx_city_temperature_london())
    }

    pub(in crate::stream_engine) fn fx_trade_oracle() -> Self {
        Self::new(StreamColumns::fx_trade_oracle())
    }
    pub(in crate::stream_engine) fn fx_trade_ibm() -> Self {
        Self::new(StreamColumns::fx_trade_ibm())
    }
    pub(in crate::stream_engine) fn fx_trade_google() -> Self {
        Self::new(StreamColumns::fx_trade_google())
    }

    pub(in crate::stream_engine) fn fx_no_promoted_rowtime() -> Self {
        Self::new(StreamColumns::fx_no_promoted_rowtime())
    }
}

impl Tuple {
    pub(in crate::stream_engine) fn fx_trade_oracle() -> Self {
        let row = Row::fx_trade_oracle();
        Self::from_row(row)
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
    pub(in crate::stream_engine) fn fx_create_source_stream_trade(stream_name: StreamName) -> Self {
        let stream = StreamModel::fx_trade_with_name(stream_name);
        Self::CreateSourceStream(stream)
    }

    pub(in crate::stream_engine) fn fx_create_source_reader_trade(
        stream_name: StreamName,
        source_server_host: IpAddr,
        source_server_port: u16,
    ) -> Self {
        let source = SourceReaderModel::fx_net(stream_name, source_server_host, source_server_port);
        Self::CreateSourceReader(source)
    }

    pub(in crate::stream_engine) fn fx_create_sink_stream_trade(stream_name: StreamName) -> Self {
        let stream = StreamModel::fx_trade_with_name(stream_name);
        Self::CreateSinkStream(stream)
    }

    pub(in crate::stream_engine) fn fx_create_sink_writer_trade(
        stream_name: StreamName,
        sink_server_host: IpAddr,
        sink_server_port: u16,
    ) -> Self {
        let sink = SinkWriterModel::fx_net(stream_name, sink_server_host, sink_server_port);
        Self::CreateSinkWriter(sink)
    }
}

impl TaskGraph {
    pub(in crate::stream_engine) fn fx_split_join() -> Self {
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
            QueueId::fx_split_join_q2(),
            TaskId::fx_split_join_t1(),
            TaskId::fx_split_join_t2(),
        );
        g.add_queue(
            QueueId::fx_split_join_q3_1(),
            TaskId::fx_split_join_t2(),
            TaskId::fx_split_join_t3(),
        );
        g.add_queue(
            QueueId::fx_split_join_q3_2(),
            TaskId::fx_split_join_t7(),
            TaskId::fx_split_join_t3(),
        );
        g.add_queue(
            QueueId::fx_split_join_q4(),
            TaskId::fx_split_join_t3(),
            TaskId::fx_split_join_t4(),
        );
        g.add_queue(
            QueueId::fx_split_join_q5(),
            TaskId::fx_split_join_t4(),
            TaskId::fx_split_join_t5(),
        );
        g.add_queue(
            QueueId::fx_split_join_q7(),
            TaskId::fx_split_join_t6(),
            TaskId::fx_split_join_t7(),
        );
        g.add_queue(
            QueueId::fx_split_join_q8(),
            TaskId::fx_split_join_t7(),
            TaskId::fx_split_join_t8(),
        );
        g.add_queue(
            QueueId::fx_split_join_q9(),
            TaskId::fx_split_join_t8(),
            TaskId::fx_split_join_t9(),
        );
        g.add_queue(
            QueueId::fx_split_join_q10(),
            TaskId::fx_split_join_t9(),
            TaskId::fx_split_join_t10(),
        );

        g
    }
}

impl TaskId {
    pub(in crate::stream_engine) fn fx_split_join_t1() -> Self {
        TaskId::Source {
            id: "source_task1".to_string(),
        }
    }
    pub(in crate::stream_engine) fn fx_split_join_t2() -> Self {
        TaskId::Pump {
            id: "pump_task2".to_string(),
            input_type: PumpInputType::Row,
        }
    }
    pub(in crate::stream_engine) fn fx_split_join_t3() -> Self {
        TaskId::Pump {
            id: "pump_task3".to_string(),
            input_type: PumpInputType::Window,
        }
    }
    pub(in crate::stream_engine) fn fx_split_join_t4() -> Self {
        TaskId::Pump {
            id: "pump_task4".to_string(),
            input_type: PumpInputType::Row,
        }
    }
    pub(in crate::stream_engine) fn fx_split_join_t5() -> Self {
        TaskId::Sink {
            id: "sink_task5".to_string(),
        }
    }
    pub(in crate::stream_engine) fn fx_split_join_t6() -> Self {
        TaskId::Source {
            id: "source_task6".to_string(),
        }
    }
    pub(in crate::stream_engine) fn fx_split_join_t7() -> Self {
        TaskId::Pump {
            id: "pump_task7".to_string(),
            input_type: PumpInputType::Row,
        }
    }
    pub(in crate::stream_engine) fn fx_split_join_t8() -> Self {
        TaskId::Pump {
            id: "pump_task8".to_string(),
            input_type: PumpInputType::Row,
        }
    }
    pub(in crate::stream_engine) fn fx_split_join_t9() -> Self {
        TaskId::Pump {
            id: "pump_task9".to_string(),
            input_type: PumpInputType::Row,
        }
    }
    pub(in crate::stream_engine) fn fx_split_join_t10() -> Self {
        TaskId::Sink {
            id: "sink_task10".to_string(),
        }
    }
}

impl QueueId {
    pub(in crate::stream_engine) fn fx_split_join_q2() -> Self {
        Self::Row(RowQueueId::fx_q2())
    }
    pub(in crate::stream_engine) fn fx_split_join_q3_1() -> Self {
        Self::Window(WindowQueueId::fx_q3_1())
    }
    pub(in crate::stream_engine) fn fx_split_join_q3_2() -> Self {
        Self::Window(WindowQueueId::fx_q3_2())
    }
    pub(in crate::stream_engine) fn fx_split_join_q4() -> Self {
        Self::Row(RowQueueId::fx_q4())
    }
    pub(in crate::stream_engine) fn fx_split_join_q5() -> Self {
        Self::Row(RowQueueId::fx_q5())
    }
    pub(in crate::stream_engine) fn fx_split_join_q7() -> Self {
        Self::Row(RowQueueId::fx_q7())
    }
    pub(in crate::stream_engine) fn fx_split_join_q8() -> Self {
        Self::Row(RowQueueId::fx_q8())
    }
    pub(in crate::stream_engine) fn fx_split_join_q9() -> Self {
        Self::Row(RowQueueId::fx_q9())
    }
    pub(in crate::stream_engine) fn fx_split_join_q10() -> Self {
        Self::Row(RowQueueId::fx_q10())
    }
}
impl RowQueueId {
    pub(in crate::stream_engine) fn fx_q2() -> Self {
        Self::new("q2".to_string())
    }
    pub(in crate::stream_engine) fn fx_q4() -> Self {
        Self::new("q4".to_string())
    }
    pub(in crate::stream_engine) fn fx_q5() -> Self {
        Self::new("q5".to_string())
    }
    pub(in crate::stream_engine) fn fx_q7() -> Self {
        Self::new("q7".to_string())
    }
    pub(in crate::stream_engine) fn fx_q8() -> Self {
        Self::new("q8".to_string())
    }
    pub(in crate::stream_engine) fn fx_q9() -> Self {
        Self::new("q9".to_string())
    }
    pub(in crate::stream_engine) fn fx_q10() -> Self {
        Self::new("q10".to_string())
    }
}
impl WindowQueueId {
    pub(in crate::stream_engine) fn fx_q3_1() -> Self {
        Self::new("q3_1".to_string())
    }
    pub(in crate::stream_engine) fn fx_q3_2() -> Self {
        Self::new("q3_2".to_string())
    }
}

impl PerformanceMetrics {
    pub(in crate::stream_engine) fn fx_split_join() -> Self {
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
    pub(in crate::stream_engine) fn fx_split_join_t1() -> Self {
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
    pub(in crate::stream_engine) fn fx_split_join_t2() -> Self {
        let task = TaskMetricsUpdateByTask::new(
            TaskId::fx_split_join_t2(),
            WallClockDuration::from_micros(50),
        );
        let in_queues = vec![InQueueMetricsUpdateByCollect::Row {
            queue_id: RowQueueId::fx_q2(),
            rows_used: 1,
            bytes_used: 100,
        }];
        let out_queues = vec![OutQueueMetricsUpdateByTask::new(
            QueueId::fx_split_join_q3_1(),
            1,
            80,
        )];
        Self::new(task, in_queues, out_queues)
    }
    pub(in crate::stream_engine) fn fx_split_join_t3() -> Self {
        let task = TaskMetricsUpdateByTask::new(
            TaskId::fx_split_join_t3(),
            WallClockDuration::from_micros(90),
        );
        let in_queues = vec![InQueueMetricsUpdateByCollect::Window {
            queue_id: WindowQueueId::fx_q3_1(),
            waiting_bytes_dispatched: 80,
            waiting_rows_dispatched: 1,
            window_gain_bytes_rows: 80,
            window_gain_bytes_states: 0,
        }];
        let out_queues = vec![];
        Self::new(task, in_queues, out_queues)
    }
    pub(in crate::stream_engine) fn fx_split_join_t6() -> Self {
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
    pub(in crate::stream_engine) fn fx_split_join_t7() -> Self {
        let task = TaskMetricsUpdateByTask::new(
            TaskId::fx_split_join_t7(),
            WallClockDuration::from_micros(50),
        );
        let in_queues = vec![InQueueMetricsUpdateByCollect::Row {
            queue_id: RowQueueId::fx_q7(),
            rows_used: 1,
            bytes_used: 800,
        }];
        let out_queues = vec![
            OutQueueMetricsUpdateByTask::new(QueueId::fx_split_join_q3_2(), 1, 150),
            OutQueueMetricsUpdateByTask::new(QueueId::fx_split_join_q8(), 1, 150),
        ];
        Self::new(task, in_queues, out_queues)
    }
    pub(in crate::stream_engine) fn fx_split_join_t8() -> Self {
        let task = TaskMetricsUpdateByTask::new(
            TaskId::fx_split_join_t8(),
            WallClockDuration::from_micros(50),
        );
        let in_queues = vec![InQueueMetricsUpdateByCollect::Row {
            queue_id: RowQueueId::fx_q8(),
            rows_used: 1,
            bytes_used: 150,
        }];
        let out_queues = vec![OutQueueMetricsUpdateByTask::new(
            QueueId::fx_split_join_q9(),
            1,
            120,
        )];
        Self::new(task, in_queues, out_queues)
    }
    pub(in crate::stream_engine) fn fx_split_join_t9() -> Self {
        let task = TaskMetricsUpdateByTask::new(
            TaskId::fx_split_join_t9(),
            WallClockDuration::from_micros(50),
        );
        let in_queues = vec![InQueueMetricsUpdateByCollect::Row {
            queue_id: RowQueueId::fx_q9(),
            rows_used: 1,
            bytes_used: 120,
        }];
        let out_queues = vec![OutQueueMetricsUpdateByTask::new(
            QueueId::fx_split_join_q10(),
            1,
            70,
        )];
        Self::new(task, in_queues, out_queues)
    }
    pub(in crate::stream_engine) fn fx_split_join_t10() -> Self {
        let task = TaskMetricsUpdateByTask::new(
            TaskId::fx_split_join_t10(),
            WallClockDuration::from_micros(200),
        );
        let in_queues = vec![InQueueMetricsUpdateByCollect::Row {
            queue_id: RowQueueId::fx_q10(),
            rows_used: 1,
            bytes_used: 70,
        }];
        let out_queues = vec![];
        Self::new(task, in_queues, out_queues)
    }
}
