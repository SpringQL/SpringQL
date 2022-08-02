// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

mod test_support;

use std::{
    path::{Path, PathBuf},
    process::{Child, Command},
    str::FromStr,
};

use springql::*;
use springql_foreign_service::sink::ForeignSink;
use springql_test_logger::setup_test_logger_with_level;

use crate::test_support::*;

const REPLAYMAN_HOST: &str = "127.0.0.1";
const ENGINE_REPLAYMAN_PORT: u16 = 19870;
const VEHICLE_CONTROL_REPLAYMAN_PORT: u16 = 19871;

fn config() -> SpringConfig {
    let mut config = SpringConfig::default();

    config.memory.upper_limit_bytes = 700_000;
    config.memory.severe_to_critical_percent = 80;
    config.memory.moderate_to_severe_percent = 50;
    config.memory.critical_to_severe_percent = 70;
    config.memory.severe_to_moderate_percent = 40;

    config.web_console.enable_report_post = true;
    config.web_console.report_interval_msec = 1000;

    config
}

const INITIAL_TIMESTAMP: &str = "2020-10-21T10:37:56.000+09:00";

fn spawn_engine_replayman(engine_dataset: &Path) -> Child {
    // prerequisite: `cargo install replayman`
    Command::new("replayman")
        .arg("--timed-by")
        .arg("Time")
        .arg("--initial-timestamp")
        .arg(INITIAL_TIMESTAMP)
        .arg("--dest-tcp")
        .arg(format!("{}:{}", REPLAYMAN_HOST, ENGINE_REPLAYMAN_PORT))
        .arg(engine_dataset)
        .spawn()
        .expect("failed to execute replayman command as child process")
}

fn spawn_vehicle_control_replayman(vehicle_control_dataset: &Path) -> Child {
    // prerequisite: `cargo install replayman`
    Command::new("replayman")
        .arg("--timed-by")
        .arg("Time")
        .arg("--initial-timestamp")
        .arg(INITIAL_TIMESTAMP)
        .arg("--dest-tcp")
        .arg(format!(
            "{}:{}",
            REPLAYMAN_HOST, VEHICLE_CONTROL_REPLAYMAN_PORT
        ))
        .arg(vehicle_control_dataset)
        .spawn()
        .expect("failed to execute replayman command as child process")
}

#[test]
fn test_e2e_replayman_aggr_join_split() {
    setup_test_logger_with_level(log::LevelFilter::Warn);

    let sink_engine_vehicle_control = ForeignSink::start().unwrap();
    let sink_phy_vehicle_speed = ForeignSink::start().unwrap();

    let ddls = vec![
        "
          CREATE SOURCE STREAM source_engine (
            Time TIMESTAMP NOT NULL ROWTIME,    
            Rpm FLOAT NOT NULL
          );
          "
        .to_string(),
        "
          CREATE SOURCE STREAM source_vehicle_control (
            Time TIMESTAMP NOT NULL ROWTIME,
            SpeedSignalFreq INTEGER NOT NULL,
            Speed FLOAT NOT NULL
          );
          "
        .to_string(),
        "
        CREATE STREAM sampled_engine (
          ts TIMESTAMP NOT NULL ROWTIME,    
          rpm FLOAT NOT NULL
        );
        "
        .to_string(),
        "
        CREATE STREAM sampled_vehicle_control (
          ts TIMESTAMP NOT NULL ROWTIME,    
          speed FLOAT NOT NULL
        );
        "
        .to_string(),
        "
        CREATE SINK STREAM sink_phy_vehicle_speed (
          ts TIMESTAMP NOT NULL ROWTIME,    
          phy_speed FLOAT
        );
        "
        .to_string(),
        "
        CREATE SINK STREAM sink_engine_vehicle_control (
          ts TIMESTAMP NOT NULL ROWTIME,    
          rpm FLOAT NOT NULL,
          speed FLOAT
        );
        "
        .to_string(),
        "
        CREATE PUMP pu_sample_engine AS
          INSERT INTO sampled_engine (ts, rpm)
          SELECT STREAM
            FLOOR_TIME(source_engine.Time, DURATION_MILLIS(100)) AS sampled_ts,
            AVG(source_engine.Rpm) AS avg_rpm
          FROM source_engine
          GROUP BY sampled_ts
          FIXED WINDOW DURATION_MILLIS(100), DURATION_MILLIS(100);
          "
        .to_string(),
        "
        CREATE PUMP pu_sample_vehicle_control AS
          INSERT INTO sampled_vehicle_control (ts, speed)
          SELECT STREAM
            FLOOR_TIME(source_vehicle_control.Time, DURATION_MILLIS(100)) AS sampled_ts,
            AVG(source_vehicle_control.Speed) AS avg_speed
          FROM source_vehicle_control
          GROUP BY sampled_ts
          FIXED WINDOW DURATION_MILLIS(100), DURATION_MILLIS(100);
          "
        .to_string(),
        "
          CREATE PUMP pu_join AS
            INSERT INTO sink_engine_vehicle_control (ts, rpm, speed)
            SELECT STREAM
              sampled_engine.ts,
              sampled_engine.rpm,
              sampled_vehicle_control.speed
            FROM sampled_engine
            LEFT OUTER JOIN sampled_vehicle_control
              ON sampled_engine.ts = sampled_vehicle_control.ts
              FIXED WINDOW DURATION_MILLIS(1000), DURATION_MILLIS(500);
          "
        .to_string(),
        "
          CREATE PUMP pu_phy_conversion AS
            INSERT INTO sink_phy_vehicle_speed (ts, phy_speed)
            SELECT STREAM
              sampled_vehicle_control.ts,
              sampled_vehicle_control.speed * 0.3
            FROM sampled_vehicle_control;
          "
        .to_string(),
        format!(
            "
          CREATE SINK WRITER tcp_sink_engine_vehicle_control FOR sink_engine_vehicle_control
            TYPE NET_CLIENT OPTIONS (
              PROTOCOL 'TCP',
              REMOTE_HOST '{remote_host}',
              REMOTE_PORT '{remote_port}'
          );
          ",
            remote_host = sink_engine_vehicle_control.host_ip(),
            remote_port = sink_engine_vehicle_control.port()
        ),
        format!(
            "
        CREATE SINK WRITER tcp_sink_phy_vehicle_speed FOR sink_phy_vehicle_speed
          TYPE NET_CLIENT OPTIONS (
            PROTOCOL 'TCP',
            REMOTE_HOST '{remote_host}',
            REMOTE_PORT '{remote_port}'
        );
        ",
            remote_host = sink_phy_vehicle_speed.host_ip(),
            remote_port = sink_phy_vehicle_speed.port()
        ),
        format!(
            "
          CREATE SOURCE READER tcp_source_engine FOR source_engine
            TYPE NET_SERVER OPTIONS (
              PROTOCOL 'TCP',
              PORT '{port}'
            );
          ",
            port = ENGINE_REPLAYMAN_PORT
        ),
        format!(
            "
          CREATE SOURCE READER tcp_source_vehicle_control FOR source_vehicle_control
            TYPE NET_SERVER OPTIONS (
              PROTOCOL 'TCP',
              PORT '{port}'
          );
        ",
            port = VEHICLE_CONTROL_REPLAYMAN_PORT
        ),
    ];

    // start pipeline
    let _pipeline = apply_ddls(&ddls, config());

    // start data input
    let mut engine_replayman = spawn_engine_replayman(
        &PathBuf::from_str(
            "/Users/sho.nakatani/.ghq/src/github.com/SpringQL/SpringQL/Engine-30sec-per25usec.tsv",
        )
        .unwrap(),
    );
    let mut vehicle_control_replayman = spawn_vehicle_control_replayman(
        &PathBuf::from_str("/Users/sho.nakatani/.ghq/src/github.com/SpringQL/SpringQL/VehicleControl-30sec-per25usec.tsv").unwrap(),
    );

    let ecode_engine = engine_replayman
        .wait()
        .expect("failed to wait on engine replayman");
    let ecode_vehicle_control = vehicle_control_replayman
        .wait()
        .expect("failed to wait on vehicle control replayman");

    assert!(ecode_engine.success());
    assert!(ecode_vehicle_control.success());

    // get outputs
    let _rows_sink_engine_vehicle_control = drain_from_sink(&sink_engine_vehicle_control);
    // log::error!("{:#?}", rows_sink_engine_vehicle_control);
    // log::error!("{}", rows_sink_engine_vehicle_control.len());

    let _rows_sink_phy_vehicle_speed = drain_from_sink(&sink_phy_vehicle_speed);
    // log::error!("{:#?}", rows_sink_phy_vehicle_speed);
    // log::error!("{}", rows_sink_phy_vehicle_speed.len());
}
