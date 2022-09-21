// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use std::{
    io::copy,
    path::Path,
    process::{Child, Command},
    time::Duration,
};

use springql_foreign_service::sink::ForeignSink;
use springql_release_test::{SpringConfig, SpringPipeline};

use tempfile::NamedTempFile;

const LOCALHOST: &str = "127.0.0.1";

const ENGINE_REPLAYMAN_PORT: u16 = 19870;
const WHEEL_SPEED_REPLAYMAN_PORT: u16 = 19871;

const INITIAL_TIMESTAMP: &str = "2020-10-21T10:37:56.000+09:00";

fn setup_logger() {
    let _ = env_logger::builder().is_test(false).try_init();
}

fn http_download_file_to_tempdir(url: &str) -> NamedTempFile {
    log::info!("Downloading file from {}", url);

    let fname = url.split('/').last().unwrap();

    let mut tempfile = tempfile::Builder::new()
        .suffix(&format!("-{}", fname))
        .tempfile()
        .unwrap();
    let response = reqwest::blocking::get(url).unwrap();

    let content = response.text().unwrap();
    copy(&mut content.as_bytes(), &mut tempfile).unwrap();

    log::info!("Finish downloading into {}", tempfile.path().display());
    tempfile
}
fn spawn_engine_replayman(engine_dataset: &Path) -> Child {
    // prerequisite: `cargo install replayman`
    Command::new("replayman")
        .arg("--timed-by")
        .arg("Time")
        .arg("--initial-timestamp")
        .arg(INITIAL_TIMESTAMP)
        .arg("--dest-tcp")
        .arg(format!("{}:{}", LOCALHOST, ENGINE_REPLAYMAN_PORT))
        .arg(engine_dataset)
        .spawn()
        .expect("failed to execute replayman command as child process")
}

fn spawn_wheel_speed_replayman(wheel_speed_dataset: &Path) -> Child {
    // prerequisite: `cargo install replayman`
    Command::new("replayman")
        .arg("--timed-by")
        .arg("Time")
        .arg("--initial-timestamp")
        .arg(INITIAL_TIMESTAMP)
        .arg("--dest-tcp")
        .arg(format!("{}:{}", LOCALHOST, WHEEL_SPEED_REPLAYMAN_PORT))
        .arg(wheel_speed_dataset)
        .spawn()
        .expect("failed to execute replayman command as child process")
}

fn main() {
    setup_logger();

    let engine_dataset = http_download_file_to_tempdir("https://raw.githubusercontent.com/SpringQL/dataset/main/pseudo-in-vehicle/Engine-30sec.tsv");
    let wheel_speed_dataset = http_download_file_to_tempdir("https://raw.githubusercontent.com/SpringQL/dataset/main/pseudo-in-vehicle/VehicleControl-30sec.tsv");

    // start servers to listen to sinks' output
    let sink_engine_wheel_speed = ForeignSink::start().unwrap();
    let sink_vehicle_speed = ForeignSink::start().unwrap();

    let pipeline = SpringPipeline::new(&SpringConfig::default()).unwrap();
    pipeline
        .command(
            "
    CREATE SOURCE STREAM source_engine (
      Time TIMESTAMP NOT NULL ROWTIME,    
      Rpm FLOAT NOT NULL
    );
    ",
        )
        .unwrap();

    pipeline
        .command(
            "
          CREATE SOURCE STREAM source_wheel_speed (
            Time TIMESTAMP NOT NULL ROWTIME,
            SpeedSignalFreq INTEGER NOT NULL,
            Speed FLOAT NOT NULL
          );
          ",
        )
        .unwrap();
    pipeline
        .command(
            "
        CREATE STREAM sampled_engine (
          ts TIMESTAMP NOT NULL ROWTIME,    
          rpm FLOAT NOT NULL
        );
        ",
        )
        .unwrap();
    pipeline
        .command(
            "
        CREATE STREAM sampled_wheel_speed (
          ts TIMESTAMP NOT NULL ROWTIME,    
          speed FLOAT NOT NULL
        );
        ",
        )
        .unwrap();
    pipeline
        .command(
            "
        CREATE SINK STREAM sink_vehicle_speed (
          ts TIMESTAMP NOT NULL ROWTIME,    
          speed FLOAT
        );
        ",
        )
        .unwrap();
    pipeline
        .command(
            "
        CREATE SINK STREAM sink_engine_wheel_speed (
          ts TIMESTAMP NOT NULL ROWTIME,    
          rpm FLOAT NOT NULL,
          speed FLOAT
        );
        ",
        )
        .unwrap();
    pipeline
        .command(
            "
        CREATE PUMP pu_sample_engine AS
          INSERT INTO sampled_engine (ts, rpm)
          SELECT STREAM
            FLOOR_TIME(source_engine.Time, DURATION_MILLIS(100)) AS sampled_ts,
            AVG(source_engine.Rpm) AS avg_rpm
          FROM source_engine
          GROUP BY sampled_ts
          FIXED WINDOW DURATION_MILLIS(100), DURATION_MILLIS(100);
          ",
        )
        .unwrap();
    pipeline
        .command(
            "
        CREATE PUMP pu_sample_wheel_speed AS
          INSERT INTO sampled_wheel_speed (ts, speed)
          SELECT STREAM
            FLOOR_TIME(source_wheel_speed.Time, DURATION_MILLIS(100)) AS sampled_ts,
            AVG(source_wheel_speed.Speed) AS avg_speed
          FROM source_wheel_speed
          GROUP BY sampled_ts
          FIXED WINDOW DURATION_MILLIS(100), DURATION_MILLIS(100);
          ",
        )
        .unwrap();
    pipeline
        .command(
            "
          CREATE PUMP pu_join AS
            INSERT INTO sink_engine_wheel_speed (ts, rpm, speed)
            SELECT STREAM
              sampled_engine.ts,
              sampled_engine.rpm,
              sampled_wheel_speed.speed
            FROM sampled_engine
            LEFT OUTER JOIN sampled_wheel_speed
              ON sampled_engine.ts = sampled_wheel_speed.ts
              FIXED WINDOW DURATION_MILLIS(1000), DURATION_MILLIS(500);
          ",
        )
        .unwrap();
    pipeline
        .command(
            "
          CREATE PUMP pu_phy_conversion AS
            INSERT INTO sink_vehicle_speed (ts, speed)
            SELECT STREAM
              sampled_wheel_speed.ts,
              sampled_wheel_speed.speed * 0.3
            FROM sampled_wheel_speed;
          ",
        )
        .unwrap();
    pipeline
        .command(format!(
            "
          CREATE SINK WRITER tcp_sink_engine_wheel_speed FOR sink_engine_wheel_speed
            TYPE NET_CLIENT OPTIONS (
              PROTOCOL 'TCP',
              REMOTE_HOST '{remote_host}',
              REMOTE_PORT '{remote_port}'
          );
          ",
            remote_host = sink_engine_wheel_speed.host_ip(),
            remote_port = sink_engine_wheel_speed.port(),
        ))
        .unwrap();
    pipeline
        .command(format!(
            "
        CREATE SINK WRITER tcp_sink_vehicle_speed FOR sink_vehicle_speed
          TYPE NET_CLIENT OPTIONS (
            PROTOCOL 'TCP',
            REMOTE_HOST '{remote_host}',
            REMOTE_PORT '{remote_port}'
        );
        ",
            remote_host = sink_vehicle_speed.host_ip(),
            remote_port = sink_vehicle_speed.port()
        ))
        .unwrap();

    // start source readers
    pipeline
        .command(format!(
            "
          CREATE SOURCE READER tcp_source_engine FOR source_engine
            TYPE NET_SERVER OPTIONS (
              PROTOCOL 'TCP',
              PORT '{port}'
            );
          ",
            port = ENGINE_REPLAYMAN_PORT
        ))
        .unwrap();
    pipeline
        .command(format!(
            "
          CREATE SOURCE READER tcp_source_wheel_speed FOR source_wheel_speed
            TYPE NET_SERVER OPTIONS (
              PROTOCOL 'TCP',
              PORT '{port}'
          );
        ",
            port = WHEEL_SPEED_REPLAYMAN_PORT
        ))
        .unwrap();

    // start external source streams
    let mut engine_replayman = spawn_engine_replayman(engine_dataset.path());
    let mut wheel_speed_replayman = spawn_wheel_speed_replayman(wheel_speed_dataset.path());

    // print sinks' outputs while waiting for replayman processes to finish
    let mut ecode_engine = None;
    let mut ecode_wheel_speed = None;
    while ecode_engine.is_none() || ecode_wheel_speed.is_none() {
        if let Some(v) = sink_engine_wheel_speed.try_receive(Duration::from_millis(100)) {
            println!("sink_engine_wheel_speed\t{}", v);
        }
        if let Some(v) = sink_vehicle_speed.try_receive(Duration::from_millis(100)) {
            println!("sink_vehicle_speed\t{:?}", v);
        }

        ecode_engine = engine_replayman
            .try_wait()
            .expect("failed to wait on engine replayman");
        ecode_wheel_speed = wheel_speed_replayman
            .try_wait()
            .expect("failed to wait on vehicle control replayman");
    }

    // check exit statuses of replayman processes
    assert!(ecode_engine.unwrap().success());
    assert!(ecode_wheel_speed.unwrap().success());
}
