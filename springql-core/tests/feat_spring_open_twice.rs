// Copyright (c) 2022 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

use springql_core::low_level_rs::{spring_config_default, spring_open};

#[test]
fn test_spring_open_twice() {
    let config = spring_config_default();
    let _ = spring_open(&config).unwrap();
    let _ = spring_open(&config).unwrap();
}
