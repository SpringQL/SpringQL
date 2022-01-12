// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

use springql_core::low_level_rs::{spring_config_default, spring_open};

#[test]
fn test_spring_open_twice() {
    let _ = spring_open(spring_config_default()).unwrap();
    let _ = spring_open(spring_config_default()).unwrap();
}
