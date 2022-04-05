// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use springql_core::low_level_rs::{spring_config_default, spring_open};

#[test]
fn test_spring_open_twice() {
    let config = spring_config_default();
    let _ = spring_open(&config).unwrap();
    let _ = spring_open(&config).unwrap();
}
