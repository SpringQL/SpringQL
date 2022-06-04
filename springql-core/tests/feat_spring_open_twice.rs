// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use springql_core::api::{spring_config_default, SpringPipeline};

#[test]
fn test_spring_open_twice() {
    let config = spring_config_default();
    SpringPipeline::new(&config).unwrap();
    SpringPipeline::new(&config).unwrap();
}
