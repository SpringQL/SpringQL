// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use springql::{SpringConfig, SpringPipeline, SpringConfigExt};

#[test]
fn test_spring_open_twice() {
    let config = SpringConfig::from_toml("").unwrap();
    SpringPipeline::new(&config).unwrap();
    SpringPipeline::new(&config).unwrap();
}
