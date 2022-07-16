// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

mod test_support;
use crate::test_support::*;
use springql::SpringPipeline;

#[test]
fn test_spring_open_twice() {
    let config = default_config();
    SpringPipeline::new(&config).unwrap();
    SpringPipeline::new(&config).unwrap();
}
