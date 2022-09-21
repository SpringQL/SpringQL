// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use springql_release_test::SpringError;

// https://rust-lang.github.io/api-guidelines/interoperability.html#error-types-are-meaningful-and-well-behaved-c-good-err
#[test]
fn test_api_guidelines_c_good_err() {
    use std::error::Error;
    use std::fmt::Display;

    fn assert_error<T: Error + Send + Sync + 'static>() {}
    assert_error::<SpringError>();

    fn assert_display<T: Display>() {}
    assert_display::<SpringError>();
}
