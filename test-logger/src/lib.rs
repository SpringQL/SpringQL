// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use std::sync::Once;

use log::LevelFilter;

/// setup env_logger for test.
pub fn setup_test_logger() {
    static INIT: Once = Once::new();

    INIT.call_once(|| {
        let _ = env_logger::builder()
            .is_test(false) // To enable color. Logs are not captured by test framework.
            .try_init();

        log_panics::init();
    });

    log::info!("setup_test_logger(): done");
}

pub fn setup_test_logger_with_level(level: LevelFilter) {
    static INIT: Once = Once::new();

    INIT.call_once(|| {
        let _ = env_logger::builder()
            .is_test(false) // To enable color. Logs are not captured by test framework.
            .filter_level(level)
            .try_init();

        log_panics::init();
    });

    log::info!("setup_test_logger({}): done", level);
}
