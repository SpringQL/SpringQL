// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

use std::sync::Once;

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
