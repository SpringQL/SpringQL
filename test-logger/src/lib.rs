use std::sync::Once;

/// setup env_logger for test.
pub fn setup_test_logger() {
    static INIT: Once = Once::new();

    INIT.call_once(|| {
        let _ = env_logger::builder()
            .is_test(false) // To enable color. Logs are not captured by test framework.
            .try_init();
    });

    log::info!("setup_test_logger(): done");
}
