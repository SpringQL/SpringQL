use springql_core::{error::SpringError, low_level_rs::spring_open};

#[test]
fn test_spring_open_twice_unavailable() {
    let _ = spring_open().unwrap();

    assert!(matches!(
        spring_open().unwrap_err(),
        SpringError::Unavailable { .. }
    ))
}
