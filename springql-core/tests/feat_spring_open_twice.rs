use springql_core::low_level_rs::spring_open;

#[test]
fn test_spring_open_twice() {
    let _ = spring_open().unwrap();
    let _ = spring_open().unwrap();
}
