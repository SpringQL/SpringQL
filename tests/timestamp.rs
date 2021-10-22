use chrono::NaiveDateTime;
use springql_core::timestamp::Timestamp;

#[test]
fn test_timestamp_ser_de() -> Result<(), Box<dyn std::error::Error>> {
    const FORMAT: &str = "%Y-%m-%d %H:%M:%S%.9f";

    let ts: Vec<Timestamp> = vec![
        NaiveDateTime::parse_from_str("2021-10-22 14:00:14.000000000", FORMAT)?,
        NaiveDateTime::parse_from_str("2021-10-22 14:00:14.000000009", FORMAT)?,
    ]
    .into_iter()
    .map(Timestamp::new)
    .collect();

    for t in ts {
        let ser = serde_json::to_string(&t)?;
        let de: Timestamp = serde_json::from_str(&ser)?;
        assert_eq!(de, t);
    }

    Ok(())
}
