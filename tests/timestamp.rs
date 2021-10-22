use springql_core::{error::Result, timestamp::Timestamp};

#[test]
fn test_timestamp_ser_de() -> Result<()> {
    let ts = vec![
        "2021-10-22 14:00:14.000000000",
        "2021-10-22 14:00:14.000000009",
    ]
    .into_iter()
    .map(|s| s.parse())
    .collect::<Result<Vec<_>>>()?;

    for t in ts {
        let ser = serde_json::to_string(&t).unwrap();
        let de: Timestamp = serde_json::from_str(&ser).unwrap();
        assert_eq!(de, t);
    }

    Ok(())
}
