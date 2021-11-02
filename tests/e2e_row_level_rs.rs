use regex::Regex;
use springql_core::error::Result;
use springql_core::low_level_rs::*;

#[ignore]
#[test]
fn test_e2e_source_sink() -> Result<()> {
    let mut pipeline = spring_open()?;

    let ddls = vec![
        "
        CREATE FOREIGN STREAM fst_trade_input (
          timestamp TIMESTAMP NOT NULL ROWTIME,    
          ticker TEXT NOT NULL,
          amount INTEGER NOT NULL
        ) SERVER NET_SERVER OPTIONS (
          REMOTE_PORT '17890'
        );
        ",
        "
        CREATE FOREIGN STREAM fst_trade_output (
          timestamp TIMESTAMP NOT NULL,    
          ticker TEXT NOT NULL,
          amount INTEGER NOT NULL
        ) SERVER IN_MEMORY_QUEUE;
        ",
        "
        CREATE PUMP pu_in_to_out AS
          INSERT INTO fst_trade_output (timestamp, ticker, amount)
          SELECT STREAM timestamp, ticker, amount FROM fst_trade_input;
        ",
    ];
    for ddl in ddls {
        let mut stmt = spring_prepare(&mut pipeline, ddl)?;
        assert_eq!(spring_step(&mut stmt)?, SpringStepSuccess::Done);
    }

    let mut stmt = spring_prepare(
        &mut pipeline,
        "SELECT timestamp, ticker, amount FROM fst_trade_output;",
    )?;

    let re_timestamp = Regex::new(r"^\d{4}-\d{2}-\d{2} \d{2}-\d{2}-\d{2}\.\d{9}").unwrap();
    let re_ticker = Regex::new(r"^[A-Z0-9]{2,5}").unwrap();

    for _ in 0..1000 {
        assert_eq!(spring_step(&mut stmt)?, SpringStepSuccess::Row);

        let timestamp = spring_column_text(&mut stmt, 0)?;
        assert!(re_timestamp.is_match(&timestamp));

        let ticker = spring_column_text(&mut stmt, 1)?;
        assert!(re_ticker.is_match(&ticker));

        let amount = spring_column_i32(&mut stmt, 2)?;
        assert!(amount > 1);
    }

    Ok(())
}
