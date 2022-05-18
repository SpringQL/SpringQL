use std::path::PathBuf;

use anyhow::Result;
use run_script::ScriptOptions;

fn main() -> Result<()> {
    let mut options = ScriptOptions::new();
    options.working_directory = Some(PathBuf::from(std::env::var("CARGO_MANIFEST_DIR")?));
    let args = vec![];
    let (code, output, error) = run_script::run(
        r#"
         WORKSPACE_MANIFEST=$(cargo locate-project --workspace | jq -r '.root')
         WORKSPACE_DIR=$(dirname ${WORKSPACE_MANIFEST})
         cd ${WORKSPACE_DIR}
         cargo deny check
         "#,
        &args,
        &options,
    )
    .unwrap();
    if code != 0 {
        eprintln!("==== STDOUT: cargo deny check ");
        eprintln!("{0}", output);
        eprintln!("==== STDERR: cargo deny check ");
        eprintln!("{0}", error);
        panic!("`cargo deny check` failed code={0}", code);
    }
    Ok(())
}
