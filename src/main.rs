use anyhow::{bail, Result};

mod database;
mod header;
mod page;
mod record;
mod varint;

fn main() -> Result<()> {
    // Parse arguments
    let args = std::env::args().collect::<Vec<_>>();
    match args.len() {
        0 | 1 => bail!("Missing <database path> and <command>"),
        2 => bail!("Missing <command>"),
        _ => {}
    }

    // Parse command and act accordingly
    let command = &args[2];
    match command.as_str() {
        ".dbinfo" => {
            let db_data = std::fs::read(&args[1])?;
            let db = database::Database::parse(&db_data)?;

            println!("database page size: {}", db.header.page_size);
            println!("number of tables: {}", db.pages[0].records.len());
        }
        _ => bail!("Missing or invalid command passed: {}", command),
    }

    Ok(())
}
