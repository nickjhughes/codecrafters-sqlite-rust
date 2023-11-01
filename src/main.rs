use anyhow::{bail, Result};
use query::Query;

mod database;
mod header;
mod page;
mod query;
mod record;
mod varint;

fn main() -> Result<()> {
    let args = std::env::args().collect::<Vec<_>>();
    match args.len() {
        0 | 1 => bail!("Missing <database path> and <command>"),
        2 => bail!("Missing <command>"),
        _ => {}
    }

    let db_data = std::fs::read(&args[1])?;
    let db = database::Database::parse(&db_data)?;

    let command = &args[2];
    match command.as_str() {
        ".dbinfo" => {
            println!("database page size: {}", db.header.page_size);
            println!("number of tables: {}", db.schema.table_count());
        }
        ".tables" => {
            let tables = db.schema.table_names();
            let tables_string = tables.join(" ");
            println!("{}", tables_string);
        }
        query_str => {
            let query = Query::parse(query_str)?;
            let results = query.execute(&db, &db_data)?;
            for row in results.iter() {
                println!("{}", row.join("|"));
            }
        }
    }

    Ok(())
}
