#[derive(Debug)]
pub enum Query {
    Select(SelectQuery),
}

#[derive(Debug)]
pub struct SelectQuery {
    pub table: String,
}

impl Query {
    pub fn parse(query_str: &str) -> anyhow::Result<Self> {
        if query_str.to_ascii_lowercase().starts_with("select") {
            let parts = query_str.split(' ');
            let table = parts.last().expect("empty query").to_ascii_lowercase();
            Ok(Query::Select(SelectQuery { table }))
        } else {
            Err(anyhow::format_err!("unsupported or invalid query type"))
        }
    }
}
