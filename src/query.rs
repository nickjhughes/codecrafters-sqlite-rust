#[derive(Debug)]
pub enum Query {
    Select(SelectQuery),
    Create(CreateQuery),
}

#[derive(Debug)]
pub struct SelectQuery {
    pub table_name: String,
}

#[derive(Debug)]
pub struct CreateQuery {
    pub column_names: Vec<String>,
}

impl Query {
    pub fn parse(query_str: &str) -> anyhow::Result<Self> {
        if query_str.to_ascii_lowercase().starts_with("select") {
            let parts = query_str.split(' ');
            let table_name = parts.last().expect("empty query").to_ascii_lowercase();
            Ok(Query::Select(SelectQuery { table_name }))
        } else if query_str.to_ascii_lowercase().starts_with("create") {
            let (_, columns_info) = query_str.split_once('(').unwrap();
            let columns_info = columns_info.strip_suffix(')').unwrap();
            let columns = columns_info.split(',');

            let mut column_names = Vec::new();
            for column_info in columns {
                let column_name = column_info.split_whitespace().next().unwrap();
                column_names.push(column_name.to_owned());
            }

            Ok(Query::Create(CreateQuery { column_names }))
        } else {
            Err(anyhow::format_err!("unsupported or invalid query type"))
        }
    }

    pub fn as_create(&self) -> Option<&CreateQuery> {
        match self {
            Query::Create(create) => Some(create),
            _ => None,
        }
    }
}
