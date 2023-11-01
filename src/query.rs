use crate::database::Database;

#[derive(Debug)]
pub enum Query {
    Select(SelectQuery),
    Create(CreateQuery),
}

#[derive(Debug)]
pub struct SelectQuery {
    pub table_name: String,
    pub columns: Vec<Column>,
}

#[derive(Debug)]
pub enum Column {
    Count,
    ColumnName(String),
}

impl Column {
    pub fn as_name(&self) -> Option<&str> {
        match self {
            Column::ColumnName(s) => Some(s),
            _ => None,
        }
    }
}

#[derive(Debug)]
pub struct CreateQuery {
    pub column_names: Vec<String>,
}

impl Query {
    pub fn parse(query_str: &str) -> anyhow::Result<Self> {
        if query_str.to_ascii_lowercase().starts_with("select") {
            let mut parts = query_str.split_whitespace();
            assert_eq!(
                parts.next().map(|s| s.to_ascii_lowercase()),
                Some("select".into())
            );

            let mut columns = Vec::new();
            for next_token in parts.by_ref() {
                if next_token == "," || next_token.to_ascii_lowercase() == "from" {
                    break;
                } else if next_token.to_ascii_lowercase().contains("count") {
                    columns.push(Column::Count);
                } else {
                    columns.push(Column::ColumnName(next_token.to_owned()))
                }
            }

            let table_name = parts.next().unwrap().to_ascii_lowercase();

            Ok(Query::Select(SelectQuery {
                table_name,
                columns,
            }))
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

    pub fn execute(&self, db: &Database, db_data: &[u8]) -> anyhow::Result<Vec<Vec<String>>> {
        match self {
            Query::Select(select) => {
                let table_root_page = db.schema.table_root_page(&select.table_name)?;
                let page = db.parse_page(&db_data, table_root_page)?;

                if select.columns.len() == 1 && matches!(select.columns[0], Column::Count) {
                    Ok(vec![vec![page.records.len().to_string()]])
                } else {
                    let mut results = Vec::new();
                    for record in page.records.iter() {
                        let mut row = Vec::new();
                        for column in select.columns.iter() {
                            let column_name = column.as_name().unwrap();
                            let value =
                                record.values.get(column_name).expect("invalid column name");
                            row.push(value.to_string());
                        }
                        results.push(row);
                    }

                    Ok(results)
                }
            }
            _ => todo!(),
        }
    }
}