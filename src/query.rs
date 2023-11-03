use crate::{database::Database, record::Value};

#[derive(Debug)]
pub enum Query {
    Select(SelectQuery),
    Create(CreateQuery),
}

#[derive(Debug)]
pub struct SelectQuery {
    pub table_name: String,
    pub columns: Vec<Column>,
    pub filters: Vec<Filter>,
}

#[derive(Debug)]
pub enum Column {
    Count,
    ColumnName(String),
}

#[derive(Debug)]
pub struct Filter {
    pub column_name: String,
    pub column_value: Value,
}

impl Column {
    #[allow(dead_code)]
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
                    let column_name = next_token.trim_start_matches(',').trim_end_matches(',');
                    columns.push(Column::ColumnName(column_name.to_owned()))
                }
            }

            let table_name = parts.next().unwrap().to_ascii_lowercase();

            let mut filters = Vec::new();
            if parts.next().map(|s| s.to_ascii_lowercase()) == Some("where".into()) {
                let column_name = parts.next().unwrap().to_ascii_lowercase();
                assert_eq!(parts.next(), Some("="));
                let column_value = {
                    let value = parts.next().unwrap();
                    if value.starts_with('\'') {
                        // Interpret as text
                        Value::Text(value.trim_matches('\'').to_owned())
                    } else {
                        // Interpret as number
                        todo!()
                    }
                };
                filters.push(Filter {
                    column_name,
                    column_value,
                });
            }

            Ok(Query::Select(SelectQuery {
                table_name,
                columns,
                filters,
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
                let page = db.parse_page(db_data, table_root_page)?;

                let mut results = Vec::new();
                for cell in page.cells.iter() {
                    let mut filtered_out = false;
                    for filter in select.filters.iter() {
                        let value = cell
                            .as_record()
                            .unwrap()
                            .values
                            .get(&filter.column_name)
                            .expect("invalid column name");
                        if *value != filter.column_value {
                            filtered_out = true;
                            break;
                        }
                    }
                    if filtered_out {
                        continue;
                    }

                    let mut row = Vec::new();
                    for column in select.columns.iter() {
                        match column {
                            Column::ColumnName(column_name) => {
                                let value = cell
                                    .as_record()
                                    .unwrap()
                                    .values
                                    .get(column_name)
                                    .expect("invalid column name");
                                row.push(value.to_string());
                            }
                            _ => {}
                        }
                    }
                    results.push(row);
                }

                if matches!(select.columns[0], Column::Count) {
                    let result_count = results.len();
                    results.clear();
                    results.push(vec![result_count.to_string()]);
                }

                Ok(results)
            }
            _ => todo!(),
        }
    }
}
