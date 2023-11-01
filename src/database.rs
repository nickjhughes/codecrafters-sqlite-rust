use super::{header::Header, page::Page};

pub struct Database {
    pub header: Header,
    pub schema: Schema,
}

#[derive(Debug)]
pub struct Schema {
    pub objects: Vec<ObjectSchema>,
}

#[allow(dead_code)]
#[derive(Debug)]
pub enum ObjectSchema {
    Table(TableSchema),
    Index,
    View,
    Trigger,
}

impl ObjectSchema {
    pub fn as_table(&self) -> Option<&TableSchema> {
        match self {
            ObjectSchema::Table(table) => Some(table),
            _ => None,
        }
    }
}

#[derive(Debug)]
pub struct TableSchema {
    pub name: String,
    pub root_page: usize,
    pub sql: String,
}

impl Database {
    pub fn parse(input: &[u8]) -> anyhow::Result<Self> {
        let (rest, header) = Header::parse(input).expect("failed to parse header");
        let (_, first_page) = Page::parse(rest, true).expect("failed to parse first page");

        let mut objects = Vec::new();
        for object_record in first_page.records.iter() {
            let object = match object_record.values.get("type").unwrap().as_text().unwrap() {
                "table" => {
                    let table_create_query =
                        object_record.values.get("sql").unwrap().as_text().unwrap();
                    dbg!(table_create_query);
                    // TODO: Parse table_create_query to get column names

                    ObjectSchema::Table(TableSchema {
                        name: object_record
                            .values
                            .get("name")
                            .unwrap()
                            .as_text()
                            .unwrap()
                            .to_owned(),
                        root_page: object_record
                            .values
                            .get("rootpage")
                            .unwrap()
                            .as_integer()
                            .unwrap() as usize,
                        sql: object_record
                            .values
                            .get("sql")
                            .unwrap()
                            .as_text()
                            .unwrap()
                            .to_owned(),
                    })
                }
                _ => todo!("non-table object"),
            };

            objects.push(object);
        }

        Ok(Database {
            header,
            schema: Schema { objects },
        })
    }

    pub fn parse_page(&self, input: &[u8], page_index: usize) -> anyhow::Result<Page> {
        assert!(page_index > 1);

        let page_input = &input[self.header.page_size as usize * (page_index - 1)
            ..self.header.page_size as usize * (page_index - 1) + self.header.page_size as usize];

        Ok(Page::parse(page_input, false)
            .expect("failed to parse page")
            .1)
    }
}

impl Schema {
    pub fn table_count(&self) -> usize {
        self.objects
            .iter()
            .filter(|o| matches!(o, ObjectSchema::Table(_)))
            .count()
    }

    pub fn table_names(&self) -> Vec<&str> {
        let mut tables = self
            .objects
            .iter()
            .filter(|o| matches!(o, ObjectSchema::Table(_)))
            .map(|o| o.as_table().unwrap().name.as_str())
            .collect::<Vec<&str>>();
        tables.sort();
        tables
    }

    pub fn table_root_page(&self, table_name: &str) -> anyhow::Result<usize> {
        Ok(self
            .objects
            .iter()
            .find(|o| {
                matches!(o, ObjectSchema::Table(_)) && o.as_table().unwrap().name == table_name
            })
            .expect("table not found")
            .as_table()
            .unwrap()
            .root_page)
    }
}
