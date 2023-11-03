use crate::{
    cell::Cell,
    header::{Header, HEADER_SIZE},
    page::Page,
    query::Query,
    record::{Record, Value},
};

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
    Index(IndexSchema),
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

    pub fn as_index(&self) -> Option<&IndexSchema> {
        match self {
            ObjectSchema::Index(index) => Some(index),
            _ => None,
        }
    }
}

#[derive(Debug)]
pub struct TableSchema {
    pub name: String,
    pub root_page: usize,
    pub sql: String,
    pub column_names: Vec<String>,
}

#[derive(Debug)]
pub struct IndexSchema {
    pub name: String,
    pub table_name: String,
    pub root_page: usize,
    pub sql: String,
    pub column_name: String,
}

impl Database {
    /// Parse the first page of the database file, containing the header and a schema.
    pub fn parse_header_and_schema(input: &[u8]) -> anyhow::Result<Self> {
        let (rest, header) = Header::parse(input).expect("failed to parse header");
        let first_page_data = &rest[0..(header.page_size - HEADER_SIZE)];
        let (_, first_page) = Page::parse(
            first_page_data,
            true,
            &[
                "type".to_string(),
                "name".to_string(),
                "tbl_name".to_string(),
                "rootpage".to_string(),
                "sql".to_string(),
            ],
            header.page_size - header.end_page_reserved_bytes,
        )
        .expect("failed to parse first page");

        let mut objects = Vec::new();
        for object_cell in first_page.cells.iter() {
            let object_record = object_cell.as_record().unwrap();
            let object = match object_record.values.get("type").unwrap().as_text().unwrap() {
                "table" => {
                    let create_query_str =
                        object_record.values.get("sql").unwrap().as_text().unwrap();
                    let create_query = Query::parse(create_query_str)?;
                    let column_names = create_query.as_create().unwrap().column_names.clone();

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
                        sql: create_query_str.to_owned(),
                        column_names,
                    })
                }
                "index" => {
                    let create_query_str =
                        object_record.values.get("sql").unwrap().as_text().unwrap();
                    let create_query = Query::parse(create_query_str)?;
                    let column_name = create_query.as_create().unwrap().column_names[0].clone();

                    ObjectSchema::Index(IndexSchema {
                        name: object_record
                            .values
                            .get("name")
                            .unwrap()
                            .as_text()
                            .unwrap()
                            .to_owned(),
                        table_name: object_record
                            .values
                            .get("tbl_name")
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
                        sql: create_query_str.to_owned(),
                        column_name,
                    })
                }
                _ => todo!("non-table/index object"),
            };

            objects.push(object);
        }

        Ok(Database {
            header,
            schema: Schema { objects },
        })
    }

    pub fn get_full_table(&self, input: &[u8], page_index: usize) -> anyhow::Result<Vec<Record>> {
        assert!(page_index > 1);

        let column_names = self
            .schema
            .objects
            .iter()
            .find(|o| {
                matches!(o, ObjectSchema::Table(_)) && o.as_table().unwrap().root_page == page_index
            })
            .map(|o| o.as_table().unwrap().column_names.clone())
            .unwrap();

        let mut records: Vec<Record> = Vec::new();
        let mut pages_to_read: Vec<usize> = vec![page_index];
        while let Some(page_index) = pages_to_read.pop() {
            let page_input = &input[self.header.page_size * (page_index - 1)
                ..self.header.page_size * (page_index - 1) + self.header.page_size];
            let page = Page::parse(
                page_input,
                false,
                &column_names,
                self.header.page_size - self.header.end_page_reserved_bytes,
            )
            .expect("failed to parse page")
            .1;

            if let Some(rightmost_pointer) = page.rightmost_pointer {
                pages_to_read.push(rightmost_pointer);
            }

            for cell in page.cells {
                match cell {
                    Cell::TableLeaf(record) => records.push(record),
                    Cell::TableInterior {
                        left_child_pointer, ..
                    } => pages_to_read.push(left_child_pointer as usize),
                    _ => unreachable!(),
                }
            }
        }

        Ok(records)
    }

    pub fn search_index(
        &self,
        input: &[u8],
        page_index: usize,
        key_name: String,
        key: Value,
    ) -> anyhow::Result<Vec<i64>> {
        assert!(page_index > 1);

        let column_name = self
            .schema
            .objects
            .iter()
            .find(|o| {
                matches!(o, ObjectSchema::Index(_)) && o.as_index().unwrap().root_page == page_index
            })
            .map(|o| o.as_index().unwrap().column_name.clone())
            .unwrap();

        let mut row_ids = Vec::new();
        let mut pages_to_read: Vec<usize> = vec![page_index];
        while let Some(page_index) = pages_to_read.pop() {
            let page_input = &input[self.header.page_size * (page_index - 1)
                ..self.header.page_size * (page_index - 1) + self.header.page_size];
            let page = Page::parse(
                page_input,
                false,
                &[column_name.clone(), "row_id".into()],
                self.header.page_size - self.header.end_page_reserved_bytes,
            )
            .expect("failed to parse page")
            .1;

            // TODO: Only parse `rightmost_pointer` and `left_child_pointer` pages
            //       if the entries we're interested in will appear there

            if let Some(rightmost_pointer) = page.rightmost_pointer {
                pages_to_read.push(rightmost_pointer);
            }

            for cell in page.cells {
                match cell {
                    Cell::IndexLeaf(record) => {
                        if *record.values.get(&key_name).unwrap() == key {
                            row_ids
                                .push(record.values.get("row_id").unwrap().as_integer().unwrap());
                        }
                    }
                    Cell::IndexInterior {
                        left_child_pointer,
                        record,
                    } => {
                        pages_to_read.push(left_child_pointer as usize);
                        if *record.values.get(&key_name).unwrap() == key {
                            row_ids
                                .push(record.values.get("row_id").unwrap().as_integer().unwrap());
                        }
                    }
                    _ => unreachable!(),
                }
            }
        }

        Ok(row_ids)
    }

    pub fn get_by_row_ids(
        &self,
        input: &[u8],
        page_index: usize,
        row_ids: &[i64],
    ) -> anyhow::Result<Vec<Record>> {
        assert!(page_index > 1);

        let column_names = self
            .schema
            .objects
            .iter()
            .find(|o| {
                matches!(o, ObjectSchema::Table(_)) && o.as_table().unwrap().root_page == page_index
            })
            .map(|o| o.as_table().unwrap().column_names.clone())
            .unwrap();

        let mut records: Vec<Record> = Vec::new();
        let mut pages_to_read: Vec<usize> = vec![page_index];
        while let Some(page_index) = pages_to_read.pop() {
            let page_input = &input[self.header.page_size * (page_index - 1)
                ..self.header.page_size * (page_index - 1) + self.header.page_size];
            let page = Page::parse(
                page_input,
                false,
                &column_names,
                self.header.page_size - self.header.end_page_reserved_bytes,
            )
            .expect("failed to parse page")
            .1;

            // TODO: When to follow rightmost pointer?
            if let Some(rightmost_pointer) = page.rightmost_pointer {
                pages_to_read.push(rightmost_pointer);
            }

            for cell in page.cells {
                match cell {
                    Cell::TableLeaf(record) => {
                        if row_ids.contains(&record.values.get("id").unwrap().as_integer().unwrap())
                        {
                            records.push(record)
                        }
                    }
                    Cell::TableInterior {
                        left_child_pointer,
                        key,
                    } => {
                        if row_ids.iter().any(|id| *id < key) {
                            pages_to_read.push(left_child_pointer as usize)
                        }
                    }
                    _ => unreachable!(),
                }
            }
        }

        Ok(records)
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
