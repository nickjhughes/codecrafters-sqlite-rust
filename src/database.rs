// use nom::multi::count;

use super::{header::Header, page::Page};

pub struct Database {
    pub header: Header,
    pub pages: Vec<Page>,
}

impl Database {
    pub fn parse(input: &[u8]) -> anyhow::Result<Self> {
        let (input, header) = Header::parse(input).expect("failed to parse header");
        let (_input, page1) = Page::parse(input, true).unwrap();
        // let (_, mut pages) =
        //     count(|input| Page::parse(input, false), header.size_in_pages - 1)(input).unwrap();
        // pages.insert(0, page1);
        let pages = vec![page1];

        Ok(Database { header, pages })
    }
}
