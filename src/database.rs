use super::header::Header;

pub struct Database {
    pub header: Header,
}

impl Database {
    pub fn parse(input: &[u8]) -> anyhow::Result<Self> {
        let (_, header) = Header::parse(input).expect("failed to parse header");

        Ok(Database { header })
    }
}
