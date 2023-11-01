use nom::{
    bytes::complete::take,
    multi::count,
    number::complete::{be_u16, be_u32, u8},
    IResult,
};

use super::record::Record;

pub struct Page {
    pub ty: PageType,
    pub records: Vec<Record>,
}

#[allow(dead_code)]
#[derive(Debug)]
pub enum PageType {
    /// The lock-byte page.
    LockByte,
    /// A freelist page.
    Freelist(FreelistPageType),
    /// A b-tree page.
    BTree(BTreePageType),
    /// A payload overflow page.
    PayloadOverflow,
    /// A pointer map page.
    PointerMap,
}

#[allow(dead_code)]
#[derive(Debug)]
pub enum FreelistPageType {
    /// A freelist trunk page.
    Trunk,
    /// A freelist leaf page.
    Leaf,
}

#[derive(Debug)]
pub enum BTreePageType {
    /// A table b-tree interior page.
    TableInterior,
    /// A table b-tree leaf page.
    TableLeaf,
    /// An index b-tree interior page.
    IndexInterior,
    /// An index b-tree leaf page.
    IndexLeaf,
}

impl Page {
    pub fn parse(input: &[u8], is_first_page: bool) -> IResult<&[u8], Self> {
        let (input, page_type) = u8(input)?;
        let page_type = PageType::try_from(page_type).expect("invalid page type");

        let (input, records) = match &page_type {
            PageType::BTree(b_tree_page_type) => {
                // Header
                let (input, _first_freelock) = be_u16(input)?;
                let (input, cell_count) = be_u16(input)?;

                let (input, cell_content_offset) = be_u16(input)?;
                let cell_content_offset = if cell_content_offset == 0 {
                    65536
                } else {
                    cell_content_offset as usize
                };

                let (input, _num_fragmented_free_bytes) = u8(input)?;

                let (input, _rightmost_pointer) = if matches!(
                    b_tree_page_type,
                    BTreePageType::IndexInterior | BTreePageType::TableInterior
                ) {
                    let (input, rightmost_pointer) = be_u32(input)?;
                    (input, Some(rightmost_pointer))
                } else {
                    (input, None)
                };

                // Cell pointer array
                let (input, _cell_pointers) = count(be_u16, cell_count as usize)(input)?;

                // Skip over unallocated space
                let bytes_read = if is_first_page { 100 } else { 0 }
                    + if matches!(
                        b_tree_page_type,
                        BTreePageType::IndexInterior | BTreePageType::TableInterior
                    ) {
                        12
                    } else {
                        8
                    }
                    + cell_count as usize * 2;
                let (input, _) = take(cell_content_offset - bytes_read)(input)?;

                // Read records
                let column_names = if is_first_page {
                    vec![
                        "type".to_string(),
                        "name".to_string(),
                        "tbl_name".to_string(),
                        "rootpage".to_string(),
                        "sql".to_string(),
                    ]
                } else {
                    todo!()
                };
                let (input, records) = count(
                    |input| Record::parse(input, &column_names),
                    cell_count as usize,
                )(input)?;

                (input, records)
            }
            _ => todo!(),
        };

        Ok((
            input,
            Page {
                ty: page_type,
                records,
            },
        ))
    }
}

impl TryFrom<u8> for PageType {
    type Error = anyhow::Error;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            2 => Ok(PageType::BTree(BTreePageType::IndexInterior)),
            5 => Ok(PageType::BTree(BTreePageType::TableInterior)),
            10 => Ok(PageType::BTree(BTreePageType::IndexLeaf)),
            13 => Ok(PageType::BTree(BTreePageType::TableLeaf)),
            _ => Err(anyhow::format_err!("unknown page type {}", value)),
        }
    }
}
