use nom::{
    bytes::complete::take,
    multi::count,
    number::complete::{be_u16, be_u32, u8},
    IResult,
};

use super::cell::Cell;

pub struct Page {
    pub ty: PageType,
    pub cells: Vec<Cell>,
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

#[derive(Debug, Clone, Copy)]
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

impl TryFrom<u8> for PageType {
    type Error = anyhow::Error;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0x02 => Ok(PageType::BTree(BTreePageType::IndexInterior)),
            0x05 => Ok(PageType::BTree(BTreePageType::TableInterior)),
            0x0a => Ok(PageType::BTree(BTreePageType::IndexLeaf)),
            0x0d => Ok(PageType::BTree(BTreePageType::TableLeaf)),
            _ => Err(anyhow::format_err!("unknown page type {}", value)),
        }
    }
}

impl Page {
    pub fn parse<'input>(
        input: &'input [u8],
        is_first_page: bool,
        column_names: &[String],
        usable_page_size: usize,
    ) -> IResult<&'input [u8], Self> {
        let (input, page_type) = u8(input)?;
        let page_type = PageType::try_from(page_type).expect("invalid page type");

        let (input, cells) = match &page_type {
            PageType::BTree(b_tree_page_type) => {
                // Header
                let (input, _first_freelock) = be_u16(input)?;
                let (input, cell_count) = be_u16(input)?;

                let (input, cell_content_offset) = be_u16(input)?;
                let _cell_content_offset = if cell_content_offset == 0 {
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
                let (input, cell_pointers) = count(be_u16, cell_count as usize)(input)?;

                // Calculate our current position in the page
                let mut position = if is_first_page { 100 } else { 0 }
                    + if matches!(
                        b_tree_page_type,
                        BTreePageType::IndexInterior | BTreePageType::TableInterior
                    ) {
                        12
                    } else {
                        8
                    }
                    + cell_count as usize * 2;

                // dbg!(
                //     _first_freelock,
                //     cell_count,
                //     cell_content_offset,
                //     _num_fragmented_free_bytes,
                //     _rightmost_pointer,
                //     &cell_pointers
                // );

                // Read cells
                let mut rest = input;
                let mut cells = Vec::with_capacity(cell_count as usize);
                for cell_offset in cell_pointers.iter().rev() {
                    let (remainder, _) = take(*cell_offset as usize - position)(rest)?;
                    position = *cell_offset as usize;
                    rest = remainder;
                    let (remainder, cell) =
                        Cell::parse(rest, *b_tree_page_type, usable_page_size, column_names)?;
                    cells.push(cell);
                    let cell_size = rest.len() - remainder.len();
                    rest = remainder;
                    position += cell_size;
                }

                (input, cells)
            }
            _ => todo!(),
        };

        Ok((
            input,
            Page {
                ty: page_type,
                cells,
            },
        ))
    }
}
