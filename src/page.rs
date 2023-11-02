use nom::{
    bytes::complete::take,
    multi::count,
    number::complete::{be_u16, be_u32, u8},
    IResult,
};

use super::{record::Record, varint::varint};

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
        dbg!(&page_type);

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
                let (input, cell_pointers) = count(be_u16, cell_count as usize)(input)?;

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

                // dbg!(
                //     _first_freelock,
                //     cell_count,
                //     cell_content_offset,
                //     _num_fragmented_free_bytes,
                //     _rightmost_pointer,
                //     cell_pointers
                // );

                // TODO: Should actually read each cell following `cell_pointers`, not consecutively

                // Read cells
                let (input, records) = count(
                    |input| Record::parse(input, column_names),
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

#[derive(Debug)]
pub struct Cell {}

impl Cell {
    pub fn parse(input: &[u8], ty: BTreePageType, usable_page_size: usize) -> IResult<&[u8], Self> {
        match ty {
            BTreePageType::TableInterior => {
                let (input, left_child_pointer) = be_u32(input)?;
                let (input, key) = varint(input)?;
            }
            BTreePageType::TableLeaf => {
                let (input, payload_size) = varint(input)?;
                let payload_size = payload_size as usize;

                let maximum_non_overflow_payload_size = usable_page_size - 35;
                if payload_size <= maximum_non_overflow_payload_size {
                    // The entire payload is stored on the b-tree leaf page
                    println!("No overflow");
                } else {
                    let minimum_before_overflow = ((usable_page_size - 12) * 32 / 255) - 23;
                    let k = minimum_before_overflow
                        + ((payload_size - minimum_before_overflow) % (usable_page_size - 4));
                    if payload_size > k && k <= maximum_non_overflow_payload_size {
                        // The first `k` bytes of the payload are stored on the btree page,
                        // and the remaining `payload_size - k` bytes are stored on overflow pages.
                        println!("Overflow of size {}", payload_size - k);
                    } else {
                        // The first `minimum_before_overflow` bytes of the payload are stored on the
                        // btree page and the remaining `payload_size - minimum_before_overflow` bytes
                        // are stored on overflow pages.
                        println!(
                            "Overflow of size {}",
                            payload_size - minimum_before_overflow
                        );
                    }
                }

                // Record::parse(input, column_names);

                // let (input, row_id) = VarInt::parse(input)?;
            }
            BTreePageType::IndexInterior => todo!(),
            BTreePageType::IndexLeaf => todo!(),
        };

        todo!()
    }
}
