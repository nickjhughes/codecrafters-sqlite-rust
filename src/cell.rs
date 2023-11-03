use nom::{number::complete::be_u32, IResult};

use crate::{page::BTreePageType, record::Record, varint::varint};

#[allow(dead_code)]
#[derive(Debug)]
pub enum Cell {
    TableLeaf(Record),
    TableInterior { left_child_pointer: u32, key: i64 },
    IndexLeaf,
    IndexInterior,
}

impl Cell {
    pub fn parse<'input>(
        input: &'input [u8],
        ty: BTreePageType,
        usable_page_size: usize,
        column_names: &[String],
    ) -> IResult<&'input [u8], Self> {
        match ty {
            BTreePageType::TableInterior => {
                let (input, left_child_pointer) = be_u32(input)?;
                let (input, key) = varint(input)?;
                Ok((
                    input,
                    Cell::TableInterior {
                        left_child_pointer,
                        key,
                    },
                ))
            }
            BTreePageType::TableLeaf => {
                let (input, payload_size) = varint(input)?;
                let payload_size = payload_size as usize;

                let maximum_non_overflow_payload_size = usable_page_size - 35;
                if payload_size <= maximum_non_overflow_payload_size {
                    // The entire payload is stored on the b-tree leaf page
                } else {
                    let minimum_before_overflow = ((usable_page_size - 12) * 32 / 255) - 23;
                    let k = minimum_before_overflow
                        + ((payload_size - minimum_before_overflow) % (usable_page_size - 4));
                    if payload_size > k && k <= maximum_non_overflow_payload_size {
                        // The first `k` bytes of the payload are stored on the btree page,
                        // and the remaining `payload_size - k` bytes are stored on overflow pages.
                        // println!("Overflow of size {}", payload_size - k);
                        todo!("cell overflow");
                    } else {
                        // The first `minimum_before_overflow` bytes of the payload are stored on the
                        // btree page and the remaining `payload_size - minimum_before_overflow` bytes
                        // are stored on overflow pages.
                        // println!(
                        //     "Overflow of size {}",
                        //     payload_size - minimum_before_overflow
                        // );
                        todo!("cell overflow");
                    }
                }

                let (input, record) = Record::parse(input, column_names)?;

                Ok((input, Cell::TableLeaf(record)))
            }
            BTreePageType::IndexInterior => todo!(),
            BTreePageType::IndexLeaf => todo!(),
        }
    }

    pub fn as_record(&self) -> Option<&Record> {
        match self {
            Cell::TableLeaf(record) => Some(record),
            _ => None,
        }
    }
}
