use nom::{bytes::complete::take, number::complete::i8, IResult};
use std::collections::HashMap;

use crate::varint::VarInt;

#[derive(Debug)]
pub struct Record {
    pub values: HashMap<String, Value>,
}

#[derive(Debug)]
pub enum ColumnType {
    Null,
    I8,
    I16,
    I24,
    I32,
    I48,
    I64,
    F64,
    Zero,
    One,
    Blob(usize),
    Text(usize),
}

#[allow(dead_code)]
#[derive(Debug)]
pub enum Value {
    Null,
    I8(i8),
    I16(i16),
    I24(i32),
    I32(i32),
    I48(i64),
    I64(i64),
    F64(f64),
    Zero,
    One,
    Blob(String),
    Text(String),
}

impl Value {
    pub fn as_text(&self) -> Option<&str> {
        match self {
            Value::Text(s) => Some(s),
            _ => None,
        }
    }
}

impl ColumnType {
    #[allow(dead_code)]
    fn size(&self) -> usize {
        match self {
            ColumnType::Null => 0,
            ColumnType::I8 => 1,
            ColumnType::I16 => 2,
            ColumnType::I24 => 3,
            ColumnType::I32 => 4,
            ColumnType::I48 => 6,
            ColumnType::I64 => 8,
            ColumnType::F64 => 8,
            ColumnType::Zero => 0,
            ColumnType::One => 0,
            ColumnType::Blob(size) | ColumnType::Text(size) => *size,
        }
    }
}

impl TryFrom<VarInt> for ColumnType {
    type Error = anyhow::Error;

    fn try_from(value: VarInt) -> Result<Self, Self::Error> {
        match value {
            VarInt::I8(v) => match v {
                0 => Ok(ColumnType::Null),
                1 => Ok(ColumnType::I8),
                2 => Ok(ColumnType::I16),
                3 => Ok(ColumnType::I24),
                4 => Ok(ColumnType::I32),
                5 => Ok(ColumnType::I48),
                6 => Ok(ColumnType::I64),
                7 => Ok(ColumnType::F64),
                8 => Ok(ColumnType::Zero),
                9 => Ok(ColumnType::One),
                10 | 11 => Err(anyhow::format_err!("invalid column type")),
                _ => {
                    if v % 2 == 0 {
                        Ok(ColumnType::Blob(((v - 12) / 2) as usize))
                    } else {
                        Ok(ColumnType::Text(((v - 13) / 2) as usize))
                    }
                }
            },
            VarInt::I16(v) => {
                if v % 2 == 0 {
                    Ok(ColumnType::Blob(((v - 12) / 2) as usize))
                } else {
                    Ok(ColumnType::Text(((v - 13) / 2) as usize))
                }
            }
            VarInt::I32(v) => {
                if v % 2 == 0 {
                    Ok(ColumnType::Blob(((v - 12) / 2) as usize))
                } else {
                    Ok(ColumnType::Text(((v - 13) / 2) as usize))
                }
            }
            VarInt::I64(v) => {
                if v % 2 == 0 {
                    Ok(ColumnType::Blob(((v - 12) / 2) as usize))
                } else {
                    Ok(ColumnType::Text(((v - 13) / 2) as usize))
                }
            }
        }
    }
}

impl Record {
    pub fn parse(input: &[u8]) -> IResult<&[u8], Self> {
        let (input, _record_size) = VarInt::parse(input)?;
        let (input, _row_id) = VarInt::parse(input)?;
        // dbg!(&record_size, &row_id);

        // Skip overflow info
        let (input, _) = take(1usize)(input)?;

        // `sqlite_schema` table has 5 columns
        let mut rest = input;
        let mut column_types = Vec::new();
        for _ in 0..5 {
            let (remainder, column_type) = VarInt::parse(rest)?;
            rest = remainder;
            let column_type = ColumnType::try_from(column_type).expect("invalid column type");
            column_types.push(column_type);
        }
        let column_names = vec!["type", "name", "tbl_name", "rootpage", "sql"];
        // dbg!(&column_types);

        let mut values = HashMap::new();
        for (column_name, column_type) in column_names.iter().zip(column_types.iter()) {
            match column_type {
                ColumnType::Null => {
                    values.insert(column_name.to_string(), Value::Null);
                }
                ColumnType::I8 => {
                    let (remainder, value) = i8(rest)?;
                    rest = remainder;
                    values.insert(column_name.to_string(), Value::I8(value));
                }
                ColumnType::I16 => todo!(),
                ColumnType::I24 => todo!(),
                ColumnType::I32 => todo!(),
                ColumnType::I48 => todo!(),
                ColumnType::I64 => todo!(),
                ColumnType::F64 => todo!(),
                ColumnType::Zero => {
                    values.insert(column_name.to_string(), Value::Zero);
                }
                ColumnType::One => {
                    values.insert(column_name.to_string(), Value::One);
                }
                ColumnType::Blob(size) => {
                    let (remainder, bytes) = take(*size)(rest)?;
                    rest = remainder;
                    values.insert(
                        column_name.to_string(),
                        Value::Blob(
                            std::str::from_utf8(bytes)
                                .expect("non utf-8 text")
                                .to_owned(),
                        ),
                    );
                }
                ColumnType::Text(size) => {
                    let (remainder, bytes) = take(*size)(rest)?;
                    rest = remainder;
                    values.insert(
                        column_name.to_string(),
                        Value::Text(
                            std::str::from_utf8(bytes)
                                .expect("non utf-8 text")
                                .to_owned(),
                        ),
                    );
                }
            }
        }

        Ok((rest, Record { values }))
    }
}
