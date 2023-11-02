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
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(String),
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match self {
            Value::Null => false,
            Value::Integer(n1) => match other {
                Value::Integer(n2) => n1 == n2,
                _ => false,
            },
            Value::Real(f1) => match other {
                Value::Real(f2) => f1 == f2,
                _ => false,
            },
            Value::Text(s1) => match other {
                Value::Text(s2) => s1 == s2,
                Value::Blob(s2) => s1 == s2,
                _ => false,
            },
            Value::Blob(s1) => match other {
                Value::Text(s2) => s1 == s2,
                Value::Blob(s2) => s1 == s2,
                _ => false,
            },
        }
    }
}

impl Value {
    pub fn as_integer(&self) -> Option<i64> {
        match self {
            Value::Integer(n) => Some(*n),
            _ => None,
        }
    }

    #[allow(dead_code)]
    pub fn as_real(&self) -> Option<f64> {
        match self {
            Value::Real(f) => Some(*f),
            _ => None,
        }
    }

    pub fn as_text(&self) -> Option<&str> {
        match self {
            Value::Text(s) => Some(s),
            _ => None,
        }
    }

    #[allow(dead_code)]
    pub fn as_blob(&self) -> Option<&str> {
        match self {
            Value::Blob(s) => Some(s),
            _ => None,
        }
    }
}

impl ToString for Value {
    fn to_string(&self) -> String {
        match self {
            Value::Null => "null".into(),
            Value::Integer(n) => n.to_string(),
            Value::Real(f) => f.to_string(),
            Value::Blob(s) => s.to_owned(),
            Value::Text(s) => s.to_owned(),
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
    pub fn parse<'input>(
        input: &'input [u8],
        column_names: &[String],
    ) -> IResult<&'input [u8], Self> {
        let (input, _record_size) = VarInt::parse(input)?;
        let (input, _row_id) = VarInt::parse(input)?;

        // Skip overflow info
        let (input, _) = take(1usize)(input)?;

        let mut rest = input;
        let mut column_types = Vec::new();
        for _ in 0..column_names.len() {
            let (remainder, column_type) = VarInt::parse(rest)?;
            rest = remainder;
            let column_type = ColumnType::try_from(column_type).expect("invalid column type");
            column_types.push(column_type);
        }
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
                    values.insert(column_name.to_string(), Value::Integer(value as i64));
                }
                ColumnType::I16 => todo!(),
                ColumnType::I24 => todo!(),
                ColumnType::I32 => todo!(),
                ColumnType::I48 => todo!(),
                ColumnType::I64 => todo!(),
                ColumnType::F64 => todo!(),
                ColumnType::Zero => {
                    values.insert(column_name.to_string(), Value::Integer(0i64));
                }
                ColumnType::One => {
                    values.insert(column_name.to_string(), Value::Integer(0i64));
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
