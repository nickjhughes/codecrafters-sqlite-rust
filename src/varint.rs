use nom::IResult;

#[allow(dead_code)]
#[derive(Debug, PartialEq)]
pub enum VarInt {
    I8(i8),
    I16(i16),
    I32(i32),
    I64(i64),
}

impl VarInt {
    pub fn parse(input: &[u8]) -> IResult<&[u8], Self> {
        let mut bytes = [0u8; 9];
        bytes[0] = input[0];
        let mut input = &input[1..];
        let mut byte_count = 1;
        while byte_count < 9 && high_bit(bytes[byte_count - 1]) {
            bytes[byte_count] = input[0];
            input = &input[1..];
            byte_count += 1;
        }

        let value = match byte_count {
            1 => VarInt::I8((bytes[0] & 0x7f) as i8),
            2 => VarInt::I16((((bytes[0] & 0x7f) as i16) << 7) | (bytes[1] & 0x7f) as i16),
            3..=9 => todo!("3+ byte varint"),
            _ => unreachable!(),
        };

        Ok((input, value))
    }
}

/// Is the high bit of this byte set?
fn high_bit(byte: u8) -> bool {
    (byte & 0xF0) >> 7 == 1
}

#[cfg(test)]
mod tests {
    use super::VarInt;

    #[test]
    fn one_byte() {
        let input = &[0x15];
        let (rest, value) = VarInt::parse(input).unwrap();
        assert!(rest.is_empty());
        assert_eq!(value, VarInt::I8(0x15));
    }

    #[test]
    fn two_bytes() {
        let input = &[0x87, 0x68];
        let (rest, value) = VarInt::parse(input).unwrap();
        assert!(rest.is_empty());
        assert_eq!(value, VarInt::I16(1000));
    }
}
