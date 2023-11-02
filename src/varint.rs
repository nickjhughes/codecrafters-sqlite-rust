use nom::IResult;

#[derive(Debug, PartialEq)]
pub struct VarInt(pub i64);

impl VarInt {
    pub fn parse(input: &[u8]) -> IResult<&[u8], Self> {
        let mut i = 0;
        let mut value: i64 = (input[i] as i64) & 0x7f;
        while high_bit(input[i]) {
            i += 1;
            value = (value << 7) | ((input[i] as i64) & 0x7f);
        }
        Ok((&input[i + 1..], VarInt(value)))
    }
}

/// Is the high bit of this byte set?
fn high_bit(byte: u8) -> bool {
    (byte & 0xf0) >> 7 == 1
}

#[cfg(test)]
mod tests {
    use super::{high_bit, VarInt};

    #[test]
    fn test_high_bit() {
        assert!(high_bit(0b10000000));
        assert!(high_bit(0b10111010));

        assert!(!high_bit(0b00000000));
        assert!(!high_bit(0b01111111));
    }

    #[test]
    fn one_byte() {
        let input = &[0x15];
        let (rest, value) = VarInt::parse(input).unwrap();
        assert!(rest.is_empty());
        assert_eq!(value, VarInt(0x15));
    }

    #[test]
    fn two_bytes() {
        let input = &[0x87, 0x68];
        let (rest, value) = VarInt::parse(input).unwrap();
        assert!(rest.is_empty());
        assert_eq!(value, VarInt(1000));
    }

    #[test]
    fn three_bytes() {
        let input = &[0xc8, 0xf2, 0x19];
        let (rest, value) = VarInt::parse(input).unwrap();
        assert!(rest.is_empty());
        assert_eq!(value, VarInt(1194265));
    }

    #[test]
    fn four_bytes() {
        let input = &[0xd1, 0x9a, 0xe2, 0x67];
        let (rest, value) = VarInt::parse(input).unwrap();
        assert!(rest.is_empty());
        assert_eq!(value, VarInt(170307943));
    }
}
