use bincode::{
    Encode,
    config::{self, Configuration},
    error::EncodeError,
};
use thiserror::Error;

pub const CONFIG: Configuration = config::standard();

pub const HEADER_SIZE: usize = size_of::<Header>();

#[derive(Error, Debug)]
pub enum FormatError {
    #[error("header of incorrect size `{0}`. Size should be {HEADER_SIZE}")]
    Header(usize),
    #[error("key of incorrect size `{0}`. Size should be {1}")]
    Key(usize, usize),
    #[error("value of incorrect size `{0}`. Size should be {1}")]
    Value(usize, usize),
}

#[derive(Clone, Debug, PartialEq)]
struct Record {
    header: Header,
    key: Vec<u8>,
    value: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq)]
struct Header {
    crc: u32,
    timestamp: u32,
    key_size: u32,
    value_size: u32,
}

impl Header {
    fn encode(self) -> Vec<u8> {
        let mut ret = Vec::with_capacity(HEADER_SIZE);
        ret.extend(self.crc.to_be_bytes());
        ret.extend(self.timestamp.to_be_bytes());
        ret.extend(self.key_size.to_be_bytes());
        ret.extend(self.value_size.to_be_bytes());
        ret
    }

    fn decode(bytes: [u8; HEADER_SIZE]) -> Self {
        Self {
            crc: u32::from_be_bytes(bytes[0..4].try_into().unwrap()),
            timestamp: u32::from_be_bytes(bytes[4..8].try_into().unwrap()),
            key_size: u32::from_be_bytes(bytes[8..12].try_into().unwrap()),
            value_size: u32::from_be_bytes(bytes[12..16].try_into().unwrap()),
        }
    }
}

impl Record {
    pub fn new(timestamp: u32, key: Vec<u8>, value: Vec<u8>) -> Self {
        Self {
            header: Header {
                crc: 0,
                timestamp,
                key_size: key.len() as u32,
                value_size: value.len() as u32,
            },
            key,
            value,
        }
    }

    pub fn try_new<K: Encode, V: Encode>(
        timestamp: u32,
        key: K,
        value: V,
    ) -> Result<Self, EncodeError> {
        let key = bincode::encode_to_vec(key, CONFIG)?;
        let value = bincode::encode_to_vec(value, CONFIG)?;
        Ok(Self::new(timestamp, key, value))
    }

    pub fn encode(self) -> Vec<u8> {
        let size = HEADER_SIZE + self.key.len() + self.value.len();
        let mut ret = Vec::with_capacity(size);
        ret.extend(self.header.encode());
        ret.extend(self.key);
        ret.extend(self.value);
        ret
    }

    pub fn decode(mut data: Vec<u8>) -> Result<Self, FormatError> {
        if data.len() < HEADER_SIZE {
            return Err(FormatError::Header(data.len()));
        }
        let header_data = data.drain(0..HEADER_SIZE).collect::<Vec<_>>();
        let header = Header::decode(header_data.try_into().unwrap());

        let key: Vec<u8> = data.drain(0..header.key_size as usize).collect();
        if key.len() != header.key_size as usize {
            return Err(FormatError::Key(
                key.len(),
                header.key_size as usize,
            ));
        }
        let value: Vec<u8> = data.drain(0..header.value_size as usize).collect();
        if value.len() != header.value_size as usize {
            return Err(FormatError::Value(
                value.len(),
                header.value_size as usize,
            ));
        }

        Ok(Self { header, key, value })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use getrandom;

    fn get_random_u32() -> u32 {
        let mut buf = [0u8; 4];
        getrandom::fill(&mut buf).unwrap();
        u32::from_ne_bytes(buf)
    }

    fn now_timestamp() -> u32 {
        let now = chrono::Local::now();
        now.timestamp() as u32
    }

    fn get_random_header() -> Header {
        Header {
            crc: 0,
            timestamp: get_random_u32(),
            key_size: get_random_u32(),
            value_size: get_random_u32(),
        }
    }

    fn get_random_kv() -> Record {
        let timestamp = now_timestamp();
        let k = uuid::Uuid::new_v4().to_string();
        let v = uuid::Uuid::new_v4().to_string();

        Record::try_new(timestamp, k, v).unwrap()
    }

    fn header_test(header: Header) {
        let data = header.clone().encode();
        let same_header = Header::decode(data.try_into().unwrap());
        assert_eq!(header, same_header);
    }

    fn kv_test(kv: Record) {
        let data = kv.clone().encode();
        let same_kv = Record::decode(data).unwrap();
        assert_eq!(kv, same_kv)
    }

    #[test]
    fn test_header_serialization() {
        let tests = vec![
            Header {
                crc: 0,
                timestamp: 10,
                key_size: 10,
                value_size: 10,
            },
            Header {
                crc: 0,
                timestamp: 0,
                key_size: 0,
                value_size: 0,
            },
            Header {
                crc: 0,
                timestamp: 10000,
                key_size: 10000,
                value_size: 10000,
            },
        ];
        for header in tests {
            header_test(header);
        }
    }

    #[test]
    fn test_random_header() {
        for _ in 0..100 {
            let header = get_random_header();
            header_test(header);
        }
    }

    #[test]
    fn test_kv_serialisation() {
        let tests = vec![
            Record::try_new(now_timestamp(), "hello", "world"),
            Record::try_new(0, "", ""),
        ];

        for test in tests {
            kv_test(test.unwrap());
        }
    }

    #[test]
    fn test_random_kv() {
        for _ in 0..100 {
            let kv = get_random_kv();
            kv_test(kv);
        }
    }
}
