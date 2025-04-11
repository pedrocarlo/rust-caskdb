mod disk_store;
mod format;

use bincode::{Encode, error::EncodeError};
use dashmap::DashMap;
use std::path::PathBuf;
use thiserror::Error;

use format::FormatError;

#[derive(Error, Debug)]
pub enum DatabaseError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Format(#[from] FormatError),
    #[error(transparent)]
    Encode(#[from] EncodeError),
}

type KeyDirectory = DashMap<Vec<u8>, KeyEntry>;

#[derive(Debug)]
struct KeyEntry {
    file_id: PathBuf,
    total_size: u32,
    value_offset: u64,
    timestamp: u32,
}

pub trait KeyValueStore {
    fn get<K: Encode>(&mut self, key: K) -> Result<Option<Vec<u8>>, DatabaseError>;
    fn set<K: Encode, V: Encode>(&mut self, key: K, value: V) -> Result<(), DatabaseError>;
}
