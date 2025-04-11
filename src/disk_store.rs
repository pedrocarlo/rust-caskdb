use bincode::Encode;
use dashmap::DashMap;
use std::{
    fs::File,
    io::{Read, Seek, Write},
    path::{Path, PathBuf},
};

use super::{
    DatabaseError, KeyDirectory, KeyEntry, KeyValueStore,
    format::{CONFIG, FormatError, HEADER_SIZE, Header, Record},
};

#[derive(Debug)]
struct DiskStore {
    // dir: PathBuf,
    file: File,
    active_path: PathBuf,
    key_dir: KeyDirectory,
    write_position: u64,
}

impl DiskStore {
    pub fn new(file: PathBuf) -> Result<Self, DatabaseError> {
        // let parent = file.parent();
        let active_path = file.clone();

        let mut write_position = 0;
        let mut key_dir = DashMap::new();

        if file.exists() {
            DiskStore::init_key_dir(&active_path, &mut key_dir, &mut write_position)?;
        }

        // TODO Unwrap for now
        let file = File::options()
            .create(true)
            .append(true)
            .read(true)
            .open(file)?;

        Ok(Self {
            file,
            active_path,
            key_dir,
            write_position,
        })
    }

    fn init_key_dir(
        file_path: &Path,
        key_dir: &mut KeyDirectory,
        write_position: &mut u64,
    ) -> Result<(), DatabaseError> {
        let mut file = File::open(file_path)?;
        let mut buf = [0u8; HEADER_SIZE];

        loop {
            let n = file.read(&mut buf)?;
            if n == 0 {
                break;
            }
            if n != HEADER_SIZE {
                return Err(DatabaseError::Format(FormatError::Header(n)));
            }
            let header = Header::decode(buf);
            // TODO when value size is zero dont read

            let mut key = vec![0; header.key_size() as usize];

            file.read_exact(&mut key)?;

            let key_entry = KeyEntry {
                file_id: file_path.to_path_buf(),
                total_size: header.total_size(),
                value_offset: *write_position,
                timestamp: header.timestamp(),
            };
            key_dir.insert(key, key_entry);
            *write_position += HEADER_SIZE as u64 + header.total_size() as u64;
            // Advance the cursor as we need to get next header entry
            file.seek(std::io::SeekFrom::Current(header.value_size() as i64))?;
        }
        Ok(())
    }
}

impl KeyValueStore for DiskStore {
    fn get<K: Encode>(&mut self, key: K) -> Result<Option<Vec<u8>>, DatabaseError> {
        let key = bincode::encode_to_vec(key, CONFIG)?;
        let Some(hint) = self.key_dir.get(&key) else {
            return Ok(None);
        };

        self.file
            .seek(std::io::SeekFrom::Start(hint.value_offset))?;

        let mut buf = vec![0; HEADER_SIZE + hint.total_size as usize];
        self.file.read_exact(&mut buf)?;
        let record = Record::decode(buf)?;

        Ok(Some(record.value))
    }

    fn set<K: Encode, V: Encode>(&mut self, key: K, value: V) -> Result<(), DatabaseError> {
        let now = chrono::Local::now().timestamp() as u32;
        let record = Record::try_new(now, key, value)?;

        // Update key_dir
        let key_entry = KeyEntry {
            timestamp: now,
            file_id: self.active_path.clone(),
            // Can avoid a seek here if we have an internal variable to keep track of current position
            value_offset: self.write_position,
            total_size: record.header.total_size(),
        };

        // TODO maybe order of operations could be a problem here
        self.key_dir.insert(record.key().to_vec(), key_entry);

        self.write_position += HEADER_SIZE as u64 + record.header.total_size() as u64;

        let data = record.encode();
        // TODO see if need to check if all bytes were written
        let _ = self.file.write(&data)?;

        // Fsync for more durability
        self.file.sync_all()?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::fs::remove_file;

    use super::*;
    use tempfile::NamedTempFile;

    fn setup() -> NamedTempFile {
        NamedTempFile::new().unwrap()
    }

    struct TestFile(PathBuf);

    impl Drop for TestFile {
        fn drop(&mut self) {
            if self.0.exists() && self.0.is_file() {
                let _ = remove_file(self.0.clone());
            }
        }
    }

    fn kv_pairs() -> Vec<(&'static str, &'static str)> {
        vec![
            ("crime and punishment", "dostoevsky"),
            ("anna karenina", "tolstoy"),
            ("war and peace", "tolstoy"),
            ("hamlet", "shakespeare"),
            ("othello", "shakespeare"),
            ("brave new world", "huxley"),
            ("dune", "frank herbert"),
        ]
    }

    #[test]
    fn test_get() {
        let file = setup();

        let mut store = DiskStore::new(file.path().to_path_buf()).unwrap();
        store.set("name", "jojo").unwrap();

        assert_eq!(
            store.get("name").unwrap().unwrap(),
            bincode::encode_to_vec("jojo", CONFIG).unwrap()
        );
    }

    #[test]
    fn test_invalid_key() {
        let file = setup();
        let mut store = DiskStore::new(file.path().to_path_buf()).unwrap();
        assert_eq!(store.get("some key").unwrap(), None)
    }

    #[test]
    fn test_persistence() {
        let file = setup();

        let mut store = DiskStore::new(file.path().to_path_buf()).unwrap();
        let tests = kv_pairs();
        for (key, value) in tests.iter() {
            store.set(*key, *value).unwrap();
            assert_eq!(
                store.get(*key).unwrap().unwrap(),
                bincode::encode_to_vec(*value, CONFIG).unwrap()
            );
        }

        let mut store = DiskStore::new(file.path().to_path_buf()).unwrap();
        for (key, value) in tests {
            assert_eq!(
                store.get(key).unwrap().unwrap(),
                bincode::encode_to_vec(value, CONFIG).unwrap()
            );
        }
    }

    #[test]
    fn test_deletion() {
        let file = setup();

        let mut store = DiskStore::new(file.path().to_path_buf()).unwrap();
        let tests = kv_pairs();
        for (key, value) in tests.iter() {
            store.set(*key, *value).unwrap();
            assert_eq!(
                store.get(*key).unwrap().unwrap(),
                bincode::encode_to_vec(*value, CONFIG).unwrap()
            );
        }
        for (key, _) in tests.iter() {
            store.set(*key, "").unwrap();
            assert_eq!(
                store.get(*key).unwrap().unwrap(),
                bincode::encode_to_vec("", CONFIG).unwrap()
            );
        }
        store.set("end", "yes").unwrap();

        let mut store = DiskStore::new(file.path().to_path_buf()).unwrap();
        for (key, _) in tests {
            assert_eq!(
                store.get(key).unwrap().unwrap(),
                bincode::encode_to_vec("", CONFIG).unwrap()
            );
        }
        assert_eq!(
            store.get("end").unwrap().unwrap(),
            bincode::encode_to_vec("yes", CONFIG).unwrap()
        )
    }

    #[test]
    fn test_get_new_file() {
        let file = TestFile(PathBuf::from("test.db"));
        let mut store = DiskStore::new(file.0.to_path_buf()).unwrap();
        store.set("name", "jojo").unwrap();

        assert_eq!(
            store.get("name").unwrap().unwrap(),
            bincode::encode_to_vec("jojo", CONFIG).unwrap()
        );

        let mut store = DiskStore::new(file.0.to_path_buf()).unwrap();

        assert_eq!(
            store.get("name").unwrap().unwrap(),
            bincode::encode_to_vec("jojo", CONFIG).unwrap()
        );
    }
}
