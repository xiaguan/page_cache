use std::sync::Arc;

use hashbrown::HashSet;
use parking_lot::{Mutex, RwLock};

use crate::backend::Backend;
use crate::block::{format_path, Block, BLOCK_SIZE};
use crate::block_slice::BlockSlice;
use crate::error::{StorageError, StorageResult};
use crate::lru::LruPolicy;
use crate::mock_io::CacheKey;
use crate::CacheManager;

/// Reader is a struct responsible for reading blocks of data from a backend storage system,
/// optionally caching these blocks using a `CacheManager` with LRU eviction policy.
#[derive(Debug)]
pub struct Reader {
    ino: u64, // The inode number associated with the file being read.
    cache: Arc<Mutex<CacheManager<CacheKey, LruPolicy<CacheKey>>>>, // Thread-safe reference to the cache manager.
    backend: Arc<dyn Backend>, // Thread-safe reference to the backend storage system.
    access_keys: Mutex<HashSet<CacheKey>>, // A set of keys that tracks accessed cache blocks.
}

impl Reader {
    /// Creates a new `Reader` instance with the given parameters.
    pub fn new(
        ino: u64,
        cache: Arc<Mutex<CacheManager<CacheKey, LruPolicy<CacheKey>>>>,
        backend: Arc<dyn Backend>,
    ) -> Self {
        Reader {
            ino,
            cache,
            backend,
            access_keys: Mutex::new(HashSet::new()),
        }
    }

    /// Try fetch the block from `CacheManager`.
    fn fetch_block_from_cache(&self, block_id: u64) -> Option<Arc<RwLock<Block>>> {
        let key = CacheKey {
            ino: self.ino,
            block_id,
        };
        let cache = self.cache.lock();
        cache.fetch(&key)
    }

    /// Mark the block as accessed.
    fn access(&self, block_id: u64) {
        let key = CacheKey {
            ino: self.ino,
            block_id,
        };
        let mut access_keys = self.access_keys.lock();
        access_keys.insert(key);
    }

    /// Fetch the block from the backend storage system.
    async fn fetch_block_from_backend(&self, block_id: u64) -> StorageResult<Arc<RwLock<Block>>> {
        let key = CacheKey {
            ino: self.ino,
            block_id,
        };
        let content = {
            let mut buf = vec![0; BLOCK_SIZE];
            self.backend
                .read(&format_path(block_id, self.ino), &mut buf)
                .await
                .map_err(|e| {
                    self.cache.lock().unpin(&key);
                    e
                })?;
            buf
        };
        match self.cache.lock().new_block(&key, &content) {
            Some(block) => Ok(block),
            None => Err(StorageError::OutOfMemory),
        }
    }

    /// Reads data from the file starting at the given offset and up to the given length.
    pub async fn read(&self, buf: &mut Vec<u8>, slices: &[BlockSlice]) -> StorageResult<usize> {
        for slice in slices {
            let block_id = slice.block_id();
            self.access(block_id);
            // Block's pin count is increased by 1.
            let block = match self.fetch_block_from_cache(block_id) {
                Some(block) => block,
                None => self.fetch_block_from_backend(block_id).await?,
            };
            {
                // Copy the data from the block to the buffer.
                let block = block.read();
                assert!(block.pin_count() >= 1);
                let offset = slice.offset() as usize;
                let size = slice.size() as usize;
                buf.extend_from_slice(&block[offset..offset + size]);
            }
            self.cache.lock().unpin(&CacheKey {
                ino: self.ino,
                block_id,
            });
        }
        Ok(buf.len())
    }

    /// Close the reader and remove the accessed cache blocks.
    pub fn close(&self) {
        let access_keys = self.access_keys.lock();
        for key in access_keys.iter() {
            self.cache.lock().remove(key);
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;

    use super::*;
    use crate::backend::backend_impl::memory_backend;
    use crate::handle::writer::Writer;
    use crate::lru::LruPolicy;
    use crate::mock_io::CacheKey;
    use crate::CacheManager;

    #[tokio::test]
    async fn test_reader() {
        let backend = Arc::new(memory_backend());
        let manger = CacheManager::<CacheKey, LruPolicy<CacheKey>>::new(10);
        let content = Bytes::from(vec![b'1'; BLOCK_SIZE]);
        let slice = BlockSlice::new(0, 0, content.len() as u64);

        let writer = Writer::new(1, manger.clone(), backend.clone());
        writer.write(&content, &[slice]).await;
        writer.flush().await;
        writer.close().await;

        let reader = Reader::new(1, manger.clone(), backend);
        let slice = BlockSlice::new(0, 0, BLOCK_SIZE as u64);
        let mut buf = Vec::with_capacity(BLOCK_SIZE);
        let size = reader.read(&mut buf, &[slice]).await.unwrap();
        assert_eq!(size, BLOCK_SIZE);
        let memory_size = manger.lock().len();
        assert_eq!(memory_size, 1);
        reader.close();
        let memory_size = manger.lock().len();
        assert_eq!(memory_size, 0);
    }
}
