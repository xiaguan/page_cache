use std::sync::Arc;

use parking_lot::{Mutex, RwLock};

use crate::backend::Backend;
use crate::block::{format_path, Block};
use crate::block_slice::BlockSlice;
use crate::lru::LruPolicy;
use crate::mock_io::CacheKey;
use crate::CacheManager;

#[derive(Debug)]
pub struct Reader {
    ino: u64,
    cache: Arc<Mutex<CacheManager<CacheKey, LruPolicy<CacheKey>>>>,
    backend: Arc<Backend>,
}

impl Reader {
    pub fn new(
        ino: u64,
        cache: Arc<Mutex<CacheManager<CacheKey, LruPolicy<CacheKey>>>>,
        backend: Arc<Backend>,
    ) -> Self {
        Reader {
            ino,
            cache,
            backend,
        }
    }

    // Try fetch the block from `CacheManager`.
    fn fetch_block_from_cache(&self, block_id: u64) -> Option<Arc<RwLock<Block>>> {
        let key = CacheKey {
            ino: self.ino,
            block_id,
        };
        let cache = self.cache.lock();
        cache.fetch(&key)
    }

    async fn fetch_block_from_backend(&self, block_id: u64) -> Arc<RwLock<Block>> {
        let key = CacheKey {
            ino: self.ino,
            block_id,
        };
        let new_block = self.cache.lock().new_block(&key).unwrap();
        {
            let mut block = new_block.write();
            self.backend
                .read(&format_path(block_id, self.ino), &mut block)
                .await;
        }
        new_block
    }

    pub async fn read(&self, buf: &mut Vec<u8>, slices: &[BlockSlice]) -> usize {
        for slice in slices {
            let block_id = slice.block_id();
            // Block's pin count is increased by 1.
            let block = match self.fetch_block_from_cache(block_id) {
                Some(block) => block,
                None => self.fetch_block_from_backend(block_id).await,
            };
            {
                // Copy the data from the block to the buffer.
                let block = block.read();
                let offset = slice.offset() as usize;
                let size = slice.size() as usize;
                buf.extend_from_slice(&block[offset..offset + size]);
            }
            // Unpin the block
            self.cache.lock().unpin(&CacheKey {
                ino: self.ino,
                block_id,
            });
        }
        0
    }
}
