use std::sync::Arc;

use parking_lot::Mutex;

use crate::lru::LruPolicy;
use crate::mock_io::CacheKey;
use crate::CacheManager;

pub struct ReadHandle {
    fh: usize,
    ino: u64,
    cache: Arc<Mutex<CacheManager<CacheKey, LruPolicy<CacheKey>>>>,
}

impl ReadHandle {
    pub async fn read(&self, buf: &mut [u8], offset: u64) -> usize {
        let cache_key = CacheKey {
            ino: self.ino,
            block_id: offset / 4096,
        };
        let block = {
            let cache = self.cache.lock();
            cache.fetch(&cache_key)
        };
        match block {
            Some(block) => {
                // Cache hit
                // Do some logic here
                {
                    buf.copy_from_slice(&block.read().as_ref());
                }
                // Unpin the block ,the block is pinned by `fetch` method
                self.cache.lock().unpin(&cache_key);
            }
            None => {
                // Cache not hit
                let new_block = {
                    let mut cache = self.cache.lock();
                    cache.new_block(&cache_key)
                };
                // TODO: if `new_block` is none, sleep and retry
                let mut new_block = new_block.unwrap();
                // Load from backend
                // self.backend.read(path,buf)
                // new_block.write().as_mut_ref()
                // unpin the block
            }
        }
        0
    }
}
