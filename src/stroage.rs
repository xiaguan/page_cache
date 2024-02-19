use std::sync::Arc;

use parking_lot::Mutex;

use crate::backend::Backend;
use crate::lru::LruPolicy;
use crate::CacheManager;

pub struct Storage {
    cache: Arc<Mutex<CacheManager<u128, LruPolicy<u128>>>>,
    backend: Backend,
}

impl Storage {
    pub fn new(cache_size: usize, backend: Backend) -> Self {
        let (tx, rx) = std::sync::mpsc::channel::<(
            u128,
            std::sync::Arc<parking_lot::RwLock<crate::block::Block>>,
        )>();
        let new_cache = CacheManager::<u128, LruPolicy<u128>>::new(cache_size, tx.clone());
        let weak = std::sync::Arc::downgrade(&new_cache);

        std::thread::spawn(move || {
            while let Ok((key, block)) = rx.recv() {
                if key == u128::MAX {
                    return;
                }
                {
                    let mut block = block.write();
                    block.set_dirty(false);
                }
                weak.upgrade().unwrap().lock().unpin(&key);
            }
        });

        Self {
            cache: new_cache,
            backend,
        }
    }
}
