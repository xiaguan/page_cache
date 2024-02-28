use std::sync::atomic::AtomicU64;
use std::sync::Arc;

use async_trait::async_trait;
use parking_lot::Mutex;

use crate::backend::Backend;
use crate::error::StorageResult;
use crate::handle::handle::{FileHandle, Handles, OpenFlag};
use crate::lru::LruPolicy;
use crate::mock_io::{CacheKey, MockIO};
use crate::CacheManager;

pub struct Storage {
    handles: Arc<Handles>,
    cur_fh: AtomicU64,
    cache: Arc<Mutex<CacheManager<CacheKey, LruPolicy<CacheKey>>>>,
    backend: Arc<dyn Backend>,
}

#[async_trait]
impl MockIO for Storage {
    fn open(&self, ino: u64, flag: OpenFlag) -> u64 {
        let new_fh = self
            .cur_fh
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let handle = FileHandle::new(new_fh, ino, self.cache.clone(), self.backend.clone(), flag);
        self.handles.add_handle(handle);
        new_fh
    }

    async fn read(&self, _ino: u64, fh: u64, offset: u64, len: usize) -> StorageResult<Vec<u8>> {
        let handle = self.handles.get_handle(fh).unwrap();
        handle.read(offset, len as u64).await
    }

    async fn write(&self, _ino: u64, fh: u64, offset: u64, buf: &Vec<u8>) -> StorageResult<()> {
        let handle = self.handles.get_handle(fh).unwrap();
        handle.write(offset, buf).await;
        Ok(())
    }

    async fn flush(&self, _ino: u64, fh: u64) {
        let handle = self.handles.get_handle(fh).unwrap();
        handle.flush().await;
    }

    async fn close(&self, fh: u64) {
        let handle = self.handles.get_handle(fh).unwrap();
        handle.close().await;
        self.handles.remove_handle(fh);
    }
}

impl Storage {
    pub fn new(
        cache: Arc<Mutex<CacheManager<CacheKey, LruPolicy<CacheKey>>>>,
        backend: Arc<dyn Backend>,
    ) -> Self {
        Storage {
            handles: Arc::new(Handles::new()),
            cur_fh: AtomicU64::new(0),
            cache,
            backend,
        }
    }

    pub fn len(&self) -> usize {
        self.cache.lock().len()
    }
}
