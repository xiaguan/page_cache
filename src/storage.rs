use std::sync::atomic::AtomicU64;
use std::sync::Arc;

use async_trait::async_trait;
use parking_lot::Mutex;

use crate::backend::Backend;
use crate::block::{format_path, BLOCK_SIZE};
use crate::block_slice::offset_to_slice;
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

    async fn truncate(&self, ino: u64, old_size: u64, new_size: u64) -> StorageResult<()> {
        // If new_size == old_size, do nothing
        if new_size == old_size {
            return Ok(());
        }
        // If new_size > old_size, fill the gap with zeros
        if new_size > old_size {
            // Create a new write handle
            let fh = self.open(ino, OpenFlag::Write);
            let handle = self.handles.get_handle(fh).unwrap();
            handle.extend(old_size, new_size).await;
            self.close(fh).await;
        } else {
            // new_size < old_size, we may need remove some block
            let start = new_size / BLOCK_SIZE as u64 + 1;
            let end = old_size / BLOCK_SIZE as u64;
            for block_id in start..end {
                self.backend.remove(&format_path(block_id, ino)).await?;
            }
        }

        Ok(())
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
