use std::sync::atomic::AtomicU64;
use std::sync::Arc;

use async_trait::async_trait;
use parking_lot::Mutex;

use crate::backend::Backend;
use crate::block::{format_path, BLOCK_SIZE};
use crate::error::StorageResult;
use crate::handle::handle::{FileHandle, Handles, OpenFlag};
use crate::lru::LruPolicy;
use crate::mock_io::{CacheKey, MockIO};
use crate::CacheManager;

/// The `Storage` struct represents a storage system that implements the `MockIO` trait.
/// It manages file handles, caching, and interacts with a backend storage.
#[derive(Debug)]
pub struct Storage {
    /// The file handles.
    handles: Arc<Handles>,
    /// The current file handle.
    cur_fh: AtomicU64,
    /// The cache manager.
    cache: Arc<Mutex<CacheManager<CacheKey, LruPolicy<CacheKey>>>>,
    /// The backend storage system.
    backend: Arc<dyn Backend>,
}

#[async_trait]
impl MockIO for Storage {
    /// Opens a file with the given inode number and flags, returning a new file handle.
    #[inline]
    fn open(&self, ino: u64, flag: OpenFlag) -> u64 {
        let new_fh = self
            .cur_fh
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let handle = FileHandle::new(new_fh, ino, self.cache.clone(), self.backend.clone(), flag);
        self.handles.add_handle(handle);
        new_fh
    }

    /// Reads data from a file specified by the file handle, starting at the given offset and reading up to `len` bytes.
    #[inline]
    async fn read(&self, _ino: u64, fh: u64, offset: u64, len: usize) -> StorageResult<Vec<u8>> {
        let handle = self.handles.get_handle(fh).unwrap();
        handle.read(offset, len as u64).await
    }

    /// Writes data to a file specified by the file handle, starting at the given offset.
    #[inline]
    async fn write(&self, _ino: u64, fh: u64, offset: u64, buf: &Vec<u8>) -> StorageResult<()> {
        let handle = self.handles.get_handle(fh).unwrap();
        handle.write(offset, buf).await;
        Ok(())
    }

    /// Flushes any pending writes to a file specified by the file handle.
    #[inline]
    async fn flush(&self, _ino: u64, fh: u64) {
        let handle = self.handles.get_handle(fh).unwrap();
        handle.flush().await;
    }

    /// Closes a file specified by the file handle.
    #[inline]
    async fn close(&self, fh: u64) {
        let handle = self.handles.get_handle(fh).unwrap();
        handle.close().await;
        self.handles.remove_handle(fh);
    }

    /// Truncates a file specified by the inode number to a new size, given the old size.
    #[inline]
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
            // new_size < old_size, we may need to remove some blocks
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
    /// Creates a new `Storage` instance with the provided cache and backend.
    #[inline]
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

    /// Returns the number of items in the cache.
    #[inline]
    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        self.cache.lock().len()
    }
}
