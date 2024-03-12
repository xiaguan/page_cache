use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use parking_lot::{Mutex, RwLock};

use super::reader::Reader;
use super::writer::Writer;
use crate::backend::Backend;
use crate::block::BLOCK_SIZE;
use crate::block_slice::offset_to_slice;
use crate::error::StorageResult;
use crate::lru::LruPolicy;
use crate::mock_io::CacheKey;
use crate::CacheManager;

/// The `FileHandleInner` struct represents the inner state of a file handle.
/// It contains the file handle, reader, and writer.
#[derive(Debug)]
pub struct FileHandleInner {
    /// The file handle.
    fh: u64,
    /// The reader.
    reader: Option<Arc<Reader>>,
    /// The writer.
    writer: Option<Arc<Writer>>,
}

/// The `OpenFlag` enum represents the mode in which a file is opened.
#[derive(Debug, Clone, Copy)]
pub enum OpenFlag {
    /// Open the file for reading.
    Read,
    /// Open the file for writing.
    Write,
    /// Open the file for reading and writing.
    ReadAndWrite,
}

impl FileHandleInner {
    /// Creates a new `FileHandleInner` instance with the given parameters.
    #[inline]
    pub fn new(
        fh: u64,
        ino: u64,
        cache: Arc<Mutex<CacheManager<CacheKey, LruPolicy<CacheKey>>>>,
        backend: Arc<dyn Backend>,
        flag: OpenFlag,
    ) -> Self {
        let reader = match flag {
            OpenFlag::Read | OpenFlag::ReadAndWrite => {
                Some(Arc::new(Reader::new(ino, cache.clone(), backend.clone())))
            }
            _ => None,
        };
        let writer = match flag {
            OpenFlag::Write | OpenFlag::ReadAndWrite => {
                Some(Arc::new(Writer::new(ino, cache.clone(), backend.clone())))
            }
            _ => None,
        };
        FileHandleInner { fh, reader, writer }
    }
}

/// The `FileHandle` struct represents a handle to an open file.
/// It contains an `Arc` of `RwLock<FileHandleInner>`.
#[derive(Debug, Clone)]
pub struct FileHandle {
    inner: Arc<RwLock<FileHandleInner>>,
}

impl FileHandle {
    /// Creates a new `FileHandle` instance with the given parameters.
    pub fn new(
        fh: u64,
        ino: u64,
        cache: Arc<Mutex<CacheManager<CacheKey, LruPolicy<CacheKey>>>>,
        backend: Arc<dyn Backend>,
        flag: OpenFlag,
    ) -> Self {
        let inner = FileHandleInner::new(fh, ino, cache, backend, flag);
        let inner = Arc::new(RwLock::new(inner));
        FileHandle { inner }
    }

    /// Returns the file handle.
    pub fn fh(&self) -> u64 {
        self.inner.read().fh
    }

    /// Reads data from the file starting at the given offset and up to the given length.
    pub async fn read(&self, offset: u64, len: u64) -> StorageResult<Vec<u8>> {
        let reader = {
            let handle = self.inner.read();
            handle.reader.as_ref().unwrap().clone()
        };
        let slices = offset_to_slice(BLOCK_SIZE as u64, offset, len);
        let mut buf = Vec::with_capacity(len as usize);
        reader.read(&mut buf, &slices).await?;
        Ok(buf)
    }

    /// Writes data to the file starting at the given offset.
    pub async fn write(&self, offset: u64, buf: &Vec<u8>) {
        let writer = {
            let handle = self.inner.read();
            handle.writer.as_ref().unwrap().clone()
        };
        let slices = offset_to_slice(BLOCK_SIZE as u64, offset, buf.len() as u64);
        writer.write(buf, &slices).await;
    }

    /// Extends the file from the old size to the new size.
    pub async fn extend(&self, old_size: u64, new_size: u64) {
        let writer = {
            let handle = self.inner.read();
            handle.writer.as_ref().unwrap().clone()
        };
        writer.extend(old_size, new_size).await
    }

    /// Flushes any pending writes to the file.
    pub async fn flush(&self) {
        let writer = {
            let handle = self.inner.read();
            handle.writer.as_ref().unwrap().clone()
        };
        writer.flush().await;
    }

    /// Closes the writer associated with the file handle.
    async fn close_writer(&self) {
        let writer = {
            let handle = self.inner.read();
            match &handle.writer {
                Some(writer) => Some(writer.clone()),
                None => None,
            }
        };
        if let Some(writer) = writer {
            writer.close().await;
        }
    }

    /// Closes the file handle, closing both the reader and writer.
    pub async fn close(&self) {
        self.close_writer().await;
        if let Some(reader) = self.inner.read().reader.as_ref() {
            reader.close();
        }
    }
}

const HANDLE_SHARD_NUM: usize = 100;

/// The `Handles` struct represents a collection of file handles.
/// It uses sharding to avoid lock contention.
#[derive(Debug)]
pub struct Handles {
    /// Use shard to avoid lock contention
    handles: [Arc<RwLock<Vec<FileHandle>>>; HANDLE_SHARD_NUM],
}

impl Handles {
    /// Creates a new `Handles` instance.
    #[must_use]
    #[inline]
    pub fn new() -> Self {
        let mut handles: Vec<_> = Vec::with_capacity(HANDLE_SHARD_NUM);
        for _ in 0..HANDLE_SHARD_NUM {
            handles.push(Arc::new(RwLock::new(Vec::new())));
        }
        let handles: [_; HANDLE_SHARD_NUM] = handles.try_into().expect("Incorrect length");
        Handles { handles }
    }

    /// Returns the shard index for the given file handle.
    fn hash(&self, fh: u64) -> usize {
        let mut hasher = DefaultHasher::new();
        fh.hash(&mut hasher);
        (hasher.finish() as usize) % HANDLE_SHARD_NUM
    }

    /// Adds a file handle to the collection.
    pub fn add_handle(&self, fh: FileHandle) {
        let index = self.hash(fh.fh());
        let shard = &self.handles[index];
        let mut shard_lock = shard.write();
        shard_lock.push(fh);
    }

    /// Removes a file handle from the collection.
    pub fn remove_handle(&self, fh: u64) -> Option<FileHandle> {
        let index = self.hash(fh);
        let shard = &self.handles[index];
        let mut shard_lock = shard.write();
        if let Some(pos) = shard_lock.iter().position(|h| h.fh() == fh) {
            Some(shard_lock.remove(pos))
        } else {
            None
        }
    }

    /// Returns a file handle from the collection.
    pub fn get_handle(&self, fh: u64) -> Option<FileHandle> {
        let index = self.hash(fh);
        let shard = &self.handles[index];
        let shard_lock = shard.read();
        let fh = shard_lock.iter().find(|h| h.fh() == fh)?;
        Some(fh.clone())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::backend::backend_impl::tmp_fs_backend;
    use crate::CacheManager;

    #[tokio::test]
    async fn test_file_handle() {
        let cache = CacheManager::new(100);
        let backend = Arc::new(tmp_fs_backend());
        let handles = Arc::new(Handles::new());
        let ino = 1;
        let fh = 1;
        let file_handle = FileHandleInner::new(fh, ino, cache, backend, OpenFlag::ReadAndWrite);
        let file_handle = Arc::new(RwLock::new(file_handle));
        let file_handle = FileHandle { inner: file_handle };
        handles.add_handle(file_handle.clone());
        let buf = vec![b'1', b'2', b'3', b'4'];
        file_handle.write(0, &buf).await;
        let read_buf = file_handle.read(0, 4).await.unwrap();
        assert_eq!(read_buf, buf);
        file_handle.flush().await;
    }
}
