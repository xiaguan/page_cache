use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use parking_lot::{Mutex, RwLock};

use super::reader::Reader;
use super::writer::Writer;
use crate::backend::Backend;
use crate::block::BLOCK_SIZE;
use crate::block_slice::offset_to_slice;
use crate::lru::LruPolicy;
use crate::mock_io::CacheKey;
use crate::CacheManager;

#[derive(Debug)]
pub struct FileHandleInner {
    fh: u64,
    reader: Option<Arc<Reader>>,
    writer: Option<Arc<Writer>>,
}

unsafe impl Send for FileHandleInner {}

pub enum OpenFlag {
    Read,
    Write,
    ReadAndWrite,
}

impl FileHandleInner {
    pub fn new(
        fh: u64,
        ino: u64,
        cache: Arc<Mutex<CacheManager<CacheKey, LruPolicy<CacheKey>>>>,
        backend: Arc<Backend>,
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
#[derive(Debug, Clone)]
pub struct FileHandle {
    inner: Arc<RwLock<FileHandleInner>>,
}

impl FileHandle {
    pub fn new(
        fh: u64,
        ino: u64,
        cache: Arc<Mutex<CacheManager<CacheKey, LruPolicy<CacheKey>>>>,
        backend: Arc<Backend>,
        flag: OpenFlag,
    ) -> Self {
        let inner = FileHandleInner::new(fh, ino, cache, backend, flag);
        let inner = Arc::new(RwLock::new(inner));
        FileHandle { inner }
    }

    pub fn fh(&self) -> u64 {
        self.inner.read().fh
    }

    pub async fn read(&self, offset: u64, len: u64) -> Vec<u8> {
        let reader = {
            let handle = self.inner.read();
            handle.reader.as_ref().unwrap().clone()
        };
        let slices = offset_to_slice(BLOCK_SIZE as u64, offset, len);
        let mut buf = Vec::with_capacity(len as usize);
        reader.read(&mut buf, &slices).await;
        buf
    }

    pub async fn write(&self, offset: u64, buf: &Vec<u8>) {
        let writer = {
            let handle = self.inner.read();
            handle.writer.as_ref().unwrap().clone()
        };
        let slices = offset_to_slice(BLOCK_SIZE as u64, offset, buf.len() as u64);
        writer.write(buf, &slices).await;
    }

    pub async fn flush(&self) {
        let writer = {
            let handle = self.inner.read();
            handle.writer.as_ref().unwrap().clone()
        };
        writer.flush().await;
    }

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

    pub async fn close(&self) {
        self.close_writer().await;
        if let Some(reader) = self.inner.read().reader.as_ref() {
            reader.close();
        }
    }
}
const HANDLE_SHARD_NUM: usize = 100;

pub struct Handles {
    handles: [Arc<RwLock<Vec<FileHandle>>>; HANDLE_SHARD_NUM],
}

unsafe impl Send for Handles {}

impl Handles {
    pub fn new() -> Self {
        let mut handles: Vec<_> = Vec::with_capacity(HANDLE_SHARD_NUM);
        for _ in 0..HANDLE_SHARD_NUM {
            handles.push(Arc::new(RwLock::new(Vec::new())));
        }
        let handles: [_; HANDLE_SHARD_NUM] = handles.try_into().expect("Incorrect length");
        Handles { handles }
    }

    fn hash(&self, fh: u64) -> usize {
        let mut hasher = DefaultHasher::new();
        fh.hash(&mut hasher);
        (hasher.finish() as usize) % HANDLE_SHARD_NUM
    }

    pub fn add_handle(&self, fh: FileHandle) {
        let index = self.hash(fh.fh());
        let shard = &self.handles[index];
        let mut shard_lock = shard.write();
        shard_lock.push(fh);
    }

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
    use crate::backend::backend::tmp_fs_backend;
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
        let read_buf = file_handle.read(0, 4).await;
        assert_eq!(read_buf, buf);
        file_handle.flush().await;
    }
}
