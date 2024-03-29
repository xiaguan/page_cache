use async_trait::async_trait;
use opendal::raw::oio::ReadExt;
use opendal::services::{Fs, Memory};
use opendal::{ErrorKind, Operator};
use tokio::io::AsyncWriteExt;

use super::Backend;
use crate::error::StorageResult;

/// The `BackendImpl` struct represents a backend storage system that implements the `Backend` trait.
#[derive(Debug)]
pub struct BackendImpl {
    operator: Operator,
}

impl BackendImpl {
    /// Creates a new `BackendImpl` instance with the given operator.
    pub fn new(operator: Operator) -> Self {
        Self { operator }
    }
}

#[async_trait]
impl Backend for BackendImpl {
    #[inline]
    async fn read(&self, path: &str, buf: &mut [u8]) -> StorageResult<usize> {
        let mut reader = self.operator.reader(path).await?;
        let mut read_size = 0;
        loop {
            let result = reader.read(buf).await;
            match result {
                Ok(size) => {
                    if size == 0 {
                        break;
                    }
                    read_size += size;
                }
                Err(e) => {
                    // If not found just return 0.
                    if e.kind() == ErrorKind::NotFound {
                        break;
                    }
                }
            }
        }
        Ok(read_size)
    }

    #[inline]
    async fn store(&self, path: &str, buf: &[u8]) -> StorageResult<()> {
        let mut writer = self.operator.writer(path).await?;
        writer.write_all(buf).await?;
        writer.close().await?;
        Ok(())
    }
    #[inline]
    async fn remove(&self, path: &str) -> StorageResult<()> {
        self.operator.remove_all(path).await?;
        Ok(())
    }
}

/// Creates a new `BackendImpl` instance with a memory backend.
pub fn memory_backend() -> BackendImpl {
    let op = Operator::new(Memory::default()).unwrap().finish();
    BackendImpl::new(op)
}

/// Creates a new `BackendImpl` instance with a temporary file system backend.
pub fn tmp_fs_backend() -> BackendImpl {
    let mut builder = Fs::default();
    builder.root("/tmp/backend/");
    let op = Operator::new(builder).unwrap().finish();
    BackendImpl::new(op)
}

#[cfg(test)]
mod tests {

    use super::*;

    #[tokio::test]
    async fn test_backend() {
        let backend = memory_backend();
        let path = "test";
        let data = b"hello world";
        backend.store(path, data).await.unwrap();
        let mut buf = vec![0u8; data.len()];
        backend.read(path, &mut buf).await.unwrap();
        assert_eq!(buf, data);
    }
}
