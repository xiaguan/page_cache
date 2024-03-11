use async_trait::async_trait;

use crate::error::StorageResult;
use crate::handle::handle::OpenFlag;

#[async_trait]
pub trait MockIO {
    fn open(&self, ino: u64, flag: OpenFlag) -> u64;
    async fn read(&self, ino: u64, fh: u64, offset: u64, len: usize) -> StorageResult<Vec<u8>>;
    async fn write(&self, ino: u64, fh: u64, offset: u64, buf: &Vec<u8>) -> StorageResult<()>;
    async fn truncate(&self, ino: u64, old_size: u64, new_size: u64) -> StorageResult<()>;
    async fn flush(&self, _ino: u64, fh: u64);
    async fn close(&self, fh: u64);
}

#[derive(Debug, Hash, Clone, PartialEq, Eq)]
pub struct CacheKey {
    pub ino: u64,
    pub block_id: u64,
}
