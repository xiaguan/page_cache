use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use smallvec::SmallVec;

#[async_trait]
pub trait MockIO {
    // 可以换成 BytesMut
    async fn read(&self, ino: u64, offset: u64, buf: BytesMut) -> u32;
    // 可以换成Bytes
    async fn write(&self, ino: u64, offset: u64, buf: Bytes) -> u32;
}

#[derive(Debug, Hash, Clone, PartialEq, Eq)]
pub struct CacheKey {
    pub ino: u64,
    pub block_id: u64,
}
