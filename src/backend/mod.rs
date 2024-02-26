pub mod backend;
pub mod memory_backend;

use std::fmt::Debug;

use async_trait::async_trait;
pub use backend::BackendImpl;

use crate::error::StorageResult;

#[async_trait]
pub trait Backend: Debug + Send + Sync {
    async fn read(&self, path: &str, buf: &mut [u8]) -> StorageResult<usize>;
    async fn store(&self, path: &str, buf: &[u8]) -> StorageResult<()>;
    async fn remove(&self, path: &str) -> StorageResult<()>;
}
