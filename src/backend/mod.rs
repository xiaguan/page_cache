/// The general backend implementation
pub mod backend_impl;
/// The memory backend implementation
pub mod memory_backend;

use std::fmt::Debug;

use async_trait::async_trait;
pub use backend_impl::BackendImpl;

use crate::error::StorageResult;

/// The `Backend` trait represents a backend storage system.
#[async_trait]
pub trait Backend: Debug + Send + Sync {
    /// Reads data from the storage system into the given buffer.
    async fn read(&self, path: &str, buf: &mut [u8]) -> StorageResult<usize>;
    /// Stores data from the given buffer into the storage system.
    async fn store(&self, path: &str, buf: &[u8]) -> StorageResult<()>;
    /// Removes the data from the storage system.
    async fn remove(&self, path: &str) -> StorageResult<()>;
}
