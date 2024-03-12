use std::sync::Arc;

use async_trait::async_trait;
use dashmap::DashMap;

use super::Backend;
use crate::error::StorageResult;
/// A memory backend for testing purposes.
#[derive(Debug, Clone)]
pub struct MemoryBackend {
    map: Arc<DashMap<String, Vec<u8>>>,
    // mock latency : ms
    latency: u64,
}

#[async_trait]
impl Backend for MemoryBackend {
    #[inline]
    async fn read(&self, path: &str, buf: &mut [u8]) -> StorageResult<usize> {
        // mock latency
        tokio::time::sleep(tokio::time::Duration::from_millis(self.latency)).await;

        let data = self.map.get(path);
        if data.is_none() {
            return Ok(0);
        }
        let data = data.unwrap();
        let len = data.len().min(buf.len());
        buf[..len].copy_from_slice(&data[..len]);
        Ok(len)
    }

    #[inline]
    async fn store(&self, path: &str, buf: &[u8]) -> StorageResult<()> {
        // mock latency
        tokio::time::sleep(tokio::time::Duration::from_millis(self.latency)).await;
        self.map.insert(path.to_owned(), buf.to_vec());
        Ok(())
    }

    #[inline]
    async fn remove(&self, path: &str) -> StorageResult<()> {
        // mock latency
        tokio::time::sleep(tokio::time::Duration::from_millis(self.latency)).await;
        self.map.remove(path);
        Ok(())
    }
}

impl MemoryBackend {
    /// Creates a new `MemoryBackend` instance with the given latency.
    #[inline]
    #[must_use]
    pub fn new(latency: u64) -> Self {
        MemoryBackend {
            map: Arc::new(DashMap::new()),
            latency,
        }
    }
}
