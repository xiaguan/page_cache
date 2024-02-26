use std::sync::Arc;

use async_trait::async_trait;
use dashmap::DashMap;

use super::Backend;
use crate::error::StorageResult;
// 实现一个内存中的 Backend
#[derive(Debug, Clone)]
pub struct MemoryBackend {
    map: Arc<DashMap<String, Vec<u8>>>,
}

#[async_trait]
impl Backend for MemoryBackend {
    async fn read(&self, path: &str, buf: &mut [u8]) -> StorageResult<usize> {
        let data = self.map.get(path);
        if data.is_none() {
            return Ok(0);
        }
        let data = data.unwrap();
        let len = data.len().min(buf.len());
        buf[..len].copy_from_slice(&data[..len]);
        Ok(len)
    }

    async fn store(&self, path: &str, buf: &[u8]) -> StorageResult<()> {
        self.map.insert(path.to_owned(), buf.to_vec());
        Ok(())
    }

    async fn remove(&self, path: &str) -> StorageResult<()> {
        self.map.remove(path);
        Ok(())
    }
}

impl MemoryBackend {
    pub fn new() -> Self {
        MemoryBackend {
            map: Arc::new(DashMap::new()),
        }
    }
}
