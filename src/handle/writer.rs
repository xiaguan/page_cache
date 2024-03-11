use std::sync::Arc;

use bytes::Bytes;
use parking_lot::{Mutex, RwLock};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::task::JoinHandle;

use crate::backend::Backend;
use crate::block::{format_path, Block, BLOCK_SIZE};
use crate::block_slice::{offset_to_slice, BlockSlice};
use crate::lru::LruPolicy;
use crate::mock_io::CacheKey;
use crate::CacheManager;

#[derive(Debug)]
pub struct Writer {
    ino: u64,
    cache: Arc<Mutex<CacheManager<CacheKey, LruPolicy<CacheKey>>>>,
    backend: Arc<dyn Backend>,
    write_back_sender: Sender<Arc<Task>>,
    write_back_handle: tokio::sync::Mutex<Option<JoinHandle<()>>>,
    access_keys: Mutex<Vec<CacheKey>>,
}

enum Task {
    Pending(Arc<WriteTask>),
    Flush,
    Finish,
}

struct WriteTask {
    cache: Arc<Mutex<CacheManager<CacheKey, LruPolicy<CacheKey>>>>,
    backend: Arc<dyn Backend>,
    ino: u64,
    block_id: u64,
    block: Arc<RwLock<Block>>,
}

async fn write_back_block(task: Arc<WriteTask>) {
    let path = format_path(task.block_id, task.ino);
    loop {
        let (content, version) = {
            let block = task.block.read();
            let content = Bytes::copy_from_slice(block.as_ref());
            let version = block.version();
            (content, version)
        };

        task.backend.store(&path, &content).await.unwrap();
        {
            let mut block = task.block.write();
            // Check version
            if block.version() != version {
                println!(
                    "Version mismatch previous: {}, current: {}",
                    version,
                    block.version()
                );
                continue;
            }
            block.set_dirty(false);
        }
        {
            task.cache.lock().unpin(&CacheKey {
                ino: task.ino,
                block_id: task.block_id,
            });
            break;
        }
    }
}

async fn write_blocks(tasks: &Vec<Arc<WriteTask>>) {
    let mut handles = Vec::new();
    for task in tasks {
        let handle = tokio::spawn(write_back_block(task.clone()));
        handles.push(handle);
    }
    for handle in handles {
        handle.await.unwrap();
    }
}

async fn write_back_work(mut write_back_receiver: Receiver<Arc<Task>>) {
    //  Create a timer to flush the cache every 200ms.
    let mut interval = tokio::time::interval(std::time::Duration::from_millis(500));
    let mut tasks = Vec::new();
    loop {
        tokio::select! {
            Some(task) = write_back_receiver.recv() => {
                match task.as_ref() {
                    Task::Pending(task) => {
                        tasks.push(task.clone());
                        if tasks.len() >= 10 {
                            write_blocks(&tasks).await;
                            tasks.clear();
                        }
                    }
                    Task::Flush => {
                        write_blocks(&tasks).await;
                        tasks.clear();
                    }
                    Task::Finish => {
                        write_blocks(&tasks).await;
                        tasks.clear();
                        return;
                    }
                }
            }
            _ = interval.tick() => {
                write_blocks(&tasks).await;
                tasks.clear();
            }
        }
    }
}

impl Writer {
    pub fn new(
        ino: u64,
        cache: Arc<Mutex<CacheManager<CacheKey, LruPolicy<CacheKey>>>>,
        backend: Arc<dyn Backend>,
    ) -> Self {
        let (tx, rx) = tokio::sync::mpsc::channel(100);
        let mut writer = Writer {
            ino,
            cache,
            backend,
            write_back_sender: tx,
            write_back_handle: tokio::sync::Mutex::new(None),
            access_keys: Mutex::new(Vec::new()),
        };
        let handle = tokio::spawn(write_back_work(rx));
        writer.write_back_handle = tokio::sync::Mutex::new(Some(handle));
        writer
    }

    fn access(&self, block_id: u64) {
        let key = CacheKey {
            ino: self.ino,
            block_id,
        };
        let mut access_keys = self.access_keys.lock();
        access_keys.push(key);
    }

    pub async fn fetch_block(&self, block_id: u64) -> Arc<RwLock<Block>> {
        let key = CacheKey {
            ino: self.ino,
            block_id,
        };

        {
            let cache = self.cache.lock();
            if let Some(block) = cache.fetch(&key) {
                return block;
            }
        }

        let path = format_path(block_id, self.ino);
        // There is a gap between the block is created and the content is read from the
        // backend. But according to the current design, concurrency
        // read/write is not supported.
        let mut buf = Vec::with_capacity(BLOCK_SIZE);
        self.backend.read(&path, &mut buf).await.unwrap();
        let block = {
            let mut cache = self.cache.lock();
            let block = cache.new_block(&key, &buf).unwrap();
            block
        };

        block
    }

    pub async fn write(&self, buf: &[u8], slices: &[BlockSlice]) {
        let mut consume_index = 0;
        for slice in slices {
            let block_id = slice.block_id();
            let write_content = &buf[consume_index..consume_index + slice.size() as usize];
            self.access(block_id);
            let block = self.fetch_block(block_id).await;
            {
                let mut block = block.write();
                block.set_dirty(true);
                let start = slice.offset() as usize;
                let end = start + slice.size() as usize;
                block[start..end].copy_from_slice(write_content);
                consume_index += slice.size() as usize;
                block.inc_version();
            }
            let task = Arc::new(WriteTask {
                cache: self.cache.clone(),
                backend: self.backend.clone(),
                ino: self.ino,
                block_id,
                block,
            });
            self.write_back_sender
                .send(Arc::new(Task::Pending(task)))
                .await
                .unwrap();
        }
    }

    pub async fn flush(&self) {
        self.write_back_sender
            .send(Arc::new(Task::Flush))
            .await
            .unwrap();
    }

    pub async fn extend(&self, old_size: u64, new_size: u64) {
        let slices = offset_to_slice(BLOCK_SIZE as u64, old_size, new_size - old_size);
        for slice in slices {
            let buf = vec![0u8; slice.size() as usize];
            self.write(&buf, &[slice]).await;
        }
    }

    pub async fn close(&self) {
        self.write_back_sender
            .send(Arc::new(Task::Finish))
            .await
            .unwrap();
        self.flush().await;
        self.write_back_handle
            .lock()
            .await
            .take()
            .unwrap()
            .await
            .unwrap();
        let keys = self.access_keys.lock();
        for key in keys.iter() {
            self.cache.lock().remove(key);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::backend::memory_backend;

    const IO_SIZE: usize = 128 * 1024;
    #[tokio::test]
    async fn test_writer() {
        let backend = Arc::new(memory_backend());
        let manger = CacheManager::<CacheKey, LruPolicy<CacheKey>>::new(10);
        let writer = Writer::new(1, manger.clone(), backend);
        let content = Bytes::from_static(&[b'1'; IO_SIZE]);
        let slice = BlockSlice::new(0, 0, content.len() as u64);
        writer.write(&content, &[slice]).await;
        let memory_size = manger.lock().len();
        assert_eq!(memory_size, 1);
        writer.close().await;
        let memory_size = manger.lock().len();
        assert_eq!(memory_size, 0);
    }
}
