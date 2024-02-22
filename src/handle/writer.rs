use std::sync::Arc;

use bytes::Bytes;
use parking_lot::{Mutex, RwLock};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::task::JoinHandle;

use crate::backend::Backend;
use crate::block::{format_path, Block};
use crate::block_slice::BlockSlice;
use crate::lru::LruPolicy;
use crate::mock_io::CacheKey;
use crate::CacheManager;

#[derive(Debug)]
pub struct Writer {
    ino: u64,
    cache: Arc<Mutex<CacheManager<CacheKey, LruPolicy<CacheKey>>>>,
    backend: Arc<Backend>,
    write_back_sender: Sender<Arc<Task>>,
    write_back_handle: Option<JoinHandle<()>>,
}

enum Task {
    Pending(Arc<WriteTask>),
    Finish,
}

struct WriteTask {
    cache: Arc<Mutex<CacheManager<CacheKey, LruPolicy<CacheKey>>>>,
    backend: Arc<Backend>,
    ino: u64,
    block_id: u64,
    block: Arc<RwLock<Block>>,
}

async fn write_back_block(task: Arc<WriteTask>) {
    let path = format!("{}-{}", task.ino, task.block_id);
    loop {
        println!("Write back block");
        let (content, version) = {
            let block = task.block.read();
            let content = Bytes::copy_from_slice(block.as_ref());
            let version = block.version();
            (content, version)
        };

        task.backend.store(&path, &content).await;
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
    let mut interval = tokio::time::interval(std::time::Duration::from_millis(200));
    let mut tasks = Vec::new();
    loop {
        tokio::select! {
            Some(task) = write_back_receiver.recv() => {
                match task.as_ref() {
                    Task::Pending(task) => {
                        println!("Receive a pending task");
                        tasks.push(task.clone());
                    }
                    Task::Finish => {
                        println!("Receive a finish task");
                        write_blocks(&tasks).await;
                        tasks.clear();
                        return;
                    }
                }
            }
            _ = interval.tick() => {
                println!("Interval expired");
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
        backend: Arc<Backend>,
    ) -> Self {
        let (tx, rx) = tokio::sync::mpsc::channel(100);
        let mut writer = Writer {
            ino,
            cache,
            backend,
            write_back_sender: tx,
            write_back_handle: None,
        };
        let handle = tokio::spawn(write_back_work(rx));
        writer.write_back_handle = Some(handle);
        writer
    }

    pub async fn fetch_block(&self, block_id: u64) -> Arc<RwLock<Block>> {
        let key = CacheKey {
            ino: self.ino,
            block_id,
        };

        let (block, not_cached) = {
            let mut cache = self.cache.lock();
            let block = cache.fetch(&key);
            match block {
                Some(block) => {
                    println!("The block is already cached");
                    (block, false)
                }
                None => {
                    println!("The block is not cached , create a new one");
                    let new_block = cache.new_block(&key).unwrap();
                    (new_block, true)
                }
            }
        };

        if not_cached {
            let path = format_path(block_id, self.ino);
            // There is a gap between the block is created and the content is read from the
            // backend. But according to the current design, concurrency
            // read/write is not supported.
            let mut block = block.write();
            let size = self.backend.read(&path, &mut block).await;
            println!("Read from backend, size: {}", size);
        }
        block
    }

    pub async fn write(&self, buf: Bytes, slices: &[BlockSlice]) {
        for slice in slices {
            let block_id = slice.block_id();
            let block = self.fetch_block(block_id).await;
            {
                let mut block = block.write();
                block.set_dirty(true);
                block[0..buf.len()].copy_from_slice(&buf);
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

    pub async fn flush(&mut self) {
        self.write_back_sender
            .send(Arc::new(Task::Finish))
            .await
            .unwrap();
        self.write_back_handle.take().unwrap().await.unwrap();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::backend::tmp_fs_backend;
    use crate::block::BLOCK_SIZE;

    #[tokio::test]
    async fn test_writer() {
        let backend = Arc::new(tmp_fs_backend());
        let manger = CacheManager::<CacheKey, LruPolicy<CacheKey>>::new(10);
        let mut writer = Writer::new(1, manger.clone(), backend);
        let content = Bytes::from_static(&[b'1'; BLOCK_SIZE]);
        let slice = BlockSlice::new(0, 0, content.len() as u64);
        writer.write(content, &[slice]).await;
        writer.flush().await;
    }
}