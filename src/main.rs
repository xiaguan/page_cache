use std::sync::Arc;

use page_cache::backend::backend::memory_backend;
use page_cache::backend::memory_backend::MemoryBackend;
use page_cache::block::BLOCK_SIZE;
use page_cache::handle::handle::OpenFlag;
use page_cache::lru::LruPolicy;
use page_cache::mock_io::{CacheKey, MockIO};
use page_cache::storage::Storage;
use page_cache::CacheManager;

async fn seq_read(storage: Arc<Storage>, ino: u64) {
    let flag = OpenFlag::Read;
    let fh = storage.open(ino, flag);
    for i in 0..256 {
        let random_block_id = rand::random::<u64>() % 256;
        let buf = storage
            .read(10, fh, random_block_id * BLOCK_SIZE as u64, BLOCK_SIZE)
            .await
            .unwrap();
        assert_eq!(buf.len(), BLOCK_SIZE);
    }
}

async fn concurrency_read() {
    let backend = Arc::new(MemoryBackend::new());
    let manger = CacheManager::<CacheKey, LruPolicy<CacheKey>>::new(300);
    let storage = Arc::new(Storage::new(manger, backend));
    // First write
    let content = vec![b'1'; BLOCK_SIZE];
    let flag = OpenFlag::Write;
    {
        let start = std::time::Instant::now();
        let fh = storage.open(10, flag);
        for i in 0..256 {
            storage
                .write(10, fh, i * BLOCK_SIZE as u64, &content)
                .await
                .unwrap();
        }
        storage.flush(10, fh).await;
        storage.close(fh).await;
        let end = std::time::Instant::now();
        println!("write Time: {:?}", end - start);
    }
    // Warm up
    for _ in 0..10 {
        seq_read(storage.clone(), 10).await;
    }
    // Concurrency read ,thread num : 1,2,4,8
    for i in 0..6 {
        let mut tasks = vec![];
        let start = tokio::time::Instant::now();
        for _ in 0..2_usize.pow(i) {
            let storage = storage.clone();
            tasks.push(tokio::spawn(async move {
                seq_read(storage, 10).await;
            }));
        }
        for task in tasks {
            task.await.unwrap();
        }
        let end = tokio::time::Instant::now();
        // throuput = 1GB/ time * thread num
        let throuput = 1.0 / (end - start).as_secs_f64() * 2_usize.pow(i) as f64;
        println!(
            "thread num : {}, read Time: {:?} thoughput: {} GB/s",
            2_usize.pow(i),
            end - start,
            throuput
        );
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    concurrency_read().await;
    Ok(())
}
