use std::sync::Arc;

use page_cache::backend::backend::memory_backend;
use page_cache::block::BLOCK_SIZE;
use page_cache::handle::handle::OpenFlag;
use page_cache::lru::LruPolicy;
use page_cache::mock_io::{CacheKey, MockIO};
use page_cache::storage::Storage;
use page_cache::CacheManager;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let backend = Arc::new(memory_backend());
    let manger = CacheManager::<CacheKey, LruPolicy<CacheKey>>::new(10000);
    let storage = Arc::new(Storage::new(manger, backend));
    let content = vec![b'1'; BLOCK_SIZE];
    let flag = OpenFlag::Write;
    let start = std::time::Instant::now();
    let fh = storage.open(10, flag);
    for i in 0..2 {
        storage.write(10, fh, i * BLOCK_SIZE as u64, &content).await;
    }
    storage.flush(10, fh).await;
    let end = std::time::Instant::now();
    println!("write 256 blocks cost {:?}", end - start);
    // Read
    let start = std::time::Instant::now();
    let fh = storage.open(10, OpenFlag::Read);
    for i in 0..2 {
        let buf = storage
            .read(10, fh, i * BLOCK_SIZE as u64, BLOCK_SIZE)
            .await;
        assert_eq!(buf.len(), BLOCK_SIZE);
    }
    let end = std::time::Instant::now();
    println!("read 256 blocks cost {:?}", end - start);
    Ok(())
}
