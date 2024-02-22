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
    for i in 0..256 {
        storage.write(10, fh, i * BLOCK_SIZE as u64, &content).await;
    }
    storage.flush(10, fh).await;
    let end = std::time::Instant::now();
    storage.close(fh).await;
    println!("write Time: {:?}", end - start);

    let flag = OpenFlag::Read;
    let fh = storage.open(10, flag);
    // Warm up
    for i in 0..256 {
        let buf = storage
            .read(10, fh, i * BLOCK_SIZE as u64, BLOCK_SIZE)
            .await;
        assert_eq!(buf.len(), BLOCK_SIZE);
    }
    println!("Warm up finish");
    let start = std::time::Instant::now();

    for i in 0..256 {
        let buf = storage
            .read(10, fh, i * BLOCK_SIZE as u64, BLOCK_SIZE)
            .await;
        assert_eq!(buf.len(), BLOCK_SIZE);
    }
    let end = std::time::Instant::now();
    storage.close(fh).await;
    println!("read Time: {:?}", end - start);

    Ok(())
}
