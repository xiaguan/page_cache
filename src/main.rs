use std::sync::Arc;

use bytes::Bytes;
use page_cache::backend::backend::memory_backend;
use page_cache::block::BLOCK_SIZE;
use page_cache::handle::writer::Writer;
use page_cache::lru::LruPolicy;
use page_cache::mock_io::CacheKey;
use page_cache::CacheManager;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let backend = Arc::new(memory_backend());
    let manger = CacheManager::<CacheKey, LruPolicy<CacheKey>>::new(10);
    let mut writer = Writer::new(1, manger.clone(), backend);
    let content = Bytes::from_static(&[b'a'; BLOCK_SIZE]);
    let slice = page_cache::block_slice::BlockSlice::new(0, 0, content.len() as u64);
    let start = std::time::Instant::now();
    writer.write(content, &[slice]).await;
    let end = std::time::Instant::now();
    println!("write time: {:?}", end - start);
    writer.flush().await;
    Ok(())
}
