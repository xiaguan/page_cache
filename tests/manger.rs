use bytes::Bytes;
use page_cache::block::{Block, BLOCK_SIZE};
use page_cache::lru::LruPolicy;
use page_cache::CacheManager;

#[test]
fn sample_test() {
    let cache_size = 10;
    let content = Bytes::from_static(&[0; BLOCK_SIZE]);

    let manger = CacheManager::<usize, LruPolicy<usize>>::new(cache_size);

    {
        let block_0 = manger.lock().new_block(&0).unwrap();
        let mut block_0 = block_0.write();
        block_0.copy_from_slice(&content);
        block_0.set_dirty(true);
    }

    {
        // Check if block_0's content is correct.
        let mut manger = manger.lock();
        let block_0 = manger.fetch(&0).unwrap();
        {
            let block_0 = block_0.read();
            assert_eq!(block_0.as_ref(), &content);
            assert_eq!(block_0.pin_count(), 2);
        }
        manger.unpin(&0);
        assert_eq!(block_0.read().pin_count(), 1);
    }

    // Create blocks [1,2,3,4,5,6,7,8,9]
    for i in 1..cache_size {
        let block = manger.lock().new_block(&i).unwrap();
        let mut block = block.write();
        block.copy_from_slice(&content);
    }

    // Now the cache is full and all blocks are pinned.
    // We can't create a new block.
    {
        assert!(manger.lock().new_block(&cache_size).is_none());
    }

    {
        let mut manger = manger.lock();
        manger.unpin(&1);
    }

    // This would evict block 1 and create a new block.
    assert!(manger.lock().new_block(&cache_size).is_some());
    {
        // Try get block 1
        assert!(manger.lock().fetch(&1).is_none());
    }
}
