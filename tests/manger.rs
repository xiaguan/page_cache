use bytes::Bytes;
use page_cache::block::{Block, BLOCK_SIZE};
use page_cache::lru::LruPolicy;
use page_cache::CacheManager;

#[test]
fn sample_test() {
    let cache_size = 10;
    let content = Bytes::from_static(&[0; BLOCK_SIZE]);

    let (tx, rx) =
        std::sync::mpsc::channel::<(usize, std::sync::Arc<parking_lot::RwLock<Block>>)>();

    let manger = CacheManager::<usize, LruPolicy<usize>>::new(cache_size, tx.clone());
    let weak = std::sync::Arc::downgrade(&manger);

    // Create a write back thread
    let handle = std::thread::spawn(move || {
        while let Ok((key, block)) = rx.recv() {
            if key == usize::MAX {
                return;
            }
            println!("Write back block {}", key);
            {
                let mut block = block.write();
                // Clear dirty
                block.set_dirty(false);
            }
            weak.upgrade().unwrap().lock().unpin(&key);
        }
    });

    {
        let block_0 = manger.lock().new_block(&0).unwrap();
        let mut block_0 = block_0.write();
        block_0.copy_from_slice(&content);
        block_0.set_dirty(true);
    }

    {
        // Check if block_0's content is correct.
        let manger = manger.lock();
        let block_0 = manger.read(&0).unwrap();
        assert_eq!(block_0.as_ref(), &content);
    }

    for i in 1..cache_size {
        let block = manger.lock().new_block(&i).unwrap();
        let mut block = block.write();
        block.copy_from_slice(&content);
    }

    println!("Full");
    // Now the cache is full and all blocks are pinned.
    // We can't create a new block.
    {
        assert!(manger.lock().new_block(&cache_size).is_none());
    }

    // Unpin pages [0,1,2,3,4]
    for i in 0..5 {
        manger.lock().unpin(&i);
    }

    assert!(manger.lock().new_block(&cache_size).is_some());

    tx.send((
        usize::MAX,
        std::sync::Arc::new(parking_lot::RwLock::new(Block::new(vec![0; BLOCK_SIZE]))),
    ))
    .unwrap();
    handle.join().unwrap();
}
