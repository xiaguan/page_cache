use std::collections::VecDeque;
use std::fmt::Debug;
use std::sync::Arc;

use block::{Block, BLOCK_SIZE};
use hashbrown::HashMap;
use parking_lot::{Mutex, RwLock};
use tracing::warn;

pub mod backend;
pub mod block;
pub mod block_slice;

pub mod handle;
pub mod lru;
pub mod mock_io;
pub mod policy;
pub mod storage;

#[derive(Debug)]
pub struct CacheManager<K, P>
where
    K: Eq + std::hash::Hash + Clone,
    P: policy::EvictPolicy<K>,
{
    policy: P,
    map: HashMap<K, Arc<RwLock<Block>>>,
    free_list: VecDeque<Arc<RwLock<Block>>>,
}

impl<K, P> CacheManager<K, P>
where
    K: Eq + std::hash::Hash + Clone + Debug,
    P: policy::EvictPolicy<K>,
{
    pub fn new(capacity: usize) -> Arc<Mutex<Self>> {
        let mut free_list = VecDeque::with_capacity(capacity);
        for _ in 0..capacity {
            let block = Arc::new(RwLock::new(Block::new(vec![0; BLOCK_SIZE])));
            free_list.push_back(block);
        }
        Arc::new(Mutex::new(CacheManager {
            policy: P::new(capacity),
            map: HashMap::with_capacity(capacity),
            free_list,
        }))
    }

    fn get_free_block(&mut self, key: &K) -> Option<Arc<RwLock<Block>>> {
        // If the queue is empty, `pop_front` returns `None`.
        let new_block = self.free_list.pop_front()?;
        // Access the policy to update the internal state.
        self.map.insert(key.clone(), new_block.clone());
        self.policy.access(key);
        new_block.write().pin();
        Some(new_block)
    }

    /// Create a new block from the cache manager's free list.
    /// Set the block's key to the given key.
    pub fn new_block(&mut self, key: &K) -> Option<Arc<RwLock<Block>>> {
        // 1. Get a free block from the free list.
        // 2. The free list is empty, evict a block from the cache.
        // 3. If the cache is full of non-evictable blocks, return None.
        if let Some(new_block) = self.get_free_block(key) {
            println!("Successfully created a new block from free list");
            return Some(new_block);
        } else {
            let evict_key = self.policy.evict();
            if evict_key.is_none() {
                warn!("The cache is full of non-evictable blocks");
                return None;
            }
            let evict_key = evict_key?;
            let evict_block = self.map.remove(&evict_key).unwrap();
            println!("Get a evict block");
            assert!(evict_block.read().pin_count() == 0);
            assert!(!evict_block.read().dirty());
            self.map.insert(key.clone(), evict_block.clone());
            self.policy.access(key);
            evict_block.write().pin();
            println!("Successfully created a new block by evicting a block from the cache");
            Some(evict_block)
        }
    }

    /// Decrement the pin count of the block associated with the given key.
    /// If the pin count reaches 0, set the block as evictable.
    pub fn unpin(&mut self, key: &K) {
        // If a block is pinned, it must exist in the map.
        let block_ref = self.map.get(key).unwrap();
        let mut block = block_ref.write();
        block.unpin();
        if block.pin_count() == 0 {
            assert!(block.dirty() == false);
            self.policy.set_evictable(key, true);
        }
    }

    pub fn fetch(&self, key: &K) -> Option<Arc<RwLock<Block>>> {
        let block = self.map.get(key)?.clone();
        self.policy.access(key);
        block.write().pin();
        Some(block)
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn evict(&mut self) {
        loop {
            let key = self.policy.evict();
            if let Some(key) = key {
                let block = self.map.remove(&key).unwrap();
                assert!(block.read().pin_count() == 0);
                assert!(!block.read().dirty());
                println!("Evict block");
            } else {
                break;
            }
        }
    }

    pub fn remove(&mut self, key: &K) -> bool {
        // 尝试从map中移除key，并立即处理None情况
        let block_ref: Arc<RwLock<Block>> = match self.map.remove(key) {
            Some(block) => block,
            None => return false,
        };

        // 获取写锁
        let mut block = block_ref.write();

        // 检查是否满足移除条件
        if block.pin_count() != 0 {
            // 如果不满足，将block_ref重新插入map，并返回失败
            self.map.insert(key.clone(), block_ref.clone());
            return false;
        }

        // 确保block不是dirty的
        assert!(!block.dirty());

        // 清理block并执行后续操作
        block.clear();
        self.free_list.push_back(block_ref.clone());
        self.policy.remove(key);

        true // 成功移除
    }
}
