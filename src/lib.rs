use std::collections::VecDeque;
use std::sync::mpsc::Sender;
use std::sync::{Arc, Weak};

use block::{Block, BLOCK_SIZE};
use guard::{ReadGuard, WriteGuard};
use hashbrown::HashMap;
use parking_lot::{Mutex, RwLock};
use tracing::warn;

pub mod block;
pub mod guard;
pub mod lru;
pub mod policy;

pub struct CacheManager<K, P>
where
    K: Eq + std::hash::Hash + Clone,
    P: policy::EvictPolicy<K>,
{
    policy: P,
    map: HashMap<K, Arc<RwLock<Block>>>,
    free_list: VecDeque<Vec<u8>>,
    weak_self: Weak<Mutex<Self>>,
    tx: Sender<(K, Arc<RwLock<Block>>)>,
}

impl<K, P> CacheManager<K, P>
where
    K: Eq + std::hash::Hash + Clone,
    P: policy::EvictPolicy<K>,
{
    pub fn new(capacity: usize, tx: Sender<(K, Arc<RwLock<Block>>)>) -> Arc<Mutex<Self>> {
        let manager = Arc::new(Mutex::new(CacheManager {
            policy: P::new(capacity),
            map: HashMap::with_capacity(capacity),
            free_list: VecDeque::from(vec![vec![0; BLOCK_SIZE]; capacity]),
            weak_self: Weak::new(),
            tx,
        }));
        manager.lock().weak_self = Arc::downgrade(&manager);
        manager
    }

    fn get_free_block(&mut self, key: &K) -> Option<Arc<RwLock<Block>>> {
        // If the queue is empty, `pop_front` returns `None`.
        let new_block = self
            .free_list
            .pop_front()
            .map(|data| Arc::new(RwLock::new(Block::new(data))))?;
        // Access the policy to update the internal state.
        self.policy.access(key);
        self.map.insert(key.clone(), new_block.clone());
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
            let evict_key = self.policy.evict()?;
            let evict_result = self.map.remove(&evict_key);
            if evict_result.is_none() {
                // Print a warning message.
                warn!("In memory cache manager is full and no evictable block found");
            }
            let evict_block = evict_result?;
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
        if block.pin_count() == 1 && block.dirty() {
            // Send to write back thread.
            println!("Sending block to write back thread in unpin");
            let _ = self.tx.send((key.clone(), block_ref.clone()));
            return;
        }
        block.unpin();
        if block.pin_count() == 0 {
            println!("Block is now evictable");
            self.policy.set_evictable(key, true);
        }
    }

    pub fn read(&self, key: &K) -> Option<ReadGuard<K, P>> {
        let guard = self.map.get(key)?.read();
        Some(guard::ReadGuard::new(
            guard,
            key.clone(),
            self.weak_self.clone(),
        ))
    }

    pub fn write(&self, key: &K) -> Option<WriteGuard<K, P>> {
        let block = self.map.get(key)?;
        // Send to write back thread.
        let _ = self.tx.send((key.clone(), block.clone()));
        // Pin the block for write back, after write back the block will be unpinned.
        block.write().pin();
        let guard = block.write();
        Some(guard::WriteGuard::new(
            guard,
            key.clone(),
            self.weak_self.clone(),
        ))
    }
}
