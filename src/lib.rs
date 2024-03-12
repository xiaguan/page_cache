//! `DatenLord`

#![deny(
    // The following are allowed by default lints according to
    // https://doc.rust-lang.org/rustc/lints/listing/allowed-by-default.html
    anonymous_parameters,
    bare_trait_objects,
    // box_pointers,
    // elided_lifetimes_in_paths, // allow anonymous lifetime
    // missing_copy_implementations, // Copy may cause unnecessary memory copy
    missing_debug_implementations,
    missing_docs, // TODO: add documents
    single_use_lifetimes, // TODO: fix lifetime names only used once
    trivial_casts, // TODO: remove trivial casts in code
    trivial_numeric_casts,
    // unreachable_pub, allow clippy::redundant_pub_crate lint instead
    // unsafe_code,
    unstable_features,
    unused_extern_crates,
    unused_import_braces,
    unused_qualifications,
    // unused_results, // TODO: fix unused results
    variant_size_differences,

    warnings, // treat all wanings as errors

    clippy::all,
    clippy::restriction,
    clippy::pedantic,
    clippy::cargo
)]
#![allow(
    // Some explicitly allowed Clippy lints, must have clear reason to allow
    clippy::blanket_clippy_restriction_lints, // allow clippy::restriction
    clippy::implicit_return, // actually omitting the return keyword is idiomatic Rust code
    clippy::module_name_repetitions, // repeation of module name in a struct name is not big deal
    clippy::multiple_crate_versions, // multi-version dependency crates is not able to fix
    clippy::panic, // allow debug_assert, panic in production code
    clippy::unreachable,  // Use `unreachable!` instead of `panic!` when impossible cases occurs
    // clippy::panic_in_result_fn,
    clippy::missing_errors_doc, // TODO: add error docs
    clippy::exhaustive_structs,
    clippy::exhaustive_enums,
    clippy::missing_panics_doc, // TODO: add panic docs
    clippy::panic_in_result_fn,
    clippy::single_char_lifetime_names,
    clippy::separated_literal_suffix, // conflict with unseparated_literal_suffix
    clippy::undocumented_unsafe_blocks, // TODO: add safety comment
    clippy::missing_safety_doc, // TODO: add safety comment
    clippy::shadow_unrelated, //it’s a common pattern in Rust code
    clippy::shadow_reuse, //it’s a common pattern in Rust code
    clippy::shadow_same, //it’s a common pattern in Rust code
    clippy::same_name_method, // Skip for protobuf generated code
    clippy::mod_module_files, // TODO: fix code structure to pass this lint
    clippy::std_instead_of_core, // Cause false positive in src/common/error.rs
    clippy::std_instead_of_alloc,
    clippy::pub_use, // TODO: fix this
    clippy::missing_trait_methods, // TODO: fix this
    clippy::arithmetic_side_effects, // TODO: fix this
    clippy::use_debug, // Allow debug print
    clippy::print_stdout, // Allow println!
    clippy::question_mark_used, // Allow ? operator
    clippy::absolute_paths,   // Allow use through absolute paths,like `std::env::current_dir`
    clippy::ref_patterns,    // Allow Some(ref x)
    clippy::single_call_fn,  // Allow function is called only once
    clippy::pub_with_shorthand,  // Allow pub(super)
    clippy::min_ident_chars,  // Allow Err(e)
    clippy::multiple_unsafe_ops_per_block, // Mainly caused by `etcd_delegate`, will remove later
    clippy::impl_trait_in_params,  // Allow impl AsRef<Path>, it's common in Rust
    clippy::missing_assert_message, // Allow assert! without message, mainly in test code
    clippy::semicolon_outside_block, // We need to choose between this and `semicolon_inside_block`, we choose outside
    clippy::similar_names, // Allow similar names, due to the existence of uid and gid
    clippy::as_conversions,
    clippy::clone_on_ref_ptr,
    clippy::integer_division,
    clippy::unwrap_used,
)]

use std::collections::VecDeque;
use std::fmt::Debug;
use std::sync::Arc;

use block::{Block, BLOCK_SIZE};
use hashbrown::HashMap;
use parking_lot::{Mutex, RwLock};
use tracing::warn;

/// Backend related modules
pub mod backend;
/// The define of the block
pub mod block;
/// The define of the block slice
pub mod block_slice;

/// Storage error module
pub mod error;
/// File handle module
pub mod handle;
/// LRU eviction policy module
pub mod lru;
/// The mock IO module
pub mod mock_io;
/// The eviction policy module
pub mod policy;
/// The storage module
pub mod storage;

/// The `CacheManager` struct is used to manage a cache of blocks with a specified capacity.
///
/// It uses an eviction policy to determine which blocks to evict when the cache is full.
#[derive(Debug)]
pub struct CacheManager<K, P>
where
    K: Eq + std::hash::Hash + Clone,
    P: policy::EvictPolicy<K>,
{
    /// The eviction policy used by the cache manager.
    policy: P,
    /// The only place to store the block
    map: HashMap<K, Arc<RwLock<Block>>>,
    /// The free list of blocks
    free_list: VecDeque<Arc<RwLock<Block>>>,
}

impl<K, P> CacheManager<K, P>
where
    K: Eq + std::hash::Hash + Clone + Debug,
    P: policy::EvictPolicy<K>,
{
    /// Creates a new `CacheManager` with the specified capacity.
    #[inline]
    #[must_use]
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

    /// Get a free block from the cache manager's free list.
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
    #[allow(clippy::indexing_slicing)]
    #[allow(clippy::unwrap_used)]
    #[inline]
    pub fn new_block(&mut self, key: &K, data: &[u8]) -> Option<Arc<RwLock<Block>>> {
        // Check if the key is already exist
        if self.map.contains_key(key) {
            // access, then pin,return
            self.policy.access(key);
            let block = self.map.get(key).unwrap().clone();
            block.write().pin();
            return Some(block);
        }
        let new_block = if let Some(new_block) = self.get_free_block(key) {
            Some(new_block)
        } else {
            let evict_key = self.policy.evict();
            if evict_key.is_none() {
                warn!("The cache is full of non-evictable blocks");
                return None;
            }
            let evict_key = evict_key?;
            // It must exist in the map
            let evict_block = self.map.remove(&evict_key).unwrap();
            assert!(evict_block.read().pin_count() == 0);
            assert!(!evict_block.read().dirty());
            self.map.insert(key.clone(), evict_block.clone());
            self.policy.access(key);
            evict_block.write().pin();
            Some(evict_block)
        };
        let new_block = new_block?;
        {
            let mut block = new_block.write();
            block[0..data.len()].copy_from_slice(data);
        }
        Some(new_block)
    }

    /// Decrement the pin count of the block associated with the given key.
    /// If the pin count reaches 0, set the block as evictable.
    #[allow(clippy::unwrap_used)]
    #[inline]
    pub fn unpin(&mut self, key: &K) {
        // If a block is pinned, it must exist in the map.
        let block_ref = self.map.get(key).unwrap();
        let mut block = block_ref.write();
        block.unpin();
        if block.pin_count() == 0 {
            assert!(!block.dirty());
            self.policy.set_evictable(key, true);
        }
    }

    /// Fetches the block associated with the given key from the cache.
    #[inline]
    pub fn fetch(&self, key: &K) -> Option<Arc<RwLock<Block>>> {
        let block = self.map.get(key)?.clone();
        self.policy.access(key);
        {
            let mut block = block.write();
            block.pin();
            assert!(block.pin_count() >= 1);
        }
        Some(block)
    }

    /// Returns the number of blocks in the cache.
    #[inline]
    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        self.map.len()
    }

    /// Evicts blocks from the cache according to the eviction policy.
    #[allow(clippy::unwrap_used)]
    #[inline]
    pub fn evict(&mut self) {
        loop {
            let key = self.policy.evict();
            if let Some(key) = key {
                let block = self.map.remove(&key).unwrap();
                assert!(block.read().pin_count() == 0);
                assert!(!block.read().dirty());
            } else {
                break;
            }
        }
    }

    /// Removes the block associated with the given key from the cache.
    /// Returns `true` if the block was successfully removed, `false` otherwise.
    #[inline]
    pub fn remove(&mut self, key: &K) -> bool {
        // Try to remove the key from the map and immediately handle the None case
        let block_ref: Arc<RwLock<Block>> = match self.map.remove(key) {
            Some(block) => block,
            None => return false,
        };

        // Get a write lock
        let mut block = block_ref.write();

        // Check if the removal condition is satisfied
        if block.pin_count() != 0 {
            // If not satisfied, reinsert block_ref into the map and return failure
            self.map.insert(key.clone(), block_ref.clone());
            return false;
        }

        // Ensure that the block is not dirty
        assert!(!block.dirty());

        // Clean up the block and perform subsequent operations
        block.clear();
        self.free_list.push_back(block_ref.clone());
        self.policy.remove(key);

        true // Successfully removed
    }
}
