use std::sync::Arc;

use parking_lot::Mutex;

use crate::backend::Backend;
use crate::lru::LruPolicy;
use crate::CacheManager;

enum HandleTyep {
    Read,
    Write,
    ReadAndWrite,
}

pub struct Storage {
    cache: Arc<Mutex<CacheManager<u128, LruPolicy<u128>>>>,
    backend: Backend,
}
