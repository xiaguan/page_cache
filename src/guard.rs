use std::hash::Hash;
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Weak};

use parking_lot::{Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::block::Block;
use crate::CacheManager;

type GuardTarget = Block;

pub struct ReadGuard<'a, K, P>
where
    K: Eq + Hash + Clone,
    P: crate::policy::EvictPolicy<K>,
{
    guard: RwLockReadGuard<'a, GuardTarget>,
    key: K,
    manger: Weak<Mutex<CacheManager<K, P>>>,
}

impl<'a, K, P> Deref for ReadGuard<'a, K, P>
where
    K: Eq + Hash + Clone,
    P: crate::policy::EvictPolicy<K>,
{
    type Target = Block;

    fn deref(&self) -> &Self::Target {
        &*self.guard
    }
}

impl<'a, K, P> ReadGuard<'a, K, P>
where
    K: Eq + Hash + Clone,
    P: crate::policy::EvictPolicy<K>,
{
    pub fn new(
        guard: RwLockReadGuard<'a, GuardTarget>,
        key: K,
        manger: Weak<Mutex<CacheManager<K, P>>>,
    ) -> Self {
        ReadGuard { guard, key, manger }
    }
}

// Impl drop

pub struct WriteGuard<'a, K, P>
where
    K: Eq + Hash + Clone,
    P: crate::policy::EvictPolicy<K>,
{
    guard: RwLockWriteGuard<'a, GuardTarget>,
    key: K,
    manger: Weak<Mutex<CacheManager<K, P>>>,
}

impl<'a, K, P> Deref for WriteGuard<'a, K, P>
where
    K: Eq + Hash + Clone,
    P: crate::policy::EvictPolicy<K>,
{
    type Target = Block;

    fn deref(&self) -> &Self::Target {
        &*self.guard
    }
}

impl<'a, K, P> DerefMut for WriteGuard<'a, K, P>
where
    K: Eq + Hash + Clone,
    P: crate::policy::EvictPolicy<K>,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        // Mark dirty
        self.guard.set_dirty(true);
        &mut *self.guard
    }
}

impl<'a, K, P> WriteGuard<'a, K, P>
where
    K: Eq + Hash + Clone,
    P: crate::policy::EvictPolicy<K>,
{
    pub fn new(
        guard: RwLockWriteGuard<'a, GuardTarget>,
        key: K,
        manger: Weak<Mutex<CacheManager<K, P>>>,
    ) -> Self {
        WriteGuard { guard, key, manger }
    }
}
