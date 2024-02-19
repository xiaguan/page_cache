use bytes::{Bytes, BytesMut};
use parking_lot::RwLock;

pub const BLOCK_SIZE: usize = 512 * 1024;

pub struct Block {
    data: Vec<u8>,
    pin_count: u32,
    dirty: bool,
}

impl Block {
    pub fn new(data: Vec<u8>) -> Self {
        debug_assert!(data.len() == BLOCK_SIZE);
        Block {
            data,
            pin_count: 0,
            dirty: false,
        }
    }

    pub fn pin_count(&self) -> u32 {
        self.pin_count
    }

    pub fn dirty(&self) -> bool {
        self.dirty
    }

    pub fn set_dirty(&mut self, dirty: bool) {
        self.dirty = dirty;
    }

    pub fn pin(&mut self) {
        self.pin_count += 1;
    }

    pub fn unpin(&mut self) {
        assert!(self.pin_count > 0);
        self.pin_count -= 1;
    }

    pub fn clear(&mut self) {
        self.dirty = false;
        self.pin_count = 0;
    }
}

impl std::ops::Deref for Block {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

impl std::ops::DerefMut for Block {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.data
    }
}
