pub const BLOCK_SIZE: usize = 4 * 1024 * 1024;

#[derive(Debug)]
pub struct Block {
    data: Vec<u8>,
    pin_count: u32,
    dirty: bool,
    version: usize,
}

impl Block {
    pub fn new(data: Vec<u8>) -> Self {
        debug_assert!(data.len() == BLOCK_SIZE);
        Block {
            data,
            pin_count: 0,
            dirty: false,
            version: 0,
        }
    }

    pub fn version(&self) -> usize {
        self.version
    }

    pub fn inc_version(&mut self) {
        self.version += 1;
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
        // for test, when something is equal to 66, it means it's cleared.
        self.data[0] = 0;
        self.data[1] = 0;
        self.dirty = false;
        self.pin_count = 0;
        self.version = 0;
    }
}

pub fn format_path(block_id: u64, ino: u64) -> String {
    format!("{}-{}", ino, block_id)
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
