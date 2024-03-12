/// The size of a block in bytes.
pub const BLOCK_SIZE: usize = 4 * 1024 * 1024;

/// Represents a block of data.
///
/// A `Block` contains a vector of bytes representing the block data,
/// along with metadata such as pin count, dirty flag, and version.
#[derive(Debug)]
pub struct Block {
    data: Vec<u8>,
    pin_count: u32,
    dirty: bool,
    version: usize,
}

impl Block {
    /// Creates a new `Block` with the given data.
    ///
    /// The length of the provided data must be equal to `BLOCK_SIZE`.
    pub fn new(data: Vec<u8>) -> Self {
        debug_assert!(data.len() == BLOCK_SIZE);
        Block {
            data,
            pin_count: 0,
            dirty: false,
            version: 0,
        }
    }

    /// Returns the current version of the block.
    pub fn version(&self) -> usize {
        self.version
    }

    /// Increments the version of the block.
    pub fn inc_version(&mut self) {
        self.version += 1;
    }

    /// Returns the current pin count of the block.
    pub fn pin_count(&self) -> u32 {
        self.pin_count
    }

    /// Returns whether the block is marked as dirty.
    pub fn dirty(&self) -> bool {
        self.dirty
    }

    /// Sets the dirty flag of the block.
    pub fn set_dirty(&mut self, dirty: bool) {
        self.dirty = dirty;
    }

    /// Increments the pin count of the block.
    pub fn pin(&mut self) {
        self.pin_count += 1;
    }

    /// Decrements the pin count of the block.
    ///
    /// # Panics
    ///
    /// Panics if the pin count is already zero.
    #[inline]
    pub fn unpin(&mut self) {
        assert!(self.pin_count > 0);
        self.pin_count -= 1;
    }

    /// Clears the block data and resets metadata.
    ///
    /// This method sets the first two bytes of the block data to 66 (for testing purposes),
    /// marks the block as not dirty, resets the pin count to zero, and resets the version to zero.
    pub fn clear(&mut self) {
        // for test, when something is equal to 66, it means it's cleared.
        self.data[0] = 66;
        self.data[1] = 66;
        self.dirty = false;
        self.pin_count = 0;
        self.version = 0;
    }
}

/// Formats a block path string given the block ID and inode number.
///
/// The block path is formatted as "{inode}/{block_id}".
#[must_use]
#[inline]
pub fn format_path(block_id: u64, ino: u64) -> String {
    format!("{ino}/{block_id}")
}

impl std::ops::Deref for Block {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

impl std::ops::DerefMut for Block {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.data
    }
}
