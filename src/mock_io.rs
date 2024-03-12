use async_trait::async_trait;

use crate::error::StorageResult;
use crate::handle::handle::OpenFlag;

/// The `MockIO` trait defines an interface for mocking I/O operations.
#[async_trait]
pub trait MockIO {
    /// Opens a file with the given inode number and flags, returning a file handle.
    fn open(&self, ino: u64, flag: OpenFlag) -> u64;

    /// Reads data from a file specified by the inode number and file handle,
    /// starting at the given offset and reading up to `len` bytes.
    async fn read(&self, ino: u64, fh: u64, offset: u64, len: usize) -> StorageResult<Vec<u8>>;

    /// Writes data to a file specified by the inode number and file handle,
    /// starting at the given offset.
    async fn write(&self, ino: u64, fh: u64, offset: u64, buf: &Vec<u8>) -> StorageResult<()>;

    /// Truncates a file specified by the inode number to a new size,
    /// given the old size and the new size.
    async fn truncate(&self, ino: u64, old_size: u64, new_size: u64) -> StorageResult<()>;

    /// Flushes any pending writes to a file specified by the inode number and file handle.
    async fn flush(&self, _ino: u64, fh: u64);

    /// Closes a file specified by the file handle.
    async fn close(&self, fh: u64);
}

/// The `CacheKey` struct represents a key used for caching file data.
/// It consists of an inode number and a block ID.
#[derive(Debug, Hash, Clone, PartialEq, Eq)]
pub struct CacheKey {
    /// The inode number.
    pub ino: u64,
    ///
    pub block_id: u64,
}
