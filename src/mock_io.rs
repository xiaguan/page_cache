use async_trait::async_trait;
use smallvec::SmallVec;

use crate::block::BLOCK_SIZE;

#[async_trait]
pub trait MockIO {
    // 可以换成 BytesMut
    async fn read(&self, ino: u64, offset: u64, buf: &mut Vec<u8>) -> u32;
    // 可以换成Bytes
    async fn write(&self, ino: u64, offset: u64, buf: &[u8]) -> u32;
}

fn offset_to_slice(block_size: u64, offset: u64, len: u64) -> SmallVec<[Slice; 2]> {
    let mut slices = SmallVec::new();
    let mut current_offset = offset;
    let mut remaining_len = len;

    while remaining_len > 0 {
        let current_block = current_offset / block_size;
        let block_internal_offset = current_offset % block_size;
        let space_in_block = block_size - block_internal_offset;
        let size_to_read = remaining_len.min(space_in_block);

        slices.push(Slice {
            block_id: current_block,
            offset: block_internal_offset,
            size: size_to_read,
        });

        remaining_len -= size_to_read;
        current_offset += size_to_read;
    }

    slices
}

struct Slice {
    block_id: u64,
    offset: u64,
    size: u64,
}

#[derive(Debug, Hash, Clone, PartialEq, Eq)]
pub struct CacheKey {
    pub ino: u64,
    pub block_id: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_offset_to_slice_single_block() {
        let slices = offset_to_slice(4, 2, 2);
        assert_eq!(slices.len(), 1);
        assert_eq!(slices[0].block_id, 0);
        assert_eq!(slices[0].offset, 2);
        assert_eq!(slices[0].size, 2);
    }

    #[test]
    fn test_offset_to_slice_cross_blocks() {
        let slices = offset_to_slice(4, 3, 6);
        assert_eq!(slices.len(), 3); // Expecting to cross 3 blocks
        assert_eq!(slices[0].block_id, 0);
        assert_eq!(slices[0].offset, 3);
        assert_eq!(slices[0].size, 1);

        assert_eq!(slices[1].block_id, 1);
        assert_eq!(slices[1].offset, 0);
        assert_eq!(slices[1].size, 4);

        assert_eq!(slices[2].block_id, 2);
        assert_eq!(slices[2].offset, 0);
        assert_eq!(slices[2].size, 1);
    }
}
