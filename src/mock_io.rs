use async_trait::async_trait;

use crate::block::BLOCK_SIZE;

#[async_trait]
pub trait MockIO {
    async fn read(&self, ino: u64, offset: u64, buf: &mut Vec<u8>) -> u32;
    async fn write(&self, ino: u64, offset: u64, buf: &[u8]) -> u32;
}

fn get_block_id(offset: u64) -> u64 {
    offset / BLOCK_SIZE as u64
}

fn get_key(ino: u64, block_id: u64) -> u128 {
    ((ino as u128) << 64) | (block_id as u128)
}
