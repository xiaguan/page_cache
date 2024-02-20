// use std::sync::Arc;

// use hashbrown::{HashMap, HashSet};
// use parking_lot::{Mutex, RwLock};
// use tokio::task::JoinHandle;

// use crate::block::Block;
// use crate::lru::LruPolicy;
// use crate::CacheManager;

// pub struct WriteHandle {
//     fh: usize,
//     cache: Arc<Mutex<CacheManager<u64, LruPolicy<u64>>>>,
//     dirty_blocks: HashMap<u64, Arc<RwLock<Block>>>,
//     write_back_handles: Vec<JoinHandle<bool>>,
// }

// async fn write_back_work(dirty_blocks: HashMap<u64, Arc<RwLock<Block>>>) ->
// bool {     let mut results = Vec::new();
//     for (block_id, block) in dirty_blocks.iter() {
//         let block = block.read();
//         results.push(tokio::spawn(async move {
//             write_block(path, *block_id, data).await
//         }));
//     }
//     true
// }

// impl WriteHandle {
//     fn write(&mut self, offset: u64, data: &[u8]) -> usize {
//         if self.dirty_blocks.len() > 5 {
//             self.write_back_handles
//                 
// .push(tokio::spawn(write_back_work(self.dirty_blocks.clone())));         }
//     }

//     async fn flush(&mut self) -> Result<()> {
//         for handle in self.write_back_handles.drain(..) {
//             handle.await?;
//         }
//         Ok(())
//     }
// }
