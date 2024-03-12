use std::sync::Arc;

use bytes::BufMut;
use page_cache::backend::memory_backend::MemoryBackend;
use page_cache::block::BLOCK_SIZE;
use page_cache::handle::handle::OpenFlag;
use page_cache::lru::LruPolicy;
use page_cache::mock_io::{CacheKey, MockIO};
use page_cache::storage::Storage;
use page_cache::CacheManager;
use rand::distributions::Distribution;
use rand::{thread_rng, Rng};
use rand_distr::Zipf;

// Bench time : default is 30s
const BENCH_TIME: u64 = 30;

// Backend latency : default is 0ms
const BACKEND_LATENCY: u64 = 0;

// Scan read thread num
const SCAN_READ_THREAD_NUM: u64 = 4;

// Random get thread num
const RANDOM_GET_THREAD_NUM: u64 = 4;

// Total test pages : default is 256
const TOTAL_TEST_BLOCKS: usize = 256;
// mb
const TOTAL_SIZE: usize = TOTAL_TEST_BLOCKS * BLOCK_SIZE / 1024 / 1024;

const IO_SIZE: usize = 1024;

fn modify_data(data: &mut Vec<u8>, user_index: u64) {
    let mut rng = thread_rng();
    let seed = rng.gen();
    data.put_u64(seed);
    data.put_u64(user_index);
    data.extend_from_slice(&[1; IO_SIZE - 16]);
    let index = 16 + seed % (IO_SIZE as u64 - 16);
    data[index as usize] = seed as u8;
    assert_eq!(data.len(), IO_SIZE);
}

fn check_data(data: &[u8], user_index: u64) {
    // 确保data有足够的长度来包含两个u64值和至少一个额外的字节
    if data.len() < 16 + 1 {
        panic!("Data does not have enough bytes.");
    }

    // 读取seed和block_id
    let seed = u64::from_be_bytes(data[0..8].try_into().unwrap());
    let stored_block_id = u64::from_be_bytes(data[8..16].try_into().unwrap());
    // 检查block_id是否匹配
    if user_index != stored_block_id {
        panic!(
            "Block ID does not match. Expected {}, found {}.",
            user_index, stored_block_id
        );
    }

    // 检查在基于seed计算的位置上的字节是否正确
    let expected_byte = seed as u8;
    let index = 16 + seed % (IO_SIZE as u64 - 16);
    let actual_byte = data[index as usize];
    if expected_byte != actual_byte {
        panic!(
            "Data at calculated position does not match. Expected {}, found {}.",
            expected_byte, actual_byte
        );
    }
}

// Open a read file handle ,read to end, but don't close the handle
async fn warm_up(storage: Arc<Storage>, ino: u64) {
    let flag = OpenFlag::Read;
    let fh = storage.open(ino, flag);
    for i in 0..TOTAL_TEST_BLOCKS {
        let buf = storage
            .read(ino, fh, (i * IO_SIZE) as u64, IO_SIZE)
            .await
            .unwrap();
        assert_eq!(buf.len(), IO_SIZE);
    }
}

async fn seq_read(storage: Arc<Storage>, ino: u64) {
    let flag = OpenFlag::Read;
    let fh = storage.open(ino, flag);
    for i in 0..TOTAL_TEST_BLOCKS {
        let buf = storage
            .read(10, fh, (i * IO_SIZE) as u64, IO_SIZE)
            .await
            .unwrap();
        assert_eq!(buf.len(), IO_SIZE);
        check_data(&buf, i as u64);
    }
    storage.close(fh).await;
}

async fn create_a_file(storage: Arc<Storage>, ino: u64) {
    let flag = OpenFlag::Write;
    let fh = storage.open(ino, flag);
    let start = std::time::Instant::now();
    for i in 0..TOTAL_TEST_BLOCKS {
        let mut content = Vec::new();
        modify_data(&mut content, i as u64);
        storage
            .write(ino, fh, (i * IO_SIZE) as u64, &content)
            .await
            .unwrap();
    }
    storage.flush(ino, fh).await;
    storage.close(fh).await;
    let end = std::time::Instant::now();
    let througput = TOTAL_SIZE as f64 / (end - start).as_secs_f64();
    println!(
        "Create a file ino {} cost {:?} thoughput: {} MB/s",
        ino,
        end - start,
        througput
    );
    let size = storage.len();
    println!("Cache size: {}", size);
}

async fn concurrency_read() {
    println!("Concurrency read test");
    println!("Write 1GB data to the storage");
    println!("Then warm up the cache");
    println!("Then do the concurrency read test, we don't close the reader handle");
    println!("In order to keep the cache warm");
    let backend = Arc::new(MemoryBackend::new(BACKEND_LATENCY));
    // Only 1 file, 256 blocks , the cache will never miss
    let manger = CacheManager::<CacheKey, LruPolicy<CacheKey>>::new(500);
    let storage = Arc::new(Storage::new(manger, backend));
    create_a_file(storage.clone(), 10).await;
    warm_up(storage.clone(), 10).await;
    // Concurrency read ,thread num : 1,2,4,8
    for i in 0..5 {
        let mut tasks = vec![];
        let start = tokio::time::Instant::now();
        for _ in 0..2_usize.pow(i) {
            let storage = storage.clone();
            tasks.push(tokio::spawn(async move {
                seq_read(storage, 10).await;
            }));
        }
        for task in tasks {
            task.await.unwrap();
        }
        let end = tokio::time::Instant::now();
        // throuput = 1GB/ time * thread num
        let throuput = TOTAL_SIZE as f64 / (end - start).as_secs_f64() * 2_usize.pow(i) as f64;
        println!(
            "thread num : {}, read Time: {:?} thoughput: {} MB/s",
            2_usize.pow(i),
            end - start,
            throuput
        );
        // assert!(storage.len() == 0);
    }
}

async fn concurrency_read_with_write() {
    println!("Concurrency read with write test");
    println!("Write 1GB data to the storage");
    println!("Then warm up the cache");
    println!(
        "Then do the concurrency read test, and a worker will write another file to the storage"
    );
    let backend = Arc::new(MemoryBackend::new(BACKEND_LATENCY));
    // Only 1 file, 256 blocks , the cache will never miss
    let manger = CacheManager::<CacheKey, LruPolicy<CacheKey>>::new(2 * TOTAL_TEST_BLOCKS + 10);
    let storage = Arc::new(Storage::new(manger, backend));
    create_a_file(storage.clone(), 10).await;
    warm_up(storage.clone(), 10).await;
    // Concurrency read ,thread num : 1,2,4,8
    for i in 0..5 {
        let mut tasks = vec![];
        let start = tokio::time::Instant::now();
        for _ in 0..2_usize.pow(i) {
            let storage = storage.clone();
            tasks.push(tokio::spawn(async move {
                seq_read(storage, 10).await;
            }));
        }
        let write_handle = tokio::spawn(create_a_file(storage.clone(), 11));
        for task in tasks {
            task.await.unwrap();
        }
        let end = tokio::time::Instant::now();
        // throuput = 1GB/ time * thread num
        let throuput = TOTAL_SIZE as f64 / (end - start).as_secs_f64() * 2_usize.pow(i) as f64;
        println!(
            "thread num : {}, read Time: {:?} thoughput: {} MB/s",
            2_usize.pow(i),
            end - start,
            throuput
        );
        // assert!(storage.len() == 0);
        write_handle.await.unwrap();
    }
}

async fn scan_worker(storage: Arc<Storage>, ino: u64, time: u64) -> usize {
    let flag = OpenFlag::Read;
    let fh = storage.open(ino, flag);
    let start = tokio::time::Instant::now();
    let mut i = 0;
    let mut scan_cnt = 0;
    while tokio::time::Instant::now() - start < tokio::time::Duration::from_secs(time) {
        let buf = storage
            .read(ino, fh, (i * IO_SIZE) as u64, IO_SIZE)
            .await
            .unwrap();
        assert_eq!(buf.len(), IO_SIZE);
        check_data(&buf, i as u64);
        i = (i + 1) % TOTAL_TEST_BLOCKS;
        scan_cnt += 1;
    }
    scan_cnt
}

async fn get_worker(storage: Arc<Storage>, ino: u64, time: u64) -> usize {
    let flag = OpenFlag::Read;
    let fh = storage.open(ino, flag);
    let start = tokio::time::Instant::now();

    // 初始化 Zipfian 分布
    let zipf = Zipf::new(TOTAL_TEST_BLOCKS as u64, 1.5).unwrap();

    let mut get_cnt = 0;
    while tokio::time::Instant::now() - start < tokio::time::Duration::from_secs(time) {
        // 使用 Zipfian 分布来选择数据块 ID
        let i = zipf.sample(&mut thread_rng()) as usize % TOTAL_TEST_BLOCKS;

        let buf = storage
            .read(ino, fh, (i * IO_SIZE) as u64, IO_SIZE)
            .await
            .unwrap();
        assert_eq!(buf.len(), IO_SIZE);
        check_data(&buf, i as u64);
        get_cnt += 1;
    }
    get_cnt
}

async fn real_workload() {
    println!("Real workload test");
    let backend = Arc::new(MemoryBackend::new(BACKEND_LATENCY));
    // A 4GB cache
    let manger = CacheManager::<CacheKey, LruPolicy<CacheKey>>::new(1024 + 10);
    let storage = Arc::new(Storage::new(manger, backend));
    // 100 is for scan worker
    create_a_file(storage.clone(), 100).await;
    let mut create_tasks = vec![];
    for i in 0..RANDOM_GET_THREAD_NUM {
        let storage = storage.clone();
        create_tasks.push(tokio::spawn(create_a_file(storage, i as u64)));
    }
    for task in create_tasks {
        task.await.unwrap();
    }
    let mut scan_tasks = vec![];
    let mut get_tasks = vec![];
    for _ in 0..SCAN_READ_THREAD_NUM {
        let storage = storage.clone();
        scan_tasks.push(tokio::spawn(scan_worker(storage, 100, BENCH_TIME)));
    }
    for i in 0..RANDOM_GET_THREAD_NUM {
        let storage = storage.clone();
        get_tasks.push(tokio::spawn(get_worker(storage, i, BENCH_TIME)));
    }

    let mut total_scan = 0;
    let mut total_get = 0;
    for task in scan_tasks {
        total_scan += task.await.unwrap();
    }
    for task in get_tasks {
        total_get += task.await.unwrap();
    }
    println!(
        "Total scan cnt: {}, total get cnt: {}",
        total_scan, total_get
    );
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // concurrency_read().await;
    real_workload().await;
    // concurrency_read_with_write().await;
    Ok(())
}

#[tokio::test]
async fn stroage_concurrency_read_test() {
    concurrency_read().await;
}

#[tokio::test]
async fn stroage_concurrency_read_with_write_test() {
    concurrency_read_with_write().await;
}

#[tokio::test]
async fn stroage_real_workload_test() {
    real_workload().await;
}
