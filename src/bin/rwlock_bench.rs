use std::sync::Arc;
use std::thread;
use std::time::Instant;

const THREAD_COUNTS: [usize; 5] = [1, 2, 4, 8, 16];
const ITERATIONS: usize = 100_000_0;

fn bench_parking_lot() {
    for &count in &THREAD_COUNTS {
        let lock = Arc::new(parking_lot::RwLock::new(0));
        let mut handles = vec![];

        let start = Instant::now();
        let write_lock = lock.clone();
        // Start a write lock
        thread::spawn(move || {
            for _ in 0..count * ITERATIONS {
                let _write = write_lock.write();
            }
        });
        for _ in 0..count {
            let lock_clone = lock.clone();
            let handle = thread::spawn(move || {
                for _ in 0..ITERATIONS {
                    let _read = lock_clone.read();
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        let duration = start.elapsed();
        println!(
            "parking_lot :Scenario with {} threads took: {:?}",
            count, duration
        );
    }
}

fn bench_std_rwlock() {
    for &count in &THREAD_COUNTS {
        let lock = Arc::new(std::sync::RwLock::new(0));
        let mut handles = vec![];

        let start = Instant::now();
        let write_lock = lock.clone();
        // Start a write lock
        thread::spawn(move || {
            for _ in 0..count * ITERATIONS {
                let _write = write_lock.write();
            }
        });
        for _ in 0..count {
            let lock_clone = lock.clone();
            let handle = thread::spawn(move || {
                for _ in 0..ITERATIONS {
                    let _read = lock_clone.read();
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        let duration = start.elapsed();
        println!(
            "std_rwlock :Scenario with {} threads took: {:?}",
            count, duration
        );
    }
}

async fn bench_tokio_rwlock() {
    for &count in &THREAD_COUNTS {
        let lock = Arc::new(tokio::sync::RwLock::new(0));
        let mut handles = vec![];

        let start = Instant::now();

        for _ in 0..count {
            let lock_clone = lock.clone();
            let handle = tokio::spawn(async move {
                for _ in 0..ITERATIONS {
                    let _read = lock_clone.read().await;
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.await.unwrap();
        }

        let duration = start.elapsed();
        println!(
            "tokio_rwlock :Scenario with {} threads took: {:?}",
            count, duration
        );
    }
}

fn bench_crossbeam_shared_lock() {
    for &count in &THREAD_COUNTS {
        let lock = Arc::new(crossbeam_utils::sync::ShardedLock::new(0));
        let mut handles = vec![];

        let start = Instant::now();
        let write_lock = lock.clone();
        // Start a write lock
        thread::spawn(move || {
            for _ in 0..count * ITERATIONS {
                let _write = write_lock.write();
            }
        });
        for _ in 0..count {
            let lock_clone = lock.clone();
            let handle = thread::spawn(move || {
                for _ in 0..ITERATIONS {
                    let _read = lock_clone.read().unwrap();
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        let duration = start.elapsed();
        println!(
            "crossbeam_shared_lock :Scenario with {} threads took: {:?}",
            count, duration
        );
    }
}

#[tokio::main]
async fn main() {
    bench_parking_lot();
    bench_std_rwlock();
    bench_crossbeam_shared_lock();
    bench_tokio_rwlock().await;
}
