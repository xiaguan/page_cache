use futures::AsyncWriteExt;
use opendal::raw::oio::ReadExt;
use opendal::services::{Fs, Memory};
use opendal::{ErrorKind, Operator};

#[derive(Debug)]
pub struct Backend {
    operator: Operator,
}

impl Backend {
    pub fn new(operator: Operator) -> Self {
        Self { operator }
    }

    pub async fn read(&self, path: &str, buf: &mut [u8]) -> usize {
        let mut reader = self.operator.reader(&path).await.unwrap();
        let mut read_size = 0;
        loop {
            let result = reader.read(buf).await;
            match result {
                Ok(size) => {
                    if size == 0 {
                        break;
                    }
                    read_size += size;
                }
                Err(e) => {
                    // If not found just return 0.
                    if e.kind() == ErrorKind::NotFound {
                        break;
                    }
                }
            }
        }
        read_size
    }

    pub async fn store(&self, path: &str, buf: &[u8]) {
        let mut writer = self.operator.writer(&path).await.unwrap();
        writer.write_all(buf).await.unwrap();
        writer.close().await.unwrap();
    }

    pub async fn remove(&self, path: &str) {
        self.operator.remove_all(&path).await.unwrap();
    }
}

pub fn memory_backend() -> Backend {
    let op = Operator::new(Memory::default()).unwrap().finish();
    Backend::new(op)
}

pub fn tmp_fs_backend() -> Backend {
    let mut builder = Fs::default();
    builder.root("/home/jinyang/backend/");
    let op = Operator::new(builder).unwrap().finish();
    Backend::new(op)
}

#[cfg(test)]
mod tests {
    use futures::executor::block_on;

    use super::*;

    #[test]
    fn test_backend() {
        let backend = memory_backend();
        let path = "test";
        let data = b"hello world";
        block_on(backend.store(path, data));
        let mut buf = Vec::with_capacity(data.len());
        block_on(backend.read(path, &mut buf));
        assert_eq!(buf, data);
    }
}
