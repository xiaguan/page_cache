use futures::AsyncWriteExt;
use opendal::raw::oio::ReadExt;
use opendal::Operator;

#[derive(Debug)]
pub struct Backend {
    operator: Operator,
}

impl Backend {
    pub fn new(operator: Operator) -> Self {
        Self { operator }
    }

    pub async fn load(&self, path: &str, buf: &mut Vec<u8>) {
        let mut reader = self.operator.reader(&path).await.unwrap();
        reader.read(buf);
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
