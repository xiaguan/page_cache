pub struct FileHandle {
    fh: u64,
    reader: Option<Reader>,
}

impl FileHandle {
    pub fn new(fh: u64, reader: Option<Reader>) -> Self {
        FileHandle { fh, reader }
    }
}
