use core::ops::Range;
use std::fmt::Debug;
use std::io::{self, BufWriter, Read, Seek, Write};
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use canister_fs::filesystem_memory::{DirEntry, File, FileSystem};
use common::file_slice::FileHandle;
use common::{HasLen, StableDeref, TerminatingWrite};

use super::WatchCallbackList;
use crate::directory::OwnedBytes;
use crate::Directory;

#[derive(Clone)]
struct FileSlice(Arc<dyn Deref<Target = [u8]> + Send + Sync>);

impl Deref for FileSlice {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        self.0.deref()
    }
}
unsafe impl StableDeref for FileSlice {}

struct FileWrapper<'a> {
    inner: DirEntry<'a>,
}

// We only have single thread in a canister.
unsafe impl<'a> Send for FileWrapper<'a> {}
unsafe impl<'a> Sync for FileWrapper<'a> {}

impl<'a> Debug for FileWrapper<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let entry = &self.inner;
        f.debug_struct("FileWrapper")
            .field("file name", &entry.file_name())
            .field("file size", &entry.len())
            .field("created at", &entry.created())
            .field("last updated at", &entry.modified())
            .finish()
    }
}

impl<'a> HasLen for FileWrapper<'a> {
    fn len(&self) -> usize {
        self.inner.len() as usize
    }
}

impl FileHandle for FileWrapper<'static> {
    fn read_bytes(&self, range: Range<usize>) -> io::Result<OwnedBytes> {
        let file_len = self.inner.len() as usize;
        let file_range = 0..=file_len;

        match file_range.contains(&range.start)
            && file_range.contains(&range.end)
            && range.start <= range.end
        {
            true => {
                let mut buffer = vec![];
                buffer.resize(range.end - range.start, 0);

                let mut file = self.inner.to_file();
                file.seek(io::SeekFrom::Start(range.start as u64))
                    .expect("Unexpected seek failed.");
                file.read_exact(&mut buffer)?;

                let res = FileSlice(Arc::new(buffer));
                Ok(OwnedBytes::new(res))
            }
            false => Err(io::ErrorKind::InvalidInput.into()),
        }
    }
}

struct FileWriter<'a> {
    inner: File<'a>,
}

// We only have single thread in a canister.
unsafe impl<'a> Send for FileWriter<'a> {}
unsafe impl<'a> Sync for FileWriter<'a> {}

impl<'a> Write for FileWriter<'a> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.inner.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

impl<'a> TerminatingWrite for FileWriter<'a> {
    fn terminate_ref(&mut self, _: common::AntiCallToken) -> io::Result<()> {
        self.inner.flush()
    }
}
struct InnerDirectory<'a> {
    fs: &'a FileSystem,
    watch_router: Arc<RwLock<WatchCallbackList>>,
}

impl<'a> Clone for InnerDirectory<'a> {
    fn clone(&self) -> Self {
        Self {
            fs: self.fs,
            watch_router: self.watch_router.clone(),
        }
    }
}

/// A Directory storing everything in canister's stable memory, powered by
/// [`ic_stable_structures`].
#[derive(Clone)]
pub struct CanisterDirectory<'a> {
    inner: InnerDirectory<'a>,
}

// We only have single thread in a canister.
unsafe impl<'a> Send for CanisterDirectory<'a> {}
unsafe impl<'a> Sync for CanisterDirectory<'a> {}

impl<'a> Debug for CanisterDirectory<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let fs = &self.inner.fs;
        f.debug_struct("CanisterDirectory")
            .field("volume_id", &fs.volume_id())
            .field("fat_type", &fs.fat_type())
            .field("cluster_size", &fs.cluster_size())
            .field("stats", &fs.stats())
            .finish()
    }
}

impl<'a> CanisterDirectory<'a> {
    /// Convert a [`FileSystem`] to [`CanisterDirectory`].
    pub fn new(fs: &'a FileSystem) -> Self {
        let inner = InnerDirectory {
            fs,
            watch_router: Default::default(),
        };
        Self { inner }
    }
}

impl Directory for CanisterDirectory<'static> {
    fn get_file_handle(
        &self,
        path: &std::path::Path,
    ) -> Result<Arc<dyn FileHandle>, super::error::OpenReadError> {
        let fs = self.inner.fs.root_dir();
        let path = path_to_string(path);
        // Try to create a new file(this will return the old one if
        // it exists.)
        {
            fs.create_file(path).map_err(|e| match e {
                canister_fs::Error::Io(e) => super::error::OpenReadError::IoError {
                    io_error: Arc::new(e),
                    filepath: path.into(),
                },
                canister_fs::Error::InvalidInput => panic!("InvalidInput path"),
                canister_fs::Error::InvalidFileNameLength => panic!("InvalidFileNameLength"),
                canister_fs::Error::UnsupportedFileNameCharacter => {
                    panic!("UnsupportedFileNameCharacter")
                }
                canister_fs::Error::NotEnoughSpace => panic!("NotEnoughSpace"),
                _ => unreachable!(),
            })?;
        }
        // We get the entry of the file here.
        let res = fs.get_entry(path).expect("Unexpected error.");
        Ok(Arc::new(FileWrapper { inner: res }))
    }

    fn delete(&self, path: &std::path::Path) -> Result<(), super::error::DeleteError> {
        let fs = self.inner.fs.root_dir();
        let path = path_to_string(path);
        fs.remove(path).map_err(|e| match e {
            canister_fs::Error::Io(e) => super::error::DeleteError::IoError {
                io_error: Arc::new(e),
                filepath: path.into(),
            },
            canister_fs::Error::NotFound => {
                super::error::DeleteError::FileDoesNotExist(path.into())
            }
            canister_fs::Error::InvalidInput => {
                panic!("InvalidInput path")
            }
            _ => unreachable!(),
        })?;
        Ok(())
    }

    fn exists(&self, path: &std::path::Path) -> Result<bool, super::error::OpenReadError> {
        let file = self.inner.fs.root_dir().open_file(path_to_string(path));

        match file {
            Ok(_) => Ok(true),
            Err(e) => match e {
                canister_fs::Error::Io(e) => Err(super::error::OpenReadError::IoError {
                    io_error: Arc::new(e),
                    filepath: path.into(),
                }),
                canister_fs::Error::InvalidInput => panic!("InvalidInput path"),
                canister_fs::Error::NotFound => Ok(false),
                _ => unreachable!(),
            },
        }
    }

    fn open_write(
        &self,
        path: &std::path::Path,
    ) -> Result<super::WritePtr, super::error::OpenWriteError> {
        let fs = self.inner.fs;
        let file = fs.root_dir().create_file(path_to_string(path)).unwrap();
        let writer = FileWriter { inner: file };

        let writer = Box::new(writer) as Box<dyn TerminatingWrite>;
        let writer = BufWriter::new(writer);
        Ok(writer)
    }

    fn atomic_read(&self, path: &std::path::Path) -> Result<Vec<u8>, super::error::OpenReadError> {
        let mut file = self
            .inner
            .fs
            .root_dir()
            .open_file(path_to_string(path))
            .map_err(|e| match e {
                canister_fs::Error::Io(e) => super::error::OpenReadError::IoError {
                    io_error: Arc::new(e),
                    filepath: PathBuf::from(path),
                },
                canister_fs::Error::InvalidInput => panic!("InvalidInput"),
                canister_fs::Error::NotFound => {
                    super::error::OpenReadError::FileDoesNotExist(PathBuf::from(path))
                }
                _ => unreachable!(),
            })?;
        let mut buffer = vec![];
        file.read_to_end(&mut buffer).unwrap();
        Ok(buffer)
    }

    fn atomic_write(&self, path: &std::path::Path, data: &[u8]) -> std::io::Result<()> {
        let mut file = self
            .inner
            .fs
            .root_dir()
            .create_file(path_to_string(path))
            .unwrap();
        file.truncate()?;
        file.write(data)?;
        file.flush()?;
        Ok(())
    }

    fn sync_directory(&self) -> std::io::Result<()> {
        Ok(())
    }

    fn watch(&self, watch_callback: super::WatchCallback) -> crate::Result<super::WatchHandle> {
        Ok(self
            .inner
            .watch_router
            .write()
            .unwrap()
            .subscribe(watch_callback))
    }
}

fn path_to_string<'a>(path: &'a Path) -> &'a str {
    path.as_os_str()
        .to_str()
        .expect("Convert Path to String failed.")
}

#[cfg(test)]
mod tests {
    use std::io::Write;
    use std::path::Path;

    use canister_fs::filesystem_memory::{FileSystem, FileSystemMemory};
    use canister_fs::time_provider::TimeProvider;
    use canister_fs::{format_volume, FormatVolumeOptions, FsOptions, StdIoWrapper};
    use common::file_slice::FileHandle;
    use common::HasLen;
    use ic_stable_structures::memory_manager::{MemoryId, MemoryManager};
    use ic_stable_structures::DefaultMemoryImpl;
    use once_cell::sync::Lazy;
    use send_wrapper::SendWrapper;

    use super::{CanisterDirectory, FileWrapper};
    use crate::Directory;

    static DIRECTORY: Lazy<SendWrapper<FileSystem>> = Lazy::new(|| {
        let memory_manager = MemoryManager::init(DefaultMemoryImpl::default());

        let memory = memory_manager.get(MemoryId::new(0));

        let memory = FileSystemMemory::new(memory);

        let options = FormatVolumeOptions::new();
        format_volume(&mut StdIoWrapper::from(memory.clone()), options).unwrap();

        let options = FsOptions::new()
            .time_provider(TimeProvider::new())
            .update_accessed_date(true);

        let res = FileSystem::new(memory, options).unwrap();
        SendWrapper::new(res)
    });

    #[test]
    fn test_file_wrapper() {
        let dir = &DIRECTORY.root_dir();
        {
            let mut file = dir.create_file("test").unwrap();
            file.write(b"tesdtad").unwrap();
            file.flush().unwrap();
        }
        let entry = dir.get_entry("test").unwrap();
        let file_wrapper = FileWrapper { inner: entry };
        let len = file_wrapper.len();
        assert_eq!(len, 7);
        let res = file_wrapper.read_bytes(0..1).unwrap();
        assert_eq!(res.as_slice(), b"t");

        let res = file_wrapper.read_bytes(0..2).unwrap();
        assert_eq!(res.as_slice(), b"te");

        let res = file_wrapper.read_bytes(1..2).unwrap();
        assert_eq!(res.as_slice(), b"e");

        let res = file_wrapper.read_bytes(7..7).unwrap();
        assert_eq!(res.as_slice(), b"");

        let res = file_wrapper.read_bytes(7..8);
        assert!(res.is_err())
    }

    #[test]
    fn test_read_write() {
        let res = CanisterDirectory::new(&DIRECTORY);

        let path = Path::new("atomic");
        res.atomic_write(path, b"datatadtasdsa").unwrap();

        let bytes = res.atomic_read(path).unwrap();

        assert_eq!(bytes, b"datatadtasdsa");

        let path = Path::new("file2");
        let mut writer = res.open_write(path).unwrap();
        writer.write_all(b"dsadsads").unwrap();
        writer.flush().unwrap();

        let file_handler = res.open_read(path).unwrap();

        assert_eq!(file_handler.len(), 8);

        res.delete(path).unwrap();
        let exists = res.exists(path).unwrap();
        assert!(!exists);
    }
}
