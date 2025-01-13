use crate::Result;

use std::fs::File;
#[cfg(unix)]
use std::os::unix::fs::FileExt;
#[cfg(windows)]
use std::os::windows::fs::FileExt;

pub trait RandomAccess: Send + Sync {
    fn read_at(&self, dst: &mut [u8], off: usize) -> Result<usize>;
}

impl RandomAccess for &[u8] {
    fn read_at(&self, dst: &mut [u8], off: usize) -> Result<usize> {
        if off > self.len() {
            return Ok(0);
        }
        let remaining = self.len() - off;
        let to_read = if dst.len() > remaining {
            remaining
        } else {
            dst.len()
        };
        (&mut dst[0..to_read]).copy_from_slice(&self[off..off + to_read]);
        Ok(to_read)
    }
}

impl RandomAccess for Vec<u8> {
    fn read_at(&self, dst: &mut [u8], off: usize) -> Result<usize> {
        (&self[..] as &[u8]).read_at(dst, off)
    }
}

#[cfg(unix)]
impl RandomAccess for File {
    fn read_at(&self, dst: &mut [u8], off: usize) -> Result<usize> {
        Ok((self as &dyn FileExt).read_at(dst, off as u64)?)
    }
}

#[cfg(windows)]
impl RandomAccess for File {
    fn read_at(&self, dst: &mut [u8], off: usize) -> Result<usize> {
        Ok((self as &dyn FileExt).seek_read(dst, off as u64)?)
    }
}
