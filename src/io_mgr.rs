use std::fs::OpenOptions;
use std::io::{Result as IoResult, Seek, SeekFrom, Write};
use std::path::Path;

use memmap2::{MmapMut, MmapOptions};

pub fn create_mmap<P: AsRef<Path>>(
    path: P,
    fsize: u64,
    offset: u64,
    len: usize,
) -> IoResult<MmapMut> {
    //eprintln!("create_mmap({}, {}, {}, {}) open", path.as_ref().display(), fsize, offset, len);
    let mut f = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(path)?;
    //eprintln!("create_mmap({}, {}, {}, {}) seek", path.as_ref().display(), fsize, offset, len);
    f.seek(SeekFrom::Start(fsize - 1))?;
    //eprintln!("create_mmap({}, {}, {}, {}) write", path.as_ref().display(), fsize, offset, len);
    f.write(&[b'\0'])?;
    let mut opts = MmapOptions::new();
    opts.offset(offset).len(len);
    //eprintln!("create_mmap({}, {}, {}, {}) map_mut", path.as_ref().display(), fsize, offset, len);
    let m = unsafe { opts.map_mut(&f) }?;
    //eprintln!("create_mmap({}, {}, {}, {}) ok", path.as_ref().display(), fsize, offset, len);
    Ok(m)
}
