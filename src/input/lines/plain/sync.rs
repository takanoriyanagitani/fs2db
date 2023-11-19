use std::fs::File;
use std::io;
use std::io::{BufRead, BufReader};
use std::path::Path;

fn path2file<P>(p: P) -> Result<File, io::Error>
where
    P: AsRef<Path>,
{
    File::open(p)
}

pub fn path2strings<P>(p: P) -> Result<impl Iterator<Item = Result<String, io::Error>>, io::Error>
where
    P: AsRef<Path>,
{
    let f: File = path2file(p)?;
    let br = BufReader::new(f);
    Ok(br.lines())
}

pub fn path2slices<P>(p: P) -> Result<impl Iterator<Item = Result<Vec<u8>, io::Error>>, io::Error>
where
    P: AsRef<Path>,
{
    let f: File = path2file(p)?;
    let br = BufReader::new(f);
    Ok(br.split(b'\n'))
}
