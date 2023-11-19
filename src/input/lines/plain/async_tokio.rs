use std::io;
use std::path::Path;

use tokio::fs::File;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio_stream::wrappers::{LinesStream, SplitStream};
use tokio_stream::Stream;

async fn path2file<P>(p: P) -> Result<File, io::Error>
where
    P: AsRef<Path>,
{
    File::open(p).await
}

pub async fn path2strings<P>(
    p: P,
) -> Result<impl Stream<Item = Result<String, io::Error>>, io::Error>
where
    P: AsRef<Path>,
{
    let f: File = path2file(p).await?;
    let br = BufReader::new(f);
    let lines = br.lines();
    let ls = LinesStream::new(lines);
    Ok(ls)
}

pub async fn path2slices<P>(
    p: P,
) -> Result<impl Stream<Item = Result<Vec<u8>, io::Error>>, io::Error>
where
    P: AsRef<Path>,
{
    let f: File = path2file(p).await?;
    let br = BufReader::new(f);
    let splited = br.split(b'\n');
    Ok(SplitStream::new(splited))
}
