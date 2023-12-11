use std::path::Path;

use futures::StreamExt;

use tokio::io::{AsyncBufReadExt, BufReader};

use tokio_stream::wrappers::{ReceiverStream, SplitStream};

use tonic::Status;

use crate::input::source::BucketSource;

#[tonic::async_trait]
pub trait ReadSource: Send + Sync + 'static {
    type Bucket: Send + Sync;
    type R: Send + Sync + tokio::io::AsyncRead;

    async fn get_src_read_by_bucket(&self, b: Self::Bucket) -> Result<Self::R, Status>;
}

pub struct ReadSrc<R> {
    rsrc: R,
}

#[tonic::async_trait]
impl<R> BucketSource for ReadSrc<R>
where
    R: ReadSource,
    <R as ReadSource>::R: Unpin,
{
    type Bucket = R::Bucket;
    type K = usize;
    type V = Vec<u8>;
    type All = ReceiverStream<Result<(Self::K, Self::V), Status>>;

    async fn get_all_by_bucket(&self, b: Self::Bucket) -> Result<Self::All, Status> {
        let r: R::R = self.rsrc.get_src_read_by_bucket(b).await?;
        let br = BufReader::new(r);
        let splited = br.split(b'\n');
        let ss = SplitStream::new(splited);
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        tokio::spawn(async move {
            let mapd = ss
                .map(|rslt| {
                    rslt.map_err(|e| Status::internal(format!("unable to get a line: {e}")))
                })
                .enumerate()
                .map(|pair: (usize, _)| {
                    let (ix, item) = pair;
                    item.map(|v: Self::V| (ix, v))
                });
            let rt = &tx;
            let _cnt: u64 = mapd
                .fold(0, |tot, rslt| async move {
                    rt.send(rslt).await.map(|_| 1 + tot).unwrap_or(tot)
                })
                .await;
        });
        Ok(ReceiverStream::new(rx))
    }
}

pub fn bytes_src_new<R>(rsrc: R) -> impl BucketSource<Bucket = R::Bucket, K = usize, V = Vec<u8>>
where
    R: ReadSource,
    <R as ReadSource>::R: Unpin,
{
    ReadSrc { rsrc }
}

#[tonic::async_trait]
pub trait FsSource: Send + Sync + 'static {
    type Bucket: Send + Sync;

    fn bucket2path(&self, b: Self::Bucket) -> Result<&Path, Status>;

    async fn get_file_by_bucket(&self, b: Self::Bucket) -> Result<tokio::fs::File, Status> {
        let p: &Path = self.bucket2path(b)?;
        tokio::fs::File::open(p)
            .await
            .map_err(|e| Status::internal(format!("unable to open a file: {e}")))
    }
}

#[tonic::async_trait]
impl<F> ReadSource for F
where
    F: FsSource,
{
    type Bucket = F::Bucket;
    type R = tokio::fs::File;

    async fn get_src_read_by_bucket(&self, b: Self::Bucket) -> Result<Self::R, Status> {
        self.get_file_by_bucket(b).await
    }
}
