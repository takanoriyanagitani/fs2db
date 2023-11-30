use std::path::{Path, PathBuf};

use futures_core::Stream;

use fs2db::futures;
use futures::StreamExt;
use futures::TryStreamExt;

use tokio::fs::File;

use fs2db::tokio_stream;
use tokio_stream::wrappers::ReceiverStream;

use fs2db::tonic;
use tonic::{Request, Response, Status};

use fs2db::rpc::fs2db::proto::source;
use source::v1::select_service_server::SelectService;
use source::v1::{AllRequest, AllResponse, InputBucket};

use crate::row::Record;

pub const SOURCE_PATH_ROOT: &str = ".";

pub async fn file2lines(f: File) -> impl Stream<Item = Result<String, Status>> {
    crate::source::lines::fs2lines::read2lines(f).await
}

pub async fn path2lines(p: &Path) -> Result<impl Stream<Item = Result<String, Status>>, Status> {
    let f: File = File::open(p)
        .await
        .map_err(|e| Status::internal(format!("Unable to open a file: {e}")))?;
    Ok(file2lines(f).await)
}

pub async fn path2rows(p: &Path) -> Result<impl Stream<Item = Result<Record, Status>>, Status> {
    let lines = path2lines(p).await?;
    Ok(crate::source::lines::lines2rows::lines2rows(lines))
}

pub struct FsSvc {
    root: PathBuf,
}

impl FsSvc {
    pub fn new_env(key: &str) -> Result<Self, Status> {
        let root: String = std::env::var(key)
            .map_err(|e| Status::invalid_argument(format!("root path unknown: {e}")))?;
        Ok(Self { root: root.into() })
    }

    pub fn new_default() -> Result<Self, Status> {
        Self::new_env("ENV_SOURCE_PATH_ROOT")
    }
}

#[tonic::async_trait]
impl SelectService for FsSvc {
    type AllStream = ReceiverStream<Result<AllResponse, Status>>;

    async fn all(&self, req: Request<AllRequest>) -> Result<Response<Self::AllStream>, Status> {
        let ar: AllRequest = req.into_inner();
        let bkt: InputBucket = ar
            .bkt
            .ok_or_else(|| Status::invalid_argument("input bucket missing"))?;
        let raw_bkt: Vec<u8> = bkt.bucket;
        let basename: String = String::from_utf8(raw_bkt)
            .map_err(|e| Status::invalid_argument(format!("invalid bucket name: {e}")))?;
        let fullname = self.root.join(basename);
        let rows = path2rows(&fullname).await?;
        let pairs = rows.map(|r| r.map(|row: Record| row.into_pair()));
        let response = pairs.map(|r| {
            r.map(|pair| {
                let (key, val) = pair;
                AllResponse { key, val }
            })
        });
        let mapd = response.map(Result::Ok::<_, Status>);
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        tokio::spawn(async move {
            let rtx = &tx;
            match mapd
                .try_fold(0, |tot, r| async move {
                    rtx.send(r)
                        .await
                        .map(|_| 1 + tot)
                        .map_err(|e| Status::internal(format!("Unable to send a result: {e}")))
                })
                .await
            {
                Ok(tot) => {
                    log::info!("sent count: {tot}");
                }
                Err(e) => {
                    log::warn!("Unable to send results: {e}");
                }
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }
}
