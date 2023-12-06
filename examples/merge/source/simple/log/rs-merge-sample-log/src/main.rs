use core::fmt;

use std::fs::Metadata;
use std::path::{Path, PathBuf};

use fs2db::futures;

use futures::{Stream, StreamExt, TryStreamExt};

use tokio::io::{AsyncBufReadExt, BufReader};

use fs2db::tokio_stream;

use tokio_stream::wrappers::LinesStream;
use tokio_stream::wrappers::ReceiverStream;

use fs2db::tonic;

use tonic::Status;

use fs2db::input::merge::svc::{Merge, MergeSource};
use fs2db::input::source::Source;

#[derive(Clone)]
pub struct LogNameSource {
    root: PathBuf,
    names: Vec<String>,
}

#[derive(Clone)]
pub struct LogFileStat {
    dev: u64,
    ino: u64,
    gid: u32,
    uid: u32,
    mode: u32,
    name: String,
}

impl fmt::Display for LogFileStat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(
            f,
            "dev={} ino={} gid={} uid={} mode={}, name={}",
            self.dev, self.ino, self.gid, self.uid, self.mode, self.name
        )
    }
}

#[cfg(target_family = "unix")]
use std::os::unix::fs::MetadataExt;

#[cfg(target_family = "unix")]
impl LogFileStat {
    fn new<M>(m: &M, name: String) -> Self
    where
        M: MetadataExt,
    {
        Self {
            dev: m.dev(),
            ino: m.ino(),
            gid: m.gid(),
            uid: m.uid(),
            mode: m.mode(),
            name,
        }
    }

    async fn from_path(root: &Path, name: &str) -> Result<Self, Status> {
        let full = root.join(name);
        let m: Metadata = tokio::fs::metadata(full)
            .await
            .map_err(|e| Status::internal(format!("Unable to get a metadata: {e}")))?;
        Ok(Self::new(&m, name.into()))
    }
}

#[tonic::async_trait]
impl MergeSource for LogNameSource {
    type K = String;
    type V = LogFileStat;

    type All = ReceiverStream<Result<(Self::K, Self::V), Status>>;

    #[cfg(target_family = "unix")]
    async fn all(&self) -> Result<Self::All, Status> {
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        let root = self.root.clone();
        let names = self.names.clone();
        tokio::spawn(async move {
            let r = root;
            let n = names;
            let ns = futures::stream::iter(n);
            let rp: &Path = &r;
            let mapd = ns.map(|basename: String| async move {
                let name: &str = &basename;
                let root: &Path = rp;
                println!("trying to open {name}...");
                LogFileStat::from_path(root, name)
                    .await
                    .map(|l| (basename, l))
            });
            let rt = &tx;
            let cnt: u64 = mapd
                .fold(0, |tot, item| async move {
                    let rslt = item.await;
                    match rt.send(rslt).await {
                        Ok(_) => 1 + tot,
                        Err(_) => tot,
                    }
                })
                .await;
            println!("cnt={cnt}");
        });
        Ok(ReceiverStream::new(rx))
    }

    #[cfg(not(target_family = "unix"))]
    async fn all(&self) -> Result<Self::All, Status> {
        Err(Status::unimplemented("unix only"))
    }
}

#[derive(Clone)]
pub struct LogContentSource {
    root: PathBuf,
    names: Vec<String>,
}

pub struct LogInfo {
    line: String,
}

pub struct LogFull {
    info: LogInfo,
    meta: LogFileStat,
}

impl fmt::Display for LogFull {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "meta({}): {}", self.meta, self.info.line)
    }
}

impl LogInfo {
    async fn raw_lines(f: tokio::fs::File) -> impl Stream<Item = Result<String, Status>> {
        let br = BufReader::new(f);
        let lines = br.lines();
        let ls: LinesStream<_> = LinesStream::new(lines);
        ls.map(|r| r.map_err(|e| Status::internal(format!("unable to read a line: {e}"))))
    }

    async fn from_file(f: tokio::fs::File) -> impl Stream<Item = Result<Self, Status>> {
        Self::raw_lines(f)
            .await
            .map(|r| r.map(|line: String| Self { line }))
    }

    async fn from_path(p: &Path) -> Result<impl Stream<Item = Result<Self, Status>>, Status> {
        let f = tokio::fs::File::open(p)
            .await
            .map_err(|e| Status::internal(format!("unable to open a file: {e}")))?;
        Ok(Self::from_file(f).await)
    }
}

#[tonic::async_trait]
impl MergeSource for LogContentSource {
    type K = String;
    type V = LogInfo;

    type All = ReceiverStream<Result<(Self::K, Self::V), Status>>;

    async fn all(&self) -> Result<Self::All, Status> {
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        let root = self.root.clone();
        let names = self.names.clone();
        tokio::spawn(async move {
            let root: &Path = &root;
            let ns = futures::stream::iter(names);
            let rtx = &tx;
            let _cnt: u64 = ns
                .fold(0, |tot, basename| async move {
                    let name: &str = &basename;
                    let root: &Path = root;
                    let full = root.join(name);
                    match LogInfo::from_path(&full).await {
                        Err(_) => tot,
                        Ok(logs) => {
                            let mapd =
                                logs.map(|r| r.map(|info: LogInfo| (basename.clone(), info)));
                            let subtot: u64 = mapd
                                .fold(0, |sub, rslt| async move {
                                    match rtx.send(rslt).await {
                                        Err(_) => sub,
                                        Ok(_) => 1 + sub,
                                    }
                                })
                                .await;
                            subtot + tot
                        }
                    }
                })
                .await;
        });
        Ok(ReceiverStream::new(rx))
    }
}

#[derive(Clone)]
pub struct Merger {}

impl Merge for Merger {
    type A = LogFileStat;
    type B = LogInfo;
    type T = LogFull;

    fn merge(&self, a: Self::A, b: Self::B) -> Result<Self::T, Status> {
        Ok(Self::T { info: b, meta: a })
    }
}

pub fn log_source_new(
    meta: LogNameSource,
    content: LogContentSource,
    m: Merger,
) -> impl Source<Item = LogFull> {
    fs2db::input::merge::simple::source_new_merged(meta, content, m)
}

async fn sub<S>(log_source: S) -> Result<u64, Status>
where
    S: Source<Item = LogFull>,
{
    let logs = log_source.all().await?;
    logs.try_fold(0, |tot, item| async move {
        println!("{item}");
        Ok(1 + tot)
    })
    .await
}

#[tokio::main]
async fn main() -> Result<(), String> {
    let ns = LogNameSource {
        root: Path::new("/var/log").to_path_buf(),
        names: vec![
            "auth.log",
            "bootstrap.log",
            "cloud-init.log",
            "kern.log",
            "cloud-init-output.log",
        ]
        .into_iter()
        .map(|s| s.into())
        .collect(),
    };
    let cs = LogContentSource {
        root: Path::new("/var/log").to_path_buf(),
        names: vec!["auth.log", "bootstrap.log", "cloud-init.log", "kern.log"]
            .into_iter()
            .map(|s| s.into())
            .collect(),
    };
    let log_source = log_source_new(ns, cs, Merger {});
    let _cnt: u64 = sub(log_source).await.map_err(|e| format!("{e}"))?;
    Ok(())
}
