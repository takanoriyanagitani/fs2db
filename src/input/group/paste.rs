//! "paste" like grouping

use futures::Stream;
use futures::StreamExt;
use futures::TryStreamExt;

use tokio_stream::wrappers::ReceiverStream;

use tonic::Status;

use crate::input::source::BucketSource;

pub struct PasteStream<B> {
    streams: Vec<B>,
}

impl<B> PasteStream<B>
where
    B: Stream + Send + Unpin + 'static,
    B::Item: Send,
{
    pub async fn into_pasted<M, T>(self, merger: M) -> impl Stream<Item = T>
    where
        T: Send + 'static,
        M: Fn(&[B::Item]) -> T + Send + 'static,
    {
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        tokio::spawn(async move {
            let mut streams: Vec<B> = self.streams;
            let sz: usize = streams.len();
            let mut buf: Vec<B::Item> = Vec::with_capacity(sz);
            loop {
                buf.clear();
                for s in &mut streams {
                    let next: Option<_> = s.next().await;
                    match next {
                        None => return,
                        Some(itm) => {
                            buf.push(itm);
                        }
                    }
                }
                let merged: T = merger(&buf);
                match tx.send(merged).await {
                    Ok(_) => {}
                    Err(_) => return,
                }
            }
        });
        ReceiverStream::new(rx)
    }
}

pub async fn streams2stream<S, M, T>(mut streams: Vec<S>, merger: M) -> impl Stream<Item = T>
where
    S: Stream + Send + Unpin + 'static,
    S::Item: Send,
    T: Send + 'static,
    M: Send + 'static + Fn(&[S::Item]) -> T,
{
    let (tx, rx) = tokio::sync::mpsc::channel(1);
    tokio::spawn(async move {
        let sz: usize = streams.len();
        let mut buf: Vec<S::Item> = Vec::with_capacity(sz);
        loop {
            buf.clear();
            for s in &mut streams {
                let next: Option<S::Item> = s.next().await;
                match next {
                    None => return,
                    Some(itm) => {
                        buf.push(itm);
                    }
                }
            }
            let merged: T = merger(&buf);
            match tx.send(merged).await {
                Ok(_) => {}
                Err(_) => return,
            }
        }
    });
    ReceiverStream::new(rx)
}

pub async fn buckets2stream<B, M, T>(
    buckets: Vec<B::Bucket>,
    merger: M,
    bs: &B,
) -> impl Stream<Item = T>
where
    B: BucketSource,
    B::All: Unpin,
    T: Send + 'static,
    M: Send + 'static + Fn(&[Result<(B::K, B::V), Status>]) -> T,
{
    let bstr = futures::stream::iter(buckets);
    let mapd = bstr.map(Ok::<_, Status>);
    let streams: Vec<_> = mapd
        .try_fold(vec![], |mut v, bkt| async move {
            let s = bs.get_all_by_bucket(bkt).await?;
            v.push(s);
            Ok(v)
        })
        .await
        .unwrap_or_default();
    streams2stream(streams, merger).await
}
