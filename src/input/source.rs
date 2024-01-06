use std::collections::BTreeMap;
use std::sync::Arc;

use futures::Stream;
use futures::StreamExt;
use futures::TryStreamExt;

use tokio_stream::wrappers::ReceiverStream;

use tonic::Status;

#[tonic::async_trait]
pub trait Source: Send + Sync + 'static {
    type Item;

    type All: Stream<Item = Result<Self::Item, Status>> + Send + 'static;

    async fn all(&self) -> Result<Self::All, Status>;
}

/// Source of buckets(a bucket has key/val pairs)
#[tonic::async_trait]
pub trait BucketSource: Send + Sync + 'static {
    type Bucket: Send + Sync;
    type K: Send + Sync;
    type V: Send + Sync;

    type All: Stream<Item = Result<(Self::K, Self::V), Status>> + Send + Unpin + 'static;

    /// Gets all key/val pairs from a bucket [`Self::Bucket`]
    async fn get_all_by_bucket(&self, b: Self::Bucket) -> Result<Self::All, Status>;
}

pub trait Mapper: Sync + Send + 'static {
    type IK: Send + Sync;
    type IV: Send + Sync;

    type OK: Send + Sync;
    type OV: Send + Sync;

    fn convert(&self, key: Self::IK, val: Self::IV) -> Result<(Self::OK, Self::OV), Status>;
}

pub struct BucketSrcMapd<M, B> {
    original: B,
    mapper: M,
}

#[tonic::async_trait]
impl<M, B> BucketSource for BucketSrcMapd<M, B>
where
    M: Clone + Mapper,
    B: BucketSource<K = M::IK, V = M::IV>,
{
    type Bucket = B::Bucket;
    type K = M::OK;
    type V = M::OV;

    type All = ReceiverStream<Result<(Self::K, Self::V), Status>>;

    /// Gets all key/val pairs from a bucket [`Self::Bucket`]
    async fn get_all_by_bucket(&self, b: Self::Bucket) -> Result<Self::All, Status> {
        let old: B::All = self.original.get_all_by_bucket(b).await?;
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        let mapper: M = self.mapper.clone();
        tokio::spawn(async move {
            let rt = &tx;
            let mapd = old.map(|rslt| {
                rslt.and_then(|pair| {
                    let (ik, iv) = pair;
                    mapper.convert(ik, iv)
                })
            });
            let _cnt: u64 = mapd
                .fold(0, |tot, rslt| async move {
                    rt.send(rslt).await.map(|_| 1 + tot).unwrap_or(tot)
                })
                .await;
        });
        Ok(ReceiverStream::new(rx))
    }
}

/// Creates a [`BucketSource`] from the original [`BucketSource`] and a [`Mapper`]
pub fn mapd_bkt_src_new<M, B>(
    mapper: M,
    original: B,
) -> impl BucketSource<Bucket = B::Bucket, K = M::OK, V = M::OV>
where
    M: Clone + Mapper,
    B: BucketSource<K = M::IK, V = M::IV>,
{
    BucketSrcMapd { original, mapper }
}

#[tonic::async_trait]
impl<A> BucketSource for Arc<A>
where
    A: BucketSource,
{
    type Bucket = A::Bucket;
    type K = A::K;
    type V = A::V;

    type All = A::All;

    async fn get_all_by_bucket(&self, b: Self::Bucket) -> Result<Self::All, Status> {
        let a: &A = self;
        a.get_all_by_bucket(b).await
    }
}

/// Merger which tries to create a merged(T) from inputs(A,B)
pub trait Merge: Send + Sync + 'static {
    type A: Send + Sync;
    type B: Send + Sync;
    type T: Send + Sync;

    fn merge(&self, a: Self::A, b: Self::B) -> Result<Self::T, Status>;
}

pub struct BucketMerge<A, B, M> {
    sa: A,
    sb: B,
    merger: M,
}

impl<A, B, M> BucketMerge<A, B, M>
where
    A: BucketSource,
    A::K: Ord,
    A::V: Clone,
{
    /// Creates [`BTreeMap`] from all key/val pairs in a bucket [`BucketSource::Bucket`]
    pub async fn to_map(&self, b: A::Bucket) -> Result<BTreeMap<A::K, A::V>, Status> {
        let all = self.sa.get_all_by_bucket(b).await?;
        all.try_fold(BTreeMap::new(), |mut m, pair| async move {
            let (k, v) = pair;
            m.insert(k, v);
            Ok(m)
        })
        .await
    }
}

#[tonic::async_trait]
impl<A, B, M> BucketSource for BucketMerge<A, B, M>
where
    A: BucketSource<Bucket = ()>,
    A::K: Ord,
    A::V: Clone,
    B: BucketSource<K = A::K>,
    M: Clone + Merge<A = A::V, B = B::V>,
{
    type Bucket = B::Bucket;
    type K = A::K;
    type V = M::T;
    type All = ReceiverStream<Result<(Self::K, Self::V), Status>>;

    async fn get_all_by_bucket(&self, b: B::Bucket) -> Result<Self::All, Status> {
        let am: BTreeMap<A::K, A::V> = self.to_map(()).await?;
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        let bs = self.sb.get_all_by_bucket(b).await?;
        let m: M = self.merger.clone();
        let mf = move |a: A::V, b: B::V| m.merge(a, b);
        tokio::spawn(async move {
            let rm = &am;
            let mapd = bs.map(|r| {
                r.and_then(|pair| {
                    let (k, v) = pair;
                    let av: A::V = rm
                        .get(&k)
                        .cloned()
                        .ok_or_else(|| Status::invalid_argument("no val found"))?;
                    let merged: M::T = mf(av, v)?;
                    Ok((k, merged))
                })
            });
            let rt = &tx;
            let _cnt: u64 = mapd
                .fold(0, |tot, r| async move {
                    rt.send(r).await.map(|_| 1 + tot).unwrap_or(tot)
                })
                .await;
        });
        Ok(ReceiverStream::new(rx))
    }
}

/// Creates a merged [`BucketSource`] by merging sa, sb.
///
/// ## Arguments
/// - sa: A [`BucketSource`] which has few key/val pairs
/// - sb: A [`BucketSource`] which may have many key/val pairs
/// - merger: A [`Merge`] which creates merged value from sa/sb using BTreeMap
pub fn bkt_src_merged_new<A, B, M>(
    sa: A,
    sb: B,
    merger: M,
) -> impl BucketSource<
    Bucket = B::Bucket,
    K = A::K,
    V = M::T,
    All = ReceiverStream<Result<(A::K, M::T), Status>>,
>
where
    A: BucketSource<Bucket = ()>,
    A::K: Ord,
    A::V: Clone,
    B: BucketSource<K = A::K>,
    M: Clone + Merge<A = A::V, B = B::V>,
{
    BucketMerge { sa, sb, merger }
}
