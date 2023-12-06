use core::fmt::Debug;

use std::collections::BTreeMap;

use futures::StreamExt;
use futures::TryStreamExt;

use tokio_stream::wrappers::ReceiverStream;

use tonic::Status;

use crate::input::merge::svc::{Merge, MergeSource};
use crate::input::source::Source;

#[derive(Clone)]
pub struct MergedSvc<A, B, M>
where
    A: Clone,
    B: Clone,
    M: Clone,
{
    sa: A,
    sb: B,
    merge: M,
}

impl<A, B, M> MergedSvc<A, B, M>
where
    A: Clone + MergeSource,
    B: Clone + MergeSource<K = <A as MergeSource>::K>,
    M: Clone + Merge<A = <A as MergeSource>::V, B = <B as MergeSource>::V>,
    A::K: Ord + Debug,
{
    pub fn merge(&self, a: A::V, b: B::V) -> Result<M::T, Status> {
        self.merge.merge(a, b)
    }
}

#[tonic::async_trait]
impl<A, B, M> Source for MergedSvc<A, B, M>
where
    A: Clone + MergeSource,
    B: Clone + MergeSource<K = <A as MergeSource>::K>,
    M: Clone + Merge<A = <A as MergeSource>::V, B = <B as MergeSource>::V>,
    A::K: Ord + Debug,
    A::V: Clone,
    Self: Clone,
{
    type Item = M::T;
    type All = ReceiverStream<Result<Self::Item, Status>>;

    async fn all(&self) -> Result<Self::All, Status> {
        let a: A::All = self.sa.all().await?;
        let amap: BTreeMap<A::K, A::V> = a
            .try_fold(BTreeMap::new(), |mut m, pair| async move {
                let (ak, av) = pair;
                m.insert(ak, av);
                Ok(m)
            })
            .await?;

        let b: B::All = self.sb.all().await?;
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        let s: Self = self.clone();
        tokio::spawn(async move {
            let rs: &Self = &s;
            let mapd = b.map(|r| {
                r.and_then(|pair| {
                    let (bk, bv) = pair;
                    let av: A::V = amap.get(&bk).cloned().ok_or_else(|| {
                        Status::invalid_argument(format!("no value found for key {bk:#?}"))
                    })?;
                    let merged = rs.merge(av, bv)?;
                    Ok(merged)
                })
            });
            let rtx = &tx;
            let _cnt: u64 = mapd
                .fold(0, |tot, r| async move {
                    match rtx.send(r).await {
                        Ok(_) => 1 + tot,
                        Err(_) => tot,
                    }
                })
                .await;
        });
        Ok(ReceiverStream::new(rx))
    }
}

/// Creates a Source using two sources.
///
/// ## Arguments
/// - sa: A source with fewer elements(its contents will be stored in a BTreeMap)
/// - sb: A source which may contain many elements
/// - merger: A merger which merges a value of sa and a value of sb
pub fn source_new_merged<A, B, M>(sa: A, sb: B, merger: M) -> impl Source<Item = M::T>
where
    A: Clone + MergeSource,
    B: Clone + MergeSource<K = <A as MergeSource>::K>,
    M: Clone + Merge<A = <A as MergeSource>::V, B = <B as MergeSource>::V>,
    A::K: Ord + Debug,
    A::V: Clone,
{
    MergedSvc {
        sa,
        sb,
        merge: merger,
    }
}
