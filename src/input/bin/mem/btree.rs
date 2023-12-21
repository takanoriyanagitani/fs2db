use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use tonic::Status;

use crate::input::join::mem::btree::MemSource;

pub trait Nearest: Sync + Send + 'static {
    type K: Sync + Send + Ord;

    fn get_nearest(&self, k: &Self::K, m: &BTreeSet<Self::K>) -> Result<Self::K, Status>;
}

pub struct BinSource<M, N> {
    msrc: M,
    near: N,
}

#[tonic::async_trait]
impl<M, N> MemSource for BinSource<M, N>
where
    M: MemSource,
    M::K: Clone,
    N: Nearest<K = M::K>,
{
    type Bucket = (Arc<BTreeSet<Self::K>>, M::Bucket);
    type K = M::K;
    type V = M::V;

    async fn get_all_by_bucket(
        &self,
        b: Self::Bucket,
    ) -> Result<BTreeMap<Self::K, Self::V>, Status> {
        let (keys, bkt) = b;
        let br: &BTreeSet<Self::K> = &keys;
        let original: BTreeMap<Self::K, Self::V> = self.msrc.get_all_by_bucket(bkt).await?;
        let replaced: BTreeMap<Self::K, Self::V> =
            original
                .into_iter()
                .try_fold(BTreeMap::new(), |mut m, pair| {
                    let (key, val) = pair;
                    self.near.get_nearest(&key, br).map(|k: Self::K| {
                        m.insert(k, val);
                        m
                    })
                })?;
        Ok(replaced)
    }
}

pub fn mem_src_bin_new<M, N>(
    msrc: M,
    near: N,
) -> impl MemSource<Bucket = (Arc<BTreeSet<M::K>>, M::Bucket), K = M::K, V = M::V>
where
    M: MemSource,
    M::K: Clone,
    N: Nearest<K = M::K>,
{
    BinSource { msrc, near }
}

#[cfg(test)]
mod test_btree {
    mod mem_src_bin_new {
        mod empty {
            use std::collections::{BTreeMap, BTreeSet};
            use std::sync::Arc;

            use tonic::Status;

            use crate::input::join::mem::btree::MemSource;

            use crate::input::bin::mem::btree::Nearest;

            struct MemSrc {}
            #[tonic::async_trait]
            impl MemSource for MemSrc {
                type Bucket = String;
                type K = i64;
                type V = f64;

                async fn get_all_by_bucket(
                    &self,
                    _b: Self::Bucket,
                ) -> Result<BTreeMap<Self::K, Self::V>, Status> {
                    Ok(BTreeMap::new())
                }
            }

            struct N {}
            impl Nearest for N {
                type K = i64;

                fn get_nearest(
                    &self,
                    _k: &Self::K,
                    _m: &BTreeSet<Self::K>,
                ) -> Result<Self::K, Status> {
                    unimplemented!()
                }
            }

            #[tokio::test]
            async fn ensure_empty() {
                let msrc = MemSrc {}; // MemSource
                let near = N {}; // Nearest

                // MemSource
                let neo = crate::input::bin::mem::btree::mem_src_bin_new(msrc, near);
                let bkt = (Arc::new(BTreeSet::new()), "".into());
                let got: BTreeMap<i64, f64> = neo.get_all_by_bucket(bkt).await.unwrap();
                assert_eq!(0, got.len());
            }
        }

        mod simple {
            use std::collections::{BTreeMap, BTreeSet};
            use std::sync::Arc;

            use core::ops::Bound;

            use tonic::Status;

            use crate::input::join::mem::btree::MemSource;

            use crate::input::bin::mem::btree::Nearest;

            struct MemSrc {}
            #[tonic::async_trait]
            impl MemSource for MemSrc {
                type Bucket = String;
                type K = i64;
                type V = f64;

                async fn get_all_by_bucket(
                    &self,
                    _b: Self::Bucket,
                ) -> Result<BTreeMap<i64, f64>, Status> {
                    Ok(BTreeMap::from_iter(vec![(333, 599.0), (634, 3776.0)]))
                }
            }

            struct N {
                lbi_offset: i64,
                ubi_offset: i64,
            }
            impl Nearest for N {
                type K = i64;

                fn get_nearest(&self, k: &i64, m: &BTreeSet<i64>) -> Result<Self::K, Status> {
                    let lbi: i64 = k - self.lbi_offset;
                    let ubi: i64 = k + self.ubi_offset;
                    let mut found = m.range((Bound::Included(&lbi), Bound::Included(&ubi)));
                    found.next().copied().ok_or_else(|| {
                        Status::not_found(format!("no key found. key={k}, lbi={lbi}, ubi={ubi}"))
                    })
                }
            }

            #[tokio::test]
            async fn simple() {
                let msrc = MemSrc {}; // MemSource
                let near = N {
                    lbi_offset: 100,
                    ubi_offset: 100,
                }; // Nearest

                // MemSource
                let neo = crate::input::bin::mem::btree::mem_src_bin_new(msrc, near);

                let bkt = (Arc::new(BTreeSet::from_iter(vec![300, 600])), "".into());
                let got: BTreeMap<i64, f64> = neo.get_all_by_bucket(bkt).await.unwrap();
                assert_eq!(2, got.len());
                assert_eq!(&599.0, got.get(&300).unwrap());
                assert_eq!(&3776.0, got.get(&600).unwrap());
            }
        }
    }
}
