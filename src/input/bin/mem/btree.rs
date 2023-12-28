use core::ops::{RangeBounds, RangeInclusive};

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use tonic::Status;

use crate::input::join::mem::btree::MemSource;

/// Key Replacer
pub trait Nearest: Sync + Send + 'static {
    type K: Sync + Send + Ord;

    /// Finds a key which has minimum difference
    /// ## Arguments
    /// - k: The original key
    /// - m: Keys which might have a key that has minimum difference
    fn get_nearest(&self, k: &Self::K, m: &BTreeSet<Self::K>) -> Result<Self::K, Status>;
}

/// Range Container
pub trait Range: Sync + Send + 'static {
    type K: Sync + Send + Ord;
    type R: RangeBounds<Self::K>;

    /// Gets RangeBounds around a key(e.g, k-100..=k+100)
    fn range(&self, key: &Self::K) -> Self::R;
}

/// Gets a key with nearest "Score"(e.g, gets a key with minimum score)
pub trait ComputeNearest: Sync + Send + 'static {
    type K: Sync + Send + Ord;
    type Score: Sync + Send + Ord;

    /// Gets a "nearest" key from (score, key) pairs.
    fn nearest<I>(&self, pairs: I) -> Result<Self::K, Status>
    where
        I: Iterator<Item = (Self::Score, Self::K)>;
}

/// Computes a score by comparing keys
pub trait ComputeDiff: Sync + Send + 'static {
    type K: Sync + Send + Ord;
    type Score: Sync + Send + Ord;

    fn compute_score(&self, a: &Self::K, b: &Self::K) -> Self::Score;
}

impl<T> Nearest for T
where
    T: Range,
    <T as Range>::K: Clone,
    T: ComputeNearest<K = <T as Range>::K>,
    T: ComputeDiff<K = <T as Range>::K, Score = <T as ComputeNearest>::Score>,
{
    type K = <T as Range>::K;

    fn get_nearest(&self, k: &Self::K, m: &BTreeSet<Self::K>) -> Result<Self::K, Status> {
        let range: T::R = self.range(k);
        let keys = m.range(range);
        let pairs = keys.map(|key: &Self::K| (self.compute_score(k, key), key.clone()));
        self.nearest(pairs)
    }
}

/// Implements [`ComputeDiff`], [`ComputeNearest`], [`Range`]
pub struct RangeII<K> {
    pub lbi_offset: K,
    pub ubi_offset: K,
}

impl<K> Clone for RangeII<K>
where
    K: Clone,
{
    fn clone(&self) -> Self {
        Self {
            lbi_offset: self.lbi_offset.clone(),
            ubi_offset: self.ubi_offset.clone(),
        }
    }
}

macro_rules! range_impl {
    ($key: ty) => {
        impl Range for RangeII<$key> {
            type K = $key;
            type R = RangeInclusive<Self::K>;

            fn range(&self, key: &Self::K) -> Self::R {
                let lbi: Self::K = key.saturating_sub(self.lbi_offset);
                let ubi: Self::K = key.saturating_add(self.ubi_offset);
                lbi..=ubi
            }
        }
    };
}

macro_rules! range_impl_many {
    ($($key: ty)*) => ($(
        range_impl!($key);
    )*)
}

range_impl_many!(u8 u16 u32 u64 u128 usize);
range_impl_many!(i8 i16 i32 i64 i128 isize);

macro_rules! cdiff_impl {
    ($key: ty, $score: ty) => {
        impl ComputeDiff for RangeII<$key> {
            type K = $key;
            type Score = $score;

            fn compute_score(&self, a: &Self::K, b: &Self::K) -> Self::Score {
                a.abs_diff(*b)
            }
        }
    };
}

macro_rules! cdiff_impl_u_many {
    ($($key: ty)*) => ($(
        cdiff_impl!($key, $key);
    )*)
}

cdiff_impl_u_many!(u8 u16 u32 u64 u128 usize);

cdiff_impl!(i8, u8);
cdiff_impl!(i16, u16);
cdiff_impl!(i32, u32);
cdiff_impl!(i64, u64);
cdiff_impl!(i128, u128);
cdiff_impl!(isize, usize);

macro_rules! cnear_impl {
    ($key: ty, $score: ty) => {
        impl ComputeNearest for RangeII<$key> {
            type K = $key;
            type Score = $score;

            fn nearest<I>(&self, pairs: I) -> Result<Self::K, Status>
            where
                I: Iterator<Item = (Self::Score, Self::K)>,
            {
                let min: Option<(Self::Score, Self::K)> = pairs.fold(None, |o, pair| match o {
                    None => Some(pair),
                    Some(old) => {
                        let (oscore, okey) = old;
                        let (score, key) = pair;
                        match oscore < score {
                            true => Some((oscore, okey)),
                            false => Some((score, key)),
                        }
                    }
                });
                min.map(|p| p.1)
                    .ok_or_else(|| Status::invalid_argument("empty pairs"))
            }
        }
    };
}

macro_rules! cnear_impl_u_many {
    ($($key: ty)*) => ($(
        cnear_impl!($key, $key);
    )*)
}

cnear_impl_u_many!(u8 u16 u32 u64 u128 usize);

cnear_impl!(i8, u8);
cnear_impl!(i16, u16);
cnear_impl!(i32, u32);
cnear_impl!(i64, u64);
cnear_impl!(i128, u128);
cnear_impl!(isize, usize);

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

/// Creates a [`MemSource`] from [`Nearest`] and [`MemSource`]
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

    mod range_ii {
        mod empty {
            use std::collections::BTreeSet;

            use crate::input::bin::mem::btree::{Nearest, RangeII};

            #[tokio::test]
            async fn ensure_empty() {
                let r = RangeII {
                    lbi_offset: 100,
                    ubi_offset: 100,
                };

                let set: BTreeSet<i32> = BTreeSet::new();
                let rslt = r.get_nearest(&0, &set);
                assert_eq!(true, rslt.is_err());
            }
        }

        mod simple {
            use std::collections::BTreeSet;

            use crate::input::bin::mem::btree::{Nearest, RangeII};

            #[tokio::test]
            async fn ensure_empty() {
                let r = RangeII {
                    lbi_offset: 100,
                    ubi_offset: 100,
                };

                let set: BTreeSet<i32> = BTreeSet::from_iter(vec![99, 500]);

                let got: i32 = r.get_nearest(&50, &set).unwrap();
                assert_eq!(99, got);

                let got: i32 = r.get_nearest(&150, &set).unwrap();
                assert_eq!(99, got);

                let got: i32 = r.get_nearest(&450, &set).unwrap();
                assert_eq!(500, got);

                let got: i32 = r.get_nearest(&550, &set).unwrap();
                assert_eq!(500, got);

                let rslt = r.get_nearest(&1000, &set);
                assert_eq!(true, rslt.is_err());

                let rslt = r.get_nearest(&-100, &set);
                assert_eq!(true, rslt.is_err());
            }
        }
    }
}
