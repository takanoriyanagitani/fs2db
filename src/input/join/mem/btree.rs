use std::collections::BTreeMap;

use futures::StreamExt;
use futures::TryStreamExt;

use tokio_stream::wrappers::ReceiverStream;

use tonic::Status;

use crate::input::source::BucketSource;

#[tonic::async_trait]
pub trait MemSource: Sync + Send + 'static {
    type Bucket: Send + Sync + Ord + Clone;

    type K: Send + Sync + Ord;
    type V: Send + Sync;

    async fn get_all_by_bucket(
        &self,
        b: Self::Bucket,
    ) -> Result<BTreeMap<Self::K, Self::V>, Status>;
}

#[tonic::async_trait]
impl<B> MemSource for B
where
    B: BucketSource,
    B::Bucket: Clone + Ord,
    B::K: Ord,
{
    type Bucket = B::Bucket;

    type K = B::K;
    type V = B::V;

    async fn get_all_by_bucket(
        &self,
        b: Self::Bucket,
    ) -> Result<BTreeMap<Self::K, Self::V>, Status> {
        let rows: B::All = BucketSource::get_all_by_bucket(self, b).await?;
        rows.try_fold(BTreeMap::new(), |mut m, pair| async move {
            let (key, val) = pair;
            m.insert(key, val);
            Ok(m)
        })
        .await
    }
}

pub fn mem_src_new<B>(bsrc: B) -> impl MemSource<Bucket = B::Bucket, K = B::K, V = B::V>
where
    B: BucketSource,
    B::Bucket: Clone + Ord,
    B::K: Ord,
{
    bsrc
}

pub trait Merger: Sync + Send + 'static {
    type Bucket: Send + Sync + Ord + Clone;

    type K: Send + Sync + Ord;

    type T: Send + Sync;
    type U: Send + Sync;

    fn merge(
        &self,
        key: Self::K,
        inputs: &BTreeMap<Self::Bucket, BTreeMap<Self::K, Self::T>>,
    ) -> Result<Self::U, Status>;
}

pub trait Grouper: Sync + Send + 'static {
    type BucketI: Send + Sync + Ord;
    type BucketO: Send + Sync + Ord + Clone;

    type K: Send + Sync + Ord;

    type T: Send + Sync;

    fn conv(&self, i: &Self::BucketI, t: &Self::T) -> Result<(Self::BucketO, Self::T), Status>;

    fn group(
        &self,
        key: Self::K,
        inputs: &BTreeMap<Self::BucketI, BTreeMap<Self::K, Self::T>>,
    ) -> Result<BTreeMap<Self::BucketO, Self::T>, Status> {
        inputs.keys().try_fold(BTreeMap::new(), |mut m, bi| {
            let kt: &BTreeMap<Self::K, Self::T> = inputs
                .get(bi)
                .ok_or_else(|| Status::invalid_argument("map for a bucket not found"))?;
            let ot: Option<&Self::T> = kt.get(&key);
            let op: Option<_> = match ot {
                None => None,
                Some(t) => Some(self.conv(bi, t)?),
            };
            match op {
                None => Ok(m),
                Some(pair) => {
                    let (bo, t) = pair;
                    m.insert(bo, t);
                    Ok(m)
                }
            }
        })
    }
}

impl<G> Merger for G
where
    G: Grouper,
    G::BucketI: Clone,
{
    type Bucket = <G as Grouper>::BucketI;

    type K = <G as Grouper>::K;

    type T = <G as Grouper>::T;
    type U = BTreeMap<<G as Grouper>::BucketO, Self::T>;

    fn merge(
        &self,
        key: Self::K,
        inputs: &BTreeMap<Self::Bucket, BTreeMap<Self::K, Self::T>>,
    ) -> Result<Self::U, Status> {
        self.group(key, inputs)
    }
}

pub struct MergedSource<M, S> {
    merger: M,
    source: S,
}

#[tonic::async_trait]
impl<M, S> BucketSource for MergedSource<M, S>
where
    M: Merger + Clone,
    S::K: Clone,
    S: MemSource<Bucket = M::Bucket, K = M::K, V = M::T>,
{
    type Bucket = (Vec<S::K>, Vec<S::Bucket>);
    type K = S::K;
    type V = M::U;

    type All = ReceiverStream<Result<(Self::K, Self::V), Status>>;

    async fn get_all_by_bucket(&self, b: Self::Bucket) -> Result<Self::All, Status> {
        let (keys, buckets) = b;
        let bs = futures::stream::iter(buckets).map(Ok::<_, Status>);
        let bmap: BTreeMap<S::Bucket, BTreeMap<S::K, S::V>> = bs
            .try_fold(BTreeMap::new(), |mut m, bkt| async move {
                let bkv: BTreeMap<S::K, S::V> = self.source.get_all_by_bucket(bkt.clone()).await?;
                m.insert(bkt, bkv);
                Ok(m)
            })
            .await?;
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        let mc: M = self.merger.clone();
        tokio::spawn(async move {
            let rt = &tx;
            let rb: &BTreeMap<_, _> = &bmap;
            let ks = futures::stream::iter(keys);
            let mr: &M = &mc;
            let mapd = ks.map(|k: S::K| {
                let ru = mr.merge(k.clone(), rb);
                ru.map(|u: Self::V| (k, u))
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

pub fn merged_src_new<M, S>(
    merger: M,
    source: S,
) -> impl BucketSource<Bucket = (Vec<S::K>, Vec<S::Bucket>), K = S::K, V = M::U>
where
    M: Merger + Clone,
    S::K: Clone,
    S: MemSource<Bucket = M::Bucket, K = M::K, V = M::T>,
{
    MergedSource { merger, source }
}

pub fn grouped_src_new<G, S>(
    grouper: G,
    source: S,
) -> impl BucketSource<Bucket = (Vec<S::K>, Vec<S::Bucket>), K = S::K, V = BTreeMap<G::BucketO, G::T>>
where
    G: Grouper + Clone,
    G::BucketI: Clone,
    S::K: Clone,
    S: MemSource<Bucket = G::BucketI, K = G::K, V = G::T>,
{
    let merger = grouper;
    MergedSource { merger, source }
}

#[cfg(test)]
mod test_btree {
    mod merged_src_new {
        mod empty {
            use std::collections::BTreeMap;

            use futures::StreamExt;

            use tonic::Status;

            use crate::input::source::BucketSource;

            use crate::input::join::mem::btree::{MemSource, Merger};

            #[derive(Clone)]
            struct M {}
            impl Merger for M {
                type Bucket = String;
                type K = i64;
                type T = String;
                type U = (String, i64, BTreeMap<String, String>);

                fn merge(
                    &self,
                    _key: Self::K,
                    _inputs: &BTreeMap<Self::Bucket, BTreeMap<Self::K, Self::T>>,
                ) -> Result<Self::U, Status> {
                    unimplemented!()
                }
            }

            struct S {}
            #[tonic::async_trait]
            impl MemSource for S {
                type Bucket = String;
                type K = i64;
                type V = String;

                async fn get_all_by_bucket(
                    &self,
                    _b: Self::Bucket,
                ) -> Result<BTreeMap<Self::K, Self::V>, Status> {
                    Ok(BTreeMap::new())
                }
            }

            #[tokio::test]
            async fn ensure_empty() {
                let mgr = M {};
                let src = S {};
                let bs = crate::input::join::mem::btree::merged_src_new(mgr, src);

                let bkt = (vec![], vec![]);
                let got = bs.get_all_by_bucket(bkt).await.unwrap();
                let cnt: usize = got.count().await;
                assert_eq!(0, cnt);
            }
        }

        mod week {
            /*

               table A: tue_2023_12_19
               | item type | hour | cnt |
               |:---------:|:----:|:---:|
               | apple     | 11   | 42  |
               | banana    | 11   | 49  |
               | candy     | 11   | 49  |
               | apple     | 12   | 47  |
               | banana    | 12   | 49  |
               | candy     | 12   | 42  |
               | apple     | 13   | 44  |
               | banana    | 13   | 45  |
               | candy     | 13   | 48  |

               table B: mon_2023_12_18
               | item type | hour | cnt |
               |:---------:|:----:|:---:|
               | apple     | 11   | 634 |
               | banana    | 11   | 614 |
               | candy     | 11   | 644 |
               | apple     | 12   | 614 |
               | banana    | 12   | 654 |
               | candy     | 12   | 694 |
               | apple     | 13   | 624 |
               | banana    | 13   | 664 |
               | candy     | 13   | 654 |

               table C: sun_2023_12_17
               | item type | hour | cnt |
               |:---------:|:----:|:---:|
               | apple     | 11   | 333 |
               | banana    | 11   | 331 |
               | candy     | 11   | 334 |
               | apple     | 12   | 331 |
               | banana    | 12   | 335 |
               | candy     | 12   | 339 |
               | apple     | 13   | 332 |
               | banana    | 13   | 336 |
               | candy     | 13   | 335 |

               merged table
               | item type | hour | value                    |
               |:---------:|:----:|:------------------------:|
               | apple     | 11   | sun:333, mon:634, tue:42 |
               | banana    | 11   | sun:331, mon:614, tue:49 |
               | candy     | 11   | sun:334, mon:644, tue:49 |
               | apple     | 12   | sun:331, mon:614, tue:47 |
               | banana    | 12   | sun:335, mon:654, tue:49 |
               | candy     | 12   | sun:339, mon:694, tue:42 |
               | apple     | 13   | sun:332, mon:624, tue:44 |
               | banana    | 13   | sun:336, mon:664, tue:45 |
               | candy     | 13   | sun:335, mon:654, tue:48 |

            */
            use std::collections::BTreeMap;

            use futures::StreamExt;

            use tonic::Status;

            use crate::input::source::BucketSource;

            use crate::input::join::mem::btree::{MemSource, Merger};

            #[derive(Clone)]
            struct M {}
            impl Merger for M {
                type Bucket = (&'static str, String); // e.g, ("tue", "2023/12/19")
                type K = (String, u8); // e.g, ("apple", 11)
                type T = (i64, f64); // e.g, (42, 3.0)
                type U = BTreeMap<&'static str, Self::T>;

                fn merge(
                    &self,
                    key: Self::K,
                    inputs: &BTreeMap<Self::Bucket, BTreeMap<Self::K, Self::T>>,
                ) -> Result<Self::U, Status> {
                    let keys = inputs.keys();
                    Ok(keys.fold(BTreeMap::new(), |mut m, bkt| {
                        let week: &str = bkt.0;
                        let om: Option<&BTreeMap<Self::K, _>> = inputs.get(&bkt);
                        let ot: Option<Self::T> = om.and_then(|b| b.get(&key)).copied();
                        match ot {
                            None => m,
                            Some(t) => {
                                m.insert(week, t);
                                m
                            }
                        }
                    }))
                }
            }

            #[derive(Default)]
            struct S {
                internal: BTreeMap<(&'static str, String), BTreeMap<(String, u8), (i64, f64)>>,
            }
            #[tonic::async_trait]
            impl MemSource for S {
                type Bucket = (&'static str, String);
                type K = (String, u8); // e.g, ("apple", 11)
                type V = (i64, f64); // e.g, (42, 3.0)

                async fn get_all_by_bucket(
                    &self,
                    b: Self::Bucket,
                ) -> Result<BTreeMap<Self::K, Self::V>, Status> {
                    self.internal
                        .get(&b)
                        .cloned()
                        .ok_or_else(|| Status::not_found(format!("no such bucket: {}", b.0)))
                }
            }

            #[tokio::test]
            async fn week_test() {
                let m: M = M {};
                let mut s: S = S::default();
                s.internal = BTreeMap::from_iter(vec![
                    (
                        ("sun", "2023/12/17".into()),
                        BTreeMap::from_iter(vec![
                            (("apple".into(), 11), (42, 3.0)),
                            (("banana".into(), 11), (49, 1.0)),
                            (("candy".into(), 11), (49, 4.0)),
                            (("apple".into(), 12), (47, 1.0)),
                            (("banana".into(), 12), (49, 5.0)),
                            (("candy".into(), 12), (42, 9.0)),
                            (("apple".into(), 13), (44, 2.0)),
                            (("banana".into(), 13), (45, 6.0)),
                            (("candy".into(), 13), (48, 5.0)),
                        ]),
                    ),
                    (
                        ("mon", "2023/12/18".into()),
                        BTreeMap::from_iter(vec![
                            (("apple".into(), 11), (634, 3.0)),
                            (("banana".into(), 11), (614, 1.0)),
                            (("candy".into(), 11), (644, 4.0)),
                            (("apple".into(), 12), (614, 1.0)),
                            (("banana".into(), 12), (654, 5.0)),
                            (("candy".into(), 12), (694, 9.0)),
                            (("apple".into(), 13), (624, 2.0)),
                            (("banana".into(), 13), (664, 6.0)),
                            (("candy".into(), 13), (654, 5.0)),
                        ]),
                    ),
                    (
                        ("tue", "2023/12/19".into()),
                        BTreeMap::from_iter(vec![
                            (("apple".into(), 11), (333, 3.0)),
                            (("banana".into(), 11), (331, 1.0)),
                            (("candy".into(), 11), (334, 4.0)),
                            (("apple".into(), 12), (331, 1.0)),
                            (("banana".into(), 12), (335, 5.0)),
                            (("candy".into(), 12), (339, 9.0)),
                            (("apple".into(), 13), (332, 2.0)),
                            (("banana".into(), 13), (336, 6.0)),
                            (("candy".into(), 13), (335, 5.0)),
                        ]),
                    ),
                ]);
                let bs = crate::input::join::mem::btree::merged_src_new(m, s);
                let bkt = (
                    vec![
                        ("apple".into(), 11),
                        ("apple".into(), 12),
                        ("apple".into(), 13),
                        ("candy".into(), 11),
                        ("candy".into(), 12),
                        ("candy".into(), 13),
                    ],
                    vec![
                        ("sun", "2023/12/17".into()),
                        ("mon", "2023/12/18".into()),
                        ("tue", "2023/12/19".into()),
                    ],
                );
                let got = bs.get_all_by_bucket(bkt).await.unwrap();
                let folded: Vec<_> = got
                    .fold(vec![], |mut v, rslt| async move {
                        v.push(rslt.unwrap());
                        v
                    })
                    .await;
                let mut i = folded.into_iter();

                let itm: ((String, u8), BTreeMap<&str, (i64, f64)>) = i.next().unwrap();
                assert_eq!(("apple".into(), 11), itm.0);
                assert_eq!(&(42, 3.0), itm.1.get("sun").unwrap());
                assert_eq!(&(634, 3.0), itm.1.get("mon").unwrap());
                assert_eq!(&(333, 3.0), itm.1.get("tue").unwrap());

                let itm: ((String, u8), BTreeMap<&str, (i64, f64)>) = i.next().unwrap();
                assert_eq!(("apple".into(), 12), itm.0);
                assert_eq!(&(47, 1.0), itm.1.get("sun").unwrap());
                assert_eq!(&(614, 1.0), itm.1.get("mon").unwrap());
                assert_eq!(&(331, 1.0), itm.1.get("tue").unwrap());

                let itm: ((String, u8), BTreeMap<&str, (i64, f64)>) = i.next().unwrap();
                assert_eq!(("apple".into(), 13), itm.0);
                assert_eq!(&(44, 2.0), itm.1.get("sun").unwrap());
                assert_eq!(&(624, 2.0), itm.1.get("mon").unwrap());

                let itm: ((String, u8), BTreeMap<&str, (i64, f64)>) = i.next().unwrap();
                assert_eq!(("candy".into(), 11), itm.0);
                assert_eq!(&(49, 4.0), itm.1.get("sun").unwrap());
                assert_eq!(&(644, 4.0), itm.1.get("mon").unwrap());
                assert_eq!(&(334, 4.0), itm.1.get("tue").unwrap());
            }
        }
    }

    mod grouped_src_new {
        mod empty {
            use std::collections::BTreeMap;

            use futures::StreamExt;

            use tonic::Status;

            use crate::input::source::BucketSource;

            use crate::input::join::mem::btree::{Grouper, MemSource};

            #[derive(Clone)]
            struct G {}
            impl Grouper for G {
                type BucketI = (u64, String);
                type BucketO = u64;
                type K = i64;
                type T = (i32, f64);

                fn conv(
                    &self,
                    _i: &Self::BucketI,
                    _t: &Self::T,
                ) -> Result<(Self::BucketO, Self::T), Status> {
                    unimplemented!()
                }
            }

            struct S {}
            #[tonic::async_trait]
            impl MemSource for S {
                type Bucket = (u64, String);
                type K = i64;
                type V = (i32, f64);

                async fn get_all_by_bucket(
                    &self,
                    _b: Self::Bucket,
                ) -> Result<BTreeMap<Self::K, Self::V>, Status> {
                    Ok(BTreeMap::new())
                }
            }

            #[tokio::test]
            async fn ensure_empty() {
                let g: G = G {};
                let s: S = S {};
                let bs = crate::input::join::mem::btree::grouped_src_new(g, s);
                let bkt = (vec![], vec![]);
                let rslt = bs.get_all_by_bucket(bkt).await.unwrap();
                let cnt: usize = rslt.count().await;
                assert_eq!(0, cnt);
            }
        }

        mod week {
            /*

                 table A: tue_2023_12_19
                 | item type | hour | cnt |
                 |:---------:|:----:|:---:|
                 | apple     | 11   | 42  |
                 | banana    | 11   | 49  |
                 | candy     | 11   | 49  |
                 | apple     | 12   | 47  |
                 | banana    | 12   | 49  |
                 | candy     | 12   | 42  |
                 | apple     | 13   | 44  |
                 | banana    | 13   | 45  |
                 | candy     | 13   | 48  |

                 table B: mon_2023_12_18
                 | item type | hour | cnt |
                 |:---------:|:----:|:---:|
                 | apple     | 11   | 634 |
                 | banana    | 11   | 614 |
                 | candy     | 11   | 644 |
                 | apple     | 12   | 614 |
                 | banana    | 12   | 654 |
                 | candy     | 12   | 694 |
                 | apple     | 13   | 624 |
                 | banana    | 13   | 664 |
                 | candy     | 13   | 654 |

                 table C: sun_2023_12_17
                 | item type | hour | cnt |
                 |:---------:|:----:|:---:|
                 | apple     | 11   | 333 |
                 | banana    | 11   | 331 |
                 | candy     | 11   | 334 |
                 | apple     | 12   | 331 |
                 | banana    | 12   | 335 |
                 | candy     | 12   | 339 |
                 | apple     | 13   | 332 |
                 | banana    | 13   | 336 |
                 | candy     | 13   | 335 |

                 merged table
                 | item type | hour | value                    |
                 |:---------:|:----:|:------------------------:|
                 | apple     | 11   | sun:333, mon:634, tue:42 |
                 | banana    | 11   | sun:331, mon:614, tue:49 |
                 | candy     | 11   | sun:334, mon:644, tue:49 |
                 | apple     | 12   | sun:331, mon:614, tue:47 |
                 | banana    | 12   | sun:335, mon:654, tue:49 |
                 | candy     | 12   | sun:339, mon:694, tue:42 |
                 | apple     | 13   | sun:332, mon:624, tue:44 |
                 | banana    | 13   | sun:336, mon:664, tue:45 |
                 | candy     | 13   | sun:335, mon:654, tue:48 |

            */
            use std::collections::BTreeMap;

            use futures::StreamExt;

            use tonic::Status;

            use crate::input::source::BucketSource;

            use crate::input::join::mem::btree::{Grouper, MemSource};

            #[derive(Clone)]
            struct G {}
            impl Grouper for G {
                type BucketI = (&'static str, String); // e.g, ("tue", "2023/12/19")
                type BucketO = &'static str; // e.g, "tue"
                type K = (String, u8); // e.g, ("apple", 11)
                type T = (i64, f64); // e.g, (42, 3.0)

                fn conv(
                    &self,
                    i: &Self::BucketI,
                    t: &Self::T,
                ) -> Result<(Self::BucketO, Self::T), Status> {
                    let wk: &str = i.0;
                    Ok((wk, *t))
                }
            }

            #[derive(Default)]
            struct S {
                internal: BTreeMap<(&'static str, String), BTreeMap<(String, u8), (i64, f64)>>,
            }
            #[tonic::async_trait]
            impl MemSource for S {
                type Bucket = (&'static str, String);
                type K = (String, u8); // e.g, ("apple", 11)
                type V = (i64, f64); // e.g, (42, 3.0)

                async fn get_all_by_bucket(
                    &self,
                    b: Self::Bucket,
                ) -> Result<BTreeMap<Self::K, Self::V>, Status> {
                    self.internal
                        .get(&b)
                        .cloned()
                        .ok_or_else(|| Status::not_found(format!("no such bucket: {}", b.0)))
                }
            }

            #[tokio::test]
            async fn week_test() {
                let g: G = G {};
                let mut s: S = S::default();
                s.internal = BTreeMap::from_iter(vec![
                    (
                        ("sun", "2023/12/17".into()),
                        BTreeMap::from_iter(vec![
                            (("apple".into(), 11), (42, 3.0)),
                            (("banana".into(), 11), (49, 1.0)),
                            (("candy".into(), 11), (49, 4.0)),
                            (("apple".into(), 12), (47, 1.0)),
                            (("banana".into(), 12), (49, 5.0)),
                            (("candy".into(), 12), (42, 9.0)),
                            (("apple".into(), 13), (44, 2.0)),
                            (("banana".into(), 13), (45, 6.0)),
                            (("candy".into(), 13), (48, 5.0)),
                        ]),
                    ),
                    (
                        ("mon", "2023/12/18".into()),
                        BTreeMap::from_iter(vec![
                            (("apple".into(), 11), (634, 3.0)),
                            (("banana".into(), 11), (614, 1.0)),
                            (("candy".into(), 11), (644, 4.0)),
                            (("apple".into(), 12), (614, 1.0)),
                            (("banana".into(), 12), (654, 5.0)),
                            (("candy".into(), 12), (694, 9.0)),
                            (("apple".into(), 13), (624, 2.0)),
                            (("banana".into(), 13), (664, 6.0)),
                            (("candy".into(), 13), (654, 5.0)),
                        ]),
                    ),
                    (
                        ("tue", "2023/12/19".into()),
                        BTreeMap::from_iter(vec![
                            (("apple".into(), 11), (333, 3.0)),
                            (("banana".into(), 11), (331, 1.0)),
                            (("candy".into(), 11), (334, 4.0)),
                            (("apple".into(), 12), (331, 1.0)),
                            (("banana".into(), 12), (335, 5.0)),
                            (("candy".into(), 12), (339, 9.0)),
                            (("apple".into(), 13), (332, 2.0)),
                            (("banana".into(), 13), (336, 6.0)),
                            (("candy".into(), 13), (335, 5.0)),
                        ]),
                    ),
                ]);
                let bs = crate::input::join::mem::btree::grouped_src_new(g, s);
                let bkt = (
                    vec![
                        ("apple".into(), 11),
                        ("apple".into(), 12),
                        ("apple".into(), 13),
                        ("candy".into(), 11),
                        ("candy".into(), 12),
                        ("candy".into(), 13),
                    ],
                    vec![
                        ("sun", "2023/12/17".into()),
                        ("mon", "2023/12/18".into()),
                        ("tue", "2023/12/19".into()),
                    ],
                );
                let got = bs.get_all_by_bucket(bkt).await.unwrap();
                let folded: Vec<_> = got
                    .fold(vec![], |mut v, rslt| async move {
                        v.push(rslt.unwrap());
                        v
                    })
                    .await;
                let mut i = folded.into_iter();

                let itm: ((String, u8), BTreeMap<&str, (i64, f64)>) = i.next().unwrap();
                assert_eq!(("apple".into(), 11), itm.0);
                assert_eq!(&(42, 3.0), itm.1.get("sun").unwrap());
                assert_eq!(&(634, 3.0), itm.1.get("mon").unwrap());
                assert_eq!(&(333, 3.0), itm.1.get("tue").unwrap());

                let itm: ((String, u8), BTreeMap<&str, (i64, f64)>) = i.next().unwrap();
                assert_eq!(("apple".into(), 12), itm.0);
                assert_eq!(&(47, 1.0), itm.1.get("sun").unwrap());
                assert_eq!(&(614, 1.0), itm.1.get("mon").unwrap());
                assert_eq!(&(331, 1.0), itm.1.get("tue").unwrap());

                let itm: ((String, u8), BTreeMap<&str, (i64, f64)>) = i.next().unwrap();
                assert_eq!(("apple".into(), 13), itm.0);
                assert_eq!(&(44, 2.0), itm.1.get("sun").unwrap());
                assert_eq!(&(624, 2.0), itm.1.get("mon").unwrap());

                let itm: ((String, u8), BTreeMap<&str, (i64, f64)>) = i.next().unwrap();
                assert_eq!(("candy".into(), 11), itm.0);
                assert_eq!(&(49, 4.0), itm.1.get("sun").unwrap());
                assert_eq!(&(644, 4.0), itm.1.get("mon").unwrap());
                assert_eq!(&(334, 4.0), itm.1.get("tue").unwrap());
            }
        }
    }
}
