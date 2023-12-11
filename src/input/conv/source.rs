use futures::StreamExt;

use tokio_stream::wrappers::ReceiverStream;

use tonic::Status;

use crate::input::source::BucketSource;

pub trait Converter: Send + Sync + 'static {
    type Input: Send + Sync;
    type Output: Send + Sync;

    fn convert(&self, i: Self::Input) -> Result<Self::Output, Status>;
}

pub struct ConvSource<B, C> {
    converter: C,
    source: B,
}

#[tonic::async_trait]
impl<B, C> BucketSource for ConvSource<B, C>
where
    B: BucketSource,
    B::K: Clone,
    C: Clone + Converter<Input = (B::K, B::V)>,
{
    type Bucket = B::Bucket;
    type K = B::K;
    type V = C::Output;
    type All = ReceiverStream<Result<(Self::K, Self::V), Status>>;

    async fn get_all_by_bucket(&self, b: Self::Bucket) -> Result<Self::All, Status> {
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        let all = self.source.get_all_by_bucket(b).await?;
        let converter: C = self.converter.clone();
        tokio::spawn(async move {
            let rc: &C = &converter;
            let mapd = all.map(|rslt| {
                rslt.and_then(|pair| {
                    let (k, v) = pair;
                    let converted: C::Output = rc.convert((k.clone(), v))?;
                    Ok((k, converted))
                })
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

pub fn conv_source_new<B, C>(
    original: B,
    converter: C,
) -> impl BucketSource<Bucket = B::Bucket, K = B::K, V = C::Output>
where
    B: BucketSource,
    B::K: Clone,
    C: Clone + Converter<Input = (B::K, B::V)>,
{
    ConvSource {
        converter,
        source: original,
    }
}

#[cfg(test)]
mod test_source {
    mod conv_source {
        mod empty {
            use tonic::Status;

            use futures::StreamExt;

            use tokio_stream::wrappers::ReceiverStream;

            use crate::input::source::BucketSource;

            use crate::input::conv::source::{conv_source_new, Converter};

            #[derive(Clone)]
            struct Conv {}

            impl Converter for Conv {
                type Input = ((), ());
                type Output = ();
                fn convert(&self, _i: Self::Input) -> Result<Self::Output, Status> {
                    Err(Status::unimplemented(
                        "empty stream must not call this method",
                    ))
                }
            }

            struct EmptySource {}

            #[tonic::async_trait]
            impl BucketSource for EmptySource {
                type Bucket = ();
                type K = ();
                type V = ();
                type All = ReceiverStream<Result<(Self::K, Self::V), Status>>;

                async fn get_all_by_bucket(&self, _b: Self::Bucket) -> Result<Self::All, Status> {
                    let (_, rx) = tokio::sync::mpsc::channel(1);
                    Ok(ReceiverStream::new(rx))
                }
            }

            #[tokio::test]
            async fn empty() {
                let bs = EmptySource {};
                let conv = Conv {};
                let neo = conv_source_new(bs, conv);
                let cst = neo.get_all_by_bucket(()).await.unwrap();
                let cnt: usize = cst.count().await;
                assert_eq!(0, cnt);
            }
        }

        mod double {
            use tonic::Status;

            use futures::StreamExt;

            use tokio_stream::wrappers::ReceiverStream;

            use crate::input::source::BucketSource;

            use crate::input::conv::source::{conv_source_new, Converter};

            #[derive(Clone)]
            struct Conv {}

            impl Converter for Conv {
                type Input = ((), u32);
                type Output = u64;
                fn convert(&self, i: Self::Input) -> Result<Self::Output, Status> {
                    let (_, v) = i;
                    let u6: u64 = v.into();
                    Ok(2 * u6)
                }
            }

            struct SimpleSource {}

            #[tonic::async_trait]
            impl BucketSource for SimpleSource {
                type Bucket = ();
                type K = ();
                type V = u32;
                type All = ReceiverStream<Result<(Self::K, Self::V), Status>>;

                async fn get_all_by_bucket(&self, _b: Self::Bucket) -> Result<Self::All, Status> {
                    let (tx, rx) = tokio::sync::mpsc::channel(1);
                    tokio::spawn(async move {
                        let pair = ((), 42);
                        tx.send(Ok(pair)).await.unwrap();
                    });
                    Ok(ReceiverStream::new(rx))
                }
            }

            #[tokio::test]
            async fn doubled() {
                let bs = SimpleSource {};
                let conv = Conv {};
                let neo = conv_source_new(bs, conv);
                let cst = neo.get_all_by_bucket(()).await.unwrap();
                let mut boxed = Box::pin(cst);
                let nex: u64 = boxed.next().await.unwrap().unwrap().1;
                assert_eq!(nex, 84);
            }
        }
    }
}
