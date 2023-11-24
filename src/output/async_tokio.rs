use core::future::Future;

use futures::TryStreamExt;

use tokio_stream::Stream;

pub async fn save_many<S, T, O, E, Fut>(inputs: S, saver: &O) -> Result<u64, E>
where
    S: Stream<Item = Result<T, E>>,
    O: Fn(T) -> Fut,
    Fut: Future<Output = Result<u64, E>>,
{
    inputs
        .try_fold(0, |tot, input| async move {
            let cnt: u64 = saver(input).await?;
            Ok(cnt + tot)
        })
        .await
}

#[tonic::async_trait]
pub trait Transaction: Sized + Sync + Send {
    type Input: Sync + Send;
    type Error;

    async fn commit(self) -> Result<(), Self::Error>;

    async fn save(&self, i: Self::Input) -> Result<u64, Self::Error>;

    async fn save_many<S>(self, inputs: S) -> Result<u64, Self::Error>
    where
        S: Stream<Item = Result<Self::Input, Self::Error>> + Send,
    {
        let r: &Self = &self;
        let cnt: u64 = inputs
            .try_fold(0, |tot, input| async move {
                let cnt: u64 = r.save(input).await?;
                Ok(tot + cnt)
            })
            .await?;
        self.commit().await?;
        Ok(cnt)
    }
}
