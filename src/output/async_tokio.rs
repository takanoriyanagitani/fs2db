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
