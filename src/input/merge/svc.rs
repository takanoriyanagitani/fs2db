use futures::Stream;

use tonic::Status;

pub trait Merge: Send + Sync + 'static {
    type A: Send;
    type B: Send;
    type T: Send;

    fn merge(&self, a: Self::A, b: Self::B) -> Result<Self::T, Status>;
}

#[tonic::async_trait]
pub trait MergeSource: Send + Sync + 'static {
    type K: Send;
    type V: Send;

    type All: Stream<Item = Result<(Self::K, Self::V), Status>> + Send + 'static;

    async fn all(&self) -> Result<Self::All, Status>;
}
