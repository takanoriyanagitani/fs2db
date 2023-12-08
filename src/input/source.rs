use futures::Stream;

use tonic::Status;

#[tonic::async_trait]
pub trait Source: Send + Sync + 'static {
    type Item;

    type All: Stream<Item = Result<Self::Item, Status>> + Send + 'static;

    async fn all(&self) -> Result<Self::All, Status>;
}

#[tonic::async_trait]
pub trait BucketSource: Send + Sync + 'static {
    type Bucket;
    type K;
    type V;

    type All: Stream<Item = Result<(Self::K, Self::V), Status>> + Send + 'static;

    async fn get_all_by_bucket(&self, b: Self::Bucket) -> Result<Self::All, Status>;
}
