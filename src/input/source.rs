use futures::Stream;

use tonic::Status;

#[tonic::async_trait]
pub trait Source: Send + Sync + 'static {
    type Item;

    type All: Stream<Item = Result<Self::Item, Status>> + Send + 'static;

    async fn all(&self) -> Result<Self::All, Status>;
}
