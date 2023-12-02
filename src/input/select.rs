use futures::Stream;

use tonic::Status;

#[tonic::async_trait]
pub trait Select {
    type Bucket;
    type Row;
    type Rows: Stream<Item = Result<Self::Row, Status>>;

    async fn all(&self, b: Self::Bucket) -> Result<Self::Rows, Status>;
}
