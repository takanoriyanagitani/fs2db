use futures::Stream;

use tonic::Status;

#[tonic::async_trait]
pub trait Upsert {
    type Row;
    type Bucket;

    async fn upsert<S>(&self, bucket: Self::Bucket, rows: S) -> Result<u64, Status>
    where
        S: Stream<Item = Result<Self::Row, Status>>;
}
