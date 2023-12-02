use tonic::codec::Streaming;
use tonic::{transport::Channel, Request, Response, Status};

use crate::input::select::Select;

use crate::rpc::fs2db::proto::source;
use source::v1::select_service_client::SelectServiceClient;
use source::v1::{AllRequest, AllResponse, InputBucket};

pub struct SelectClient {
    pub cli: SelectServiceClient<Channel>,
}

#[tonic::async_trait]
impl Select for SelectClient {
    type Bucket = Vec<u8>;
    type Row = AllResponse;
    type Rows = Streaming<AllResponse>;

    async fn all(&self, bucket: Vec<u8>) -> Result<Self::Rows, Status> {
        let req = AllRequest {
            bkt: Some(InputBucket { bucket }),
        };
        let res: Response<_> = self.cli.clone().all(Request::new(req)).await?;
        let s: Streaming<_> = res.into_inner();
        Ok(s)
    }
}
