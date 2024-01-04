use tokio::io::BufReader;

use tonic::Status;

use async_compression::tokio::bufread::GzipDecoder;

use crate::input::conv::lines::ReadSource;

pub struct GzipDecodedSrc<R> {
    encoded: R,
}

#[tonic::async_trait]
impl<R> ReadSource for GzipDecodedSrc<R>
where
    R: ReadSource,
{
    type Bucket = R::Bucket;
    type R = GzipDecoder<BufReader<R::R>>;

    async fn get_src_read_by_bucket(&self, b: Self::Bucket) -> Result<Self::R, Status> {
        let ar: R::R = self.encoded.get_src_read_by_bucket(b).await?;
        let br: BufReader<R::R> = BufReader::new(ar);
        let gr: GzipDecoder<BufReader<_>> = GzipDecoder::new(br);
        Ok(gr)
    }
}

/// Creates a [`ReadSource`] from gzipped [`ReadSource`]
pub fn read_src_gzip_decoded_new<R>(encoded: R) -> impl ReadSource<Bucket = R::Bucket>
where
    R: ReadSource,
{
    GzipDecodedSrc { encoded }
}
