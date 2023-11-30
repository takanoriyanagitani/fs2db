use futures_core::Stream;

use fs2db::futures;
use futures::StreamExt;

use fs2db::tokio_stream;
use tokio_stream::wrappers::LinesStream;

use fs2db::tonic;
use tonic::Status;

use tokio::io::{AsyncBufReadExt, AsyncRead, BufReader};

pub async fn read2lines<R>(rdr: R) -> impl Stream<Item = Result<String, Status>>
where
    R: AsyncRead,
{
    let br = BufReader::new(rdr);
    let lines = br.lines();
    let ls = LinesStream::new(lines);
    ls.map(|r| r.map_err(|e| Status::internal(format!("Unable to read a line: {e}"))))
}
