use futures_core::Stream;

use fs2db::futures;
use futures::StreamExt;

use fs2db::tonic;
use tonic::Status;

use crate::row::Record;

pub fn lines2rows<S>(lines: S) -> impl Stream<Item = Result<Record, Status>>
where
    S: Stream<Item = Result<String, Status>>,
{
    lines.map(|r| r.and_then(|line: String| crate::source::line2row::line2row(line.as_str())))
}
