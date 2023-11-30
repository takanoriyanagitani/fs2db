use fs2db::tonic;
use tonic::Status;

use crate::row::Record;

pub fn line2row(line: &str) -> Result<Record, Status> {
    let mut splited = line.split(':');
    let so = splited
        .next()
        .ok_or_else(|| Status::invalid_argument("offset missing"))?;
    let si = splited
        .next()
        .ok_or_else(|| Status::invalid_argument("id missing"))?;
    let st = splited
        .next()
        .ok_or_else(|| Status::invalid_argument("title missing"))?;

    let offset: i64 =
        str::parse(so).map_err(|e| Status::invalid_argument(format!("invalid offset: {e}")))?;
    let id: i64 =
        str::parse(si).map_err(|e| Status::invalid_argument(format!("invalid id: {e}")))?;
    let title: String = st.into();
    Ok(Record { offset, id, title })
}
