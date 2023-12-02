use futures::StreamExt;

use tonic::Status;

use crate::input::select::Select;
use crate::output::upsert::Upsert;

pub async fn upsert_selected<I, O, S, U, C>(
    ibucket: I,
    obucket: O,
    sel: &S,
    ups: &U,
    conv: &C,
) -> Result<u64, Status>
where
    S: Select<Bucket = I>,
    U: Upsert<Bucket = O>,
    C: Fn(<S as Select>::Row) -> Result<<U as Upsert>::Row, Status>,
{
    let rows = sel.all(ibucket).await?;

    let converted = rows.map(|r| r.and_then(conv));

    ups.upsert(obucket, converted).await
}
