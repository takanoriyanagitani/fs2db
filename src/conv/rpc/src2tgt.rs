use futures::stream::StreamExt;

use tonic::{transport::Channel, Status};

use crate::rpc::fs2db::proto::source;
use source::v1::drop_service_client::DropServiceClient;
use source::v1::drop_svc::CheckedRequest;
use source::v1::select_service_client::SelectServiceClient as SelSrc;
use source::v1::AllRequest as SelAll;
use source::v1::InputBucket;

use crate::rpc::fs2db::proto::target;
use target::v1::sel_svc::AllRequest as TgtAll;
use target::v1::select_service_client::SelectServiceClient as SelTgt;
use target::v1::upsert_service_client::UpsertServiceClient;
use target::v1::upst_svc::ManyRequest;
use target::v1::OutputBucket;

/// Gets all key/val pairs from a bucket and upserts all of them.
/// 1. Gets all key/val pairs from a bucket
/// 2. Ignore errors(incomplete key/val pairs will be rejected later)
/// 3. Upserts all
/// 4. Returns number of rows upserted
pub async fn upsert_selected(
    u: &mut UpsertServiceClient<Channel>,
    s: &mut SelSrc<Channel>,
    i: InputBucket,
    o: OutputBucket,
) -> Result<u64, Status> {
    let req = SelAll { bkt: Some(i) };
    let pairs = s.all(req).await?.into_inner();
    let noerr = pairs.filter_map(|r| async move { r.ok() });
    let reqs = noerr.map(move |a| {
        let key = a.key;
        let val = a.val;
        ManyRequest {
            bkt: Some(o.clone()),
            key,
            val,
        }
    });
    let res = u.many(reqs).await?.into_inner();
    Ok(res.upserted)
}

/// Drop a source bucket after verification.
/// 1. Gets all key/check pairs from a target bucket
/// 2. Ignore errors(incomplete pairs will be rejected later)
/// 3. Drop a source bucket if all pairs verified
/// 4. Returns number of keys which was stored in a source bucket
pub async fn drop_all_if_verified(
    t: &mut SelTgt<Channel>,
    d: &mut DropServiceClient<Channel>,
    i: InputBucket,
    o: OutputBucket,
) -> Result<u64, Status> {
    let req = TgtAll { bkt: Some(o) };
    let pairs = t.all(req).await?.into_inner();
    let noerr = pairs.filter_map(|r: Result<_, _>| async move { r.ok() });
    let reqs = noerr.map(move |a| {
        let key = a.key;
        let check = a.check;
        CheckedRequest {
            bkt: Some(i.clone()),
            key,
            check,
        }
    });
    let res = d.checked(reqs).await?.into_inner();
    Ok(res.keys_count)
}
