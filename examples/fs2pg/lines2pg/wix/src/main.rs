use std::path::Path;
use std::sync::Arc;

const INPUT_FILENAME_DEFAULT: &str = "enwiki-20231101-pages-articles-multistream-index.txt";
const OUTPUT_DBNAME: &str = "testdb_2023_11_19_09_49_03";

const UPSERT_QUERY: &str = r#"
	INSERT INTO wix(
		ofst,
		id,
		title
	)
	VALUES(
		$1::BIGINT,
		$2::BIGINT,
		$3::TEXT
	)
"#;

use fs2db::output::async_tokio::Transaction;
use fs2db::tonic::Status;

use deadpool_postgres::tokio_postgres;
use deadpool_postgres::{Client, Pool};
use deadpool_postgres::{Config, GenericClient, Manager, ManagerConfig, RecyclingMethod};

use tokio_postgres::NoTls;

use fs2db::input::lines::plain;
use fs2db::{Stream, StreamExt};

struct Record {
    offset: i64,
    id: i64,
    title: String,
}

impl TryFrom<String> for Record {
    type Error = String;
    fn try_from(line: String) -> Result<Self, Self::Error> {
        let mut splited = line.split(':');
        let so: &str = splited
            .next()
            .ok_or_else(|| String::from("offset missing"))?;
        let si: &str = splited.next().ok_or_else(|| String::from("id missing"))?;
        let st: &str = splited
            .next()
            .ok_or_else(|| String::from("title missing"))?;
        let offset: i64 = str::parse(so).map_err(|e| format!("invalid offset: {e}"))?;
        let id: i64 = str::parse(si).map_err(|e| format!("invalid id: {e}"))?;
        Ok(Self {
            offset,
            id,
            title: st.into(),
        })
    }
}

struct NoTxSaver {
    p: Arc<Pool>,
}

impl NoTxSaver {
    async fn upsert(&self, item: &Record) -> Result<u64, Status> {
        let client: Client = self
            .p
            .get()
            .await
            .map_err(|e| Status::internal(format!("Unable to get a client: {e}")))?;
        client
            .execute(UPSERT_QUERY, &[&item.offset, &item.id, &item.title])
            .await
            .map_err(|e| Status::internal(format!("Unable to upsert: {e}")))
    }

    async fn upsert_many<S>(&self, items: S) -> Result<u64, Status>
    where
        S: Stream<Item = Result<Record, Status>>,
    {
        fs2db::output::async_tokio::save_many(items, &|item: Record| async move {
            self.upsert(&item).await
        })
        .await
    }
}

struct Saver<'a> {
    tx: deadpool_postgres::Transaction<'a>,
}

struct SaverFactory {
    p: Arc<Pool>,
}

impl SaverFactory {
    async fn save_many<S>(&self, inputs: S) -> Result<u64, Status>
    where
        S: Stream<Item = Result<Record, Status>> + Send,
    {
        let mut client: Client = self
            .p
            .get()
            .await
            .map_err(|e| Status::internal(format!("Unable to get a client: {e}")))?;
        let tx = client
            .transaction()
            .await
            .map_err(|e| Status::internal(format!("Unable to begin a transaction: {e}")))?;
        let saver = Saver { tx };
        let cnt: u64 = saver.save_many(inputs).await?;
        Ok(cnt)
    }
}

#[fs2db::tonic::async_trait]
impl<'a> fs2db::output::async_tokio::Transaction for Saver<'a> {
    type Input = Record;
    type Error = Status;

    async fn commit(self) -> Result<(), Self::Error> {
        self.tx
            .commit()
            .await
            .map_err(|e| Status::internal(format!("Unable to commit: {e}")))
    }

    async fn save(&self, i: Record) -> Result<u64, Self::Error> {
        self.tx
            .execute(UPSERT_QUERY, &[&i.offset, &i.id, &i.title])
            .await
            .map_err(|e| Status::internal(format!("Unable to save an item: {e}")))
    }
}

async fn with_tx(p: Arc<Pool>, input_filename: &Path) -> Result<u64, Status> {
    let records = plain::async_tokio::path2strings(input_filename)
        .await
        .map_err(|e| Status::internal(format!("Unable to get input lines: {e}")))?
        .map(|r: Result<String, _>| {
            r.map_err(|e| Status::internal(format!("Unable to get a line: {e}")))
        })
        .map(|r: Result<String, Status>| {
            r.and_then(|s| Record::try_from(s).map_err(Status::internal))
        });
    let sf = SaverFactory { p };
    let cnt: u64 = sf.save_many(records).await?;
    Ok(cnt)
}

async fn without_tx(p: Arc<Pool>, input_filename: &Path) -> Result<u64, Status> {
    let records = plain::async_tokio::path2strings(input_filename)
        .await
        .map_err(|e| Status::internal(format!("Unable to get input lines: {e}")))?
        .map(|r: Result<String, _>| {
            r.map_err(|e| Status::internal(format!("Unable to get a line: {e}")))
        })
        .map(|r: Result<String, Status>| {
            r.and_then(|s| Record::try_from(s).map_err(Status::internal))
        });
    let saver = NoTxSaver { p };
    let cnt: u64 = saver.upsert_many(records).await?;
    Ok(cnt)
}

#[tokio::main]
async fn main() -> Result<(), String> {
    let input_filename: String = std::env::var("ENV_INPUT_FILENAME")
        .ok()
        .unwrap_or_else(|| INPUT_FILENAME_DEFAULT.into());

    let mut tpcfg = tokio_postgres::Config::new();
    tpcfg.host_path("/run/postgresql");
    tpcfg.user("postgres");

    let mut cfg = Config::new();
    let dbname: String = std::env::var("ENV_DBNAME")
        .ok()
        .unwrap_or_else(|| OUTPUT_DBNAME.into());
    tpcfg.dbname(dbname.as_str());
    let m = Manager::from_config(
        tpcfg,
        NoTls,
        ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        },
    );
    cfg.dbname = Some(dbname);
    let pool: Pool = Pool::builder(m)
        .max_size(2)
        .build()
        .map_err(|e| format!("Unable to build a pool {e}"))?;

    let use_tx: bool = std::env::var("ENV_USE_TX")
        .ok()
        .and_then(|s| str::parse(s.as_str()).ok())
        .unwrap_or(false);

    let ap: Arc<Pool> = Arc::new(pool);

    let input_filename = Path::new(&input_filename);

    let cnt: u64 = match use_tx {
        true => with_tx(ap, input_filename).await,
        false => without_tx(ap, input_filename).await,
    }
    .map_err(|e| format!("Unable to save: {e}"))?;

    println!("upserted: {cnt}");
    Ok(())
}
