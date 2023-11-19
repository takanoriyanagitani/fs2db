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

struct Out {
    pool: Pool,
}

impl Out {
    async fn upsert(&self, item: &Record) -> Result<u64, String> {
        let p: Pool = self.pool.clone();
        let client: Client = p
            .get()
            .await
            .map_err(|e| format!("Unable to get a client: {e}"))?;
        client
            .execute(UPSERT_QUERY, &[&item.offset, &item.id, &item.title])
            .await
            .map_err(|e| format!("Unable to upsert: {e}"))
    }

    async fn upsert_many<S>(&self, items: S) -> Result<u64, String>
    where
        S: Stream<Item = Result<Record, String>>,
    {
        fs2db::output::async_tokio::save_many(items, &|item: Record| async move {
            self.upsert(&item).await
        })
        .await
    }
}

#[tokio::main]
async fn main() -> Result<(), String> {
    let input_filename: String = std::env::var("ENV_INPUT_FILENAME")
        .ok()
        .unwrap_or_else(|| INPUT_FILENAME_DEFAULT.into());
    let records = plain::async_tokio::path2strings(input_filename)
        .await
        .map_err(|e| format!("Unable to get input lines: {e}"))?
        .map(|r: Result<String, _>| r.map_err(|e| format!("Unable to get a line: {e}")))
        .map(|r: Result<String, _>| r.and_then(Record::try_from));

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
    let out = Out { pool };
    let upserts: u64 = out.upsert_many(records).await?;
    println!("upserts: {upserts}");
    Ok(())
}
