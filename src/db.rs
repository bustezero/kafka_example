use deadpool_postgres::{Config, ManagerConfig, Pool, RecyclingMethod, Runtime};
use serde::{Deserialize, Serialize};
use tokio_postgres::NoTls;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DataChangeEvent {
    pub key: String,
    pub value: String,
}

#[derive(Clone)]
pub struct DB {
    pub pool: Pool,
}

impl DB {
    pub async fn new(dbname: &str, user: &str, password: &str, host: &str, port: &str) -> Self {
        let mut cfg = Config::new();
        cfg.dbname = Some(dbname.to_string());
        cfg.user = Some(user.to_string());
        cfg.password = Some(password.to_string());
        cfg.host = Some(host.to_string());
        cfg.port = Some(port.parse().unwrap());
        cfg.manager = Some(ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        });

        let pool = cfg.create_pool(Some(Runtime::Tokio1), NoTls).unwrap();

        let db = DB { pool };

        // Initialize the database by creating the table
        db.init().await.expect("Failed to initialize database");

        db
    }

    pub async fn init(&self) -> Result<(), String> {
        let client = self.pool.get().await.map_err(|e| e.to_string())?;
        client
            .execute(
                "
                CREATE TABLE IF NOT EXISTS data_change_events (
                    id SERIAL PRIMARY KEY,
                    key VARCHAR(255) NOT NULL,
                    value TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                ",
                &[],
            )
            .await
            .map_err(|e| e.to_string())?;
        Ok(())
    }

    pub async fn insert_event(&self, event: &DataChangeEvent) -> Result<(), String> {
        let client = self.pool.get().await.map_err(|e| e.to_string())?;
        client
            .execute(
                "INSERT INTO data_change_events (key, value) VALUES ($1, $2)",
                &[&event.key, &event.value],
            )
            .await
            .map_err(|e| e.to_string())?;
        Ok(())
    }

    pub async fn get_events(
        &self,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<DataChangeEvent>, String> {
        let client = self.pool.get().await.map_err(|e| e.to_string())?;
        let stmt = client
            .prepare("SELECT key, value FROM data_change_events ORDER BY id LIMIT $1 OFFSET $2")
            .await
            .map_err(|e| e.to_string())?;
        let rows = client
            .query(&stmt, &[&limit, &offset])
            .await
            .map_err(|e| e.to_string())?;

        let events = rows
            .iter()
            .map(|row| DataChangeEvent {
                key: row.get("key"),
                value: row.get("value"),
            })
            .collect();
        Ok(events)
    }
}
