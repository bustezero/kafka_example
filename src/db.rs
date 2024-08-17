use chrono::{DateTime, Utc};
use deadpool_postgres::{Config, ManagerConfig, Pool, RecyclingMethod, Runtime};
use serde::{Deserialize, Serialize};
use tokio_postgres::NoTls;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DataChangeEvent {
    pub key: String,
    pub value: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Order {
    pub order_id: String, // Changed to String
    pub order_type: String,
    pub product_type: String,
    pub quantity: i32,
    pub price: f64,
    pub status: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Clone)]
pub struct DB {
    pub pool: Pool,
}

impl DB {
    pub async fn new(
        dbname: &str,
        user: &str,
        password: &str,
        host: &str,
        port: &str,
        clean_db: bool,
    ) -> Self {
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

        // Initialize the database by creating the tables
        if clean_db {
            db.clean().await.expect("Failed to clean database");
        }
        db.init().await.expect("Failed to initialize database");

        db
    }

    pub async fn clean(&self) -> Result<(), String> {
        let client = self.pool.get().await.map_err(|e| e.to_string())?;

        // Drop the tables if they exist
        client
            .execute("DROP TABLE IF EXISTS orders", &[])
            .await
            .map_err(|e| e.to_string())?;
        client
            .execute("DROP TABLE IF EXISTS data_change_events", &[])
            .await
            .map_err(|e| e.to_string())?;

        Ok(())
    }

    pub async fn init(&self) -> Result<(), String> {
        let client = self.pool.get().await.map_err(|e| e.to_string())?;

        // Create the data_change_events table
        client
            .execute(
                "
                CREATE TABLE IF NOT EXISTS data_change_events (
                    id SERIAL PRIMARY KEY,
                    key VARCHAR(255) NOT NULL,
                    value TEXT NOT NULL,
                    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
                )
                ",
                &[],
            )
            .await
            .map_err(|e| e.to_string())?;

        // Create the orders table
        client
            .execute(
                "
                CREATE TABLE IF NOT EXISTS orders (
                    order_id UUID PRIMARY KEY,
                    order_type VARCHAR(10) NOT NULL CHECK (order_type IN ('BUY', 'SELL')),
                    product_type VARCHAR(20) NOT NULL,
                    quantity INT NOT NULL,
                    price DECIMAL NOT NULL,
                    status VARCHAR(10) NOT NULL CHECK (status IN ('PENDING', 'MATCHED', 'FAILED', 'CANCELLED')),
                    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
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

    pub async fn list_events(
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
            .query(&stmt, &[&(limit as i64), &(offset as i64)])
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

    pub async fn insert_order(&self, order: &Order) -> Result<(), String> {
        let client = self.pool.get().await.map_err(|e| e.to_string())?;
        client
            .execute(
                "
                INSERT INTO orders (order_id, order_type, product_type, quantity, price, status, created_at, updated_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                ",
                &[
                    &order.order_id,
                    &order.order_type,
                    &order.product_type,
                    &(order.quantity as i32),
                    &(order.price as f64),
                    &order.status,
                    &order.created_at,
                    &order.updated_at,
                ],
            )
            .await
            .map_err(|e| e.to_string())?;
        Ok(())
    }

    pub async fn list_order(&self, limit: i64, offset: i64) -> Result<Vec<Order>, String> {
        let client = self.pool.get().await.map_err(|e| e.to_string())?;
        let stmt = client
            .prepare(
                "
                SELECT order_id, order_type, product_type, quantity, price, status, created_at, updated_at
                FROM orders
                ORDER BY created_at DESC
                LIMIT $1 OFFSET $2
                ",
            )
            .await
            .map_err(|e| e.to_string())?;
        let rows = client
            .query(&stmt, &[&(limit as i64), &(offset as i64)])
            .await
            .map_err(|e| e.to_string())?;

        let orders = rows
            .iter()
            .map(|row| Order {
                order_id: row.get("order_id"),
                order_type: row.get("order_type"),
                product_type: row.get("product_type"),
                quantity: row.get("quantity"),
                price: row.get("price"),
                status: row.get("status"),
                created_at: row.get("created_at"),
                updated_at: row.get("updated_at"),
            })
            .collect();
        Ok(orders)
    }

    pub async fn get_order_by_uuid(&self, order_id: &str) -> Result<Option<Order>, String> {
        let client = self.pool.get().await.map_err(|e| e.to_string())?;
        let stmt = client
            .prepare(
                "
                SELECT order_id, order_type, product_type, quantity, price, status, created_at, updated_at
                FROM orders
                WHERE order_id = $1
                ",
            )
            .await
            .map_err(|e| e.to_string())?;
        let rows = client
            .query(&stmt, &[&order_id])
            .await
            .map_err(|e| e.to_string())?;

        if let Some(row) = rows.iter().next() {
            Ok(Some(Order {
                order_id: row.get("order_id"),
                order_type: row.get("order_type"),
                product_type: row.get("product_type"),
                quantity: row.get("quantity"),
                price: row.get("price"),
                status: row.get("status"),
                created_at: row.get("created_at"),
                updated_at: row.get("updated_at"),
            }))
        } else {
            Ok(None)
        }
    }
}
