use axum::{
    routing::{get, post},
    Router,
};
use config::{Config, File};
use env_logger::Builder;
use log::{info, LevelFilter};
use serde::Deserialize;

mod db;
mod handlers;
mod kafka;
use db::DB;
use handlers::{consume_events, receive_handler, send_handler, AppState};
use kafka::{create_consumer, create_producer};

#[derive(Debug, Deserialize)]
struct WebConfig {
    listen_address: String,
    listen_port: String,
}

#[derive(Debug, Deserialize)]
struct DatabaseConfig {
    dbname: String,
    user: String,
    password: String,
    host: String,
    port: String,
}

#[derive(Debug, Deserialize)]
struct AppConfig {
    web: WebConfig,
    kafka: kafka::KafkaConfig,
    database: DatabaseConfig,
}

#[tokio::main]
async fn main() {
    Builder::new().filter(None, LevelFilter::Info).init();
    info!("Starting the Kafka HTTP server");

    // 加载配置文件
    let settings = Config::builder()
        .add_source(File::with_name("config/kafka_example.yaml"))
        .build()
        .unwrap();

    let config: AppConfig = settings.try_deserialize().unwrap();

    let topic: &str = &config.kafka.topic;
    let topics = &[topic];

    let producer = create_producer(&config.kafka);
    let consumer = create_consumer(&config.kafka, topics);

    let db = DB::new(
        &config.database.dbname,
        &config.database.user,
        &config.database.password,
        &config.database.host,
        &config.database.port,
    )
    .await;

    let state = AppState {
        producer,
        consumer,
        db,
    };

    let app = Router::new()
        .route("/send", post(send_handler))
        .route("/receive", get(receive_handler))
        .with_state(state.clone());

    let listener_address_port = format!("{}:{}", config.web.listen_address, config.web.listen_port);
    let listener = tokio::net::TcpListener::bind(listener_address_port)
        .await
        .unwrap();
    info!("Listening on {}", listener.local_addr().unwrap());

    tokio::select! {
        _ = axum::serve(listener, app) => (),
        _ = consume_events(state) => (),
    }
}
