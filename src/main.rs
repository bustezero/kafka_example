use axum::{
    extract::{Json, Query, State},
    routing::{get, post},
    Router,
};
use config::{Config, File};
use env_logger::Builder;
use log::{error, info, LevelFilter};
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::{ClientConfig, Message};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio_stream::StreamExt;

mod db;
use db::{DataChangeEvent, DB};

#[derive(Debug, Deserialize)]
struct WebConfig {
    listen_address: String,
    listen_port: String,
}

#[derive(Debug, Deserialize)]
struct KafkaConfig {
    brokers: String,
    topic: String,
    group_id: String,
    message_timeout_ms: String,
    auto_offset_reset: String,
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
    kafka: KafkaConfig,
    database: DatabaseConfig,
}

#[derive(Serialize, Deserialize)]
struct Pagination {
    page: usize,
    per_page: usize,
}

#[derive(Clone)]
struct AppState {
    producer: FutureProducer,
    consumer: Arc<StreamConsumer>,
    db: DB,
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

    let kafka_brokers = &config.kafka.brokers;
    let topic = &config.kafka.topic;
    let group_id = &config.kafka.group_id;
    let message_timeout_ms = &config.kafka.message_timeout_ms;
    let auto_offset_reset = &config.kafka.auto_offset_reset;
    let topics = [topic.as_str()];

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", kafka_brokers)
        .set("message.timeout.ms", message_timeout_ms)
        .create()
        .expect("Producer creation error");

    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", kafka_brokers)
        .set("group.id", group_id)
        .set("auto.offset.reset", auto_offset_reset)
        .create()
        .expect("Consumer creation failed");

    consumer
        .subscribe(&topics)
        .expect("Failed to subscribe to topics");

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
        consumer: Arc::new(consumer),
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

async fn send_handler(
    State(state): State<AppState>,
    Json(event): Json<DataChangeEvent>,
) -> Result<Json<&'static str>, String> {
    let payload = serde_json::to_string(&event).unwrap();
    let record = FutureRecord::to("data_changes")
        .payload(&payload)
        .key(&event.key);

    match state
        .producer
        .send(record, std::time::Duration::from_secs(0))
        .await
    {
        Ok(_) => {
            info!("Sent data change event to Kafka: {:?}", event);
            Ok(Json("Message sent successfully"))
        }
        Err((e, _)) => {
            error!("Failed to send event to Kafka: {:?}", e);
            Err(format!("Failed to send message: {:?}", e))
        }
    }
}

async fn receive_handler(
    State(state): State<AppState>,
    Query(pagination): Query<Pagination>,
) -> Result<Json<Vec<DataChangeEvent>>, String> {
    let events = state
        .db
        .get_events(
            pagination.per_page as i64,
            (pagination.page * pagination.per_page) as i64,
        )
        .await?;
    Ok(Json(events))
}

async fn consume_events(state: AppState) {
    let mut stream = state.consumer.stream();
    while let Some(result) = stream.next().await {
        match result {
            Ok(borrowed_message) => {
                if let Some(payload) = borrowed_message
                    .payload()
                    .and_then(|p| std::str::from_utf8(p).ok())
                {
                    let event: DataChangeEvent = serde_json::from_str(payload).unwrap();
                    info!("Received data change event: {:?}", event);

                    if let Err(e) = state.db.insert_event(&event).await {
                        error!("Failed to insert event into DB: {:?}", e);
                    }
                }
            }
            Err(e) => error!("Kafka error: {}", e),
        }
    }
}
