use axum::{
    extract::{Json, State},
    routing::{get, post},
    Router,
};
use env_logger::Builder;
use log::{error, info, LevelFilter};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::consumer::{StreamConsumer, Consumer};
use rdkafka::{ClientConfig, Message};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_stream::StreamExt;

#[derive(Serialize, Deserialize, Debug, Clone)]
struct DataChangeEvent {
    key: String,
    value: String,
}

#[derive(Clone)]
struct AppState {
    producer: FutureProducer,
    consumer: Arc<StreamConsumer>,
    received_messages: Arc<Mutex<Vec<DataChangeEvent>>>,
}

#[tokio::main]
async fn main() {
    Builder::new().filter(None, LevelFilter::Info).init();
    info!("Starting the Kafka HTTP server");

    let kafka_brokers = "172.20.19.27:9092";
    let topic = "data_changes";
    let group_id = "data_sync_group";
    let topics = [topic];

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", kafka_brokers)
        .set("message.timeout.ms", "5000")
        .create()
        .expect("Producer creation error");

    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", kafka_brokers)
        .set("group.id", group_id)
        .set("auto.offset.reset", "earliest")
        .create()
        .expect("Consumer creation failed");

    consumer.subscribe(&topics).expect("Failed to subscribe to topics");

    let received_messages = Arc::new(Mutex::new(Vec::new()));

    let state = AppState {
        producer,
        consumer: Arc::new(consumer),
        received_messages: received_messages.clone(),
    };

    let app = Router::new()
        .route("/send", post(send_handler))
        .route("/receive", get(receive_handler))
        .with_state(state.clone());

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
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
    let record = FutureRecord::to("data_changes").payload(&payload).key(&event.key);

    match state.producer.send(record, std::time::Duration::from_secs(0)).await {
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

async fn receive_handler(State(state): State<AppState>) -> Json<Vec<DataChangeEvent>> {
    let received_messages = state.received_messages.lock().await.clone();
    Json(received_messages)
}

async fn consume_events(state: AppState) {
    let mut stream = state.consumer.stream();
    while let Some(result) = stream.next().await {
        match result {
            Ok(borrowed_message) => {
                if let Some(payload) = borrowed_message.payload().and_then(|p| std::str::from_utf8(p).ok()) {
                    let event: DataChangeEvent = serde_json::from_str(payload).unwrap();
                    info!("Received data change event: {:?}", event);

                    let mut messages = state.received_messages.lock().await;
                    messages.push(event);
                }
            }
            Err(e) => error!("Kafka error: {}", e),
        }
    }
}
