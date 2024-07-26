use axum::extract::{Json, Query, State};
use log::{error, info};
use rdkafka::consumer::StreamConsumer;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::Message;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio_stream::StreamExt;

use crate::db::{DataChangeEvent, DB};

#[derive(Serialize, Deserialize)]
pub struct Pagination {
    page: usize,
    per_page: usize,
}

#[derive(Clone)]
pub struct AppState {
    pub producer: FutureProducer,
    pub consumer: Arc<StreamConsumer>,
    pub db: DB,
}

pub async fn send_handler(
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

pub async fn receive_handler(
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

pub async fn consume_events(state: AppState) {
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
