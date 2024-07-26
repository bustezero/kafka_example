use std::sync::Arc;

use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::producer::FutureProducer;
use rdkafka::ClientConfig;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct KafkaConfig {
    pub brokers: String,
    pub topic: String,
    pub group_id: String,
    pub message_timeout_ms: String,
    pub auto_offset_reset: String,
}

pub fn create_producer(kafka_config: &KafkaConfig) -> FutureProducer {
    ClientConfig::new()
        .set("bootstrap.servers", &kafka_config.brokers)
        .set("message.timeout.ms", &kafka_config.message_timeout_ms)
        .create()
        .expect("Producer creation error")
}

pub fn create_consumer(kafka_config: &KafkaConfig, topics: &[&str]) -> Arc<StreamConsumer> {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &kafka_config.brokers)
        .set("group.id", &kafka_config.group_id)
        .set("auto.offset.reset", &kafka_config.auto_offset_reset)
        .create()
        .expect("Consumer creation failed");

    consumer
        .subscribe(topics)
        .expect("Failed to subscribe to topics");

    Arc::new(consumer)
}
