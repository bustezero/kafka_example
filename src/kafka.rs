use std::sync::Arc;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::producer::FutureProducer;
use rdkafka::ClientConfig;
use serde::Deserialize;

#[derive(Debug, Deserialize, Clone)]
pub struct KafkaConfig {
    pub brokers: String,
    pub group_id: String,
    pub message_timeout_ms: String,
    pub auto_offset_reset: String,
    pub topics: Vec<String>,
}

#[derive(Clone)]
pub struct KafkaClient {
    pub producer: FutureProducer,
    pub consumer: Arc<StreamConsumer>,
}

impl KafkaClient {
    pub fn new(kafka_config: &KafkaConfig, topics: &[&str]) -> Self {
        let producer = ClientConfig::new()
            .set("bootstrap.servers", &kafka_config.brokers)
            .set("message.timeout.ms", &kafka_config.message_timeout_ms)
            .create()
            .expect("Producer creation error");

        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", &kafka_config.brokers)
            .set("group.id", &kafka_config.group_id)
            .set("auto.offset.reset", &kafka_config.auto_offset_reset)
            .create()
            .expect("Consumer creation failed");

        consumer
            .subscribe(topics)
            .expect("Failed to subscribe to topics");

        Self {
            producer,
            consumer: Arc::new(consumer),
        }
    }
}
