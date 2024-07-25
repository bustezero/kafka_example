use env_logger::Builder;
use log::{error, info, LevelFilter};
use serde::{Serialize, Deserialize};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::consumer::{StreamConsumer, Consumer};
use rdkafka::{ClientConfig, Message};
use rand::{Rng, SeedableRng};
use rand::rngs::StdRng;
use std::time::Duration;
use tokio_stream::StreamExt;
use std::sync::Arc;
use tokio::sync::Mutex;

#[tokio::main]
async fn main() {
    Builder::new().filter(None, LevelFilter::Info).init();
    info!("Starting the Kafka example application");

    let kafka_brokers = "172.20.19.27:9092";
    let topic = "data_changes";
    let group_id = "data_sync_group";
    let topics = ["data_changes"];

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", kafka_brokers)
        .set("message.timeout.ms", "5000")
        .create()
        .expect("Producer creation error");

    let rng = StdRng::from_entropy();

    let message_count = Arc::new(Mutex::new(0));
    let message_count_clone = Arc::clone(&message_count);

    // 启动生产者任务
    let producer_task = tokio::spawn(async move {
        produce_event(producer, topic.to_string(), rng).await;
    });

    // 启动消费者任务
    let consumer_task = tokio::spawn(async move {
        consume_events(kafka_brokers, group_id, &topics, message_count_clone).await;
    });

    // 等待任务完成
    let _ = tokio::join!(producer_task, consumer_task);
}

#[derive(Serialize, Deserialize, Debug)]
struct DataChangeEvent {
    key: String,
    value: String,
}

async fn produce_event(producer: FutureProducer, topic: String, mut rng: StdRng) {
    for _ in 0..5 {
        let key = format!("key{}", rng.gen::<u32>());
        let value = format!("value{}", rng.gen::<u32>());
        let event = DataChangeEvent { key, value };
        let payload = serde_json::to_string(&event).unwrap();
        let record = FutureRecord::to(&topic)
            .payload(&payload)
            .key(&event.key);

        match producer.send(record, Duration::from_secs(0)).await {
            Ok(_) => info!("Sent data change event to Kafka: {:?}", event),
            Err((e, _)) => error!("Failed to send event to Kafka: {:?}", e),
        }

        tokio::time::sleep(tokio::time::Duration::from_secs(rng.gen_range(1..5))).await;
    }
}

async fn consume_events(brokers: &str, group_id: &str, topics: &[&str], message_count: Arc<Mutex<i32>>) {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("group.id", group_id)
        .set("auto.offset.reset", "earliest")
        .create()
        .expect("Consumer creation failed");

    consumer.subscribe(topics).expect("Failed to subscribe to topics");

    let mut stream = consumer.stream();
    while let Some(result) = stream.next().await {
        match result {
            Ok(borrowed_message) => {
                if let Some(payload) = borrowed_message.payload().and_then(|p| std::str::from_utf8(p).ok()) {
                    let event: DataChangeEvent = serde_json::from_str(payload).unwrap();
                    info!("Received data change event: {:?}", event);

                    let mut count = message_count.lock().await;
                    *count += 1;
                    if *count >= 5 {
                        info!("Received 5 messages, stopping consumer");
                        break;
                    }
                }
            }
            Err(e) => error!("Kafka error: {}", e),
        }
    }
}
