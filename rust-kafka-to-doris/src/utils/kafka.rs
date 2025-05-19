use anyhow::Result;
use log::{info, warn};
use rdkafka::client::ClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, ConsumerContext, Rebalance, StreamConsumer};
use rdkafka::error::KafkaResult;
use rdkafka::message::Message;
use rdkafka::topic_partition_list::TopicPartitionList;
use std::time::Duration;

// Custom consumer context to handle Kafka consumer events
pub struct CustomContext;

impl ClientContext for CustomContext {}

impl ConsumerContext for CustomContext {
    fn pre_rebalance(&self, rebalance: &Rebalance) {
        info!("Pre-rebalance: {:?}", rebalance);
    }

    fn post_rebalance(&self, rebalance: &Rebalance) {
        info!("Post-rebalance: {:?}", rebalance);
    }

    fn commit_callback(&self, result: KafkaResult<()>, _offsets: &TopicPartitionList) {
        match result {
            Ok(_) => info!("Offsets committed successfully"),
            Err(e) => warn!("Error committing offsets: {:?}", e),
        }
    }
}

// Type alias for the Stream Consumer with our custom context
type LoggingConsumer = StreamConsumer<CustomContext>;

// Create a Kafka consumer configuration
pub fn create_consumer(brokers: &str, group_id: &str, topics: &[&str]) -> Result<LoggingConsumer> {
    let context = CustomContext;

    let consumer: LoggingConsumer = ClientConfig::new()
        .set("group.id", group_id)
        .set("bootstrap.servers", brokers)
        .set("enable.auto.commit", "true")
        .set("auto.offset.reset", "earliest")
        .set("session.timeout.ms", "6000")
        .create_with_context(context)?;

    consumer.subscribe(topics)?;
    
    info!("Kafka consumer created and subscribed to topics: {:?}", topics);
    Ok(consumer)
}

// Read messages from Kafka and print them to the console
pub async fn read_and_print(consumer: &LoggingConsumer) -> Result<()> {
    info!("Starting to consume messages from Kafka");
    
    loop {
        match consumer.recv().await {
            Ok(message) => {
                let payload = match message.payload_view::<str>() {
                    Some(Ok(s)) => s,
                    Some(Err(_)) => {
                        warn!("Error parsing message payload as UTF-8");
                        "Invalid UTF-8 sequence"
                    }
                    None => {
                        warn!("Empty message received");
                        "Empty message"
                    }
                };

                let topic = message.topic();
                let partition = message.partition();
                let offset = message.offset();
                
                info!(
                    "Received message from topic {}, partition {}, offset {}: {}",
                    topic, partition, offset, payload
                );
                
                // Here we would later implement the functionality to either store in memory
                // or write to a file based on the --in-mem flag
                
                println!("Message: {}", payload);
                
                consumer.commit_message(&message, CommitMode::Async)?;
            }
            Err(e) => {
                warn!("Error while receiving message: {:?}", e);
                // Adding a short delay to avoid tight loops in case of errors
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
}
