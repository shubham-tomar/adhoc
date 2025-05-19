mod utils;

use anyhow::Result;
use clap::Parser;
use log::{error, info};
use utils::cli::{Args, parse_topics};

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize the logger
    env_logger::init();
    
    // Parse command-line arguments
    let args = Args::parse();
    info!("Starting Kafka to Doris application");
    info!("Configuration: in-memory mode = {}", args.in_mem);
    
    // Convert topics string to a Vec of &str
    let topics_str = args.topics.clone();
    let topics = parse_topics(&topics_str);
    let topics_ref: Vec<&str> = topics.iter().map(|s| s.as_ref()).collect();
    
    // Create Kafka consumer
    let consumer = match utils::kafka::create_consumer(&args.brokers, &args.group_id, &topics_ref) {
        Ok(c) => c,
        Err(e) => {
            error!("Failed to create Kafka consumer: {}", e);
            return Err(e);
        }
    };
    
    // Start consuming messages
    if let Err(e) = utils::kafka::read_and_print(&consumer).await {
        error!("Error while consuming messages: {}", e);
        return Err(e);
    }
    
    Ok(())
}
