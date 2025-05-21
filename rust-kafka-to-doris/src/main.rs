mod utils;

use anyhow::Result;
use clap::Parser;
use log::{error, info};
use utils::cli::{Args, parse_topics};

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize the logger
    if std::env::var("RUST_LOG").is_err() {
        // If RUST_LOG is not set, default to INFO level
        std::env::set_var("RUST_LOG", "debug");
    }
    env_logger::init();
    
    // Parse command-line arguments
    let args = Args::parse();
    info!("Starting Kafka to Doris application");
    info!("Configuration: in-memory mode = {}", args.in_mem);
    
    // Convert topics string to a Vec of &str
    let topics_str = args.topics.clone();
    let topics = parse_topics(&topics_str);
    let topics_ref: Vec<&str> = topics.iter().map(|s| s.as_ref()).collect();
    
    // Get SASL parameters as options
    let sasl_mechanism = args.kafka_sasl_mechanism.as_deref();
    let sasl_username = args.kafka_sasl_username.as_deref();
    let sasl_password = args.kafka_sasl_password.as_deref();
    
    // Create Kafka consumer with authentication parameters
    let consumer = match utils::kafka::create_consumer(
        &args.brokers, 
        &args.group_id, 
        &topics_ref,
        sasl_mechanism,
        sasl_username,
        sasl_password,
        &args.kafka_security_protocol
    ) {
        Ok(c) => c,
        Err(e) => {
            error!("Failed to create Kafka consumer: {}", e);
            return Err(e);
        }
    };
    
    // Start batch creation process
    utils::kafka::create_batch(
        &consumer, 
        args.batch_size, 
        args.flush_interval, 
        args.in_mem, 
        args.output_path.as_deref(),
        &args.doris_host,
        &args.doris_db,
        &args.doris_table,
        &args.doris_user,
        &args.doris_password,
        args.connect_timeout,
        args.concurrency,
        args.use_mysql,
        args.mysql_port
    ).await?;
    
    info!("Processing completed successfully");
    Ok(())
}
