mod utils;

use anyhow::Result;
use clap::Parser;
use log::{error, info};
use utils::cli::{Args, parse_topics};
use utils::Config;

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
    // let topics_ref: Vec<&str> = topics.iter().map(|s| s.as_ref()).collect();
    
    // Get SASL parameters as options
    let sasl_mechanism = args.kafka_sasl_mechanism.as_deref();
    let sasl_username = args.kafka_sasl_username.as_deref();
    let sasl_password = args.kafka_sasl_password.as_deref();
    let config_path = args.config_path;

    let config = utils::load_config(&config_path).unwrap();
    println!("Config: {:#?}", config);
    let jobs = config.jobs;

    // Collect task handles so we can wait for them
    let mut handles = Vec::new();

    for job in jobs {
        let topic = job.topic.clone();
        let table = job.table.clone();
    
        // Clone all needed fields from args before the move
        let brokers = args.brokers.clone();
        let group_id = args.group_id.clone();
        let kafka_security_protocol = args.kafka_security_protocol.clone();
        let output_path = args.output_path.clone();
        let doris_host = args.doris_host.clone();
        let doris_db = args.doris_db.clone();
        let doris_user = args.doris_user.clone();
        let doris_password = args.doris_password.clone();
        let cloud_cluster = args.cloud_cluster.clone();
    
        let sasl_mechanism = args.kafka_sasl_mechanism.clone();
        let sasl_username = args.kafka_sasl_username.clone();
        let sasl_password = args.kafka_sasl_password.clone();
    
        let batch_size = args.batch_size;
        let flush_interval = args.flush_interval;
        let in_mem = args.in_mem;
        let connect_timeout = args.connect_timeout;
        let concurrency = args.concurrency;
        let use_mysql = args.use_mysql;
        let mysql_port = args.mysql_port;
    
        // Store the handle for each spawned task
        let handle = tokio::spawn(async move {
            let consumer = match utils::kafka::create_consumer(
                &brokers,
                &group_id,
                &[&topic],
                sasl_mechanism.as_deref(),
                sasl_username.as_deref(),
                sasl_password.as_deref(),
                &kafka_security_protocol,
            ) {
                Ok(c) => c,
                Err(e) => {
                    error!("Failed to create Kafka consumer for topic {}: {}", topic, e);
                    return Err(e);
                }
            };
    
            info!("Started consumer for topic: {} -> table: {}", topic, table);
            utils::kafka::create_batch(
                &consumer,
                batch_size,
                flush_interval,
                in_mem,
                output_path.as_deref(),
                &doris_host,
                &doris_db,
                &table,
                &doris_user,
                &doris_password,
                connect_timeout,
                concurrency,
                use_mysql,
                mysql_port,
                &cloud_cluster,
            ).await?;
    
            Ok::<_, anyhow::Error>(())
        });
        
        handles.push(handle);
    }
    
    info!("All consumers started, waiting for tasks to complete");
    
    // Wait for all tasks to complete or handle errors
    for handle in handles {
        match handle.await {
            Ok(result) => {
                if let Err(e) = result {
                    error!("Task failed: {}", e);
                }
            },
            Err(e) => error!("Failed to join task: {}", e),
        }
    }
    
    info!("Processing completed successfully");
    Ok(())
}
