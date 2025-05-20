use anyhow::{Result, anyhow};
use futures::{StreamExt, FutureExt, future::join_all};
use log::{info, warn, error};
use mysql::prelude::*;
use mysql::{Opts, OptsBuilder, Pool};
use rdkafka::client::ClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, ConsumerContext, Rebalance, StreamConsumer};
use rdkafka::error::KafkaResult;
use rdkafka::message::Message;
use rdkafka::topic_partition_list::TopicPartitionList;
use serde_json;
use tokio::sync::Semaphore;
use uuid::Uuid;
use std::time::{Duration, Instant};
use std::fs;
use std::path::Path;
use std::io::Write;
use std::collections::HashMap;
use tokio::fs::File;
use tokio::io::{AsyncWriteExt, AsyncReadExt};
use base64::encode;
use reqwest::{Client, header::{HeaderMap, HeaderValue}};

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
pub fn create_consumer(
    brokers: &str, 
    group_id: &str, 
    topics: &[&str],
    sasl_mechanism: Option<&str>,
    sasl_username: Option<&str>,
    sasl_password: Option<&str>,
    security_protocol: &str
) -> Result<LoggingConsumer> {
    let context = CustomContext;
    
    // Start building the client configuration
    let mut config = ClientConfig::new();
    
    // Set required properties
    config.set("group.id", group_id)
          .set("bootstrap.servers", brokers)
          .set("auto.offset.reset", "earliest");
    
    // Configure security if SASL parameters are provided
    if let (Some(mechanism), Some(username), Some(password)) = (sasl_mechanism, sasl_username, sasl_password) {
        info!("Configuring Kafka SASL authentication with mechanism: {}", mechanism);
        config.set("security.protocol", security_protocol)
              .set("sasl.mechanism", mechanism)
              .set("sasl.username", username)
              .set("sasl.password", password);
    } else if security_protocol != "PLAINTEXT" {
        // If security protocol is not PLAINTEXT but SASL parameters are incomplete
        if sasl_mechanism.is_some() || sasl_username.is_some() || sasl_password.is_some() {
            warn!("Incomplete SASL configuration. All of mechanism, username, and password must be provided.");
        }
        info!("Using security protocol: {}", security_protocol);
        config.set("security.protocol", security_protocol);
    }
    
    // Create the consumer with our custom context
    let consumer: LoggingConsumer = config.create_with_context(context)?;

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

pub async fn insert_file_to_doris(
    file_path: &str,
    doris_host: &str,
    db: &str,
    table: &str,
    user: &str,
    password: &str,
    timeout_secs: u64,
) -> Result<()> {
    // Read file contents
    let mut file = File::open(file_path).await?;
    let mut file_contents = Vec::new();
    file.read_to_end(&mut file_contents).await?;

    // Check file size
    let file_size = file_contents.len();
    println!("📦 File to be loaded: {}, size: {} bytes", file_path, file_size);
    
    let doris_host = doris_host.trim_end_matches('/');
    
    let url = format!("{}/api/{}/{}/_stream_load", doris_host, db, table);
    println!("🌐 Sending Stream Load to: {}", url);

    // Build HTTP client
    let client = Client::builder()
        .timeout(Duration::from_secs(timeout_secs))
        .http1_only()
        .pool_max_idle_per_host(0)
        .build()
        .map_err(|e| anyhow!("Failed to build HTTP client: {}", e))?;

    // Build headers
    let mut headers = HeaderMap::new();
    headers.insert("Expect", HeaderValue::from_static("100-continue"));
    headers.insert("format", HeaderValue::from_static("json"));
    // headers.insert("read_json_by_line", HeaderValue::from_static("true"));
    headers.insert("strip_outer_array", HeaderValue::from_static("true"));
    let label = format!("kafka_load_{}", Uuid::new_v4());
    headers.insert(
        "label",
        HeaderValue::from_str(&label).unwrap_or(HeaderValue::from_static("kafka_load")),
    );

    // Send request with authentication in headers instead of using basic_auth method
    let mut auth_value = format!("{}:", user);
    if !password.is_empty() {
        auth_value = format!("{}:{}", user, password);
    }
    let auth_header = format!("Basic {}", base64::encode(auth_value));
    if let Ok(header_value) = HeaderValue::from_str(&auth_header) {
        headers.insert("Authorization", header_value);
    } else {
        println!("Warning: Could not create Authorization header");
    }
    
    let response = client
        .put(&url)
        .headers(headers)
        .body(file_contents)
        .send()
        .await
        .map_err(|e| anyhow!("Stream Load request failed: {}", e))?;

    let status = response.status();
    let body = response.text().await?;

    if !status.is_success() || body.to_lowercase().contains("fail") || body.to_lowercase().contains("error") {
        Err(anyhow!("❌ Doris Stream Load failed ({}): {}", status, body))
    } else {
        println!("✅ Stream Load succeeded with label `{}`: {}", label, body);
        Ok(())
    }
}

async fn process_in_memory(
    batch: &[String],
    doris_host: &str,
    db: &str,
    table: &str,
    user: &str,
    password: &str,
    timeout_secs: u64
) -> anyhow::Result<()> {
    println!("Processing {} records in memory", batch.len());
    
    // Create JSON array payload from batch items
    let mut json_payload = String::with_capacity(batch.len() * 200); // Rough estimate for size
    json_payload.push('[');
    
    // Add each record to the JSON array
    for (i, record) in batch.iter().enumerate() {
        if i > 0 {
            json_payload.push_str(",\n");
        }
        json_payload.push_str(record);
    }
    
    json_payload.push(']');
    
    // Convert to bytes for sending
    let payload_bytes = json_payload.into_bytes();
    let payload_size = payload_bytes.len();
    println!("📦 In-memory payload size: {} bytes", payload_size);
    
    // Send data directly to Doris using Stream Load API
    // Normalize the Doris host URL - ensure it doesn't end with a slash
    let doris_host = doris_host.trim_end_matches('/');
    
    let url = format!("{}/api/{}/{}/_stream_load", doris_host, db, table);
    println!("🌐 Sending Stream Load to: {}", url);

    // Build HTTP client
    let client = Client::builder()
        .timeout(Duration::from_secs(timeout_secs))
        .http1_only()
        .pool_max_idle_per_host(0)
        .build()
        .map_err(|e| anyhow!("Failed to build HTTP client: {}", e))?;

    // Build headers
    let mut headers = HeaderMap::new();
    headers.insert("Expect", HeaderValue::from_static("100-continue"));
    headers.insert("format", HeaderValue::from_static("json"));
    headers.insert("strip_outer_array", HeaderValue::from_static("true"));
    
    // Generate a unique label for the Stream Load job
    let label = format!("kafka_in_memory_{}", Uuid::new_v4());
    if let Ok(header_value) = HeaderValue::from_str(&label) {
        headers.insert("label", header_value);
    }
    
    // Add authentication header
    let mut auth_value = format!("{}:", user);
    if !password.is_empty() {
        auth_value = format!("{}:{}", user, password);
    }
    let auth_header = format!("Basic {}", base64::encode(auth_value));
    if let Ok(header_value) = HeaderValue::from_str(&auth_header) {
        headers.insert("Authorization", header_value);
    } else {
        println!("Warning: Could not create Authorization header");
    }
    
    // Send request
    let response = client
        .put(&url)
        .headers(headers)
        .body(payload_bytes)
        .send()
        .await
        .map_err(|e| anyhow!("Stream Load request failed: {}", e))?;

    let status = response.status();
    let body = response.text().await?;

    if !status.is_success() || body.to_lowercase().contains("fail") || body.to_lowercase().contains("error") {
        Err(anyhow!("❌ In-memory Stream Load failed ({}): {}", status, body))
    } else {
        println!("✅ In-memory Stream Load succeeded with label `{}`: {}", label, body);
        Ok(())
    }
}

pub async fn create_batch(
    consumer: &LoggingConsumer, 
    batch_size: usize, 
    flush_interval_secs: u64, 
    in_mem: bool,
    output_path: Option<&str>,
    doris_host: &str,
    doris_db: &str,
    doris_table: &str,
    doris_user: &str,
    doris_password: &str,
    connect_timeout: u64,
    concurrency: usize,
    use_mysql: bool,
    mysql_port: u16
) -> Result<()> {
    info!("Starting batch processing with concurrency level: {}", concurrency);
    
    // Create a semaphore to limit concurrent tasks
    let semaphore = std::sync::Arc::new(Semaphore::new(concurrency));
    let mut batch: Vec<String> = Vec::with_capacity(batch_size);
    let mut last_flush = Instant::now();
    let mut stream = consumer.stream();
    
    // Track in-flight tasks for handling errors
    let mut in_flight_tasks: Vec<tokio::task::JoinHandle<Result<()>>> = Vec::new();

    while let Some(msg_result) = stream.next().await {
        // First, check completed tasks to handle any errors
        let mut i = 0;
        while i < in_flight_tasks.len() {
            if in_flight_tasks[i].is_finished() {
                let task = in_flight_tasks.swap_remove(i);
                // We just want to check if there were errors, but don't wait
                if let Some(task_result) = task.now_or_never() {
                    if let Err(e) = task_result {
                        error!("Task completed with error: {}", e);
                        // Continue processing other tasks, don't fail the entire operation
                    }
                }
            } else {
                i += 1;
            }
        }
        
        let msg = match msg_result {
            Ok(m) => m,
            Err(e) => {
                eprintln!("Kafka error: {}", e);
                continue;
            }
        };

        if let Some(Ok(payload)) = msg.payload_view::<str>() {
            batch.push(payload.to_string());
        }

        let should_flush = batch.len() >= batch_size || last_flush.elapsed() >= Duration::from_secs(flush_interval_secs);

        if should_flush && !batch.is_empty() {
            // Create clones of all parameters needed for parallel processing
            let batch_to_process = batch.clone();
            let doris_host = doris_host.to_string();
            let doris_db = doris_db.to_string();
            let doris_table = doris_table.to_string();
            let doris_user = doris_user.to_string();
            let doris_password = doris_password.to_string();
            let output_path_owned = output_path.map(|s| s.to_string());
            let semaphore_clone = semaphore.clone();
            let is_in_mem = in_mem;
            
            // Commit the message before spawning the task
            // consumer.commit_message(&msg, CommitMode::Async)?;
            
            // Spawn a task for processing this batch
            let handle = tokio::spawn(async move {
                // Acquire semaphore permit to limit concurrency
                let _permit = semaphore_clone.acquire().await.expect("Failed to acquire semaphore");
                
                let batch_size = batch_to_process.len();
                info!("Processing batch of {} messages", batch_size);
                
                // Process based on protocol and mode flags
                let result = if use_mysql {
                    // Extract hostname without http:// protocol prefix for MySQL
                    let host_parts: Vec<&str> = doris_host.split("://").collect();
                    let clean_host = if host_parts.len() > 1 { host_parts[1] } else { host_parts[0] };
                    
                    info!("Using MySQL protocol to insert data");
                    process_via_mysql(
                        &batch_to_process,
                        clean_host,
                        mysql_port,
                        &doris_db,
                        &doris_table,
                        &doris_user,
                        &doris_password,
                        connect_timeout
                    ).await
                } else if is_in_mem {
                    info!("Using Stream Load HTTP protocol in memory");
                    process_in_memory(
                        &batch_to_process,
                        &doris_host,
                        &doris_db,
                        &doris_table,
                        &doris_user,
                        &doris_password,
                        connect_timeout
                    ).await
                } else {
                    // Write batch to file
                    let file_id = Uuid::new_v4().to_string();
                    let dir_path = match output_path_owned {
                        Some(path) => path,
                        None => std::env::temp_dir().display().to_string(),
                    };
                    let file_path = format!("{}/kafka_batch_{}.json", dir_path, file_id);
                    
                    match File::create(&file_path).await {
                        Ok(mut file) => {
                            // Write each JSON object with proper formatting
                            if let Err(e) = file.write(b"[").await {
                                return Err(anyhow::anyhow!("Failed to write to file: {}", e));
                            }
                            
                            for (i, record) in batch_to_process.iter().enumerate() {
                                // Write the JSON object
                                if i > 0 {
                                    if let Err(e) = file.write(b",\n").await {
                                        return Err(anyhow::anyhow!("Failed to write to file: {}", e));
                                    }
                                }
                                if let Err(e) = file.write(record.as_bytes()).await {
                                    return Err(anyhow::anyhow!("Failed to write to file: {}", e));
                                }
                            }
                            
                            if let Err(e) = file.write(b"]").await {
                                return Err(anyhow::anyhow!("Failed to write to file: {}", e));
                            }
                            
                            // Process the file
                            let load_result = insert_file_to_doris(
                                &file_path,
                                &doris_host,
                                &doris_db,
                                &doris_table,
                                &doris_user,
                                &doris_password,
                                connect_timeout
                            ).await;
                            
                            // Clean up the file regardless of the result
                            let _ = fs::remove_file(&file_path);
                            
                            load_result
                        },
                        Err(e) => Err(anyhow::anyhow!("Failed to create file: {}", e)),
                    }
                };
                
                // Log result and return
                match &result {
                    Ok(_) => {
                        // consumer.commit_message(&msg, CommitMode::Async)?;
                        info!("✅ Batch of {} messages processed successfully", batch_size);
                    },
                    Err(e) => error!("❌ Failed to process batch: {}", e),
                }
                
                result
            });
            
            // Add the task to our in-flight list
            in_flight_tasks.push(handle);
            
            // Clear the batch and reset the timer
            batch.clear();
            last_flush = Instant::now();
        }
    }
    
    // Wait for all in-flight tasks to complete
    info!("Waiting for {} in-flight tasks to complete", in_flight_tasks.len());
    let results = join_all(in_flight_tasks).await;
    
    // Check for errors but don't fail if some tasks failed
    let mut error_count = 0;
    let results_len = results.len(); // Store the length before consuming the vector
    
    for result in results {
        match result {
            Ok(Ok(_)) => {}, // Task succeeded
            Ok(Err(e)) => {
                error_count += 1;
                error!("Task completed with error: {}", e);
            },
            Err(e) => {
                error_count += 1;
                error!("Task panicked: {}", e);
            }
        }
    }
    
    if error_count > 0 {
        warn!("{} out of {} tasks completed with errors", error_count, results_len);
    } else {
        info!("All {} tasks completed successfully", results_len);
    }
    
    // We don't fail the overall operation even if some tasks failed
    Ok(())
}

/// Insert data into Doris using the MySQL protocol (JDBC compatible)
/// This uses the standard MySQL connector to connect directly to Doris FE node
/// and execute SQL statements
pub async fn process_via_mysql(
    batch: &[String],
    host: &str,
    port: u16,
    database: &str,
    table: &str,
    username: &str,
    password: &str,
    connect_timeout: u64,
) -> Result<()> {
    if batch.is_empty() {
        return Ok(()); // Nothing to do
    }

    info!("Connecting to Doris via MySQL protocol: {}:{}/{}", host, port, database);

    // Build the MySQL connection URL with all necessary parameters
    let url = format!("mysql://{}:{}@{}:{}/{}", 
        username, password, host, port, database);

    // Create a connection pool with specific options
    let opts = Opts::from_url(&url)
        .map_err(|e| anyhow!("Failed to parse MySQL URL: {}", e))?;
    
    let builder = OptsBuilder::from_opts(opts)
        .tcp_connect_timeout(Some(std::time::Duration::from_secs(connect_timeout)))
        .stmt_cache_size(Some(0)) // Disable statement cache for better compatibility
        .prefer_socket(false);    // Force TCP connection

    let pool = Pool::new(builder)
        .map_err(|e| anyhow!("Failed to create MySQL connection pool: {}", e))?;

    // Get a connection from the pool
    let mut conn = pool.get_conn()
        .map_err(|e| anyhow!("Failed to connect to Doris via MySQL: {}", e))?;

    // Prepare the SQL statement for the insert
    // This assumes the batch contains JSON objects compatible with the table structure
    let mut success_count = 0;
    let mut error_count = 0;

    // Process each JSON record
    for (i, json_str) in batch.iter().enumerate() {
        // Parse the JSON to extract fields and values
        match serde_json::from_str::<serde_json::Value>(json_str) {
            Ok(json_value) => {
                if let serde_json::Value::Object(obj) = json_value {
                    // Extract field names and values
                    let fields: Vec<String> = obj.keys()
                        .map(|k| format!("`{}`", k))
                        .collect();
                    
                    let values: Vec<String> = obj.values()
                        .map(|v| {
                            match v {
                                serde_json::Value::String(s) => format!("'{}'", s.replace("'", "''")),
                                serde_json::Value::Null => "NULL".to_string(),
                                serde_json::Value::Object(_) | serde_json::Value::Array(_) => {
                                    // Handle nested objects and arrays by escaping and using string literals
                                    // This passes the JSON as a string which Doris can parse based on column type
                                    format!("'{}'", v.to_string().replace("'", "''"))
                                },
                                _ => v.to_string()
                            }
                        })
                        .collect();

                    // Construct the INSERT statement
                    let insert_sql = format!(
                        "INSERT INTO `{}`.`{}` ({}) VALUES ({})", 
                        database, table, 
                        fields.join(", "), 
                        values.join(", ")
                    );

                    // Execute the SQL statement
                    match conn.query_drop(&insert_sql) {
                        Ok(_) => {
                            success_count += 1;
                            if i % 100 == 0 { // Log progress every 100 records
                                info!("Inserted record {} via MySQL", i);
                            }
                        },
                        Err(e) => {
                            error_count += 1;
                            error!("Failed to insert record {}: {}", i, e);
                            // Continue with next record
                        }
                    }
                } else {
                    error!("Record {} is not a JSON object", i);
                    error_count += 1;
                }
            },
            Err(e) => {
                error!("Failed to parse JSON for record {}: {}", i, e);
                error_count += 1;
            }
        }
    }

    // Log results
    if error_count > 0 {
        warn!("MySQL insert completed with {} successes and {} errors", success_count, error_count);
    } else {
        info!("MySQL insert completed successfully for all {} records", success_count);
    }

    Ok(())
}
