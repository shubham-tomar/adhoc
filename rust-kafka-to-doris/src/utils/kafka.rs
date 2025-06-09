use anyhow::{Result, anyhow};
use futures::StreamExt;
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
    info!("📦 File to be loaded: {}, size: {} bytes", file_path, file_size);
    
    // Normalize the Doris host URL - ensure it has the proper format with http:// prefix
    let doris_host = if !doris_host.starts_with("http://") && !doris_host.starts_with("https://") {
        format!("http://{}", doris_host.trim_end_matches('/'))
    } else {
        doris_host.trim_end_matches('/').to_string()
    };
    
    let url = format!("{}/api/{}/{}/_stream_load", doris_host, db, table);
    info!("🌐 Sending Stream Load to: {}", url);

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
        info!("✅ Stream Load succeeded with label `{}`: {}", label, body);
        Ok(())
    }
}

pub async fn process_in_memory(
    batch: &[String],
    doris_host: &str,
    db: &str,
    table: &str,
    user: &str,
    password: &str,
    timeout_secs: u64,
    cloud_cluster: &str,
) -> Result<()> {
    // Construct JSON payload
    let mut json_payload = String::with_capacity(batch.len() * 200);
    json_payload.push('[');
    for (i, record) in batch.iter().enumerate() {
        if i > 0 {
            json_payload.push_str(",\n");
        }
        json_payload.push_str(record);
    }
    json_payload.push(']');
    let payload_bytes = json_payload.into_bytes();

    // Normalize Doris host
    let doris_host = if !doris_host.starts_with("http://") && !doris_host.starts_with("https://") {
        format!("http://{}", doris_host.trim_end_matches('/'))
    } else {
        doris_host.trim_end_matches('/').to_string()
    };
    let url = format!("{}/api/{}/{}/_stream_load", doris_host, db, table);
    let label = format!("kafka_in_memory_{}", Uuid::new_v4());

    // Build headers
    let mut headers = HeaderMap::new();
    headers.insert("Expect", HeaderValue::from_static("100-continue"));
    headers.insert("Content-Type", HeaderValue::from_static("application/json"));
    headers.insert("format", HeaderValue::from_static("json"));
    headers.insert("cloud_cluster", HeaderValue::from_str(cloud_cluster).unwrap());
    headers.insert("strip_outer_array", HeaderValue::from_static("true"));
    headers.insert("label", HeaderValue::from_str(&label).unwrap());

    // Build HTTP client with redirect disabled
    let client = Client::builder()
        .timeout(Duration::from_secs(timeout_secs))
        .http1_only()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .map_err(|e| anyhow!("Failed to build HTTP client: {}", e))?;

    // Function to send the request (allows retry on redirect)
    async fn send_stream_load_request(
        client: &Client,
        url: &str,
        headers: HeaderMap,
        user: &str,
        password: &str,
        body: Vec<u8>,
    ) -> Result<reqwest::Response> {
        client
            .put(url)
            .headers(headers)
            .basic_auth(user, Some(password))
            .body(body)
            .send()
            .await
            .map_err(|e| anyhow!("Stream Load request failed: {}", e))
    }

    // First attempt
    let mut response = send_stream_load_request(&client, &url, headers.clone(), user, password, payload_bytes.clone()).await?;

    // Handle redirect manually
    if response.status().is_redirection() {
        if let Some(location) = response.headers().get("Location") {
            let redirected_url = location.to_str().map_err(|e| anyhow!("Invalid redirect URL: {}", e))?;
            info!("🔁 Redirected to: {}", redirected_url);

            response = send_stream_load_request(&client, redirected_url, headers, user, password, payload_bytes).await?;
        } else {
            return Err(anyhow!("Redirected but no Location header provided"));
        }
    }

    let status = response.status();
    let body = response.text().await?;

    if !status.is_success() || body.to_lowercase().contains("fail") || body.to_lowercase().contains("error") {
        Err(anyhow!("❌ In-memory Stream Load failed ({}): {}", status, body))
    } else {
        info!("✅ In-memory Stream Load succeeded with label `{}`: {}", label, body);
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
    _concurrency: usize, // Keeping parameter but not using it 
    use_mysql: bool,
    mysql_port: u16,
    cloud_cluster: &str,
) -> Result<()> {
    
    let mut batch: Vec<String> = Vec::with_capacity(batch_size);
    let mut last_flush = Instant::now();
    let mut stream = consumer.stream();
    
    let mut success_count = 0;
    let mut error_count = 0;

    while let Some(msg_result) = stream.next().await {
        let msg = match msg_result {
            Ok(m) => m,
            Err(e) => {
                error!("Kafka error: {}", e);
                continue;
            }
        };

        if let Some(Ok(payload)) = msg.payload_view::<str>() {
            batch.push(payload.to_string());
        }

        let should_flush = batch.len() >= batch_size || last_flush.elapsed() >= Duration::from_secs(flush_interval_secs);

        if should_flush && !batch.is_empty() {
            let batch_to_process = batch.clone();
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
                    doris_db,
                    doris_table,
                    doris_user,
                    doris_password,
                    connect_timeout
                ).await
            } else if in_mem {
                info!("Using Stream Load HTTP protocol in memory");
                process_in_memory(
                    &batch_to_process,
                    doris_host,
                    doris_db,
                    doris_table,
                    doris_user,
                    doris_password,
                    connect_timeout,
                    cloud_cluster
                ).await
            } else {
                // Write batch to file
                let file_id = Uuid::new_v4().to_string();
                let dir_path = match output_path {
                    Some(path) => path.to_string(),
                    None => std::env::temp_dir().display().to_string(),
                };
                let file_path = format!("{}/kafka_batch_{}.json", dir_path, file_id);
                
                match File::create(&file_path).await {
                    Ok(mut file) => {
                        // Write each JSON object with proper formatting
                        if let Err(e) = file.write(b"[").await {
                            Err(anyhow!("Failed to write to file: {}", e))
                        } else {
                            // Write each record
                            for (i, record) in batch_to_process.iter().enumerate() {
                                if i > 0 && file.write(b",\n").await.is_err() {
                                    return Err(anyhow!("Failed to write delimiter to file"));
                                }
                                if file.write(record.as_bytes()).await.is_err() {
                                    return Err(anyhow!("Failed to write record to file"));
                                }
                            }
                            
                            // Close JSON array
                            if file.write(b"]").await.is_err() {
                                return Err(anyhow!("Failed to write closing bracket to file"));
                            }
                            
                            // Process the file
                            let load_result = insert_file_to_doris(
                                &file_path,
                                doris_host,
                                doris_db,
                                doris_table,
                                doris_user,
                                doris_password,
                                connect_timeout
                            ).await;
                            
                            // Clean up the file regardless of the result
                            let _ = fs::remove_file(&file_path);
                            
                            load_result
                        }
                    },
                    Err(e) => Err(anyhow!("Failed to create file: {}", e)),
                }
            };
            
            // Process result
            match result {
                Ok(_) => {
                    success_count += 1;
                    info!("✅ Batch of {} messages processed successfully", batch_size);
                    // Commit message after successful processing
                    consumer.commit_message(&msg, CommitMode::Async)?;
                },
                Err(e) => {
                    error_count += 1;
                    error!("❌ Failed to process batch: {}", e);
                    // We don't commit on failure so messages can be reprocessed
                }
            }
            
            // Clear the batch and reset the timer
            batch.clear();
            last_flush = Instant::now();
        }
    }
    
    // Log overall statistics
    if error_count > 0 {
        warn!("Completed with {} successful batches and {} failed batches", success_count, error_count);
    } else if success_count > 0 {
        info!("All {} batches completed successfully", success_count);
    } else {
        info!("No batches were processed");
    }
    
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
    // let url = format!("mysql://{}:{}@{}:{}/{}", 
    //     username, password, "k8s-devstage-devstage-3ffb228664-a66aa19a393a375a.elb.us-east-1.amazonaws.com", 8030, database);
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
