use clap::Parser;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    /// Kafka broker addresses (comma-separated)
    #[arg(short, long, default_value = "localhost:9092")]
    pub brokers: String,

    /// Kafka consumer group ID
    #[arg(short, long, default_value = "rust-kafka-doris-group")]
    pub group_id: String,

    /// Kafka topics to consume (comma-separated)
    #[arg(short, long, default_value = "dummy-src")]
    pub topics: String,

    /// Process data in memory without writing to disk
    #[arg(long, default_value_t = false)]
    pub in_mem: bool,

    /// Optional path to store data on disk (when --in-mem is false)
    #[arg(short, long, default_value = "./src/tmp")]
    pub output_path: Option<String>,
    
    /// Batch size for message processing
    #[arg(long, default_value_t = 10000)]
    pub batch_size: usize,
    
    /// Flush interval in seconds
    #[arg(long, default_value_t = 10)]
    pub flush_interval: u64,
    
    /// Number of concurrent workers for parallel processing
    #[arg(long, default_value_t = 2)]
    pub concurrency: usize,
    
    /// Doris host URL
    #[arg(long, default_value = "http://127.0.0.1:8040")]
    pub doris_host: String,
    
    /// Connection timeout in seconds
    #[arg(long, default_value_t = 30)]
    pub connect_timeout: u64,
    
    /// Use MySQL protocol instead of HTTP for Doris connection
    #[arg(long, default_value_t = false)]
    pub use_mysql: bool,

    /// Use Cloud Cluster for Doris connection
    #[arg(long, default_value = "")]
    pub cloud_cluster: String,
    
    /// MySQL port for Doris FE node (only used when use_mysql is true)
    #[arg(long, default_value_t = 9030)]
    pub mysql_port: u16,
    
    /// Doris database name (use --doris-database for compatibility)
    #[arg(long, default_value = "test", alias = "doris-database")]
    pub doris_db: String,
    
    /// Doris BE (Backend) node address
    #[arg(long)]
    pub doris_be_node: Option<String>,
    
    /// Kafka SASL mechanism (e.g., PLAIN, SCRAM-SHA-256, SCRAM-SHA-512)
    #[arg(long)]
    pub kafka_sasl_mechanism: Option<String>,
    
    /// Kafka SASL username
    #[arg(long)]
    pub kafka_sasl_username: Option<String>,
    
    /// Kafka SASL password
    #[arg(long)]
    pub kafka_sasl_password: Option<String>,
    
    /// Kafka security protocol (e.g., PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL)
    #[arg(long, default_value = "PLAINTEXT")]
    pub kafka_security_protocol: String,
    
    /// Doris table name
    #[arg(long, default_value = "users")]
    pub doris_table: String,
    
    /// Doris username
    #[arg(long, default_value = "root")]
    pub doris_user: String,
    
    /// Doris password
    #[arg(long, default_value = "")]
    pub doris_password: String,

    /// Path to config file
    #[arg(long, default_value = "config.json")]
    pub config_path: String,
}

// Parse the topics string into a Vec of string slices
pub fn parse_topics(topics_str: &str) -> Vec<&str> {
    topics_str.split(',').collect()
}
