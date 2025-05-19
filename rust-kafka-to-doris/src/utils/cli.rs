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
    #[arg(short, long)]
    pub output_path: Option<String>,
}

// Parse the topics string into a Vec of string slices
pub fn parse_topics(topics_str: &str) -> Vec<&str> {
    topics_str.split(',').collect()
}
