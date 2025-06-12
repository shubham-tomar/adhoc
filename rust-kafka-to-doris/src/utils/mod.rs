pub mod cli;
pub mod kafka;

use std::fs::File;
use std::io::Read;
use std::path::Path;
use serde::{Deserialize};

#[derive(Debug, Deserialize)]
pub struct Config {
    pub jobs: Vec<Job>,
}

#[derive(Debug, Deserialize)]
pub struct Job {
    pub topic: String,
    pub table: String,
}

pub fn load_config(path: &str) -> Result<Config, anyhow::Error> {
    let mut file = File::open(path)?;
    let mut contents = String::new();
    file.read_to_string(&mut contents)?;
    let config: Config = serde_json::from_str(&contents)?;
    Ok(config)
}
