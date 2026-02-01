use anyhow::Result;
use clap::Parser;
use dht::{Config, Node};

#[tokio::main]
async fn main() -> Result<()> {
    let config = Config::parse();

    println!("I am {}", config.name);
    println!("I will connect to {:?}", config.connections);

    tokio::spawn(async move { Node::new(config) });

    Ok(())
}
