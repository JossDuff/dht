use anyhow::Result;
use clap::Parser;
use dht::{Config, Node};

#[tokio::main]
async fn main() -> Result<()> {
    let config = Config::parse();

    println!("I am {}", config.name);
    println!("I will connect to {:?}", config.connections);

    // runs in background
    let handle = tokio::spawn(async move { Node::new(config).await });
    handle.await??;

    Ok(())
}
