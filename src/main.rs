use anyhow::Result;
use clap::Parser;
use dht::{Config, Node};
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    let config = Config::parse();

    info!("I am {}", config.name);
    info!("I will connect to {:?}", config.connections);

    // runs in background
    let handle = tokio::spawn(async move { Node::new(config).await });
    handle.await??;

    Ok(())
}
