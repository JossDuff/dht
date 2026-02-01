use anyhow::Result;
use clap::Parser;
use dht::{Config, Node};
use tokio::sync::oneshot;
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    let config = Config::parse();

    info!("I am {}", config.name);
    info!("I will connect to {:?}", config.connections);

    let (sender, receiver) = oneshot::channel();
    // runs in background
    let handle = tokio::spawn(async move { Node::<u64, u64>::new(config, sender).await });

    // wait until the node is ready
    receiver.await?;
    info!("Starting test.");

    handle.await??;

    Ok(())
}
