mod config;
mod net;

use anyhow::Result;
pub use config::Config;
use net::{connect_all, Peers};
use serde::{Deserialize, Serialize};

pub type NodeId = String;

pub struct Node {
    config: Config,
    peers: Peers,
}

impl Node {
    pub async fn new(config: Config) -> Result<()> {
        // connect to all other nodes, then send ready check
        //let connections = make_connections(self.config);
        println!("I'm running!");
        let mut peers = connect_all(&config.name, config.connections).await?;

        loop {
            match peers.inbox.recv().await {
                Some((from, msg)) => {
                    println!("Got {:?} from {}", msg, from);
                    // Handle message, send responses via peers.send(&to, msg)
                }
                None => {
                    println!("All connections closed");
                    break;
                }
            }
        }

        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Message {
    Hello { from: NodeId },
}
