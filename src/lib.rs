mod config;
mod net;

use anyhow::{anyhow, Result};
pub use config::Config;
use net::{connect_all, Peers};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, fmt};
use tokio::sync::oneshot;
use tracing::info;

#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq, PartialOrd, Hash)]
pub struct NodeId {
    sunlab_name: String,
    id: usize,
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}_{}", self.sunlab_name, self.id)
    }
}

pub struct Node<K, V> {
    config: Config,
    peers: Peers<K, V>,
}

impl<K, V> Node<K, V> {
    pub async fn new(config: Config, ready_sender: oneshot::Sender<bool>) -> Result<()> {
        // connect to all other nodes, then send ready check
        //let connections = make_connections(self.config);
        info!("I'm running!");
        let mut peers: Peers<K, V> = connect_all(&config.name, config.connections).await?;

        // tell the test harness we're ready
        let _ = ready_sender
            .send(true)
            .map_err(|_| anyhow!("The receiver for the test harness ready check dropped"))?;

        // Key value store kept at this node
        let db: HashMap<K, V> = HashMap::new();

        // main event loop to respond to peers
        loop {
            match peers.inbox.recv().await {
                Some((from, msg)) => {
                    info!("Got {:?} from {}", msg, from);
                    match msg {
                        Message::Get { key, req_id } => {
                            todo!()
                        }
                        Message::Put { key, val, req_id } => {
                            todo!()
                        }
                    }
                    // Handle message, send responses via peers.send(&to, msg)
                }
                None => {
                    info!("All connections closed");
                    break;
                }
            }
        }

        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Message<K, V> {
    Ready { from: NodeId },
    Get { key: K, req_id: u64 },
    GetResponse { val: Option<V>, req_id: u64 },
    Put { key: K, val: V, req_id: u64 },
    PutResponse { success: bool, req_id: u64 },
}
