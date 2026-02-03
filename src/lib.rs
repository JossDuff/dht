mod config;
mod net;

use anyhow::{anyhow, Result};
pub use config::Config;
use net::{connect_all, Peers};
use serde::{Deserialize, Serialize};
use std::{
    collections::{hash_map::DefaultHasher, HashMap},
    fmt::{self, Debug},
    hash::{Hash, Hasher},
    sync::Arc,
};
use tokio::sync::{oneshot, Mutex};
use tracing::{debug, info, warn};

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
    // all the nodes in this cluster, in consistent ordering
    cluster: Vec<NodeId>,
    my_node_id: NodeId,
    db: Arc<Mutex<HashMap<K, V>>>,
}

impl<K, V> Node<K, V>
where
    K: Send
        + Sync
        + 'static
        + Debug
        + Serialize
        + for<'de> Deserialize<'de>
        + Hash
        + Eq
        + PartialEq,
    V: Send + Sync + 'static + Debug + Serialize + for<'de> Deserialize<'de> + Clone,
{
    pub async fn new(config: Config) -> Result<Self> {
        let db = Arc::new(Mutex::new(HashMap::new()));
        let (peers, cluster, my_node_id): (Peers<K, V>, Vec<NodeId>, NodeId) =
            connect_all(&config.name, &config.connections).await?;

        Ok(Self {
            config,
            peers,
            cluster,
            my_node_id,
            db,
        })
    }

    pub async fn run(&mut self) -> Result<()> {
        // main event loop to respond to peers
        loop {
            match self.peers.inbox.recv().await {
                Some((from, msg)) => {
                    info!("Got {:?} from {}", msg, from);
                    match msg {
                        Message::Get { key, req_id } => {
                            // let owner_node = peers.get_key_owner(&key);
                            // // if the key is on this machine
                            // if owner_node == &peers.my_node_id {
                            //     todo!();
                            // }
                            let result = self.db.get(&key).map(|v| v.clone());
                            let resp: Message<K, V> = Message::GetResponse {
                                val: result,
                                req_id,
                            };
                            self.peers.send(&from, resp).await.map_err(|e| {
                                anyhow!("Error sending GetResponse to node {}: {}", from, e)
                            })?;
                            debug!(
                                "Sent GetResponse to {} for key {:?} req_id {}",
                                from, key, req_id
                            );
                        }
                        Message::Put { key, val, req_id } => {
                            let result = db.insert(key, val);
                            //let result = db.get(&key).map(|v| v.clone());
                            let resp: Message<K, V> = Message::GetResponse {
                                val: result,
                                req_id,
                            };
                            peers.send(&from, resp).await.map_err(|e| {
                                anyhow!("Error sending GetResponse to node {}: {}", from, e)
                            })?;
                            debug!(
                                "Sent PutResponse to {} for key {:?} req_id {}",
                                from, key, req_id
                            );
                        }
                        Message::GetResponse { val, req_id } => {
                            todo!()
                        }
                        Message::PutResponse { success, req_id } => {
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

    // maps the key to the sunlab node who stores the value
    pub fn get_key_owner(&self, key: &K) -> &NodeId {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        let cluster_index = (hasher.finish() as usize) % self.cluster.len();
        &self.cluster[cluster_index]
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Message<K, V> {
    Get { key: K, req_id: u64 },
    GetResponse { val: Option<V>, req_id: u64 },
    Put { key: K, val: V, req_id: u64 },
    PutResponse { success: bool, req_id: u64 },
}
