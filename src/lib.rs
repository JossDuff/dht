mod config;
mod net;

use anyhow::{anyhow, Result};
pub use config::Config;
use net::{connect_all, Peers};
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::{
    collections::{hash_map::DefaultHasher, HashMap},
    fmt::{self, Debug},
    hash::{Hash, Hasher},
    sync::{Arc, RwLock},
};
use tokio::sync::{mpsc, oneshot, Mutex};
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

pub struct Node<K, V: Clone> {
    config: Config,
    peers: Peers<K, V>,
    // all the nodes in this cluster, in consistent ordering
    cluster: Vec<NodeId>,
    my_node_id: NodeId,
    local_inbox: mpsc::Receiver<LocalMessage<K, V>>,
    db: Arc<Mutex<HashMap<K, V>>>,
    // TODO: use RwLock
    peer_response_senders: Arc<Mutex<HashMap<u64, oneshot::Sender<LocalMessage<K, V>>>>>,
    local_response_receivers: Arc<Mutex<HashMap<u64, oneshot::Receiver<LocalMessage<K, V>>>>>,
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
    pub async fn new(config: Config) -> Result<(Self, mpsc::Sender<LocalMessage<K, V>>)> {
        let db = Arc::new(Mutex::new(HashMap::new()));
        let peer_response_senders = Arc::new(Mutex::new(HashMap::new()));
        let local_response_receivers = Arc::new(Mutex::new(HashMap::new()));

        // this is a barrier
        let (peers, cluster, my_node_id): (Peers<K, V>, Vec<NodeId>, NodeId) =
            connect_all::<K, V>(&config.name, &config.connections).await?;

        // for sending/ receiving messages from the test harness
        let (local_sender, local_inbox) = mpsc::channel(16);

        Ok((
            Self {
                config,
                peers,
                cluster,
                my_node_id,
                local_inbox,
                peer_response_senders,
                local_response_receivers,
                db,
            },
            local_sender,
        ))
    }

    pub async fn run(&self) -> Result<()> {
        // TODO: event loop for local messages
        // main event loop to respond to peers
        Ok(())
    }

    // handles messages from the test harness and from the network loop when peers respond
    async fn run_local_loop(&mut self) -> Result<()> {
        loop {
            match self.local_inbox.recv().await {
                Some(msg) => match msg {
                    LocalMessage::Get {
                        key,
                        response_sender,
                    } => {
                        let key_owner = self.get_key_owner(&key);
                        // key is locally owned
                        if *key_owner == self.my_node_id {
                            let resp = self.local_get(&key).await;
                            response_sender.send(resp).map_err(|_| {
                                anyhow!("Receiver for local get on key {:?} dropped", key)
                            })?;
                        } else {
                            // we need to request the key's owner
                            let req_id: u64 = rand::thread_rng().gen();
                            let request: PeerMessage<K, V> = PeerMessage::Get { key, req_id };
                            let _ = self.peers.send(key_owner, request).await?;

                            let peer_response_senders = self.peer_response_senders.lock().await;
                            peer_response_senders.insert(req_id, response_sender);

                            //request_responses: Arc<Mutex<HashMap<u64, oneshot::Sender<LocalMessage<K, V>>>>>,
                        }
                    }
                    LocalMessage::Put {
                        key,
                        val,
                        response_sender,
                    } => {
                        todo!()
                    }
                    // handle a getResponse from a peer
                    LocalMessage::GetResponse { key, val } => {
                        todo!()
                    }
                    LocalMessage::PutResponse { key, success } => {
                        todo!()
                    }
                },
                None => {
                    info!("Test ended");
                    break;
                }
            }
        }

        Ok(())
    }

    // handles messages from the network
    async fn run_network_loop(&self) -> Result<()> {
        loop {
            match self.peers.inbox.recv().await {
                Some((from, msg)) => {
                    info!("Got {:?} from {}", msg, from);
                    match msg {
                        PeerMessage::Get { key, req_id } => {
                            let result = self.local_get(key).await;
                            let resp: PeerMessage<K, V> = PeerMessage::GetResponse {
                                key: *key,
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
                        PeerMessage::Put { key, val, req_id } => {
                            let result = self.local_insert(*key, val).await;
                            let resp: PeerMessage<K, V> = PeerMessage::PutResponse {
                                key: *key,
                                success: result,
                                req_id,
                            };

                            self.peers.send(&from, resp).await.map_err(|e| {
                                anyhow!("Error sending PutResponse to node {}: {}", from, e)
                            })?;

                            debug!(
                                "Sent PutResponse to {} for key {:?} req_id {}",
                                from, key, req_id
                            );
                        }
                        // received a response from a peer about a previous get request
                        PeerMessage::GetResponse { key, val, req_id } => {
                            // look up the channel for sending the response
                            let req_resp = self.request_responses.lock().await;
                            match req_resp.get(&req_id) {
                                Some(sender) => {
                                    sender
                                        .send(LocalMessage::GetResponse {
                                            req_id,
                                            key: *key,
                                            val,
                                        })
                                        .await
                                        .map_err(|e| {
                                            anyhow!(
                                                "Error sending local GetResponse for request {}",
                                                req_id
                                            )
                                        })?;
                                }
                                None => {
                                    return Err(anyhow!("Receiver for req_id {} dropped", req_id));
                                }
                            };
                        }
                        // received a response from a peer about a previous put request
                        PeerMessage::PutResponse {
                            key,
                            success,
                            req_id,
                        } => {
                            // look up the channel for sending the response
                            let req_resp = self.request_responses.lock().await;
                            match req_resp.get(&req_id) {
                                Some(sender) => {
                                    sender
                                        .send(LocalMessage::PutResponse {
                                            req_id,
                                            key: *key,
                                            success,
                                        })
                                        .await
                                        .map_err(|e| {
                                            anyhow!(
                                                "Error sending local PutResponse for request {}",
                                                req_id
                                            )
                                        })?;
                                }
                                None => {
                                    return Err(anyhow!("Receiver for req_id {} dropped", req_id));
                                }
                            };
                        }
                    }
                }
                None => {
                    info!("All connections closed");
                    break;
                }
            }
        }
        Ok(())
    }

    async fn local_get(&self, key: &K) -> Option<V> {
        let db = self.db.lock().await;
        db.get(key).map(|x| x.clone())
    }

    async fn local_insert(&self, key: K, value: V) -> bool {
        let mut db = self.db.lock().await;

        match db.get(&key) {
            // an element already exists, return false
            Some(_) => false,
            // there is no element, insert and return true
            None => {
                let _ = db.insert(key, value);
                true
            }
        }
    }

    // maps the key to the sunlab node who stores the value
    fn get_key_owner(&self, key: &K) -> &NodeId {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        let cluster_index = (hasher.finish() as usize) % self.cluster.len();
        &self.cluster[cluster_index]
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum PeerMessage<K, V> {
    Get { key: K, req_id: u64 },
    GetResponse { key: K, val: Option<V>, req_id: u64 },
    Put { key: K, val: V, req_id: u64 },
    PutResponse { key: K, success: bool, req_id: u64 },
}

#[derive(Clone, Debug)]
pub enum LocalMessage<K, V: Clone> {
    Get {
        key: K,
        response_sender: oneshot::Sender<Option<V>>,
    },
    Put {
        key: K,
        val: V,
        response_sender: oneshot::Sender<bool>,
    },
    GetResponse {
        req_id: u64,
        key: K,
        val: Option<V>,
    },
    PutResponse {
        req_id: u64,
        key: K,
        success: bool,
    },
}
