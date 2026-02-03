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
use tracing::{debug, error, info, warn};

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
    awaiting_put_response: Arc<Mutex<HashMap<u64, oneshot::Sender<bool>>>>,
    awaiting_get_response: Arc<Mutex<HashMap<u64, oneshot::Sender<Option<V>>>>>,
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
        + PartialEq
        + Clone
        + Copy,
    V: Send + Sync + 'static + Debug + Serialize + for<'de> Deserialize<'de> + Clone,
{
    pub async fn new(config: Config) -> Result<(Self, mpsc::Sender<LocalMessage<K, V>>)> {
        let db = Arc::new(Mutex::new(HashMap::new()));
        let awaiting_put_response: Arc<Mutex<HashMap<u64, oneshot::Sender<bool>>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let awaiting_get_response: Arc<Mutex<HashMap<u64, oneshot::Sender<Option<V>>>>> =
            Arc::new(Mutex::new(HashMap::new()));

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
                awaiting_get_response,
                awaiting_put_response,
                db,
            },
            local_sender,
        ))
    }

    pub async fn run(&mut self) -> Result<()> {
        loop {
            tokio::select! {
                Some(local_msg) = self.local_inbox.recv() => {
                    self.handle_local_message(local_msg).await?;
                }
                Some((from, peer_msg)) = self.peers.inbox.recv() => {
                    self.handle_peer_message(from, peer_msg).await?;
                }
                else => {
                    info!("All channels closed");
                    break;
                }
            }
        }
        Ok(())
    }

    // handles messages from the test harness and from the network loop when peers respond
    async fn handle_local_message(&self, msg: LocalMessage<K, V>) -> Result<()> {
        match msg {
            LocalMessage::Get {
                key,
                response_sender,
            } => {
                let key_owner = self.get_key_owner(&key);
                // key is locally owned, immedietly respond
                if *key_owner == self.my_node_id {
                    let resp = self.local_get(&key).await;
                    response_sender
                        .send(resp)
                        .map_err(|_| anyhow!("Receiver for local get on key {:?} dropped", key))?;
                } else {
                    // we need to request the key's owner
                    let req_id: u64 = rand::thread_rng().gen();
                    let request: PeerMessage<K, V> = PeerMessage::Get { key, req_id };
                    let _ = self.peers.send(key_owner, request).await?;

                    // create a task awaiting the response from a peer
                    let mut awaiting_get_response = self.awaiting_get_response.lock().await;
                    let (get_sender, get_receiver) = oneshot::channel();
                    awaiting_get_response.insert(req_id, get_sender);

                    // start a non-blocking task that awaits this response and sends back to test
                    // harness
                    tokio::spawn(async move {
                        match get_receiver.await {
                            Ok(peer_get_response) => {
                                // send get response back to test harness
                                response_sender.send(peer_get_response);
                            }
                            Err(e) => {
                                error!("Receive error waiting for getResponse: {}", e);
                            }
                        }
                    });
                }
            }
            LocalMessage::Put {
                key,
                val,
                response_sender,
            } => {
                let key_owner = self.get_key_owner(&key);
                // key is locally owned, immedietly respond
                if *key_owner == self.my_node_id {
                    let resp = self.local_insert(key, val).await;
                    response_sender
                        .send(resp)
                        .map_err(|_| anyhow!("Receiver for local put on key {:?} dropped", key))?;
                } else {
                    // we need to request the key's owner
                    let req_id: u64 = rand::thread_rng().gen();
                    let request: PeerMessage<K, V> = PeerMessage::Put { key, req_id, val };
                    let _ = self.peers.send(key_owner, request).await?;

                    // create a task awaiting the response from a peer
                    let mut awaiting_put_response = self.awaiting_put_response.lock().await;
                    let (put_sender, put_receiver) = oneshot::channel();
                    awaiting_put_response.insert(req_id, put_sender);

                    // start a non-blocking task that awaits this response and sends back to test
                    // harness
                    tokio::spawn(async move {
                        match put_receiver.await {
                            Ok(peer_put_response) => {
                                // send get response back to test harness
                                response_sender.send(peer_put_response);
                            }
                            Err(e) => {
                                error!("Receive error waiting for putResponse: {}", e);
                            }
                        }
                    });
                }
            }
        }

        Ok(())
    }

    // handles messages from the network
    async fn handle_peer_message(&self, from: NodeId, msg: PeerMessage<K, V>) -> Result<()> {
        info!("Got {:?} from {}", msg, from);
        match msg {
            // peer is asking us for a get request
            PeerMessage::Get { key, req_id } => {
                let result = self.local_get(&key).await;
                let resp: PeerMessage<K, V> = PeerMessage::GetResponse {
                    key: key.clone(),
                    val: result,
                    req_id,
                };

                self.peers
                    .send(&from, resp)
                    .await
                    .map_err(|e| anyhow!("Error sending GetResponse to node {}: {}", from, e))?;

                debug!(
                    "Sent GetResponse to {} for key {:?} req_id {}",
                    from, key, req_id
                );
            }
            // peer is asking us for a put request
            PeerMessage::Put { key, val, req_id } => {
                let result = self.local_insert(key.clone(), val).await;
                let resp: PeerMessage<K, V> = PeerMessage::PutResponse {
                    key,
                    success: result,
                    req_id,
                };

                self.peers
                    .send(&from, resp)
                    .await
                    .map_err(|e| anyhow!("Error sending PutResponse to node {}: {}", from, e))?;

                debug!(
                    "Sent PutResponse to {} for key {:?} req_id {}",
                    from, key, req_id
                );
            }
            // received a response from a peer about a previous get request
            PeerMessage::GetResponse { key, val, req_id } => {
                // look up the channel for sending the response
                let mut awaiting_get_response = self.awaiting_get_response.lock().await;
                match awaiting_get_response.remove(&req_id) {
                    Some(sender) => {
                        // send the response to the local task awaiting it
                        sender.send(val).map_err(|_| {
                            anyhow!("Error sending local GetResponse for request {}", req_id)
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
                let mut awaiting_put_response = self.awaiting_put_response.lock().await;
                match awaiting_put_response.remove(&req_id) {
                    Some(sender) => {
                        // send the response for the local task awaiting it
                        sender.send(success).map_err(|_| {
                            anyhow!("Error sending local PutResponse for request {}", req_id)
                        })?;
                    }
                    None => {
                        return Err(anyhow!("Receiver for req_id {} dropped", req_id));
                    }
                };
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

pub struct GetResponse<V>(Option<V>);

#[derive(Debug)]
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
}
