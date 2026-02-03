use anyhow::Result;
use clap::Parser;
use dht::{Config, LocalMessage, Node};
use rand::Rng;
use std::time::Instant;
use std::{collections::HashMap, sync::mpsc};
use tokio::sync::oneshot;
use tracing::{error, info};

// TODO: varying number of worker threads
#[tokio::main]
async fn main() -> Result<()> {
    let config = Config::parse();
    let num_keys = config.num_keys;

    info!("I am {}", config.name);
    info!("I will connect to {:?}", config.connections);

    // make initial connections
    let (mut node, sender) = Node::<u64, u8>::new(config).await?;

    // run the node in its own task
    tokio::spawn(async move {
        if let Err(e) = node.run().await {
            error!("{e}");
        }
    });

    info!("Generating {} key value pairs", num_keys);
    let test_data = generate_test_data(num_keys);

    info!("Starting test");
    let start = Instant::now();
    let mut handles = Vec::with_capacity(test_data.len());

    for data in test_data {
        let sender = sender.clone();
        handles.push(tokio::spawn(async move {
            match data.operation {
                Operation::Get => {
                    let (response_sender, response_receiver) = oneshot::channel();
                    let message = LocalMessage::Get {
                        key: data.key,
                        response_sender,
                    };
                    sender.send(message).await.unwrap();
                    response_receiver.await.unwrap();
                }
                Operation::Put => {
                    let (response_sender, response_receiver) = oneshot::channel();
                    let message = LocalMessage::Put {
                        key: data.key,
                        val: data.val,
                        response_sender,
                    };
                    sender.send(message).await.unwrap();
                    response_receiver.await.unwrap();
                }
            }
        }));
    }

    // Wait for all to complete
    for handle in handles {
        handle.await?;
    }

    let elapsed = start.elapsed();
    info!("Completed {} operations in {:?}", num_keys, elapsed);
    info!(
        "Throughput: {:.2} ops/sec",
        num_keys as f64 / elapsed.as_secs_f64()
    );

    // wait until the node is ready
    info!("Starting test.");

    Ok(())
}

fn generate_test_data(num_keys: u64) -> Vec<TestData> {
    let mut rng = rand::thread_rng();
    (0..num_keys)
        .map(|_| TestData {
            key: rng.gen(),
            val: rng.gen(),
            operation: if rng.gen_bool(0.8) {
                Operation::Get
            } else {
                Operation::Put
            },
        })
        .collect()
}

struct TestData {
    key: u64,
    val: u8,
    operation: Operation,
}

enum Operation {
    Get,
    Put,
}
