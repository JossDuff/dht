use clap::Parser;
use dht::{Config, Node};

fn main() {
    let config = Config::parse();

    println!("I am {}", config.name);
    println!("I will connect to {:?}", config.connections);

    let node = Node::new(config);
    node.run();
}
