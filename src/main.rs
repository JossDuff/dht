use clap::Parser;
use dht::config::Config;

fn main() {
    let config = Config::parse();

    println!("I am {}", config.name);
    println!("I will connect to {:?}", config.connections);
}
