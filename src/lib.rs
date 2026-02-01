mod config;

pub use config::Config;

pub struct Node {
    config: Config,
}

impl Node {
    pub fn new(config: Config) -> Self {
        Self { config }
    }

    pub fn run(self) {
        // connect to all other nodes, then send ready check
        //let connections = make_connections(self.config);
        println!("I'm running!");
    }
}

// fn make_connections(config: Config) -> todo!() {
//     todo!()
// }
