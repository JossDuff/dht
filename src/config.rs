use clap::Parser;

/// Simple program to greet a person
#[derive(Parser, Debug)]
#[command(version, about)]
pub struct Config {
    /// Name of this node
    #[arg(short, long, required = true)]
    pub name: String,

    /// Names of sunlab nodes to connect to
    #[arg(short, long, required = true, value_delimiter = ',')]
    pub connections: Vec<String>,

    /// Maximum key value (for test harness)
    #[arg(short, long, default_value = "9999")]
    pub max_key: u64,
}
