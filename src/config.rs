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
}
