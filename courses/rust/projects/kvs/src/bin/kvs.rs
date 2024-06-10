use clap::{Args, Parser, Subcommand};
use std::process::exit;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Set the value of a string key to a string
    Get(GetArgs),
    /// Get the string value of a given string key
    Set(SetArgs),
    /// Remove a given key
    Rm(RmArgs),
}

#[derive(Args)]
struct GetArgs {
    /// A string key
    key: String,
}

#[derive(Args)]
struct SetArgs {
    /// A string key
    key: String,
    /// The string value of the key
    value: String,
}

#[derive(Args)]
struct RmArgs {
    /// A string key
    key: String,
}

fn main() {
    let cli = Cli::parse();

    match cli.command {
        Commands::Get(_) => {
            eprintln!("unimplemented");
            exit(1);
        }
        Commands::Rm(_) => {
            eprintln!("unimplemented");
            exit(1);
        }
        Commands::Set(_) => {
            eprintln!("unimplemented");
            exit(1);
        }
    }
}
