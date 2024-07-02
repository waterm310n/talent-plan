use clap::{Args, Parser, Subcommand};
use kvs::{KvStore, KvsError, Result};
use serde::de::value;
use std::{env::current_dir, process::exit};

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

fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Get(get_args) => {
            let mut kv_store = KvStore::open(current_dir()?)?;
            match kv_store.get(get_args.key) {
                Ok(value) => match value {
                    Some(value) => println!("{}", value),
                    None => println!("Key not found"),
                },
                Err(e) => {
                    println!("{}", e)
                }
            }
            Ok(())
        }
        Commands::Rm(rm_args) => {
            let mut kv_store = KvStore::open(current_dir()?)?;
            match kv_store.remove(rm_args.key){
                Ok(()) => {Ok(())},
                Err(e) => {
                    match e {
                        KvsError::KeyNotFound => {
                            println!("Key not found")
                        }
                        _ => {}
                    }
                    Err(e)
                }
            }
            
        }
        Commands::Set(set_args) => {
            let mut kv_store = KvStore::open(current_dir()?)?;
            kv_store.set(set_args.key, set_args.value)?;
            Ok(())
        }
    }
}
