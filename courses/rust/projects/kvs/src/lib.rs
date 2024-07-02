#![deny(missing_docs)]
//! A simple key/value store.

pub use command::{CommandPos,DataCommand};
pub use kv::KvStore;
pub use error::{KvsError, Result};

mod kv;
mod error;
mod command;