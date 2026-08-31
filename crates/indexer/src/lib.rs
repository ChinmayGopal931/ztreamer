pub mod codec;
pub mod head;
pub mod index;
pub mod ingest;
pub mod parser;
pub mod pipeline;
pub mod source;
mod tree;

pub type Digest = [u8; 32];
pub type Ciphertext = [u8; 52];
