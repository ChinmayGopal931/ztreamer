pub mod codec;
pub mod head;
pub mod index;
pub mod ingest;
pub mod parser;
pub mod pipeline;
pub mod source;

pub type Digest = [u8; 32];
pub type EphemeralKey = [u8; 32];
pub type Ciphertext = [u8; 52];
