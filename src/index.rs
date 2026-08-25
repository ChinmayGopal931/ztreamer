use std::{fs, io, path::Path};

use heed::{
    Database, Env, EnvOpenOptions,
    byteorder::BigEndian,
    types::{Bytes, U32},
};

pub const SCHEMA_VERSION: u32 = 1;
pub const RANGE_SIZE: u32 = 1_000;
pub const PERSIST_DEPTH: u32 = 10;
pub const SEAL_DEPTH: u32 = 100;

type HeightDb = Database<U32<BigEndian>, Bytes>;

/// Ztreamer's four-database LMDB index.
pub struct Index {
    _env: Env,
    _metadata: Database<Bytes, Bytes>,
    _sealed_ranges: HeightDb,
    _mutable_blocks: HeightDb,
    _hash_to_height: Database<Bytes, U32<BigEndian>>,
}

impl Index {
    /// Opens the index and rejects an environment created for another chain or format.
    pub fn open(
        path: impl AsRef<Path>,
        map_size: usize,
        network: &str,
        genesis_hash: [u8; 32],
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        fs::create_dir_all(path.as_ref())?;
        // SAFETY: callers must not open this path with incompatible LMDB options in this process.
        let env = unsafe {
            EnvOpenOptions::new()
                .map_size(map_size)
                .max_dbs(4)
                .open(path.as_ref())?
        };
        let mut txn = env.write_txn()?;
        let metadata = env.create_database(&mut txn, Some("metadata"))?;
        let sealed_ranges = env.create_database(&mut txn, Some("sealed_ranges"))?;
        let mutable_blocks = env.create_database(&mut txn, Some("mutable_blocks"))?;
        let hash_to_height = env.create_database(&mut txn, Some("hash_to_height"))?;

        let identity = identity(network, genesis_hash);
        match metadata.get(&txn, b"identity".as_slice())? {
            Some(stored) if stored != identity => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "LMDB network, genesis hash, or fixed index policy does not match",
                )
                .into());
            }
            None => metadata.put(&mut txn, b"identity".as_slice(), identity.as_slice())?,
            Some(_) => {}
        }
        txn.commit()?;

        Ok(Self {
            _env: env,
            _metadata: metadata,
            _sealed_ranges: sealed_ranges,
            _mutable_blocks: mutable_blocks,
            _hash_to_height: hash_to_height,
        })
    }
}

fn identity(network: &str, genesis_hash: [u8; 32]) -> Vec<u8> {
    [
        SCHEMA_VERSION.to_be_bytes().as_slice(),
        RANGE_SIZE.to_be_bytes().as_slice(),
        PERSIST_DEPTH.to_be_bytes().as_slice(),
        SEAL_DEPTH.to_be_bytes().as_slice(),
        genesis_hash.as_slice(),
        network.as_bytes(),
    ]
    .concat()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn creates_four_databases_and_checks_chain_identity() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("index");

        drop(Index::open(&path, 10 * 1024 * 1024, "Mainnet", [1; 32]).unwrap());
        drop(Index::open(&path, 10 * 1024 * 1024, "Mainnet", [1; 32]).unwrap());
        assert!(Index::open(&path, 10 * 1024 * 1024, "Testnet", [1; 32]).is_err());
        assert!(Index::open(&path, 10 * 1024 * 1024, "Mainnet", [2; 32]).is_err());
    }
}
