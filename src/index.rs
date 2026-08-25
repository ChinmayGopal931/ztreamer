use std::{fs, io, path::Path};

use heed::{
    Database, Env, EnvOpenOptions, RoTxn, RwTxn,
    byteorder::BigEndian,
    types::{Bytes, U32},
};

use crate::codec::{CodecError, CompactBlockRecord, TreeSizes, decode_range_record, encode_range};

pub const SCHEMA_VERSION: u32 = 1;
pub const RANGE_SIZE: u32 = 1_000;
pub const PERSIST_DEPTH: u32 = 10;
pub const SEAL_DEPTH: u32 = 100;

const STATE_FORMAT_VERSION: u8 = 1;
const STATE_BYTES: usize = 62;
const STATE: &[u8] = b"state";

type HeightDb = Database<U32<BigEndian>, Bytes>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BlockId {
    pub height: u32,
    pub hash: [u8; 32],
}

impl BlockId {
    pub const fn new(height: u32, hash: [u8; 32]) -> Self {
        Self { height, hash }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct IndexState {
    pub(crate) durable_tip: Option<BlockId>,
    pub(crate) sealed_through: Option<u32>,
    pub(crate) generation: u64,
    pub(crate) tree_sizes: TreeSizes,
}

impl IndexState {
    pub fn durable_tip(&self) -> Option<BlockId> {
        self.durable_tip
    }

    pub fn sealed_through(&self) -> Option<u32> {
        self.sealed_through
    }

    pub fn generation(&self) -> u64 {
        self.generation
    }

    pub fn tree_sizes(&self) -> TreeSizes {
        self.tree_sizes
    }
}

/// A batch whose ordering and cumulative tree sizes were checked by the ordered builder.
pub struct WriteBatch {
    pub(crate) base_generation: u64,
    pub(crate) source_tip: BlockId,
    pub(crate) records: Vec<CompactBlockRecord>,
}

#[derive(Debug, thiserror::Error)]
pub enum IndexError {
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error(transparent)]
    Heed(#[from] heed::Error),
    #[error(transparent)]
    Codec(#[from] CodecError),
    #[error("LMDB network, genesis hash, or fixed index policy does not match")]
    Identity,
    #[error("LMDB metadata is incomplete or inconsistent")]
    Metadata,
    #[error("write batch was built from stale index generation {batch_generation}")]
    StaleBatch { batch_generation: u64 },
    #[error("mutable range starting at {start} is incomplete")]
    IncompleteRange { start: u32 },
    #[error("LMDB compact coverage is not contiguous at height {height}")]
    Continuity { height: u32 },
    #[error("LMDB hash index does not match compact block at height {height}")]
    HashIndex { height: u32 },
    #[error("height or generation overflow")]
    Overflow,
}

/// Ztreamer's four-database LMDB index.
pub struct Index {
    env: Env,
    metadata: Database<Bytes, Bytes>,
    sealed_ranges: HeightDb,
    mutable_blocks: HeightDb,
    hash_to_height: Database<Bytes, U32<BigEndian>>,
}

impl Index {
    /// Opens the index and rejects an environment created for another chain or format.
    pub fn open(
        path: impl AsRef<Path>,
        map_size: usize,
        network: &str,
        genesis_hash: [u8; 32],
    ) -> Result<Self, IndexError> {
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
            Some(stored) if stored != identity => return Err(IndexError::Identity),
            None => metadata.put(&mut txn, b"identity".as_slice(), identity.as_slice())?,
            Some(_) => {}
        }
        txn.commit()?;

        let index = Self {
            env,
            metadata,
            sealed_ranges,
            mutable_blocks,
            hash_to_height,
        };
        index.verify_continuity()?;
        Ok(index)
    }

    pub fn state(&self) -> Result<IndexState, IndexError> {
        let txn = self.env.read_txn()?;
        read_state(self.metadata, &txn)
    }

    /// Checks sealed-range summaries and every mutable suffix row without rescanning sealed blocks.
    pub fn verify_continuity(&self) -> Result<(), IndexError> {
        let txn = self.env.read_txn()?;
        let state = read_state(self.metadata, &txn)?;
        let Some(tip) = state.durable_tip else {
            if !self.sealed_ranges.is_empty(&txn)?
                || !self.mutable_blocks.is_empty(&txn)?
                || !self.hash_to_height.is_empty(&txn)?
            {
                return Err(IndexError::Metadata);
            }
            return Ok(());
        };

        let sealed_blocks = state
            .sealed_through
            .map_or(0, |height| u64::from(height) + 1);
        if self.sealed_ranges.len(&txn)? != sealed_blocks / u64::from(RANGE_SIZE)
            || self.mutable_blocks.len(&txn)? != u64::from(tip.height) + 1 - sealed_blocks
            || self.hash_to_height.len(&txn)? != u64::from(tip.height) + 1
        {
            return Err(IndexError::Continuity { height: 0 });
        }

        let mut next_height = 0u64;
        let mut previous_hash = None;
        for entry in self.sealed_ranges.iter(&txn)? {
            let (start, bytes) = entry?;
            if u64::from(start) != next_height {
                return Err(IndexError::Continuity {
                    height: next_height as u32,
                });
            }
            let first = decode_range_record(bytes, 0)?;
            let last = decode_range_record(bytes, RANGE_SIZE as usize - 1)?;
            if previous_hash.is_some_and(|hash| first.previous_hash != hash) {
                return Err(IndexError::Continuity { height: start });
            }
            self.verify_hash_entry(&txn, &first)?;
            self.verify_hash_entry(&txn, &last)?;
            previous_hash = Some(last.hash);
            next_height += u64::from(RANGE_SIZE);
        }
        if next_height != sealed_blocks {
            return Err(IndexError::Continuity {
                height: next_height as u32,
            });
        }

        for entry in self.mutable_blocks.iter(&txn)? {
            let (height, bytes) = entry?;
            if u64::from(height) != next_height {
                return Err(IndexError::Continuity {
                    height: next_height as u32,
                });
            }
            let record = CompactBlockRecord::decode(bytes)?;
            if record.height != height
                || previous_hash.is_some_and(|hash| record.previous_hash != hash)
            {
                return Err(IndexError::Continuity { height });
            }
            self.verify_hash_entry(&txn, &record)?;
            previous_hash = Some(record.hash);
            next_height += 1;
        }
        let tip_record = if state
            .sealed_through
            .is_some_and(|sealed| tip.height <= sealed)
        {
            let start = tip.height - tip.height % RANGE_SIZE;
            decode_range_record(
                self.sealed_ranges
                    .get(&txn, &start)?
                    .ok_or(IndexError::Continuity { height: tip.height })?,
                (tip.height - start) as usize,
            )?
        } else {
            CompactBlockRecord::decode(
                self.mutable_blocks
                    .get(&txn, &tip.height)?
                    .ok_or(IndexError::Continuity { height: tip.height })?,
            )?
        };
        if next_height != u64::from(tip.height) + 1
            || previous_hash != Some(tip.hash)
            || state.tree_sizes != tip_record.end_tree_sizes
        {
            return Err(IndexError::Continuity { height: tip.height });
        }
        Ok(())
    }

    fn verify_hash_entry(
        &self,
        txn: &RoTxn<'_>,
        record: &CompactBlockRecord,
    ) -> Result<(), IndexError> {
        if self.hash_to_height.get(txn, record.hash.as_slice())? != Some(record.height) {
            return Err(IndexError::HashIndex {
                height: record.height,
            });
        }
        Ok(())
    }

    /// Atomically writes a batch and packs newly sealed ranges.
    pub fn write(&self, batch: WriteBatch) -> Result<IndexState, IndexError> {
        let mut txn = self.env.write_txn()?;
        let mut state = read_state(self.metadata, &txn)?;
        let first = batch
            .records
            .first()
            .expect("builders never emit empty batches");
        let expected_height = match state.durable_tip {
            Some(tip) => tip.height.checked_add(1).ok_or(IndexError::Overflow)?,
            None => 0,
        };
        if state.generation != batch.base_generation
            || first.height != expected_height
            || state
                .durable_tip
                .is_some_and(|tip| first.previous_hash != tip.hash)
        {
            return Err(IndexError::StaleBatch {
                batch_generation: batch.base_generation,
            });
        }

        for record in &batch.records {
            let encoded = record.encode()?;
            self.mutable_blocks
                .put(&mut txn, &record.height, encoded.as_slice())?;
            self.hash_to_height
                .put(&mut txn, record.hash.as_slice(), &record.height)?;
        }
        let last = batch.records.last().expect("the batch is non-empty");
        state.durable_tip = Some(BlockId::new(last.height, last.hash));
        state.tree_sizes = last.end_tree_sizes;
        self.pack_sealed_ranges(&mut txn, batch.source_tip.height, &mut state)?;
        state.generation = state
            .generation
            .checked_add(1)
            .ok_or(IndexError::Overflow)?;
        let encoded = encode_state(state);
        self.metadata.put(&mut txn, STATE, encoded.as_slice())?;
        txn.commit()?;
        Ok(state)
    }

    fn pack_sealed_ranges(
        &self,
        txn: &mut RwTxn<'_>,
        source_tip: u32,
        state: &mut IndexState,
    ) -> Result<(), IndexError> {
        let Some(seal_cutoff) = source_tip.checked_sub(SEAL_DEPTH) else {
            return Ok(());
        };
        let mut start = state.sealed_through.map_or(Ok(0), |height| {
            height.checked_add(1).ok_or(IndexError::Overflow)
        })?;

        loop {
            let end = start
                .checked_add(RANGE_SIZE - 1)
                .ok_or(IndexError::Overflow)?;
            // exit early if the range is not ready to be sealed yet
            if end > seal_cutoff
                || state
                    .durable_tip
                    .is_none_or(|durable_tip| end > durable_tip.height)
            {
                break;
            }
            let records = (start..=end)
                .map(|height| {
                    self.mutable_blocks
                        .get(txn, &height)?
                        .ok_or(IndexError::IncompleteRange { start })
                        .and_then(|bytes| CompactBlockRecord::decode(bytes).map_err(Into::into))
                })
                .collect::<Result<Vec<_>, IndexError>>()?;
            let encoded = encode_range(&records)?;
            self.sealed_ranges.put(txn, &start, encoded.as_slice())?;
            for height in start..=end {
                self.mutable_blocks.delete(txn, &height)?;
            }
            state.sealed_through = Some(end);
            start = end.checked_add(1).ok_or(IndexError::Overflow)?;
        }
        Ok(())
    }
}

fn read_state(metadata: Database<Bytes, Bytes>, txn: &RoTxn<'_>) -> Result<IndexState, IndexError> {
    metadata
        .get(txn, STATE)?
        .map(decode_state)
        .transpose()
        .map(Option::unwrap_or_default)
}

fn encode_state(state: IndexState) -> Vec<u8> {
    let tip = state
        .durable_tip
        .expect("only non-empty index state is persisted");
    let mut bytes = Vec::with_capacity(STATE_BYTES);
    bytes.push(STATE_FORMAT_VERSION);
    bytes.extend_from_slice(&tip.height.to_be_bytes());
    bytes.extend_from_slice(&tip.hash);
    bytes.push(u8::from(state.sealed_through.is_some()));
    bytes.extend_from_slice(&state.sealed_through.unwrap_or_default().to_be_bytes());
    bytes.extend_from_slice(&state.generation.to_be_bytes());
    bytes.extend_from_slice(&state.tree_sizes.sapling.to_be_bytes());
    bytes.extend_from_slice(&state.tree_sizes.orchard.to_be_bytes());
    bytes.extend_from_slice(&state.tree_sizes.ironwood.to_be_bytes());
    bytes
}

fn decode_state(bytes: &[u8]) -> Result<IndexState, IndexError> {
    if bytes.len() != STATE_BYTES || bytes[0] != STATE_FORMAT_VERSION || bytes[37] > 1 {
        return Err(IndexError::Metadata);
    }
    let u32_at = |start| {
        u32::from_be_bytes(
            bytes[start..start + 4]
                .try_into()
                .expect("state length was checked"),
        )
    };
    let tip = BlockId::new(
        u32_at(1),
        bytes[5..37].try_into().expect("state length was checked"),
    );
    let sealed_through = (bytes[37] == 1).then(|| u32_at(38));
    if sealed_through
        .is_some_and(|sealed| sealed > tip.height || sealed % RANGE_SIZE != RANGE_SIZE - 1)
    {
        return Err(IndexError::Metadata);
    }
    Ok(IndexState {
        durable_tip: Some(tip),
        sealed_through,
        generation: u64::from_be_bytes(bytes[42..50].try_into().expect("state length was checked")),
        tree_sizes: TreeSizes {
            sapling: u32_at(50),
            orchard: u32_at(54),
            ironwood: u32_at(58),
        },
    })
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
    use crate::{codec::decode_range_record, ingest::OrderedBuilder, parser::PreparedCompactBlock};

    #[test]
    fn creates_four_databases_and_checks_chain_identity() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("index");

        drop(Index::open(&path, 10 * 1024 * 1024, "Mainnet", [1; 32]).unwrap());
        drop(Index::open(&path, 10 * 1024 * 1024, "Mainnet", [1; 32]).unwrap());
        assert!(Index::open(&path, 10 * 1024 * 1024, "Testnet", [1; 32]).is_err());
        assert!(Index::open(&path, 10 * 1024 * 1024, "Mainnet", [2; 32]).is_err());
    }

    #[test]
    fn atomically_writes_restarts_and_packs_sealed_ranges() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("index");
        let index = Index::open(&path, 10 * 1024 * 1024, "Mainnet", [1; 32]).unwrap();
        let stale = batch(&index, 0..=0, 1_100);
        let state = index.write(batch(&index, 0..=1_000, 1_100)).unwrap();

        assert_eq!(state.durable_tip().unwrap().height, 1_000);
        assert_eq!(state.sealed_through(), Some(999));
        assert_eq!(state.generation(), 1);
        let txn = index.env.read_txn().unwrap();
        assert!(index.mutable_blocks.get(&txn, &0).unwrap().is_none());
        assert!(index.mutable_blocks.get(&txn, &1_000).unwrap().is_some());
        let range = index.sealed_ranges.get(&txn, &0).unwrap().unwrap();
        assert_eq!(decode_range_record(range, 537).unwrap().height, 537);
        assert_eq!(
            index
                .hash_to_height
                .get(&txn, hash(537).as_slice())
                .unwrap(),
            Some(537)
        );
        drop(txn);

        assert!(matches!(
            index.write(stale),
            Err(IndexError::StaleBatch {
                batch_generation: 0
            })
        ));
        assert_eq!(index.state().unwrap(), state);

        drop(index);
        let reopened = Index::open(&path, 10 * 1024 * 1024, "Mainnet", [1; 32]).unwrap();
        assert_eq!(reopened.state().unwrap(), state);
    }

    #[test]
    fn restart_rejects_a_gap_in_mutable_coverage() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("index");
        let index = Index::open(&path, 10 * 1024 * 1024, "Mainnet", [1; 32]).unwrap();
        index.write(batch(&index, 0..=10, 20)).unwrap();
        let mut txn = index.env.write_txn().unwrap();
        index.mutable_blocks.delete(&mut txn, &5).unwrap();
        txn.commit().unwrap();
        drop(index);

        assert!(matches!(
            Index::open(&path, 10 * 1024 * 1024, "Mainnet", [1; 32]),
            Err(IndexError::Continuity { .. })
        ));
    }

    fn batch(index: &Index, heights: std::ops::RangeInclusive<u32>, source_tip: u32) -> WriteBatch {
        let mut builder = OrderedBuilder::new(index.state().unwrap(), 10 * 1024 * 1024).unwrap();
        for height in heights {
            builder.push(prepared(height)).unwrap();
        }
        builder
            .build_batch(BlockId::new(source_tip, hash(source_tip)), 10 * 1024 * 1024)
            .unwrap()
            .unwrap()
    }

    fn prepared(height: u32) -> PreparedCompactBlock {
        PreparedCompactBlock {
            height,
            hash: hash(height),
            previous_hash: height.checked_sub(1).map(hash).unwrap_or([0; 32]),
            time: height,
            header: Vec::new(),
            transactions: Vec::new(),
            sapling_additions: 0,
            orchard_additions: 0,
            ironwood_additions: 0,
        }
    }

    fn hash(height: u32) -> [u8; 32] {
        let mut hash = [0; 32];
        hash[..4].copy_from_slice(&height.to_be_bytes());
        hash
    }
}
