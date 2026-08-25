use std::collections::BTreeMap;

use crate::{
    codec::{CodecError, CompactBlockRecord, TreeSizes, encoded_record_len},
    index::{BlockId, IndexState, PERSIST_DEPTH, WriteBatch},
    parser::PreparedCompactBlock,
};

#[derive(Debug, thiserror::Error)]
pub enum IngestError {
    #[error(transparent)]
    Codec(#[from] CodecError),
    #[error("prepared height {height} is stale or duplicated")]
    DuplicateHeight { height: u32 },
    #[error("block {height} does not connect to the previous canonical hash")]
    Parent { height: u32 },
    #[error("block {height} commitment-tree size overflow")]
    TreeSize { height: u32 },
    #[error("ordered pending bytes exceed the {limit}-byte budget")]
    PendingBytes { limit: usize },
    #[error("write batch bytes exceed the {limit}-byte budget")]
    BatchBytes { limit: usize },
    #[error("height, generation, or byte count overflow")]
    Overflow,
}

/// Restores parser output to canonical height order under a byte budget.
pub struct OrderedBuilder {
    next_height: u32,
    previous_hash: Option<[u8; 32]>,
    tree_sizes: TreeSizes,
    generation: u64,
    pending: BTreeMap<u32, (PreparedCompactBlock, usize)>,
    pending_bytes: usize,
    max_pending_bytes: usize,
}

impl OrderedBuilder {
    pub fn new(state: IndexState, max_pending_bytes: usize) -> Result<Self, IngestError> {
        Ok(Self {
            next_height: match state.durable_tip {
                Some(tip) => tip.height.checked_add(1).ok_or(IngestError::Overflow)?,
                None => 0,
            },
            previous_hash: state.durable_tip.map(|tip| tip.hash),
            tree_sizes: state.tree_sizes,
            generation: state.generation,
            pending: BTreeMap::new(),
            pending_bytes: 0,
            max_pending_bytes,
        })
    }

    pub fn push(&mut self, block: PreparedCompactBlock) -> Result<(), IngestError> {
        if block.height < self.next_height || self.pending.contains_key(&block.height) {
            return Err(IngestError::DuplicateHeight {
                height: block.height,
            });
        }
        let bytes = encoded_record_len(block.header.len(), &block.transactions)?;
        let pending_bytes = self
            .pending_bytes
            .checked_add(bytes)
            .ok_or(IngestError::Overflow)?;
        if pending_bytes > self.max_pending_bytes {
            return Err(IngestError::PendingBytes {
                limit: self.max_pending_bytes,
            });
        }
        self.pending_bytes = pending_bytes;
        self.pending.insert(block.height, (block, bytes));
        Ok(())
    }

    pub(crate) fn pending_bytes(&self) -> usize {
        self.pending_bytes
    }

    /// Builds one bounded durable batch, leaving gaps and depth-0..9 blocks pending.
    pub fn build_batch(
        &mut self,
        source_tip: BlockId,
        max_batch_bytes: usize,
    ) -> Result<Option<WriteBatch>, IngestError> {
        let base_generation = self.generation;
        let mut batch_bytes = 0usize;
        let mut records = Vec::new();

        while let Some((block, bytes)) = self.pending.get(&self.next_height) {
            if source_tip.height.saturating_sub(block.height) < PERSIST_DEPTH
                || source_tip.height < block.height
            {
                break;
            }
            if batch_bytes
                .checked_add(*bytes)
                .ok_or(IngestError::Overflow)?
                > max_batch_bytes
            {
                if records.is_empty() {
                    return Err(IngestError::BatchBytes {
                        limit: max_batch_bytes,
                    });
                }
                break;
            }
            if self
                .previous_hash
                .is_some_and(|previous| block.previous_hash != previous)
            {
                return Err(IngestError::Parent {
                    height: block.height,
                });
            }
            let end_tree_sizes = TreeSizes {
                sapling: self
                    .tree_sizes
                    .sapling
                    .checked_add(block.sapling_additions)
                    .ok_or(IngestError::TreeSize {
                        height: block.height,
                    })?,
                orchard: self
                    .tree_sizes
                    .orchard
                    .checked_add(block.orchard_additions)
                    .ok_or(IngestError::TreeSize {
                        height: block.height,
                    })?,
                ironwood: self
                    .tree_sizes
                    .ironwood
                    .checked_add(block.ironwood_additions)
                    .ok_or(IngestError::TreeSize {
                        height: block.height,
                    })?,
            };
            let next_height = self
                .next_height
                .checked_add(1)
                .ok_or(IngestError::Overflow)?;
            let (block, bytes) = self
                .pending
                .remove(&self.next_height)
                .expect("the pending block was just checked");

            batch_bytes += bytes;
            self.pending_bytes -= bytes;
            self.next_height = next_height;
            self.previous_hash = Some(block.hash);
            self.tree_sizes = end_tree_sizes;
            records.push(CompactBlockRecord {
                height: block.height,
                hash: block.hash,
                previous_hash: block.previous_hash,
                time: block.time,
                header: block.header,
                transactions: block.transactions,
                end_tree_sizes,
            });
        }

        if records.is_empty() {
            return Ok(None);
        }
        self.generation = self
            .generation
            .checked_add(1)
            .ok_or(IngestError::Overflow)?;
        Ok(Some(WriteBatch {
            base_generation,
            source_tip,
            records,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::{CompactSaplingOutput, CompactTransaction};

    #[test]
    fn reassembles_out_of_order_blocks_and_assigns_tree_sizes() {
        let mut first = prepared(0);
        first.transactions.push(CompactTransaction {
            index: 1,
            txid: [7; 32],
            sapling_spends: Vec::new(),
            sapling_outputs: vec![CompactSaplingOutput {
                cmu: [1; 32],
                ephemeral_key: [2; 32],
                ciphertext: [3; 52],
            }],
            orchard_actions: Vec::new(),
            ironwood_actions: Vec::new(),
        });
        first.sapling_additions = 1;
        let mut builder = OrderedBuilder::new(IndexState::default(), 1_000_000).unwrap();

        builder.push(prepared(1)).unwrap();
        assert!(
            builder
                .build_batch(BlockId::new(11, hash(11)), 1_000_000)
                .unwrap()
                .is_none()
        );
        builder.push(first).unwrap();
        let batch = builder
            .build_batch(BlockId::new(11, hash(11)), 1_000_000)
            .unwrap()
            .unwrap();
        assert_eq!(batch.records.len(), 2);
        assert_eq!(batch.records[0].height, 0);
        assert_eq!(batch.records[1].height, 1);
        assert_eq!(batch.records[1].end_tree_sizes.sapling, 1);
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
