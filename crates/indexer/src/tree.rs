use std::{
    fmt::Display,
    io::Cursor,
    sync::mpsc::{self, Sender},
    thread,
    time::{Duration, Instant},
};

use bincode::Options;
use heed::{
    Database, Env,
    byteorder::BigEndian,
    types::{Bytes, U32},
};
use serde::{Deserialize, Serialize};
use zakura_chain::{
    orchard::tree as orchard, sapling::tree as sapling, serialization::ZcashDeserialize,
    subtree::TRACKED_SUBTREE_HEIGHT,
};

use crate::{
    codec::CompactBlockRecord,
    index::{IndexError, RANGE_SIZE},
};

const MAX_ANCHOR_BYTES: u64 = 1 << 20;
type HeightDb = Database<U32<BigEndian>, Bytes>;

/// A serialized tree checkpoint stored in LMDB at the end of a sealed range.
#[derive(Default, Deserialize, Serialize)]
pub(super) struct Anchor {
    hash: [u8; 32],
    sapling: sapling::NoteCommitmentTree,
    orchard: orchard::NoteCommitmentTree,
    ironwood: orchard::NoteCommitmentTree,
}

impl Anchor {
    pub(super) fn append_blocks(
        &mut self,
        blocks: &[CompactBlockRecord],
    ) -> Result<(), IndexError> {
        let Self {
            sapling,
            orchard,
            ironwood,
            ..
        } = self;
        let (sapling, (orchard, ironwood)) = rayon::join(
            || {
                let commitments = blocks
                    .iter()
                    .flat_map(|block| &block.transactions)
                    .flat_map(|tx| &tx.sapling_outputs)
                    .map(|output| {
                        sapling::NoteCommitmentUpdate::zcash_deserialize(Cursor::new(output.cmu))
                    })
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|error| IndexError::Tree(error.to_string()))?;
                append_sapling(sapling, &commitments)
            },
            || {
                rayon::join(
                    || append_orchard(orchard, &commitments(blocks, false)?),
                    || append_orchard(ironwood, &commitments(blocks, true)?),
                )
            },
        );
        sapling?;
        orchard?;
        ironwood
    }

    fn encode(&self) -> Result<Vec<u8>, IndexError> {
        Self::options()
            .serialize(self)
            .map_err(|error| IndexError::Tree(error.to_string()))
    }

    pub(super) fn decode(bytes: &[u8]) -> Result<Self, IndexError> {
        Self::options()
            .deserialize(bytes)
            .map_err(|error| IndexError::Tree(error.to_string()))
    }

    fn options() -> impl Options {
        bincode::DefaultOptions::new()
            .with_fixint_encoding()
            .with_big_endian()
            .with_limit(MAX_ANCHOR_BYTES)
            .reject_trailing_bytes()
    }

    pub(super) fn hash(&self) -> [u8; 32] {
        self.hash
    }

    pub(super) fn into_result(self) -> TreeStateResult {
        TreeStateResult {
            sapling: self.sapling.to_rpc_bytes(),
            orchard: self.orchard.to_rpc_bytes(),
            ironwood: self.ironwood.to_rpc_bytes(),
        }
    }
}

/// Locally derived commitment trees ready for a `TreeState` RPC response.
pub struct TreeStateResult {
    pub sapling: Vec<u8>,
    pub orchard: Vec<u8>,
    pub ironwood: Vec<u8>,
}

#[derive(Clone, Copy, Debug, Default)]
pub(super) struct TreeStats {
    pub(super) jobs: u64,
    pub(super) blocks: u64,
    pub(super) compute: Duration,
    pub(super) wall: Duration,
}

/// Commands processed in order by the single tree-state worker.
enum Command {
    /// Append one complete sealed range.
    Range(Vec<CompactBlockRecord>),
    /// Discard worker state at and after this range start.
    Reset(u32),
    /// Report completion of all preceding commands.
    Flush(Sender<Result<TreeStats, String>>),
}

pub(super) struct TreeQueue {
    tx: Option<Sender<Command>>,
    worker: Option<thread::JoinHandle<()>>,
}

impl TreeQueue {
    pub(super) fn start(env: Env, tree_anchors: HeightDb) -> Self {
        let (tx, rx) = mpsc::channel();
        let worker = thread::Builder::new()
            .name("tree-state-indexer".into())
            .spawn(move || {
                let mut worker = Worker::new(env, tree_anchors);
                while let Ok(command) = rx.recv() {
                    match command {
                        Command::Range(blocks) if worker.error.is_none() => {
                            if let Err(error) = worker.range(blocks) {
                                worker.error = Some(error.to_string());
                            }
                        }
                        Command::Reset(start) => {
                            if let Err(error) = worker.reset(start) {
                                worker.error = Some(error.to_string());
                            }
                        }
                        Command::Flush(reply) => {
                            let result =
                                worker.error.clone().map_or_else(|| Ok(worker.stats()), Err);
                            let _ = reply.send(result);
                        }
                        Command::Range(_) => {}
                    }
                }
            })
            .expect("tree-state worker thread starts");
        Self {
            tx: Some(tx),
            worker: Some(worker),
        }
    }

    pub(super) fn range(&self, blocks: Vec<CompactBlockRecord>) {
        let _ = self
            .tx
            .as_ref()
            .expect("queue is live")
            .send(Command::Range(blocks));
    }

    pub(super) fn reset(&self, start: u32) {
        let _ = self
            .tx
            .as_ref()
            .expect("queue is live")
            .send(Command::Reset(start));
    }

    pub(super) fn flush(&self) -> Result<TreeStats, String> {
        let (tx, rx) = mpsc::channel();
        self.tx
            .as_ref()
            .expect("queue is live")
            .send(Command::Flush(tx))
            .map_err(|_| "worker stopped".to_string())?;
        rx.recv().map_err(|_| "worker stopped".to_string())?
    }
}

impl Drop for TreeQueue {
    fn drop(&mut self) {
        self.tx.take();
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

struct Worker {
    env: Env,
    tree_anchors: HeightDb,
    anchor: Anchor,
    anchor_height: Option<u32>,
    started: Option<Instant>,
    jobs: u64,
    blocks: u64,
    compute: Duration,
    error: Option<String>,
}

impl Worker {
    fn new(env: Env, tree_anchors: HeightDb) -> Self {
        let mut worker = Self {
            env,
            tree_anchors,
            anchor: Anchor::default(),
            anchor_height: None,
            started: None,
            jobs: 0,
            blocks: 0,
            compute: Duration::ZERO,
            error: None,
        };
        if let Err(error) = worker.resume() {
            worker.error = Some(error.to_string());
        }
        worker
    }

    fn resume(&mut self) -> Result<(), IndexError> {
        let txn = self.env.read_txn()?;
        if let Some((height, bytes)) = self.tree_anchors.last(&txn)? {
            self.anchor_height = Some(height);
            self.anchor = Anchor::decode(bytes)?;
        }
        Ok(())
    }

    fn reset(&mut self, start: u32) -> Result<(), IndexError> {
        self.error = None;
        self.anchor_height = start.checked_sub(1);
        self.anchor = self
            .anchor_height
            .map(|height| self.load(height))
            .transpose()?
            .flatten()
            .unwrap_or_default();
        Ok(())
    }

    fn load(&self, height: u32) -> Result<Option<Anchor>, IndexError> {
        let txn = self.env.read_txn()?;
        self.tree_anchors
            .get(&txn, &height)?
            .map(Anchor::decode)
            .transpose()
    }

    fn range(&mut self, blocks: Vec<CompactBlockRecord>) -> Result<(), IndexError> {
        let start = blocks.first().ok_or(IndexError::Metadata)?.height;
        let end = blocks.last().ok_or(IndexError::Metadata)?.height;
        if blocks.len() != RANGE_SIZE as usize || end != start + RANGE_SIZE - 1 {
            return Err(IndexError::IncompleteRange { start });
        }
        if self.anchor_height.is_some_and(|height| end <= height) {
            return Ok(());
        }
        let expected = self.anchor_height.map_or(0, |height| height + 1);
        if start != expected {
            return Err(IndexError::Tree(format!(
                "expected range {expected}, got {start}"
            )));
        }
        let started = Instant::now();
        self.started.get_or_insert(started);
        let hash = blocks.last().expect("range is non-empty").hash;
        self.anchor.append_blocks(&blocks)?;
        self.anchor.hash = hash;
        let encoded = self.anchor.encode()?;
        let mut txn = self.env.write_txn()?;
        self.tree_anchors.put(&mut txn, &end, &encoded)?;
        txn.commit()?;
        self.anchor_height = Some(end);
        self.jobs += 1;
        self.blocks += u64::from(RANGE_SIZE);
        self.compute += started.elapsed();
        Ok(())
    }

    fn stats(&self) -> TreeStats {
        TreeStats {
            jobs: self.jobs,
            blocks: self.blocks,
            compute: self.compute,
            wall: self.started.map_or(Duration::ZERO, |time| time.elapsed()),
        }
    }
}

fn append_sapling(
    tree: &mut sapling::NoteCommitmentTree,
    commitments: &[sapling::NoteCommitmentUpdate],
) -> Result<(), IndexError> {
    append_chunks(tree.position(), commitments, |chunk| {
        tree.append_batch(chunk)
    })
}

fn append_orchard(
    tree: &mut orchard::NoteCommitmentTree,
    commitments: &[orchard::NoteCommitmentUpdate],
) -> Result<(), IndexError> {
    append_chunks(tree.position(), commitments, |chunk| {
        tree.append_batch(chunk)
    })
}

fn append_chunks<T, E: Display, R>(
    position: Option<u64>,
    commitments: &[T],
    mut append: impl FnMut(&[T]) -> Result<R, E>,
) -> Result<(), IndexError> {
    let subtree_size = 1usize << TRACKED_SUBTREE_HEIGHT;
    let mut size = position.map_or(0, |position| position as usize + 1);
    let mut offset = 0;
    while offset < commitments.len() {
        let to_boundary = subtree_size - size % subtree_size;
        let end = (offset + to_boundary).min(commitments.len());
        append(&commitments[offset..end]).map_err(|error| IndexError::Tree(error.to_string()))?;
        size += end - offset;
        offset = end;
    }
    Ok(())
}

fn commitments(
    blocks: &[CompactBlockRecord],
    ironwood: bool,
) -> Result<Vec<orchard::NoteCommitmentUpdate>, IndexError> {
    blocks
        .iter()
        .flat_map(|block| &block.transactions)
        .flat_map(|tx| {
            if ironwood {
                tx.ironwood_actions.as_slice()
            } else {
                tx.orchard_actions.as_slice()
            }
        })
        .map(|action| {
            orchard::NoteCommitmentUpdate::zcash_deserialize(Cursor::new(action.commitment))
        })
        .collect::<Result<_, _>>()
        .map_err(|error| IndexError::Tree(error.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bulk_append_matches_sequential_append() {
        let commitments = (1u64..=64)
            .map(|value| {
                let mut bytes = [0; 32];
                bytes[..8].copy_from_slice(&value.to_le_bytes());
                sapling::NoteCommitmentUpdate::zcash_deserialize(Cursor::new(bytes)).unwrap()
            })
            .collect::<Vec<_>>();
        let mut sequential = sapling::NoteCommitmentTree::default();
        for commitment in &commitments {
            sequential.append(*commitment).unwrap();
        }
        let mut bulk = sapling::NoteCommitmentTree::default();
        append_sapling(&mut bulk, &commitments).unwrap();
        assert_eq!(bulk.to_rpc_bytes(), sequential.to_rpc_bytes());
    }
}
