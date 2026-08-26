use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
        mpsc::sync_channel,
    },
    thread,
};

use zakura_chain::block::{self, Height};
use zakura_state::ZakuraDb;

use crate::{
    index::{Index, IndexError, IndexState},
    ingest::{IngestError, OrderedBuilder},
    parser::{CompactParseError, RawIndexBlock, parse_block},
};

const MIB: usize = 1024 * 1024;
pub(crate) const MAX_SOURCE_BLOCKS: u32 = 256;
pub(crate) const MAX_SOURCE_BYTES: usize = 64 * MIB;

#[derive(Clone, Debug, Eq, PartialEq)]
struct SourceRange {
    blocks: Vec<RawIndexBlock>,
    retained_body_floor: block::Height,
    source_tip: Option<(block::Height, block::Hash)>,
}

#[derive(Clone, Copy, Debug)]
pub struct PipelineConfig {
    pub fetch_workers: usize,
    pub parser_workers: usize,
    pub source_segment_blocks: u32,
    pub max_source_bytes: usize,
    pub max_pending_bytes: usize,
    pub max_batch_bytes: usize,
}

impl Default for PipelineConfig {
    fn default() -> Self {
        let workers = thread::available_parallelism().map_or(1, usize::from);
        Self {
            fetch_workers: workers.min(4),
            parser_workers: workers,
            source_segment_blocks: 64,
            max_source_bytes: 64 * MIB,
            max_pending_bytes: 64 * MIB,
            max_batch_bytes: 16 * MIB,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum PipelineError {
    #[error(transparent)]
    Parse(#[from] CompactParseError),
    #[error(transparent)]
    Ingest(#[from] IngestError),
    #[error(transparent)]
    Index(#[from] IndexError),
    #[error("pipeline configuration has a zero or out-of-range limit")]
    Config,
    #[error("Zakura has no finalized source tip")]
    NoSourceTip,
    #[error("Zakura has pruned the required block body at height {height}")]
    MissingBody { height: u32 },
    #[error("Zakura block at height {height} needs {required} source bytes, limit is {limit}")]
    BlockExceedsByteLimit {
        height: u32,
        required: usize,
        limit: usize,
    },
    #[error("Zakura source size overflowed at height {height}")]
    SourceSize { height: u32 },
    #[error("Zakura source changed during historical ingestion")]
    SourceChanged,
    #[error("LMDB durable tip at height {height} does not match Zakura")]
    DurableTipMismatch { height: u32 },
    #[error("Zakura returned a gap or wrong height: expected {expected}, got {actual:?}")]
    SourceGap { expected: u32, actual: Option<u32> },
    #[error("pipeline channel closed after a worker failed")]
    Worker,
    #[error("pipeline worker panicked")]
    Panic,
}

/// Ingests the finalized durable prefix using bounded fetch, parse, and write stages.
///
/// The captured finalized prefix is immutable; the live source tip may grow during this call.
pub(crate) fn sync_historical(
    index: &Index,
    db: &ZakuraDb,
    config: PipelineConfig,
) -> Result<IndexState, PipelineError> {
    validate_config(config)?;
    let initial_state = index.state()?;
    let durable_tip = initial_state.durable_tip();
    let start = durable_tip.map_or(Ok(0), |tip| {
        tip.height.checked_add(1).ok_or(IngestError::Overflow)
    })?;
    let request_bytes = (config.max_source_bytes / config.fetch_workers).clamp(1, MAX_SOURCE_BYTES);
    let probe = read_range(
        db,
        durable_tip.map_or(start, |tip| tip.height),
        u32::from(durable_tip.is_some()),
        request_bytes,
    )?;
    let (tip_height, tip_hash) = probe.source_tip.ok_or(PipelineError::NoSourceTip)?;
    if let Some(durable_tip) = durable_tip
        && probe.blocks.first().is_none_or(|block| {
            block.height.0 != durable_tip.height || block.hash.0 != durable_tip.hash
        })
    {
        return Err(PipelineError::DurableTipMismatch {
            height: durable_tip.height,
        });
    }
    if start < probe.retained_body_floor.0 {
        return Err(PipelineError::MissingBody { height: start });
    }
    let target = tip_height.0;
    if start > target {
        return Ok(initial_state);
    }

    thread::scope(|scope| {
        let next = Arc::new(AtomicU64::new(u64::from(start)));
        let (raw_tx, raw_rx) = sync_channel(0);
        let raw_rx = Arc::new(Mutex::new(raw_rx));
        let (prepared_tx, prepared_rx) = sync_channel(0);
        let (batch_tx, batch_rx) = sync_channel(1);
        let writer = scope.spawn(move || {
            let mut result = Ok(initial_state);
            while let Ok(batch) = batch_rx.recv() {
                if let Ok(state) = &mut result {
                    match index.write(batch) {
                        Ok(next) => *state = next,
                        Err(error) => result = Err(error.into()),
                    }
                }
            }
            result
        });

        let parsers: Vec<_> = (0..config.parser_workers)
            .map(|_| {
                let raw_rx = Arc::clone(&raw_rx);
                let prepared_tx = prepared_tx.clone();
                scope.spawn(move || -> Result<(), PipelineError> {
                    loop {
                        let block = match raw_rx.lock().map_err(|_| PipelineError::Panic)?.recv() {
                            Ok(block) => block,
                            Err(_) => return Ok(()),
                        };
                        let prepared = parse_block(&block)?;
                        if prepared_tx.send(prepared).is_err() {
                            return Ok(());
                        }
                    }
                })
            })
            .collect();
        drop(prepared_tx);

        let fetchers: Vec<_> = (0..config.fetch_workers)
            .map(|_| {
                let raw_tx = raw_tx.clone();
                let next = Arc::clone(&next);
                scope.spawn(move || -> Result<(), PipelineError> {
                    loop {
                        let segment_start = next
                            .fetch_add(u64::from(config.source_segment_blocks), Ordering::Relaxed);
                        if segment_start > u64::from(target) {
                            return Ok(());
                        }
                        let segment_end = segment_start
                            .saturating_add(u64::from(config.source_segment_blocks - 1))
                            .min(u64::from(target));
                        let mut cursor = segment_start as u32;
                        while u64::from(cursor) <= segment_end {
                            let count = (segment_end - u64::from(cursor) + 1) as u32;
                            let range = read_range(db, cursor, count, request_bytes)?;
                            if range.source_tip.is_none_or(|(height, hash)| {
                                height < tip_height || (height == tip_height && hash != tip_hash)
                            }) {
                                return Err(PipelineError::SourceChanged);
                            }
                            if cursor < range.retained_body_floor.0 {
                                return Err(PipelineError::MissingBody { height: cursor });
                            }
                            if range.blocks.is_empty() {
                                return Err(PipelineError::MissingBody { height: cursor });
                            }
                            for block in range.blocks {
                                if u64::from(cursor) > segment_end || block.height.0 != cursor {
                                    return Err(PipelineError::SourceGap {
                                        expected: cursor,
                                        actual: Some(block.height.0),
                                    });
                                }
                                if block.height == tip_height && block.hash != tip_hash {
                                    return Err(PipelineError::SourceChanged);
                                }
                                cursor = cursor.checked_add(1).ok_or(IngestError::Overflow)?;
                                if raw_tx.send(block).is_err() {
                                    return Ok(());
                                }
                            }
                        }
                    }
                })
            })
            .collect();
        drop(raw_tx);

        let mut builder = OrderedBuilder::new(initial_state, config.max_pending_bytes)?;
        while let Ok(block) = prepared_rx.recv() {
            builder.push(block)?;
            if builder.pending_bytes() >= config.max_batch_bytes
                && let Some(batch) =
                    builder.build_batch(Some(target), Some(target), config.max_batch_bytes)?
            {
                batch_tx.send(batch).map_err(|_| PipelineError::Worker)?;
            }
        }
        while let Some(batch) =
            builder.build_batch(Some(target), Some(target), config.max_batch_bytes)?
        {
            batch_tx.send(batch).map_err(|_| PipelineError::Worker)?;
        }
        drop(batch_tx);

        for parser in parsers {
            parser.join().map_err(|_| PipelineError::Panic)??;
        }
        for fetcher in fetchers {
            fetcher.join().map_err(|_| PipelineError::Panic)??;
        }
        writer.join().map_err(|_| PipelineError::Panic)?
    })
}

fn read_range(
    db: &ZakuraDb,
    start: u32,
    count: u32,
    max_bytes: usize,
) -> Result<SourceRange, PipelineError> {
    let source_tip = db.tip();
    let retained_body_floor = db.prune_height().unwrap_or(Height::MIN);
    let mut blocks = Vec::new();
    let mut response_bytes = 0usize;

    for offset in 0..count {
        let Some(height) = start.checked_add(offset).map(Height) else {
            break;
        };
        if source_tip.is_none_or(|(tip, _)| height > tip) {
            break;
        }
        let hash = db.hash(height).ok_or(PipelineError::SourceGap {
            expected: height.0,
            actual: None,
        })?;
        let bytes = db
            .raw_block_bytes(height.into())
            .ok_or(PipelineError::MissingBody { height: height.0 })?;
        let txids = db
            .transaction_hashes_for_block(height.into())
            .ok_or(PipelineError::MissingBody { height: height.0 })?
            .to_vec();
        let block_bytes = bytes
            .len()
            .checked_add(
                txids
                    .len()
                    .checked_mul(32)
                    .ok_or(PipelineError::SourceSize { height: height.0 })?,
            )
            .ok_or(PipelineError::SourceSize { height: height.0 })?;
        let next_response_bytes = response_bytes
            .checked_add(block_bytes)
            .ok_or(PipelineError::SourceSize { height: height.0 })?;
        if next_response_bytes > max_bytes {
            if blocks.is_empty() {
                return Err(PipelineError::BlockExceedsByteLimit {
                    height: height.0,
                    required: block_bytes,
                    limit: max_bytes,
                });
            }
            break;
        }
        response_bytes = next_response_bytes;
        blocks.push(RawIndexBlock {
            height,
            hash,
            bytes,
            txids,
        });
    }

    Ok(SourceRange {
        blocks,
        retained_body_floor,
        source_tip,
    })
}

fn validate_config(config: PipelineConfig) -> Result<(), PipelineError> {
    if config.fetch_workers == 0
        || config.parser_workers == 0
        || !(1..=MAX_SOURCE_BLOCKS).contains(&config.source_segment_blocks)
        || config.max_source_bytes < config.fetch_workers
        || config.max_pending_bytes == 0
        || config.max_batch_bytes == 0
        || config.max_batch_bytes > config.max_pending_bytes
    {
        return Err(PipelineError::Config);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc::{TrySendError, sync_channel};

    use super::*;
    use crate::parser::PreparedCompactBlock;
    use crate::{Digest, codec::encoded_record_len};

    #[test]
    fn builder_stays_one_batch_ahead_of_a_blocked_writer() {
        let mut builder = OrderedBuilder::new(IndexState::default(), MIB).unwrap();
        for height in 0..3 {
            builder.push(prepared(height)).unwrap();
        }
        let batch_bytes = encoded_record_len(&[], &[]).unwrap();
        let first = builder
            .build_batch(Some(20), Some(20), batch_bytes)
            .unwrap()
            .unwrap();
        let (batch_tx, batch_rx) = sync_channel::<crate::index::WriteBatch>(1);
        let (started_tx, started_rx) = sync_channel(0);
        let (release_tx, release_rx) = sync_channel(0);

        thread::scope(|scope| {
            let writer = scope.spawn(move || {
                let batch = batch_rx.recv().unwrap();
                assert_eq!(batch.records[0].height, 0);
                started_tx.send(()).unwrap();
                release_rx.recv().unwrap();
            });

            batch_tx.send(first).unwrap();
            started_rx.recv().unwrap();
            let second = builder
                .build_batch(Some(20), Some(20), batch_bytes)
                .unwrap()
                .unwrap();
            assert_eq!(second.records[0].height, 1);
            batch_tx.send(second).unwrap();
            let third = builder
                .build_batch(Some(20), Some(20), batch_bytes)
                .unwrap()
                .unwrap();
            assert!(matches!(
                batch_tx.try_send(third),
                Err(TrySendError::Full(_))
            ));

            release_tx.send(()).unwrap();
            writer.join().unwrap();
        });
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

    fn hash(height: u32) -> Digest {
        let mut hash = [0; 32];
        hash[..4].copy_from_slice(&height.to_be_bytes());
        hash
    }
}
