use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
        mpsc::{SyncSender, sync_channel},
    },
    thread,
    time::{Duration, Instant},
};
use tracing::info;

use zakura_chain::{
    block::{self, Height},
    serialization::{CompactSizeMessage, ZcashSerialize},
};
use zakura_state::{TransactionLocation, ZakuraDb};

use crate::{
    index::{Index, IndexError, IndexState},
    ingest::{IngestError, OrderedBuilder},
    parser::{CompactParseError, RawIndexBlock, parse_block},
};

const MIB: usize = 1024 * 1024;
pub(crate) const MAX_SOURCE_BLOCKS: u32 = 4096;
pub(crate) const MAX_SOURCE_BYTES: usize = 64 * MIB;

#[derive(Clone, Debug, Eq, PartialEq)]
struct SourceRange {
    blocks: Vec<RawIndexBlock>,
    retained_body_floor: block::Height,
    source_tip: Option<(block::Height, block::Hash)>,
}

#[derive(Default)]
struct FetchStats {
    read: Duration,
    header_read: Duration,
    transaction_read: Duration,
    txid_read: Duration,
    assemble: Duration,
    send_wait: Duration,
    ranges: u64,
    blocks: u64,
    bytes: u64,
}

impl FetchStats {
    fn add(&mut self, other: Self) {
        self.read += other.read;
        self.header_read += other.header_read;
        self.transaction_read += other.transaction_read;
        self.txid_read += other.txid_read;
        self.assemble += other.assemble;
        self.send_wait += other.send_wait;
        self.ranges += other.ranges;
        self.blocks += other.blocks;
        self.bytes += other.bytes;
    }
}

#[derive(Default)]
struct ParseStats {
    receive_wait: Duration,
    parse: Duration,
    send_wait: Duration,
    blocks: u64,
    bytes: u64,
}

#[derive(Default)]
struct WriteStats {
    write: Duration,
    batches: u64,
    blocks: u64,
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
            fetch_workers: workers.min(8),
            parser_workers: workers,
            source_segment_blocks: 256,
            max_source_bytes: 64 * MIB,
            max_pending_bytes: 256 * MIB,
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
    let pipeline_started = Instant::now();
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
    // The live-head reconciler validates durable records newer than Zakura's finalized tip.
    if durable_tip.is_some_and(|tip| tip.height > tip_height.0) {
        return Ok(initial_state);
    }
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
        let (raw_tx, raw_rx) = sync_channel::<RawIndexBlock>(0);
        let raw_rx = Arc::new(Mutex::new(raw_rx));
        let (prepared_tx, prepared_rx) = sync_channel::<crate::parser::PreparedCompactBlock>(0);
        let (batch_tx, batch_rx) = sync_channel::<crate::index::WriteBatch>(1);
        let writer = scope.spawn(move || {
            let mut result: Result<IndexState, PipelineError> = Ok(initial_state);
            let mut stats = WriteStats::default();
            while let Ok(batch) = batch_rx.recv() {
                if let Ok(state) = &mut result {
                    stats.batches += 1;
                    stats.blocks += batch.records.len() as u64;
                    let started = Instant::now();
                    match index.write(batch) {
                        Ok(next) => *state = next,
                        Err(error) => result = Err(error.into()),
                    }
                    stats.write += started.elapsed();
                }
            }
            result.map(|state| (state, stats))
        });

        let parsers: Vec<_> = (0..config.parser_workers)
            .map(|_| {
                let raw_rx = Arc::clone(&raw_rx);
                let prepared_tx = prepared_tx.clone();
                scope.spawn(move || -> Result<ParseStats, PipelineError> {
                    let mut stats = ParseStats::default();
                    loop {
                        let started = Instant::now();
                        let block = match raw_rx.lock().map_err(|_| PipelineError::Panic)?.recv() {
                            Ok(block) => block,
                            Err(_) => return Ok(stats),
                        };
                        stats.receive_wait += started.elapsed();
                        stats.blocks += 1;
                        stats.bytes += block.bytes.len() as u64;
                        let started = Instant::now();
                        let prepared = parse_block(&block)?;
                        stats.parse += started.elapsed();
                        let started = Instant::now();
                        if prepared_tx.send(prepared).is_err() {
                            return Ok(stats);
                        }
                        stats.send_wait += started.elapsed();
                    }
                })
            })
            .collect();
        drop(prepared_tx);

        let fetchers: Vec<_> = (0..config.fetch_workers)
            .map(|_| {
                let raw_tx = raw_tx.clone();
                let next = Arc::clone(&next);
                scope.spawn(move || -> Result<FetchStats, PipelineError> {
                    let mut stats = FetchStats::default();
                    loop {
                        let segment_start = next
                            .fetch_add(u64::from(config.source_segment_blocks), Ordering::Relaxed);
                        if segment_start > u64::from(target) {
                            return Ok(stats);
                        }
                        let segment_end = segment_start
                            .saturating_add(u64::from(config.source_segment_blocks - 1))
                            .min(u64::from(target))
                            as u32;
                        stats.add(stream_source(
                            db,
                            segment_start as u32,
                            segment_end,
                            (segment_end == target).then_some(tip_hash),
                            config.max_source_bytes,
                            &raw_tx,
                        )?);
                    }
                })
            })
            .collect();
        drop(raw_tx);

        let mut builder = OrderedBuilder::new(initial_state, config.max_pending_bytes)?;
        let mut coordinator_receive_wait = Duration::ZERO;
        let mut batch_send_wait = Duration::ZERO;
        loop {
            let started = Instant::now();
            let Ok(block) = prepared_rx.recv() else {
                break;
            };
            coordinator_receive_wait += started.elapsed();
            builder.push(block)?;
            if builder.ready_bytes() >= config.max_batch_bytes
                && let Some(batch) =
                    builder.build_batch(Some(target), Some(target), config.max_batch_bytes)?
            {
                let started = Instant::now();
                batch_tx.send(batch).map_err(|_| PipelineError::Worker)?;
                batch_send_wait += started.elapsed();
            }
        }
        while let Some(batch) =
            builder.build_batch(Some(target), Some(target), config.max_batch_bytes)?
        {
            let started = Instant::now();
            batch_tx.send(batch).map_err(|_| PipelineError::Worker)?;
            batch_send_wait += started.elapsed();
        }
        drop(batch_tx);

        let mut parse_stats = ParseStats::default();
        for parser in parsers {
            let stats = parser.join().map_err(|_| PipelineError::Panic)??;
            parse_stats.receive_wait += stats.receive_wait;
            parse_stats.parse += stats.parse;
            parse_stats.send_wait += stats.send_wait;
            parse_stats.blocks += stats.blocks;
            parse_stats.bytes += stats.bytes;
        }
        let mut fetch_stats = FetchStats::default();
        for fetcher in fetchers {
            let stats = fetcher.join().map_err(|_| PipelineError::Panic)??;
            fetch_stats.read += stats.read;
            fetch_stats.header_read += stats.header_read;
            fetch_stats.transaction_read += stats.transaction_read;
            fetch_stats.txid_read += stats.txid_read;
            fetch_stats.assemble += stats.assemble;
            fetch_stats.send_wait += stats.send_wait;
            fetch_stats.ranges += stats.ranges;
            fetch_stats.blocks += stats.blocks;
            fetch_stats.bytes += stats.bytes;
        }
        let (state, write_stats) = writer.join().map_err(|_| PipelineError::Panic)??;
        let compact_wall = pipeline_started.elapsed();
        info!(
            fetch_read_seconds = fetch_stats.read.as_secs_f64(),
            fetch_header_read_seconds = fetch_stats.header_read.as_secs_f64(),
            fetch_transaction_read_seconds = fetch_stats.transaction_read.as_secs_f64(),
            fetch_txid_read_seconds = fetch_stats.txid_read.as_secs_f64(),
            fetch_assemble_seconds = fetch_stats.assemble.as_secs_f64(),
            fetch_send_wait_seconds = fetch_stats.send_wait.as_secs_f64(),
            fetch_ranges = fetch_stats.ranges,
            fetch_blocks = fetch_stats.blocks,
            fetch_bytes = fetch_stats.bytes,
            parser_receive_wait_seconds = parse_stats.receive_wait.as_secs_f64(),
            parse_seconds = parse_stats.parse.as_secs_f64(),
            parser_send_wait_seconds = parse_stats.send_wait.as_secs_f64(),
            parsed_blocks = parse_stats.blocks,
            parsed_bytes = parse_stats.bytes,
            coordinator_receive_wait_seconds = coordinator_receive_wait.as_secs_f64(),
            batch_send_wait_seconds = batch_send_wait.as_secs_f64(),
            write_seconds = write_stats.write.as_secs_f64(),
            write_batches = write_stats.batches,
            written_blocks = write_stats.blocks,
            "historical pipeline stage totals"
        );
        info!(
            elapsed_seconds = compact_wall.as_secs_f64(),
            blocks = write_stats.blocks,
            "compact block index build complete"
        );
        if index.tree_index_enabled() {
            let tree = index.flush_tree_index()?;
            info!(
                elapsed_seconds = tree.wall.as_secs_f64(),
                compute_seconds = tree.compute.as_secs_f64(),
                jobs = tree.jobs,
                blocks = tree.blocks,
                "tree state index build complete"
            );
        }
        Ok(state)
    })
}

fn stream_source(
    db: &ZakuraDb,
    start: u32,
    target: u32,
    target_hash: Option<block::Hash>,
    max_block_bytes: usize,
    raw_tx: &SyncSender<RawIndexBlock>,
) -> Result<FetchStats, PipelineError> {
    let mut stats = FetchStats::default();
    let mut headers = db.block_headers_by_height_range(Height(start)..=Height(target));
    let transaction_range = TransactionLocation::min_for_height(Height(start))
        ..=TransactionLocation::max_for_height(Height(target));
    let mut transactions = db
        .raw_transactions_by_location_range(transaction_range.clone())
        .peekable();
    let mut transaction_hashes = db
        .transaction_hashes_by_location_range(transaction_range)
        .peekable();

    for raw_height in start..=target {
        let height = Height(raw_height);
        let started = Instant::now();
        let (actual_height, header) = headers.next().ok_or(PipelineError::SourceGap {
            expected: raw_height,
            actual: None,
        })?;
        stats.header_read += started.elapsed();
        if actual_height != height {
            return Err(PipelineError::SourceGap {
                expected: raw_height,
                actual: Some(actual_height.0),
            });
        }

        let mut block_transactions = Vec::new();
        let mut txids = Vec::new();
        loop {
            let started = Instant::now();
            let transaction = transactions.next_if(|(location, _)| location.height == height);
            stats.transaction_read += started.elapsed();
            let Some((transaction_location, transaction)) = transaction else {
                break;
            };
            block_transactions.push(transaction);

            let started = Instant::now();
            let Some((location, txid)) = transaction_hashes.next() else {
                return Err(PipelineError::SourceChanged);
            };
            stats.txid_read += started.elapsed();
            if location != transaction_location {
                return Err(PipelineError::SourceChanged);
            }
            txids.push(txid);
        }
        if block_transactions.is_empty() {
            return Err(PipelineError::MissingBody { height: raw_height });
        }
        if transaction_hashes
            .peek()
            .is_some_and(|(location, _)| location.height == height)
        {
            return Err(PipelineError::SourceChanged);
        }

        let started = Instant::now();
        let hash = header.hash();
        if height.0 == target && target_hash.is_some_and(|target_hash| hash != target_hash) {
            return Err(PipelineError::SourceChanged);
        }
        let tx_count = CompactSizeMessage::try_from(block_transactions.len())
            .expect("stored block transaction count is valid")
            .zcash_serialize_to_vec()
            .expect("serialization to a byte vector cannot fail");
        let size = header
            .zcash_serialized_size()
            .checked_add(tx_count.len())
            .and_then(|size| {
                block_transactions
                    .iter()
                    .try_fold(size, |size, transaction| {
                        size.checked_add(transaction.raw_bytes().len())
                    })
            })
            .ok_or(PipelineError::SourceSize { height: raw_height })?;
        let block_bytes = size
            .checked_add(
                txids
                    .len()
                    .checked_mul(32)
                    .ok_or(PipelineError::SourceSize { height: raw_height })?,
            )
            .ok_or(PipelineError::SourceSize { height: raw_height })?;
        if block_bytes > max_block_bytes {
            return Err(PipelineError::BlockExceedsByteLimit {
                height: raw_height,
                required: block_bytes,
                limit: max_block_bytes,
            });
        }
        let mut bytes = Vec::with_capacity(size);
        header
            .zcash_serialize(&mut bytes)
            .expect("serialization to a byte vector cannot fail");
        bytes.extend_from_slice(&tx_count);
        for transaction in block_transactions {
            bytes.extend_from_slice(transaction.raw_bytes());
        }
        stats.assemble += started.elapsed();
        stats.read = stats.header_read + stats.transaction_read + stats.txid_read + stats.assemble;
        stats.ranges = 1;
        stats.blocks += 1;
        stats.bytes += bytes.len() as u64;

        let started = Instant::now();
        if raw_tx
            .send(RawIndexBlock {
                height,
                hash,
                bytes,
                txids,
            })
            .is_err()
        {
            return Ok(stats);
        }
        stats.send_wait += started.elapsed();
    }

    Ok(stats)
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

    let Some(end) = count
        .checked_sub(1)
        .and_then(|offset| start.checked_add(offset))
        .zip(source_tip)
        .map(|(end, (tip, _))| end.min(tip.0))
        .filter(|end| start <= *end)
    else {
        return Ok(SourceRange {
            blocks,
            retained_body_floor,
            source_tip,
        });
    };

    let headers = (start..=end)
        .map(|raw_height| {
            let height = Height(raw_height);
            db.block_header(height.into())
                .map(|header| (height, header))
                .ok_or(PipelineError::SourceGap {
                    expected: raw_height,
                    actual: None,
                })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let mut transactions = db
        .transactions_by_location_range(
            TransactionLocation::min_for_height(Height(start))
                ..=TransactionLocation::max_for_height(Height(end)),
        )
        .peekable();

    for (height, header) in headers {
        let mut block_transactions = Vec::new();
        while transactions
            .peek()
            .is_some_and(|(location, _)| location.height == height)
        {
            let (_, transaction) = transactions
                .next()
                .expect("the transaction was just checked");
            block_transactions.push(Arc::new(transaction));
        }
        if block_transactions.is_empty() {
            return Err(PipelineError::MissingBody { height: height.0 });
        }

        let block = block::Block {
            header,
            transactions: block_transactions,
        };
        let hash = block.hash();
        let txids = block
            .transactions
            .iter()
            .map(|transaction| transaction.hash())
            .collect::<Vec<_>>();
        let bytes = block
            .zcash_serialize_to_vec()
            .expect("serialization to a byte vector cannot fail");
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
