use std::{
    pin::Pin,
    sync::Arc,
    time::{Duration, Instant},
};

use tokio::sync::{Semaphore, mpsc, watch};
use tokio_stream::{Stream, wrappers::ReceiverStream};
use tonic::{Request, Response, Status};
use tower::ServiceExt;
use zakura_chain::{block, serialization::ZcashSerialize, subtree::NoteCommitmentSubtreeIndex};
use zakura_state::{ReadRequest, ReadResponse, ReadStateService};

use crate::{
    head::{CanonicalBlockSource, HeadSyncError},
    index::{BlockId, Index, IndexError, IndexState},
    pipeline::PipelineConfig,
    proto::{self, compact_tx_streamer_server::CompactTxStreamer},
    serve::{PoolSelection, compact_block, compact_block_nullifiers},
};

type RpcStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send>>;
const MAX_RANGE_READERS: usize = 16;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServingSnapshot {
    pub generation: u64,
    pub durable_tip: Option<BlockId>,
    pub visible_tip: Option<BlockId>,
    pub volatile_head: Arc<[crate::codec::CompactBlockRecord]>,
    pub ready: bool,
    pub tip_fresh: bool,
    pub last_source_success: Option<Instant>,
    pub source_error: Option<Arc<str>>,
}

impl From<IndexState> for ServingSnapshot {
    fn from(state: IndexState) -> Self {
        Self {
            generation: state.generation(),
            durable_tip: state.durable_tip(),
            visible_tip: state.durable_tip(),
            volatile_head: Arc::default(),
            ready: true,
            tip_fresh: false,
            last_source_success: None,
            source_error: None,
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("volatile head does not connect to the durable index")]
pub struct SnapshotError;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Readiness {
    pub historical: bool,
    pub tip: bool,
    pub recovering: bool,
    pub source_error: Option<Arc<str>>,
}

#[derive(Clone, Copy, Debug)]
pub struct HeadFollowerConfig {
    pub poll_interval: Duration,
    pub attempt_timeout: Duration,
    pub freshness_timeout: Duration,
}

impl Default for HeadFollowerConfig {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_secs(1),
            attempt_timeout: Duration::from_secs(30),
            freshness_timeout: Duration::from_secs(30),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum HeadFollowerError {
    #[error("head follower durations are invalid")]
    Config,
}

impl ServingSnapshot {
    fn with_head(
        state: IndexState,
        volatile_head: Vec<crate::codec::CompactBlockRecord>,
    ) -> Result<Self, SnapshotError> {
        let mut expected_height = state
            .durable_tip()
            .map_or(0, |tip| tip.height.saturating_add(1));
        let mut previous_hash = state.durable_tip().map(|tip| tip.hash);
        for block in &volatile_head {
            if block.height != expected_height
                || previous_hash.is_some_and(|hash| block.previous_hash != hash)
            {
                return Err(SnapshotError);
            }
            expected_height = expected_height.checked_add(1).ok_or(SnapshotError)?;
            previous_hash = Some(block.hash);
        }
        let visible_tip = volatile_head
            .last()
            .map(|block| BlockId::new(block.height, block.hash))
            .or(state.durable_tip());
        Ok(Self {
            generation: state.generation(),
            durable_tip: state.durable_tip(),
            visible_tip,
            volatile_head: volatile_head.into(),
            ready: true,
            tip_fresh: true,
            last_source_success: Some(Instant::now()),
            source_error: None,
        })
    }

    fn volatile(&self, height: u32) -> Option<&crate::codec::CompactBlockRecord> {
        self.volatile_head
            .binary_search_by_key(&height, |block| block.height)
            .ok()
            .map(|index| &self.volatile_head[index])
    }
}

/// Restricted CompactTxStreamer service over the compact index.
#[derive(Clone)]
pub struct CompactService {
    index: Arc<Index>,
    snapshot: watch::Sender<ServingSnapshot>,
    chain_name: Arc<str>,
    zakura: ReadStateService,
    range_readers: Arc<Semaphore>,
}

impl CompactService {
    pub fn new(
        index: Arc<Index>,
        state: IndexState,
        chain_name: impl Into<Arc<str>>,
        zakura: ReadStateService,
    ) -> Self {
        let (snapshot, _) = watch::channel(state.into());
        Self {
            index,
            snapshot,
            chain_name: chain_name.into(),
            zakura,
            range_readers: Arc::new(Semaphore::new(MAX_RANGE_READERS)),
        }
    }

    pub fn publish(&self, state: IndexState) {
        self.snapshot.send_replace(state.into());
    }

    pub fn publish_head(
        &self,
        state: IndexState,
        volatile_head: Vec<crate::codec::CompactBlockRecord>,
    ) -> Result<(), SnapshotError> {
        self.snapshot
            .send_replace(ServingSnapshot::with_head(state, volatile_head)?);
        Ok(())
    }

    /// Stops new chain-data requests while a deep replacement is staged.
    pub fn begin_recovery(&self) {
        let mut snapshot = self.snapshot();
        snapshot.ready = false;
        snapshot.tip_fresh = false;
        self.snapshot.send_replace(snapshot);
    }

    pub fn readiness(&self) -> Readiness {
        let snapshot = self.snapshot();
        Readiness {
            historical: snapshot.durable_tip.is_some(),
            tip: snapshot.ready && snapshot.tip_fresh,
            recovering: !snapshot.ready,
            source_error: snapshot.source_error,
        }
    }

    /// Reconciles one head view and fails closed when it detects a deep reorg.
    pub async fn sync_head(
        &self,
        source: &mut impl CanonicalBlockSource,
        config: PipelineConfig,
    ) -> Result<IndexState, HeadSyncError> {
        let snapshot = self.snapshot();
        let result =
            crate::head::sync_head_once(&self.index, source, &snapshot.volatile_head, config).await;
        if matches!(result, Err(HeadSyncError::DeepReorg { .. })) {
            self.begin_recovery();
        }
        let (state, head) = result?;
        self.publish_head(state, head)
            .expect("head reconciliation must produce a connected snapshot");
        Ok(state)
    }

    /// Stages and publishes a deep replacement while new requests remain disabled.
    pub async fn recover_deep_reorg(
        &self,
        source: &mut impl CanonicalBlockSource,
        config: PipelineConfig,
    ) -> Result<IndexState, HeadSyncError> {
        self.begin_recovery();
        let snapshot = self.snapshot();
        let (state, head) =
            crate::head::recover_deep_reorg(&self.index, source, &snapshot.volatile_head, config)
                .await?;
        self.publish_head(state, head)
            .expect("deep recovery must produce a connected snapshot");
        Ok(state)
    }

    /// Polls Zakura until `shutdown` becomes true. Source errors are retried.
    pub async fn follow_head(
        &self,
        mut source: impl CanonicalBlockSource,
        pipeline: PipelineConfig,
        config: HeadFollowerConfig,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<(), HeadFollowerError> {
        if config.poll_interval.is_zero()
            || config.attempt_timeout.is_zero()
            || config.freshness_timeout.is_zero()
            || config.poll_interval > config.freshness_timeout
            || config.attempt_timeout > config.freshness_timeout
        {
            return Err(HeadFollowerError::Config);
        }

        let mut interval = tokio::time::interval(config.poll_interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = interval.tick() => {}
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return Ok(());
                    }
                    continue;
                }
            }

            let attempt = async {
                if self.snapshot().ready {
                    match self.sync_head(&mut source, pipeline).await {
                        Err(HeadSyncError::DeepReorg { .. }) => {
                            self.recover_deep_reorg(&mut source, pipeline).await
                        }
                        result => result,
                    }
                } else {
                    self.recover_deep_reorg(&mut source, pipeline).await
                }
            };
            let result = tokio::select! {
                result = tokio::time::timeout(config.attempt_timeout, attempt) => {
                    result
                        .map_err(|_| "Zakura head attempt timed out".to_owned())
                        .and_then(|result| result.map_err(|error| error.to_string()))
                }
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return Ok(());
                    }
                    continue;
                }
            };
            if let Err(error) = result {
                self.mark_source_failure(error, config.freshness_timeout);
            }
        }
    }

    fn mark_source_failure(&self, error: String, freshness_timeout: Duration) {
        let mut snapshot = self.snapshot();
        snapshot.tip_fresh = snapshot
            .last_source_success
            .is_some_and(|last| last.elapsed() < freshness_timeout);
        snapshot.source_error = Some(error.into());
        self.snapshot.send_replace(snapshot);
    }

    fn snapshot(&self) -> ServingSnapshot {
        self.snapshot.borrow().clone()
    }

    async fn record(
        &self,
        request: proto::BlockId,
    ) -> Result<crate::codec::CompactBlockRecord, Status> {
        let snapshot = self.snapshot();
        ensure_ready(&snapshot)?;
        if request.hash.is_empty() {
            let height = u32::try_from(request.height)
                .map_err(|_| Status::invalid_argument("block height exceeds u32"))?;
            if let Some(record) = snapshot.volatile(height) {
                ensure_tip_ready(&snapshot)?;
                return Ok(record.clone());
            }
        } else {
            let hash: [u8; 32] = request
                .hash
                .as_slice()
                .try_into()
                .map_err(|_| Status::invalid_argument("block hash must be 32 bytes"))?;
            if let Some(record) = snapshot
                .volatile_head
                .iter()
                .find(|record| record.hash == hash)
            {
                ensure_tip_ready(&snapshot)?;
                return Ok(record.clone());
            }
        }
        let index = Arc::clone(&self.index);
        let generation = snapshot.generation;
        let lookup = if request.hash.is_empty() {
            let height = u32::try_from(request.height)
                .map_err(|_| Status::invalid_argument("block height exceeds u32"))?;
            tokio::task::spawn_blocking(move || index.read_block(generation, height).map(Some))
        } else {
            let hash: [u8; 32] = request
                .hash
                .try_into()
                .map_err(|_| Status::invalid_argument("block hash must be 32 bytes"))?;
            tokio::task::spawn_blocking(move || index.read_block_by_hash(generation, hash))
        };
        lookup
            .await
            .map_err(|error| Status::unavailable(format!("LMDB reader failed: {error}")))?
            .map_err(index_status)?
            .ok_or_else(|| Status::not_found("block is not in the indexed canonical chain"))
    }

    async fn block(
        &self,
        request: proto::BlockId,
        nullifiers: bool,
    ) -> Result<proto::CompactBlock, Status> {
        let record = self.record(request).await?;
        let pools = PoolSelection::from_request(&[]).expect("empty pool selection is valid");
        Ok(if nullifiers {
            compact_block_nullifiers(&record, pools)
        } else {
            compact_block(&record, pools)
        })
    }

    async fn range(
        &self,
        request: proto::BlockRange,
        nullifiers: bool,
    ) -> Result<RpcStream<proto::CompactBlock>, Status> {
        let (start, end) = range_heights(&request)?;
        let pools = PoolSelection::from_request(&request.pool_types)?;
        let snapshot = self.snapshot();
        ensure_ready(&snapshot)?;
        let visible_tip = snapshot
            .visible_tip
            .ok_or_else(|| Status::unavailable("compact index is empty"))?;
        if start > visible_tip.height || end > visible_tip.height {
            return Err(Status::out_of_range(format!(
                "range exceeds compact tip {}",
                visible_tip.height
            )));
        }
        if !snapshot.tip_fresh
            && snapshot
                .durable_tip
                .is_none_or(|durable| start > durable.height || end > durable.height)
        {
            return Err(Status::unavailable("canonical head source is stale"));
        }
        let index = Arc::clone(&self.index);
        let permit = Arc::clone(&self.range_readers)
            .acquire_owned()
            .await
            .map_err(|_| Status::unavailable("range reader pool is closed"))?;
        // Capacity one plus the codec's fixed per-record limit bounds decoded response memory.
        let (sender, receiver) = mpsc::channel(1);
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let emit = |record: &crate::codec::CompactBlockRecord| {
                let block = if nullifiers {
                    compact_block_nullifiers(record, pools)
                } else {
                    compact_block(record, pools)
                };
                sender.blocking_send(Ok(block)).is_ok()
            };
            let durable_tip = snapshot.durable_tip.map(|tip| tip.height);
            let ascending = start <= end;
            let result = (|| -> Result<(), IndexError> {
                if ascending {
                    if let Some(tip) = durable_tip
                        && start <= tip
                    {
                        index.read_range(snapshot.generation, start, end.min(tip), |record| {
                            emit(&record)
                        })?;
                    }
                    let first = durable_tip.map_or(start, |tip| start.max(tip.saturating_add(1)));
                    emit_volatile(&snapshot, first, end, true, &emit)
                } else {
                    let last_volatile =
                        durable_tip.map_or(end, |tip| end.max(tip.saturating_add(1)));
                    emit_volatile(&snapshot, start, last_volatile, false, &emit)?;
                    if let Some(tip) = durable_tip
                        && end <= tip
                    {
                        index.read_range(snapshot.generation, start.min(tip), end, |record| {
                            emit(&record)
                        })?;
                    }
                    Ok(())
                }
            })();
            if let Err(error) = result {
                let _ = sender.blocking_send(Err(index_status(error)));
            }
        });
        Ok(Box::pin(ReceiverStream::new(receiver)))
    }

    async fn tree_state(&self, request: proto::BlockId) -> Result<proto::TreeState, Status> {
        let record = self.record(request).await?;
        let source = self.zakura.clone();
        let hash = block::Hash(record.hash);
        let (sapling, orchard, ironwood) = tokio::try_join!(
            source
                .clone()
                .oneshot(ReadRequest::SaplingTree(hash.into())),
            source
                .clone()
                .oneshot(ReadRequest::OrchardTree(hash.into())),
            source.oneshot(ReadRequest::IronwoodTree(hash.into())),
        )
        .map_err(source_status)?;

        let sapling_tree = match sapling {
            ReadResponse::SaplingTree(tree) => tree
                .map(|tree| hex::encode(tree.to_rpc_bytes()))
                .unwrap_or_default(),
            _ => return Err(Status::internal("unexpected Sapling tree response")),
        };
        let orchard_tree = match orchard {
            ReadResponse::OrchardTree(tree) => tree
                .map(|tree| hex::encode(tree.to_rpc_bytes()))
                .unwrap_or_default(),
            _ => return Err(Status::internal("unexpected Orchard tree response")),
        };
        let ironwood_tree = match ironwood {
            ReadResponse::IronwoodTree(tree) => tree
                .map(|tree| hex::encode(tree.to_rpc_bytes()))
                .unwrap_or_default(),
            _ => return Err(Status::internal("unexpected Ironwood tree response")),
        };

        Ok(proto::TreeState {
            network: self.chain_name.to_string(),
            height: u64::from(record.height),
            hash: hash.to_string(),
            time: record.time,
            sapling_tree,
            orchard_tree,
            ironwood_tree,
        })
    }

    fn subtree_stream(
        &self,
        roots: Vec<(Vec<u8>, u32)>,
        generation: u64,
    ) -> RpcStream<proto::SubtreeRoot> {
        let index = Arc::clone(&self.index);
        let (sender, receiver) = mpsc::channel(1);
        tokio::task::spawn_blocking(move || {
            for (root_hash, height) in roots {
                let block = match index.read_block(generation, height) {
                    Ok(block) => block,
                    Err(error) => {
                        let _ = sender.blocking_send(Err(index_status(error)));
                        return;
                    }
                };
                if sender
                    .blocking_send(Ok(proto::SubtreeRoot {
                        root_hash,
                        completing_block_hash: block.hash.to_vec(),
                        completing_block_height: u64::from(height),
                    }))
                    .is_err()
                {
                    return;
                }
            }
        });
        Box::pin(ReceiverStream::new(receiver))
    }

    fn unsupported(method: &'static str) -> Status {
        Status::unimplemented(format!("Ztreamer does not support {method}"))
    }
}

fn emit_volatile(
    snapshot: &ServingSnapshot,
    start: u32,
    end: u32,
    ascending: bool,
    emit: &impl Fn(&crate::codec::CompactBlockRecord) -> bool,
) -> Result<(), IndexError> {
    if (ascending && start > end) || (!ascending && start < end) {
        return Ok(());
    }
    let mut height = start;
    loop {
        let record = snapshot
            .volatile(height)
            .ok_or(IndexError::Coverage { height })?;
        if !emit(record) || height == end {
            return Ok(());
        }
        height = if ascending {
            height.checked_add(1).ok_or(IndexError::Overflow)?
        } else {
            height.checked_sub(1).ok_or(IndexError::Overflow)?
        };
    }
}

#[tonic::async_trait]
impl CompactTxStreamer for CompactService {
    type GetBlockRangeStream = RpcStream<proto::CompactBlock>;
    type GetBlockRangeNullifiersStream = RpcStream<proto::CompactBlock>;
    type GetTaddressTxidsStream = RpcStream<proto::RawTransaction>;
    type GetTaddressTransactionsStream = RpcStream<proto::RawTransaction>;
    type GetMempoolTxStream = RpcStream<proto::CompactTx>;
    type GetMempoolStreamStream = RpcStream<proto::RawTransaction>;
    type GetSubtreeRootsStream = RpcStream<proto::SubtreeRoot>;
    type GetAddressUtxosStreamStream = RpcStream<proto::GetAddressUtxosReply>;

    async fn get_latest_block(
        &self,
        _request: Request<proto::ChainSpec>,
    ) -> Result<Response<proto::BlockId>, Status> {
        let tip = self.snapshot();
        ensure_tip_ready(&tip)?;
        let tip = tip
            .visible_tip
            .ok_or_else(|| Status::unavailable("compact index is empty"))?;
        Ok(Response::new(proto::BlockId {
            height: u64::from(tip.height),
            hash: tip.hash.to_vec(),
        }))
    }

    async fn get_block(
        &self,
        request: Request<proto::BlockId>,
    ) -> Result<Response<proto::CompactBlock>, Status> {
        Ok(Response::new(
            self.block(request.into_inner(), false).await?,
        ))
    }

    async fn get_block_nullifiers(
        &self,
        request: Request<proto::BlockId>,
    ) -> Result<Response<proto::CompactBlock>, Status> {
        Ok(Response::new(self.block(request.into_inner(), true).await?))
    }

    async fn get_block_range(
        &self,
        request: Request<proto::BlockRange>,
    ) -> Result<Response<Self::GetBlockRangeStream>, Status> {
        Ok(Response::new(
            self.range(request.into_inner(), false).await?,
        ))
    }

    async fn get_block_range_nullifiers(
        &self,
        request: Request<proto::BlockRange>,
    ) -> Result<Response<Self::GetBlockRangeNullifiersStream>, Status> {
        Ok(Response::new(self.range(request.into_inner(), true).await?))
    }

    async fn get_lightd_info(
        &self,
        _request: Request<proto::Empty>,
    ) -> Result<Response<proto::LightdInfo>, Status> {
        let tip = self.snapshot().visible_tip;
        Ok(Response::new(proto::LightdInfo {
            version: env!("CARGO_PKG_VERSION").to_owned(),
            vendor: "Ztreamer".to_owned(),
            taddr_support: false,
            chain_name: self.chain_name.to_string(),
            block_height: tip.map_or(0, |tip| u64::from(tip.height)),
            estimated_height: tip.map_or(0, |tip| u64::from(tip.height)),
            lightwallet_protocol_version: "0.5.0".to_owned(),
            ..Default::default()
        }))
    }

    async fn ping(
        &self,
        _request: Request<proto::Duration>,
    ) -> Result<Response<proto::PingResponse>, Status> {
        Ok(Response::new(proto::PingResponse { entry: 1, exit: 0 }))
    }

    async fn get_transaction(
        &self,
        request: Request<proto::TxFilter>,
    ) -> Result<Response<proto::RawTransaction>, Status> {
        let hash: [u8; 32] = request
            .into_inner()
            .hash
            .try_into()
            .map_err(|_| Status::invalid_argument("transaction hash must be 32 bytes"))?;
        let response = self
            .zakura
            .clone()
            .oneshot(ReadRequest::Transaction(zakura_chain::transaction::Hash(
                hash,
            )))
            .await
            .map_err(source_status)?;
        let transaction = match response {
            ReadResponse::Transaction(Some(transaction)) => transaction,
            ReadResponse::Transaction(None) => {
                return Err(Status::not_found("transaction not found"));
            }
            _ => return Err(Status::internal("unexpected transaction response")),
        };
        Ok(Response::new(proto::RawTransaction {
            data: transaction
                .tx
                .zcash_serialize_to_vec()
                .map_err(|error| Status::internal(error.to_string()))?,
            height: u64::from(transaction.height.0),
        }))
    }

    async fn get_tree_state(
        &self,
        request: Request<proto::BlockId>,
    ) -> Result<Response<proto::TreeState>, Status> {
        Ok(Response::new(self.tree_state(request.into_inner()).await?))
    }

    async fn get_latest_tree_state(
        &self,
        _request: Request<proto::Empty>,
    ) -> Result<Response<proto::TreeState>, Status> {
        let snapshot = self.snapshot();
        ensure_tip_ready(&snapshot)?;
        let tip = snapshot
            .visible_tip
            .ok_or_else(|| Status::unavailable("compact index is empty"))?;
        Ok(Response::new(
            self.tree_state(proto::BlockId {
                height: u64::from(tip.height),
                hash: tip.hash.to_vec(),
            })
            .await?,
        ))
    }

    async fn get_subtree_roots(
        &self,
        request: Request<proto::GetSubtreeRootsArg>,
    ) -> Result<Response<Self::GetSubtreeRootsStream>, Status> {
        let request = request.into_inner();
        let start_index = NoteCommitmentSubtreeIndex(
            request
                .start_index
                .try_into()
                .map_err(|_| Status::invalid_argument("subtree start index exceeds u16"))?,
        );
        let limit = match request.max_entries {
            0 => None,
            limit => Some(NoteCommitmentSubtreeIndex(limit.try_into().map_err(
                |_| Status::invalid_argument("subtree entry limit exceeds u16"),
            )?)),
        };
        let source = self.zakura.clone();
        let response = match proto::ShieldedProtocol::try_from(request.shielded_protocol) {
            Ok(proto::ShieldedProtocol::Sapling) => {
                source
                    .oneshot(ReadRequest::SaplingSubtrees { start_index, limit })
                    .await
            }
            Ok(proto::ShieldedProtocol::Orchard) => {
                source
                    .oneshot(ReadRequest::OrchardSubtrees { start_index, limit })
                    .await
            }
            Ok(proto::ShieldedProtocol::Ironwood) => {
                source
                    .oneshot(ReadRequest::IronwoodSubtrees { start_index, limit })
                    .await
            }
            Err(_) => return Err(Status::invalid_argument("invalid shielded protocol")),
        }
        .map_err(source_status)?;
        let roots = match response {
            ReadResponse::SaplingSubtrees(subtrees) => subtrees
                .into_values()
                .map(|subtree| (subtree.root.to_bytes().to_vec(), subtree.end_height.0))
                .collect(),
            ReadResponse::OrchardSubtrees(subtrees) => subtrees
                .into_values()
                .map(|subtree| (subtree.root.to_repr().to_vec(), subtree.end_height.0))
                .collect(),
            ReadResponse::IronwoodSubtrees(subtrees) => subtrees
                .into_values()
                .map(|subtree| (subtree.root.to_repr().to_vec(), subtree.end_height.0))
                .collect(),
            _ => return Err(Status::internal("unexpected subtree response")),
        };
        let generation = self.snapshot().generation;
        Ok(Response::new(self.subtree_stream(roots, generation)))
    }

    async fn send_transaction(
        &self,
        _request: Request<proto::RawTransaction>,
    ) -> Result<Response<proto::SendResponse>, Status> {
        Err(Self::unsupported("SendTransaction"))
    }

    async fn get_taddress_txids(
        &self,
        _request: Request<proto::TransparentAddressBlockFilter>,
    ) -> Result<Response<Self::GetTaddressTxidsStream>, Status> {
        Err(Self::unsupported("GetTaddressTxids"))
    }

    async fn get_taddress_transactions(
        &self,
        _request: Request<proto::TransparentAddressBlockFilter>,
    ) -> Result<Response<Self::GetTaddressTransactionsStream>, Status> {
        Err(Self::unsupported("GetTaddressTransactions"))
    }

    async fn get_taddress_balance(
        &self,
        _request: Request<proto::AddressList>,
    ) -> Result<Response<proto::Balance>, Status> {
        Err(Self::unsupported("GetTaddressBalance"))
    }

    async fn get_taddress_balance_stream(
        &self,
        _request: Request<tonic::Streaming<proto::Address>>,
    ) -> Result<Response<proto::Balance>, Status> {
        Err(Self::unsupported("GetTaddressBalanceStream"))
    }

    async fn get_mempool_tx(
        &self,
        _request: Request<proto::GetMempoolTxRequest>,
    ) -> Result<Response<Self::GetMempoolTxStream>, Status> {
        Err(Self::unsupported("GetMempoolTx"))
    }

    async fn get_mempool_stream(
        &self,
        _request: Request<proto::Empty>,
    ) -> Result<Response<Self::GetMempoolStreamStream>, Status> {
        Err(Self::unsupported("GetMempoolStream"))
    }

    async fn get_address_utxos(
        &self,
        _request: Request<proto::GetAddressUtxosArg>,
    ) -> Result<Response<proto::GetAddressUtxosReplyList>, Status> {
        Err(Self::unsupported("GetAddressUtxos"))
    }

    async fn get_address_utxos_stream(
        &self,
        _request: Request<proto::GetAddressUtxosArg>,
    ) -> Result<Response<Self::GetAddressUtxosStreamStream>, Status> {
        Err(Self::unsupported("GetAddressUtxosStream"))
    }
}

fn range_heights(request: &proto::BlockRange) -> Result<(u32, u32), Status> {
    let endpoint = |name, endpoint: &Option<proto::BlockId>| {
        let endpoint = endpoint
            .as_ref()
            .ok_or_else(|| Status::invalid_argument(format!("range.{name} is required")))?;
        if !endpoint.hash.is_empty() {
            return Err(Status::invalid_argument("range endpoints must be heights"));
        }
        u32::try_from(endpoint.height)
            .map_err(|_| Status::invalid_argument("range height exceeds u32"))
    };
    Ok((
        endpoint("start", &request.start)?,
        endpoint("end", &request.end)?,
    ))
}

fn ensure_ready(snapshot: &ServingSnapshot) -> Result<(), Status> {
    if snapshot.ready {
        Ok(())
    } else {
        Err(Status::unavailable("deep reorg recovery is active"))
    }
}

fn ensure_tip_ready(snapshot: &ServingSnapshot) -> Result<(), Status> {
    ensure_ready(snapshot)?;
    if snapshot.tip_fresh {
        Ok(())
    } else {
        Err(Status::unavailable("canonical head source is stale"))
    }
}

fn source_status(error: zakura_state::BoxError) -> Status {
    Status::unavailable(format!("Zakura read failed: {error}"))
}

fn index_status(error: IndexError) -> Status {
    match error {
        IndexError::Coverage { .. } => Status::out_of_range(error.to_string()),
        IndexError::Generation { .. } => Status::unavailable(error.to_string()),
        _ => Status::internal(error.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio_stream::StreamExt;
    use zakura_chain::{block, parameters::Network, transaction};
    use zakura_state::Config;

    use crate::{
        parser::RawIndexBlock,
        pipeline::{PipelineConfig, SourceRange, sync_historical},
    };

    struct HeadSource(Vec<RawIndexBlock>);

    #[tonic::async_trait]
    impl CanonicalBlockSource for HeadSource {
        async fn block(
            &mut self,
            height: u32,
        ) -> Result<Option<RawIndexBlock>, crate::head::HeadError> {
            Ok(self.0.get(height as usize).cloned())
        }
    }

    #[test]
    fn follower_polls_until_shutdown() {
        tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap()
            .block_on(async {
                let dir = tempfile::tempdir().unwrap();
                let index = Arc::new(
                    Index::open(dir.path(), 10 * 1024 * 1024, "Mainnet", [9; 32]).unwrap(),
                );
                let (_state_service, read_service, _tip, _change) = zakura_state::init(
                    Config::ephemeral(),
                    &Network::Mainnet,
                    block::Height::MAX,
                    0,
                )
                .await;
                let service =
                    CompactService::new(index, IndexState::default(), "main", read_service);
                let (shutdown, receiver) = watch::channel(false);
                let follower = {
                    let service = service.clone();
                    tokio::spawn(async move {
                        service
                            .follow_head(
                                HeadSource((0..=12).map(raw_block).collect()),
                                PipelineConfig::default(),
                                HeadFollowerConfig {
                                    poll_interval: Duration::from_millis(1),
                                    attempt_timeout: Duration::from_millis(100),
                                    freshness_timeout: Duration::from_millis(100),
                                },
                                receiver,
                            )
                            .await
                    })
                };

                tokio::time::timeout(Duration::from_secs(1), async {
                    while !service.readiness().tip {
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                })
                .await
                .unwrap();
                assert_eq!(service.snapshot().visible_tip.unwrap().height, 12);
                shutdown.send(true).unwrap();
                follower.await.unwrap().unwrap();
            });
    }

    #[test]
    fn streams_cross_range_descending_and_rejects_transparent_data() {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(async {
                let dir = tempfile::tempdir().unwrap();
                let index = Arc::new(
                    Index::open(dir.path(), 10 * 1024 * 1024, "Mainnet", [9; 32]).unwrap(),
                );
                let tip = (block::Height(1_005), block::Hash(hash(1_005)));
                let state = sync_historical(
                    &index,
                    |start, count, _| {
                        Ok(SourceRange {
                            blocks: (start..start + count).map(raw_block).collect(),
                            retained_body_floor: block::Height(0),
                            source_tip: Some(tip),
                        })
                    },
                    PipelineConfig {
                        fetch_workers: 2,
                        parser_workers: 2,
                        source_segment_blocks: 64,
                        max_source_bytes: 1024 * 1024,
                        max_pending_bytes: 1024 * 1024,
                        max_batch_bytes: 1024 * 1024,
                    },
                )
                .unwrap();
                let (_state_service, read_service, _tip, _change) = zakura_state::init(
                    Config::ephemeral(),
                    &Network::Mainnet,
                    block::Height::MAX,
                    0,
                )
                .await;
                let service = CompactService::new(index, state, "main", read_service);
                assert_eq!(
                    service.readiness(),
                    Readiness {
                        historical: true,
                        tip: false,
                        recovering: false,
                        source_error: None,
                    }
                );
                let range = proto::BlockRange {
                    start: Some(proto::BlockId {
                        height: 1_002,
                        hash: Vec::new(),
                    }),
                    end: Some(proto::BlockId {
                        height: 998,
                        hash: Vec::new(),
                    }),
                    pool_types: Vec::new(),
                };
                let mut stream = service
                    .get_block_range(Request::new(range.clone()))
                    .await
                    .unwrap()
                    .into_inner();
                let mut heights = Vec::new();
                while let Some(block) = stream.next().await {
                    heights.push(block.unwrap().height);
                }
                assert_eq!(heights, [1_002, 1_001, 1_000, 999, 998]);

                service
                    .publish_head(state, vec![record(1_006), record(1_007)])
                    .unwrap();
                assert!(service.readiness().tip);
                let mut stream = service
                    .get_block_range(Request::new(proto::BlockRange {
                        start: Some(proto::BlockId {
                            height: 1_004,
                            hash: Vec::new(),
                        }),
                        end: Some(proto::BlockId {
                            height: 1_007,
                            hash: Vec::new(),
                        }),
                        pool_types: Vec::new(),
                    }))
                    .await
                    .unwrap()
                    .into_inner();
                let mut heights = Vec::new();
                while let Some(block) = stream.next().await {
                    heights.push(block.unwrap().height);
                }
                assert_eq!(heights, [1_004, 1_005, 1_006, 1_007]);

                let recovery_range = proto::BlockRange {
                    start: Some(proto::BlockId {
                        height: 1_004,
                        hash: Vec::new(),
                    }),
                    end: Some(proto::BlockId {
                        height: 1_007,
                        hash: Vec::new(),
                    }),
                    pool_types: Vec::new(),
                };
                let mut pinned = service
                    .get_block_range(Request::new(recovery_range.clone()))
                    .await
                    .unwrap()
                    .into_inner();
                service.begin_recovery();
                assert_eq!(
                    service
                        .get_block_range(Request::new(recovery_range))
                        .await
                        .err()
                        .unwrap()
                        .code(),
                    tonic::Code::Unavailable
                );
                let mut heights = Vec::new();
                while let Some(block) = pinned.next().await {
                    heights.push(block.unwrap().height);
                }
                assert_eq!(heights, [1_004, 1_005, 1_006, 1_007]);
                service
                    .publish_head(state, vec![record(1_006), record(1_007)])
                    .unwrap();
                service.mark_source_failure("offline".to_owned(), Duration::ZERO);
                assert!(!service.readiness().tip);
                assert_eq!(
                    service
                        .get_latest_block(Request::new(proto::ChainSpec::default()))
                        .await
                        .unwrap_err()
                        .code(),
                    tonic::Code::Unavailable
                );
                assert!(
                    service
                        .get_block(Request::new(proto::BlockId {
                            height: 1_000,
                            hash: Vec::new(),
                        }))
                        .await
                        .is_ok()
                );

                let mut transparent = range;
                transparent.pool_types = vec![proto::PoolType::Transparent as i32];
                assert_eq!(
                    service
                        .get_block_range(Request::new(transparent))
                        .await
                        .err()
                        .unwrap()
                        .code(),
                    tonic::Code::InvalidArgument
                );
                assert_eq!(
                    service
                        .send_transaction(Request::new(proto::RawTransaction::default()))
                        .await
                        .unwrap_err()
                        .code(),
                    tonic::Code::Unimplemented
                );
            });
    }

    fn raw_block(height: u32) -> RawIndexBlock {
        let mut bytes = vec![0; 140];
        bytes[4..36].copy_from_slice(&height.checked_sub(1).map(hash).unwrap_or([0; 32]));
        bytes[100..104].copy_from_slice(&height.to_le_bytes());
        bytes.extend_from_slice(&[0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
        RawIndexBlock {
            height: block::Height(height),
            hash: block::Hash(hash(height)),
            bytes,
            txids: vec![transaction::Hash(hash(height))],
        }
    }

    fn record(height: u32) -> crate::codec::CompactBlockRecord {
        crate::codec::CompactBlockRecord {
            height,
            hash: hash(height),
            previous_hash: hash(height - 1),
            time: height,
            header: Vec::new(),
            transactions: Vec::new(),
            end_tree_sizes: crate::codec::TreeSizes::default(),
        }
    }

    fn hash(height: u32) -> [u8; 32] {
        let mut hash = [0; 32];
        hash[..4].copy_from_slice(&height.to_be_bytes());
        hash
    }
}
