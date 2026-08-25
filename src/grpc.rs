use std::{pin::Pin, sync::Arc};

use tokio::sync::{Semaphore, mpsc, watch};
use tokio_stream::{Stream, wrappers::ReceiverStream};
use tonic::{Request, Response, Status};
use tower::ServiceExt;
use zakura_chain::{block, serialization::ZcashSerialize, subtree::NoteCommitmentSubtreeIndex};
use zakura_state::{ReadRequest, ReadResponse, ReadStateService};

use crate::{
    index::{BlockId, Index, IndexError, IndexState},
    proto::{self, compact_tx_streamer_server::CompactTxStreamer},
    serve::{PoolSelection, compact_block, compact_block_nullifiers},
};

type RpcStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send>>;
const MAX_RANGE_READERS: usize = 16;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ServingSnapshot {
    pub generation: u64,
    pub visible_tip: Option<BlockId>,
}

impl From<IndexState> for ServingSnapshot {
    fn from(state: IndexState) -> Self {
        Self {
            generation: state.generation(),
            visible_tip: state.durable_tip(),
        }
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

    fn snapshot(&self) -> ServingSnapshot {
        *self.snapshot.borrow()
    }

    async fn record(
        &self,
        request: proto::BlockId,
    ) -> Result<crate::codec::CompactBlockRecord, Status> {
        let snapshot = self.snapshot();
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
        let index = Arc::clone(&self.index);
        let permit = Arc::clone(&self.range_readers)
            .acquire_owned()
            .await
            .map_err(|_| Status::unavailable("range reader pool is closed"))?;
        // Capacity one plus the codec's fixed per-record limit bounds decoded response memory.
        let (sender, receiver) = mpsc::channel(1);
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let result = index.read_range(snapshot.generation, start, end, |record| {
                let block = if nullifiers {
                    compact_block_nullifiers(&record, pools)
                } else {
                    compact_block(&record, pools)
                };
                sender.blocking_send(Ok(block)).is_ok()
            });
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
        let tip = self
            .snapshot()
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
        let tip = self
            .snapshot()
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
    use zakura_state::{CompactIndexSourceRange, Config, RawIndexBlock, RawIndexTransaction};

    use crate::pipeline::{PipelineConfig, sync_historical};

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
                        Ok(CompactIndexSourceRange {
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
        let mut header = vec![0; 140];
        header[4..36].copy_from_slice(&height.checked_sub(1).map(hash).unwrap_or([0; 32]));
        header[100..104].copy_from_slice(&height.to_le_bytes());
        RawIndexBlock {
            height: block::Height(height),
            hash: block::Hash(hash(height)),
            header,
            transactions: vec![RawIndexTransaction {
                txid: transaction::Hash(hash(height)),
                bytes: vec![1, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            }],
        }
    }

    fn hash(height: u32) -> [u8; 32] {
        let mut hash = [0; 32];
        hash[..4].copy_from_slice(&height.to_be_bytes());
        hash
    }
}
