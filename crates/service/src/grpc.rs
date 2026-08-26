//! gRPC adapter for [`crate::CompactService`].

use tonic::{Request, Response, Status};
use ztreamer_protocol::proto::{self, compact_tx_streamer_server::CompactTxStreamer};

use crate::{CompactService, service::RpcStream};

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
        Ok(Response::new(self.latest_block()?))
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

    async fn get_transaction(
        &self,
        request: Request<proto::TxFilter>,
    ) -> Result<Response<proto::RawTransaction>, Status> {
        Ok(Response::new(self.transaction(request.into_inner()).await?))
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
        Ok(Response::new(self.latest_tree_state().await?))
    }

    async fn get_subtree_roots(
        &self,
        request: Request<proto::GetSubtreeRootsArg>,
    ) -> Result<Response<Self::GetSubtreeRootsStream>, Status> {
        Ok(Response::new(
            self.subtree_roots(request.into_inner()).await?,
        ))
    }

    async fn get_lightd_info(
        &self,
        _request: Request<proto::Empty>,
    ) -> Result<Response<proto::LightdInfo>, Status> {
        Ok(Response::new(self.lightd_info()))
    }

    async fn ping(
        &self,
        _request: Request<proto::Duration>,
    ) -> Result<Response<proto::PingResponse>, Status> {
        Ok(Response::new(self.ping()))
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
