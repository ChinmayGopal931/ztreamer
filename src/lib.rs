pub mod codec;
pub mod grpc;
pub mod head;
pub mod index;
pub mod ingest;
pub mod parser;
pub mod pipeline;
mod serve;
pub mod source;

pub mod proto {
    tonic::include_proto!("cash.z.wallet.sdk.rpc");

    pub const FILE_DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("lightwalletd_descriptor");
}

pub mod zakura_proto {
    tonic::include_proto!("zebra.indexer.rpc");
}
