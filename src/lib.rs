pub mod codec;
pub mod index;
pub mod parser;

pub mod proto {
    tonic::include_proto!("cash.z.wallet.sdk.rpc");

    pub const FILE_DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("lightwalletd_descriptor");
}
