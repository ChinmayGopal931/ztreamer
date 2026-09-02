//! Lightwallet protobuf types and Ztreamer's Zakura p2p protocol definitions.

pub mod p2p;

/// Generated Lightwallet protocol types and service descriptors.
pub mod proto {
    tonic::include_proto!("cash.z.wallet.sdk.rpc");

    pub const FILE_DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("lightwalletd_descriptor");
}
