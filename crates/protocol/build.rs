fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_prost_build::configure()
        .file_descriptor_set_path(
            std::path::PathBuf::from(std::env::var("OUT_DIR")?).join("lightwalletd_descriptor.bin"),
        )
        .compile_protos(
            &[
                "proto/lightwalletd/compact_formats.proto",
                "proto/lightwalletd/service.proto",
            ],
            &["proto/lightwalletd"],
        )?;
    Ok(())
}
