fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::compile_protos("proto/sah.proto")?;
    tonic_build::compile_protos("proto/synth_rpc.proto")?;
    tonic_build::compile_protos("proto/hello.proto")?;
    Ok(())
}
