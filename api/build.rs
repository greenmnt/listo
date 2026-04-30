fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto = "../proto/listo.proto";
    println!("cargo:rerun-if-changed={proto}");
    println!("cargo:rerun-if-changed=../proto");

    tonic_build::configure()
        .build_client(false) // server only — frontend gets its client from buf
        .build_server(true)
        .compile_protos(&[proto], &["../proto"])?;
    Ok(())
}
