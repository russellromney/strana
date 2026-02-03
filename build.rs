fn main() -> Result<(), Box<dyn std::error::Error>> {
    prost_build::Config::new()
        .out_dir("src/")
        .compile_protos(&["proto/graphd.proto"], &["proto/"])?;
    Ok(())
}
