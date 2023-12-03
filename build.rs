use std::io;

#[cfg(all(feature = "source", feature = "target"))]
fn main() -> Result<(), io::Error> {
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile(
            &[
                "fs2db/proto/source/v1/source.proto",
                "fs2db/proto/target/v1/target.proto",
            ],
            &["fs2db-proto"],
        )?;
    Ok(())
}

#[cfg(all(feature = "source", not(feature = "target")))]
fn main() -> Result<(), io::Error> {
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile(&["fs2db/proto/source/v1/source.proto"], &["fs2db-proto"])?;
    Ok(())
}
