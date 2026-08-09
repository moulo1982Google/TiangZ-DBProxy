use std::{env, fs, path::PathBuf};

use sha2::{Digest, Sha256};

fn main() {
    const SCHEMA: &str = "proto/dbproxy.proto";

    println!("cargo:rerun-if-changed={SCHEMA}");
    let protoc = protoc_bin_vendored::protoc_bin_path().expect("vendored protoc is unavailable");
    // Cargo build scripts run in their own process. Setting PROTOC here only selects the
    // repository-pinned compiler for prost-build and cannot race with application threads.
    unsafe {
        env::set_var("PROTOC", protoc);
    }
    prost_build::compile_protos(&[SCHEMA], &["proto"]).expect("DBProxy proto generation failed");

    let schema = fs::read(SCHEMA).expect("DBProxy proto schema is unreadable");
    let fingerprint = format!("{:x}", Sha256::digest(schema));
    let output = PathBuf::from(env::var_os("OUT_DIR").expect("OUT_DIR is missing"))
        .join("protocol_fingerprint.rs");
    fs::write(
        output,
        format!("pub const PROTOCOL_FINGERPRINT: &str = \"{fingerprint}\";\n"),
    )
    .expect("protocol fingerprint generation failed");
}
