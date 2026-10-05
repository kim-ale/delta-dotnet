use std::env;
use std::path::PathBuf;

fn main() {
    let crate_dir = env::var("CARGO_MANIFEST_DIR").expect("Cargo sets the manifest directory");
    let output = PathBuf::from(env::var_os("OUT_DIR").expect("Cargo sets OUT_DIR"));
    let config = cbindgen::Config::from_file(PathBuf::from(&crate_dir).join("cbindgen.toml"))
        .expect("host cbindgen configuration must be valid");
    cbindgen::generate_with_config(&crate_dir, config)
        .expect("host credential declarations must generate")
        .write_to_file(output.join("delta_dotnet_kernel_credentials.h"));
    println!("cargo:rerun-if-changed=src/abi.rs");
    println!("cargo:rerun-if-changed=src/lib.rs");
    println!("cargo:rerun-if-changed=src/broker.rs");
    println!("cargo:rerun-if-changed=src/engine.rs");
    println!("cargo:rerun-if-changed=cbindgen.toml");
}
