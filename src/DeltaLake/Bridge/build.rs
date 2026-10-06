extern crate cbindgen;

use std::{env, path::Path};

fn main() {
    let crate_dir = env::var("CARGO_MANIFEST_DIR").unwrap();
    let root = Path::new(crate_dir.as_str());
    let mut config = cbindgen::Config::from_file(root.join("cbindgen.toml").as_path()).unwrap();
    config.cpp_compat = true;
    let output = env::var_os("DELTA_LAKE_BRIDGE_HEADER_OUTPUT")
        .unwrap_or_else(|| "include/delta-lake-bridge.h".into());
    println!("cargo:rerun-if-env-changed=DELTA_LAKE_BRIDGE_HEADER_OUTPUT");
    let changed = cbindgen::Builder::new()
        .with_config(config)
        .with_crate(crate_dir)
        .with_pragma_once(true)
        .with_language(cbindgen::Language::C)
        .generate()
        .expect("Unable to generate bindings")
        .write_to_file(output);

    // If this changed and an env var disallows change, error
    if let Ok(env_val) = env::var("DELTA_LAKE_BRIDGE_DISABLE_HEADER_CHANGE") {
        if changed && env_val == "true" {
            println!("cargo:warning=bridge's header file changed unexpectedly from what's on disk");
            std::process::exit(1);
        }
    }
}
