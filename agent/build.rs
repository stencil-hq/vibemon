//! Generates Rust types (prost) and canonical proto3 JSON impls (pbjson) for
//! the vendored E2B envd protos, consumed by `src/envd/pb.rs`. Uses protox, so
//! no `protoc` is needed. A no-op unless the target OS is Linux (the envd
//! server only exists in the guest build).

use std::env;

const PROTO_ROOT: &str = "../spec/e2b/infra/packages/envd/spec";
const PROTOS: [&str; 2] = ["process/process.proto", "filesystem/filesystem.proto"];

fn main() -> Result<(), Box<dyn std::error::Error>> {
	println!("cargo:rerun-if-changed=build.rs");
	if env::var("CARGO_CFG_TARGET_OS").as_deref() != Ok("linux") {
		return Ok(());
	}
	for proto in PROTOS {
		println!("cargo:rerun-if-changed={PROTO_ROOT}/{proto}");
	}
	let mut compiler = protox::Compiler::new([PROTO_ROOT])?;
	compiler.include_imports(true);
	compiler.open_files(PROTOS)?;
	let descriptors = compiler.encode_file_descriptor_set();

	let mut config = prost_build::Config::new();
	// Disable prost's built-in `.google.protobuf` → `::prost_types` mapping so
	// well-known types come from pbjson-types (which carry serde impls).
	config.compile_well_known_types();
	config.extern_path(".google.protobuf", "::pbjson_types");
	config.compile_fds(compiler.file_descriptor_set())?;

	pbjson_build::Builder::new()
		.register_descriptors(&descriptors)?
		.ignore_unknown_fields()
		.build(&[".process", ".filesystem"])?;
	Ok(())
}
