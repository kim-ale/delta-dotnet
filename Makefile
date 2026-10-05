.DEFAULT_GOAL := generate-kernel-bindings

.PHONY: generate-kernel-headers generate-kernel-credential-header generate-kernel-bindings generate-kernel-credential-bindings generate-bridge-bindings generate-bindings

generate-kernel-headers:
	env -u CARGO_TARGET_DIR cargo build --manifest-path src/DeltaLake/Kernel/NativeHost/Cargo.toml --locked --target-dir src/DeltaLake/Kernel/NativeHost/target
	cp src/DeltaLake/Kernel/delta-kernel-rs/target/ffi-headers/delta_kernel_ffi.h src/DeltaLake/Kernel/include/

generate-kernel-credential-header: generate-kernel-headers
	cbindgen --config src/DeltaLake/Kernel/NativeHost/cbindgen.toml --output src/DeltaLake/Kernel/include/delta_dotnet_kernel_credentials.h src/DeltaLake/Kernel/NativeHost

generate-kernel-bindings: generate-kernel-headers
	ClangSharpPInvokeGenerator -I "$$(llvm-config --libdir)/clang/20/include" -D DEFINE_DEFAULT_ENGINE_BASE=1 @src/DeltaLake/Kernel/GenerateInterop.rsp

generate-kernel-credential-bindings: generate-kernel-credential-header
	output=$$(mktemp -d) && \
	ClangSharpPInvokeGenerator -I "$$(llvm-config --libdir)/clang/20/include" -I src/DeltaLake/Kernel/include -D DEFINE_DEFAULT_ENGINE_BASE=1 @src/DeltaLake/Kernel/GenerateCredentialInterop.rsp --output "$$output/KernelCredentialInterop.cs" && \
	printf 'Credential comparison bindings: %s/KernelCredentialInterop.cs\n' "$$output"

generate-bridge-bindings:
	cargo build --manifest-path src/DeltaLake/Bridge/Cargo.toml --target-dir src/DeltaLake/Bridge/target
	ClangSharpPInvokeGenerator -I "$$(llvm-config --libdir)/clang/20/include" @src/DeltaLake/Bridge/GenerateInterop.rsp

generate-bindings: generate-bridge-bindings generate-kernel-bindings