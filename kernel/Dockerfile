# Toolchain image for kernel/build.sh on hosts without a Linux userland
# (`just kernel` on macOS). Ubuntu 24.04 matches the release `kernel` job, so
# images built here are bit-identical to released ones.
FROM ubuntu:24.04

RUN apt-get update \
	&& case "$(dpkg --print-architecture)" in \
		arm64) cross="gcc-x86-64-linux-gnu libc6-dev-amd64-cross" ;; \
		*) cross="gcc-aarch64-linux-gnu libc6-dev-arm64-cross" ;; \
	esac \
	&& DEBIAN_FRONTEND=noninteractive apt-get install --yes --no-install-recommends \
		bc bison build-essential ca-certificates cpio curl dwarves flex libelf-dev libssl-dev \
		perl python3 xz-utils zstd $cross \
	&& rm -rf /var/lib/apt/lists/*
