#!/usr/bin/env bash
# Builds the vmon guest kernels: cloud-hypervisor's `ch_defconfig` (the same
# tree as the kernel `vmon serve` downloads by default) plus the fragments in
# kernel/config/. See kernel/README.md.
#
# Usage: kernel/build.sh [--arch aarch64|x86_64]... [--out DIR] [--cache DIR] [--jobs N] [--config-only] [--nvidia]
#   --config-only  resolve and verify the .config, write config-<arch>, skip compiling
#   --nvidia       also build the NVIDIA GPU driver bundle for each kernel (kernel/nvidia.sh)
#
# Runs on Linux (aarch64 or x86_64); the other arch is cross-compiled with the
# Debian/Ubuntu `gcc-<arch>-linux-gnu` toolchain. Outputs, in --out
# (default <repo>/target/kernel):
#   Image-aarch64           arm64 boot image (pass to `vmon serve --kernel`)
#   bzImage-x86_64          x86_64 boot image (pass to `vmon serve --kernel`)
#   vmlinux-x86_64          x86_64 ELF (debug info stripped, BTF kept)
#   config-<arch>           the resolved .config
#   SHA256SUMS              checksums of everything above
#   vmon-nvidia-<arch>/     GPU driver bundle (--nvidia)
set -euo pipefail

# Kernel source: cloud-hypervisor/linux at the tag the node's default kernel
# pin (node/src/assets.rs) was released from.
KERNEL_TAG=ch-release-v6.16.9-20260508
KERNEL_URL="https://codeload.github.com/cloud-hypervisor/linux/tar.gz/refs/tags/${KERNEL_TAG}"
KERNEL_SHA256=a2aaf2b459842810f3521cdfe8f5fa97bd816768019e184b6dafa0c7254d6f21
# Commit date of the tag; fixed so rebuilds are bit-identical.
BUILD_TIMESTAMP="2026-05-08 13:23:23 UTC"

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(dirname "$here")"
out="$repo/target/kernel"
cache="${XDG_CACHE_HOME:-$HOME/.cache}/vmon-kernel"
jobs="$(nproc)"
arches=()
config_only=0
nvidia=0

usage() {
	sed -n '5,8p' "${BASH_SOURCE[0]}" | sed 's/^# //'
	exit "$1"
}

while (($#)); do
	case "$1" in
	--arch) arches+=("$2"); shift 2 ;;
	--out) out="$2"; shift 2 ;;
	--cache) cache="$2"; shift 2 ;;
	--jobs) jobs="$2"; shift 2 ;;
	--config-only) config_only=1; shift ;;
	--nvidia) nvidia=1; shift ;;
	-h | --help) usage 0 ;;
	*) echo "build.sh: unknown argument: $1" >&2; usage 2 ;;
	esac
done
((${#arches[@]})) || arches=(aarch64 x86_64)

die() {
	echo "build.sh: $*" >&2
	exit 1
}

[[ "$(uname -s)" == Linux ]] || die "kernel builds run on Linux (on macOS: just kernel builds in a Docker container)"
host_arch="$(uname -m)"

# python3: the BTF tooling (resolve_btfids → libbpf) generates its headers.
need=(make gcc flex bison bc perl python3 pahole cpio xz sha256sum curl)
((nvidia == 0)) || need+=(zstd readelf strings)
for arch in "${arches[@]}"; do
	case "$arch" in
	aarch64 | x86_64) ;;
	*) die "unsupported arch '$arch' (aarch64|x86_64)" ;;
	esac
	[[ "$arch" == "$host_arch" ]] || need+=("$arch-linux-gnu-gcc")
done
missing=()
for tool in "${need[@]}"; do
	command -v "$tool" >/dev/null 2>&1 || missing+=("$tool")
done
if ((${#missing[@]})); then
	die "missing tools: ${missing[*]}
  Debian/Ubuntu: sudo apt-get install build-essential flex bison bc libelf-dev libssl-dev dwarves python3 cpio xz-utils curl gcc-aarch64-linux-gnu gcc-x86-64-linux-gnu (+ zstd for --nvidia)"
fi

# --- source -----------------------------------------------------------------

mkdir -p "$cache" "$out"
tarball="$cache/linux-$KERNEL_TAG.tar.gz"
if [[ ! -f "$tarball" ]] || ! echo "$KERNEL_SHA256  $tarball" | sha256sum --check --status; then
	echo "==> downloading $KERNEL_URL"
	curl -fL --retry 3 -o "$tarball.part" "$KERNEL_URL"
	echo "$KERNEL_SHA256  $tarball.part" | sha256sum --check --status ||
		die "checksum mismatch for $KERNEL_URL (got $(sha256sum "$tarball.part" | cut -d' ' -f1))"
	mv "$tarball.part" "$tarball"
fi
src="$cache/src/linux-$KERNEL_TAG"
if [[ ! -f "$src/.vmon-extracted" ]]; then
	echo "==> extracting $tarball"
	rm -rf "$src"
	mkdir -p "$src"
	tar -xzf "$tarball" -C "$src" --strip-components=1
	touch "$src/.vmon-extracted"
fi

# Reproducible build metadata (uname -v, /proc/version).
export KBUILD_BUILD_TIMESTAMP="$BUILD_TIMESTAMP"
export KBUILD_BUILD_USER=vmon
export KBUILD_BUILD_HOST=vmon
export KBUILD_BUILD_VERSION=1

# Fails unless every option in the fragments resolved to exactly the
# requested value (catches unmet dependencies and renamed symbols).
verify_config() {
	local config="$1"
	shift
	local fragment line sym want got bad=0
	for fragment in "$@"; do
		while IFS= read -r line; do
			if [[ "$line" =~ ^(CONFIG_[A-Za-z0-9_]+)=(.*)$ ]]; then
				sym="${BASH_REMATCH[1]}"
				want="${BASH_REMATCH[2]}"
			elif [[ "$line" =~ ^#\ (CONFIG_[A-Za-z0-9_]+)\ is\ not\ set$ ]]; then
				sym="${BASH_REMATCH[1]}"
				want=n
			else
				continue
			fi
			got="$(grep -E "^$sym=" "$config" | cut -d= -f2- || true)"
			[[ -n "$got" ]] || got=n
			if [[ "$got" != "$want" ]]; then
				echo "  $sym: want $want, got $got (${fragment##*/})" >&2
				bad=1
			fi
		done <"$fragment"
	done
	((bad == 0)) || die "config fragments did not apply cleanly (see above)"
}

build_arch() {
	local arch="$1" karch target cross="" build
	case "$arch" in
	aarch64) karch=arm64 target=Image ;;
	x86_64) karch=x86 target=bzImage ;;
	esac
	[[ "$arch" == "$host_arch" ]] || cross="$arch-linux-gnu-"
	build="$cache/build/$KERNEL_TAG-$arch"
	mkdir -p "$build"
	local make_args=(-C "$src" O="$build" ARCH="$karch" CROSS_COMPILE="$cross" -j"$jobs")
	local fragments=("$here/config/common.config" "$here/config/$arch.config")

	echo "==> [$arch] configuring (ch_defconfig + ${fragments[*]##*/})"
	make "${make_args[@]}" -s ch_defconfig
	"$src/scripts/kconfig/merge_config.sh" -m -O "$build" "$build/.config" "${fragments[@]}" >/dev/null
	make "${make_args[@]}" -s olddefconfig
	# Module support exists only for the GPU driver bundle's out-of-tree
	# modules: build in every option olddefconfig resolved to `m` (defaults
	# that were `y` while MODULES=n), so the kernel itself ships no modules.
	sed -i 's/=m$/=y/' "$build/.config"
	make "${make_args[@]}" -s olddefconfig
	if grep -q '=m$' "$build/.config"; then
		die "[$arch] options left as modules: $(grep '=m$' "$build/.config" | tr '\n' ' ')"
	fi
	verify_config "$build/.config" "${fragments[@]}"
	if ((config_only)); then
		install -m 0644 "$build/.config" "$out/config-$arch"
		echo "==> [$arch] config verified: $out/config-$arch"
		return
	fi

	echo "==> [$arch] building $target with $jobs jobs"
	# JOBS=1: scripts/Makefile.btf runs pahole with -j$(JOBS); multi-threaded
	# BTF encoding orders types nondeterministically, which shifts every
	# section after .BTF and breaks bit-identical rebuilds.
	make "${make_args[@]}" -s JOBS=1 "$target"

	case "$arch" in
	aarch64)
		install -m 0644 "$build/arch/arm64/boot/Image" "$out/Image-aarch64"
		;;
	x86_64)
		install -m 0644 "$build/arch/x86/boot/bzImage" "$out/bzImage-x86_64"
		"${cross}strip" --strip-debug -o "$out/vmlinux-x86_64" "$build/vmlinux"
		;;
	esac
	install -m 0644 "$build/.config" "$out/config-$arch"
	echo "==> [$arch] $(cat "$build/include/config/kernel.release")"

	if ((nvidia)); then
		# The kernel builds no modules; this only runs modpost over vmlinux to
		# write the Module.symvers that out-of-tree modules link against.
		make "${make_args[@]}" -s modules
		"$here/nvidia.sh" "$arch" "$src" "$build" "$out" "$cache" "$jobs" "$cross"
	fi
}

for arch in "${arches[@]}"; do
	build_arch "$arch"
done

(
	cd "$out"
	files=()
	for f in Image-aarch64 bzImage-x86_64 vmlinux-x86_64 config-aarch64 config-x86_64; do
		if [[ -f "$f" ]]; then files+=("$f"); fi
	done
	((config_only)) || sha256sum "${files[@]}" >SHA256SUMS
	((config_only)) || cat SHA256SUMS
)
echo "==> kernels in $out"
