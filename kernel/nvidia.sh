#!/usr/bin/env bash
# Builds the NVIDIA GPU driver bundle for one vmon guest kernel: the NVIDIA
# open kernel modules compiled against that kernel's build tree, the GSP
# firmware, and the userspace CUDA driver libraries + nvidia-smi from the same
# driver release. GPU sandboxes get the bundle read-only over virtio-fs (tag
# `vmon_gpu`) and vmon-agent loads it at boot. See kernel/README.md.
#
# Run through `kernel/build.sh --nvidia` (`just gpu-driver`), which builds the
# kernel first and passes its trees:
#   nvidia.sh ARCH KERNEL_SRC KERNEL_BUILD OUT CACHE JOBS [CROSS_COMPILE]
#
# Output: OUT/vmon-nvidia-ARCH/
#   manifest.json   {"driver": "<version>", "kernel": "<guest kernel release>"}
#   modules/        nvidia.ko, nvidia-uvm.ko
#   firmware/nvidia/<version>/   GSP firmware (firmware_class.path = firmware/)
#   lib/            CUDA driver libraries with their soname/dev symlinks
#   bin/            nvidia-smi, nvidia-debugdump, nvidia-cuda-mps-control/-server
set -euo pipefail

# Driver release: the open kernel modules support Turing and newer, including
# Blackwell GB20x (GeForce RTX 50xx). Pins are NVIDIA's published sha256sums
# (`<file>.sha256sum` next to each installer). x86_64 uses the no-compat32
# installer: no 32-bit libraries.
NVIDIA_VERSION=615.71.09
NVIDIA_URL_BASE=https://download.nvidia.com/XFree86
declare -A NVIDIA_RUN=(
	[x86_64]="Linux-x86_64/$NVIDIA_VERSION/NVIDIA-Linux-x86_64-$NVIDIA_VERSION-no-compat32.run"
	[aarch64]="Linux-aarch64/$NVIDIA_VERSION/NVIDIA-Linux-aarch64-$NVIDIA_VERSION.run"
)
declare -A NVIDIA_SHA256=(
	[x86_64]=e5545862c291f3991a91ee40dd9c7a71cccc69247a193d5e5b891bfd964ac104
	[aarch64]=21b7a442113b7057e69cf65a2d8f4d0d8705ec2a0d67ed906fbb1177810e8163
)
# Kernel modules for compute: the core driver and Unified Memory. No
# modeset/drm (no display in a sandbox) and no peermem (no RDMA).
MODULES=(nvidia nvidia-uvm)
# Userspace driver libraries, the "compute" + "utility" sets the NVIDIA
# Container Toolkit mounts into CUDA containers (OpenCL, graphics, video and
# the confidential-computing PKCS#11 helpers are left out).
LIBS=(
	"libcuda.so.$NVIDIA_VERSION"
	"libcudadebugger.so.$NVIDIA_VERSION"
	"libnvidia-ml.so.$NVIDIA_VERSION"
	"libnvidia-cfg.so.$NVIDIA_VERSION"
	"libnvidia-ptxjitcompiler.so.$NVIDIA_VERSION"
	"libnvidia-nvvm.so.$NVIDIA_VERSION"
	libnvidia-nvvm70.so.4
	"libnvidia-gpucomp.so.$NVIDIA_VERSION"
	"libnvidia-allocator.so.$NVIDIA_VERSION"
	"libnvidia-tileiras.so.$NVIDIA_VERSION"
	"libnvidia-sandboxutils.so.$NVIDIA_VERSION"
)
BINS=(nvidia-smi nvidia-debugdump nvidia-cuda-mps-control nvidia-cuda-mps-server)

die() {
	echo "nvidia.sh: $*" >&2
	exit 1
}

(($# == 6 || $# == 7)) || die "usage: nvidia.sh ARCH KERNEL_SRC KERNEL_BUILD OUT CACHE JOBS [CROSS_COMPILE] (run through kernel/build.sh --nvidia)"
arch="$1" ksrc="$2" kbuild="$3" out="$4" cache="$5" jobs="$6" cross="${7:-}"
case "$arch" in
aarch64) karch=arm64 ;;
x86_64) karch=x86_64 ;;
*) die "unsupported arch '$arch' (aarch64|x86_64)" ;;
esac
for tool in zstd readelf strings "${cross}strip" "${cross}ld" "${cross}objdump"; do
	command -v "$tool" >/dev/null 2>&1 || die "missing tool: $tool (Debian/Ubuntu: zstd binutils)"
done
[[ -f "$kbuild/Module.symvers" ]] || die "$kbuild has no Module.symvers (build the kernel's modules target first)"
krelease="$(cat "$kbuild/include/config/kernel.release")"

# --- installer ------------------------------------------------------------------

run_rel="${NVIDIA_RUN[$arch]}"
run_name="${run_rel##*/}"
mkdir -p "$cache/nvidia"
run="$cache/nvidia/$run_name"
if [[ ! -f "$run" ]] || ! echo "${NVIDIA_SHA256[$arch]}  $run" | sha256sum --check --status; then
	echo "==> [$arch] downloading $NVIDIA_URL_BASE/$run_rel"
	curl -fL --retry 3 -o "$run.part" "$NVIDIA_URL_BASE/$run_rel"
	echo "${NVIDIA_SHA256[$arch]}  $run.part" | sha256sum --check --status ||
		die "checksum mismatch for $run_name (got $(sha256sum "$run.part" | cut -d' ' -f1))"
	mv "$run.part" "$run"
fi
pkg="$cache/nvidia/src/${run_name%.run}"
if [[ ! -f "$pkg/.vmon-extracted" ]]; then
	echo "==> [$arch] extracting $run_name"
	rm -rf "$pkg"
	mkdir -p "${pkg%/*}"
	# makeself payload (zstd tarball); --extract-only never runs the installer.
	sh "$run" --extract-only --target "$pkg" >/dev/null
	touch "$pkg/.vmon-extracted"
fi

# --- kernel modules ------------------------------------------------------------------

mod_build="$cache/nvidia/build/$NVIDIA_VERSION-$arch"
rm -rf "$mod_build"
mkdir -p "${mod_build%/*}"
cp -a "$pkg/kernel-open" "$mod_build"
echo "==> [$arch] building ${MODULES[*]} $NVIDIA_VERSION against $krelease"
# CC defaults to the compiler named in the kernel's CONFIG_CC_VERSION_TEXT
# (the cross gcc for a cross build); LD/OBJDUMP must match it.
make -C "$mod_build" -s -j"$jobs" modules \
	SYSSRC="$ksrc" SYSOUT="$kbuild" ARCH="$karch" CROSS_COMPILE="$cross" \
	LD="${cross}ld" OBJDUMP="${cross}objdump" \
	NV_KERNEL_MODULES="${MODULES[*]}"

# --- bundle -------------------------------------------------------------------------

bundle="$out/vmon-nvidia-$arch"
stage="$bundle.part"
rm -rf "$stage"
mkdir -p "$stage/modules" "$stage/firmware/nvidia/$NVIDIA_VERSION" "$stage/lib" "$stage/bin"
for module in "${MODULES[@]}"; do
	ko="$mod_build/$module.ko"
	magic="$(strings "$ko" | sed -n 's/^vermagic=//p')"
	[[ "$magic" == "$krelease "* ]] || die "$module.ko vermagic '$magic' does not match kernel $krelease"
	"${cross}strip" --strip-debug -o "$stage/modules/$module.ko" "$ko"
	chmod 0644 "$stage/modules/$module.ko"
done
install -m 0644 "$pkg"/firmware/*.bin "$stage/firmware/nvidia/$NVIDIA_VERSION/"
for lib in "${LIBS[@]}"; do
	install -m 0755 "$pkg/$lib" "$stage/lib/$lib"
done
# Soname and dev symlinks (libcuda.so.1 -> libcuda.so.<version>, libcuda.so ->
# libcuda.so.1, ...) from the installer's .manifest: the share is read-only in
# the guest, so ldconfig cannot create them there. Lines read
# `<name> 0000 <TYPE>_SYMLINK NATIVE [<path>] <target> MODULE:<m>`.
declare -A links=()
while read -ra fields; do
	links[${fields[0]}]="${fields[${#fields[@]} - 2]}"
done < <(grep -E '^lib[^ ]+ 0000 [A-Z_]+_SYMLINK NATIVE ' "$pkg/.manifest")
for name in "${!links[@]}"; do
	# Keep links whose chain ends at a bundled library.
	final="${links[$name]}"
	while [[ -n "${links[$final]:-}" ]]; do final="${links[$final]}"; done
	if [[ -f "$stage/lib/$final" ]]; then ln -sfn "${links[$name]}" "$stage/lib/$name"; fi
done
for lib in "${LIBS[@]}"; do
	soname="$(readelf -d "$stage/lib/$lib" | sed -n 's/.*(SONAME).*\[\(.*\)\]/\1/p')"
	[[ -z "$soname" || "$soname" == "$lib" || -e "$stage/lib/$soname" ]] ||
		die "$lib: no symlink for its soname $soname in the installer manifest"
done
for bin in "${BINS[@]}"; do
	install -m 0755 "$pkg/$bin" "$stage/bin/$bin"
done
install -m 0644 "$pkg/LICENSE" "$stage/LICENSE"
printf '{"driver": "%s", "kernel": "%s"}\n' "$NVIDIA_VERSION" "$krelease" >"$stage/manifest.json"
rm -rf "$bundle"
mv "$stage" "$bundle"
echo "==> [$arch] GPU driver bundle $NVIDIA_VERSION for $krelease: $bundle"
