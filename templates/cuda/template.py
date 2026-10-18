"""`cuda`: NVIDIA CUDA runtime on Ubuntu 24.04 for GPU sandboxes.

Built `fromImage` on `nvidia/cuda:13.0.3-runtime-ubuntu24.04` (CUDA 13.0
runtime libraries; the guest driver bundle ships NVIDIA 615.71.09, which runs
any CUDA 12.x/13.x runtime). The image carries no kernel driver or `libcuda`:
on GPU sandboxes the guest agent mounts the node's `vmon-nvidia-<arch>/`
bundle, loads the driver, and registers its user-space libraries (`libcuda`,
`nvidia-smi`) with `ldconfig`. Do not install NVIDIA driver packages here; a
second, different `libcuda` would shadow the bundle's.

On top of the image this template

- sets `VMON_GPUS=1`, the default GPU count of its sandboxes
  (`metadata["vmon.gpus"]` overrides it per sandbox, `"0"` opts out);
- installs `python3`, `pip`, and `venv`;
- installs `vmon-gpu-check` (`gpu-check.py`), a dependency-free self-check
  that lists the GPUs through the driver API and round-trips a buffer through
  device memory with the CUDA runtime;
- makes `user` the default user, `/home/user` the default workdir.

No start command: GPU sandboxes always cold-boot, so nothing would be captured
in a memory snapshot.
"""

from pathlib import Path

from e2b import Template

CPU_COUNT = 2
# The NVIDIA driver and CUDA context need headroom beyond a CPU sandbox.
MEMORY_MB = 4096
# Room for the CUDA runtime image (~2.5 GiB unpacked) plus workloads.
MIN_FREE_DISK_MB = 8192

IMAGE = "nvidia/cuda:13.0.3-runtime-ubuntu24.04"


def make_template():
	return (
		Template(file_context_path=Path(__file__).parent)
		.from_image(IMAGE)
		.set_envs({"VMON_GPUS": "1"})
		.apt_install(["python3", "python3-pip", "python3-venv"], no_install_recommends=True)
		.copy("gpu-check.py", "/usr/local/bin/vmon-gpu-check", user="root", mode=0o755)
		.set_user("user")
		.set_workdir("/home/user")
	)
