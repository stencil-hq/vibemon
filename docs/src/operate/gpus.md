# GPUs

Sandboxes can get whole NVIDIA GPUs (or NVIDIA vGPU virtual functions) passed through with VFIO, so CUDA programs run in them unchanged. This page covers preparing a host, installing the guest driver bundle, requesting GPUs from the E2B SDKs, what pause and snapshots do to a GPU sandbox, and where the dashboard shows GPUs.

GPU passthrough needs a Linux x86_64 node with KVM. macOS (HVF) and Windows nodes never publish GPUs.

## How it fits together

- Every node lists the NVIDIA display-class PCI functions of its host in its gossip capacity: whole GPUs, and SR-IOV virtual functions that have a vGPU profile (a physical function with VFs enabled is not listed). Binding a GPU to `vfio-pci` is the operator's opt-in. A GPU that is not ready for passthrough is still listed, with a `problem` that says why, and sandboxes never get it.
- A sandbox asks for a GPU count (see [Requesting GPUs](#requesting-gpus)). The scheduler only places it on a node with that many free, assignable GPUs. The node reserves them atomically and starts the VM with them.
- In the guest, the agent mounts the node's [GPU driver bundle](kernel.md#nvidia-gpu-driver-bundle) and loads the NVIDIA driver before envd starts. `nvidia-smi` and the CUDA driver library then work as in a container started by the NVIDIA Container Toolkit. The template supplies the CUDA runtime.
- GPU sandboxes always cold-boot. They cannot snapshot guest memory, so pauses and snapshots keep only the filesystem (see [Pause, snapshots, and resume](#pause-snapshots-and-resume)).

## Preparing a host

The example host has a GeForce RTX 5070 at `0000:52:00.0` (`10de:2f04`) and the GPU's HDMI audio function at `0000:52:00.1` (`10de:2f80`), on an Intel CPU running Fedora. Change the IDs, addresses, and distribution commands for your hardware.

1. Enable VT-d (Intel) or AMD-Vi (AMD) in the firmware setup.

2. Find the vendor:device IDs of the GPU and of every other function in its slot:

   ```sh
   lspci -nn -d 10de:
   # 52:00.0 VGA compatible controller [0300]: NVIDIA Corporation GB205 [GeForce RTX 5070] [10de:2f04]
   # 52:00.1 Audio device [0403]: NVIDIA Corporation Device [10de:2f80]
   ```

3. Turn on the IOMMU and have `vfio-pci` claim those IDs at boot. On Intel CPUs:

   ```sh
   sudo grubby --update-kernel=ALL \
     --args="intel_iommu=on iommu=pt vfio-pci.ids=10de:2f04,10de:2f80 vfio-pci.disable_idle_d3=1"
   ```

   `vfio-pci.disable_idle_d3=1` keeps the GPU powered (D0) between VMs. Without it, `vfio-pci` puts the idle GPU into D3hot after a sandbox ends, and on an RTX 5070 the next VM's open then failed with a config-read timeout that the platform escalated to a host reset. Nodes report a whole GPU as unassignable until the parameter is set.

   AMD CPUs enable the IOMMU by default; add `iommu=pt vfio-pci.ids=… vfio-pci.disable_idle_d3=1`. On Debian and Ubuntu, put the same arguments in `GRUB_CMDLINE_LINUX` in `/etc/default/grub` and run `sudo update-grub`.

4. Keep the host's NVIDIA and nouveau drivers off the GPU, and make the sound driver wait for `vfio-pci` so it cannot take the audio function first. `/etc/modprobe.d/vmon-gpu.conf`:

   ```text
   blacklist nouveau
   blacklist nvidia
   blacklist nvidia_drm
   blacklist nvidia_modeset
   blacklist nvidia_uvm
   softdep snd_hda_intel pre: vfio-pci
   ```

5. Load the VFIO modules at boot. `/etc/modules-load.d/vmon-gpu.conf`:

   ```text
   vfio-pci
   iommufd
   ```

   Also stop host services that keep loading the NVIDIA driver, if installed: `sudo systemctl disable --now nvidia-persistenced nvidia-powerd`.

6. Regenerate the initramfs so it includes the new module options (`sudo dracut --force` on Fedora, `sudo update-initramfs -u` on Debian and Ubuntu), then reboot.

7. Check the result:

   ```sh
   lspci -nnk -s 52:00          # both functions: "Kernel driver in use: vfio-pci"
   ls /sys/bus/pci/devices/0000:52:00.0/iommu_group/devices
   # 0000:52:00.0  0000:52:00.1   (every function of the group must be on vfio-pci)
   ls /dev/iommu /dev/vfio/devices/
   ```

Every function in the GPU's IOMMU group must be bound to `vfio-pci`. If the group also holds unrelated devices (common when the slot sits behind a chipset bridge without ACS), move the card to another slot.

Do not reset a GPU from the host with secondary bus resets (`reset_subordinate`, `setpci` bridge control, or PCI remove and rescan). On GeForce RTX 50xx this can drop the GPU off the bus until the host is power-cycled.

## Installing the guest driver bundle

The guest needs an NVIDIA driver built for the exact guest kernel it boots. Build the bundle together with the kernel:

```sh
just gpu-driver --arch x86_64    # = kernel/build.sh --nvidia --arch x86_64
```

This writes `target/kernel/vmon-nvidia-x86_64/`, containing NVIDIA driver 615.71.09 built for the current guest kernel. See [NVIDIA GPU driver bundle](kernel.md#nvidia-gpu-driver-bundle) for its contents. Install it next to the `vmon` binary, beside `vmon-kernel-x86_64`, as `vmon-nvidia-x86_64/`, or point the node at it with `vmon serve --gpu-driver <DIR>` (`VMON_GPU_DRIVER`). In a source checkout, the node also finds `target/kernel/vmon-nvidia-<arch>/`. Rebuild and reinstall the bundle whenever you rebuild the kernel. A bundle built for another kernel makes every GPU on the node report a problem.

## GPU problems

A node publishes each GPU it finds with an empty `problem` when the GPU is assignable. Otherwise the problem says what to fix:

- the GPU is not bound to `vfio-pci` (for example, it is bound to `nvidia`);
- the IOMMU is off, or the GPU has no IOMMU group;
- another member of its IOMMU group is not on `vfio-pci`;
- the VFIO device node (`/dev/vfio/devices/vfioN`) or `/dev/iommu` is missing;
- no guest driver bundle was found, or the bundle was built for another kernel;
- the host does not support passthrough (not Linux/KVM/x86_64).

**Unclean VM exits.** On current NVIDIA GPUs a VFIO function-level reset does not reset the GPU's firmware. A GPU whose VM exited while the driver was still loaded therefore cannot be used by the next VM. Before the node kills a GPU sandbox or pauses it, it asks the guest agent to stop GPU users and unload the driver, with a 30 s budget. If that fails, or the VM exits on its own (guest poweroff, VMM crash, a failed boot), the node quarantines the GPU with the problem `needs host reboot: VM exited without releasing the GPU`. Quarantine persists across node restarts and clears automatically after a host reboot. A reboot is the only fix; do not try a bus reset.

## Requesting GPUs

A sandbox gets this many GPUs, decided at create:

1. `metadata["vmon.gpus"]`, if set. This works with stock E2B SDKs:

   ```python
   from e2b import Sandbox

   sbx = Sandbox.create("cuda", metadata={"vmon.gpus": "2"})
   ```

   ```js
   const sbx = await Sandbox.create('cuda', { metadata: { 'vmon.gpus': '2' } })
   ```

2. Otherwise, the template's environment variable `VMON_GPUS`, set when the template is built:

   ```python
   template = Template().from_image("nvidia/cuda:13.0.3-runtime-ubuntu24.04").set_envs({"VMON_GPUS": "1"})
   ```

   A Dockerfile `ENV VMON_GPUS=1` does the same.

3. Otherwise, 0.

Each value must be a decimal from `0` to `8`. Anything else fails the create with 400, and the message names `vmon.gpus` or `VMON_GPUS`. Metadata always wins, so `{"vmon.gpus": "0"}` starts a GPU template without a GPU, which helps when preparing files.

When no node can take the request, the create fails with 503:

| `error_code` | Meaning |
|---|---|
| `sandbox_no_compatible_node` | No live, ready node has that many assignable GPUs at all. |
| `sandbox_capacity_unavailable` | Nodes have enough GPUs, but other sandboxes hold them right now. Retry later. |

Give GPU templates at least 2 GiB of memory; the driver and a CUDA context need it on top of the workload.

### The `cuda` template

`just templates cuda` (or `uv run templates/build.py cuda`) builds the public `cuda` template. It is `nvidia/cuda:13.0.3-runtime-ubuntu24.04` (the CUDA 13.0 runtime; the bundle's driver runs any CUDA 12.x or 13.x runtime) plus `python3`, `pip`, and `venv`, with `VMON_GPUS=1`, 2 vCPUs, and 4 GiB of memory. Its `vmon-gpu-check` command lists the GPUs and round-trips a buffer through each one's memory:

```python
sbx = Sandbox.create("cuda")
print(sbx.commands.run("vmon-gpu-check").stdout)
print(sbx.commands.run("nvidia-smi").stdout)
```

Templates for GPUs need a glibc image with the CUDA runtime they use, and must not ship NVIDIA driver libraries (`libcuda`, `libnvidia-*`) of their own. Those would shadow the bundle's driver.

## Pause, snapshots, and resume

A GPU's state cannot be captured in a VM snapshot, so GPU sandboxes never save memory:

- **Pause** (`sbx.pause()`, with or without memory) and **autoPause** on timeout stop the sandbox and keep only its filesystem. The node unloads the driver first and frees the GPUs.
- **Resume** (`Sandbox.connect`, the deprecated resume, or `autoResume` traffic) boots the paused disk again and reserves GPUs again. Guest processes and memory are gone; the template's start and ready commands run again. If the node holding the paused sandbox has no free GPUs, the resume fails with 503 `sandbox_capacity_unavailable`. With an object store, a paused GPU sandbox whose node is gone resumes on another node with free GPUs.
- **Snapshots** (`sbx.create_snapshot()`) capture only the filesystem. Sandboxes created from such a snapshot cold-boot. To be sure they get GPUs, pass `metadata["vmon.gpus"]` when you create them.
- **Forks** capture the filesystem once, then cold-boot each child on the same node with the same GPU count. Each child needs free GPUs there.
- `autoPauseMemory` and `autoResume` are validated as the E2B API defines them and stored as requested; only the pause itself drops the memory.
- **Migration**: GPU sandboxes are not migrated. `POST /admin/sandboxes/{id}/migrate` refuses them with 400, and draining a node leaves them running there until they end.

## Dashboard

- **Nodes** lists a "GPUs free" column (free / total, plus how many are unavailable) when any node has GPUs.
- A node's page has a **GPUs** card with each GPU's model, PCI address, and state: free, in use by a sandbox (linked), or its problem.
- A sandbox's page shows its GPU count.

The data comes from `GET /admin/cluster` (each node member's `node.gpus: [{address, model, sandbox, problem}]`) and `GET /admin/sandboxes` (`gpus` per sandbox). See [Administration](admin.md).
