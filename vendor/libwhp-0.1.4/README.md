# libwhp

Windows Hypervisor Platform API for Rust:
https://docs.microsoft.com/en-us/virtualization/api/

This crate takes advantage of the safety, lifetime, memory management and
error handling features available in Rust while retaining the original design
of the native Windows Hypervisor Platform (WHP) API.

## Prerequisites

Make sure to have at least:

* Windows 10 build 17134 (RS5 or above)
* Windows Server 1809 (RS5 or above)

Enable the Windows Hypervisor Platform and reboot:

```
Dism /Online /Enable-Feature /FeatureName:HypervisorPlatform
shutdown /r /t 0
```

Last but not least, install [Rust on Windows](https://www.rust-lang.org/en-US/install.html).

## Running the demo example

1. Clone the project's repository:
```
git clone https://github.com/insula-rs/libwhp
cd libwhp
```

2. This example includes a payload (the "guest" binary) that needs
to be compiled using GCC, e.g. with WSL
([Windows Subsystem for Linux](https://docs.microsoft.com/en-us/windows/wsl/install-win10)).
All we need is make, gcc and ld. On Ubuntu:

```
wsl sudo apt-get update
wsl sudo apt-get dist-upgrade -y
wsl sudo apt-get install gcc make binutils -y
```

3. Build the payload:

```
pushd examples\payload
wsl make
popd
```

4. Build and run the example:

```
cargo run --example demo
```

Here's what it does:

* Checks for the hypervisor presence
* Creates a partition
* Sets various partition properties, like the allowed exit types and CPUID results
* Allocates and maps memory
* Creates a vCPU
* Sets up registers for long mode (64 bit)
* Reads the payload in memory (payload.img)
* Sets up the MMIO / IO port intruction emulator and related callbacks
* Starts the vCPU loop
* Handles various types of exits: CPUID, MSR read / write, IO port, MMIO, Halt, etc
