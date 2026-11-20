//! Guest physical memory layout, per architecture.

/// Each virtio-mmio device occupies one 4 KiB window.
pub const MMIO_DEVICE_SIZE: u64 = 0x1000;

// ===========================================================================
// x86_64 (mirrors the well-tested Firecracker/crosvm layout). All boot data
// structures live in the first 1 MiB of guest RAM.
// ===========================================================================
#[cfg(target_arch = "x86_64")]
pub use x86_64::*;

#[cfg(target_arch = "x86_64")]
mod x86_64 {
	/// Zero page (`boot_params`) handed to the kernel in `%rsi`.
	pub const ZERO_PAGE_START: u64 = 0x7000;
	/// Initial stack pointer for the boot vCPU.
	pub const BOOT_STACK_POINTER: u64 = 0x8ff0;
	/// Boot page-table pages (PML4 / PDPTE / PDE).
	pub const PML4_START: u64 = 0x9000;
	pub const PDPTE_START: u64 = 0xa000;
	pub const PDE_START: u64 = 0xb000;
	/// Boot GDT / IDT locations.
	pub const BOOT_GDT_OFFSET: u64 = 0x500;
	pub const BOOT_IDT_OFFSET: u64 = 0x520;
	/// First byte the kernel image is loaded at (1 MiB).
	pub const HIMEM_START: u64 = 0x0010_0000;
	/// Kernel command line.
	pub const CMDLINE_START: u64 = 0x0002_0000;
	pub const CMDLINE_MAX_SIZE: usize = 0x1_0000;
	/// Extended BIOS Data Area; the MP table is written here.
	pub const EBDA_START: u64 = 0x0009_fc00;
	/// First address past the 32-bit space (4 GiB).
	pub const FIRST_ADDR_PAST_32BITS: u64 = 1 << 32;
	/// Size of the hole below 4 GiB reserved for memory-mapped IO (768 MiB).
	pub const MEM_32BIT_GAP_SIZE: u64 = 768 << 20;
	/// Start of the MMIO gap — guest RAM below 4 GiB stops here; virtio devices
	/// live here.
	pub const MMIO_MEM_START: u64 = FIRST_ADDR_PAST_32BITS - MEM_32BIT_GAP_SIZE;
	/// Size of the virtio-mmio device aperture.
	pub const MMIO_MEM_SIZE: u64 = MEM_32BIT_GAP_SIZE;
	/// Legacy PCI config mechanism #1 ports (0xcf8 address + 0xcfc data).
	pub const PCI_CONFIG_IO_BASE: u64 = 0x0cf8;
	pub const PCI_CONFIG_IO_SIZE: u64 = 0x8;
	/// PCI Express ECAM/MMCONFIG window for bus 0.
	pub const PCI_ECAM_BASE: u64 = 0xe000_0000;
	pub const PCI_ECAM_SIZE: u64 = 0x0010_0000;
	/// Fixed virtio-pci BAR aperture below the disjoint ECAM window.
	pub const PCI_VIRTIO_MMIO_START: u64 = MMIO_MEM_START;
	pub const PCI_VIRTIO_MMIO_SIZE: u64 = PCI_ECAM_BASE - PCI_VIRTIO_MMIO_START;
	pub const PCI_VIRTIO_BAR_SIZE: u64 = 0x1_0000;
	/// Local / IO APIC default physical bases (from `apicdef.h`).
	pub const APIC_DEFAULT_PHYS_BASE: u32 = 0xfee0_0000;
	pub const IO_APIC_DEFAULT_PHYS_BASE: u32 = 0xfec0_0000;
	/// ovnfs call page (`OVNFS_MMIO_BASE_X86_64` of ovnfs `kmod/ovnfs_wire.h`):
	/// a fixed address the guest module defaults to, below the IOAPIC.
	#[cfg(unix)]
	pub const OVNFS_MMIO_BASE: u64 = 0xefff_0000;
	/// COM1 uses legacy IRQ 4; virtio devices take consecutive GSIs from 5.
	pub const SERIAL_IRQ: u32 = 4;
	pub const IRQ_BASE: u32 = 5;
	/// In-kernel IOAPIC input pins (all identity-mapped by the MP table).
	pub const NUM_IOAPIC_PINS: u32 = 24;
	/// One-past-the-last GSI available to virtio devices.
	pub const IRQ_END: u32 = NUM_IOAPIC_PINS;
}

// ===========================================================================
// aarch64 (mirrors the Firecracker arm64 / QEMU-virt style map). RAM at 2 GiB;
// GIC, serial and virtio MMIO live in the sub-2 GiB device region.
// ===========================================================================
#[cfg(target_arch = "aarch64")]
pub use aarch64::*;

#[cfg(target_arch = "aarch64")]
mod aarch64 {
	/// Start of guest RAM.
	pub const DRAM_BASE: u64 = 0x8000_0000;
	/// Kernel Image load offset within RAM (2 MiB; leaves low RAM as scratch).
	pub const KERNEL_OFFSET: u64 = 0x20_0000;
	/// Maximum device-tree blob size.
	pub const FDT_MAX_SIZE: u64 = 0x20_0000;

	/// `GICv3` distributor and redistributor.
	pub const GIC_DIST_BASE: u64 = 0x0800_0000;
	pub const GIC_DIST_SIZE: u64 = 0x1_0000;
	pub const GIC_REDIST_BASE: u64 = 0x080a_0000;
	pub const GIC_REDIST_SIZE_PER_CPU: u64 = 0x2_0000;

	/// 16550 serial (MMIO) and its SPI GSI.
	pub const SERIAL_MMIO_BASE: u64 = 0x0900_0000;
	pub const SERIAL_MMIO_SIZE: u64 = 0x1000;
	pub const SERIAL_IRQ: u32 = 0;

	/// ovnfs call page (`OVNFS_MMIO_BASE_ARM64` in ovnfs's `kmod/ovnfs_wire.h`):
	/// a fixed address the guest module defaults to, between the serial port
	/// and the virtio-mmio window.
	#[cfg(unix)]
	pub const OVNFS_MMIO_BASE: u64 = 0x0910_0000;

	/// virtio-mmio device window base and first virtio GSI.
	pub const MMIO_MEM_START: u64 = 0x0a00_0000;
	/// Size of the virtio-mmio device aperture below guest RAM.
	pub const MMIO_MEM_SIZE: u64 = DRAM_BASE - MMIO_MEM_START;
	pub const IRQ_BASE: u32 = 1;
	/// `GICv3` exposes INTIDs 32..127 as SPIs; FDT encodes those as 0..95.
	pub const GIC_SPI_COUNT: u32 = 128 - 32;
	/// One-past-the-last SPI number available to virtio devices.
	pub const IRQ_END: u32 = GIC_SPI_COUNT;
}
