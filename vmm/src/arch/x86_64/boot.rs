//! Kernel / initrd / command-line loading and zero-page (`boot_params`) setup.

use std::{cmp::max, fs::File, io::Read, path::Path};

#[cfg(target_os = "linux")]
use kvm_bindings::{CpuId, kvm_segment, kvm_sregs, kvm_userspace_memory_region};
#[cfg(target_os = "linux")]
use kvm_ioctls::VmFd;
use linux_loader::loader::{
	Cmdline, KernelLoader,
	bootparam::{boot_params, setup_header},
	bzimage::BzImage,
	elf::Elf,
	load_cmdline,
};
use vm_memory::{Address, Bytes, GuestAddress, GuestMemory, GuestMemoryRegion};

use super::mptable;
#[cfg(target_os = "linux")]
use crate::hv::Vcpu;
#[cfg(target_os = "windows")]
use crate::hv::{CpuId, Vcpu, Vm};
use crate::{
	bail,
	layout::{
		APIC_DEFAULT_PHYS_BASE, CMDLINE_START, EBDA_START, FIRST_ADDR_PAST_32BITS, HIMEM_START,
		IO_APIC_DEFAULT_PHYS_BASE, ZERO_PAGE_START,
	},
	memory::GuestMemoryMmap,
	result::Result,
};

// Linux x86 boot protocol magic values.
const KERNEL_BOOT_FLAG_MAGIC: u16 = 0xaa55;
const KERNEL_HDR_MAGIC: u32 = 0x5372_6448;
const KERNEL_LOADER_OTHER: u8 = 0xff;
const KERNEL_MIN_ALIGNMENT_BYTES: u32 = 0x0100_0000;
const E820_RAM: u32 = 1;
// 64-bit entry point offset into the protected-mode bzImage kernel.
const BZIMAGE_64BIT_ENTRY_OFFSET: u64 = 0x200;
const GUEST_PAGE_SIZE: u64 = 4096;

pub const UEFI_RESET_VECTOR: u64 = FIRST_ADDR_PAST_32BITS - 0x10;
const UEFI_ACPI_RSDP_ADDR: u64 = 0x000e_0000;
const UEFI_ACPI_TABLES_START: u64 = 0x000e_1000;
const ACPI_HEADER_LEN: usize = 36;
/// A loaded guest kernel: where to start executing, plus the bzImage setup
/// header (absent for a raw ELF `vmlinux`).
pub struct LoadedKernel {
	pub entry:           GuestAddress,
	pub setup_header:    Option<setup_header>,
	/// Extra ROM mapping kept alive while a UEFI guest runs.
	pub firmware_memory: Option<GuestMemoryMmap>,
}

/// Load a kernel image (raw ELF `vmlinux` or `bzImage`) into guest memory.
pub fn load_kernel(path: &Path, mem: &GuestMemoryMmap) -> Result<LoadedKernel> {
	let mut file = std::io::Cursor::new(
		std::fs::read(path).map_err(|e| format!("opening kernel {}: {e}", path.display()))?,
	);
	let himem = Some(GuestAddress(HIMEM_START));

	// Try ELF (uncompressed vmlinux) first; fall back to bzImage.
	if let Ok(res) = Elf::load(mem, None, &mut file, himem) {
		Ok(LoadedKernel {
			entry:           res.kernel_load,
			setup_header:    None,
			firmware_memory: None,
		})
	} else {
		file.rewind_to_start()?;
		let res = BzImage::load(mem, None, &mut file, himem)
			.map_err(|e| format!("loading bzImage {}: {e}", path.display()))?;
		Ok(LoadedKernel {
			entry:           res
				.kernel_load
				.checked_add(BZIMAGE_64BIT_ENTRY_OFFSET)
				.ok_or("bzImage entry overflow")?,
			setup_header:    res.setup_header,
			firmware_memory: None,
		})
	}
}

/// Map an x86 UEFI firmware image as ROM below 4 GiB and return the boot entry.
///
/// Firmware images such as OVMF are linked so their reset vector lives at
/// physical `0xfffffff0`; the whole file is therefore placed at the top of the
/// 32-bit address space. The mapping is separate from guest RAM so normal RAM
/// snapshots do not include operator-supplied firmware bytes.
#[cfg(any(target_os = "linux", target_os = "windows"))]
pub fn load_uefi_firmware(
	path: &Path,
	ram: &GuestMemoryMmap,
	#[cfg(target_os = "linux")] vm_fd: &VmFd,
	#[cfg(target_os = "windows")] vm: &Vm,
) -> Result<LoadedKernel> {
	#[cfg(target_os = "linux")]
	let firmware_memory = map_uefi_firmware(path, ram, vm_fd)?;
	#[cfg(target_os = "windows")]
	let firmware_memory = map_uefi_firmware(path, ram, vm)?;
	Ok(LoadedKernel {
		entry:           GuestAddress(UEFI_RESET_VECTOR),
		setup_header:    None,
		firmware_memory: Some(firmware_memory),
	})
}

/// Register the firmware ROM memslot used for fresh UEFI boots and restores.
#[cfg(any(target_os = "linux", target_os = "windows"))]
pub fn map_uefi_firmware(
	path: &Path,
	ram: &GuestMemoryMmap,
	#[cfg(target_os = "linux")] vm_fd: &VmFd,
	#[cfg(target_os = "windows")] vm: &Vm,
) -> Result<GuestMemoryMmap> {
	let mut file =
		File::open(path).map_err(|e| format!("opening firmware {}: {e}", path.display()))?;
	let mut buf = Vec::new();
	file.read_to_end(&mut buf)?;
	if buf.is_empty() {
		bail!("firmware {} is empty", path.display());
	}
	if !(buf.len() as u64).is_multiple_of(GUEST_PAGE_SIZE) {
		bail!("firmware {} size {} is not {GUEST_PAGE_SIZE}-byte aligned", path.display(), buf.len());
	}
	if buf.len() as u64 > crate::layout::MEM_32BIT_GAP_SIZE {
		bail!("firmware {} size {} exceeds the x86 MMIO gap", path.display(), buf.len());
	}

	let size = buf.len();
	let base = GuestAddress(FIRST_ADDR_PAST_32BITS - size as u64);
	let minimum_firmware_base = u64::from(APIC_DEFAULT_PHYS_BASE) + GUEST_PAGE_SIZE;
	if base.raw_value() < minimum_firmware_base {
		bail!("firmware {} size {} overlaps the local APIC page", path.display(), buf.len());
	}
	let firmware = GuestMemoryMmap::from_ranges(&[(base, size)])
		.map_err(|e| format!("allocating firmware ROM at {:#x}: {e}", base.raw_value()))?;
	firmware.write_slice(&buf, base)?;

	#[cfg(target_os = "windows")]
	{
		let _ = ram;
		vm.map_firmware(&firmware)?;
		Ok(firmware)
	}

	#[cfg(target_os = "linux")]
	{
		let region = firmware
			.find_region(base)
			.ok_or("firmware ROM region disappeared after allocation")?;
		let slot = u32::try_from(ram.iter().count()).map_err(|_| "too many RAM slots")?;
		let kvm_region = kvm_userspace_memory_region {
			slot,
			guest_phys_addr: base.raw_value(),
			memory_size: region.len(),
			userspace_addr: region.as_ptr() as u64,
			flags: 0,
		};
		// SAFETY: `firmware` owns the mmap backing `userspace_addr` and is
		// retained by the VMM for at least as long as the VM can run.
		unsafe { vm_fd.set_user_memory_region(kvm_region)? };
		Ok(firmware)
	}
}

/// Read an initrd/initramfs image into guest RAM above the low boot-data area,
/// below the kernel's highest permitted initrd address.
///
/// `initrd_addr_max` is the kernel's documented ceiling (from the bzImage setup
/// header, or `0x37FF_FFFF` for raw ELF / old protocols). The image is placed
/// page-aligned as high as possible within `min(lowmem_end,
/// initrd_addr_max+1)`, but never below 1 MiB where the zero page, command
/// line, page tables, MP table and legacy firmware-reserved areas live.
pub fn load_initrd(
	path: &Path,
	mem: &GuestMemoryMmap,
	initrd_addr_max: u64,
) -> Result<(GuestAddress, usize)> {
	let mut file =
		File::open(path).map_err(|e| format!("opening initrd {}: {e}", path.display()))?;
	let mut buf = Vec::new();
	file.read_to_end(&mut buf)?;
	let size = buf.len();
	let size_u64 = u64::try_from(size).map_err(|_| "initrd size does not fit in u64")?;

	let first = mem
		.find_region(GuestAddress(0))
		.ok_or("no low memory region for initrd")?;
	let lowmem = first.len();
	// Exclusive upper bound: stay within RAM and at/below the kernel's ceiling.
	let ceiling = lowmem.min(initrd_addr_max.saturating_add(1));
	if ceiling <= HIMEM_START {
		bail!("no room for initrd above x86 boot data below {ceiling:#x}");
	}
	let available = ceiling - HIMEM_START;
	if size_u64 > available {
		bail!("initrd ({size} bytes) does not fit between {HIMEM_START:#x} and {ceiling:#x}");
	}
	let addr = (ceiling - size_u64) & !(GUEST_PAGE_SIZE - 1);
	let addr = GuestAddress(addr);
	mem.write_slice(&buf, addr)?;
	Ok((addr, size))
}

/// Build a `Cmdline` from a complete command-line string.
pub fn build_cmdline(s: &str) -> Result<Cmdline> {
	let mut cmdline = Cmdline::new(crate::layout::CMDLINE_MAX_SIZE)?;
	cmdline.insert_str(s)?;
	Ok(cmdline)
}

/// Write the command line, MP table and zero page so the guest can boot.
pub fn configure_system(
	mem: &GuestMemoryMmap,
	num_cpus: u8,
	cmdline: &Cmdline,
	initrd: Option<(GuestAddress, usize)>,
	setup_header: Option<setup_header>,
) -> Result<()> {
	let cmdline_size = cmdline
		.as_cstring()
		.map_err(|e| format!("serializing cmdline: {e}"))?
		.as_bytes_with_nul()
		.len();

	load_cmdline(mem, GuestAddress(CMDLINE_START), cmdline)
		.map_err(|e| format!("writing cmdline: {e}"))?;

	mptable::setup_mptable(mem, num_cpus)?;

	let have_setup_header = setup_header.is_some();
	let mut params = boot_params { hdr: setup_header.unwrap_or_default(), ..Default::default() };
	// Bootloader-owned fields, always set:
	params.hdr.type_of_loader = KERNEL_LOADER_OTHER;
	params.hdr.cmd_line_ptr = u32::try_from(CMDLINE_START).unwrap();
	if let Some((addr, size)) = initrd {
		validate_initrd_range(mem, addr, size)?;
		params.hdr.ramdisk_image = u32::try_from(addr.raw_value())
			.map_err(|_| "initrd address exceeds 32-bit Linux boot protocol field")?;
		params.hdr.ramdisk_size =
			u32::try_from(size).map_err(|_| "initrd size exceeds 32-bit Linux boot protocol field")?;
	}
	if have_setup_header {
		// `cmdline_size` is the kernel's read-only maximum; just check we fit.
		let max = params.hdr.cmdline_size;
		if max != 0 && cmdline_size.saturating_sub(1) as u32 > max {
			bail!("command line ({cmdline_size} bytes) exceeds kernel max {max}");
		}
	} else {
		// Raw ELF: synthesize the magic fields a bzImage header would carry.
		params.hdr.boot_flag = KERNEL_BOOT_FLAG_MAGIC;
		params.hdr.header = KERNEL_HDR_MAGIC;
		params.hdr.kernel_alignment = KERNEL_MIN_ALIGNMENT_BYTES;
		params.hdr.cmdline_size = u32::try_from(cmdline_size).unwrap();
	}

	// e820 map: usable RAM in [0, EBDA_START), then each RAM region above 1 MiB.
	// The [EBDA_START, HIMEM_START) hole is left reserved (MP table lives
	// there).
	add_e820_entry(&mut params, 0, EBDA_START, E820_RAM)?;
	let himem = GuestAddress(HIMEM_START);
	for region in mem.iter() {
		let region_end = region.last_addr().raw_value();
		let addr = max(himem.raw_value(), region.start_addr().raw_value());
		if addr > region_end {
			continue;
		}
		add_e820_entry(&mut params, addr, region_end - addr + 1, E820_RAM)?;
	}

	mem.write_obj(params, GuestAddress(ZERO_PAGE_START))
		.map_err(|e| format!("writing zero page: {e}"))?;
	Ok(())
}

/// Write minimal ACPI tables discoverable by x86 UEFI firmware/guests.
pub fn configure_uefi_system(
	mem: &GuestMemoryMmap,
	num_cpus: u8,
	pci_mmconfig_base: u64,
	pci_mmconfig_size: u64,
) -> Result<()> {
	if num_cpus == 0 {
		bail!("UEFI ACPI tables require at least one vCPU");
	}

	let mut next = GuestAddress(UEFI_ACPI_TABLES_START);

	let dsdt = build_dsdt();
	let dsdt_addr = write_acpi_table(mem, &mut next, &dsdt)?;

	let fadt = build_fadt(dsdt_addr.raw_value());
	let fadt_addr = write_acpi_table(mem, &mut next, &fadt)?;

	let madt = build_madt(num_cpus);
	let madt_addr = write_acpi_table(mem, &mut next, &madt)?;

	let mcfg = build_mcfg(pci_mmconfig_base, pci_mmconfig_size);
	let mcfg_addr = write_acpi_table(mem, &mut next, &mcfg)?;

	let xsdt = build_xsdt(&[fadt_addr.raw_value(), madt_addr.raw_value(), mcfg_addr.raw_value()]);
	let xsdt_addr = write_acpi_table(mem, &mut next, &xsdt)?;

	let rsdp = build_rsdp(xsdt_addr.raw_value());
	let rsdp_end = GuestAddress(UEFI_ACPI_RSDP_ADDR + rsdp.len() as u64 - 1);
	if !mem.address_in_range(rsdp_end) {
		bail!("not enough guest memory for ACPI RSDP");
	}
	mem.write_slice(&rsdp, GuestAddress(UEFI_ACPI_RSDP_ADDR))
		.map_err(|e| format!("writing ACPI RSDP: {e}"))?;
	Ok(())
}

/// Configure one x86 vCPU for a firmware reset-vector entry instead of the
/// direct Linux 64-bit boot protocol.
#[cfg(any(target_os = "linux", target_os = "windows"))]
pub fn configure_uefi_vcpu(
	vcpu: &Vcpu,
	base_cpuid: &CpuId,
	cpu_id: u8,
	entry: GuestAddress,
) -> Result<()> {
	#[cfg(target_os = "windows")]
	{
		vcpu.set_cpuid_template(base_cpuid, cpu_id)?;
		vcpu.setup_msrs()?;
		vcpu.set_lint()?;
		if cpu_id == 0 {
			vcpu.setup_uefi_sregs()?;
			vcpu.setup_uefi_regs(entry.raw_value())?;
			vcpu.setup_fpu()?;
		} else {
			vcpu.suspend_for_startup()?;
		}
		Ok(())
	}

	#[cfg(target_os = "linux")]
	{
		super::cpuid::setup_cpuid(vcpu, base_cpuid, cpu_id)?;
		super::msr::setup_msrs(vcpu)?;
		super::interrupts::set_lint(vcpu)?;
		if cpu_id == 0 {
			setup_uefi_reset_sregs(vcpu)?;
			setup_uefi_reset_regs(vcpu, entry.raw_value())?;
			super::regs::setup_fpu(vcpu)?;
		}
		Ok(())
	}
}

#[cfg(target_os = "linux")]
fn setup_uefi_reset_regs(vcpu: &Vcpu, entry: u64) -> Result<()> {
	let regs = kvm_bindings::kvm_regs {
		rflags: 0x0000_0000_0000_0002,
		rip: entry & 0xffff,
		..Default::default()
	};
	vcpu.fd().set_regs(&regs)?;
	Ok(())
}

#[cfg(target_os = "linux")]
fn setup_uefi_reset_sregs(vcpu: &Vcpu) -> Result<()> {
	const X86_CR0_PE: u64 = 0x1;
	const X86_CR0_PG: u64 = 0x8000_0000;
	const EFER_LME: u64 = 0x100;
	const EFER_LMA: u64 = 0x400;

	let mut sregs: kvm_sregs = vcpu.fd().get_sregs()?;
	sregs.cs = segment(0xf000, 0xffff_0000, 0x0b);
	sregs.ds = segment(0, 0, 0x03);
	sregs.es = segment(0, 0, 0x03);
	sregs.fs = segment(0, 0, 0x03);
	sregs.gs = segment(0, 0, 0x03);
	sregs.ss = segment(0, 0, 0x03);
	sregs.cr0 &= !(X86_CR0_PE | X86_CR0_PG);
	sregs.efer &= !(EFER_LME | EFER_LMA);
	vcpu.fd().set_sregs(&sregs)?;
	Ok(())
}

#[cfg(target_os = "linux")]
fn segment(selector: u16, base: u64, type_: u8) -> kvm_segment {
	kvm_segment { base, limit: 0xffff, selector, type_, present: 1, s: 1, ..Default::default() }
}

fn write_acpi_table(
	mem: &GuestMemoryMmap,
	next: &mut GuestAddress,
	table: &[u8],
) -> Result<GuestAddress> {
	let addr = *next;
	let end = addr
		.checked_add(table.len() as u64 - 1)
		.ok_or("ACPI table address overflow")?;
	if !mem.address_in_range(end) {
		bail!("not enough guest memory for ACPI table");
	}
	mem.write_slice(table, addr)
		.map_err(|e| format!("writing ACPI table: {e}"))?;
	*next = addr.unchecked_add(align_up(table.len() as u64, 16));
	Ok(addr)
}

fn build_rsdp(xsdt_addr: u64) -> Vec<u8> {
	let mut bytes = vec![0u8; 36];
	bytes[0..8].copy_from_slice(b"RSD PTR ");
	bytes[9..15].copy_from_slice(b"TINYVM");
	bytes[15] = 2;
	let len = bytes.len() as u32;
	put_u32(&mut bytes, 20, len);
	put_u64(&mut bytes, 24, xsdt_addr);
	bytes[8] = checksum(&bytes[..20]);
	let extended_checksum = checksum(&bytes);
	bytes[32] = extended_checksum;
	bytes
}

fn build_xsdt(entries: &[u64]) -> Vec<u8> {
	let mut table = new_sdt(*b"XSDT", entries.len() * 8, 1);
	for (idx, entry) in entries.iter().enumerate() {
		put_u64(&mut table, ACPI_HEADER_LEN + idx * 8, *entry);
	}
	finish_sdt(&mut table);
	table
}

fn build_fadt(dsdt_addr: u64) -> Vec<u8> {
	const FADT_LEN: usize = 244;

	let mut table = new_sdt(*b"FACP", FADT_LEN - ACPI_HEADER_LEN, 6);
	put_u32(&mut table, 40, dsdt_addr as u32);
	table[45] = 0; // Unspecified PM profile.
	put_u16(&mut table, 46, 9); // SCI IRQ, conventional but unused here.
	table[131] = 6; // FADT minor version.
	put_u64(&mut table, 140, dsdt_addr);
	finish_sdt(&mut table);
	table
}

fn build_madt(num_cpus: u8) -> Vec<u8> {
	let body_len = 8 + usize::from(num_cpus) * 8 + 12;
	let mut table = new_sdt(*b"APIC", body_len, 5);
	put_u32(&mut table, ACPI_HEADER_LEN, APIC_DEFAULT_PHYS_BASE);
	put_u32(&mut table, ACPI_HEADER_LEN + 4, 1); // PC-AT dual-8259 present.
	let mut off = ACPI_HEADER_LEN + 8;
	for cpu_id in 0..num_cpus {
		table[off] = 0; // Processor Local APIC.
		table[off + 1] = 8;
		table[off + 2] = cpu_id;
		table[off + 3] = cpu_id;
		put_u32(&mut table, off + 4, 1); // Enabled.
		off += 8;
	}
	table[off] = 1; // I/O APIC.
	table[off + 1] = 12;
	table[off + 2] = num_cpus.saturating_add(1);
	put_u32(&mut table, off + 4, IO_APIC_DEFAULT_PHYS_BASE);
	put_u32(&mut table, off + 8, 0);
	finish_sdt(&mut table);
	table
}

fn build_mcfg(base: u64, size: u64) -> Vec<u8> {
	let mut table = new_sdt(*b"MCFG", 8 + 16, 1);
	let end_bus = size
		.checked_div(1 << 20)
		.and_then(|buses| buses.checked_sub(1))
		.unwrap_or(0)
		.min(255) as u8;
	put_u64(&mut table, ACPI_HEADER_LEN + 8, base);
	put_u16(&mut table, ACPI_HEADER_LEN + 16, 0);
	table[ACPI_HEADER_LEN + 18] = 0;
	table[ACPI_HEADER_LEN + 19] = end_bus;
	finish_sdt(&mut table);
	table
}

fn build_dsdt() -> Vec<u8> {
	let mut table = new_sdt(*b"DSDT", 0, 2);
	finish_sdt(&mut table);
	table
}

fn new_sdt(signature: [u8; 4], body_len: usize, revision: u8) -> Vec<u8> {
	let mut table = vec![0u8; ACPI_HEADER_LEN + body_len];
	table[0..4].copy_from_slice(&signature);
	let len = table.len() as u32;
	put_u32(&mut table, 4, len);
	table[8] = revision;
	table[10..16].copy_from_slice(b"TINYVM");
	table[16..24].copy_from_slice(b"VMON    ");
	put_u32(&mut table, 24, 1);
	table[28..32].copy_from_slice(b"TVMM");
	put_u32(&mut table, 32, 1);
	table
}

fn finish_sdt(table: &mut [u8]) {
	table[9] = 0;
	table[9] = checksum(table);
}

fn checksum(bytes: &[u8]) -> u8 {
	let sum = bytes.iter().fold(0u8, |acc, byte| acc.wrapping_add(*byte));
	0u8.wrapping_sub(sum)
}

const fn align_up(value: u64, align: u64) -> u64 {
	(value + align - 1) & !(align - 1)
}

fn put_u16(buf: &mut [u8], offset: usize, value: u16) {
	buf[offset..offset + 2].copy_from_slice(&value.to_le_bytes());
}

fn put_u32(buf: &mut [u8], offset: usize, value: u32) {
	buf[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
}

fn put_u64(buf: &mut [u8], offset: usize, value: u64) {
	buf[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
}

fn validate_initrd_range(mem: &GuestMemoryMmap, addr: GuestAddress, size: usize) -> Result<()> {
	if size == 0 {
		return Ok(());
	}
	if addr.raw_value() < HIMEM_START {
		bail!("initrd at {:#x} overlaps x86 boot data below {HIMEM_START:#x}", addr.raw_value());
	}
	let end = addr
		.checked_add(size as u64 - 1)
		.ok_or("initrd address range overflow")?;
	if end.raw_value() > u64::from(u32::MAX) {
		bail!(
			"initrd at {:#x} ({size} bytes) exceeds 32-bit Linux boot protocol range",
			addr.raw_value()
		);
	}
	if !mem.check_range(addr, size) {
		bail!("initrd at {:#x} ({size} bytes) is outside guest RAM", addr.raw_value());
	}
	Ok(())
}

fn add_e820_entry(params: &mut boot_params, addr: u64, size: u64, mem_type: u32) -> Result<()> {
	let idx = params.e820_entries as usize;
	if idx >= params.e820_table.len() {
		bail!("too many e820 entries");
	}
	params.e820_table[idx].addr = addr;
	params.e820_table[idx].size = size;
	params.e820_table[idx].type_ = mem_type;
	params.e820_entries += 1;
	Ok(())
}

/// Small extension so loader sources can rewind between format attempts.
trait RewindExt {
	fn rewind_to_start(&mut self) -> Result<()>;
}
impl<T: std::io::Seek> RewindExt for T {
	fn rewind_to_start(&mut self) -> Result<()> {
		std::io::Seek::seek(self, std::io::SeekFrom::Start(0))?;
		Ok(())
	}
}
