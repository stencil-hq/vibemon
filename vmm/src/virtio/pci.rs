//! `x86_64` virtio-pci transport and legacy PCI config mechanism.
//!
//! This is intentionally separate from `virtio::mmio`: the device backends stay
//! transport-neutral and are activated with the same queues, interrupt object,
//! and worker eventfd used by the mmio path.

use std::sync::{Arc, Weak};

use kvm_bindings::kvm_msi;
use kvm_ioctls::VmFd;
use parking_lot::Mutex;
use tracing::error;
use virtio_bindings::bindings::virtio_config::{VIRTIO_CONFIG_S_DRIVER_OK, VIRTIO_CONFIG_S_FAILED};
use virtio_queue::{Queue, QueueState, QueueT};
use vmm_sys_util::eventfd::EventFd;

use crate::{
	devices::{BusDevice, pci::PciFunction},
	layout::PCI_VIRTIO_BAR_SIZE,
	memory::GuestMemoryMmap,
	result::{Result, err},
	snapshot::{MsixEntrySer, MsixStateSer, PciTransportStateSer},
	virtio::{Interrupt, VirtioDevice},
};

const PCI_VENDOR_ID_REDHAT_QUMRANET: u16 = 0x1af4;
const PCI_DEVICE_ID_VIRTIO_MODERN_BASE: u16 = 0x1040;

const PCI_COMMAND: usize = 0x04;
const PCI_STATUS: usize = 0x06;
const PCI_REVISION_ID: usize = 0x08;
const PCI_BAR0: usize = 0x10;
const PCI_SUBSYSTEM_VENDOR_ID: usize = 0x2c;
const PCI_SUBSYSTEM_ID: usize = 0x2e;
const PCI_CAPABILITY_LIST: usize = 0x34;
const PCI_INTERRUPT_LINE: usize = 0x3c;
const PCI_INTERRUPT_PIN: usize = 0x3d;

const PCI_STATUS_CAP_LIST: u16 = 0x0010;
const PCI_CAP_ID_VENDOR: u8 = 0x09;
const PCI_CAP_ID_MSIX: u8 = 0x11;

const COMMON_CAP_OFFSET: u8 = 0x40;
const NOTIFY_CAP_OFFSET: u8 = 0x50;
const ISR_CAP_OFFSET: u8 = 0x64;
const DEVICE_CAP_OFFSET: u8 = 0x74;
const MSIX_CAP_OFFSET: u8 = 0x84;

const VIRTIO_PCI_CAP_COMMON_CFG: u8 = 1;
const VIRTIO_PCI_CAP_NOTIFY_CFG: u8 = 2;
const VIRTIO_PCI_CAP_ISR_CFG: u8 = 3;
const VIRTIO_PCI_CAP_DEVICE_CFG: u8 = 4;

const COMMON_CFG_OFFSET: u64 = 0x0000;
const COMMON_CFG_SIZE: u64 = 0x1000;
const NOTIFY_CFG_OFFSET: u64 = 0x1000;
const NOTIFY_CFG_SIZE: u64 = 0x1000;
const ISR_CFG_OFFSET: u64 = 0x2000;
const ISR_CFG_SIZE: u64 = 0x1000;
const DEVICE_CFG_OFFSET: u64 = 0x3000;
const DEVICE_CFG_SIZE: u64 = 0x1000;
const MSIX_TABLE_OFFSET: u64 = 0x4000;
const MSIX_TABLE_SIZE: u64 = 0x1000;
const MSIX_PBA_OFFSET: u64 = 0x5000;
const MSIX_PBA_SIZE: u64 = 0x1000;

const COMMON_LEN: usize = 56;
const COMMON_DEVICE_FEATURE_SELECT: usize = 0;
const COMMON_DEVICE_FEATURE: usize = 4;
const COMMON_DRIVER_FEATURE_SELECT: usize = 8;
const COMMON_DRIVER_FEATURE: usize = 12;
const COMMON_MSIX_CONFIG: usize = 16;
const COMMON_NUM_QUEUES: usize = 18;
const COMMON_DEVICE_STATUS: usize = 20;
const COMMON_CONFIG_GENERATION: usize = 21;
const COMMON_QUEUE_SELECT: usize = 22;
const COMMON_QUEUE_SIZE: usize = 24;
const COMMON_QUEUE_MSIX_VECTOR: usize = 26;
const COMMON_QUEUE_ENABLE: usize = 28;
const COMMON_QUEUE_NOTIFY_OFF: usize = 30;
const COMMON_QUEUE_DESC: usize = 32;
const COMMON_QUEUE_DRIVER: usize = 40;
const COMMON_QUEUE_DEVICE: usize = 48;

const NOTIFY_OFF_MULTIPLIER: u32 = 4;
const VIRTIO_MSI_NO_VECTOR: u16 = 0xffff;
const PCI_COMMAND_IO: u16 = 0x0001;
const PCI_COMMAND_MEMORY: u16 = 0x0002;

const MSIX_TABLE_ENTRY_SIZE: usize = 16;
const MSIX_VECTOR_MASKED: u32 = 0x0000_0001;
const MSIX_TABLE_SIZE_MASK: u16 = 0x07ff;
const MSIX_FUNCTION_MASK: u16 = 0x4000;
const MSIX_ENABLE: u16 = 0x8000;

#[derive(Clone, Copy)]
struct MsixEntry {
	msg_addr:    u64,
	msg_data:    u32,
	vector_ctrl: u32,
}

impl Default for MsixEntry {
	fn default() -> Self {
		Self { msg_addr: 0, msg_data: 0, vector_ctrl: MSIX_VECTOR_MASKED }
	}
}

impl MsixEntry {
	const fn masked(self) -> bool {
		self.vector_ctrl & MSIX_VECTOR_MASKED != 0
	}

	fn to_bytes(self) -> [u8; MSIX_TABLE_ENTRY_SIZE] {
		let mut bytes = [0u8; MSIX_TABLE_ENTRY_SIZE];
		bytes[0..8].copy_from_slice(&self.msg_addr.to_le_bytes());
		bytes[8..12].copy_from_slice(&self.msg_data.to_le_bytes());
		bytes[12..16].copy_from_slice(&self.vector_ctrl.to_le_bytes());
		bytes
	}

	fn from_bytes(bytes: [u8; MSIX_TABLE_ENTRY_SIZE]) -> Self {
		Self {
			msg_addr:    u64::from_le_bytes(bytes[0..8].try_into().unwrap()),
			msg_data:    u32::from_le_bytes(bytes[8..12].try_into().unwrap()),
			vector_ctrl: u32::from_le_bytes(bytes[12..16].try_into().unwrap()),
		}
	}
}

impl From<MsixEntry> for MsixEntrySer {
	fn from(entry: MsixEntry) -> Self {
		Self {
			msg_addr:    entry.msg_addr,
			msg_data:    entry.msg_data,
			vector_ctrl: entry.vector_ctrl,
		}
	}
}

impl From<&MsixEntrySer> for MsixEntry {
	fn from(entry: &MsixEntrySer) -> Self {
		Self {
			msg_addr:    entry.msg_addr,
			msg_data:    entry.msg_data,
			vector_ctrl: entry.vector_ctrl,
		}
	}
}

struct MsixState {
	vm_fd:         Arc<VmFd>,
	// Interrupt delivery does not carry a queue index yet, so all MSI-X
	// notifications use the same vector until per-queue signaling is plumbed.
	shared_vector: u16,
	control:       u16,
	table:         Vec<MsixEntry>,
	pending:       u64,
}

impl MsixState {
	fn new(vm_fd: Arc<VmFd>) -> Self {
		const VECTOR_COUNT: usize = 1;
		Self {
			vm_fd,
			shared_vector: VIRTIO_MSI_NO_VECTOR,
			control: (VECTOR_COUNT as u16 - 1) & MSIX_TABLE_SIZE_MASK,
			table: vec![MsixEntry::default(); VECTOR_COUNT],
			pending: 0,
		}
	}

	fn from_state(vm_fd: Arc<VmFd>, state: &MsixStateSer) -> Self {
		let table = if state.table.is_empty() {
			vec![MsixEntry::default()]
		} else {
			state.table.iter().map(MsixEntry::from).collect()
		};
		Self {
			vm_fd,
			shared_vector: state.shared_vector,
			control: state.control,
			table,
			pending: state.pending,
		}
	}

	fn save(&self) -> MsixStateSer {
		MsixStateSer {
			shared_vector: self.shared_vector,
			control:       self.control,
			pending:       self.pending,
			table:         self.table.iter().copied().map(MsixEntrySer::from).collect(),
		}
	}

	const fn control(&self) -> u16 {
		self.control
	}

	const fn config_vector(&self) -> u16 {
		self.shared_vector
	}

	const fn queue_vector(&self, _queue_select: usize) -> u16 {
		self.shared_vector
	}

	fn set_config_vector(&mut self, vector: u16) {
		self.shared_vector = self.accept_vector(vector);
	}

	fn set_queue_vector(&mut self, _queue_select: usize, vector: u16) {
		self.shared_vector = self.accept_vector(vector);
	}

	fn write_control(&mut self, value: u16) -> u16 {
		let writable = value & (MSIX_FUNCTION_MASK | MSIX_ENABLE);
		self.control = (self.control & MSIX_TABLE_SIZE_MASK) | writable;
		self.flush_pending();
		self.control
	}

	const fn reset_vectors(&mut self) {
		self.shared_vector = VIRTIO_MSI_NO_VECTOR;
		self.pending = 0;
	}

	fn read_table(&self, offset: u64, data: &mut [u8]) {
		data.fill(0);
		let start = offset as usize;
		let table_bytes = self.table.len() * MSIX_TABLE_ENTRY_SIZE;
		if start >= table_bytes {
			return;
		}
		for (i, out) in data.iter_mut().enumerate() {
			let byte = start + i;
			if byte >= table_bytes {
				break;
			}
			let entry = self.table[byte / MSIX_TABLE_ENTRY_SIZE].to_bytes();
			*out = entry[byte % MSIX_TABLE_ENTRY_SIZE];
		}
	}

	fn write_table(&mut self, offset: u64, data: &[u8]) {
		let start = offset as usize;
		let table_bytes = self.table.len() * MSIX_TABLE_ENTRY_SIZE;
		if start >= table_bytes || data.is_empty() {
			return;
		}
		let end = (start + data.len()).min(table_bytes);
		let first = start / MSIX_TABLE_ENTRY_SIZE;
		let last = (end - 1) / MSIX_TABLE_ENTRY_SIZE;
		for (idx, entry) in (first..=last).zip(&mut self.table[first..=last]) {
			let entry_start = idx * MSIX_TABLE_ENTRY_SIZE;
			let entry_end = entry_start + MSIX_TABLE_ENTRY_SIZE;
			let copy_start = start.max(entry_start);
			let copy_end = end.min(entry_end);
			let mut bytes = entry.to_bytes();
			bytes[copy_start - entry_start..copy_end - entry_start]
				.copy_from_slice(&data[copy_start - start..copy_end - start]);
			*entry = MsixEntry::from_bytes(bytes);
		}
		self.flush_pending();
	}

	fn read_pba(&self, offset: u64, data: &mut [u8]) {
		let bytes = self.pending.to_le_bytes();
		copy_from(&bytes, offset as usize, data, 0);
	}

	fn signal_interrupt(&mut self) -> Result<bool> {
		if !self.enabled() {
			return Ok(false);
		}
		let Some(vector) = self.notification_vector() else {
			return Ok(false);
		};
		let vector = usize::from(vector);
		if vector >= self.table.len() {
			return Ok(false);
		}
		if self.vector_blocked(vector) {
			self.pending |= 1u64 << vector;
			return Ok(true);
		}
		if !self.inject(vector)? {
			self.pending |= 1u64 << vector;
		}
		Ok(true)
	}

	fn flush_pending(&mut self) {
		if !self.enabled() || self.pending == 0 {
			return;
		}
		for vector in 0..self.table.len() {
			let bit = 1u64 << vector;
			if self.pending & bit == 0 || self.vector_blocked(vector) {
				continue;
			}
			if self.inject(vector).unwrap_or(false) {
				self.pending &= !bit;
			}
		}
	}

	fn inject(&self, vector: usize) -> Result<bool> {
		let entry = self.table[vector];
		let msi = kvm_msi {
			address_lo: entry.msg_addr as u32,
			address_hi: (entry.msg_addr >> 32) as u32,
			data: entry.msg_data,
			..Default::default()
		};
		Ok(self.vm_fd.signal_msi(msi)? > 0)
	}

	const fn enabled(&self) -> bool {
		self.control & MSIX_ENABLE != 0
	}

	const fn function_masked(&self) -> bool {
		self.control & MSIX_FUNCTION_MASK != 0
	}

	fn vector_blocked(&self, vector: usize) -> bool {
		self.function_masked() || self.table[vector].masked()
	}

	fn notification_vector(&self) -> Option<u16> {
		(self.shared_vector != VIRTIO_MSI_NO_VECTOR).then_some(self.shared_vector)
	}

	fn accept_vector(&self, vector: u16) -> u16 {
		if vector == VIRTIO_MSI_NO_VECTOR || usize::from(vector) < self.table.len() {
			vector
		} else {
			VIRTIO_MSI_NO_VECTOR
		}
	}
}

/// One modern virtio-pci function backed by a single preallocated MMIO BAR.
pub struct PciTransport {
	device:    Arc<Mutex<dyn VirtioDevice>>,
	mem:       GuestMemoryMmap,
	interrupt: Arc<Interrupt>,
	queue_evt: EventFd,

	device_features: u64,
	queue_count:     usize,
	queue_max_sizes: Vec<u16>,
	queues:          Vec<Queue>,

	config_space: [u8; 256],
	bar_base:     u64,
	bar0_probe:   bool,
	command:      u16,

	device_features_select: u32,
	driver_features_select: u32,
	acked_features:         u64,
	status:                 u32,
	queue_select:           usize,
	activated:              bool,

	msix: Arc<Mutex<MsixState>>,
}

pub struct PciCommonState {
	pub device_features_select: u32,
	pub driver_features_select: u32,
	pub acked_features:         u64,
	pub status:                 u32,
	pub activated:              bool,
	pub queues:                 Vec<QueueState>,
}

impl PciTransport {
	#[allow(clippy::too_many_arguments, reason = "constructor wires transport-owned device state")]
	pub fn new(
		device: Arc<Mutex<dyn VirtioDevice>>,
		mem: GuestMemoryMmap,
		interrupt: Arc<Interrupt>,
		vm_fd: Arc<VmFd>,
		queue_evt: EventFd,
		bar_base: u64,
		gsi: u32,
	) -> Result<Self> {
		let (device_type, device_features, queue_sizes) = {
			let d = device.lock();
			(d.device_type(), d.features(), d.queue_max_sizes().to_vec())
		};
		let queues = new_queues(&queue_sizes)?;

		let msix_state = MsixState::new(vm_fd);
		let msix_control = msix_state.control();
		Ok(Self {
			device,
			mem,
			interrupt,
			queue_evt,
			device_features,
			queue_count: queue_sizes.len(),
			queue_max_sizes: queue_sizes,
			queues,
			config_space: build_config_space(device_type, bar_base, gsi, msix_control),
			bar_base,
			bar0_probe: false,
			command: 0,
			device_features_select: 0,
			driver_features_select: 0,
			acked_features: 0,
			status: 0,
			queue_select: 0,
			activated: false,
			msix: Arc::new(Mutex::new(msix_state)),
		})
	}

	pub fn attach_interrupt_notifier(transport: &Arc<Mutex<Self>>, interrupt: &Arc<Interrupt>) {
		let msix = transport.lock().msix.clone();
		let weak: Weak<Mutex<MsixState>> = Arc::downgrade(&msix);
		interrupt.set_notifier(move || {
			let Some(msix) = weak.upgrade() else {
				return Ok(false);
			};
			msix.lock().signal_interrupt()
		});
	}

	/// Snapshot the PCI-specific config/BAR/MSI-X state for serialization.
	pub fn save(&self) -> PciTransportStateSer {
		PciTransportStateSer {
			config_space: self.config_snapshot().to_vec(),
			bar_base:     self.bar_base,
			bar0_probe:   self.bar0_probe,
			command:      self.command,
			msix:         self.msix.lock().save(),
		}
	}

	/// Snapshot the virtio common control-plane registers shared with MMIO
	/// state.
	pub fn common_state(&self) -> PciCommonState {
		let live_queues = self.device.lock().queue_states();
		let queues = if live_queues.is_empty() {
			self.queues.iter().map(|q| q.state()).collect()
		} else {
			live_queues
		};
		PciCommonState {
			device_features_select: self.device_features_select,
			driver_features_select: self.driver_features_select,
			acked_features: self.acked_features,
			status: self.status,
			activated: self.activated,
			queues,
		}
	}

	#[allow(clippy::too_many_arguments, reason = "restore path supplies serialized transport state")]
	pub fn from_state(
		device: Arc<Mutex<dyn VirtioDevice>>,
		mem: GuestMemoryMmap,
		interrupt: Arc<Interrupt>,
		vm_fd: Arc<VmFd>,
		queue_evt: EventFd,
		pci: &PciTransportStateSer,
		common: PciCommonState,
	) -> Result<Self> {
		if pci.config_space.len() != 256 {
			return Err(err(format!("PCI config space length {} != 256", pci.config_space.len())));
		}
		let (device_features, queue_sizes) = {
			let d = device.lock();
			(d.features(), d.queue_max_sizes().to_vec())
		};
		if common.queues.len() != queue_sizes.len() {
			return Err(err(format!(
				"PCI queue count {} != device queue count {}",
				common.queues.len(),
				queue_sizes.len()
			)));
		}
		let queues = common
			.queues
			.into_iter()
			.map(Queue::try_from)
			.collect::<std::result::Result<Vec<_>, _>>()?;
		let mut config_space = [0u8; 256];
		config_space.copy_from_slice(&pci.config_space);
		Ok(Self {
			device,
			mem,
			interrupt,
			queue_evt,
			device_features,
			queue_count: queue_sizes.len(),
			queue_max_sizes: queue_sizes,
			queues,
			config_space,
			bar_base: pci.bar_base,
			bar0_probe: pci.bar0_probe,
			command: pci.command,
			device_features_select: common.device_features_select,
			driver_features_select: common.driver_features_select,
			acked_features: common.acked_features,
			status: common.status,
			queue_select: 0,
			activated: false,
			msix: Arc::new(Mutex::new(MsixState::from_state(vm_fd, &pci.msix))),
		})
	}

	pub(crate) fn bar_offset(&self, addr: u64) -> Option<u64> {
		if self.bar0_probe || !self.memory_enabled() || self.bar_base == 0 {
			return None;
		}
		let end = self.bar_base.checked_add(PCI_VIRTIO_BAR_SIZE)?;
		(addr >= self.bar_base && addr < end).then_some(addr - self.bar_base)
	}

	const fn memory_enabled(&self) -> bool {
		self.command & PCI_COMMAND_MEMORY != 0
	}

	#[allow(
		clippy::needless_pass_by_ref_mut,
		reason = "BAR read path keeps &mut self for transport read/write API symmetry"
	)]
	fn read_bar(&mut self, offset: u64, data: &mut [u8]) {
		match offset {
			COMMON_CFG_OFFSET..=COMMON_CFG_END => self.read_common(offset - COMMON_CFG_OFFSET, data),
			NOTIFY_CFG_OFFSET..=NOTIFY_CFG_END => data.fill(0),
			ISR_CFG_OFFSET..=ISR_CFG_END => self.read_isr(offset - ISR_CFG_OFFSET, data),
			DEVICE_CFG_OFFSET..=DEVICE_CFG_END => self
				.device
				.lock()
				.read_config(offset - DEVICE_CFG_OFFSET, data),
			MSIX_TABLE_OFFSET..=MSIX_TABLE_END => {
				self.read_msix_table(offset - MSIX_TABLE_OFFSET, data);
			},
			MSIX_PBA_OFFSET..=MSIX_PBA_END => self.read_msix_pba(offset - MSIX_PBA_OFFSET, data),
			_ => data.fill(0),
		}
	}

	fn write_bar(&mut self, offset: u64, data: &[u8]) {
		match offset {
			COMMON_CFG_OFFSET..=COMMON_CFG_END => self.write_common(offset - COMMON_CFG_OFFSET, data),
			NOTIFY_CFG_OFFSET..=NOTIFY_CFG_END => {
				let _ = self.queue_evt.write(1);
			},
			ISR_CFG_OFFSET..=ISR_CFG_END => {},
			DEVICE_CFG_OFFSET..=DEVICE_CFG_END => self
				.device
				.lock()
				.write_config(offset - DEVICE_CFG_OFFSET, data),
			MSIX_TABLE_OFFSET..=MSIX_TABLE_END => {
				self.write_msix_table(offset - MSIX_TABLE_OFFSET, data);
			},
			MSIX_PBA_OFFSET..=MSIX_PBA_END => {},
			_ => {},
		}
	}

	fn read_config(&self, offset: usize, data: &mut [u8]) {
		let cfg = self.config_snapshot();
		copy_from(&cfg, offset, data, 0xff);
	}

	fn write_config(&mut self, offset: usize, data: &[u8]) {
		let mut cfg = self.config_snapshot();
		overlay(&mut cfg, offset, data);

		if touches(offset, data.len(), PCI_COMMAND, 2) {
			self.command =
				read_u16(&cfg, PCI_COMMAND) & (PCI_COMMAND_IO | PCI_COMMAND_MEMORY | 0x0004);
			write_u16(&mut self.config_space, PCI_COMMAND, self.command);
		}
		if touches(offset, data.len(), PCI_BAR0, 4) {
			let value = read_u32(&cfg, PCI_BAR0);
			if value == u32::MAX {
				self.bar0_probe = true;
			} else {
				self.bar0_probe = false;
				self.bar_base = u64::from(value & 0xffff_fff0);
			}
		}
		if touches(offset, data.len(), PCI_INTERRUPT_LINE, 1) {
			self.config_space[PCI_INTERRUPT_LINE] = cfg[PCI_INTERRUPT_LINE];
		}
		let msix_control_offset = usize::from(MSIX_CAP_OFFSET) + 2;
		if touches(offset, data.len(), msix_control_offset, 2) {
			let msix_control = self
				.msix
				.lock()
				.write_control(read_u16(&cfg, msix_control_offset));
			write_u16(&mut self.config_space, msix_control_offset, msix_control);
		}
	}

	fn config_snapshot(&self) -> [u8; 256] {
		let mut cfg = self.config_space;
		let msix_control = self.msix.lock().control();
		write_u16(&mut cfg, PCI_COMMAND, self.command);
		write_u16(&mut cfg, PCI_STATUS, PCI_STATUS_CAP_LIST);
		let bar = if self.bar0_probe {
			(!(PCI_VIRTIO_BAR_SIZE as u32 - 1)) & 0xffff_fff0
		} else {
			self.bar_base as u32 & 0xffff_fff0
		};
		write_u32(&mut cfg, PCI_BAR0, bar);
		write_u16(&mut cfg, usize::from(MSIX_CAP_OFFSET) + 2, msix_control);
		cfg
	}

	fn read_common(&self, offset: u64, data: &mut [u8]) {
		let common = self.common_bytes();
		copy_from(&common, offset as usize, data, 0);
	}

	fn write_common(&mut self, offset: u64, data: &[u8]) {
		let offset = offset as usize;
		let mut common = self.common_bytes();
		overlay(&mut common, offset, data);

		if touches(offset, data.len(), COMMON_DEVICE_FEATURE_SELECT, 4) {
			self.device_features_select = read_u32(&common, COMMON_DEVICE_FEATURE_SELECT);
		}
		if touches(offset, data.len(), COMMON_DRIVER_FEATURE_SELECT, 4) {
			self.driver_features_select = read_u32(&common, COMMON_DRIVER_FEATURE_SELECT);
		}
		if touches(offset, data.len(), COMMON_DRIVER_FEATURE, 4) && self.driver_features_select < 2 {
			let shift = 32 * self.driver_features_select;
			let mask = 0xffff_ffffu64 << shift;
			let selected = (u64::from(read_u32(&common, COMMON_DRIVER_FEATURE)) << shift)
				& self.device_features
				& mask;
			self.acked_features = (self.acked_features & !mask) | selected;
		}
		if touches(offset, data.len(), COMMON_MSIX_CONFIG, 2) {
			self
				.msix
				.lock()
				.set_config_vector(read_u16(&common, COMMON_MSIX_CONFIG));
		}
		if touches(offset, data.len(), COMMON_QUEUE_SELECT, 2) {
			self.queue_select = usize::from(read_u16(&common, COMMON_QUEUE_SELECT));
		}
		if touches(offset, data.len(), COMMON_QUEUE_SIZE, 2) {
			self.set_selected_queue_size(read_u16(&common, COMMON_QUEUE_SIZE));
		}
		if touches(offset, data.len(), COMMON_QUEUE_MSIX_VECTOR, 2)
			&& self.queue_select < self.queue_count
		{
			self
				.msix
				.lock()
				.set_queue_vector(self.queue_select, read_u16(&common, COMMON_QUEUE_MSIX_VECTOR));
		}
		if touches(offset, data.len(), COMMON_QUEUE_ENABLE, 2) {
			let enabled = read_u16(&common, COMMON_QUEUE_ENABLE) == 1;
			self.with_queue_mut(|q| q.set_ready(enabled));
		}
		if touches(offset, data.len(), COMMON_QUEUE_DESC, 8) {
			let addr = read_u64(&common, COMMON_QUEUE_DESC);
			self.with_queue_mut(|q| {
				q.set_desc_table_address(Some(addr as u32), Some((addr >> 32) as u32));
			});
		}
		if touches(offset, data.len(), COMMON_QUEUE_DRIVER, 8) {
			let addr = read_u64(&common, COMMON_QUEUE_DRIVER);
			self.with_queue_mut(|q| {
				q.set_avail_ring_address(Some(addr as u32), Some((addr >> 32) as u32));
			});
		}
		if touches(offset, data.len(), COMMON_QUEUE_DEVICE, 8) {
			let addr = read_u64(&common, COMMON_QUEUE_DEVICE);
			self.with_queue_mut(|q| {
				q.set_used_ring_address(Some(addr as u32), Some((addr >> 32) as u32));
			});
		}
		if touches(offset, data.len(), COMMON_DEVICE_STATUS, 1) {
			self.set_status(u32::from(common[COMMON_DEVICE_STATUS]));
		}
	}

	fn common_bytes(&self) -> [u8; COMMON_LEN] {
		let mut bytes = [0u8; COMMON_LEN];
		let (config_msix_vector, queue_msix_vector) = {
			let msix = self.msix.lock();
			(msix.config_vector(), msix.queue_vector(self.queue_select))
		};
		let feature_value = match self.device_features_select {
			0 => self.device_features as u32,
			1 => (self.device_features >> 32) as u32,
			_ => 0,
		};
		let driver_value = match self.driver_features_select {
			0 => self.acked_features as u32,
			1 => (self.acked_features >> 32) as u32,
			_ => 0,
		};
		write_u32(&mut bytes, COMMON_DEVICE_FEATURE_SELECT, self.device_features_select);
		write_u32(&mut bytes, COMMON_DEVICE_FEATURE, feature_value);
		write_u32(&mut bytes, COMMON_DRIVER_FEATURE_SELECT, self.driver_features_select);
		write_u32(&mut bytes, COMMON_DRIVER_FEATURE, driver_value);
		write_u16(&mut bytes, COMMON_MSIX_CONFIG, config_msix_vector);
		write_u16(&mut bytes, COMMON_NUM_QUEUES, self.queue_count as u16);
		bytes[COMMON_DEVICE_STATUS] = self.status as u8;
		bytes[COMMON_CONFIG_GENERATION] = 0;
		write_u16(&mut bytes, COMMON_QUEUE_SELECT, self.queue_select as u16);
		if let Some(q) = self.queues.get(self.queue_select) {
			let state = q.state();
			write_u16(&mut bytes, COMMON_QUEUE_SIZE, state.size);
			write_u16(&mut bytes, COMMON_QUEUE_MSIX_VECTOR, queue_msix_vector);
			write_u16(&mut bytes, COMMON_QUEUE_ENABLE, u16::from(state.ready));
			write_u16(&mut bytes, COMMON_QUEUE_NOTIFY_OFF, self.queue_select as u16);
			write_u64(&mut bytes, COMMON_QUEUE_DESC, state.desc_table);
			write_u64(&mut bytes, COMMON_QUEUE_DRIVER, state.avail_ring);
			write_u64(&mut bytes, COMMON_QUEUE_DEVICE, state.used_ring);
		} else if let Some(&max_size) = self.queue_max_sizes.get(self.queue_select) {
			write_u16(&mut bytes, COMMON_QUEUE_SIZE, max_size);
			write_u16(&mut bytes, COMMON_QUEUE_NOTIFY_OFF, self.queue_select as u16);
		}
		bytes
	}

	fn read_isr(&self, offset: u64, data: &mut [u8]) {
		data.fill(0);
		if offset == 0 && !data.is_empty() {
			data[0] = self.interrupt.take_status() as u8;
		}
	}

	fn read_msix_table(&self, offset: u64, data: &mut [u8]) {
		self.msix.lock().read_table(offset, data);
	}

	#[allow(
		clippy::needless_pass_by_ref_mut,
		reason = "MSI-X table write path keeps &mut self for transport API symmetry"
	)]
	fn write_msix_table(&mut self, offset: u64, data: &[u8]) {
		self.msix.lock().write_table(offset, data);
	}

	fn read_msix_pba(&self, offset: u64, data: &mut [u8]) {
		self.msix.lock().read_pba(offset, data);
	}

	fn with_queue_mut(&mut self, f: impl FnOnce(&mut Queue)) {
		if let Some(q) = self.queues.get_mut(self.queue_select) {
			f(q);
		}
	}

	fn set_selected_queue_size(&mut self, size: u16) {
		if size == 0 {
			return;
		}
		self.with_queue_mut(|q| {
			if size <= q.max_size() {
				q.set_size(size);
			}
		});
	}

	fn reset(&mut self) -> Result<()> {
		let (device_features, queue_sizes) = {
			let mut device = self.device.lock();
			device.reset()?;
			(device.features(), device.queue_max_sizes().to_vec())
		};
		self.device_features = device_features;
		self.queue_count = queue_sizes.len();
		self.queue_max_sizes = queue_sizes;
		self.queues = new_queues(&self.queue_max_sizes)?;
		self.device_features_select = 0;
		self.driver_features_select = 0;
		self.acked_features = 0;
		self.status = 0;
		self.queue_select = 0;
		self.activated = false;
		self.msix.lock().reset_vectors();
		self.interrupt.clear();
		Ok(())
	}

	fn set_status(&mut self, value: u32) {
		if value == 0 {
			if let Err(e) = self.reset() {
				error!("virtio-pci reset failed: {e}");
				crate::metrics::record_device_worker_error();
				self.status = VIRTIO_CONFIG_S_FAILED;
				self.activated = false;
			}
			return;
		}
		self.status = value;
		if value & VIRTIO_CONFIG_S_DRIVER_OK != 0 && !self.activated {
			self.activate();
		}
	}

	fn activate(&mut self) {
		if !self.queues.iter().all(|q| q.is_valid(&self.mem)) {
			error!("virtio-pci device activation refused invalid queue configuration");
			crate::metrics::record_device_worker_error();
			self.status |= VIRTIO_CONFIG_S_FAILED;
			return;
		}

		let queues = std::mem::take(&mut self.queues);
		let mut device = self.device.lock();
		device.ack_features(self.acked_features);
		match device.activate(self.mem.clone(), self.interrupt.clone(), queues) {
			Ok(()) => self.activated = true,
			Err(e) => {
				error!("virtio-pci device activation failed: {e}");
				crate::metrics::record_device_worker_error();
				self.status |= VIRTIO_CONFIG_S_FAILED;
			},
		}
	}

	/// Restore-path counterpart to [`activate`](Self::activate): hand restored
	/// queues to the backend and surface activation errors to the caller.
	pub fn reactivate(&mut self) -> Result<()> {
		if !self.queues.iter().all(|q| q.is_valid(&self.mem)) {
			return Err(err("virtio-pci device restore refused invalid queue configuration"));
		}
		let queues = std::mem::take(&mut self.queues);
		let mut device = self.device.lock();
		device.ack_features(self.acked_features);
		device.activate(self.mem.clone(), self.interrupt.clone(), queues)?;
		self.activated = true;
		Ok(())
	}
}

impl BusDevice for PciTransport {
	fn read(&mut self, offset: u64, data: &mut [u8]) {
		if self.memory_enabled() {
			self.read_bar(offset, data);
		} else {
			data.fill(0xff);
		}
	}

	fn write(&mut self, offset: u64, data: &[u8]) {
		if self.memory_enabled() {
			self.write_bar(offset, data);
		}
	}
}

impl PciFunction for Mutex<PciTransport> {
	fn read_config(&self, offset: usize, data: &mut [u8]) {
		self.lock().read_config(offset, data);
	}

	fn write_config(&self, offset: usize, data: &[u8]) {
		self.lock().write_config(offset, data);
	}

	fn read_bar(&self, addr: u64, data: &mut [u8]) -> bool {
		let mut transport = self.lock();
		let Some(offset) = transport.bar_offset(addr) else {
			return false;
		};
		transport.read_bar(offset, data);
		true
	}

	fn write_bar(&self, addr: u64, data: &[u8]) -> bool {
		let mut transport = self.lock();
		let Some(offset) = transport.bar_offset(addr) else {
			return false;
		};
		transport.write_bar(offset, data);
		true
	}
}

const COMMON_CFG_END: u64 = COMMON_CFG_OFFSET + COMMON_CFG_SIZE - 1;
const NOTIFY_CFG_END: u64 = NOTIFY_CFG_OFFSET + NOTIFY_CFG_SIZE - 1;
const ISR_CFG_END: u64 = ISR_CFG_OFFSET + ISR_CFG_SIZE - 1;
const DEVICE_CFG_END: u64 = DEVICE_CFG_OFFSET + DEVICE_CFG_SIZE - 1;
const MSIX_TABLE_END: u64 = MSIX_TABLE_OFFSET + MSIX_TABLE_SIZE - 1;
const MSIX_PBA_END: u64 = MSIX_PBA_OFFSET + MSIX_PBA_SIZE - 1;

fn new_queues(queue_sizes: &[u16]) -> Result<Vec<Queue>> {
	let mut queues = Vec::with_capacity(queue_sizes.len());
	for &size in queue_sizes {
		queues.push(Queue::new(size)?);
	}
	Ok(queues)
}

fn build_config_space(device_type: u32, bar_base: u64, gsi: u32, msix_control: u16) -> [u8; 256] {
	let mut cfg = [0u8; 256];
	write_u16(&mut cfg, 0x00, PCI_VENDOR_ID_REDHAT_QUMRANET);
	write_u16(&mut cfg, 0x02, PCI_DEVICE_ID_VIRTIO_MODERN_BASE + device_type as u16);
	write_u16(&mut cfg, PCI_STATUS, PCI_STATUS_CAP_LIST);
	cfg[PCI_REVISION_ID] = 1;
	write_u32(&mut cfg, PCI_BAR0, bar_base as u32 & 0xffff_fff0);
	write_u16(&mut cfg, PCI_SUBSYSTEM_VENDOR_ID, PCI_VENDOR_ID_REDHAT_QUMRANET);
	write_u16(&mut cfg, PCI_SUBSYSTEM_ID, device_type as u16);
	cfg[PCI_CAPABILITY_LIST] = COMMON_CAP_OFFSET;
	cfg[PCI_INTERRUPT_LINE] = gsi as u8;
	cfg[PCI_INTERRUPT_PIN] = 1; // INTA# legacy fallback.

	write_virtio_cap(
		&mut cfg,
		COMMON_CAP_OFFSET,
		NOTIFY_CAP_OFFSET,
		VIRTIO_PCI_CAP_COMMON_CFG,
		COMMON_CFG_OFFSET,
		COMMON_LEN as u32,
	);
	write_virtio_notify_cap(&mut cfg, NOTIFY_CAP_OFFSET, ISR_CAP_OFFSET);
	write_virtio_cap(
		&mut cfg,
		ISR_CAP_OFFSET,
		DEVICE_CAP_OFFSET,
		VIRTIO_PCI_CAP_ISR_CFG,
		ISR_CFG_OFFSET,
		1,
	);
	write_virtio_cap(
		&mut cfg,
		DEVICE_CAP_OFFSET,
		MSIX_CAP_OFFSET,
		VIRTIO_PCI_CAP_DEVICE_CFG,
		DEVICE_CFG_OFFSET,
		DEVICE_CFG_SIZE as u32,
	);
	write_msix_cap(&mut cfg, MSIX_CAP_OFFSET, msix_control);
	cfg
}

fn write_virtio_cap(
	cfg: &mut [u8; 256],
	offset: u8,
	next: u8,
	cfg_type: u8,
	cap_offset: u64,
	cap_length: u32,
) {
	let off = usize::from(offset);
	cfg[off] = PCI_CAP_ID_VENDOR;
	cfg[off + 1] = next;
	cfg[off + 2] = 16;
	cfg[off + 3] = cfg_type;
	cfg[off + 4] = 0; // BAR 0.
	cfg[off + 5] = 0;
	cfg[off + 6] = 0;
	cfg[off + 7] = 0;
	write_u32(cfg, off + 8, cap_offset as u32);
	write_u32(cfg, off + 12, cap_length);
}

fn write_virtio_notify_cap(cfg: &mut [u8; 256], offset: u8, next: u8) {
	write_virtio_cap(
		cfg,
		offset,
		next,
		VIRTIO_PCI_CAP_NOTIFY_CFG,
		NOTIFY_CFG_OFFSET,
		NOTIFY_CFG_SIZE as u32,
	);
	let off = usize::from(offset);
	cfg[off + 2] = 20;
	write_u32(cfg, off + 16, NOTIFY_OFF_MULTIPLIER);
}

fn write_msix_cap(cfg: &mut [u8; 256], offset: u8, msix_control: u16) {
	let off = usize::from(offset);
	cfg[off] = PCI_CAP_ID_MSIX;
	cfg[off + 1] = 0;
	write_u16(cfg, off + 2, msix_control);
	write_u32(cfg, off + 4, MSIX_TABLE_OFFSET as u32);
	write_u32(cfg, off + 8, MSIX_PBA_OFFSET as u32);
}

fn copy_from(src: &[u8], offset: usize, data: &mut [u8], fill: u8) {
	data.fill(fill);
	if offset >= src.len() {
		return;
	}
	let n = data.len().min(src.len() - offset);
	data[..n].copy_from_slice(&src[offset..offset + n]);
}

fn overlay(dst: &mut [u8], offset: usize, data: &[u8]) {
	if offset >= dst.len() {
		return;
	}
	let n = data.len().min(dst.len() - offset);
	dst[offset..offset + n].copy_from_slice(&data[..n]);
}

const fn touches(
	write_offset: usize,
	write_len: usize,
	field_offset: usize,
	field_len: usize,
) -> bool {
	let write_end = write_offset.saturating_add(write_len);
	let field_end = field_offset.saturating_add(field_len);
	write_offset < field_end && field_offset < write_end
}

fn read_u16(buf: &[u8], offset: usize) -> u16 {
	u16::from_le_bytes(buf[offset..offset + 2].try_into().unwrap())
}

fn read_u32(buf: &[u8], offset: usize) -> u32 {
	u32::from_le_bytes(buf[offset..offset + 4].try_into().unwrap())
}

fn read_u64(buf: &[u8], offset: usize) -> u64 {
	u64::from_le_bytes(buf[offset..offset + 8].try_into().unwrap())
}

fn write_u16(buf: &mut [u8], offset: usize, value: u16) {
	buf[offset..offset + 2].copy_from_slice(&value.to_le_bytes());
}

fn write_u32(buf: &mut [u8], offset: usize, value: u32) {
	buf[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
}

fn write_u64(buf: &mut [u8], offset: usize, value: u64) {
	buf[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
}
