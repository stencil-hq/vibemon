//! Platform TAP/vmnet device wrapper for virtio-net.
//!
//! Linux uses `/dev/net/tun` in TAP + vnet-header mode. macOS uses
//! `vmnet.framework` shared networking behind the same virtio-net-facing
//! abstraction and wakes the device worker through a pipe-backed event fd.

/// Size of the virtio-net vnet header carried before each Ethernet frame.
pub const VNET_HDR_SIZE: usize = 12;
/// TAP/vmnet checksum-offload feature bit used by virtio-net negotiation.
pub const TUN_F_CSUM: u32 = 0x01;
/// TAP/vmnet IPv4 TSO offload feature bit used by virtio-net negotiation.
pub const TUN_F_TSO4: u32 = 0x02;
/// TAP/vmnet IPv6 TSO offload feature bit used by virtio-net negotiation.
pub const TUN_F_TSO6: u32 = 0x04;

#[cfg(target_os = "linux")]
mod platform {
	use std::{
		fs::{File, OpenOptions},
		io::{self, Read, Write},
		os::unix::io::{AsRawFd, RawFd},
	};

	use crate::{
		bail,
		os::libc_abi::IoctlRequest,
		result::Result,
		tap::{TUN_F_CSUM, TUN_F_TSO4, TUN_F_TSO6, VNET_HDR_SIZE},
	};

	// _IOW('T', 202, int)
	const TUNSETIFF: IoctlRequest = 0x4004_54ca;
	// _IOW('T', 208, unsigned int)
	const TUNSETOFFLOAD: IoctlRequest = 0x4004_54d0;
	// _IOW('T', 216, int)
	const TUNSETVNETHDRSZ: IoctlRequest = 0x4004_54d8;
	// Linux UAPI flags not exposed by every libc target.
	const IFF_VNET_HDR: libc::c_int = 0x4000;
	// `struct ifreq` is 40 bytes; flags live as an i16 at offset 16.
	const IFREQ_SIZE: usize = 40;
	const IFREQ_FLAGS_OFFSET: usize = 16;

	/// Linux TAP interface opened in vnet-header mode.
	pub struct Tap {
		file: File,
		mac:  [u8; 6],
	}

	impl Tap {
		/// Attach to (or create) the tap interface named `name`.
		pub fn open(name: &str, mac: [u8; 6]) -> Result<Self> {
			if name.len() >= 16 {
				bail!("tap name {name:?} too long");
			}
			let file = OpenOptions::new()
				.read(true)
				.write(true)
				.open("/dev/net/tun")
				.map_err(|e| format!("opening /dev/net/tun: {e}"))?;

			let mut ifreq = [0u8; IFREQ_SIZE];
			ifreq[..name.len()].copy_from_slice(name.as_bytes());
			let flags: i16 = (libc::IFF_TAP | libc::IFF_NO_PI | IFF_VNET_HDR) as i16;
			ifreq[IFREQ_FLAGS_OFFSET..IFREQ_FLAGS_OFFSET + 2].copy_from_slice(&flags.to_le_bytes());

			// SAFETY: valid fd and a correctly sized ifreq buffer.
			let ret = unsafe { libc::ioctl(file.as_raw_fd(), TUNSETIFF, ifreq.as_mut_ptr()) };
			if ret < 0 {
				return Err(format!("TUNSETIFF({name}): {}", io::Error::last_os_error()).into());
			}

			set_vnet_hdr_size(file.as_raw_fd(), VNET_HDR_SIZE)?;
			set_offloads(file.as_raw_fd(), 0)?;

			set_nonblocking(file.as_raw_fd())?;
			Ok(Self { file, mac })
		}

		/// Read one vnet-header-prefixed Ethernet frame from the TAP fd.
		pub fn read(&self, buf: &mut [u8]) -> io::Result<usize> {
			(&self.file).read(buf)
		}

		/// Write one vnet-header-prefixed Ethernet frame to the TAP fd.
		pub fn write(&self, buf: &[u8]) -> io::Result<usize> {
			(&self.file).write(buf)
		}

		/// Enable the negotiated Linux TAP offloads.
		pub fn set_offloads(&self, offloads: u32) -> Result<()> {
			set_offloads(self.file.as_raw_fd(), offloads)
		}

		/// Return Linux TAP offload flags this backend can safely advertise.
		#[expect(
			clippy::unused_self,
			reason = "all net backends expose the same instance-shaped offload API"
		)]
		pub const fn supported_offloads(&self) -> u32 {
			TUN_F_CSUM | TUN_F_TSO4 | TUN_F_TSO6
		}

		/// Return the MAC address advertised to the virtio-net guest.
		pub const fn mac(&self) -> [u8; 6] {
			self.mac
		}

		/// Return the raw TAP fd polled by the virtio worker.
		pub fn as_raw_fd(&self) -> RawFd {
			self.file.as_raw_fd()
		}
	}

	fn set_vnet_hdr_size(fd: RawFd, size: usize) -> Result<()> {
		let mut size = size as libc::c_int;
		// SAFETY: fd is valid, request expects a pointer to an int-sized header length.
		let ret = unsafe { libc::ioctl(fd, TUNSETVNETHDRSZ, &mut size) };
		if ret < 0 {
			return Err(
				format!("TUNSETVNETHDRSZ({}): {}", VNET_HDR_SIZE, io::Error::last_os_error()).into(),
			);
		}
		Ok(())
	}

	fn set_offloads(fd: RawFd, offloads: u32) -> Result<()> {
		// SAFETY: fd is valid, request consumes the offload bitmask value.
		let ret = unsafe { libc::ioctl(fd, TUNSETOFFLOAD, libc::c_ulong::from(offloads)) };
		if ret < 0 {
			return Err(
				format!("TUNSETOFFLOAD({offloads:#x}): {}", io::Error::last_os_error()).into(),
			);
		}
		Ok(())
	}

	fn set_nonblocking(fd: RawFd) -> Result<()> {
		// SAFETY: fd is valid for the duration of the call.
		let flags = unsafe { libc::fcntl(fd, libc::F_GETFL, 0) };
		if flags < 0 {
			return Err(io::Error::last_os_error().into());
		}
		// SAFETY: fd is valid and flags came from a successful F_GETFL call for
		// the same descriptor, with O_NONBLOCK ORed into the existing bitmask.
		let ret = unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) };
		if ret < 0 {
			return Err(io::Error::last_os_error().into());
		}
		Ok(())
	}
}

#[cfg(target_os = "macos")]
mod platform {
	use std::{
		ffi::{CStr, CString, c_char, c_int, c_void},
		io,
		os::unix::io::{AsRawFd, RawFd},
		ptr,
	};

	use block::RcBlock;

	use crate::{
		os::EventFd,
		result::{Result, err},
		tap::VNET_HDR_SIZE,
	};

	type DispatchQueue = *mut c_void;
	type InterfaceRef = *mut c_void;
	type InterfaceEvent = u32;
	type VmnetReturn = u32;
	type XpcObject = *mut c_void;

	const EFD_NONBLOCK: i32 = 0x800;
	const VMNET_SHARED_MODE: u64 = 1001;
	const VMNET_INTERFACE_PACKETS_AVAILABLE: InterfaceEvent = 1 << 0;
	const VMNET_SUCCESS: VmnetReturn = 1000;
	const VMNET_FAILURE: VmnetReturn = 1001;
	const VMNET_MEM_FAILURE: VmnetReturn = 1002;
	const VMNET_INVALID_ARGUMENT: VmnetReturn = 1003;
	const VMNET_SETUP_INCOMPLETE: VmnetReturn = 1004;
	const VMNET_INVALID_ACCESS: VmnetReturn = 1005;
	const VMNET_PACKET_TOO_BIG: VmnetReturn = 1006;
	const VMNET_BUFFER_EXHAUSTED: VmnetReturn = 1007;
	const VMNET_TOO_MANY_PACKETS: VmnetReturn = 1008;
	const VMNET_SHARING_SERVICE_BUSY: VmnetReturn = 1009;
	const VMNET_NOT_AUTHORIZED: VmnetReturn = 1010;
	const DEFAULT_START_ADDRESS: &str = "192.168.249.1";
	const DEFAULT_END_ADDRESS: &str = "192.168.249.254";
	const DEFAULT_SUBNET_MASK: &str = "255.255.255.0";
	const DEFAULT_MTU: u64 = 1500;
	const MAX_PACKET_SIZE: u64 = 65_536;

	#[repr(C)]
	#[expect(clippy::struct_field_names, reason = "field names mirror vmnet's C vmpktdesc layout")]
	struct Vmpktdesc {
		vm_pkt_size:   libc::size_t,
		vm_pkt_iov:    *mut libc::iovec,
		vm_pkt_iovcnt: u32,
		vm_flags:      u32,
	}

	#[link(name = "vmnet", kind = "framework")]
	unsafe extern "C" {
		fn vmnet_start_interface(
			interface_desc: XpcObject,
			queue: DispatchQueue,
			handler: *mut c_void,
		) -> InterfaceRef;
		fn vmnet_interface_set_event_callback(
			interface: InterfaceRef,
			event_mask: InterfaceEvent,
			queue: DispatchQueue,
			callback: *mut c_void,
		) -> VmnetReturn;
		fn vmnet_read(
			interface: InterfaceRef,
			packets: *mut Vmpktdesc,
			pktcnt: *mut c_int,
		) -> VmnetReturn;
		fn vmnet_write(
			interface: InterfaceRef,
			packets: *mut Vmpktdesc,
			pktcnt: *mut c_int,
		) -> VmnetReturn;
		fn vmnet_stop_interface(
			interface: InterfaceRef,
			queue: DispatchQueue,
			handler: *mut c_void,
		) -> VmnetReturn;

		static vmnet_operation_mode_key: *const c_char;
		static vmnet_mac_address_key: *const c_char;
		static vmnet_allocate_mac_address_key: *const c_char;
		static vmnet_mtu_key: *const c_char;
		static vmnet_max_packet_size_key: *const c_char;
		static vmnet_start_address_key: *const c_char;
		static vmnet_end_address_key: *const c_char;
		static vmnet_subnet_mask_key: *const c_char;
	}

	unsafe extern "C" {
		fn dispatch_get_global_queue(identifier: isize, flags: usize) -> DispatchQueue;
		fn xpc_dictionary_create_empty() -> XpcObject;
		fn xpc_dictionary_set_bool(xdict: XpcObject, key: *const c_char, value: bool);
		fn xpc_dictionary_set_string(xdict: XpcObject, key: *const c_char, value: *const c_char);
		fn xpc_dictionary_set_uint64(xdict: XpcObject, key: *const c_char, value: u64);
		fn xpc_dictionary_get_string(xdict: XpcObject, key: *const c_char) -> *const c_char;
		fn xpc_release(object: XpcObject);
	}

	/// macOS vmnet shared-mode interface adapted to the TAP/vnet-header surface.
	pub struct Tap {
		interface:     InterfaceRef,
		queue:         DispatchQueue,
		ready_evt:     EventFd,
		virtio_header: bool,
		mac:           [u8; 6],
		_event_block:  RcBlock<(InterfaceEvent, XpcObject), ()>,
	}

	// SAFETY: vmnet interfaces are owned by one virtio device and every access
	// goes through the device mutex before the worker thread moves the device.
	unsafe impl Send for Tap {}

	impl Tap {
		/// Start a vmnet shared-mode interface for the guest MAC address.
		pub fn open(name: &str, mac: [u8; 6]) -> Result<Self> {
			// SAFETY: dispatch_get_global_queue is safe to call with the default
			// priority identifier and zero flags; it returns a process-wide queue.
			let queue = unsafe { dispatch_get_global_queue(0, 0) };
			let ready_evt = EventFd::new(EFD_NONBLOCK)
				.map_err(|e| err(format!("creating vmnet readiness eventfd: {e}")))?;
			let requested_mac = mac;
			let interface_desc = InterfaceDesc::new(requested_mac)?;
			let virtio_header = interface_desc.virtio_header;

			let status = start_interface(interface_desc.as_xpc(), queue)?;
			let interface = status.interface;
			if status.status != VMNET_SUCCESS {
				stop_interface(interface, queue);
				return Err(vmnet_error("starting vmnet shared interface", status.status, Some(name)));
			}
			let event_block = match install_packet_callback(interface, queue, &ready_evt) {
				Ok(block) => block,
				Err(e) => {
					stop_interface(interface, queue);
					return Err(e);
				},
			};
			let tap = Self {
				interface,
				queue,
				ready_evt,
				virtio_header,
				mac: status.mac.unwrap_or(requested_mac),
				_event_block: event_block,
			};
			Ok(tap)
		}

		/// Read one vnet-header-prefixed Ethernet frame from vmnet.
		pub fn read(&self, buf: &mut [u8]) -> io::Result<usize> {
			let _ = self.ready_evt.read();
			if self.virtio_header {
				return read_vmnet_packet(self.interface, buf);
			}
			if buf.len() < VNET_HDR_SIZE {
				return Err(io::Error::new(
					io::ErrorKind::InvalidInput,
					"virtio-net RX buffer is smaller than the vnet header",
				));
			}
			let n = read_vmnet_packet(self.interface, &mut buf[VNET_HDR_SIZE..])?;
			buf[..VNET_HDR_SIZE].fill(0);
			Ok(n + VNET_HDR_SIZE)
		}

		/// Write one vnet-header-prefixed Ethernet frame to vmnet.
		pub fn write(&self, buf: &[u8]) -> io::Result<usize> {
			if self.virtio_header {
				return write_vmnet_packet(self.interface, buf);
			}
			if buf.len() < VNET_HDR_SIZE {
				return Err(io::Error::new(
					io::ErrorKind::InvalidInput,
					"virtio-net TX buffer is smaller than the vnet header",
				));
			}
			let n = write_vmnet_packet(self.interface, &buf[VNET_HDR_SIZE..])?;
			Ok(n + VNET_HDR_SIZE)
		}

		/// Reject TAP offloads; vmnet does not expose checksum/GSO metadata here.
		#[expect(
			clippy::unused_self,
			reason = "all net backends expose the same instance-shaped offload API"
		)]
		pub fn set_offloads(&self, offloads: u32) -> Result<()> {
			if offloads == 0 {
				Ok(())
			} else {
				Err(err(format!("vmnet does not support negotiated TAP offloads {offloads:#x}")))
			}
		}

		/// Return vmnet offload flags this backend can safely advertise.
		#[expect(
			clippy::unused_self,
			reason = "all net backends expose the same instance-shaped offload API"
		)]
		pub const fn supported_offloads(&self) -> u32 {
			0
		}

		/// Return the MAC address vmnet expects the guest to use.
		pub const fn mac(&self) -> [u8; 6] {
			self.mac
		}

		/// Return the readiness fd polled by the virtio worker.
		pub fn as_raw_fd(&self) -> RawFd {
			self.ready_evt.as_raw_fd()
		}
	}

	impl Drop for Tap {
		fn drop(&mut self) {
			// SAFETY: self.interface is the live vmnet interface owned by this Tap,
			// and null queue/callback unregisters packet notifications before stop.
			let _ = unsafe {
				vmnet_interface_set_event_callback(
					self.interface,
					VMNET_INTERFACE_PACKETS_AVAILABLE,
					ptr::null_mut(),
					ptr::null_mut(),
				)
			};
			stop_interface(self.interface, self.queue);
		}
	}

	struct InterfaceDesc {
		xpc:           XpcObject,
		virtio_header: bool,
		strings:       Vec<CString>,
	}

	impl InterfaceDesc {
		fn new(mac: [u8; 6]) -> Result<Self> {
			// SAFETY: xpc_dictionary_create_empty returns a retained XPC object or
			// null; the result is checked before it is passed to other XPC calls.
			let xpc = unsafe { xpc_dictionary_create_empty() };
			if xpc.is_null() {
				return Err(err("creating vmnet interface descriptor failed"));
			}
			let mut desc = Self { xpc, virtio_header: false, strings: Vec::new() };
			// SAFETY: vmnet_operation_mode_key is a process-provided static C key.
			desc.set_uint64(unsafe { vmnet_operation_mode_key }, VMNET_SHARED_MODE);
			// SAFETY: vmnet_mtu_key is a process-provided static C key.
			desc.set_uint64(unsafe { vmnet_mtu_key }, DEFAULT_MTU);
			// SAFETY: vmnet_max_packet_size_key is a process-provided static C key.
			desc.set_uint64(unsafe { vmnet_max_packet_size_key }, MAX_PACKET_SIZE);
			// SAFETY: vmnet_start_address_key is a process-provided static C key.
			desc.set_string(unsafe { vmnet_start_address_key }, DEFAULT_START_ADDRESS)?;
			// SAFETY: vmnet_end_address_key is a process-provided static C key.
			desc.set_string(unsafe { vmnet_end_address_key }, DEFAULT_END_ADDRESS)?;
			// SAFETY: vmnet_subnet_mask_key is a process-provided static C key.
			desc.set_string(unsafe { vmnet_subnet_mask_key }, DEFAULT_SUBNET_MASK)?;
			let mac_address = format!(
				"{:02x}:{:02x}:{:02x}:{:02x}:{:02x}:{:02x}",
				mac[0], mac[1], mac[2], mac[3], mac[4], mac[5],
			);
			// SAFETY: vmnet_mac_address_key is a process-provided static C key.
			desc.set_string(unsafe { vmnet_mac_address_key }, &mac_address)?;
			// SAFETY: vmnet_allocate_mac_address_key is a process-provided static C key.
			desc.set_bool(unsafe { vmnet_allocate_mac_address_key }, false);
			if let Some(key) = dynamic_vmnet_key(b"vmnet_enable_virtio_header_key\0") {
				desc.set_bool(key, true);
				desc.virtio_header = true;
			}
			Ok(desc)
		}

		const fn as_xpc(&self) -> XpcObject {
			self.xpc
		}

		fn set_bool(&self, key: *const c_char, value: bool) {
			// SAFETY: self.xpc is a live mutable dictionary retained by InterfaceDesc,
			// key is a valid vmnet C string key, and value is copied by XPC.
			unsafe { xpc_dictionary_set_bool(self.xpc, key, value) };
		}

		fn set_uint64(&self, key: *const c_char, value: u64) {
			// SAFETY: self.xpc is a live mutable dictionary retained by InterfaceDesc,
			// key is a valid vmnet C string key, and value is copied by XPC.
			unsafe { xpc_dictionary_set_uint64(self.xpc, key, value) };
		}

		fn set_string(&mut self, key: *const c_char, value: &str) -> Result<()> {
			let value = CString::new(value)
				.map_err(|_| err(format!("vmnet string value {value:?} contains NUL")))?;
			// SAFETY: self.xpc is a live mutable dictionary, key is a valid vmnet C
			// string key, and value.as_ptr() remains alive because value is stored.
			unsafe { xpc_dictionary_set_string(self.xpc, key, value.as_ptr()) };
			self.strings.push(value);
			Ok(())
		}
	}

	impl Drop for InterfaceDesc {
		fn drop(&mut self) {
			// SAFETY: self.xpc is the retained XPC object created for this descriptor
			// and has not been released elsewhere.
			unsafe { xpc_release(self.xpc) };
		}
	}

	struct StartStatus {
		interface: InterfaceRef,
		status:    VmnetReturn,
		mac:       Option<[u8; 6]>,
	}

	fn start_interface(xpc: XpcObject, queue: DispatchQueue) -> Result<StartStatus> {
		let (tx, rx) = flume::bounded(1);
		let block = block::ConcreteBlock::new(move |status: VmnetReturn, params: XpcObject| {
			let _ = tx.send((status, vmnet_param_mac(params)));
		});
		let block = block.copy();
		let interface = {
			// SAFETY: xpc and queue are live for the call, and block is copied so the
			// Objective-C callback object remains valid until completion is received.
			unsafe { vmnet_start_interface(xpc, queue, &*block as *const _ as *mut c_void) }
		};
		if interface.is_null() {
			return Err(err(
				"vmnet_start_interface returned null (requires root and com.apple.vm.networking)",
			));
		}
		let (status, mac) = rx
			.recv()
			.map_err(|e| err(format!("waiting for vmnet start completion: {e}")))?;
		Ok(StartStatus { interface, status, mac })
	}

	fn install_packet_callback(
		interface: InterfaceRef,
		queue: DispatchQueue,
		ready_evt: &EventFd,
	) -> Result<RcBlock<(InterfaceEvent, XpcObject), ()>> {
		let wake = ready_evt
			.try_clone()
			.map_err(|e| err(format!("cloning vmnet readiness eventfd: {e}")))?;
		let block = block::ConcreteBlock::new(move |events: InterfaceEvent, _event: XpcObject| {
			if events & VMNET_INTERFACE_PACKETS_AVAILABLE != 0 {
				let _ = wake.write(1);
			}
		});
		let block = block.copy();
		// SAFETY: interface and queue belong to the live Tap, and block is copied
		// so vmnet can retain the callback for the interface lifetime.
		let status = unsafe {
			vmnet_interface_set_event_callback(
				interface,
				VMNET_INTERFACE_PACKETS_AVAILABLE,
				queue,
				&*block as *const _ as *mut c_void,
			)
		};
		vmnet_status(status, "installing vmnet packet callback")?;
		Ok(block)
	}

	fn read_vmnet_packet(interface: InterfaceRef, buf: &mut [u8]) -> io::Result<usize> {
		let mut iov = libc::iovec { iov_base: buf.as_mut_ptr().cast(), iov_len: buf.len() };
		let mut packet = Vmpktdesc {
			vm_pkt_size:   iov.iov_len,
			vm_pkt_iov:    &mut iov,
			vm_pkt_iovcnt: 1,
			vm_flags:      0,
		};
		let mut count = 1;
		// SAFETY: interface is a live vmnet handle, packet points to one writable
		// iovec covering buf, and count points to the initialized packet count.
		let status = unsafe { vmnet_read(interface, &mut packet, &mut count) };
		vmnet_io_status(status)?;
		if count == 0 {
			return Err(io::Error::from(io::ErrorKind::WouldBlock));
		}
		Ok(packet.vm_pkt_size)
	}

	fn write_vmnet_packet(interface: InterfaceRef, buf: &[u8]) -> io::Result<usize> {
		let mut iov =
			libc::iovec { iov_base: buf.as_ptr().cast::<c_void>().cast_mut(), iov_len: buf.len() };
		let mut packet = Vmpktdesc {
			vm_pkt_size:   iov.iov_len,
			vm_pkt_iov:    &mut iov,
			vm_pkt_iovcnt: 1,
			vm_flags:      0,
		};
		let mut count = 1;
		// SAFETY: interface is a live vmnet handle, packet points to one readable
		// iovec covering buf, and count points to the initialized packet count.
		let status = unsafe { vmnet_write(interface, &mut packet, &mut count) };
		vmnet_io_status(status)?;
		if count == 0 {
			return Err(io::Error::from(io::ErrorKind::WouldBlock));
		}
		Ok(packet.vm_pkt_size)
	}

	fn stop_interface(interface: InterfaceRef, queue: DispatchQueue) {
		let (tx, rx) = flume::bounded(1);
		let block = block::ConcreteBlock::new(move |status: VmnetReturn| {
			let _ = tx.send(status);
		});
		let block = block.copy();
		let status = {
			// SAFETY: interface and queue identify the live vmnet interface being
			// stopped, and block remains alive until the completion status is read.
			unsafe { vmnet_stop_interface(interface, queue, &*block as *const _ as *mut c_void) }
		};
		if status == VMNET_SUCCESS {
			let _ = rx.recv();
		}
	}

	fn vmnet_param_mac(params: XpcObject) -> Option<[u8; 6]> {
		if params.is_null() {
			return None;
		}
		// SAFETY: params is a non-null XPC dictionary from vmnet, and the vmnet
		// MAC address key is a process-provided static C string.
		let value = unsafe { xpc_dictionary_get_string(params, vmnet_mac_address_key) };
		if value.is_null() {
			return None;
		}
		// SAFETY: value is a non-null NUL-terminated C string owned by params and
		// remains valid while params is alive for this callback.
		let value = unsafe { CStr::from_ptr(value) };
		parse_mac(&value.to_string_lossy())
	}

	fn parse_mac(s: &str) -> Option<[u8; 6]> {
		let mut mac = [0u8; 6];
		let mut parts = s.split(':');
		for byte in &mut mac {
			*byte = u8::from_str_radix(parts.next()?, 16).ok()?;
		}
		if parts.next().is_none() {
			Some(mac)
		} else {
			None
		}
	}

	fn dynamic_vmnet_key(symbol: &[u8]) -> Option<*const c_char> {
		// SAFETY: symbol is supplied by callers as a NUL-terminated byte string,
		// and RTLD_DEFAULT searches already-loaded process images.
		let ptr = unsafe { libc::dlsym(libc::RTLD_DEFAULT, symbol.as_ptr().cast()) };
		if ptr.is_null() {
			return None;
		}
		// SAFETY: dlsym returned a non-null address for a vmnet key symbol, whose
		// ABI is a pointer to a static C string key.
		let key = unsafe { *(ptr as *const *const c_char) };
		if key.is_null() { None } else { Some(key) }
	}

	fn vmnet_status(status: VmnetReturn, op: &str) -> Result<()> {
		if status == VMNET_SUCCESS {
			Ok(())
		} else {
			Err(vmnet_error(op, status, None))
		}
	}

	fn vmnet_error(op: &str, status: VmnetReturn, name: Option<&str>) -> crate::result::Error {
		let target = name.map_or(String::new(), |name| format!(" for --tap {name:?}"));
		let hint = match status {
			VMNET_INVALID_ACCESS | VMNET_NOT_AUTHORIZED => {
				" (requires root and com.apple.vm.networking entitlement)"
			},
			_ => "",
		};
		err(format!("{op}{target}: {}{hint}", status_name(status)))
	}

	fn vmnet_io_status(status: VmnetReturn) -> io::Result<()> {
		match status {
			VMNET_SUCCESS => Ok(()),
			VMNET_PACKET_TOO_BIG => {
				Err(io::Error::new(io::ErrorKind::InvalidInput, status_name(status)))
			},
			VMNET_BUFFER_EXHAUSTED => Err(io::Error::from(io::ErrorKind::WouldBlock)),
			_ => Err(io::Error::other(status_name(status))),
		}
	}

	const fn status_name(status: VmnetReturn) -> &'static str {
		match status {
			VMNET_SUCCESS => "VMNET_SUCCESS",
			VMNET_FAILURE => "VMNET_FAILURE",
			VMNET_MEM_FAILURE => "VMNET_MEM_FAILURE",
			VMNET_INVALID_ARGUMENT => "VMNET_INVALID_ARGUMENT",
			VMNET_SETUP_INCOMPLETE => "VMNET_SETUP_INCOMPLETE",
			VMNET_INVALID_ACCESS => "VMNET_INVALID_ACCESS",
			VMNET_PACKET_TOO_BIG => "VMNET_PACKET_TOO_BIG",
			VMNET_BUFFER_EXHAUSTED => "VMNET_BUFFER_EXHAUSTED",
			VMNET_TOO_MANY_PACKETS => "VMNET_TOO_MANY_PACKETS",
			VMNET_SHARING_SERVICE_BUSY => "VMNET_SHARING_SERVICE_BUSY",
			VMNET_NOT_AUTHORIZED => "VMNET_NOT_AUTHORIZED",
			_ => "VMNET_UNKNOWN_STATUS",
		}
	}
}

#[cfg(target_os = "windows")]
mod platform {
	use std::{
		ffi::c_void,
		io,
		os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle, RawHandle},
		ptr,
	};

	use parking_lot::Mutex;
	use windows_sys::Win32::{
		Foundation::{
			ERROR_IO_INCOMPLETE, ERROR_IO_PENDING, GENERIC_READ, GENERIC_WRITE, HANDLE,
			INVALID_HANDLE_VALUE,
		},
		Storage::FileSystem::{
			CreateFileW, FILE_ATTRIBUTE_SYSTEM, FILE_FLAG_OVERLAPPED, FILE_SHARE_READ,
			FILE_SHARE_WRITE, OPEN_EXISTING, ReadFile, WriteFile,
		},
		System::{
			IO::{CancelIoEx, DeviceIoControl, GetOverlappedResult, OVERLAPPED},
			Threading::{CreateEventW, ResetEvent},
		},
	};

	use crate::{
		result::{Result, err},
		tap::VNET_HDR_SIZE,
	};

	const TAP_IOCTL_SET_MEDIA_STATUS: u32 = 0x0022_000c;
	const TAP_FRAME_CAPACITY: usize = 65_536;

	struct OverlappedOp {
		event:      OwnedHandle,
		overlapped: Box<OVERLAPPED>,
	}

	#[allow(
		clippy::non_send_fields_in_send_ty,
		reason = "OVERLAPPED is pinned in a Box and all access is serialized until completion"
	)]
	// SAFETY: the boxed `OVERLAPPED` stays at a stable address, its embedded raw
	// pointer is always null, and callers serialize every access to an operation.
	unsafe impl Send for OverlappedOp {}

	impl OverlappedOp {
		fn new() -> io::Result<Self> {
			// SAFETY: null security attributes/name create a private,
			// non-inheritable manual-reset event.
			let event = unsafe { CreateEventW(ptr::null(), 1, 0, ptr::null()) };
			if event.is_null() {
				return Err(io::Error::last_os_error());
			}
			// SAFETY: `CreateEventW` returned a fresh owned handle.
			let event = unsafe { OwnedHandle::from_raw_handle(event as RawHandle) };
			let mut op = Self { event, overlapped: Box::new(OVERLAPPED::default()) };
			op.reset()?;
			Ok(op)
		}

		fn reset(&mut self) -> io::Result<()> {
			// SAFETY: `event` is a live event handle.
			if unsafe { ResetEvent(self.event.as_raw_handle() as HANDLE) } == 0 {
				return Err(io::Error::last_os_error());
			}
			*self.overlapped = OVERLAPPED::default();
			self.overlapped.hEvent = self.event.as_raw_handle() as HANDLE;
			Ok(())
		}

		fn as_mut_ptr(&mut self) -> *mut OVERLAPPED {
			std::ptr::from_mut(self.overlapped.as_mut())
		}

		fn complete(&self, handle: HANDLE, wait: bool) -> io::Result<usize> {
			let mut transferred = 0u32;
			// SAFETY: `handle` and the event-backed `OVERLAPPED` remain live until
			// this operation has completed.
			let ok = unsafe {
				GetOverlappedResult(handle, self.overlapped.as_ref(), &mut transferred, i32::from(wait))
			};
			if ok == 0 {
				let error = io::Error::last_os_error();
				if error.raw_os_error() == Some(ERROR_IO_INCOMPLETE as i32) {
					return Err(io::Error::from(io::ErrorKind::WouldBlock));
				}
				return Err(error);
			}
			Ok(transferred as usize)
		}
	}

	struct ReadState {
		op:      OverlappedOp,
		frame:   Box<[u8]>,
		pending: bool,
	}

	impl ReadState {
		fn new() -> io::Result<Self> {
			Ok(Self {
				op:      OverlappedOp::new()?,
				frame:   vec![0; TAP_FRAME_CAPACITY].into_boxed_slice(),
				pending: false,
			})
		}

		fn arm(&mut self, handle: HANDLE) -> io::Result<()> {
			self.op.reset()?;
			let length = u32::try_from(self.frame.len())
				.map_err(|_| io::Error::from(io::ErrorKind::InvalidInput))?;
			// SAFETY: the boxed frame and boxed `OVERLAPPED` remain at stable
			// addresses until completion; `Tap::drop` drains a pending operation.
			let started = unsafe {
				ReadFile(handle, self.frame.as_mut_ptr(), length, ptr::null_mut(), self.op.as_mut_ptr())
			};
			accept_overlapped_start(started)?;
			self.pending = true;
			Ok(())
		}
	}

	/// TAP-Windows adapter with one continuously pending packet read.
	pub struct Tap {
		handle: OwnedHandle,
		read:   Mutex<ReadState>,
		write:  Mutex<OverlappedOp>,
		mac:    [u8; 6],
	}

	impl Tap {
		/// Open a TAP-Windows adapter. `name` may be a full device path or an
		/// adapter GUID, in which case the standard TAP-Windows path is built.
		pub fn open(name: &str, mac: [u8; 6]) -> Result<Self> {
			let path = if name.starts_with(r"\\.\Global\") {
				name.to_owned()
			} else {
				format!(r"\\.\Global\{name}.tap")
			};
			let wide: Vec<u16> = path.encode_utf16().chain(Some(0)).collect();
			// SAFETY: `wide` is NUL-terminated and all optional pointers are null.
			let handle = unsafe {
				CreateFileW(
					wide.as_ptr(),
					GENERIC_READ | GENERIC_WRITE,
					FILE_SHARE_READ | FILE_SHARE_WRITE,
					ptr::null(),
					OPEN_EXISTING,
					FILE_ATTRIBUTE_SYSTEM | FILE_FLAG_OVERLAPPED,
					ptr::null_mut(),
				)
			};
			if handle == INVALID_HANDLE_VALUE {
				return Err(err(format!(
					"opening TAP-Windows adapter {name:?}: {}",
					io::Error::last_os_error()
				)));
			}
			// SAFETY: `CreateFileW` returned a fresh owned handle.
			let handle = unsafe { OwnedHandle::from_raw_handle(handle as RawHandle) };
			let mut write = OverlappedOp::new()?;
			set_media_status(handle.as_raw_handle() as HANDLE, &mut write)?;
			let mut read = ReadState::new()?;
			read.arm(handle.as_raw_handle() as HANDLE)?;
			Ok(Self { handle, read: Mutex::new(read), write: Mutex::new(write), mac })
		}

		/// Receive one Ethernet frame with a synthesized virtio-net header.
		pub fn read(&self, buf: &mut [u8]) -> io::Result<usize> {
			if buf.len() < VNET_HDR_SIZE {
				return Err(io::Error::new(
					io::ErrorKind::InvalidInput,
					"virtio-net receive buffer is smaller than its header",
				));
			}
			let handle = self.handle.as_raw_handle() as HANDLE;
			let mut read = self.read.lock();
			if !read.pending {
				read.arm(handle)?;
			}
			let frame_len = read.op.complete(handle, false)?;
			read.pending = false;
			let packet_len = VNET_HDR_SIZE
				.checked_add(frame_len)
				.ok_or_else(|| io::Error::from(io::ErrorKind::InvalidData))?;
			let copy_result = if packet_len > buf.len() {
				Err(io::Error::new(
					io::ErrorKind::InvalidData,
					format!("TAP-Windows frame of {frame_len} bytes exceeds receive buffer"),
				))
			} else {
				buf[..VNET_HDR_SIZE].fill(0);
				buf[VNET_HDR_SIZE..packet_len].copy_from_slice(&read.frame[..frame_len]);
				Ok(packet_len)
			};
			read.arm(handle)?;
			copy_result
		}

		/// Send one Ethernet frame after removing its virtio-net header.
		pub fn write(&self, buf: &[u8]) -> io::Result<usize> {
			let frame = buf.get(VNET_HDR_SIZE..).ok_or_else(|| {
				io::Error::new(
					io::ErrorKind::InvalidInput,
					"virtio-net transmit buffer is smaller than its header",
				)
			})?;
			let length =
				u32::try_from(frame.len()).map_err(|_| io::Error::from(io::ErrorKind::InvalidInput))?;
			let handle = self.handle.as_raw_handle() as HANDLE;
			let mut op = self.write.lock();
			op.reset()?;
			// SAFETY: `frame` and the boxed `OVERLAPPED` remain live until the
			// synchronous completion wait below returns.
			let started =
				unsafe { WriteFile(handle, frame.as_ptr(), length, ptr::null_mut(), op.as_mut_ptr()) };
			accept_overlapped_start(started)?;
			let written = op.complete(handle, true)?;
			Ok(VNET_HDR_SIZE + written)
		}

		/// Configure negotiated offloads; TAP-Windows accepts raw frames only.
		#[allow(
			clippy::unused_self,
			reason = "network backends expose one instance-based offload interface"
		)]
		pub fn set_offloads(&self, offloads: u32) -> Result<()> {
			if offloads == 0 {
				Ok(())
			} else {
				Err(err("TAP-Windows does not expose virtio offload metadata"))
			}
		}

		/// Return the TAP offload mask exposed to the virtio guest.
		#[allow(
			clippy::unused_self,
			reason = "network backends expose one instance-based offload interface"
		)]
		pub const fn supported_offloads(&self) -> u32 {
			0
		}

		/// Return the guest-visible MAC address.
		pub const fn mac(&self) -> [u8; 6] {
			self.mac
		}

		/// Return the event signaled when the pending packet read completes.
		pub fn as_raw_handle(&self) -> RawHandle {
			self.read.lock().op.event.as_raw_handle()
		}
	}

	impl Drop for Tap {
		fn drop(&mut self) {
			let read = self.read.get_mut();
			if !read.pending {
				return;
			}
			let handle = self.handle.as_raw_handle() as HANDLE;
			// SAFETY: both handles and the pending `OVERLAPPED` are live. Waiting
			// after cancellation prevents Windows from retaining the frame buffer.
			unsafe {
				CancelIoEx(handle, read.op.overlapped.as_ref());
			}
			let _ = read.op.complete(handle, true);
			read.pending = false;
		}
	}

	fn set_media_status(handle: HANDLE, op: &mut OverlappedOp) -> io::Result<()> {
		let enabled = 1u32;
		op.reset()?;
		// SAFETY: the input and boxed `OVERLAPPED` remain live until completion.
		let started = unsafe {
			DeviceIoControl(
				handle,
				TAP_IOCTL_SET_MEDIA_STATUS,
				std::ptr::from_ref(&enabled).cast::<c_void>(),
				std::mem::size_of_val(&enabled) as u32,
				ptr::null_mut(),
				0,
				ptr::null_mut(),
				op.as_mut_ptr(),
			)
		};
		accept_overlapped_start(started)?;
		op.complete(handle, true)?;
		Ok(())
	}

	fn accept_overlapped_start(started: i32) -> io::Result<()> {
		if started != 0 {
			return Ok(());
		}
		let error = io::Error::last_os_error();
		if error.raw_os_error() == Some(ERROR_IO_PENDING as i32) {
			Ok(())
		} else {
			Err(error)
		}
	}
}

pub use platform::Tap;
