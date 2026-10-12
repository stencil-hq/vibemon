//! 16550A serial console backed by vm-superio.
//!
//! Output goes straight to the host stdout fd (unbuffered, so prompts without a
//! trailing newline still appear); input bytes are injected from a host stdin
//! reader thread via [`SerialDevice::enqueue`], which raises the COM1 IRQ.

use std::io::{self, Write};

use vm_superio::{
	Serial,
	serial::{NoEvents, SerialState},
};

use crate::{
	devices::BusDevice,
	hv::IrqLine,
	result::{Result, err},
};

const UART_REGISTER_COUNT: u64 = 8;

/// Unbuffered writer to the host stdout file descriptor (the console log),
/// shared by the UART and the virtio-console log port.
pub struct ConsoleOut;

impl Write for ConsoleOut {
	fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
		#[cfg(unix)]
		{
			loop {
				// SAFETY: `buf.as_ptr()` is valid for `buf.len()` bytes, stdout is
				// a process-global fd, and `write` does not retain the pointer.
				let n = unsafe { libc::write(libc::STDOUT_FILENO, buf.as_ptr().cast(), buf.len()) };
				if n >= 0 {
					return Ok(n as usize);
				}
				let err = io::Error::last_os_error();
				if err.kind() != io::ErrorKind::Interrupted {
					return Err(err);
				}
			}
		}
		#[cfg(target_os = "windows")]
		{
			std::io::stdout().write(buf)
		}
	}

	fn flush(&mut self) -> io::Result<()> {
		std::io::stdout().flush()
	}
}

/// A COM port exposed on the port-IO bus.
pub struct SerialDevice {
	inner: Serial<IrqLine, NoEvents, ConsoleOut>,
}

impl SerialDevice {
	/// Create a serial device that raises the supplied guest IRQ line.
	pub fn new(interrupt: IrqLine) -> Self {
		Self { inner: Serial::new(interrupt, ConsoleOut) }
	}

	/// Restore a serial device from state using the supplied guest IRQ line.
	pub fn from_state(interrupt: IrqLine, state: &SerialState) -> Result<Self> {
		let inner = Serial::from_state(state, interrupt, NoEvents, ConsoleOut)
			.map_err(|e| err(format!("restoring serial state: {e}")))?;
		Ok(Self { inner })
	}

	/// Snapshot the serial device registers and FIFOs.
	pub fn state(&self) -> SerialState {
		self.inner.state()
	}

	/// Feed host input bytes into the receive FIFO (raising RX interrupts).
	pub fn enqueue(&mut self, bytes: &[u8]) {
		let cap = self.inner.fifo_capacity();
		let n = bytes.len().min(cap);
		if n > 0 {
			let _ = self.inner.enqueue_raw_bytes(&bytes[..n]);
		}
	}
}

impl BusDevice for SerialDevice {
	fn read(&mut self, offset: u64, data: &mut [u8]) {
		data.fill(0);
		if offset < UART_REGISTER_COUNT
			&& let Some(b) = data.first_mut()
		{
			*b = self.inner.read(offset as u8);
		}
	}

	fn write(&mut self, offset: u64, data: &[u8]) {
		if offset < UART_REGISTER_COUNT
			&& let Some(b) = data.first()
		{
			let _ = self.inner.write(offset as u8, *b);
		}
	}
}
