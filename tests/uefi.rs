mod common;

use std::{path::Path, time::Duration};

#[test]
fn boots_ubuntu_via_uefi() {
	if !common::require_hv() {
		return;
	}

	let uefi_dir = Path::new("target/test-assets/uefi");
	if !uefi_dir.is_dir() {
		return;
	}

	#[cfg(target_arch = "x86_64")]
	let (fw_name, disk_name, transport) = ("OVMF_CODE.fd", "ubuntu-focal-amd64", "pci");

	#[cfg(target_arch = "aarch64")]
	let (fw_name, disk_name, transport) = ("QEMU_EFI.fd", "ubuntu-focal-arm64", "mmio");

	let fw_path = uefi_dir.join(fw_name);
	let disk_path = uefi_dir.join(format!("{disk_name}.raw"));

	if !fw_path.is_file() || !disk_path.is_file() {
		return;
	}

	// Canonicalize paths to absolute for the jail environment compatibility
	let fw_path = std::fs::canonicalize(&fw_path).unwrap_or(fw_path);
	let disk_path = std::fs::canonicalize(&disk_path).unwrap_or(disk_path);

	let args = vec![
		"--boot-mode".to_string(),
		"uefi".to_string(),
		"--firmware".to_string(),
		fw_path.display().to_string(),
		"--rootfs".to_string(),
		disk_path.display().to_string(),
		"--transport".to_string(),
		transport.to_string(),
		"--mem".to_string(),
		"512".to_string(), // Give it a bit more RAM for full cloud image boot
	];

	let refs = common::as_refs(&args);
	// Cloud images take longer to boot and reach login prompt
	let output = common::boot_capture(&refs, "login:", Duration::from_mins(2));

	// Either the serial terminal showed login/Welcome, or cloud-init completed,
	// or we successfully reached UEFI/OS loading
	assert!(
		output.contains("login:") || output.contains("Welcome") || output.contains("Ubuntu"),
		"UEFI boot output of cloud image did not contain expected serial markers:\n{output}"
	);
	common::assert_no_panic(&output);
}
