//! Host interface addresses: which IPs a process advertises to its peers, and
//! how local a peer's address is from this host (ARCHITECTURE.md §5.2).
//!
//! Every member advertises all of its reachable IPs; peers rank them with
//! [`rank`] (same subnet first) and gossip fails over between them, so a
//! member stays reachable while any one of its addresses is.

use std::{
	collections::HashSet,
	ffi::CStr,
	mem::{MaybeUninit, size_of},
	net::{IpAddr, Ipv4Addr, Ipv6Addr, UdpSocket},
};

/// One address assigned to an up local interface.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Interface {
	/// Interface name (`eth0`, `en0`, `tailscale0`).
	pub name:   String,
	pub ip:     IpAddr,
	/// Network prefix length of the address (from its netmask).
	pub prefix: u8,
}

impl Interface {
	/// Whether `ip` lies in this interface's network.
	pub fn contains(&self, ip: IpAddr) -> bool {
		match (self.ip, ip) {
			(IpAddr::V4(net), IpAddr::V4(ip)) => {
				let mask = u32::MAX
					.checked_shl(32 - u32::from(self.prefix.min(32)))
					.unwrap_or(0);
				u32::from(net) & mask == u32::from(ip) & mask
			},
			(IpAddr::V6(net), IpAddr::V6(ip)) => {
				let mask = u128::MAX
					.checked_shl(128 - u32::from(self.prefix.min(128)))
					.unwrap_or(0);
				u128::from(net) & mask == u128::from(ip) & mask
			},
			_ => false,
		}
	}

	/// Container/VM bridges and veths (`docker0`, `veth*`, `vmon*`, …): their
	/// addresses are host-local and often identical on every host.
	fn is_virtual(&self) -> bool {
		const PREFIXES: &[&str] = &[
			"docker", "br-", "veth", "virbr", "vmon", "cni", "flannel", "cali", "vxlan", "lxc", "lxd",
			"podman", "tap", "vnet", "bridge",
		];
		PREFIXES.iter().any(|prefix| self.name.starts_with(prefix))
	}
}

/// Every address of every up interface (`getifaddrs`); empty when the call
/// fails.
pub fn interfaces() -> Vec<Interface> {
	let mut head: *mut libc::ifaddrs = std::ptr::null_mut();
	// SAFETY: getifaddrs stores a list head in `head`, freed below.
	if unsafe { libc::getifaddrs(&raw mut head) } != 0 {
		return Vec::new();
	}
	let mut out = Vec::new();
	let mut cursor = head;
	while !cursor.is_null() {
		// SAFETY: `cursor` is a node of the list getifaddrs returned; the list
		// stays alive until freeifaddrs.
		let entry = unsafe { &*cursor };
		cursor = entry.ifa_next;
		if entry.ifa_flags & libc::IFF_UP.cast_unsigned() == 0 {
			continue;
		}
		// SAFETY: ifa_addr is null or a sockaddr of its own family.
		let Some(ip) = (unsafe { sockaddr_ip(entry.ifa_addr, None) }) else {
			continue;
		};
		// SAFETY: ifa_netmask is null or a sockaddr (possibly truncated, and on
		// BSDs without a family) holding a mask of the address's family.
		let mask = unsafe { sockaddr_ip(entry.ifa_netmask, Some(ip)) };
		let prefix = match mask {
			Some(IpAddr::V4(mask)) => u32::from(mask).leading_ones(),
			Some(IpAddr::V6(mask)) => u128::from(mask).leading_ones(),
			None if ip.is_ipv4() => 32,
			None => 128,
		};
		// SAFETY: ifa_name is a NUL-terminated string owned by the list.
		let name = unsafe { CStr::from_ptr(entry.ifa_name) }
			.to_string_lossy()
			.into_owned();
		out.push(Interface { name, ip, prefix: u8::try_from(prefix).unwrap_or(128) });
	}
	// SAFETY: `head` came from getifaddrs and is freed exactly once.
	unsafe { libc::freeifaddrs(head) };
	out
}

/// IP of the sockaddr at `addr`, read as the family of `like` when given
/// (netmasks), else as its own `sa_family`.
///
/// # Safety
///
/// `addr` is null or points to a sockaddr whose readable length is its
/// `sa_len` (BSDs) or the full struct of its family (Linux).
unsafe fn sockaddr_ip(addr: *const libc::sockaddr, like: Option<IpAddr>) -> Option<IpAddr> {
	if addr.is_null() {
		return None;
	}
	let family = match like {
		Some(IpAddr::V4(_)) => libc::AF_INET,
		Some(IpAddr::V6(_)) => libc::AF_INET6,
		// SAFETY: non-null per the caller; sa_family is within every sockaddr.
		None => i32::from(unsafe { (*addr).sa_family }),
	};
	match family {
		libc::AF_INET => {
			// SAFETY: forwarded caller contract.
			let sin: libc::sockaddr_in = unsafe { read_sockaddr(addr) };
			Some(IpAddr::V4(Ipv4Addr::from(u32::from_be(sin.sin_addr.s_addr))))
		},
		libc::AF_INET6 => {
			// SAFETY: forwarded caller contract.
			let sin6: libc::sockaddr_in6 = unsafe { read_sockaddr(addr) };
			Some(IpAddr::V6(Ipv6Addr::from(sin6.sin6_addr.s6_addr)))
		},
		_ => None,
	}
}

/// Copies the sockaddr at `addr` into a zeroed `T`. BSD netmasks may be
/// shorter than their struct, so the copy is bounded by `sa_len` there.
///
/// # Safety
///
/// Same as [`sockaddr_ip`]; `T` is a plain-old-data sockaddr struct.
unsafe fn read_sockaddr<T>(addr: *const libc::sockaddr) -> T {
	let mut out = MaybeUninit::<T>::zeroed();
	#[cfg(any(target_os = "macos", target_os = "ios", target_os = "freebsd"))]
	// SAFETY: non-null per the caller; sa_len is the first byte of a BSD sockaddr.
	let len = usize::from(unsafe { (*addr).sa_len }).min(size_of::<T>());
	#[cfg(not(any(target_os = "macos", target_os = "ios", target_os = "freebsd")))]
	let len = size_of::<T>();
	// SAFETY: `len` readable bytes at `addr` (caller contract), `len` ≤ the
	// size of `out`; the regions do not overlap.
	unsafe { std::ptr::copy_nonoverlapping(addr.cast::<u8>(), out.as_mut_ptr().cast::<u8>(), len) };
	// SAFETY: sockaddr structs are valid for any bit pattern, including zero.
	unsafe { out.assume_init() }
}

/// IPs peers may reach a wildcard bind of `bind`'s family at.
///
/// Every interface address except loopback, link-local, and container/VM
/// bridges, with the default route's source IP first, then IPv4 before IPv6.
/// An IPv4 wildcard (`0.0.0.0`) only accepts IPv4; `[::]` accepts both.
/// Loopback only when the host has nothing else.
pub fn advertisable(bind: IpAddr) -> Vec<IpAddr> {
	let ipv6 = bind.is_ipv6();
	let mut seen = HashSet::new();
	let mut ips: Vec<IpAddr> = interfaces()
		.into_iter()
		.filter(|iface| !iface.is_virtual() && is_routable(iface.ip))
		.filter(|iface| ipv6 || iface.ip.is_ipv4())
		.map(|iface| iface.ip)
		.filter(|ip| seen.insert(*ip))
		.collect();
	let primary = default_route_ip();
	ips.sort_by_key(|ip| (Some(*ip) != primary, ip.is_ipv6()));
	if ips.is_empty() {
		ips.push(if ipv6 {
			IpAddr::V6(Ipv6Addr::LOCALHOST)
		} else {
			IpAddr::V4(Ipv4Addr::LOCALHOST)
		});
	}
	ips
}

/// Source IP the kernel picks for the IPv4 default route, if any.
fn default_route_ip() -> Option<IpAddr> {
	let socket = UdpSocket::bind((Ipv4Addr::UNSPECIFIED, 0)).ok()?;
	socket.connect((Ipv4Addr::new(192, 0, 2, 1), 9)).ok()?;
	Some(socket.local_addr().ok()?.ip())
}

/// Unicast, non-loopback, non-link-local.
const fn is_routable(ip: IpAddr) -> bool {
	match ip {
		IpAddr::V4(ip) => {
			!(ip.is_loopback()
				|| ip.is_unspecified()
				|| ip.is_link_local()
				|| ip.is_multicast()
				|| ip.is_broadcast())
		},
		IpAddr::V6(ip) => {
			!(ip.is_loopback()
				|| ip.is_unspecified()
				|| ip.is_unicast_link_local()
				|| ip.is_multicast()
				|| ip.to_ipv4_mapped().is_some())
		},
	}
}

/// How far a peer address is from this host; peers are reached through their
/// lowest-ranked reachable address:
///
/// 0. one of this host's own IPs, or on a directly attached subnet (LAN);
/// 1. other private space (RFC 1918, IPv6 ULA): routed private networks;
/// 2. overlays: CGNAT `100.64.0.0/10` and Tailscale's `fd7a:115c:a1e0::/48`;
/// 3. anything else (public).
pub fn rank(ip: IpAddr, local: &[Interface]) -> u8 {
	let attached = local
		.iter()
		.any(|iface| iface.ip == ip || (!iface.is_virtual() && iface.contains(ip)));
	if attached {
		return 0;
	}
	match ip {
		IpAddr::V4(ip) => {
			let [a, b, ..] = ip.octets();
			if a == 100 && (64..128).contains(&b) {
				2
			} else if ip.is_private() {
				1
			} else {
				3
			}
		},
		IpAddr::V6(ip) => {
			let segments = ip.segments();
			if segments[..3] == [0xfd7a, 0x115c, 0xa1e0] {
				2
			} else if ip.is_unique_local() {
				1
			} else {
				3
			}
		},
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn iface(name: &str, ip: &str, prefix: u8) -> Interface {
		Interface { name: name.into(), ip: ip.parse().unwrap(), prefix }
	}

	#[test]
	fn rank_prefers_attached_then_private_then_overlay_then_public() {
		let local = [
			iface("br0", "192.168.1.69", 24),
			iface("tailscale0", "100.97.175.76", 32),
			iface("docker0", "172.17.0.1", 16),
		];
		let rank = |ip: &str| rank(ip.parse().unwrap(), &local);
		assert_eq!(rank("192.168.1.70"), 0, "same LAN subnet");
		assert_eq!(rank("100.97.175.76"), 0, "own address");
		assert_eq!(rank("10.200.108.18"), 1, "routed private");
		assert_eq!(rank("172.17.0.1"), 0, "own docker bridge address");
		assert_eq!(rank("172.17.0.2"), 1, "another host's docker subnet is not our LAN");
		assert_eq!(rank("100.127.23.30"), 2, "tailscale peer");
		assert_eq!(rank("fd7a:115c:a1e0::9034:171f"), 2, "tailscale IPv6");
		assert_eq!(rank("fd00::1"), 1, "ULA");
		assert_eq!(rank("93.115.27.162"), 3, "public");
		assert_eq!(rank("2a01:4f9:4b:1a08::2"), 3, "public IPv6");
	}

	#[test]
	fn contains_honours_prefix_edges() {
		assert!(iface("en0", "10.0.0.1", 0).contains("8.8.8.8".parse().unwrap()));
		assert!(!iface("en0", "10.0.0.1", 32).contains("10.0.0.2".parse().unwrap()));
		assert!(iface("en0", "fe80::1", 64).contains("fe80::2".parse().unwrap()));
		assert!(!iface("en0", "10.0.0.1", 8).contains("::ffff:10.0.0.2".parse().unwrap()));
	}

	#[test]
	fn advertisable_skips_loopback_and_respects_family() {
		let v4 = advertisable(IpAddr::V4(Ipv4Addr::UNSPECIFIED));
		assert!(v4.iter().all(IpAddr::is_ipv4), "{v4:?}");
		match v4.as_slice() {
			[] => panic!("advertisable returned nothing"),
			[only] => assert!(only.is_ipv4()),
			many => assert!(many.iter().all(|ip| !ip.is_loopback()), "{many:?}"),
		}
	}
}
