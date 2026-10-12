//! Time helpers for envd: RFC 3339 parsing (`/init.timestamp`), HTTP-date
//! formatting/parsing (`Last-Modified`, `If-Modified-Since`), and stepping
//! `CLOCK_REALTIME` from the host's timestamp.

use std::time::{SystemTime, UNIX_EPOCH};

/// Days since 1970-01-01 for a proleptic Gregorian date.
const fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
	let y = if m <= 2 { y - 1 } else { y };
	let era = if y >= 0 { y } else { y - 399 } / 400;
	let yoe = y - era * 400;
	let mp = (m + 9) % 12;
	let doy = (153 * mp + 2) / 5 + d - 1;
	let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
	era * 146_097 + doe - 719_468
}

/// (year, month, day) for days since 1970-01-01.
const fn civil_from_days(z: i64) -> (i64, i64, i64) {
	let z = z + 719_468;
	let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
	let doe = z - era * 146_097;
	let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
	let y = yoe + era * 400;
	let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
	let mp = (5 * doy + 2) / 153;
	let d = doy - (153 * mp + 2) / 5 + 1;
	let m = if mp < 10 { mp + 3 } else { mp - 9 };
	(if m <= 2 { y + 1 } else { y }, m, d)
}

fn num(s: &str) -> Option<i64> {
	(!s.is_empty() && s.bytes().all(|b| b.is_ascii_digit())).then(|| s.parse().ok())?
}

/// Parse an RFC 3339 timestamp (`2024-05-01T12:00:00.123Z`,
/// `...+02:00`) into unix nanoseconds.
#[allow(clippy::many_single_char_names, reason = "calendar field names (y, d, h, ...)")]
pub fn parse_rfc3339(s: &str) -> Option<i128> {
	let s = s.trim();
	if s.len() < 20 || !s.is_char_boundary(10) {
		return None;
	}
	let (date, rest) = (&s[..10], &s[11..]);
	if !matches!(s.as_bytes()[10], b'T' | b't' | b' ') {
		return None;
	}
	let mut dp = date.split('-');
	let (y, mo, d) = (num(dp.next()?)?, num(dp.next()?)?, num(dp.next()?)?);
	let time_end = rest.find(['Z', 'z', '+', '-']).unwrap_or(rest.len());
	let (clock, zone) = rest.split_at(time_end);
	let (hms, frac) = clock.split_once('.').unwrap_or((clock, ""));
	let mut tp = hms.split(':');
	let (h, mi, sec) = (num(tp.next()?)?, num(tp.next()?)?, num(tp.next()?)?);
	if !(1..=12).contains(&mo) || !(1..=31).contains(&d) || h > 23 || mi > 59 || sec > 60 {
		return None;
	}
	let nanos: i128 = if frac.is_empty() {
		0
	} else {
		if !frac.bytes().all(|b| b.is_ascii_digit()) {
			return None;
		}
		let digits: String = frac.chars().chain(std::iter::repeat('0')).take(9).collect();
		digits.parse().ok()?
	};
	let offset_secs = match zone {
		"Z" | "z" => 0,
		z if z.len() == 6 => {
			let sign = if z.starts_with('-') { -1 } else { 1 };
			let (oh, om) = z[1..].split_once(':')?;
			sign * (num(oh)? * 3600 + num(om)? * 60)
		},
		_ => return None,
	};
	let secs = days_from_civil(y, mo, d) * 86_400 + h * 3600 + mi * 60 + sec - offset_secs;
	Some(i128::from(secs) * 1_000_000_000 + nanos)
}

/// Current unix time in nanoseconds.
pub fn now_ns() -> i128 {
	SystemTime::now().duration_since(UNIX_EPOCH).map_or_else(
		|e| -i128::try_from(e.duration().as_nanos()).unwrap_or(0),
		|d| i128::try_from(d.as_nanos()).unwrap_or(i128::MAX),
	)
}

/// Current unix time in seconds.
pub fn now_unix() -> i64 {
	i64::try_from(now_ns() / 1_000_000_000).unwrap_or(i64::MAX)
}

const DAYS: [&str; 7] = ["Thu", "Fri", "Sat", "Sun", "Mon", "Tue", "Wed"];
const MONTHS: [&str; 12] =
	["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];

/// IMF-fixdate (`Sun, 06 Nov 1994 08:49:37 GMT`) for unix seconds.
pub fn http_date(unix: i64) -> String {
	let days = unix.div_euclid(86_400);
	let secs = unix.rem_euclid(86_400);
	let (y, m, d) = civil_from_days(days);
	let weekday = DAYS[usize::try_from(days.rem_euclid(7)).unwrap_or(0)];
	let month = MONTHS[usize::try_from(m - 1).unwrap_or(0)];
	format!(
		"{weekday}, {d:02} {month} {y:04} {:02}:{:02}:{:02} GMT",
		secs / 3600,
		secs % 3600 / 60,
		secs % 60
	)
}

/// Parse an IMF-fixdate into unix seconds.
#[allow(clippy::many_single_char_names, reason = "calendar field names (y, d, h, ...)")]
pub fn parse_http_date(s: &str) -> Option<i64> {
	let parts: Vec<&str> = s.split_whitespace().collect();
	if parts.len() != 6 || parts[5] != "GMT" {
		return None;
	}
	let d = num(parts[1])?;
	let m = i64::try_from(MONTHS.iter().position(|&mo| mo == parts[2])? + 1).ok()?;
	let y = num(parts[3])?;
	let mut tp = parts[4].split(':');
	let (h, mi, sec) = (num(tp.next()?)?, num(tp.next()?)?, num(tp.next()?)?);
	Some(days_from_civil(y, m, d) * 86_400 + h * 3600 + mi * 60 + sec)
}

/// Whether the guest clock must be stepped to `host_ns`: guest more than
/// 50 ms behind or more than 5 s ahead (envd's asymmetric gate).
pub const fn needs_step(guest_ns: i128, host_ns: i128) -> bool {
	guest_ns < host_ns - 50_000_000 || guest_ns > host_ns + 5_000_000_000
}

/// Step `CLOCK_REALTIME` to `unix_ns`.
pub fn set_realtime(unix_ns: i128) -> std::io::Result<()> {
	// SAFETY: timespec is plain data; zero is a valid value.
	let mut ts: libc::timespec = unsafe { std::mem::zeroed() };
	ts.tv_sec = unix_ns
		.div_euclid(1_000_000_000)
		.try_into()
		.unwrap_or_default();
	ts.tv_nsec = unix_ns
		.rem_euclid(1_000_000_000)
		.try_into()
		.unwrap_or_default();
	// SAFETY: ts is a valid timespec.
	if unsafe { libc::clock_settime(libc::CLOCK_REALTIME, &ts) } == 0 {
		Ok(())
	} else {
		Err(std::io::Error::last_os_error())
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn rfc3339_variants() {
		assert_eq!(parse_rfc3339("1970-01-01T00:00:00Z"), Some(0));
		assert_eq!(parse_rfc3339("2024-02-29T12:30:15.5Z"), Some(1_709_209_815_500_000_000));
		// Offsets shift to UTC.
		assert_eq!(parse_rfc3339("2024-02-29T14:30:15.5+02:00"), Some(1_709_209_815_500_000_000));
		assert_eq!(parse_rfc3339("2024-02-29T10:30:15.5-02:00"), Some(1_709_209_815_500_000_000));
		// Go's RFC3339Nano with 9 digits.
		assert_eq!(parse_rfc3339("1970-01-01T00:00:01.000000001Z"), Some(1_000_000_001));
		assert_eq!(parse_rfc3339("2024-13-01T00:00:00Z"), None);
		assert_eq!(parse_rfc3339("2024-01-01 00:00:00"), None);
		assert_eq!(parse_rfc3339("garbage"), None);
	}

	#[test]
	fn http_date_round_trip() {
		assert_eq!(http_date(784_111_777), "Sun, 06 Nov 1994 08:49:37 GMT");
		assert_eq!(parse_http_date("Sun, 06 Nov 1994 08:49:37 GMT"), Some(784_111_777));
		assert_eq!(http_date(0), "Thu, 01 Jan 1970 00:00:00 GMT");
		assert_eq!(parse_http_date("not a date"), None);
	}

	#[test]
	fn clock_step_gate_is_asymmetric() {
		let host = 1_000_000_000_000;
		assert!(!needs_step(host - 49_000_000, host));
		assert!(needs_step(host - 51_000_000, host));
		assert!(!needs_step(host + 4_900_000_000, host));
		assert!(needs_step(host + 5_100_000_000, host));
	}
}
