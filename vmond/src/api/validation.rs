use std::{
	collections::HashMap,
	net::{IpAddr, Ipv4Addr},
};

use serde::de::DeserializeOwned;
use serde_json::Value;

use super::error::{ApiError, ApiResult};
use crate::{
	engine::ExecRequest,
	models::{ExecBody, ExtendBody, ForkBody, MAX_FORK_CLONES, NetworkBody, SandboxCreate},
	registry::PersistencePolicy,
};

const ALLOWED_HA: &[&str] = &["async", "async+rerun", "off", "rerun"];
const ALLOWED_ARCH: &[&str] = &["aarch64", "x86_64"];

pub fn from_value<T: DeserializeOwned>(value: Value) -> ApiResult<T> {
	serde_json::from_value(value).map_err(|_| ApiError::invalid("invalid request"))
}

pub fn validate_create_value(value: &Value) -> ApiResult<()> {
	positive_int(value, "cpus")?;
	positive_int(value, "memory")?;
	positive_int(value, "disk_mb")?;
	non_negative_int(value, "pool_size")?;
	non_negative_float(value, "timeout")?;
	non_negative_int(value, "timeout_secs")?;
	non_negative_float(value, "idle_timeout_secs")?;
	non_negative_int(value, "activity_threshold_bytes")?;
	validate_persistence_value(value.get("persistence"))?;
	validate_nics_value(value.get("nics"))?;
	if field_truthy(value, "remote_page_url")
		|| field_truthy(value, "remote_page_token")
		|| field_truthy(value, "remote_page_digest")
	{
		return Err(ApiError::invalid("remote_page_* fields are server-internal"));
	}
	if bool_field(value, "block_network")?.unwrap_or(false)
		&& array_present_non_empty(value, "ports")?
	{
		return Err(ApiError::invalid("ports cannot be exposed when block_network=True"));
	}
	validate_ports(value.get("ports"))?;
	validate_cidrs("egress_allow", value.get("egress_allow"))?;
	validate_cidrs("inbound_cidr_allowlist", value.get("inbound_cidr_allowlist"))?;
	validate_domains("egress_allow_domains", value.get("egress_allow_domains"))?;
	validate_ha_value(value.get("ha"))?;
	validate_arch_value(value.get("arch"))?;
	Ok(())
}

pub fn validate_create(body: &SandboxCreate) -> ApiResult<()> {
	if body.cpus == 0 {
		return Err(ApiError::invalid("cpus must be positive"));
	}
	if body.memory == 0 {
		return Err(ApiError::invalid("memory must be positive"));
	}
	if body.disk_mb == 0 {
		return Err(ApiError::invalid("disk_mb must be positive"));
	}
	if let Some(timeout) = body.timeout
		&& (!timeout.is_finite() || timeout < 0.0)
	{
		return Err(ApiError::invalid("timeout must be non-negative"));
	}
	if let Some(timeout) = body.idle_timeout_secs
		&& (!timeout.is_finite() || timeout < 0.0)
	{
		return Err(ApiError::invalid("idle_timeout_secs must be non-negative"));
	}
	if let Some(PersistencePolicy::Sticky { priority }) = &body.persistence
		&& *priority > 10
	{
		return Err(ApiError::invalid("sticky persistence priority must be between 0 and 10"));
	}
	if body.block_network && body.ports.as_ref().is_some_and(|ports| !ports.is_empty()) {
		return Err(ApiError::invalid("ports cannot be exposed when block_network=True"));
	}
	if let Some(ports) = &body.ports
		&& ports.contains(&0)
	{
		return Err(ApiError::invalid("ports must be TCP port numbers from 1 to 65535"));
	}
	validate_cidr_strings("egress_allow", body.egress_allow.as_deref())?;
	validate_cidr_strings("inbound_cidr_allowlist", body.inbound_cidr_allowlist.as_deref())?;
	validate_domain_strings("egress_allow_domains", body.egress_allow_domains.as_deref())?;
	validate_nics(body.nics.as_deref())?;
	validate_ha_str(body.ha.as_deref())?;
	validate_arch_str(body.arch.as_deref())?;
	Ok(())
}

fn validate_persistence_value(value: Option<&Value>) -> ApiResult<()> {
	let Some(value) = value else {
		return Ok(());
	};
	let policy = serde_json::from_value::<PersistencePolicy>(value.clone())
		.map_err(|_| ApiError::invalid("invalid persistence policy"))?;
	if policy
		.sticky_priority()
		.is_some_and(|priority| priority > 10)
	{
		return Err(ApiError::invalid("sticky persistence priority must be between 0 and 10"));
	}
	Ok(())
}

fn validate_nics_value(value: Option<&Value>) -> ApiResult<()> {
	let Some(value) = value.filter(|value| !value.is_null()) else {
		return Ok(());
	};
	let Some(items) = value.as_array() else {
		return Err(ApiError::invalid("invalid request"));
	};
	if items.len() > 1 {
		return Err(ApiError::invalid("vmon VMs have a single NIC"));
	}
	for item in items {
		let Some(item) = item.as_object() else {
			return Err(ApiError::invalid("invalid VPC NIC"));
		};
		let vpc = item.get("vpc").and_then(Value::as_str).unwrap_or_default();
		if !valid_vpc_id(vpc) {
			return Err(ApiError::invalid("VPC NIC vpc must be a vpc-<8hex> identifier"));
		}
		if item.get("default").and_then(Value::as_bool) != Some(true) {
			return Err(ApiError::invalid("vmon VMs have a single NIC"));
		}
		match item.get("ipv4") {
			Some(Value::Bool(true)) => {},
			Some(Value::String(address)) if address.parse::<Ipv4Addr>().is_ok() => {},
			_ => {
				return Err(ApiError::invalid("VPC NIC ipv4 must be a valid IPv4 address or true"));
			},
		}
	}
	Ok(())
}

fn validate_nics(nics: Option<&[crate::models::NicSpec]>) -> ApiResult<()> {
	if nics.is_some_and(|nics| nics.len() > 1) {
		return Err(ApiError::invalid("vmon VMs have a single NIC"));
	}
	for nic in nics.unwrap_or_default() {
		if !nic.default {
			return Err(ApiError::invalid("vmon VMs have a single NIC"));
		}
		if !valid_vpc_id(&nic.vpc) {
			return Err(ApiError::invalid("VPC NIC vpc must be a vpc-<8hex> identifier"));
		}
		match &nic.ipv4 {
			Value::Bool(true) => {},
			Value::String(address) if address.parse::<Ipv4Addr>().is_ok() => {},
			_ => {
				return Err(ApiError::invalid("VPC NIC ipv4 must be a valid IPv4 address or true"));
			},
		}
	}
	Ok(())
}

fn valid_vpc_id(value: &str) -> bool {
	value.len() == 12
		&& value.starts_with("vpc-")
		&& value[4..].bytes().all(|byte| byte.is_ascii_hexdigit())
}

pub fn validate_exec(body: &ExecBody) -> ApiResult<ExecRequest> {
	if body.cmd.is_empty() {
		return Err(ApiError::invalid("exec cmd must not be empty"));
	}
	if body.cmd.first().is_some_and(String::is_empty) {
		return Err(ApiError::invalid("exec cmd[0] must not be empty"));
	}
	if let Some(timeout) = body.timeout
		&& (!timeout.is_finite() || timeout < 0.0)
	{
		return Err(ApiError::invalid("timeout must be non-negative"));
	}
	Ok(ExecRequest {
		cmd:     body.cmd.clone(),
		tty:     body.tty,
		env:     body.env.clone(),
		workdir: body.workdir.clone().or_else(|| body.cwd.clone()),
		timeout: body.timeout.map(|timeout| timeout.min(60.0)),
	})
}

pub fn validate_extend(body: &ExtendBody) -> ApiResult<()> {
	if body.secs == 0 {
		return Err(ApiError::invalid("secs must be positive"));
	}
	Ok(())
}

pub fn validate_network(body: &NetworkBody) -> ApiResult<()> {
	if body.block_network.is_none() && body.cidr_allow.is_none() && body.domain_allow.is_none() {
		return Err(ApiError::invalid("network request must set at least one field"));
	}
	validate_cidr_strings("cidr_allow", body.cidr_allow.as_deref())?;
	validate_domain_strings("domain_allow", body.domain_allow.as_deref())?;
	Ok(())
}

pub fn validate_fork(body: &ForkBody) -> ApiResult<()> {
	if !(1..=MAX_FORK_CLONES).contains(&body.count) {
		return Err(ApiError::invalid(format!("count must be between 1 and {MAX_FORK_CLONES}")));
	}
	Ok(())
}

pub fn parse_tag_filters(values: &[String]) -> ApiResult<Option<HashMap<String, String>>> {
	if values.is_empty() {
		return Ok(None);
	}
	let mut parsed = HashMap::new();
	for value in values {
		let (key, tag_value) = value
			.split_once('=')
			.or_else(|| value.split_once(':'))
			.ok_or_else(|| ApiError::invalid("tag filters must be K=V"))?;
		if key.is_empty() {
			return Err(ApiError::invalid("tag filters must be K=V"));
		}
		parsed.insert(key.to_owned(), tag_value.to_owned());
	}
	Ok(Some(parsed))
}

fn positive_int(value: &Value, key: &str) -> ApiResult<()> {
	if let Some(number) = int_field(value, key)?
		&& number <= 0
	{
		return Err(ApiError::invalid(format!("{key} must be positive")));
	}
	Ok(())
}

fn non_negative_int(value: &Value, key: &str) -> ApiResult<()> {
	if let Some(number) = int_field(value, key)?
		&& number < 0
	{
		return Err(ApiError::invalid(format!("{key} must be non-negative")));
	}
	Ok(())
}

fn non_negative_float(value: &Value, key: &str) -> ApiResult<()> {
	let Some(raw) = value.get(key) else {
		return Ok(());
	};
	if raw.is_null() {
		return Ok(());
	}
	let Some(number) = raw.as_f64() else {
		return Err(ApiError::invalid("invalid request"));
	};
	if !number.is_finite() || number < 0.0 {
		return Err(ApiError::invalid(format!("{key} must be non-negative")));
	}
	Ok(())
}

fn int_field(value: &Value, key: &str) -> ApiResult<Option<i64>> {
	let Some(raw) = value.get(key) else {
		return Ok(None);
	};
	if raw.is_null() {
		return Ok(None);
	}
	if let Some(number) = raw.as_i64() {
		return Ok(Some(number));
	}
	if let Some(number) = raw.as_u64() {
		return i64::try_from(number)
			.map(Some)
			.map_err(|_| ApiError::invalid("invalid request"));
	}
	Err(ApiError::invalid("invalid request"))
}

fn bool_field(value: &Value, key: &str) -> ApiResult<Option<bool>> {
	let Some(raw) = value.get(key) else {
		return Ok(None);
	};
	if raw.is_null() {
		return Ok(None);
	}
	raw.as_bool()
		.map(Some)
		.ok_or_else(|| ApiError::invalid("invalid request"))
}

fn array_present_non_empty(value: &Value, key: &str) -> ApiResult<bool> {
	let Some(raw) = value.get(key) else {
		return Ok(false);
	};
	if raw.is_null() {
		return Ok(false);
	}
	raw.as_array()
		.map(|items| !items.is_empty())
		.ok_or_else(|| ApiError::invalid("invalid request"))
}

fn field_truthy(value: &Value, key: &str) -> bool {
	match value.get(key) {
		None | Some(Value::Null) => false,
		Some(Value::String(text)) => !text.is_empty(),
		Some(Value::Bool(flag)) => *flag,
		Some(Value::Array(items)) => !items.is_empty(),
		Some(Value::Object(items)) => !items.is_empty(),
		Some(Value::Number(_)) => true,
	}
}

fn validate_ports(value: Option<&Value>) -> ApiResult<()> {
	let Some(value) = value else {
		return Ok(());
	};
	if value.is_null() {
		return Ok(());
	}
	let Some(items) = value.as_array() else {
		return Err(ApiError::invalid("invalid request"));
	};
	for item in items {
		let Some(port) = item.as_u64() else {
			return Err(ApiError::invalid("ports must be TCP port numbers from 1 to 65535"));
		};
		if !(1..=65_535).contains(&port) {
			return Err(ApiError::invalid("ports must be TCP port numbers from 1 to 65535"));
		}
	}
	Ok(())
}

fn validate_cidrs(name: &str, value: Option<&Value>) -> ApiResult<()> {
	let Some(value) = value else {
		return Ok(());
	};
	if value.is_null() {
		return Ok(());
	}
	let Some(items) = value.as_array() else {
		return Err(ApiError::invalid("invalid request"));
	};
	for item in items {
		let Some(text) = item.as_str() else {
			return Err(ApiError::invalid(format!("{name} entries must be valid CIDR networks")));
		};
		validate_cidr(name, text)?;
	}
	Ok(())
}

fn validate_domains(name: &str, value: Option<&Value>) -> ApiResult<()> {
	let Some(value) = value else {
		return Ok(());
	};
	if value.is_null() {
		return Ok(());
	}
	let Some(items) = value.as_array() else {
		return Err(ApiError::invalid("invalid request"));
	};
	for item in items {
		let Some(text) = item.as_str() else {
			return Err(ApiError::invalid(format!("{name} entries must be non-empty")));
		};
		if text.trim().is_empty() {
			return Err(ApiError::invalid(format!("{name} entries must be non-empty")));
		}
	}
	Ok(())
}

fn validate_ha_value(value: Option<&Value>) -> ApiResult<()> {
	let Some(value) = value else {
		return Ok(());
	};
	if value.is_null() {
		return Ok(());
	}
	let Some(text) = value.as_str() else {
		return Err(ApiError::invalid(ha_error()));
	};
	validate_ha_str(Some(text))
}

fn validate_arch_value(value: Option<&Value>) -> ApiResult<()> {
	let Some(value) = value else {
		return Ok(());
	};
	if value.is_null() {
		return Ok(());
	}
	let Some(text) = value.as_str() else {
		return Err(ApiError::invalid(arch_error()));
	};
	validate_arch_str(Some(text))
}

fn validate_cidr_strings(name: &str, values: Option<&[String]>) -> ApiResult<()> {
	for value in values.unwrap_or_default() {
		validate_cidr(name, value)?;
	}
	Ok(())
}

fn validate_domain_strings(name: &str, values: Option<&[String]>) -> ApiResult<()> {
	for value in values.unwrap_or_default() {
		if value.trim().is_empty() {
			return Err(ApiError::invalid(format!("{name} entries must be non-empty")));
		}
	}
	Ok(())
}

fn validate_ha_str(value: Option<&str>) -> ApiResult<()> {
	if let Some(value) = value
		&& !ALLOWED_HA.contains(&value)
	{
		return Err(ApiError::invalid(ha_error()));
	}
	Ok(())
}

fn validate_arch_str(value: Option<&str>) -> ApiResult<()> {
	if let Some(value) = value
		&& !ALLOWED_ARCH.contains(&value)
	{
		return Err(ApiError::invalid(arch_error()));
	}
	Ok(())
}

fn validate_cidr(name: &str, text: &str) -> ApiResult<()> {
	let (addr, prefix) = text
		.split_once('/')
		.map_or((text, None), |(addr, prefix)| (addr, Some(prefix)));
	let ip = addr
		.parse::<IpAddr>()
		.map_err(|_| ApiError::invalid(format!("{name} entries must be valid CIDR networks")))?;
	if let Some(prefix) = prefix {
		let prefix = prefix
			.parse::<u8>()
			.map_err(|_| ApiError::invalid(format!("{name} entries must be valid CIDR networks")))?;
		let max = if ip.is_ipv4() { 32 } else { 128 };
		if prefix > max {
			return Err(ApiError::invalid(format!("{name} entries must be valid CIDR networks")));
		}
	}
	Ok(())
}

fn ha_error() -> String {
	format!("ha must be one of: {}", ALLOWED_HA.join(", "))
}

fn arch_error() -> String {
	format!("arch must be one of: {}", ALLOWED_ARCH.join(", "))
}
