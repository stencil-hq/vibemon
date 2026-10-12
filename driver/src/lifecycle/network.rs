//! E2B sandbox network configuration and its node egress translation.
//!
//! `SandboxNetworkConfig` / `SandboxNetworkUpdateConfig` become the node
//! egress policy plus ingress settings (`local://e2b-api.md` F §8, §3.1).
//!
//! Egress validation/normalization is delegated to
//! `vmon_node::egress::policy::normalize`; this module folds
//! `allow_internet_access`, enforces the E2B API-level count limits, and
//! validates ingress settings (`httpsPorts`, `maskRequestHost`).

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use vmon_node::api::{EgressProxy, EgressProxyTls, NetworkPolicy, NetworkRule};
/// CIDR that blocks all IPv4 egress; `allow_internet_access=false` means
/// `denyOut=[this]` (shared with the node egress policy).
pub use vmon_node::egress::policy::ALL_TRAFFIC;

use crate::error::{ApiError, ApiResult};

/// envd port; it always speaks plain HTTP and may not be listed in
/// `httpsPorts`.
pub const ENVD_PORT: u16 = 49983;
const MAX_HTTPS_PORTS: usize = 128;
const MAX_RULE_DOMAINS: usize = 10;
const MAX_RULE_DOMAIN_LEN: usize = 128;
const MAX_RULES_PER_DOMAIN: usize = 1;
const MAX_RULE_HEADERS: usize = 20;
const MAX_HEADER_NAME_LEN: usize = 64;
const MAX_HEADER_VALUE_LEN: usize = 2048;

/// `SandboxNetworkConfig` as sent on create (and echoed, redacted, by `GET
/// /sandboxes/{id}`).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NetworkConfig {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub allow_public_traffic: Option<bool>,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub allow_out:            Option<Vec<String>>,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub deny_out:             Option<Vec<String>>,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub egress_proxy:         Option<EgressProxyConfig>,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub mask_request_host:    Option<String>,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub https_ports:          Option<Vec<i64>>,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub rules:                Option<BTreeMap<String, Vec<RuleConfig>>>,
}

/// `SandboxNetworkUpdateConfig` (`PUT /sandboxes/{id}/network`): a full
/// replacement of the egress settings; omitted fields are cleared.
#[derive(Clone, Debug, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NetworkUpdate {
	#[serde(default)]
	pub allow_out:             Option<Vec<String>>,
	#[serde(default)]
	pub deny_out:              Option<Vec<String>>,
	#[serde(default)]
	pub egress_proxy:          Option<EgressProxyConfig>,
	#[serde(default)]
	pub rules:                 Option<BTreeMap<String, Vec<RuleConfig>>>,
	#[serde(default, rename = "allow_internet_access")]
	pub allow_internet_access: Option<bool>,
}

/// `SandboxEgressProxyConfig`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EgressProxyConfig {
	pub address:  String,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub username: Option<String>,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub password: Option<String>,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub tls:      Option<EgressProxyTlsConfig>,
}

/// `SandboxEgressProxyTLSConfig`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EgressProxyTlsConfig {
	pub enabled:     bool,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub server_name: Option<String>,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub ca_cert:     Option<String>,
}

/// `SandboxNetworkRule`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuleConfig {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub transform: Option<TransformConfig>,
}

/// `SandboxNetworkTransform`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransformConfig {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub headers: Option<BTreeMap<String, String>>,
}

/// Ingress settings carried in the KV route.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Ingress {
	pub allow_public_traffic: bool,
	pub mask_request_host:    Option<String>,
	pub https_ports:          Vec<u16>,
}

/// Egress-relevant subset shared by create and update.
pub struct EgressInput<'a> {
	pub allow_out:             Option<&'a [String]>,
	pub deny_out:              Option<&'a [String]>,
	pub egress_proxy:          Option<&'a EgressProxyConfig>,
	pub rules:                 Option<&'a BTreeMap<String, Vec<RuleConfig>>>,
	pub allow_internet_access: Option<bool>,
}

impl NetworkConfig {
	/// Egress input of a create request.
	pub fn egress(&self, allow_internet_access: Option<bool>) -> EgressInput<'_> {
		EgressInput {
			allow_out: self.allow_out.as_deref(),
			deny_out: self.deny_out.as_deref(),
			egress_proxy: self.egress_proxy.as_ref(),
			rules: self.rules.as_ref(),
			allow_internet_access,
		}
	}

	/// Copy without secrets (proxy password, CA) for `GET /sandboxes/{id}`.
	pub fn redacted(&self) -> Self {
		let mut out = self.clone();
		if let Some(proxy) = out.egress_proxy.as_mut() {
			proxy.password = None;
			if let Some(tls) = proxy.tls.as_mut() {
				tls.ca_cert = None;
			}
		}
		out
	}

	/// Validates and extracts the ingress settings.
	pub fn ingress(&self) -> ApiResult<Ingress> {
		let ports = self.https_ports.as_deref().unwrap_or_default();
		if ports.len() > MAX_HTTPS_PORTS {
			return Err(ApiError::bad_request(format!(
				"httpsPorts may list at most {MAX_HTTPS_PORTS} ports"
			)));
		}
		let mut https_ports = Vec::with_capacity(ports.len());
		for &port in ports {
			let port = u16::try_from(port)
				.ok()
				.filter(|p| *p != 0)
				.ok_or_else(|| ApiError::bad_request(format!("invalid https port {port}")))?;
			if port == ENVD_PORT {
				return Err(ApiError::bad_request(format!("port {ENVD_PORT} cannot be an https port")));
			}
			if https_ports.contains(&port) {
				return Err(ApiError::bad_request(format!("duplicate https port {port}")));
			}
			https_ports.push(port);
		}
		let mask_request_host = self.mask_request_host.clone().filter(|m| !m.is_empty());
		Ok(Ingress {
			allow_public_traffic: self.allow_public_traffic.unwrap_or(true),
			mask_request_host,
			https_ports,
		})
	}

	/// Replaces the egress part with an update while keeping ingress settings.
	pub fn with_update(&self, update: &NetworkUpdate) -> Self {
		Self {
			allow_out: update.allow_out.clone(),
			deny_out: update.deny_out.clone(),
			egress_proxy: update.egress_proxy.clone(),
			rules: update.rules.clone(),
			..self.clone()
		}
	}
}

impl NetworkUpdate {
	/// Egress input of an update request.
	pub fn egress(&self) -> EgressInput<'_> {
		EgressInput {
			allow_out:             self.allow_out.as_deref(),
			deny_out:              self.deny_out.as_deref(),
			egress_proxy:          self.egress_proxy.as_ref(),
			rules:                 self.rules.as_ref(),
			allow_internet_access: self.allow_internet_access,
		}
	}
}

/// Builds the normalized node egress policy. `allow_internet_access=false`
/// replaces `denyOut` with `0.0.0.0/0` (allowOut entries are preserved).
pub fn egress_policy(input: &EgressInput<'_>) -> ApiResult<NetworkPolicy> {
	let deny_out = if input.allow_internet_access == Some(false) {
		vec![ALL_TRAFFIC.to_owned()]
	} else {
		input.deny_out.unwrap_or_default().to_vec()
	};
	let rules = match input.rules {
		Some(rules) => convert_rules(rules)?,
		None => BTreeMap::new(),
	};
	let egress_proxy = input.egress_proxy.map(|proxy| EgressProxy {
		address:  proxy.address.clone(),
		username: proxy.username.clone(),
		password: proxy.password.clone(),
		tls:      proxy.tls.as_ref().map(|tls| EgressProxyTls {
			enabled:     tls.enabled,
			server_name: tls.server_name.clone(),
			ca_cert:     tls.ca_cert.clone(),
		}),
	});
	let policy = NetworkPolicy {
		allow_out: input.allow_out.unwrap_or_default().to_vec(),
		deny_out,
		egress_proxy,
		rules,
	};
	vmon_node::egress::policy::normalize(policy).map_err(ApiError::bad_request)
}

fn convert_rules(
	rules: &BTreeMap<String, Vec<RuleConfig>>,
) -> ApiResult<BTreeMap<String, Vec<NetworkRule>>> {
	if rules.len() > MAX_RULE_DOMAINS {
		return Err(ApiError::bad_request(format!(
			"network rules may target at most {MAX_RULE_DOMAINS} domains"
		)));
	}
	let mut out = BTreeMap::new();
	for (domain, list) in rules {
		if domain.is_empty() || domain.len() > MAX_RULE_DOMAIN_LEN {
			return Err(ApiError::bad_request(format!(
				"network rule domain must be 1..={MAX_RULE_DOMAIN_LEN} characters"
			)));
		}
		if list.len() > MAX_RULES_PER_DOMAIN {
			return Err(ApiError::bad_request(format!(
				"at most {MAX_RULES_PER_DOMAIN} rule per domain is allowed ({domain})"
			)));
		}
		let mut converted = Vec::with_capacity(list.len());
		for rule in list {
			let headers = rule
				.transform
				.as_ref()
				.and_then(|t| t.headers.clone())
				.unwrap_or_default();
			if headers.len() > MAX_RULE_HEADERS {
				return Err(ApiError::bad_request(format!(
					"a rule may set at most {MAX_RULE_HEADERS} headers"
				)));
			}
			if let Some((name, _)) = headers
				.iter()
				.find(|(n, _)| n.is_empty() || n.len() > MAX_HEADER_NAME_LEN)
			{
				return Err(ApiError::bad_request(format!("invalid header name {name:?}")));
			}
			if let Some((name, _)) = headers.iter().find(|(_, v)| v.len() > MAX_HEADER_VALUE_LEN) {
				return Err(ApiError::bad_request(format!(
					"header {name} value exceeds {MAX_HEADER_VALUE_LEN} characters"
				)));
			}
			converted.push(NetworkRule { headers });
		}
		out.insert(domain.clone(), converted);
	}
	Ok(out)
}

/// Node features a policy requires (`egress-proxy` for rules/SOCKS proxy,
/// `egress-domains` for domain allow entries).
pub fn required_features(policy: &NetworkPolicy) -> Vec<&'static str> {
	if policy.needs_proxy() {
		vec!["egress-proxy"]
	} else if policy.has_domains() {
		vec!["egress-domains"]
	} else {
		Vec::new()
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn strings(v: &[&str]) -> Vec<String> {
		v.iter().map(ToString::to_string).collect()
	}

	#[test]
	fn disallowing_internet_replaces_deny_but_keeps_allow() {
		let allow = strings(&["1.1.1.1/32"]);
		let deny = strings(&["8.8.8.0/24"]);
		let input = EgressInput {
			allow_out:             Some(&allow),
			deny_out:              Some(&deny),
			egress_proxy:          None,
			rules:                 None,
			allow_internet_access: Some(false),
		};
		let policy = egress_policy(&input).unwrap();
		assert_eq!(policy.deny_out, vec![ALL_TRAFFIC.to_owned()]);
		assert_eq!(policy.allow_out, allow);
		// `true` leaves the explicit deny list alone.
		let policy =
			egress_policy(&EgressInput { allow_internet_access: Some(true), ..input }).unwrap();
		assert_eq!(policy.deny_out, deny);
	}

	#[test]
	fn rule_limits_are_enforced_at_the_api() {
		let rule = |n: usize| RuleConfig {
			transform: Some(TransformConfig {
				headers: Some((0..n).map(|i| (format!("x-h{i}"), "v".into())).collect()),
			}),
		};
		let ok = BTreeMap::from([("api.example.com".to_owned(), vec![rule(MAX_RULE_HEADERS)])]);
		assert_eq!(
			convert_rules(&ok).unwrap()["api.example.com"][0]
				.headers
				.len(),
			MAX_RULE_HEADERS
		);
		let too_many_headers =
			BTreeMap::from([("a.com".to_owned(), vec![rule(MAX_RULE_HEADERS + 1)])]);
		assert!(convert_rules(&too_many_headers).is_err());
		let two_rules = BTreeMap::from([("a.com".to_owned(), vec![rule(1), rule(1)])]);
		assert!(convert_rules(&two_rules).is_err());
		let domains: BTreeMap<_, _> = (0..=MAX_RULE_DOMAINS)
			.map(|i| (format!("d{i}.com"), vec![rule(1)]))
			.collect();
		assert!(convert_rules(&domains).is_err());
		let long_value = BTreeMap::from([("a.com".to_owned(), vec![RuleConfig {
			transform: Some(TransformConfig {
				headers: Some(BTreeMap::from([("x".into(), "v".repeat(MAX_HEADER_VALUE_LEN + 1))])),
			}),
		}])]);
		assert!(convert_rules(&long_value).is_err());
	}

	#[test]
	fn ingress_defaults_public_and_validates_ports() {
		let cfg = NetworkConfig::default();
		assert_eq!(cfg.ingress().unwrap(), Ingress {
			allow_public_traffic: true,
			..Ingress::default()
		});
		let ports =
			|p: Vec<i64>| NetworkConfig { https_ports: Some(p), ..NetworkConfig::default() }.ingress();
		assert_eq!(ports(vec![443, 8443]).unwrap().https_ports, vec![443, 8443]);
		for bad in [vec![0], vec![65_536], vec![-1], vec![i64::from(ENVD_PORT)], vec![443, 443]] {
			assert!(ports(bad.clone()).is_err(), "{bad:?}");
		}
		assert!(ports((1..=129).collect()).is_err());
		let private = NetworkConfig {
			allow_public_traffic: Some(false),
			mask_request_host: Some(String::new()),
			..NetworkConfig::default()
		};
		let ingress = private.ingress().unwrap();
		assert!(!ingress.allow_public_traffic);
		assert_eq!(ingress.mask_request_host, None);
	}

	#[test]
	fn redaction_drops_secrets_only() {
		let cfg = NetworkConfig {
			egress_proxy: Some(EgressProxyConfig {
				address:  "proxy:1080".into(),
				username: Some("u".into()),
				password: Some("secret".into()),
				tls:      Some(EgressProxyTlsConfig {
					enabled:     true,
					server_name: Some("p".into()),
					ca_cert:     Some("PEM".into()),
				}),
			}),
			..NetworkConfig::default()
		};
		let red = cfg.redacted();
		let proxy = red.egress_proxy.unwrap();
		assert_eq!(proxy.password, None);
		assert_eq!(proxy.username.as_deref(), Some("u"));
		let tls = proxy.tls.unwrap();
		assert_eq!((tls.ca_cert, tls.server_name.as_deref()), (None, Some("p")));
	}

	#[test]
	fn update_replaces_egress_and_keeps_ingress() {
		let cfg = NetworkConfig {
			allow_public_traffic: Some(false),
			allow_out: Some(strings(&["1.1.1.1"])),
			deny_out: Some(strings(&[ALL_TRAFFIC])),
			https_ports: Some(vec![443]),
			..NetworkConfig::default()
		};
		let updated = cfg.with_update(&NetworkUpdate {
			deny_out: Some(strings(&["10.0.0.0/8"])),
			..NetworkUpdate::default()
		});
		assert_eq!(updated.allow_out, None);
		assert_eq!(updated.deny_out, Some(strings(&["10.0.0.0/8"])));
		assert_eq!(updated.allow_public_traffic, Some(false));
		assert_eq!(updated.https_ports, Some(vec![443]));
	}

	#[test]
	fn proxy_and_domains_select_node_features() {
		let mut policy = NetworkPolicy::default();
		let features = required_features(&policy);
		assert!(features.is_empty(), "unexpected features: {features:?}");
		policy.allow_out = strings(&["*.example.com"]);
		assert_eq!(required_features(&policy), vec!["egress-domains"]);
		policy
			.rules
			.insert("a.com".into(), vec![NetworkRule::default()]);
		assert_eq!(required_features(&policy), vec!["egress-proxy"]);
	}
}
