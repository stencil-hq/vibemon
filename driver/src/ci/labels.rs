//! `runs-on` labels of vmon CI jobs: `<size>[/<option>…]`.
//!
//! A job is vmon's when one of its labels is a size label family member
//! (`vmon`, `vmon-<x>`, either optionally followed by `/options`). Such a job
//! needs exactly one of them, plus at most the GitHub default labels
//! `self-hosted`, `linux`, and `x64` (case-insensitive); anything else refuses
//! the job with a reason. Options: `from=<template>` (a template reference,
//! whose `namespace/alias` form takes the next segment too) and `debug`.

use std::fmt;

use serde::{Deserialize, Serialize};

/// Runner sizes (`runs-on` size labels).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Size {
	#[serde(rename = "vmon-small")]
	Small,
	#[serde(rename = "vmon")]
	Medium,
	#[serde(rename = "vmon-large")]
	Large,
	#[serde(rename = "vmon-xlarge")]
	XLarge,
}

impl Size {
	/// Every size, smallest first.
	pub const ALL: [Self; 4] = [Self::Small, Self::Medium, Self::Large, Self::XLarge];

	/// The size label.
	pub const fn label(self) -> &'static str {
		match self {
			Self::Small => "vmon-small",
			Self::Medium => "vmon",
			Self::Large => "vmon-large",
			Self::XLarge => "vmon-xlarge",
		}
	}

	pub const fn vcpus(self) -> u32 {
		match self {
			Self::Small => 2,
			Self::Medium => 4,
			Self::Large => 8,
			Self::XLarge => 16,
		}
	}

	pub const fn memory_mb(self) -> u64 {
		match self {
			Self::Small => 4096,
			Self::Medium => 8192,
			Self::Large => 16384,
			Self::XLarge => 32768,
		}
	}

	fn parse(label: &str) -> Option<Self> {
		Self::ALL
			.into_iter()
			.find(|size| size.label().eq_ignore_ascii_case(label))
	}
}

impl fmt::Display for Size {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.write_str(self.label())
	}
}

/// What a vmon job asks for.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunnerSpec {
	/// The whole vmon label as the job spelled it (the JIT runner's label).
	pub label:    String,
	pub size:     Size,
	/// `from=` template reference (default: `--ci-template`).
	#[serde(default)]
	pub template: Option<String>,
	/// Keep the sandbox of a failed job for [`super::HOLD`].
	#[serde(default)]
	pub debug:    bool,
}

/// Verdict on a job's labels.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Labels {
	/// No vmon label: another runner pool's job.
	NotOurs,
	Spec(RunnerSpec),
	/// A vmon job that must never run, with the reason.
	Refused(String),
}

/// Labels allowed next to the vmon label.
const DEFAULT_LABELS: [&str; 3] = ["self-hosted", "linux", "x64"];

/// Whether `label` belongs to the vmon label family.
fn is_vmon(label: &str) -> bool {
	let head = label.split('/').next().unwrap_or_default();
	head.eq_ignore_ascii_case("vmon")
		|| head
			.get(..5)
			.is_some_and(|prefix| prefix.eq_ignore_ascii_case("vmon-"))
}

/// Parses a job's `runs-on` labels.
pub fn parse(labels: &[String]) -> Labels {
	let ours: Vec<&String> = labels.iter().filter(|label| is_vmon(label)).collect();
	let [label] = ours[..] else {
		if ours.is_empty() {
			return Labels::NotOurs;
		}
		let names: Vec<&str> = ours.iter().map(|label| label.as_str()).collect();
		return Labels::Refused(format!(
			"a job takes exactly one vmon label, got {}",
			names.join(", ")
		));
	};
	if let Some(other) = labels.iter().find(|candidate| {
		!is_vmon(candidate)
			&& !DEFAULT_LABELS
				.iter()
				.any(|default| default.eq_ignore_ascii_case(candidate))
	}) {
		return Labels::Refused(format!(
			"label '{other}' cannot accompany '{label}' (only self-hosted, linux, and x64 can)"
		));
	}
	match parse_label(label) {
		Ok(spec) => Labels::Spec(spec),
		Err(reason) => Labels::Refused(reason),
	}
}

fn parse_label(label: &str) -> Result<RunnerSpec, String> {
	let mut segments = label.split('/');
	let head = segments.next().unwrap_or_default();
	let size = Size::parse(head).ok_or_else(|| {
		let sizes: Vec<&str> = Size::ALL.iter().map(|size| size.label()).collect();
		format!("unknown size '{head}' in '{label}' (sizes: {})", sizes.join(", "))
	})?;
	let mut spec = RunnerSpec { label: label.to_owned(), size, template: None, debug: false };
	let mut segments = segments.peekable();
	while let Some(option) = segments.next() {
		if option.eq_ignore_ascii_case("debug") {
			if spec.debug {
				return Err(format!("duplicate option 'debug' in '{label}'"));
			}
			spec.debug = true;
		} else if let Some(template) = option.strip_prefix("from=") {
			if spec.template.is_some() {
				return Err(format!("duplicate option 'from' in '{label}'"));
			}
			let mut template = template.to_owned();
			// `from=namespace/alias`: the alias is the next segment unless it
			// is an option itself.
			if let Some(alias) =
				segments.next_if(|next| !next.contains('=') && !next.eq_ignore_ascii_case("debug"))
			{
				template.push('/');
				template.push_str(alias);
			}
			if template.is_empty() || template.starts_with('/') || template.ends_with('/') {
				return Err(format!("option 'from' needs a template in '{label}'"));
			}
			spec.template = Some(template);
		} else {
			let name = option.split('=').next().unwrap_or(option);
			return Err(format!("unknown option '{name}' in '{label}' (options: from=, debug)"));
		}
	}
	Ok(spec)
}

#[cfg(test)]
mod tests {
	use super::*;

	fn labels(raw: &[&str]) -> Labels {
		parse(
			&raw
				.iter()
				.map(|label| (*label).to_owned())
				.collect::<Vec<_>>(),
		)
	}

	fn spec(raw: &[&str]) -> RunnerSpec {
		match labels(raw) {
			Labels::Spec(spec) => spec,
			other => panic!("{raw:?}: {other:?}"),
		}
	}

	fn refused(raw: &[&str]) -> String {
		match labels(raw) {
			Labels::Refused(reason) => reason,
			other => panic!("{raw:?}: {other:?}"),
		}
	}

	#[test]
	fn sizes_map_to_vcpus_and_memory() {
		for (label, vcpus, memory) in [
			("vmon-small", 2, 4096),
			("vmon", 4, 8192),
			("vmon-large", 8, 16384),
			("VMON-XLarge", 16, 32768),
		] {
			let spec = spec(&[label]);
			assert_eq!((spec.size.vcpus(), spec.size.memory_mb()), (vcpus, memory), "{label}");
			assert_eq!((spec.template, spec.debug), (None, false));
			assert_eq!(spec.label, label);
		}
	}

	#[test]
	fn options_and_default_labels() {
		let parsed = spec(&["self-hosted", "vmon-large/from=omp-ci", "Linux", "X64"]);
		assert_eq!(parsed.size, Size::Large);
		assert_eq!(parsed.template.as_deref(), Some("omp-ci"));
		assert!(!parsed.debug);

		let parsed = spec(&["vmon-xlarge/from=omp-ci/debug"]);
		assert_eq!((parsed.template.as_deref(), parsed.debug), (Some("omp-ci"), true));

		let parsed = spec(&["vmon/debug/from=acme/omp-ci:v2"]);
		assert_eq!((parsed.template.as_deref(), parsed.debug), (Some("acme/omp-ci:v2"), true));

		let parsed = spec(&["vmon/from=acme/omp-ci/debug"]);
		assert_eq!((parsed.template.as_deref(), parsed.debug), (Some("acme/omp-ci"), true));
	}

	#[test]
	fn foreign_jobs_are_not_ours() {
		assert_eq!(labels(&["ubuntu-latest"]), Labels::NotOurs);
		assert_eq!(labels(&["self-hosted", "linux"]), Labels::NotOurs);
		assert_eq!(labels(&["vmonx"]), Labels::NotOurs);
		assert_eq!(labels(&[]), Labels::NotOurs);
	}

	#[test]
	fn refusals_name_the_problem() {
		assert!(refused(&["vmon-huge"]).contains("unknown size 'vmon-huge'"));
		assert!(refused(&["vmon", "vmon-large"]).contains("exactly one vmon label"));
		assert!(refused(&["vmon", "gpu"]).contains("label 'gpu' cannot accompany 'vmon'"));
		assert!(refused(&["vmon/debug/debug"]).contains("duplicate option 'debug'"));
		assert!(refused(&["vmon/from=a/from=b"]).contains("duplicate option 'from'"));
		assert!(refused(&["vmon/fast"]).contains("unknown option 'fast'"));
		assert!(refused(&["vmon/cpu=4"]).contains("unknown option 'cpu'"));
		assert!(refused(&["vmon/from="]).contains("needs a template"));
	}
}
