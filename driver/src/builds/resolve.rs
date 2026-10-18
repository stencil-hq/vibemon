//! Template reference resolution: template id, `alias`, `namespace/alias`,
//! each with an optional `:tag`; public templates of other teams resolve,
//! deleted templates do not (ARCHITECTURE.md §7).

use super::{
	ResolvedTemplate, artifact_sources,
	names::{TemplateName, looks_like_template_id, parse_name},
};
use crate::{
	error::{ApiError, ApiResult},
	state::AppState,
	store::{Team, builds::BuildStatus, templates::TemplateRow},
};

/// One place a reference may point to, in lookup order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Lookup {
	/// A template id.
	Id(String),
	/// `(namespace, alias)`; namespace `""` is a bare alias.
	Alias { namespace: String, alias: String },
}

/// Where a parsed reference is looked up.
///
/// In order: an id-shaped bare reference as id first, then the caller's
/// namespace, then bare aliases
/// (held only by system teams' public templates, so `base` falls back to the
/// default template unless the caller owns one). An explicit namespace is
/// looked up only there.
pub fn lookup_order(name: &TemplateName, team_slug: &str) -> Vec<Lookup> {
	if let Some(namespace) = &name.namespace {
		return vec![Lookup::Alias { namespace: namespace.clone(), alias: name.alias.clone() }];
	}
	let mut order = Vec::with_capacity(3);
	if looks_like_template_id(&name.alias) {
		order.push(Lookup::Id(name.alias.clone()));
	}
	order.push(Lookup::Alias { namespace: team_slug.to_owned(), alias: name.alias.clone() });
	order.push(Lookup::Alias { namespace: String::new(), alias: name.alias.clone() });
	order
}

/// First live template a reference points to, with the alias used (if any).
pub async fn find_template(
	state: &AppState,
	team: &Team,
	name: &TemplateName,
) -> ApiResult<Option<(TemplateRow, Option<String>)>> {
	for lookup in lookup_order(name, &team.slug) {
		let found = match &lookup {
			Lookup::Id(id) => state.store.template(id).await?.map(|t| (t, None)),
			Lookup::Alias { namespace, alias } => state
				.store
				.template_by_alias(namespace, alias)
				.await?
				.map(|t| (t, Some(alias.clone()))),
		};
		if found.is_some() {
			return Ok(found);
		}
	}
	Ok(None)
}

/// How a caller may use a found template.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TemplateAccess {
	/// The caller's team owns it.
	Owner,
	/// Another team's public template (usable for sandboxes and `fromTemplate`).
	Public,
	/// Another team's private template.
	Denied,
}

impl TemplateAccess {
	/// Classifies `template` for `team_id`.
	pub fn of(template: &TemplateRow, team_id: &str) -> Self {
		if template.team_id == team_id {
			Self::Owner
		} else if template.public {
			Self::Public
		} else {
			Self::Denied
		}
	}
}

/// Upstream wording for unknown templates.
pub(super) fn template_not_found(reference: &str) -> ApiError {
	ApiError::not_found(format!("template '{reference}' not found"))
}

fn no_access(template_id: &str) -> ApiError {
	ApiError::forbidden(format!("you don't have access to template '{template_id}'"))
}

/// Parses `reference` (`400 Invalid template reference`).
fn parse_reference(reference: &str) -> ApiResult<TemplateName> {
	parse_name(reference)
		.map_err(|e| ApiError::bad_request(format!("Invalid template reference: {e}")))
}

/// Template owned by `team` for management endpoints (`{templateID}` path
/// params accept ids and names). Another team's public template is 403,
/// a private or missing one 404.
pub async fn owned_template(
	state: &AppState,
	team: &Team,
	reference: &str,
) -> ApiResult<(TemplateRow, TemplateName)> {
	let name = parse_reference(reference)?;
	let Some((template, _)) = find_template(state, team, &name).await? else {
		return Err(template_not_found(reference));
	};
	match TemplateAccess::of(&template, &team.id) {
		TemplateAccess::Owner => Ok((template, name)),
		TemplateAccess::Public => Err(no_access(&template.id)),
		TemplateAccess::Denied => Err(template_not_found(reference)),
	}
}

/// Resolves a sandbox `templateID` to a ready build with its artifact
/// facts and alive sources, and counts the spawn (written in the background,
/// see [`super::spawns`]).
pub async fn resolve_template(
	state: &AppState,
	team: &Team,
	template_ref: &str,
) -> ApiResult<ResolvedTemplate> {
	let name = parse_reference(template_ref)?;
	let Some((template, alias_used)) = find_template(state, team, &name).await? else {
		return Err(template_not_found(template_ref));
	};
	if TemplateAccess::of(&template, &team.id) == TemplateAccess::Denied {
		return Err(no_access(&template.id));
	}
	let build_id = match state
		.store
		.tag_build(&template.id, name.tag_or_default())
		.await?
	{
		Some(build_id) => build_id,
		None if name.tag.is_some() => {
			return Err(ApiError::not_found(format!(
				"tag '{}' does not exist for template '{}'",
				name.tag_or_default(),
				template.id
			)));
		},
		None => state
			.store
			.latest_ready_builds(std::slice::from_ref(&template.id))
			.await?
			.into_iter()
			.next()
			.map(|build| build.id)
			.ok_or_else(|| {
				ApiError::not_found(format!("template '{}' has no ready build", template.id))
			})?,
	};
	let build = state
		.store
		.build(&build_id)
		.await?
		.ok_or_else(|| template_not_found(template_ref))?;
	let (BuildStatus::Ready, Some(artifact)) = (build.status(), build.artifact) else {
		return Err(ApiError::not_found(format!("template '{}' has no ready build", template.id)));
	};
	let sources = artifact_sources(state, &build_id).await?;
	let alias = match alias_used {
		Some(alias) => Some(alias),
		None => state
			.store
			.aliases_of(std::slice::from_ref(&template.id))
			.await?
			.into_iter()
			.next()
			.map(|a| a.alias),
	};
	state.spawns.record(&template.id);
	Ok(ResolvedTemplate { template_id: template.id, build_id, alias, artifact, sources })
}

#[cfg(test)]
mod tests {
	use chrono::Utc;

	use super::*;

	fn alias(namespace: &str, alias: &str) -> Lookup {
		Lookup::Alias { namespace: namespace.into(), alias: alias.into() }
	}

	#[test]
	fn bare_alias_tries_team_namespace_then_bare() {
		let name = parse_name("my-tpl:v1").unwrap();
		assert_eq!(lookup_order(&name, "acme"), [alias("acme", "my-tpl"), alias("", "my-tpl")]);
	}

	#[test]
	fn id_shaped_reference_is_tried_as_id_first() {
		let name = parse_name("abcdefghij0123456789").unwrap();
		assert_eq!(lookup_order(&name, "acme")[0], Lookup::Id("abcdefghij0123456789".into()));
		assert_eq!(lookup_order(&name, "acme").len(), 3);
	}

	#[test]
	fn explicit_namespace_is_exclusive() {
		let name = parse_name("other-team/tpl").unwrap();
		assert_eq!(lookup_order(&name, "acme"), [alias("other-team", "tpl")]);
	}

	fn template(team: &str, public: bool) -> TemplateRow {
		TemplateRow {
			id: "t".into(),
			team_id: team.into(),
			public,
			kind: "template".into(),
			source_sandbox_id: None,
			created_at: Utc::now(),
			updated_at: Utc::now(),
			spawn_count: 0,
			last_spawned_at: None,
			build_count: 0,
			deleted_at: None,
		}
	}

	#[test]
	fn access_classification() {
		assert_eq!(TemplateAccess::of(&template("a", false), "a"), TemplateAccess::Owner);
		assert_eq!(TemplateAccess::of(&template("a", true), "a"), TemplateAccess::Owner);
		assert_eq!(TemplateAccess::of(&template("a", true), "b"), TemplateAccess::Public);
		assert_eq!(TemplateAccess::of(&template("a", false), "b"), TemplateAccess::Denied);
	}
}
