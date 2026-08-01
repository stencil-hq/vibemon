//! gRPC surface of the v1 API (proto/vmon/v1/api.proto).
//!
//! Eleven tonic services implemented against [`EngineApi`] or their dedicated
//! domains, mounted into the axum router next to the kept HTTP routes (healthz,
//! metrics, SSE, WS, ports proxy, static web). Every RPC failure is a gRPC
//! status carrying the stable vmond error code in `vmon-code` response
//! metadata. Requests for sandboxes owned by a mesh peer are re-issued over a
//! tonic channel to the owner.

use std::{
	collections::{BTreeMap, HashMap, HashSet},
	io::{Read as _, Seek as _, SeekFrom},
	pin::Pin,
	sync::Arc,
	thread,
};

use axum::http::HeaderMap;
use serde_json::{Value, json};
use tokio::sync::mpsc;
use tokio_stream::{Stream, StreamExt as _, wrappers::ReceiverStream};
use tonic::{
	Code, Request, Response, Status, Streaming,
	metadata::{MetadataMap, MetadataValue},
	service::Routes,
	transport::{Channel, Endpoint},
};
use vmon_proto::v1 as pb;

use crate::{
	ErrorCode,
	api::{
		error::{ApiError, ApiResult, join_error},
		routes::compact_json,
		sandbox_stream,
		state::{ApiState, PRINCIPAL_KEY_HEADER, PRINCIPAL_ROLE_HEADER, PRINCIPAL_TENANT_HEADER},
		validation,
	},
	engine::{
		EngineApi, ExecExit, ExecStream as EngineExecStream, pty::PtyStream as EnginePtyStream,
	},
	image::{self, normalize_oci_arch},
	mesh::{
		proxy::{self, MeshError, MeshPeer, OwnerProxyDecision, OwnerRecord},
		routes::{CreateRecordWire, MeshControl, MeshRecordStore, MeshRouteState, apply_view_detail},
		runtime::record_is_staging,
	},
	models::{ExecBody, ExtendBody, ForkBody, NetworkBody, PoolPutBody, RestoreBody, SandboxCreate},
	security::{
		Principal,
		credentials::{Credential, CredentialMetadata},
	},
};

/// Mirror of the old REST `BODY_LIMIT`, applied to encode and decode on the
/// server, on every forwarding client, and on `/grpc` bridge frames.
pub(super) const MAX_MESSAGE_SIZE: usize = 64 * 1024 * 1024;
const MESH_HOP_KEY: &str = "x-vmon-mesh-hop";

pub(super) type BoxStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send>>;
const ARTIFACT_CHUNK_SIZE: usize = 64 * 1024;

fn stream_verified_artifact(
	mut reader: crate::function::artifact::VerifiedArtifactReader,
	offset: u64,
	length: u64,
) -> BoxStream<pb::ArtifactChunk> {
	let (tx, rx) = mpsc::channel(8);
	tokio::task::spawn_blocking(move || {
		if let Err(error) = reader.seek(SeekFrom::Start(offset)) {
			let _ = tx.blocking_send(Err(status_from(&ApiError::from(crate::EngineError::engine(
				error.to_string(),
			)))));
			return;
		}
		if length == 0 {
			let _ = tx.blocking_send(Ok(pb::ArtifactChunk { offset, data: Vec::new(), eof: true }));
			return;
		}
		let mut remaining = length;
		let mut frame_offset = offset;
		while remaining != 0 {
			let frame_len = remaining.min(ARTIFACT_CHUNK_SIZE as u64) as usize;
			let mut data = vec![0_u8; frame_len];
			if let Err(error) = reader.read_exact(&mut data) {
				let _ = tx.blocking_send(Err(status_from(&ApiError::from(
					crate::EngineError::engine(error.to_string()),
				))));
				return;
			}
			remaining -= frame_len as u64;
			let frame = pb::ArtifactChunk { offset: frame_offset, data, eof: remaining == 0 };
			frame_offset += frame_len as u64;
			if tx.blocking_send(Ok(frame)).is_err() {
				return;
			}
		}
	});
	Box::pin(ReceiverStream::new(rx))
}

fn stream_call_event_pages<F>(mut load_page: F, after_sequence: u64) -> BoxStream<pb::CallEvent>
where
	F: FnMut(u64) -> crate::Result<Vec<pb::CallEvent>> + Send + 'static,
{
	const PAGE_SIZE: usize = 10_000;
	let (tx, rx) = mpsc::channel(32);
	tokio::task::spawn_blocking(move || {
		let mut cursor = after_sequence;
		loop {
			let events = match load_page(cursor) {
				Ok(events) => events,
				Err(error) => {
					let _ = tx.blocking_send(Err(status_from(&ApiError::from(error))));
					return;
				},
			};
			let page_len = events.len();
			for event in events {
				if event.sequence <= cursor {
					let _ = tx.blocking_send(Err(Status::internal("call event replay did not advance")));
					return;
				}
				cursor = event.sequence;
				if tx.blocking_send(Ok(event)).is_err() {
					return;
				}
			}
			if page_len < PAGE_SIZE {
				return;
			}
		}
	});
	Box::pin(ReceiverStream::new(rx))
}

fn stream_persisted_call_events(
	domain: Arc<crate::function::FunctionDomain>,
	call_id: String,
	after_sequence: u64,
) -> BoxStream<pb::CallEvent> {
	stream_call_event_pages(
		move |cursor| domain.store().events_after(&call_id, cursor, 10_000),
		after_sequence,
	)
}

enum ArtifactUploadFrame {
	Chunk(Vec<u8>),
	Finish,
	Abort,
}

/// Map an [`ApiError`] onto the contract's gRPC status table with stable code,
/// retryability, and recovery guidance metadata.
pub fn status_from(err: &ApiError) -> Status {
	let code = match err.code() {
		"not_found" => Code::NotFound,
		"invalid" => Code::InvalidArgument,
		"forbidden" => Code::PermissionDenied,
		"unauthorized" if err.status() == axum::http::StatusCode::FORBIDDEN => Code::PermissionDenied,
		"unauthorized" => Code::Unauthenticated,
		"not_running" | "actor_lost" | "unavailable_secret" | "ha_unavailable" => {
			Code::FailedPrecondition
		},
		"busy" | "conflict" => Code::Aborted,
		"checksum" => Code::DataLoss,
		"deadline" => Code::DeadlineExceeded,
		"unsupported" => Code::Unimplemented,
		// engine_error plus mesh transport codes (unreachable, ambiguous, …).
		_ => Code::Unavailable,
	};
	let mut status = Status::new(code, err.message().to_owned());
	if let Ok(value) = MetadataValue::try_from(err.code()) {
		status.metadata_mut().insert("vmon-code", value);
	}
	status.metadata_mut().insert(
		"vmon-retryable",
		MetadataValue::from_static(if err.retryable() { "true" } else { "false" }),
	);
	if let Ok(value) = MetadataValue::try_from(err.action()) {
		status.metadata_mut().insert("vmon-action", value);
	}
	status
}

impl From<ApiError> for Status {
	fn from(err: ApiError) -> Self {
		status_from(&err)
	}
}

/// Build every v1 service with uniform 64 MiB message limits. The same
/// [`Routes`] instance shape serves native h2c and the `/grpc` bridge.
pub(super) fn service(state: ApiState) -> Routes {
	let api = GrpcApi::new(state);
	Routes::default()
		.add_service(
			pb::sandbox_service_server::SandboxServiceServer::new(api.clone())
				.max_decoding_message_size(MAX_MESSAGE_SIZE)
				.max_encoding_message_size(MAX_MESSAGE_SIZE),
		)
		.add_service(
			pb::snapshot_service_server::SnapshotServiceServer::new(api.clone())
				.max_decoding_message_size(MAX_MESSAGE_SIZE)
				.max_encoding_message_size(MAX_MESSAGE_SIZE),
		)
		.add_service(
			pb::credential_service_server::CredentialServiceServer::new(api.clone())
				.max_decoding_message_size(MAX_MESSAGE_SIZE)
				.max_encoding_message_size(MAX_MESSAGE_SIZE),
		)
		.add_service(
			pb::volume_service_server::VolumeServiceServer::new(api.clone())
				.max_decoding_message_size(MAX_MESSAGE_SIZE)
				.max_encoding_message_size(MAX_MESSAGE_SIZE),
		)
		.add_service(
			pb::pool_service_server::PoolServiceServer::new(api.clone())
				.max_decoding_message_size(MAX_MESSAGE_SIZE)
				.max_encoding_message_size(MAX_MESSAGE_SIZE),
		)
		.add_service(
			pb::vpc_service_server::VpcServiceServer::new(api.clone())
				.max_decoding_message_size(MAX_MESSAGE_SIZE)
				.max_encoding_message_size(MAX_MESSAGE_SIZE),
		)
		.add_service(
			pb::artifact_service_server::ArtifactServiceServer::new(api.clone())
				.max_decoding_message_size(MAX_MESSAGE_SIZE)
				.max_encoding_message_size(MAX_MESSAGE_SIZE),
		)
		.add_service(
			pb::function_service_server::FunctionServiceServer::new(api.clone())
				.max_decoding_message_size(MAX_MESSAGE_SIZE)
				.max_encoding_message_size(MAX_MESSAGE_SIZE),
		)
		.add_service(
			pb::call_service_server::CallServiceServer::new(api.clone())
				.max_decoding_message_size(MAX_MESSAGE_SIZE)
				.max_encoding_message_size(MAX_MESSAGE_SIZE),
		)
		.add_service(
			pb::actor_service_server::ActorServiceServer::new(api.clone())
				.max_decoding_message_size(MAX_MESSAGE_SIZE)
				.max_encoding_message_size(MAX_MESSAGE_SIZE),
		)
		.add_service(
			pb::system_service_server::SystemServiceServer::new(api)
				.max_decoding_message_size(MAX_MESSAGE_SIZE)
				.max_encoding_message_size(MAX_MESSAGE_SIZE),
		)
}

/// The gRPC sub-router mounted into axum for native h2c clients.
pub(super) fn routes(state: ApiState) -> axum::Router {
	service(state).prepare().into_axum_router()
}

#[derive(Clone)]
pub struct GrpcApi {
	state: ApiState,
}

/// Resolved mesh owner hop: a lazy tonic channel plus the peer credential.
struct ForwardTarget {
	owner:     String,
	channel:   Channel,
	token:     Arc<str>,
	principal: Principal,
}

impl ForwardTarget {
	fn request<T>(&self, message: T) -> Request<T> {
		let mut request = Request::new(message);
		forward_metadata(request.metadata_mut(), &self.token, &self.principal);
		request
	}

	fn sandbox_client(&self) -> pb::sandbox_service_client::SandboxServiceClient<Channel> {
		pb::sandbox_service_client::SandboxServiceClient::new(self.channel.clone())
			.max_decoding_message_size(MAX_MESSAGE_SIZE)
			.max_encoding_message_size(MAX_MESSAGE_SIZE)
	}
}

fn forward_metadata(metadata: &mut MetadataMap, token: &str, principal: &Principal) {
	if !token.is_empty()
		&& let Ok(value) = MetadataValue::try_from(format!("Bearer {token}"))
	{
		metadata.insert("authorization", value);
	}
	metadata.insert(MESH_HOP_KEY, MetadataValue::from_static("1"));
	insert_principal_metadata(metadata, principal);
}

fn insert_principal_metadata(metadata: &mut MetadataMap, principal: &Principal) {
	let role = if principal.is_admin() {
		"admin"
	} else {
		"client"
	};
	for (name, value) in [
		(PRINCIPAL_TENANT_HEADER, principal.tenant.as_str()),
		(PRINCIPAL_ROLE_HEADER, role),
		(PRINCIPAL_KEY_HEADER, principal.key_id.as_str()),
	] {
		if let Ok(value) = MetadataValue::try_from(value) {
			metadata.insert(name, value);
		}
	}
}

fn metadata_principal(metadata: &MetadataMap) -> Result<Principal, Status> {
	let role = metadata
		.get(PRINCIPAL_ROLE_HEADER)
		.and_then(|value| value.to_str().ok());
	match role {
		Some("admin") => Ok(Principal::local_admin()),
		Some("client") => {
			let tenant = metadata
				.get(PRINCIPAL_TENANT_HEADER)
				.and_then(|value| value.to_str().ok());
			let key_id = metadata
				.get(PRINCIPAL_KEY_HEADER)
				.and_then(|value| value.to_str().ok());
			let (Some(tenant), Some(key_id)) = (tenant, key_id) else {
				return Err(status_from(&ApiError::unauthorized(
					"authenticated principal metadata is incomplete",
				)));
			};
			Principal::client(tenant, key_id).map_err(|_| {
				status_from(&ApiError::unauthorized("authenticated principal metadata is invalid"))
			})
		},
		_ => Err(status_from(&ApiError::unauthorized("authenticated principal metadata is missing"))),
	}
}

fn request_principal<T>(request: &Request<T>) -> Result<Principal, Status> {
	request
		.extensions()
		.get::<Principal>()
		.cloned()
		.map_or_else(|| metadata_principal(request.metadata()), Ok)
}
fn scope_sandbox_create(principal: &Principal, body: &mut SandboxCreate) -> Result<(), ApiError> {
	if principal.is_admin() {
		return Ok(());
	}
	if body.template.is_some()
		|| body.dockerfile.is_some()
		|| body.fs_dir.is_some()
		|| body
			.volumes
			.as_ref()
			.is_some_and(|volumes| !volumes.is_empty())
		|| body
			.s3_mounts
			.as_ref()
			.is_some_and(|mounts| !mounts.is_empty())
		|| body.pool_size != 0
	{
		return Err(ApiError::forbidden(
			"tenant sandboxes cannot use host-local templates, builds, mounts, volumes, or pools",
		));
	}
	if body
		.image
		.as_deref()
		.is_some_and(|reference| !image::is_registry_reference(reference))
	{
		return Err(ApiError::forbidden("tenant sandboxes require a registry image reference"));
	}
	body.owner_tenant.clone_from(&principal.tenant);
	body.encryption_key_id.clone_from(&principal.key_id);
	if let Some(key) = body.idempotency_key.as_mut().filter(|key| !key.is_empty()) {
		*key = format!("tenant:{}:{key}", principal.tenant);
	}
	Ok(())
}

fn is_mesh_hop(metadata: &MetadataMap) -> bool {
	metadata.contains_key(MESH_HOP_KEY)
}

/// Adapter exposing [`MeshRouteState`] through the owner-routing traits so the
/// gRPC layer reuses the exact REST forwarding decision (locate scatter,
/// durable record fallback, hop and presence early exits).
struct MeshView {
	state:   MeshRouteState,
	node_id: String,
	runtime: Arc<crate::mesh::runtime::MeshRuntime>,
}

impl proxy::OwnerRouter for MeshView {
	fn mesh_enabled(&self) -> bool {
		self.state.mesh.enabled()
	}

	fn local_node_id(&self) -> &str {
		&self.node_id
	}

	fn owner_of(&self, sandbox_id: &str) -> proxy::MeshResult<Option<String>> {
		self
			.state
			.mesh
			.owner_of(sandbox_id)
			.map_err(|err| proxy::MeshError::new(err.code, err.message))
	}

	fn authoritative_owner(&self, sandbox_id: &str) -> proxy::MeshResult<Option<OwnerRecord>> {
		self
			.state
			.mesh
			.authoritative_owner(sandbox_id)
			.map_err(|err| proxy::MeshError::new(err.code, err.message))
			.map(|owner| {
				owner.map(|(owner, epoch)| OwnerRecord {
					owner,
					epoch: u64::try_from(epoch).unwrap_or(0),
				})
			})
	}

	fn local_epoch(&self, sandbox_id: &str) -> u64 {
		u64::try_from(self.state.mesh.local_epoch(sandbox_id)).unwrap_or(0)
	}

	fn local_restore_staging(&self, sandbox_id: &str) -> bool {
		self.runtime.restoring_epoch(sandbox_id).is_some()
	}

	fn fence_local(&self, sandbox_id: &str, expected_epoch: u64) {
		self
			.runtime
			.schedule_fence(sandbox_id.to_owned(), expected_epoch);
	}

	fn peer_url(&self, node_id: &str) -> Option<String> {
		self.state.mesh.peer_url(node_id)
	}

	fn peers(&self) -> Vec<MeshPeer> {
		self
			.state
			.mesh
			.peers()
			.into_iter()
			.map(|peer| MeshPeer { node_id: peer.node_id, advertise: peer.advertise })
			.collect()
	}

	fn record_owner(&self, sandbox_id: &str, owner: &str, epoch: u64) {
		self
			.state
			.mesh
			.record_owner(sandbox_id, owner, i64::try_from(epoch).unwrap_or(i64::MAX));
	}
}

impl proxy::SandboxPresence for MeshView {
	fn has_sandbox(&self, sandbox_id: &str) -> bool {
		self.state.engine.has_sandbox(sandbox_id)
	}
}

impl proxy::RecordOwnerLookup for MeshView {
	fn owner_record(&self, sandbox_id: &str) -> proxy::MeshResult<Option<OwnerRecord>> {
		self
			.state
			.records
			.get(sandbox_id)
			.map_err(|error| proxy::MeshError::new("internal", error.to_string()))
			.map(|record| {
				record.map(|record| OwnerRecord {
					owner: record.owner,
					epoch: u64::try_from(record.epoch).unwrap_or(0),
				})
			})
	}
}

fn mesh_api_error(err: MeshError) -> ApiError {
	ApiError::new(err.status(), err.code, err.message)
}

fn function_ha_admission(
	spec: &pb::FunctionSpec,
	nodes: &[crate::mesh::state::NodeState],
	remote_execution_available: bool,
) -> ApiResult<()> {
	let resources = spec.resources.as_ref();
	let policy = resources
		.and_then(|resources| pb::HighAvailabilityPolicy::try_from(resources.high_availability).ok())
		.unwrap_or(pb::HighAvailabilityPolicy::Unspecified);
	let scope = match policy {
		pb::HighAvailabilityPolicy::Host => "host",
		pb::HighAvailabilityPolicy::Zone => "zone",
		pb::HighAvailabilityPolicy::Unspecified | pb::HighAvailabilityPolicy::None => return Ok(()),
	};
	let required_arch = resources
		.and_then(|resources| pb::CpuArchitecture::try_from(resources.architecture).ok())
		.and_then(|arch| match arch {
			pb::CpuArchitecture::Amd64 => Some("x86_64"),
			pb::CpuArchitecture::Arm64 => Some("aarch64"),
			pb::CpuArchitecture::Unspecified => None,
		});
	let compatible = nodes
		.iter()
		.filter(|node| {
			required_arch.is_none_or(|required| {
				normalize_oci_arch(Some(&node.arch)).as_deref() == Some(required)
			})
		})
		.collect::<Vec<_>>();
	let domains = match policy {
		pb::HighAvailabilityPolicy::Host => compatible
			.iter()
			.map(|node| node.node_id.as_str())
			.filter(|host| !host.is_empty())
			.collect::<HashSet<_>>(),
		pb::HighAvailabilityPolicy::Zone => compatible
			.iter()
			.map(|node| node.region.as_str())
			.filter(|zone| !zone.is_empty())
			.collect::<HashSet<_>>(),
		pb::HighAvailabilityPolicy::Unspecified | pb::HighAvailabilityPolicy::None => {
			unreachable!("non-HA policies returned above")
		},
	};
	if domains.len() < 2 {
		return Err(ApiError::function(
			"ha_unavailable",
			format!(
				"function HA {scope} requires at least 2 compatible mesh {scope}s; found {}; \
				 cross-node function execution is unavailable",
				domains.len()
			),
		));
	}
	if !remote_execution_available {
		return Err(ApiError::function(
			"ha_unavailable",
			format!(
				"function HA {scope} cannot be admitted: cross-node function execution is \
				 unavailable; use HIGH_AVAILABILITY_POLICY_NONE"
			),
		));
	}
	Ok(())
}

/// Relay a forwarded server-stream response as this server's boxed stream,
/// preserving response metadata; per-item statuses (incl. `vmon-code`) flow
/// through untouched.
fn relay_stream<T: Send + 'static>(response: Response<Streaming<T>>) -> Response<BoxStream<T>> {
	let (metadata, stream, extensions) = response.into_parts();
	let stream: BoxStream<T> = Box::pin(stream);
	Response::from_parts(metadata, stream, extensions)
}

/// Forward a sandbox-scoped RPC to the mesh owner when one is resolved. The
/// borrow of `$message` for the id ends before the message is moved into the
/// forwarded request.
macro_rules! forward_to_owner {
	($self:ident, $metadata:ident, $id:expr, $message:ident, $method:ident) => {
		if let Some(target) = $self.forward_target(&$metadata, $id).await? {
			return target
				.sandbox_client()
				.$method(target.request($message))
				.await;
		}
	};
	($self:ident, $metadata:ident, $id:expr, $message:ident, $method:ident,stream) => {
		if let Some(target) = $self.forward_target(&$metadata, $id).await? {
			return target
				.sandbox_client()
				.$method(target.request($message))
				.await
				.map(relay_stream);
		}
	};
}

/// Whether portable history should be served here, forwarded to its owner, or
/// adopted after owner resolution conclusively reports that the owner is gone.
enum PortableOwnerRoute<T> {
	ServeLocal,
	Forward(T),
	Adopt,
}

/// Keep normal owner forwarding for portable history operations, but permit
/// adoption only when owner resolution conclusively reports that the owner is
/// unreachable.
fn portable_owner_route<T>(
	result: Result<Option<T>, Status>,
) -> Result<PortableOwnerRoute<T>, Status> {
	match result {
		Ok(Some(target)) => Ok(PortableOwnerRoute::Forward(target)),
		Ok(None) => Ok(PortableOwnerRoute::ServeLocal),
		Err(status)
			if status
				.metadata()
				.get("vmon-code")
				.and_then(|value| value.to_str().ok())
				== Some("unreachable") =>
		{
			Ok(PortableOwnerRoute::Adopt)
		},
		Err(status) => Err(status),
	}
}

/// A peer connection failure produces tonic `Unavailable` before any response
/// metadata exists. Responses produced by this API always carry `vmon-code`;
/// preserve those application errors because a mutation may have committed.
fn portable_peer_response<T>(response: Result<T, Status>) -> Result<Option<T>, Status> {
	match response {
		Ok(response) => Ok(Some(response)),
		Err(status)
			if status.code() == Code::Unavailable && !status.metadata().contains_key("vmon-code") =>
		{
			Ok(None)
		},
		Err(status) => Err(status),
	}
}

/// A projected suspended marker is non-serving, but its designated local
/// claimant must receive exactly Resume to begin the fenced materialization.
const fn resume_forwards_to_owner(passive_suspend_marker: bool) -> bool {
	!passive_suspend_marker
}

/// Only an owner-resolution failure tagged `unreachable` proves no peer RPC
/// was attempted and may begin suspended-marker adoption.
fn resume_owner_resolution_lost(status: &Status) -> bool {
	status
		.metadata()
		.get("vmon-code")
		.and_then(|value| value.to_str().ok())
		== Some("unreachable")
}

fn portable_restore_proof(
	owner_matches: bool,
	owner_healthy: bool,
	confirmations: usize,
	quorum_needed: usize,
) -> Result<(), Status> {
	if !owner_matches || owner_healthy || confirmations < quorum_needed {
		return Err(status_from(&ApiError::function(
			"ha_unavailable",
			"portable rollback requires a fenced dead-owner restore quorum",
		)));
	}
	Ok(())
}

impl GrpcApi {
	pub const fn new(state: ApiState) -> Self {
		Self { state }
	}

	async fn engine_call<T, F>(&self, call: F) -> Result<T, Status>
	where
		T: Send + 'static,
		F: FnOnce(Arc<dyn EngineApi>) -> crate::Result<T> + Send + 'static,
	{
		let engine = self.state.engine.clone();
		tokio::task::spawn_blocking(move || call(engine))
			.await
			.map_err(|err| status_from(&join_error(err)))?
			.map_err(|err| status_from(&ApiError::from(err)))
	}

	/// Shared create pipeline for unary `Create` and streamed `BatchCreate`:
	/// parse, validate, tenant-scope, admit, then boot via mesh or engine.
	///
	/// With `no_wait` the sandbox name is assigned here and the boot moves to
	/// a detached task together with the admission guard; the admitted
	/// identity is returned immediately with status `starting`.
	async fn create_sandbox_pipeline(
		&self,
		principal: &Principal,
		mesh_hop: bool,
		request: pb::CreateSandboxRequest,
	) -> Result<Value, ApiError> {
		let value: Value = serde_json::from_str(&request.spec_json)
			.map_err(|_| ApiError::invalid("invalid request"))?;
		validation::validate_create_value(&value)?;
		let mut body: SandboxCreate = validation::from_value(value)?;
		scope_sandbox_create(principal, &mut body)?;
		validation::validate_create(&body)?;
		if request.no_wait && body.name.as_deref().is_none_or(str::is_empty) {
			body.name = Some(sandbox_stream::generate_sid());
		}
		let value = serde_json::to_value(&body).map_err(|error| {
			ApiError::from(crate::EngineError::engine(format!("serializing sandbox create: {error}")))
		})?;
		// Orch mode: reserve an admission slot so schedulers get a
		// deterministic busy rejection; the guard spans the boot, detached
		// or not.
		let admit = match self.state.orch_worker.as_ref() {
			Some(worker) => Some(worker.admit()?),
			None => None,
		};
		let mesh = self
			.state
			.mesh
			.clone()
			.filter(|mesh| mesh.mesh_enabled() && !mesh_hop);
		if request.no_wait {
			let sid = body.name.clone().unwrap_or_default();
			let owner_tenant = body.owner_tenant.clone();
			// Detached boot: the admission guard moves with it, so the slot
			// stays held until the boot settles. Failures only warn here —
			// the facade publishes the `failed` event Watch clients consume.
			if let Some(mesh) = mesh {
				let params = value.as_object().cloned().unwrap_or_default();
				let boot_sid = sid.clone();
				tokio::spawn(async move {
					let _admit = admit;
					if let Err(error) = mesh.create_sandbox(params).await {
						tracing::warn!(sid = %boot_sid, %error, "no_wait mesh sandbox create failed");
					}
				});
			} else {
				let engine = self.state.engine.clone();
				let boot_gate = self.state.boot_gate.clone();
				let boot_sid = sid.clone();
				tokio::spawn(async move {
					// Queue behind the boot gate without pinning a blocking-pool
					// thread; the admission slot stays reserved while queued.
					let Ok(permit) = boot_gate.acquire_owned().await else {
						return; // the gate is never closed
					};
					let _ = tokio::task::spawn_blocking(move || {
						let _admit = admit;
						let _permit = permit;
						if let Err(error) = engine.create(body) {
							tracing::warn!(sid = %boot_sid, %error, "no_wait sandbox create failed");
						}
					})
					.await;
				});
			}
			return Ok(json!({
				"id": sid,
				"name": sid,
				"status": "starting",
				"owner_tenant": owner_tenant,
			}));
		}
		let _admit = admit;
		if let Some(mesh) = mesh {
			mesh
				.create_sandbox(value.as_object().cloned().unwrap_or_default())
				.await
				.map_err(ApiError::from)
		} else {
			let engine = self.state.engine.clone();
			// Local boots are CPU-heavy; the gate bounds their concurrency while
			// the caller's gRPC deadline still governs the wait.
			let _permit = self.state.boot_gate.acquire().await.ok();
			tokio::task::spawn_blocking(move || engine.create(body))
				.await
				.map_err(join_error)?
				.map_err(ApiError::from)
		}
	}

	fn admit_function_ha(&self, spec: &pb::FunctionSpec) -> Result<(), Status> {
		let nodes = self
			.state
			.mesh
			.as_ref()
			.map_or_else(Vec::new, |mesh| mesh.function_placement_nodes());
		// Sandbox forwarding has no function-worker equivalent: peer function
		// stores, artifacts, call leases, and result streams are process-local.
		function_ha_admission(spec, &nodes, false).map_err(|error| status_from(&error))
	}

	async fn function_call<T, F>(&self, call: F) -> Result<T, Status>
	where
		T: Send + 'static,
		F: FnOnce(Arc<crate::function::FunctionDomain>) -> crate::Result<T> + Send + 'static,
	{
		let domain = self.state.functions.clone();
		tokio::task::spawn_blocking(move || call(domain))
			.await
			.map_err(|err| status_from(&join_error(err)))?
			.map_err(|err| status_from(&ApiError::from(err)))
	}

	async fn authorize_sandbox(
		&self,
		principal: &Principal,
		sandbox_id: &str,
	) -> Result<(), Status> {
		if principal.is_admin() || sandbox_id.is_empty() {
			return Ok(());
		}
		if let Some(mesh) = self.state.mesh.as_ref() {
			let record = MeshRecordStore::get(mesh.as_ref(), sandbox_id)
				.map_err(|error| status_from(&gossip_mesh_api_error(error)))?;
			if let Some(record) = record {
				let owner = record
					.params
					.get("owner_tenant")
					.and_then(Value::as_str)
					.unwrap_or("default");
				return principal
					.require_tenant(owner)
					.map_err(|error| status_from(&ApiError::from(error)));
			}
		}
		let engine = self.state.engine.clone();
		let sandbox_id = sandbox_id.to_owned();
		let result = tokio::task::spawn_blocking(move || engine.get(&sandbox_id))
			.await
			.map_err(|error| status_from(&join_error(error)))?;
		match result {
			Ok(view) => {
				let owner = view
					.get("owner_tenant")
					.and_then(Value::as_str)
					.unwrap_or("default");
				principal
					.require_tenant(owner)
					.map_err(|error| status_from(&ApiError::from(error)))
			},
			Err(error) if error.code == ErrorCode::NotFound => Ok(()),
			Err(error) => Err(status_from(&ApiError::from(error))),
		}
	}

	/// A durable suspended marker projected on this node is intentionally
	/// non-serving, except that Resume must reach the local Engine to claim and
	/// materialize it. Active restore staging remains blocked by normal routing.
	async fn local_passive_resume(
		&self,
		metadata: &MetadataMap,
		sandbox_id: &str,
	) -> Result<bool, Status> {
		let principal = metadata_principal(metadata)?;
		self.authorize_sandbox(&principal, sandbox_id).await?;
		let Some(mesh) = self.state.mesh.as_ref() else {
			return Ok(false);
		};
		let mesh = Arc::clone(mesh);
		let sandbox_id = sandbox_id.to_owned();
		tokio::task::spawn_blocking(move || mesh.passive_suspend_marker(&sandbox_id))
			.await
			.map_err(|error| status_from(&join_error(error)))?
			.map_err(|error| status_from(&ApiError::from(error)))
	}

	/// Query only the durable suspended marker state; unlike
	/// `local_passive_resume`, this works before a local placeholder exists.
	async fn has_durable_suspended_marker(&self, sandbox_id: &str) -> Result<bool, Status> {
		let Some(mesh) = self.state.mesh.as_ref() else {
			return Ok(false);
		};
		let mesh = Arc::clone(mesh);
		let sandbox_id = sandbox_id.to_owned();
		tokio::task::spawn_blocking(move || mesh.has_suspended_marker(&sandbox_id))
			.await
			.map_err(|error| status_from(&join_error(error)))?
			.map_err(|error| status_from(&ApiError::from(error)))
	}

	async fn adopt_suspended_marker(&self, sandbox_id: &str) -> Result<(), Status> {
		let sandbox_id = sandbox_id.to_owned();
		self
			.engine_call(move |engine| engine.adopt_suspended_marker(&sandbox_id))
			.await
	}

	fn credential_scope(
		&self,
		principal: &Principal,
		requested: Option<String>,
	) -> Result<(String, String), Status> {
		if principal.is_admin() {
			let tenant = requested
				.filter(|tenant| !tenant.is_empty())
				.unwrap_or_else(|| "default".to_owned());
			let key_id = self
				.state
				.config
				.tenant_keys
				.get(&tenant)
				.cloned()
				.unwrap_or_else(|| "default".to_owned());
			let principal = Principal::client(tenant, key_id)
				.map_err(|error| status_from(&ApiError::from(error)))?;
			return Ok((principal.tenant, principal.key_id));
		}
		if requested
			.as_deref()
			.is_some_and(|tenant| !tenant.is_empty() && tenant != principal.tenant)
		{
			return Err(status_from(&ApiError::forbidden("credential tenant does not match caller")));
		}
		Ok((principal.tenant.clone(), principal.key_id.clone()))
	}

	/// Resolve the mesh owner hop for a sandbox-scoped RPC. `None` means serve
	/// locally: mesh disabled, request already a hop, sandbox present here, or
	/// no owner known anywhere.
	async fn forward_target(
		&self,
		metadata: &MetadataMap,
		sandbox_id: &str,
	) -> Result<Option<ForwardTarget>, Status> {
		let principal = metadata_principal(metadata)?;
		self.authorize_sandbox(&principal, sandbox_id).await?;
		let Some(mesh) = self.state.mesh.as_ref() else {
			return Ok(None);
		};
		if sandbox_id.is_empty() || !mesh.mesh_enabled() || is_mesh_hop(metadata) {
			return Ok(None);
		}
		let state = mesh.route_state();
		let view = MeshView { node_id: state.mesh.node_id(), state, runtime: Arc::clone(mesh) };
		let path = format!("/v1/sandboxes/{sandbox_id}");
		let decision = proxy::owner_proxy_decision(
			&view,
			&view,
			Some(&view),
			view.state.transport.client(),
			&path,
			&HeaderMap::new(),
			&view.state.outbound_token,
		)
		.await
		.map_err(|err| status_from(&mesh_api_error(err)))?;
		match decision {
			OwnerProxyDecision::ServeLocal => Ok(None),
			OwnerProxyDecision::Forward { owner, peer_url } => {
				let endpoint = Endpoint::from_shared(peer_url).map_err(|err| {
					status_from(&mesh_api_error(MeshError::invalid(format!("invalid peer URL: {err}"))))
				})?;
				Ok(Some(ForwardTarget {
					owner,
					channel: endpoint.connect_lazy(),
					token: view.state.outbound_token.clone(),
					principal,
				}))
			},
		}
	}

	/// Portable history may be adopted after a source node is definitively
	/// unavailable. Do not apply this exception to other sandbox operations.
	async fn portable_history_target(
		&self,
		metadata: &MetadataMap,
		sandbox_id: &str,
	) -> Result<PortableOwnerRoute<ForwardTarget>, Status> {
		portable_owner_route(self.forward_target(metadata, sandbox_id).await)
	}

	/// Admit an owner-loss rollback only when the ownership record still names
	/// the failed peer and the surviving membership has restore quorum.
	fn portable_rollback_admission(
		&self,
		sandbox_id: &str,
		expected_owner: Option<&str>,
	) -> Result<(), Status> {
		let Some(mesh) = self.state.mesh.as_ref() else {
			return Err(status_from(&ApiError::function(
				"ha_unavailable",
				"portable rollback requires mesh restore quorum",
			)));
		};
		let state = mesh.route_state();
		let (owner, _) = state
			.mesh
			.authoritative_owner(sandbox_id)
			.map_err(|_| {
				status_from(&ApiError::function(
					"ha_unavailable",
					"portable rollback could not verify the authoritative owner",
				))
			})?
			.ok_or_else(|| {
				status_from(&ApiError::function(
					"ha_unavailable",
					"portable rollback has no authoritative owner record",
				))
			})?;
		let confirmations = 1
			+ state
				.mesh
				.peers()
				.into_iter()
				.filter(|peer| peer.healthy)
				.count();
		portable_restore_proof(
			expected_owner.is_none_or(|expected| expected == owner),
			state.mesh.is_peer_healthy(&owner),
			confirmations,
			state.mesh.quorum_needed(),
		)
	}
}

fn function_now_millis() -> u64 {
	use std::time::{SystemTime, UNIX_EPOCH};
	SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.unwrap_or_default()
		.as_millis()
		.try_into()
		.unwrap_or(u64::MAX)
}

fn artifact_digest(reference: Option<&pb::ArtifactRef>) -> Result<(Vec<u8>, String), Status> {
	let digest = reference
		.and_then(|reference| reference.digest.as_ref())
		.ok_or_else(|| status_from(&ApiError::invalid("artifact digest is required")))?;
	if digest.algorithm != pb::DigestAlgorithm::Sha256 as i32 || digest.value.len() != 32 {
		return Err(status_from(&ApiError::invalid(
			"artifact digest must be a 32-byte SHA-256 value",
		)));
	}
	Ok((digest.value.clone(), hex::encode(&digest.value)))
}

fn terminal_call_status(status: i32) -> bool {
	matches!(
		pb::CallStatus::try_from(status).unwrap_or(pb::CallStatus::Unspecified),
		pb::CallStatus::Succeeded | pb::CallStatus::Failed | pb::CallStatus::Cancelled
	)
}

pub(super) fn json_view(value: &Value) -> pb::JsonView {
	pb::JsonView { json: compact_json(value) }
}

/// Convert a proto `ExecStart` into the REST-era `ExecBody` so
/// [`validation::validate_exec`] applies identical rules (empty-cmd checks,
/// timeout finiteness and the 60s cap). The proto surface has no `cwd` alias
/// and an empty env map means "inherit" exactly like the omitted REST field.
fn exec_body(start: pb::ExecStart) -> ExecBody {
	ExecBody {
		cmd:     start.cmd,
		workdir: start.workdir,
		cwd:     None,
		env:     (!start.env.is_empty()).then_some(start.env),
		timeout: start.timeout,
		tty:     start.tty,
	}
}

/// The mandatory first `ExecInput` of `Exec`: a `start` payload naming the
/// sandbox.
fn require_exec_start(frame: Option<pb::ExecInput>) -> Result<pb::ExecStart, Status> {
	match frame.and_then(|frame| frame.input) {
		Some(pb::exec_input::Input::Start(start)) => {
			if start.sandbox_id.is_empty() {
				Err(ApiError::invalid("exec start requires sandbox_id").into())
			} else {
				Ok(start)
			}
		},
		_ => Err(ApiError::invalid("first exec frame must be start").into()),
	}
}

/// The mandatory first `ExecInput` of `Shell`: the shell params JSON document
/// (an object; empty text counts as `{}`).
fn require_shell_params(frame: Option<pb::ExecInput>) -> Result<Value, Status> {
	let Some(pb::exec_input::Input::ShellParamsJson(text)) = frame.and_then(|frame| frame.input)
	else {
		return Err(ApiError::invalid("first shell frame must be shell_params_json").into());
	};
	if text.trim().is_empty() {
		return Ok(json!({}));
	}
	let value: Value = serde_json::from_str(&text)
		.map_err(|err| ApiError::invalid(format!("invalid shell request: {err}")))?;
	if value.is_object() {
		Ok(value)
	} else {
		Err(ApiError::invalid("invalid shell request").into())
	}
}

fn resize_dims(resize: pb::Resize) -> ApiResult<(u16, u16)> {
	Ok((resize_dim(resize.rows)?, resize_dim(resize.cols)?))
}

fn resize_dim(raw: u32) -> ApiResult<u16> {
	u16::try_from(raw)
		.ok()
		.filter(|dim| *dim > 0)
		.ok_or_else(|| ApiError::invalid("resize must be [rows, cols]"))
}

/// Parse a schemaless `body_json` document (`RestoreBody` / `ForkBody` /
/// `PoolPutBody`). Empty text means the empty document, mirroring `"{}"`.
fn parse_body_json<T>(text: &str) -> Result<T, Status>
where
	T: serde::de::DeserializeOwned,
{
	let text = if text.trim().is_empty() { "{}" } else { text };
	let value: Value =
		serde_json::from_str(text).map_err(|_| Status::from(ApiError::invalid("invalid request")))?;
	Ok(validation::from_value(value)?)
}

const fn chunk_output(stream: pb::Stream, data: Vec<u8>) -> pb::ExecOutput {
	pb::ExecOutput {
		output: Some(pb::exec_output::Output::Chunk(pb::Output { stream: stream as i32, data })),
	}
}

const fn exit_output(exit: ExecExit) -> pb::ExecOutput {
	pb::ExecOutput {
		output: Some(pb::exec_output::Output::Exit(pb::Exit {
			code:   exit.code,
			signal: exit.signal,
		})),
	}
}

const fn ready_output(sandbox_id: String) -> pb::ExecOutput {
	pb::ExecOutput { output: Some(pb::exec_output::Output::Ready(pb::Ready { sandbox_id })) }
}

/// Keep the last `tail` newline-separated lines of a console buffer.
fn tail_bytes(mut data: Vec<u8>, tail: u64) -> Vec<u8> {
	if tail == 0 {
		return Vec::new();
	}
	let mut lines = 0_u64;
	let mut start = 0_usize;
	for (index, byte) in data.iter().enumerate().rev() {
		// A trailing newline terminates the last line rather than opening one.
		if *byte == b'\n' && index + 1 != data.len() {
			lines += 1;
			if lines == tail {
				start = index + 1;
				break;
			}
		}
	}
	if lines < tail || start == 0 {
		data
	} else {
		data.split_off(start)
	}
}

fn spawn_chunk_forward(
	rx: flume::Receiver<Vec<u8>>,
	stream: pb::Stream,
	tx: mpsc::Sender<pb::ExecOutput>,
) -> thread::JoinHandle<()> {
	thread::spawn(move || {
		while let Ok(chunk) = rx.recv() {
			if tx.blocking_send(chunk_output(stream, chunk)).is_err() {
				break;
			}
		}
	})
}

fn spawn_exit_forward(
	rx: flume::Receiver<ExecExit>,
	streams: [thread::JoinHandle<()>; 2],
	tx: mpsc::Sender<pb::ExecOutput>,
) {
	thread::spawn(move || {
		let exit = rx.recv();
		for stream in streams {
			let _ = stream.join();
		}
		if let Ok(exit) = exit {
			let _ = tx.blocking_send(exit_output(exit));
		}
	});
}

async fn shell_cleanup(engine: &Arc<dyn EngineApi>, cleanup: Option<String>) {
	if let Some(name) = cleanup {
		let engine = engine.clone();
		let _ = tokio::task::spawn_blocking(move || engine.shell_cleanup(&name)).await;
	}
}

/// Bridge a live engine exec onto the gRPC bidi stream, mirroring the WS pump
/// semantics: Output/Exit frames out, stdin/eof/resize in, `Exit` then clean
/// stream end, and `kill(15)` when the client stream ends or the RPC is
/// cancelled before the exit was observed. Control errors terminate the
/// stream as a status.
fn pump_exec(
	engine: Arc<dyn EngineApi>,
	stream: EngineExecStream,
	mut inbound: Streaming<pb::ExecInput>,
	ready: Option<String>,
	cleanup: Option<String>,
) -> BoxStream<pb::ExecOutput> {
	let EngineExecStream { mut control, stdout, stderr, exit } = stream;
	let (out_tx, out_rx) = mpsc::channel::<Result<pb::ExecOutput, Status>>(32);
	let (events_tx, mut events_rx) = mpsc::channel::<pb::ExecOutput>(32);
	let stdout_forward = spawn_chunk_forward(stdout, pb::Stream::Stdout, events_tx.clone());
	let stderr_forward = spawn_chunk_forward(stderr, pb::Stream::Stderr, events_tx.clone());
	spawn_exit_forward(exit, [stdout_forward, stderr_forward], events_tx);
	tokio::spawn(async move {
		if let Some(name) = ready
			&& out_tx.send(Ok(ready_output(name))).await.is_err()
		{
			shell_cleanup(&engine, cleanup).await;
			return;
		}
		let output_tx = out_tx.clone();
		let output = async move {
			while let Some(event) = events_rx.recv().await {
				let is_exit = matches!(event.output, Some(pb::exec_output::Output::Exit(_)));
				if output_tx.send(Ok(event)).await.is_err() || is_exit {
					break;
				}
			}
		};
		let input = async move {
			loop {
				let Ok(Some(frame)) = inbound.message().await else {
					let _ = control.kill(15);
					break;
				};
				let result = match frame.input {
					Some(pb::exec_input::Input::Stdin(data)) => {
						control.write_stdin(&data).map_err(ApiError::from)
					},
					Some(pb::exec_input::Input::Eof(_)) => control.close_stdin().map_err(ApiError::from),
					Some(pb::exec_input::Input::Resize(resize)) => resize_dims(resize)
						.and_then(|(rows, cols)| control.resize(rows, cols).map_err(ApiError::from)),
					// Mid-stream request payloads are ignored, like the WS pump.
					Some(
						pb::exec_input::Input::Start(_)
						| pb::exec_input::Input::ShellParamsJson(_)
						| pb::exec_input::Input::PtyOpen(_)
						| pb::exec_input::Input::PtyAttach(_),
					)
					| None => Ok(()),
				};
				if let Err(err) = result {
					let _ = out_tx.send(Err(status_from(&err))).await;
					break;
				}
			}
		};
		tokio::select! {
			() = output => {},
			() = input => {},
		}
		shell_cleanup(&engine, cleanup).await;
	});
	Box::pin(ReceiverStream::new(out_rx))
}

fn pty_session(value: &Value) -> Result<pb::PtySession, Status> {
	let field = |name| value.get(name);
	Ok(pb::PtySession {
		session_id:             field("session_id")
			.and_then(Value::as_str)
			.ok_or_else(|| Status::internal("agent omitted PTY session_id"))?
			.to_owned(),
		running:                field("running").and_then(Value::as_bool).unwrap_or(true),
		exit_code:              field("exit_code").and_then(Value::as_i64),
		cols:                   field("cols").and_then(Value::as_u64).unwrap_or(80) as u32,
		rows:                   field("rows").and_then(Value::as_u64).unwrap_or(24) as u32,
		exec:                   field("exec").and_then(Value::as_str).map(str::to_owned),
		created_at_unix_millis: field("created_at_unix_millis")
			.and_then(Value::as_u64)
			.unwrap_or_default() as i64,
		attached_count:         field("attached_count")
			.and_then(Value::as_u64)
			.unwrap_or_default() as u32,
		suspended:              field("suspended").and_then(Value::as_bool).unwrap_or(false),
	})
}

fn pump_pty(
	stream: EnginePtyStream,
	mut inbound: Streaming<pb::ExecInput>,
) -> Result<BoxStream<pb::ExecOutput>, Status> {
	let EnginePtyStream { session, mut control, stdout, exit } = stream;
	let metadata = pty_session(&session)?;
	let (out_tx, out_rx) = mpsc::channel::<Result<pb::ExecOutput, Status>>(32);
	tokio::spawn(async move {
		if out_tx
			.send(Ok(pb::ExecOutput { output: Some(pb::exec_output::Output::Pty(metadata)) }))
			.await
			.is_err()
		{
			let _ = control.detach();
			return;
		}
		let (events_tx, mut events_rx) = mpsc::channel::<pb::ExecOutput>(32);
		let stdout_forward = spawn_chunk_forward(stdout, pb::Stream::Console, events_tx.clone());
		thread::spawn(move || {
			if let Ok(exit) = exit.recv() {
				let _ = stdout_forward.join();
				let _ = events_tx.blocking_send(exit_output(exit));
			}
		});
		let output_tx = out_tx.clone();
		let output = async move {
			while let Some(event) = events_rx.recv().await {
				let exited = matches!(event.output, Some(pb::exec_output::Output::Exit(_)));
				if output_tx.send(Ok(event)).await.is_err() || exited {
					break;
				}
			}
		};
		let input = async move {
			loop {
				let Ok(Some(frame)) = inbound.message().await else {
					break;
				};
				let result: ApiResult<()> = match frame.input {
					Some(pb::exec_input::Input::Stdin(data)) => {
						control.write(&data).map_err(ApiError::from)
					},
					Some(pb::exec_input::Input::Resize(resize)) => resize_dims(resize)
						.and_then(|(rows, cols)| control.resize(rows, cols).map_err(ApiError::from)),
					Some(pb::exec_input::Input::Eof(_)) => break,
					_ => Ok(()),
				};
				if let Err(error) = result {
					let _ = out_tx.send(Err(status_from(&error))).await;
					break;
				}
			}
			let _ = control.detach();
		};
		tokio::select! {
			() = output => {},
			() = input => {},
		}
	});
	Ok(Box::pin(ReceiverStream::new(out_rx)))
}

#[tonic::async_trait]
impl pb::artifact_service_server::ArtifactService for GrpcApi {
	type GetStream = BoxStream<pb::ArtifactChunk>;

	async fn put(
		&self,
		request: Request<Streaming<pb::PutArtifactRequest>>,
	) -> Result<Response<pb::ArtifactRecord>, Status> {
		let mut stream = request.into_inner();
		let first = stream
			.message()
			.await?
			.ok_or_else(|| status_from(&ApiError::invalid("artifact upload requires a header")))?;
		let Some(pb::put_artifact_request::Frame::Header(header)) = first.frame else {
			return Err(status_from(&ApiError::invalid(
				"artifact upload first frame must be a header",
			)));
		};
		let digest = header
			.expected_digest
			.as_ref()
			.ok_or_else(|| status_from(&ApiError::invalid("expected digest is required")))?;
		if digest.algorithm != pb::DigestAlgorithm::Sha256 as i32 || digest.value.len() != 32 {
			return Err(status_from(&ApiError::invalid(
				"expected digest must be a 32-byte SHA-256 value",
			)));
		}
		let artifact_quota = self.state.config.function_artifact_max_bytes;
		if header.expected_size_bytes > artifact_quota {
			let mut status = Status::resource_exhausted(format!(
				"artifact size {} exceeds configured quota {artifact_quota}",
				header.expected_size_bytes
			));
			status
				.metadata_mut()
				.insert("vmon-code", MetadataValue::from_static("artifact_quota"));
			return Err(status);
		}
		let expected = digest.value.clone();
		let returned_digest = digest.clone();
		let media_type = header.media_type_presence.map(|presence| match presence {
			pb::put_artifact_header::MediaTypePresence::MediaType(value) => value,
		});
		let ttl_millis = header.ttl_millis_presence.map(|presence| match presence {
			pb::put_artifact_header::TtlMillisPresence::TtlMillis(value) => value,
		});
		let domain = self.state.functions.clone();
		let artifacts = domain.artifacts().clone();
		let expected_size = header.expected_size_bytes;
		let writer = tokio::task::spawn_blocking(move || {
			artifacts.begin_put(&expected, expected_size, artifact_quota)
		})
		.await
		.map_err(|error| status_from(&join_error(error)))?
		.map_err(|error| status_from(&ApiError::from(error)))?;
		let (tx, mut rx) = mpsc::channel::<ArtifactUploadFrame>(8);
		let commit = tokio::task::spawn_blocking(move || {
			let mut writer = writer;
			loop {
				match rx.blocking_recv() {
					Some(ArtifactUploadFrame::Chunk(chunk)) => writer.write_chunk(&chunk)?,
					Some(ArtifactUploadFrame::Finish) => break,
					Some(ArtifactUploadFrame::Abort) | None => {
						return Err(crate::EngineError::invalid("artifact upload interrupted"));
					},
				}
			}
			let stored = writer.finalize()?;
			let created_at = function_now_millis();
			let expires_at = ttl_millis.map(|ttl| created_at.saturating_add(ttl));
			domain.store().record_stored_artifact(
				&stored,
				media_type.as_deref(),
				created_at,
				expires_at,
			)?;
			let (size, canonical_media, canonical_created, canonical_expires, _) =
				domain.store().stat_artifact(&stored.digest)?;
			Ok::<_, crate::EngineError>(pb::ArtifactRecord {
				r#ref: Some(pb::ArtifactRef { digest: Some(returned_digest) }),
				size_bytes: size,
				stored_size_bytes: size,
				media_type_presence: canonical_media
					.map(pb::artifact_record::MediaTypePresence::MediaType),
				created_at_unix_millis: canonical_created,
				expires_at_unix_millis_presence: canonical_expires
					.map(pb::artifact_record::ExpiresAtUnixMillisPresence::ExpiresAtUnixMillis),
			})
		});
		loop {
			let frame = match stream.message().await {
				Ok(Some(frame)) => frame,
				Ok(None) => {
					tx.send(ArtifactUploadFrame::Finish)
						.await
						.map_err(|_| Status::cancelled("artifact writer stopped"))?;
					break;
				},
				Err(error) => {
					let _ = tx.send(ArtifactUploadFrame::Abort).await;
					let _ = commit.await;
					return Err(error);
				},
			};
			if let Some(pb::put_artifact_request::Frame::Data(data)) = frame.frame {
				tx.send(ArtifactUploadFrame::Chunk(data))
					.await
					.map_err(|_| Status::cancelled("artifact writer stopped"))?;
			} else {
				let _ = tx.send(ArtifactUploadFrame::Abort).await;
				let _ = commit.await;
				return Err(status_from(&ApiError::invalid(
					"artifact upload frames after the header must contain data",
				)));
			}
		}
		let record = commit
			.await
			.map_err(|error| status_from(&join_error(error)))?
			.map_err(|error| {
				if error.message.contains("digest mismatch") || error.message.contains("size mismatch")
				{
					status_from(&ApiError::function("checksum", error.message))
				} else {
					status_from(&ApiError::from(error))
				}
			})?;
		Ok(Response::new(record))
	}

	async fn get(
		&self,
		request: Request<pb::GetArtifactRequest>,
	) -> Result<Response<Self::GetStream>, Status> {
		let request = request.into_inner();
		let (_, digest) = artifact_digest(request.artifact.as_ref())?;
		let artifacts = self.state.functions.artifacts().clone();
		let reader = tokio::task::spawn_blocking(move || artifacts.open_verified(&digest, None))
			.await
			.map_err(|error| status_from(&join_error(error)))?
			.map_err(|error| status_from(&ApiError::from(error)))?;
		let size = reader.len();
		let (offset, length) = if let Some(pb::get_artifact_request::RangePresence::Range(range)) =
			request.range_presence
		{
			let end = range
				.offset
				.checked_add(range.length)
				.filter(|end| *end <= size)
				.ok_or_else(|| status_from(&ApiError::invalid("artifact range is out of bounds")))?;
			(range.offset, end - range.offset)
		} else {
			(0, size)
		};
		Ok(Response::new(stream_verified_artifact(reader, offset, length)))
	}

	async fn stat(
		&self,
		request: Request<pb::ArtifactRef>,
	) -> Result<Response<pb::ArtifactRecord>, Status> {
		let reference = request.into_inner();
		let (_, digest) = artifact_digest(Some(&reference))?;
		let (size, media_type, created_at, expires_at, _path) = self
			.function_call(move |domain| domain.store().stat_artifact(&digest))
			.await?;
		Ok(Response::new(pb::ArtifactRecord {
			r#ref: Some(reference),
			size_bytes: size,
			stored_size_bytes: size,
			media_type_presence: media_type.map(pb::artifact_record::MediaTypePresence::MediaType),
			created_at_unix_millis: created_at,
			expires_at_unix_millis_presence: expires_at
				.map(pb::artifact_record::ExpiresAtUnixMillisPresence::ExpiresAtUnixMillis),
		}))
	}
}

#[tonic::async_trait]
impl pb::function_service_server::FunctionService for GrpcApi {
	async fn register(
		&self,
		request: Request<pb::RegisterFunctionRequest>,
	) -> Result<Response<pb::FunctionRevision>, Status> {
		let request = request.into_inner();
		let spec = request
			.spec
			.as_ref()
			.ok_or_else(|| status_from(&ApiError::invalid("function spec is required")))?;
		self.admit_function_ha(spec)?;
		let revision = self
			.function_call(move |domain| {
				let mut spec = request
					.spec
					.clone()
					.ok_or_else(|| crate::EngineError::invalid("function spec is required"))?;
				let architecture = spec
					.resources
					.as_ref()
					.and_then(|resources| pb::CpuArchitecture::try_from(resources.architecture).ok())
					.unwrap_or(pb::CpuArchitecture::Unspecified);
				let image = spec
					.image
					.as_ref()
					.ok_or_else(|| crate::EngineError::invalid("function image is required"))?;
				spec.image = Some(domain.realize_image(image, architecture)?);
				let revision = domain.store().register_function(
					&spec,
					&request.request_id,
					function_now_millis(),
				)?;
				let revision_id = revision
					.r#ref
					.as_ref()
					.map(|reference| reference.revision_id.clone())
					.ok_or_else(|| crate::EngineError::engine("registered revision has no identity"))?;
				for material in request.transient_secrets {
					let secret = material.secret.ok_or_else(|| {
						crate::EngineError::invalid("transient secret reference is required")
					})?;
					domain.set_secret(revision_id.clone(), &secret, material.value);
				}
				domain.refresh_revision_availability(&revision_id)
			})
			.await?;
		Ok(Response::new(revision))
	}

	async fn get(
		&self,
		request: Request<pb::GetFunctionRequest>,
	) -> Result<Response<pb::FunctionRevision>, Status> {
		let request = request.into_inner();
		let revision = self
			.function_call(move |domain| {
				match request.function.and_then(|selector| selector.selection) {
					Some(pb::function_selector::Selection::Current(function)) => {
						domain.store().get_active_revision(&function)
					},
					Some(pb::function_selector::Selection::Pinned(requested)) => {
						let requested_function = requested.function.as_ref().ok_or_else(|| {
							crate::EngineError::invalid("pinned revision function is required")
						})?;
						let loaded = domain.store().get_revision(&requested.revision_id)?;
						if loaded
							.r#ref
							.as_ref()
							.and_then(|reference| reference.function.as_ref())
							!= Some(requested_function)
						{
							return Err(crate::EngineError::not_found(
								"function revision does not belong to the requested function",
							));
						}
						Ok(loaded)
					},
					None => Err(crate::EngineError::invalid("function selector is required")),
				}
			})
			.await?;
		Ok(Response::new(revision))
	}

	async fn list(
		&self,
		request: Request<pb::ListFunctionsRequest>,
	) -> Result<Response<pb::ListFunctionsResponse>, Status> {
		let request = request.into_inner();
		let page = self
			.function_call(move |domain| {
				let namespace = request
					.namespace_presence
					.as_ref()
					.map(|presence| match presence {
						pb::list_functions_request::NamespacePresence::Namespace(value) => value.as_str(),
					});
				let mut page =
					domain
						.store()
						.list_revisions(namespace, request.page_size, &request.page_token)?;
				if let Some(pb::list_functions_request::FunctionPresence::Function(function)) =
					request.function_presence
				{
					page.items.retain(|revision| {
						revision
							.r#ref
							.as_ref()
							.and_then(|reference| reference.function.as_ref())
							== Some(&function)
					});
				}
				Ok(page)
			})
			.await?;
		Ok(Response::new(pb::ListFunctionsResponse {
			revisions:       page.items,
			next_page_token: page.next_page_token,
		}))
	}

	async fn activate(
		&self,
		request: Request<pb::ActivateFunctionRequest>,
	) -> Result<Response<pb::FunctionRecord>, Status> {
		let request = request.into_inner();
		let record = self
			.function_call(move |domain| {
				let revision = request
					.revision
					.as_ref()
					.ok_or_else(|| crate::EngineError::invalid("revision is required"))?;
				let expected =
					request
						.expected_current_presence
						.as_ref()
						.map(|presence| match presence {
							pb::activate_function_request::ExpectedCurrentPresence::ExpectedCurrent(
								value,
							) => value,
						});
				domain
					.store()
					.activate_function(revision, expected, function_now_millis())
			})
			.await?;
		Ok(Response::new(record))
	}

	async fn delete(
		&self,
		request: Request<pb::DeleteFunctionRequest>,
	) -> Result<Response<pb::Ok>, Status> {
		let request = request.into_inner();
		self
			.function_call(move |domain| {
				let revision = request
					.revision
					.as_ref()
					.ok_or_else(|| crate::EngineError::invalid("revision is required"))?;
				domain.store().delete_revision(revision)
			})
			.await?;
		Ok(Response::new(pb::Ok {}))
	}

	async fn activate_app(
		&self,
		request: Request<pb::ActivateAppRequest>,
	) -> Result<Response<pb::AppRevision>, Status> {
		let request = request.into_inner();
		let revision = self
			.function_call(move |domain| domain.store().activate_app(&request, function_now_millis()))
			.await?;
		Ok(Response::new(revision))
	}

	async fn get_app(
		&self,
		request: Request<pb::GetAppRequest>,
	) -> Result<Response<pb::AppRevision>, Status> {
		let request = request.into_inner();
		let revision = self
			.function_call(move |domain| match request.app.and_then(|selector| selector.selection) {
				Some(pb::app_selector::Selection::Current(app)) => domain.store().get_active_app(&app),
				Some(pb::app_selector::Selection::Pinned(requested)) => {
					let requested_app = requested.app.as_ref().ok_or_else(|| {
						crate::EngineError::invalid("pinned app revision app is required")
					})?;
					let loaded = domain.store().get_app_revision(&requested.revision_id)?;
					if loaded
						.r#ref
						.as_ref()
						.and_then(|reference| reference.app.as_ref())
						!= Some(requested_app)
					{
						return Err(crate::EngineError::not_found(
							"app revision does not belong to the requested app",
						));
					}
					Ok(loaded)
				},
				None => Err(crate::EngineError::invalid("app selector is required")),
			})
			.await?;
		Ok(Response::new(revision))
	}

	async fn rollback_app(
		&self,
		request: Request<pb::RollbackAppRequest>,
	) -> Result<Response<pb::AppRevision>, Status> {
		let request = request.into_inner();
		let revision = self
			.function_call(move |domain| domain.store().rollback_app(&request, function_now_millis()))
			.await?;
		Ok(Response::new(revision))
	}

	async fn create_schedule(
		&self,
		request: Request<pb::CreateScheduleRequest>,
	) -> Result<Response<pb::ScheduleRecord>, Status> {
		let request = request.into_inner();
		let domain = self.state.functions.clone();
		let schedule = tokio::task::spawn_blocking(move || {
			domain.create_schedule(&request, function_now_millis())
		})
		.await
		.map_err(|error| status_from(&join_error(error)))?
		.map_err(|error| status_from(&ApiError::from(error)))?;
		Ok(Response::new(schedule))
	}

	async fn get_schedule(
		&self,
		request: Request<pb::ScheduleRef>,
	) -> Result<Response<pb::ScheduleRecord>, Status> {
		let reference = request.into_inner();
		let schedule = self
			.function_call(move |domain| domain.store().get_schedule(&reference.schedule_id))
			.await?;
		Ok(Response::new(schedule))
	}

	async fn list_schedules(
		&self,
		request: Request<pb::ListSchedulesRequest>,
	) -> Result<Response<pb::ListSchedulesResponse>, Status> {
		let request = request.into_inner();
		let page = self
			.function_call(move |domain| domain.store().list_schedules(&request))
			.await?;
		Ok(Response::new(pb::ListSchedulesResponse {
			schedules:       page.items,
			next_page_token: page.next_page_token,
		}))
	}

	async fn delete_schedule(
		&self,
		request: Request<pb::ScheduleRef>,
	) -> Result<Response<pb::Ok>, Status> {
		let reference = request.into_inner();
		self
			.function_call(move |domain| domain.store().delete_schedule(&reference.schedule_id))
			.await?;
		Ok(Response::new(pb::Ok {}))
	}
}

#[tonic::async_trait]
impl pb::call_service_server::CallService for GrpcApi {
	type StreamInputsStream = BoxStream<pb::StreamCallInputsResponse>;
	type WatchStream = BoxStream<pb::CallEvent>;

	async fn create(
		&self,
		request: Request<pb::CreateCallRequest>,
	) -> Result<Response<pb::CallRecord>, Status> {
		let request = request.into_inner();
		if let Some(revision_id) = request
			.target
			.as_ref()
			.and_then(|target| target.function.as_ref())
			.map(|revision| revision.revision_id.clone())
			.filter(|revision_id| !revision_id.is_empty())
		{
			let revision = self
				.function_call(move |domain| domain.store().get_revision(&revision_id))
				.await?;
			let spec = revision
				.spec
				.as_ref()
				.ok_or_else(|| Status::internal("function revision is missing its spec"))?;
			self.admit_function_ha(spec)?;
		}
		let record = self
			.function_call(move |domain| {
				let record = domain
					.store()
					.create_call(&request, function_now_millis())?;
				domain.notify_work();
				Ok(record)
			})
			.await?;
		Ok(Response::new(record))
	}

	async fn stream_inputs(
		&self,
		request: Request<Streaming<pb::StreamCallInputsRequest>>,
	) -> Result<Response<Self::StreamInputsStream>, Status> {
		const INPUT_WINDOW: u32 = 8;
		let mut stream = request.into_inner();
		let first = stream
			.message()
			.await?
			.ok_or_else(|| status_from(&ApiError::invalid("input stream requires a call frame")))?;
		let call = match first.frame {
			Some(pb::stream_call_inputs_request::Frame::Call(call)) if !call.call_id.is_empty() => {
				call
			},
			_ => {
				return Err(status_from(&ApiError::invalid(
					"input stream first frame must be a call reference",
				)));
			},
		};
		let committed = self
			.function_call({
				let call_id = call.call_id.clone();
				move |domain| Ok(domain.store().get_call(&call_id)?.input_count)
			})
			.await?;
		let (tx, rx) = mpsc::channel(INPUT_WINDOW as usize);
		tx.send(Ok(pb::StreamCallInputsResponse {
			call:                   Some(call.clone()),
			committed_input_count:  committed,
			last_input_presence:    None,
			max_inputs_outstanding: INPUT_WINDOW,
		}))
		.await
		.map_err(|_| Status::cancelled("input response stream closed"))?;
		let domain = self.state.functions.clone();
		tokio::spawn(async move {
			loop {
				let frame = match stream.message().await {
					Ok(Some(frame)) => frame,
					Ok(None) => break,
					Err(error) => {
						let _ = tx.send(Err(error)).await;
						break;
					},
				};
				let Some(pb::stream_call_inputs_request::Frame::Input(input)) = frame.frame else {
					let _ = tx
						.send(Err(status_from(&ApiError::invalid(
							"input frames after the opener must contain an input",
						))))
						.await;
					break;
				};
				let input_ref =
					pb::InputRef { input_id: input.input_id.clone(), input_index: input.index };
				let call_id = call.call_id.clone();
				let commit_domain = domain.clone();
				let result = tokio::task::spawn_blocking(move || {
					let frontier = commit_domain.store().event_frontier(&call_id)?;
					let committed =
						commit_domain
							.store()
							.append_input(&call_id, &input, function_now_millis())?;
					for event in commit_domain
						.store()
						.events_after(&call_id, frontier, 10_000)?
					{
						commit_domain.publish_call_event(event);
					}
					commit_domain.notify_work();
					Ok::<_, crate::EngineError>(committed)
				})
				.await;
				let committed = match result {
					Ok(Ok(committed)) => committed,
					Ok(Err(error)) => {
						let _ = tx.send(Err(status_from(&ApiError::from(error)))).await;
						break;
					},
					Err(error) => {
						let _ = tx.send(Err(status_from(&join_error(error)))).await;
						break;
					},
				};
				if tx
					.send(Ok(pb::StreamCallInputsResponse {
						call:                   Some(call.clone()),
						committed_input_count:  committed,
						last_input_presence:    Some(
							pb::stream_call_inputs_response::LastInputPresence::LastInput(input_ref),
						),
						max_inputs_outstanding: INPUT_WINDOW,
					}))
					.await
					.is_err()
				{
					break;
				}
			}
		});
		Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
	}

	async fn close_inputs(
		&self,
		request: Request<pb::CloseCallInputsRequest>,
	) -> Result<Response<pb::CallRecord>, Status> {
		let request = request.into_inner();
		let record = self
			.function_call(move |domain| {
				let call = request
					.call
					.as_ref()
					.ok_or_else(|| crate::EngineError::invalid("call is required"))?;
				let frontier = domain.store().event_frontier(&call.call_id)?;
				let record = domain.store().close_inputs(
					&call.call_id,
					request.expected_input_count,
					function_now_millis(),
				)?;
				for event in domain
					.store()
					.events_after(&call.call_id, frontier, 10_000)?
				{
					domain.publish_call_event(event);
				}
				domain.notify_work();
				Ok(record)
			})
			.await?;
		Ok(Response::new(record))
	}

	async fn get(&self, request: Request<pb::CallRef>) -> Result<Response<pb::CallRecord>, Status> {
		let call = request.into_inner();
		let record = self
			.function_call(move |domain| domain.store().get_call(&call.call_id))
			.await?;
		Ok(Response::new(record))
	}

	async fn list(
		&self,
		request: Request<pb::ListCallsRequest>,
	) -> Result<Response<pb::ListCallsResponse>, Status> {
		let request = request.into_inner();
		let page = self
			.function_call(move |domain| domain.store().list_calls(&request))
			.await?;
		Ok(Response::new(pb::ListCallsResponse {
			calls:           page.items,
			next_page_token: page.next_page_token,
		}))
	}

	async fn get_result(
		&self,
		request: Request<pb::GetCallResultRequest>,
	) -> Result<Response<pb::CallResult>, Status> {
		let request = request.into_inner();
		let result = self
			.function_call(move |domain| {
				let call = request
					.call
					.as_ref()
					.ok_or_else(|| crate::EngineError::invalid("call is required"))?;
				domain.store().get_result(&call.call_id, request.index)
			})
			.await?;
		Ok(Response::new(result))
	}

	async fn list_results(
		&self,
		request: Request<pb::ListCallResultsRequest>,
	) -> Result<Response<pb::ListCallResultsResponse>, Status> {
		let request = request.into_inner();
		let cursor = request
			.cursor
			.ok_or_else(|| status_from(&ApiError::invalid("result cursor is required")))?;
		let call = cursor
			.call
			.ok_or_else(|| status_from(&ApiError::invalid("cursor call is required")))?;
		let page_size = if request.page_size == 0 {
			100
		} else {
			request.page_size.min(1000)
		};
		let call_id = call.call_id.clone();
		let after = cursor.after_sequence;
		let (results, end) = self
			.function_call(move |domain| {
				let results = domain.store().results_after(&call_id, after, page_size)?;
				let last = results.last().map_or(after, |result| result.sequence);
				let end = domain.store().results_after(&call_id, last, 1)?.is_empty();
				Ok((results, end))
			})
			.await?;
		let after_sequence = results.last().map_or(after, |result| result.sequence);
		Ok(Response::new(pb::ListCallResultsResponse {
			results,
			next_cursor: Some(pb::ResultCursor { call: Some(call), after_sequence }),
			end,
		}))
	}

	async fn watch(
		&self,
		request: Request<pb::WatchCallRequest>,
	) -> Result<Response<Self::WatchStream>, Status> {
		let request = request.into_inner();
		let cursor = request
			.cursor
			.ok_or_else(|| status_from(&ApiError::invalid("event cursor is required")))?;
		let call = cursor
			.call
			.ok_or_else(|| status_from(&ApiError::invalid("cursor call is required")))?;
		if call.call_id.is_empty() {
			return Err(status_from(&ApiError::invalid("call id is required")));
		}
		let current = self
			.function_call({
				let call_id = call.call_id.clone();
				move |domain| domain.store().get_call(&call_id)
			})
			.await?;
		if !request.follow || terminal_call_status(current.status) {
			return Ok(Response::new(stream_persisted_call_events(
				Arc::clone(&self.state.functions),
				call.call_id,
				cursor.after_sequence,
			)));
		}
		let domain = Arc::clone(&self.state.functions);
		let call_id = call.call_id;
		let mut watch = domain
			.watch_call(&call_id, cursor.after_sequence)
			.map_err(|error| status_from(&ApiError::from(error)))?;
		let (tx, rx) = mpsc::channel(32);
		tokio::spawn(async move {
			let mut last_sequence = cursor.after_sequence;
			loop {
				match watch.recv().await {
					Ok(event) => {
						let is_terminal = matches!(
							event.payload.as_ref(),
							Some(pb::call_event::Payload::Status(status))
								if terminal_call_status(status.status)
						);
						let sequence = event.sequence;
						if tx.send(Ok(event)).await.is_err() {
							break;
						}
						last_sequence = sequence;
						if is_terminal {
							break;
						}
					},
					Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
						match domain.watch_call(&call_id, last_sequence) {
							Ok(recovered) => watch = recovered,
							Err(error) => {
								let _ = tx.send(Err(status_from(&ApiError::from(error)))).await;
								break;
							},
						}
					},
					Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
				}
			}
		});
		Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
	}

	async fn cancel(
		&self,
		request: Request<pb::CancelCallRequest>,
	) -> Result<Response<pb::CallRecord>, Status> {
		let request = request.into_inner();
		let call = request
			.call
			.as_ref()
			.ok_or_else(|| status_from(&ApiError::invalid("call is required")))?;
		let record = self
			.state
			.functions
			.cancel_call(&call.call_id, &request.reason, &request.request_id)
			.await
			.map_err(|error| status_from(&ApiError::from(error)))?;
		Ok(Response::new(record))
	}
}

#[tonic::async_trait]
impl pb::actor_service_server::ActorService for GrpcApi {
	async fn create(
		&self,
		request: Request<pb::CreateActorRequest>,
	) -> Result<Response<pb::ActorRecord>, Status> {
		let request = request.into_inner();
		let actor = self
			.state
			.functions
			.create_actor(&request)
			.await
			.map_err(|error| status_from(&ApiError::from(error)))?;
		Ok(Response::new(actor))
	}

	async fn get(
		&self,
		request: Request<pb::ActorRef>,
	) -> Result<Response<pb::ActorRecord>, Status> {
		let actor = request.into_inner();
		let record = self
			.function_call(move |domain| domain.store().get_actor(&actor.actor_id))
			.await?;
		Ok(Response::new(record))
	}

	async fn checkpoint(
		&self,
		request: Request<pb::CheckpointActorRequest>,
	) -> Result<Response<pb::ActorCheckpoint>, Status> {
		let request = request.into_inner();
		let checkpoint = self
			.state
			.functions
			.checkpoint_actor(&request)
			.await
			.map_err(|error| status_from(&ApiError::from(error)))?;
		Ok(Response::new(checkpoint))
	}

	async fn restore(
		&self,
		request: Request<pb::RestoreActorRequest>,
	) -> Result<Response<pb::ActorRecord>, Status> {
		let request = request.into_inner();
		let actor = self
			.state
			.functions
			.restore_actor(&request)
			.await
			.map_err(|error| status_from(&ApiError::from(error)))?;
		Ok(Response::new(actor))
	}

	async fn fork(
		&self,
		request: Request<pb::ForkActorRequest>,
	) -> Result<Response<pb::ActorRecord>, Status> {
		let request = request.into_inner();
		let actor = self
			.state
			.functions
			.fork_actor(&request)
			.await
			.map_err(|error| status_from(&ApiError::from(error)))?;
		Ok(Response::new(actor))
	}

	async fn delete(&self, request: Request<pb::ActorRef>) -> Result<Response<pb::Ok>, Status> {
		let actor = request.into_inner();
		self
			.state
			.functions
			.delete_actor(&actor.actor_id)
			.await
			.map_err(|error| status_from(&ApiError::from(error)))?;
		Ok(Response::new(pb::Ok {}))
	}
}

fn gossip_mesh_api_error(err: crate::mesh::gossip::MeshError) -> ApiError {
	ApiError::new(err.http_status(), err.code, err.message)
}

fn record_matches_tags(record: &CreateRecordWire, tags: Option<&HashMap<String, String>>) -> bool {
	let Some(tags) = tags else {
		return true;
	};
	let Some(record_tags) = record.params.get("tags").and_then(|v| v.as_object()) else {
		return tags.is_empty();
	};
	for (k, v) in tags {
		let Some(record_v) = record_tags.get(k).and_then(|v| v.as_str()) else {
			return false;
		};
		if record_v != v {
			return false;
		}
	}
	true
}

fn mesh_record_placeholder(record: &CreateRecordWire, owner_tenant: &str) -> Value {
	let staging = record_is_staging(&record.params);
	let mut view = serde_json::json!({
		"id": record.sid,
		"name": record.sid,
		"ha": record.ha,
		"restart_policy": record.restart_policy,
		"created_at": record.created_at,
		"status": if staging { "starting" } else { "running" },
		"owner_tenant": owner_tenant,
	});
	if !staging {
		view
			.as_object_mut()
			.expect("mesh record placeholder is an object")
			.insert("node".to_owned(), Value::String(record.owner.clone()));
	}
	view
}

fn credential_record(metadata: CredentialMetadata) -> pb::CredentialRecord {
	pb::CredentialRecord {
		name:                   metadata.name,
		allowed_domains:        metadata.allowed_domains,
		header_names:           metadata.header_names,
		expires_at_unix_millis: metadata.expires_at_unix_millis,
		requests_per_minute:    metadata.requests_per_minute,
		version:                metadata.version,
	}
}

#[tonic::async_trait]
impl pb::sandbox_service_server::SandboxService for GrpcApi {
	type AttachStream = BoxStream<pb::ExecOutput>;
	type BatchCreateStream = BoxStream<pb::BatchCreateResponse>;
	type ExecStream = BoxStream<pb::ExecOutput>;
	type LogsStream = BoxStream<pb::LogChunk>;
	type PtyAttachStream = BoxStream<pb::ExecOutput>;
	type PtyOpenStream = BoxStream<pb::ExecOutput>;
	type ShellStream = BoxStream<pb::ExecOutput>;
	type WatchStream = BoxStream<pb::JsonView>;

	/// `BatchCreate`: run each streamed item through the unary create pipeline
	/// concurrently, reporting per-item outcomes as they finish (out of
	/// order). The stream itself only fails on transport or auth errors.
	async fn batch_create(
		&self,
		request: Request<Streaming<pb::BatchCreateRequest>>,
	) -> Result<Response<Self::BatchCreateStream>, Status> {
		/// In-process bound on concurrently admitted batch items; admission
		/// and gRPC flow control are the real gates, this only bounds memory.
		const BATCH_CREATE_CONCURRENCY: usize = 1024;
		let principal = request_principal(&request)?;
		let (metadata, _, mut inbound) = request.into_parts();
		let mesh_hop = is_mesh_hop(&metadata);
		let api = self.clone();
		let (tx, out) = mpsc::channel::<Result<pb::BatchCreateResponse, Status>>(256);
		tokio::spawn(async move {
			let slots = Arc::new(tokio::sync::Semaphore::new(BATCH_CREATE_CONCURRENCY));
			loop {
				let item = match inbound.message().await {
					Ok(Some(item)) => item,
					Ok(None) => break,
					Err(status) => {
						let _ = tx.send(Err(status)).await;
						break;
					},
				};
				let Ok(permit) = Arc::clone(&slots).acquire_owned().await else {
					break;
				};
				let api = api.clone();
				let principal = principal.clone();
				let tx = tx.clone();
				tokio::spawn(async move {
					let _permit = permit;
					let outcome = match item.create {
						Some(create) => {
							match api
								.create_sandbox_pipeline(&principal, mesh_hop, create)
								.await
							{
								Ok(view) => pb::batch_create_response::Outcome::Json(compact_json(&view)),
								Err(error) => {
									pb::batch_create_response::Outcome::Error(pb::BatchCreateError {
										code:    error.code().to_owned(),
										message: error.message().to_owned(),
									})
								},
							}
						},
						None => pb::batch_create_response::Outcome::Error(pb::BatchCreateError {
							code:    "invalid".to_owned(),
							message: "batch item carries no create payload".to_owned(),
						}),
					};
					let _ = tx
						.send(Ok(pb::BatchCreateResponse { seq: item.seq, outcome: Some(outcome) }))
						.await;
				});
			}
			// Dropping this sender closes the outbound stream once every
			// outstanding item task has reported.
		});
		Ok(Response::new(Box::pin(ReceiverStream::new(out))))
	}

	/// Watch one sandbox's lifecycle as `JsonView` frames.
	///
	/// The bus is subscribed before the initial `get`, so no transition is
	/// lost between the snapshot frame and the event stream. Unknown sids
	/// wait for their first event only with `until_ready` (the client's
	/// deadline governs). Named creates rejected during facade preparation
	/// publish the same terminal `failed` event as launch failures.
	async fn watch(
		&self,
		request: Request<pb::WatchSandboxRequest>,
	) -> Result<Response<Self::WatchStream>, Status> {
		let pb::WatchSandboxRequest { id, until_ready } = request.into_inner();
		let events = self.state.engine.subscribe_events();
		let engine = self.state.engine.clone();
		let lookup = id.clone();
		let initial = match tokio::task::spawn_blocking(move || engine.get(&lookup))
			.await
			.map_err(|err| status_from(&join_error(err)))?
		{
			Ok(view) => Some(view),
			Err(error) if error.code == ErrorCode::NotFound && until_ready => None,
			Err(error) => return Err(status_from(&ApiError::from(error))),
		};
		Ok(Response::new(sandbox_stream::watch_stream(id, until_ready, initial, events)))
	}

	async fn create(
		&self,
		request: Request<pb::CreateSandboxRequest>,
	) -> Result<Response<pb::JsonView>, Status> {
		let principal = request_principal(&request)?;
		let (metadata, _, message) = request.into_parts();
		let view = self
			.create_sandbox_pipeline(&principal, is_mesh_hop(&metadata), message)
			.await
			.map_err(|error| status_from(&error))?;
		Ok(Response::new(json_view(&view)))
	}

	async fn list(
		&self,
		request: Request<pb::ListSandboxesRequest>,
	) -> Result<Response<pb::ListSandboxesResponse>, Status> {
		let principal = request_principal(&request)?;
		let message = request.into_inner();
		let tags = validation::parse_tag_filters(&message.tags)?;
		let mut rows = if let Some(mesh) = self.state.mesh.clone()
			&& mesh.mesh_enabled()
		{
			let records = MeshRecordStore::list(&*mesh)
				.map_err(|err| status_from(&gossip_mesh_api_error(err)))?;
			let mut views = Vec::new();
			let local_node = MeshControl::node_id(&*mesh);
			for rec in records {
				let owner_tenant = rec
					.params
					.get("owner_tenant")
					.and_then(Value::as_str)
					.unwrap_or("default");
				if principal.require_tenant(owner_tenant).is_err() {
					continue;
				}
				if !record_matches_tags(&rec, tags.as_ref()) {
					continue;
				}
				if record_is_staging(&rec.params) {
					views.push(mesh_record_placeholder(&rec, owner_tenant));
					continue;
				}
				let sid = rec.sid.clone();
				if rec.owner == local_node {
					let engine = self.state.engine.clone();
					let view_res = tokio::task::spawn_blocking(move || engine.get(&sid)).await;
					if let Ok(Ok(view)) = view_res {
						views.push(apply_view_detail(view, &rec));
					} else {
						views.push(mesh_record_placeholder(&rec, owner_tenant));
					}
				} else {
					views.push(mesh_record_placeholder(&rec, owner_tenant));
				}
			}
			views
		} else {
			self.engine_call(move |engine| engine.list(tags)).await?
		};
		if !principal.is_admin() {
			rows.retain(|view| {
				view
					.get("owner_tenant")
					.and_then(Value::as_str)
					.unwrap_or("default")
					== principal.tenant
			});
		}
		let sandboxes_json = rows.into_iter().map(|row| compact_json(&row)).collect();
		Ok(Response::new(pb::ListSandboxesResponse { sandboxes_json }))
	}

	async fn get(&self, request: Request<pb::SandboxRef>) -> Result<Response<pb::JsonView>, Status> {
		let (metadata, _, message) = request.into_parts();
		forward_to_owner!(self, metadata, &message.id, message, get);
		let id = message.id;
		let view = self.engine_call(move |engine| engine.get(&id)).await?;
		Ok(Response::new(json_view(&view)))
	}

	async fn stop(
		&self,
		request: Request<pb::StopSandboxRequest>,
	) -> Result<Response<pb::JsonView>, Status> {
		let (metadata, _, message) = request.into_parts();
		forward_to_owner!(self, metadata, &message.id, message, stop);
		let pb::StopSandboxRequest { id, returncode } = message;
		let view = self
			.engine_call(move |engine| engine.stop_with_returncode(&id, returncode))
			.await?;
		Ok(Response::new(json_view(&view)))
	}

	async fn resize(
		&self,
		request: Request<pb::ResizeSandboxRequest>,
	) -> Result<Response<pb::JsonView>, Status> {
		let (metadata, _, message) = request.into_parts();
		forward_to_owner!(self, metadata, &message.id, message, resize);
		let pb::ResizeSandboxRequest { id, cpus, memory_mib, disk_mb } = message;
		let view = self
			.engine_call(move |engine| engine.resize(&id, cpus, memory_mib, disk_mb))
			.await?;
		Ok(Response::new(json_view(&view)))
	}

	async fn remove(
		&self,
		request: Request<pb::SandboxRef>,
	) -> Result<Response<pb::JsonView>, Status> {
		let (metadata, _, message) = request.into_parts();
		forward_to_owner!(self, metadata, &message.id, message, remove);
		let id = message.id;
		let view = self.engine_call(move |engine| engine.remove(&id)).await?;
		Ok(Response::new(json_view(&view)))
	}

	async fn terminate(
		&self,
		request: Request<pb::SandboxRef>,
	) -> Result<Response<pb::JsonView>, Status> {
		let (metadata, _, message) = request.into_parts();
		forward_to_owner!(self, metadata, &message.id, message, terminate);
		let id = message.id;
		let view = self
			.engine_call(move |engine| engine.terminate(&id, "api"))
			.await?;
		Ok(Response::new(json_view(&view)))
	}

	async fn pause(
		&self,
		request: Request<pb::SandboxRef>,
	) -> Result<Response<pb::JsonView>, Status> {
		let (metadata, _, message) = request.into_parts();
		forward_to_owner!(self, metadata, &message.id, message, pause);
		let id = message.id;
		let view = self.engine_call(move |engine| engine.pause(&id)).await?;
		Ok(Response::new(json_view(&view)))
	}

	async fn resume(
		&self,
		request: Request<pb::SandboxRef>,
	) -> Result<Response<pb::JsonView>, Status> {
		let (metadata, _, message) = request.into_parts();
		if resume_forwards_to_owner(self.local_passive_resume(&metadata, &message.id).await?) {
			let adopt = match self.forward_target(&metadata, &message.id).await {
				Ok(Some(target)) => match target
					.sandbox_client()
					.resume(target.request(message.clone()))
					.await
				{
					Ok(response) => return Ok(response),
					Err(status) => return Err(status),
				},
				Ok(None) => self.has_durable_suspended_marker(&message.id).await?,
				Err(status) if resume_owner_resolution_lost(&status) => {
					self.has_durable_suspended_marker(&message.id).await?
				},
				Err(status) => return Err(status),
			};
			if adopt && let Err(status) = self.adopt_suspended_marker(&message.id).await {
				if status.code() == Code::Aborted
					&& let Some(target) = self.forward_target(&metadata, &message.id).await?
				{
					return target
						.sandbox_client()
						.resume(target.request(message))
						.await;
				}
				return Err(status);
			}
		}
		let id = message.id;
		let view = match self
			.engine_call({
				let id = id.clone();
				move |engine| engine.resume(&id)
			})
			.await
		{
			Ok(view) => view,
			Err(status)
				if matches!(status.code(), Code::NotFound | Code::Aborted)
					&& self.has_durable_suspended_marker(&id).await? =>
			{
				self.adopt_suspended_marker(&id).await?;
				self.engine_call(move |engine| engine.resume(&id)).await?
			},
			Err(status) => return Err(status),
		};
		Ok(Response::new(json_view(&view)))
	}

	async fn suspend(
		&self,
		request: Request<pb::SandboxRef>,
	) -> Result<Response<pb::JsonView>, Status> {
		let (metadata, _, message) = request.into_parts();
		forward_to_owner!(self, metadata, &message.id, message, suspend);
		let id = message.id;
		let view = self.engine_call(move |engine| engine.suspend(&id)).await?;
		Ok(Response::new(json_view(&view)))
	}

	async fn extend(
		&self,
		request: Request<pb::ExtendSandboxRequest>,
	) -> Result<Response<pb::JsonView>, Status> {
		let (metadata, _, message) = request.into_parts();
		forward_to_owner!(self, metadata, &message.id, message, extend);
		validation::validate_extend(&ExtendBody { secs: message.secs })?;
		let pb::ExtendSandboxRequest { id, secs } = message;
		let view = self
			.engine_call(move |engine| engine.extend(&id, secs))
			.await?;
		Ok(Response::new(json_view(&view)))
	}

	async fn set_idle_timeout(
		&self,
		request: Request<pb::SetIdleTimeoutRequest>,
	) -> Result<Response<pb::JsonView>, Status> {
		let (metadata, _, message) = request.into_parts();
		forward_to_owner!(self, metadata, &message.id, message, set_idle_timeout);
		let pb::SetIdleTimeoutRequest { id, idle_timeout_secs } = message;
		let secs = idle_timeout_secs
			.ok_or_else(|| Status::from(ApiError::invalid("idle_timeout_secs is required")))?;
		let view = self
			.engine_call(move |engine| engine.set_idle_timeout(&id, secs))
			.await?;
		Ok(Response::new(json_view(&view)))
	}

	async fn metrics(
		&self,
		request: Request<pb::SandboxRef>,
	) -> Result<Response<pb::JsonView>, Status> {
		let (metadata, _, message) = request.into_parts();
		forward_to_owner!(self, metadata, &message.id, message, metrics);
		let id = message.id;
		let view = self.engine_call(move |engine| engine.metrics(&id)).await?;
		Ok(Response::new(json_view(&view)))
	}

	async fn logs(
		&self,
		request: Request<pb::LogsRequest>,
	) -> Result<Response<Self::LogsStream>, Status> {
		let (metadata, _, message) = request.into_parts();
		forward_to_owner!(self, metadata, &message.id, message, logs, stream);
		let pb::LogsRequest { id, follow, tail } = message;
		if follow {
			let history = match tail {
				Some(tail) => {
					let history_id = id.clone();
					Some(tail_bytes(
						self
							.engine_call(move |engine| engine.logs(&history_id))
							.await?,
						tail,
					))
				},
				None => None,
			};
			let rx = self
				.engine_call(move |engine| engine.logs_follow(&id))
				.await?;
			let (tx, out) = mpsc::channel::<Result<pb::LogChunk, Status>>(32);
			if let Some(history) = history.filter(|history| !history.is_empty()) {
				let _ = tx.try_send(Ok(pb::LogChunk { data: history }));
			}
			thread::spawn(move || {
				while let Ok(chunk) = rx.recv() {
					if tx.blocking_send(Ok(pb::LogChunk { data: chunk })).is_err() {
						break;
					}
				}
			});
			return Ok(Response::new(Box::pin(ReceiverStream::new(out))));
		}
		let data = self.engine_call(move |engine| engine.logs(&id)).await?;
		let data = match tail {
			Some(tail) => tail_bytes(data, tail),
			None => data,
		};
		Ok(Response::new(Box::pin(tokio_stream::once(Ok(pb::LogChunk { data })))))
	}

	async fn exec_capture(
		&self,
		request: Request<pb::ExecCaptureRequest>,
	) -> Result<Response<pb::ExecCaptureResponse>, Status> {
		let (metadata, _, message) = request.into_parts();
		forward_to_owner!(self, metadata, &message.id, message, exec_capture);
		let pb::ExecCaptureRequest { id, exec } = message;
		let exec = exec.ok_or_else(|| Status::from(ApiError::invalid("exec start is required")))?;
		// The wrapper's `id` is authoritative; `ExecStart.sandbox_id` is ignored.
		let exec_request = validation::validate_exec(&exec_body(exec))?;
		let capture = self
			.engine_call(move |engine| engine.exec_capture(&id, exec_request))
			.await?;
		Ok(Response::new(pb::ExecCaptureResponse {
			code:   capture.exit,
			signal: None,
			stdout: capture.stdout,
			stderr: capture.stderr,
		}))
	}

	async fn exec(
		&self,
		request: Request<Streaming<pb::ExecInput>>,
	) -> Result<Response<Self::ExecStream>, Status> {
		let (metadata, _, mut inbound) = request.into_parts();
		let start = require_exec_start(inbound.message().await?)?;
		if let Some(target) = self.forward_target(&metadata, &start.sandbox_id).await? {
			let first = pb::ExecInput { input: Some(pb::exec_input::Input::Start(start)) };
			let outbound = tokio_stream::once(first).chain(inbound.filter_map(|frame| frame.ok()));
			return target
				.sandbox_client()
				.exec(target.request(outbound))
				.await
				.map(relay_stream);
		}
		let id = start.sandbox_id.clone();
		let exec_request = validation::validate_exec(&exec_body(start))?;
		let stream = self
			.engine_call(move |engine| engine.exec_stream(&id, exec_request))
			.await?;
		Ok(Response::new(pump_exec(self.state.engine.clone(), stream, inbound, None, None)))
	}

	async fn pty_open(
		&self,
		request: Request<Streaming<pb::ExecInput>>,
	) -> Result<Response<Self::PtyOpenStream>, Status> {
		let principal = request_principal(&request)?;
		let (metadata, _, mut inbound) = request.into_parts();
		let Some(pb::exec_input::Input::PtyOpen(start)) =
			inbound.message().await?.and_then(|frame| frame.input)
		else {
			return Err(Status::invalid_argument("PTY open requires pty_open first frame"));
		};
		if start.sandbox_id.is_empty() {
			return Err(Status::invalid_argument("PTY open requires sandbox_id"));
		}
		self
			.authorize_sandbox(&principal, &start.sandbox_id)
			.await?;
		if let Some(target) = self.forward_target(&metadata, &start.sandbox_id).await? {
			let first = pb::ExecInput { input: Some(pb::exec_input::Input::PtyOpen(start)) };
			let outbound = tokio_stream::once(first).chain(inbound.filter_map(|frame| frame.ok()));
			return target
				.sandbox_client()
				.pty_open(target.request(outbound))
				.await
				.map(relay_stream);
		}
		let id = start.sandbox_id;
		let params = json!({
			"session_id": start.session_id,
			"cols": start.cols,
			"rows": start.rows,
			"exec": start.exec,
			"env": start.env,
			"workdir": start.workdir,
		});
		let stream = self
			.engine_call(move |engine| engine.pty_open(&id, params))
			.await?;
		Ok(Response::new(pump_pty(stream, inbound)?))
	}

	async fn pty_attach(
		&self,
		request: Request<Streaming<pb::ExecInput>>,
	) -> Result<Response<Self::PtyAttachStream>, Status> {
		let principal = request_principal(&request)?;
		let (metadata, _, mut inbound) = request.into_parts();
		let Some(pb::exec_input::Input::PtyAttach(start)) =
			inbound.message().await?.and_then(|frame| frame.input)
		else {
			return Err(Status::invalid_argument("PTY attach requires pty_attach first frame"));
		};
		if start.sandbox_id.is_empty() || start.session_id.is_empty() {
			return Err(Status::invalid_argument("PTY attach requires sandbox_id and session_id"));
		}
		self
			.authorize_sandbox(&principal, &start.sandbox_id)
			.await?;
		if let Some(target) = self.forward_target(&metadata, &start.sandbox_id).await? {
			let first = pb::ExecInput { input: Some(pb::exec_input::Input::PtyAttach(start)) };
			let outbound = tokio_stream::once(first).chain(inbound.filter_map(|frame| frame.ok()));
			return target
				.sandbox_client()
				.pty_attach(target.request(outbound))
				.await
				.map(relay_stream);
		}
		let id = start.sandbox_id;
		let params = json!({
			"session_id": start.session_id,
			"cols": start.cols,
			"rows": start.rows,
		});
		let stream = self
			.engine_call(move |engine| engine.pty_attach(&id, params))
			.await?;
		Ok(Response::new(pump_pty(stream, inbound)?))
	}

	async fn pty_list(
		&self,
		request: Request<pb::SandboxRef>,
	) -> Result<Response<pb::PtySessionList>, Status> {
		let principal = request_principal(&request)?;
		let (metadata, _, message) = request.into_parts();
		self.authorize_sandbox(&principal, &message.id).await?;
		forward_to_owner!(self, metadata, &message.id, message, pty_list);
		let id = message.id;
		let sessions = self.engine_call(move |engine| engine.pty_list(&id)).await?;
		Ok(Response::new(pb::PtySessionList {
			sessions: sessions.iter().map(pty_session).collect::<Result<_, _>>()?,
		}))
	}

	async fn pty_close(
		&self,
		request: Request<pb::PtyCloseRequest>,
	) -> Result<Response<pb::PtySessionCloseResponse>, Status> {
		let principal = request_principal(&request)?;
		let (metadata, _, message) = request.into_parts();
		self.authorize_sandbox(&principal, &message.id).await?;
		forward_to_owner!(self, metadata, &message.id, message, pty_close);
		let id = message.id;
		let session_id = message.session_id;
		let response = self
			.engine_call(move |engine| engine.pty_close(&id, &session_id))
			.await?;
		Ok(Response::new(pb::PtySessionCloseResponse {
			session_id: response
				.get("session_id")
				.and_then(Value::as_str)
				.unwrap_or_default()
				.to_owned(),
			exit_code:  response.get("exit_code").and_then(Value::as_i64),
		}))
	}

	async fn pty_exec(
		&self,
		request: Request<pb::PtyExecRequest>,
	) -> Result<Response<pb::PtyExecResponse>, Status> {
		let principal = request_principal(&request)?;
		let (metadata, _, message) = request.into_parts();
		self.authorize_sandbox(&principal, &message.id).await?;
		forward_to_owner!(self, metadata, &message.id, message, pty_exec);
		let id = message.id;
		let session_id = message.session_id;
		let command = message.command;
		let timeout = message.timeout.unwrap_or(60.0);
		let capture = self
			.engine_call(move |engine| engine.pty_exec(&id, &session_id, &command, timeout))
			.await?;
		Ok(Response::new(pb::PtyExecResponse {
			code:   capture.exit,
			stdout: capture.stdout,
			stderr: capture.stderr,
		}))
	}

	async fn shell(
		&self,
		request: Request<Streaming<pb::ExecInput>>,
	) -> Result<Response<Self::ShellStream>, Status> {
		let principal = request_principal(&request)?;
		let mut inbound = request.into_inner();
		let mut params = require_shell_params(inbound.message().await?)?;
		if !principal.is_admin() {
			let sandbox_id = params
				.get("ref")
				.and_then(Value::as_str)
				.filter(|sandbox_id| !sandbox_id.is_empty())
				.ok_or_else(|| {
					status_from(&ApiError::forbidden(
						"tenant shell sessions must reference an owned sandbox",
					))
				})?;
			self.authorize_sandbox(&principal, sandbox_id).await?;
			params
				.as_object_mut()
				.expect("validated shell params object")
				.insert("_vmon_owner_tenant".to_owned(), json!(principal.tenant));
		}
		let engine = self.state.engine.clone();
		let session = tokio::task::spawn_blocking(move || engine.shell_start(params))
			.await
			.map_err(|err| status_from(&join_error(err)))?
			.map_err(|err| status_from(&ApiError::from(err)))?;
		let ready = session.name.clone();
		let cleanup = session.ephemeral.then(|| session.name.clone());
		Ok(Response::new(pump_exec(
			self.state.engine.clone(),
			session.stream,
			inbound,
			Some(ready),
			cleanup,
		)))
	}

	async fn attach(
		&self,
		request: Request<pb::SandboxRef>,
	) -> Result<Response<Self::AttachStream>, Status> {
		let (metadata, _, message) = request.into_parts();
		forward_to_owner!(self, metadata, &message.id, message, attach, stream);
		let id = message.id;
		let logs = self
			.engine_call(move |engine| engine.logs_follow(&id))
			.await?;
		let (tx, rx) = mpsc::channel::<Result<pb::ExecOutput, Status>>(32);
		thread::spawn(move || {
			while let Ok(chunk) = logs.recv() {
				if tx
					.blocking_send(Ok(chunk_output(pb::Stream::Console, chunk)))
					.is_err()
				{
					break;
				}
			}
		});
		Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
	}

	async fn file_read(
		&self,
		request: Request<pb::FilePathRequest>,
	) -> Result<Response<pb::FileContent>, Status> {
		let (metadata, _, message) = request.into_parts();
		forward_to_owner!(self, metadata, &message.id, message, file_read);
		let pb::FilePathRequest { id, path } = message;
		let data = self
			.engine_call(move |engine| engine.file_read(&id, &path))
			.await?;
		Ok(Response::new(pb::FileContent { data }))
	}

	async fn file_write(
		&self,
		request: Request<pb::FileWriteRequest>,
	) -> Result<Response<pb::Ok>, Status> {
		let (metadata, _, message) = request.into_parts();
		forward_to_owner!(self, metadata, &message.id, message, file_write);
		let pb::FileWriteRequest { id, path, data } = message;
		self
			.engine_call(move |engine| engine.file_write(&id, &path, &data))
			.await?;
		Ok(Response::new(pb::Ok {}))
	}

	async fn file_delete(
		&self,
		request: Request<pb::FileDeleteRequest>,
	) -> Result<Response<pb::Ok>, Status> {
		let (metadata, _, message) = request.into_parts();
		forward_to_owner!(self, metadata, &message.id, message, file_delete);
		let pb::FileDeleteRequest { id, path, recursive } = message;
		self
			.engine_call(move |engine| engine.file_delete(&id, &path, recursive))
			.await?;
		Ok(Response::new(pb::Ok {}))
	}

	async fn file_list(
		&self,
		request: Request<pb::FilePathRequest>,
	) -> Result<Response<pb::JsonView>, Status> {
		let (metadata, _, message) = request.into_parts();
		forward_to_owner!(self, metadata, &message.id, message, file_list);
		let pb::FilePathRequest { id, path } = message;
		let value = self
			.engine_call(move |engine| engine.file_list(&id, &path))
			.await?;
		Ok(Response::new(json_view(&value)))
	}

	async fn file_stat(
		&self,
		request: Request<pb::FilePathRequest>,
	) -> Result<Response<pb::JsonView>, Status> {
		let (metadata, _, message) = request.into_parts();
		forward_to_owner!(self, metadata, &message.id, message, file_stat);
		let pb::FilePathRequest { id, path } = message;
		let value = self
			.engine_call(move |engine| engine.file_stat(&id, &path))
			.await?;
		Ok(Response::new(json_view(&value)))
	}

	async fn network_get(
		&self,
		request: Request<pb::SandboxRef>,
	) -> Result<Response<pb::JsonView>, Status> {
		let (metadata, _, message) = request.into_parts();
		forward_to_owner!(self, metadata, &message.id, message, network_get);
		let id = message.id;
		let value = self
			.engine_call(move |engine| engine.network_get(&id))
			.await?;
		Ok(Response::new(json_view(&value)))
	}

	async fn network_set(
		&self,
		request: Request<pb::NetworkSetRequest>,
	) -> Result<Response<pb::JsonView>, Status> {
		let (metadata, _, message) = request.into_parts();
		forward_to_owner!(self, metadata, &message.id, message, network_set);
		let pb::NetworkSetRequest { id, block_network, cidr_allow, domain_allow } = message;
		let body = NetworkBody {
			block_network,
			cidr_allow: cidr_allow.map(|list| list.values),
			domain_allow: domain_allow.map(|list| list.values),
		};
		validation::validate_network(&body)?;
		let value = self
			.engine_call(move |engine| engine.network_set(&id, body))
			.await?;
		Ok(Response::new(json_view(&value)))
	}

	async fn tunnels(
		&self,
		request: Request<pb::SandboxRef>,
	) -> Result<Response<pb::JsonView>, Status> {
		let (metadata, _, message) = request.into_parts();
		forward_to_owner!(self, metadata, &message.id, message, tunnels);
		let id = message.id;
		let value = self.engine_call(move |engine| engine.tunnels(&id)).await?;
		Ok(Response::new(json_view(&value)))
	}

	async fn migrate(
		&self,
		request: Request<pb::MigrateRequest>,
	) -> Result<Response<pb::JsonView>, Status> {
		let (metadata, _, message) = request.into_parts();
		if message.target.is_empty() {
			return Err(ApiError::invalid("target node id is required").into());
		}
		forward_to_owner!(self, metadata, &message.id, message, migrate);
		let pb::MigrateRequest { id, target } = message;
		let value = if let Some(mesh) = self.state.mesh.clone() {
			mesh
				.migrate_sandbox(id, target)
				.await
				.map_err(|err| status_from(&ApiError::from(err)))?
		} else {
			self
				.engine_call(move |engine| engine.migrate(&id, &target))
				.await?
		};
		Ok(Response::new(json_view(&value)))
	}

	async fn snapshot(
		&self,
		request: Request<pb::SnapshotRequest>,
	) -> Result<Response<pb::JsonView>, Status> {
		let (metadata, _, message) = request.into_parts();
		forward_to_owner!(self, metadata, &message.id, message, snapshot);
		let pb::SnapshotRequest { id, name, stop } = message;
		let value = self
			.engine_call(move |engine| engine.snapshot(&id, name, stop))
			.await?;
		Ok(Response::new(json_view(&value)))
	}

	async fn snapshot_fs(
		&self,
		request: Request<pb::SnapshotFsRequest>,
	) -> Result<Response<pb::JsonView>, Status> {
		let (metadata, _, message) = request.into_parts();
		forward_to_owner!(self, metadata, &message.id, message, snapshot_fs);
		let pb::SnapshotFsRequest { id, name } = message;
		let value = self
			.engine_call(move |engine| engine.snapshot_fs(&id, name))
			.await?;
		Ok(Response::new(json_view(&value)))
	}

	async fn history(
		&self,
		request: Request<pb::SandboxRef>,
	) -> Result<Response<pb::RecoveryPointList>, Status> {
		let (metadata, _, message) = request.into_parts();
		if let PortableOwnerRoute::Forward(target) =
			self.portable_history_target(&metadata, &message.id).await?
			&& let Some(response) = portable_peer_response(
				target
					.sandbox_client()
					.history(target.request(message.clone()))
					.await,
			)? {
			return Ok(response);
		}
		let id = message.id;
		let points = self
			.engine_call(move |engine| engine.history(&id))
			.await?
			.into_iter()
			.map(|point| pb::RecoveryPoint {
				name:                   point.name,
				kind:                   point.kind,
				created_at_unix_millis: point.created_at_unix_millis,
				size_bytes:             point.size_bytes,
			})
			.collect();
		Ok(Response::new(pb::RecoveryPointList { points }))
	}

	async fn rollback(
		&self,
		request: Request<pb::RollbackSandboxRequest>,
	) -> Result<Response<pb::JsonView>, Status> {
		let (metadata, _, message) = request.into_parts();
		let admission_owner = match self.portable_history_target(&metadata, &message.id).await? {
			PortableOwnerRoute::ServeLocal => None,
			PortableOwnerRoute::Adopt => Some(None),
			PortableOwnerRoute::Forward(target) => {
				match portable_peer_response(
					target
						.sandbox_client()
						.rollback(target.request(message.clone()))
						.await,
				)? {
					Some(response) => return Ok(response),
					None => Some(Some(target.owner)),
				}
			},
		};
		if let Some(owner) = admission_owner {
			self.portable_rollback_admission(&message.id, owner.as_deref())?;
		}
		let pb::RollbackSandboxRequest { id, recovery_point } = message;
		let view = self
			.engine_call(move |engine| engine.rollback(&id, &recovery_point))
			.await?;
		Ok(Response::new(json_view(&view)))
	}
}

#[tonic::async_trait]
impl pb::snapshot_service_server::SnapshotService for GrpcApi {
	async fn list(
		&self,
		_request: Request<pb::ListSnapshotsRequest>,
	) -> Result<Response<pb::SnapshotList>, Status> {
		let snapshots = self.engine_call(|engine| engine.snapshots()).await?;
		Ok(Response::new(pb::SnapshotList { snapshots }))
	}

	async fn restore(
		&self,
		request: Request<pb::RestoreSnapshotRequest>,
	) -> Result<Response<pb::JsonView>, Status> {
		let pb::RestoreSnapshotRequest { name, body_json } = request.into_inner();
		let body: RestoreBody = parse_body_json(&body_json)?;
		let value = self
			.engine_call(move |engine| engine.restore(&name, body))
			.await?;
		Ok(Response::new(json_view(&value)))
	}

	async fn fork(
		&self,
		request: Request<pb::ForkSnapshotRequest>,
	) -> Result<Response<pb::JsonView>, Status> {
		let pb::ForkSnapshotRequest { name, body_json } = request.into_inner();
		let body: ForkBody = parse_body_json(&body_json)?;
		validation::validate_fork(&body)?;
		let value = self
			.engine_call(move |engine| engine.fork(&name, body))
			.await?;
		Ok(Response::new(json_view(&value)))
	}

	async fn delete(&self, request: Request<pb::SnapshotRef>) -> Result<Response<pb::Ok>, Status> {
		let name = request.into_inner().name;
		self
			.engine_call(move |engine| engine.snapshot_delete(&name))
			.await?;
		Ok(Response::new(pb::Ok {}))
	}
}

#[tonic::async_trait]
impl pb::credential_service_server::CredentialService for GrpcApi {
	async fn list(
		&self,
		request: Request<pb::ListCredentialsRequest>,
	) -> Result<Response<pb::CredentialList>, Status> {
		let principal = request_principal(&request)?;
		let requested = request.into_inner().tenant;
		let (tenant, _) = self.credential_scope(&principal, requested)?;
		let credentials = self
			.engine_call(move |engine| engine.credential_list(&tenant))
			.await?
			.into_iter()
			.map(credential_record)
			.collect();
		Ok(Response::new(pb::CredentialList { credentials }))
	}

	async fn put(
		&self,
		request: Request<pb::PutCredentialRequest>,
	) -> Result<Response<pb::CredentialRecord>, Status> {
		let principal = request_principal(&request)?;
		let pb::PutCredentialRequest {
			name,
			allowed_domains,
			headers,
			expires_at_unix_millis,
			requests_per_minute,
			tenant: requested,
		} = request.into_inner();
		let (tenant, key_id) = self.credential_scope(&principal, requested)?;
		let mut injected = BTreeMap::new();
		for header in headers {
			if injected.insert(header.name.clone(), header.value).is_some() {
				return Err(status_from(&ApiError::invalid(format!(
					"duplicate credential header {:?}",
					header.name
				))));
			}
		}
		let credential = Credential {
			name,
			allowed_domains,
			headers: injected,
			expires_at_unix_millis,
			requests_per_minute,
			version: String::new(),
		};
		let metadata = self
			.engine_call(move |engine| engine.credential_put(&tenant, &key_id, credential))
			.await?;
		Ok(Response::new(credential_record(metadata)))
	}

	async fn delete(
		&self,
		request: Request<pb::DeleteCredentialRequest>,
	) -> Result<Response<pb::Ok>, Status> {
		let principal = request_principal(&request)?;
		let pb::DeleteCredentialRequest { credential, tenant: requested } = request.into_inner();
		let name = credential
			.ok_or_else(|| status_from(&ApiError::invalid("credential reference is required")))?
			.name;
		let (tenant, _) = self.credential_scope(&principal, requested)?;
		self
			.engine_call(move |engine| engine.credential_delete(&tenant, &name))
			.await?;
		Ok(Response::new(pb::Ok {}))
	}
}

#[tonic::async_trait]
impl pb::volume_service_server::VolumeService for GrpcApi {
	async fn list(
		&self,
		_request: Request<pb::ListVolumesRequest>,
	) -> Result<Response<pb::VolumeList>, Status> {
		let volumes = self.engine_call(|engine| engine.volume_list()).await?;
		Ok(Response::new(pb::VolumeList { volumes }))
	}

	async fn create(&self, request: Request<pb::VolumeRef>) -> Result<Response<pb::Ok>, Status> {
		let name = request.into_inner().name;
		self
			.engine_call(move |engine| engine.volume_create(&name))
			.await?;
		Ok(Response::new(pb::Ok {}))
	}

	async fn delete(&self, request: Request<pb::VolumeRef>) -> Result<Response<pb::Ok>, Status> {
		let name = request.into_inner().name;
		self
			.engine_call(move |engine| engine.volume_delete(&name))
			.await?;
		Ok(Response::new(pb::Ok {}))
	}
}

fn vpc_tenant(principal: &Principal) -> String {
	if principal.is_admin() {
		"default".to_owned()
	} else {
		principal.tenant.clone()
	}
}

fn vpc_message(vpc: crate::engine::vpc::Vpc) -> pb::Vpc {
	pb::Vpc {
		id:                     vpc.id,
		name:                   vpc.name,
		cidr:                   vpc.cidr,
		created_at_unix_millis: i64::try_from(vpc.created_at_unix_millis).unwrap_or(i64::MAX),
	}
}

#[tonic::async_trait]
impl pb::vpc_service_server::VpcService for GrpcApi {
	async fn create(
		&self,
		request: Request<pb::VpcCreateRequest>,
	) -> Result<Response<pb::Vpc>, Status> {
		let tenant = vpc_tenant(&request_principal(&request)?);
		let pb::VpcCreateRequest { name, cidr } = request.into_inner();
		let vpc = self
			.engine_call(move |engine| {
				engine.vpc_create(
					&tenant,
					(!name.is_empty()).then_some(name.as_str()),
					(!cidr.is_empty()).then_some(cidr.as_str()),
				)
			})
			.await?;
		Ok(Response::new(vpc_message(vpc)))
	}

	async fn list(
		&self,
		request: Request<pb::ListVpcsRequest>,
	) -> Result<Response<pb::VpcList>, Status> {
		let tenant = vpc_tenant(&request_principal(&request)?);
		let vpcs = self
			.engine_call(move |engine| engine.vpc_list(&tenant))
			.await?;
		Ok(Response::new(pb::VpcList { vpcs: vpcs.into_iter().map(vpc_message).collect() }))
	}

	async fn delete(&self, request: Request<pb::VpcRef>) -> Result<Response<pb::Ok>, Status> {
		let tenant = vpc_tenant(&request_principal(&request)?);
		let id = request.into_inner().id;
		self
			.engine_call(move |engine| engine.vpc_delete(&tenant, &id))
			.await?;
		Ok(Response::new(pb::Ok {}))
	}
}

#[tonic::async_trait]
impl pb::pool_service_server::PoolService for GrpcApi {
	async fn list(
		&self,
		_request: Request<pb::ListPoolsRequest>,
	) -> Result<Response<pb::JsonView>, Status> {
		let value = self.engine_call(|engine| engine.pool_list()).await?;
		Ok(Response::new(json_view(&value)))
	}

	async fn set(
		&self,
		request: Request<pb::PoolSetRequest>,
	) -> Result<Response<pb::JsonView>, Status> {
		let pb::PoolSetRequest { reference, body_json } = request.into_inner();
		let body: PoolPutBody = parse_body_json(&body_json)?;
		let value = self
			.engine_call(move |engine| engine.pool_set(&reference, body))
			.await?;
		Ok(Response::new(json_view(&value)))
	}

	async fn delete(&self, request: Request<pb::PoolRef>) -> Result<Response<pb::Ok>, Status> {
		let reference = request.into_inner().reference;
		self
			.engine_call(move |engine| engine.pool_delete(&reference))
			.await?;
		Ok(Response::new(pb::Ok {}))
	}
}

#[tonic::async_trait]
impl pb::system_service_server::SystemService for GrpcApi {
	type EventsStream = BoxStream<pb::JsonView>;

	async fn info(
		&self,
		_request: Request<pb::InfoRequest>,
	) -> Result<Response<pb::JsonView>, Status> {
		let value = self.engine_call(|engine| engine.info()).await?;
		Ok(Response::new(json_view(&value)))
	}

	async fn events(
		&self,
		_request: Request<pb::EventsRequest>,
	) -> Result<Response<Self::EventsStream>, Status> {
		let rx = self.state.engine.subscribe_events();
		let (tx, out) = mpsc::channel::<Result<pb::JsonView, Status>>(32);
		thread::spawn(move || {
			while let Ok(payload) = rx.recv() {
				if tx.blocking_send(Ok(json_view(&payload))).is_err() {
					break;
				}
			}
		});
		Ok(Response::new(Box::pin(ReceiverStream::new(out))))
	}

	async fn mesh_status(
		&self,
		_request: Request<pb::MeshStatusRequest>,
	) -> Result<Response<pb::JsonView>, Status> {
		let Some(mesh) = self.state.mesh.as_ref() else {
			return Err(
				ApiError::from(crate::EngineError::unsupported("mesh is not configured")).into(),
			);
		};
		let state = mesh.route_state();
		let value =
			tokio::task::spawn_blocking(move || crate::mesh::routes::mesh_status_view(&state))
				.await
				.map_err(|err| status_from(&join_error(err)))?
				.map_err(|err| status_from(&ApiError::new(err.http_status(), err.code, err.message)))?;
		Ok(Response::new(json_view(&value)))
	}
}

#[cfg(test)]
mod tests {
	use std::collections::HashMap;

	use super::*;
	use crate::{EngineError, ErrorCode};

	#[test]
	fn status_mapping_covers_every_error_code_and_carries_vmon_code() {
		let cases = [
			(ErrorCode::NotFound, Code::NotFound),
			(ErrorCode::Invalid, Code::InvalidArgument),
			(ErrorCode::Unauthorized, Code::Unauthenticated),
			(ErrorCode::Forbidden, Code::PermissionDenied),
			(ErrorCode::NotRunning, Code::FailedPrecondition),
			(ErrorCode::Busy, Code::Aborted),
			(ErrorCode::Unsupported, Code::Unimplemented),
			(ErrorCode::Engine, Code::Unavailable),
		];
		for (code, expected) in cases {
			let status = status_from(&ApiError::from(EngineError::new(code, "boom")));
			assert_eq!(status.code(), expected, "grpc code for {code:?}");
			assert_eq!(status.message(), "boom");
			let carried = status
				.metadata()
				.get("vmon-code")
				.and_then(|value| value.to_str().ok())
				.map(str::to_owned);
			assert_eq!(carried.as_deref(), Some(code.as_str()));
		}
	}

	#[test]
	fn status_metadata_explains_retryability_and_recovery() {
		let invalid = status_from(&ApiError::invalid("bad request"));
		assert_eq!(
			invalid
				.metadata()
				.get("vmon-retryable")
				.and_then(|value| value.to_str().ok()),
			Some("false")
		);
		assert_eq!(
			invalid
				.metadata()
				.get("vmon-action")
				.and_then(|value| value.to_str().ok()),
			Some("correct the request parameters")
		);

		let engine = status_from(&ApiError::from(EngineError::engine("host unavailable")));
		assert_eq!(
			engine
				.metadata()
				.get("vmon-retryable")
				.and_then(|value| value.to_str().ok()),
			Some("true")
		);
	}

	#[test]
	fn function_errors_have_stable_grpc_codes_and_metadata() {
		for (stable, expected) in [
			("actor_lost", Code::FailedPrecondition),
			("unavailable_secret", Code::FailedPrecondition),
			("ha_unavailable", Code::FailedPrecondition),
			("checksum", Code::DataLoss),
			("conflict", Code::Aborted),
			("deadline", Code::DeadlineExceeded),
		] {
			let status = status_from(&ApiError::new(axum::http::StatusCode::CONFLICT, stable, "boom"));
			assert_eq!(status.code(), expected, "{stable}");
			assert_eq!(
				status
					.metadata()
					.get("vmon-code")
					.and_then(|value| value.to_str().ok()),
				Some(stable)
			);
		}
	}

	fn ha_spec(policy: pb::HighAvailabilityPolicy, arch: pb::CpuArchitecture) -> pb::FunctionSpec {
		pb::FunctionSpec {
			resources: Some(pb::ResourceSpec {
				high_availability: policy.into(),
				architecture: arch.into(),
				..Default::default()
			}),
			..Default::default()
		}
	}

	fn topology_node(id: &str, zone: &str, arch: &str) -> crate::mesh::state::NodeState {
		let mut node = crate::mesh::state::NodeState::new(id, format!("https://{id}"));
		node.region = zone.into();
		node.arch = arch.into();
		node
	}

	#[test]
	fn none_is_admitted_without_mesh_or_remote_executor() {
		function_ha_admission(
			&ha_spec(pb::HighAvailabilityPolicy::None, pb::CpuArchitecture::Arm64),
			&[],
			false,
		)
		.unwrap();
	}

	#[test]
	fn host_and_zone_never_degrade_to_local_execution() {
		let nodes = [
			topology_node("host-a", "zone-a", "aarch64"),
			topology_node("host-b", "zone-b", "aarch64"),
		];
		for policy in [pb::HighAvailabilityPolicy::Host, pb::HighAvailabilityPolicy::Zone] {
			let error =
				function_ha_admission(&ha_spec(policy, pb::CpuArchitecture::Arm64), &nodes, false)
					.unwrap_err();
			assert_eq!(error.code(), "ha_unavailable");
			assert!(
				error
					.message()
					.contains("cross-node function execution is unavailable")
			);
			assert_eq!(status_from(&error).code(), Code::FailedPrecondition);
		}
	}

	#[test]
	fn insufficient_and_arch_incompatible_topology_are_actionable() {
		let nodes =
			[topology_node("host-a", "zone-a", "x86_64"), topology_node("host-b", "zone-b", "x86_64")];
		let error = function_ha_admission(
			&ha_spec(pb::HighAvailabilityPolicy::Zone, pb::CpuArchitecture::Arm64),
			&nodes,
			false,
		)
		.unwrap_err();
		assert_eq!(error.code(), "ha_unavailable");
		assert!(
			error
				.message()
				.contains("requires at least 2 compatible mesh zones; found 0")
		);
		assert!(
			error
				.message()
				.contains("cross-node function execution is unavailable")
		);
	}
	#[test]
	fn mesh_transport_codes_map_to_unavailable_or_aborted() {
		let unreachable = status_from(&mesh_api_error(MeshError::unreachable("gone")));
		assert_eq!(unreachable.code(), Code::Unavailable);
		assert_eq!(
			unreachable
				.metadata()
				.get("vmon-code")
				.and_then(|value| value.to_str().ok()),
			Some("unreachable")
		);
		let conflict = status_from(&mesh_api_error(MeshError::conflict("taken")));
		assert_eq!(conflict.code(), Code::Aborted);
	}

	#[test]
	fn projected_suspended_marker_bypasses_owner_forwarding_only_for_resume() {
		assert!(resume_forwards_to_owner(false), "ordinary sandboxes retain owner forwarding");
		assert!(
			!resume_forwards_to_owner(true),
			"the projected suspended claimant receives local Resume"
		);
	}

	#[test]
	fn cross_node_resume_adopts_only_before_a_peer_rpc() {
		let resolution_loss =
			status_from(&mesh_api_error(MeshError::unreachable("source A is gone")));
		assert!(resume_owner_resolution_lost(&resolution_loss));

		// A transport status after forwarding may mean the owner already
		// committed Resume. It is never safe to replay locally.
		let dead_peer = Status::unavailable("connection lost after send");
		assert!(!resume_owner_resolution_lost(&dead_peer));

		let ambiguous =
			status_from(&mesh_api_error(MeshError::ambiguous("source A may have accepted Resume")));
		assert!(!resume_owner_resolution_lost(&ambiguous));

		let application =
			status_from(&ApiError::from(EngineError::engine("source A storage failed during Resume")));
		assert!(!resume_owner_resolution_lost(&application));
	}

	#[test]
	fn portable_history_keeps_healthy_owner_forwarding() {
		let route = portable_owner_route(Ok(Some("https://healthy-owner")));
		assert!(matches!(
			route.expect("healthy owner must remain forwarded"),
			PortableOwnerRoute::Forward("https://healthy-owner")
		));
	}

	#[test]
	fn portable_history_adopts_locally_only_after_definitive_owner_loss() {
		let route = portable_owner_route::<&str>(Err(status_from(&mesh_api_error(
			MeshError::unreachable("owner gone"),
		))));
		assert!(matches!(
			route.expect("unavailable owner permits adoption"),
			PortableOwnerRoute::Adopt
		));
	}

	#[tokio::test]
	async fn portable_history_adopts_after_known_peer_transport_failure() {
		let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
			.await
			.expect("reserve local port");
		let address = listener.local_addr().expect("local address");
		drop(listener);
		let channel = Endpoint::from_shared(format!("http://{address}"))
			.expect("valid dead-peer endpoint")
			.connect_lazy();
		let error = pb::sandbox_service_client::SandboxServiceClient::new(channel)
			.history(Request::new(pb::SandboxRef { id: "portable-sandbox".to_owned() }))
			.await
			.expect_err("dead known peer must fail before producing an application response");
		assert_eq!(error.code(), Code::Unavailable);
		assert!(
			portable_peer_response::<Response<pb::RecoveryPointList>>(Err(error))
				.expect("dead peer permits fenced local adoption")
				.is_none()
		);
	}

	#[test]
	fn portable_rollback_requires_dead_owner_and_restore_quorum() {
		portable_restore_proof(true, false, 2, 2)
			.expect("dead owner with quorum permits fenced claim");

		let healthy_owner = portable_restore_proof(true, true, 2, 2)
			.expect_err("a partitioned but healthy owner must not be adopted");
		assert_eq!(healthy_owner.code(), Code::FailedPrecondition);

		let no_quorum = portable_restore_proof(true, false, 1, 2)
			.expect_err("dead owner without quorum must not be adopted");
		assert_eq!(no_quorum.code(), Code::FailedPrecondition);

		let stale_owner = portable_restore_proof(false, false, 2, 2)
			.expect_err("a changed owner record must fence the claimant");
		assert_eq!(stale_owner.code(), Code::FailedPrecondition);
	}

	#[test]
	fn portable_history_does_not_adopt_after_ambiguous_owner_response() {
		let error = portable_peer_response::<()>(Err(status_from(&mesh_api_error(
			MeshError::ambiguous("owner may have committed rollback"),
		))))
		.expect_err("ambiguous owner response must not be replayed locally");
		assert_eq!(error.code(), Code::Unavailable);
		assert_eq!(
			error
				.metadata()
				.get("vmon-code")
				.and_then(|value| value.to_str().ok()),
			Some("ambiguous")
		);
	}

	#[test]
	fn portable_history_does_not_adopt_after_application_unavailable() {
		let error = portable_peer_response::<()>(Err(status_from(&ApiError::from(
			EngineError::engine("peer could not read its storage"),
		))))
		.expect_err("application errors must not be replayed locally");
		assert_eq!(error.code(), Code::Unavailable);
		assert_eq!(
			error
				.metadata()
				.get("vmon-code")
				.and_then(|value| value.to_str().ok()),
			Some("engine_error")
		);
	}

	#[test]
	fn portable_history_missing_point_and_competing_claim_have_stable_statuses() {
		let missing = status_from(&ApiError::from(EngineError::not_found(
			"portable recovery point not committed",
		)));
		assert_eq!(missing.code(), Code::NotFound);

		let competing_claim = status_from(&ApiError::new(
			axum::http::StatusCode::CONFLICT,
			"conflict",
			"portable restore ownership epoch changed",
		));
		assert_eq!(competing_claim.code(), Code::Aborted);
		assert_eq!(
			competing_claim
				.metadata()
				.get("vmon-code")
				.and_then(|value| value.to_str().ok()),
			Some("conflict")
		);
	}

	#[test]
	fn exec_start_converts_to_engine_request_with_timeout_cap() {
		let start = pb::ExecStart {
			cmd:        vec!["sh".to_owned(), "-c".to_owned(), "ls".to_owned()],
			workdir:    Some("/tmp".to_owned()),
			env:        HashMap::from([("A".to_owned(), "1".to_owned())]),
			timeout:    Some(120.0),
			tty:        true,
			sandbox_id: "sb".to_owned(),
		};
		let request = validation::validate_exec(&exec_body(start)).expect("valid exec start");
		assert_eq!(request.cmd, ["sh", "-c", "ls"]);
		assert!(request.tty);
		assert_eq!(request.workdir.as_deref(), Some("/tmp"));
		assert_eq!(
			request
				.env
				.as_ref()
				.and_then(|env| env.get("A"))
				.map(String::as_str),
			Some("1")
		);
		assert_eq!(request.timeout, Some(60.0));
	}

	#[test]
	fn exec_start_empty_env_means_inherit_and_empty_cmd_is_rejected() {
		let start = pb::ExecStart { cmd: vec!["true".to_owned()], ..Default::default() };
		let request = validation::validate_exec(&exec_body(start)).expect("valid exec start");
		assert_eq!(request.env, None);
		assert_eq!(request.workdir, None);

		let empty = pb::ExecStart::default();
		let err = validation::validate_exec(&exec_body(empty)).expect_err("empty cmd rejected");
		assert_eq!(status_from(&err).code(), Code::InvalidArgument);
	}

	fn start_input(start: pb::ExecStart) -> pb::ExecInput {
		pb::ExecInput { input: Some(pb::exec_input::Input::Start(start)) }
	}

	#[test]
	fn first_exec_frame_must_be_start_naming_a_sandbox() {
		let missing = require_exec_start(None).expect_err("stream end rejected");
		assert_eq!(missing.code(), Code::InvalidArgument);
		assert_eq!(
			missing
				.metadata()
				.get("vmon-code")
				.and_then(|value| value.to_str().ok()),
			Some("invalid")
		);

		let stdin = require_exec_start(Some(pb::ExecInput {
			input: Some(pb::exec_input::Input::Stdin(b"hi".to_vec())),
		}))
		.expect_err("stdin-first rejected");
		assert_eq!(stdin.code(), Code::InvalidArgument);

		let unnamed = require_exec_start(Some(start_input(pb::ExecStart::default())))
			.expect_err("start without sandbox_id rejected");
		assert_eq!(unnamed.code(), Code::InvalidArgument);
		assert!(unnamed.message().contains("sandbox_id"));

		let start = require_exec_start(Some(start_input(pb::ExecStart {
			cmd: vec!["true".to_owned()],
			sandbox_id: "sb-1".to_owned(),
			..Default::default()
		})))
		.expect("valid start accepted");
		assert_eq!(start.sandbox_id, "sb-1");
	}

	#[test]
	fn first_shell_frame_must_be_params_json_object() {
		let missing = require_shell_params(None).expect_err("stream end rejected");
		assert_eq!(missing.code(), Code::InvalidArgument);

		let wrong = require_shell_params(Some(start_input(pb::ExecStart::default())))
			.expect_err("start-first rejected");
		assert_eq!(wrong.code(), Code::InvalidArgument);

		let params_frame = |text: &str| {
			Some(pb::ExecInput {
				input: Some(pb::exec_input::Input::ShellParamsJson(text.to_owned())),
			})
		};
		assert_eq!(require_shell_params(params_frame("")).expect("empty defaults"), json!({}));
		assert_eq!(
			require_shell_params(params_frame(r#"{"image":"alpine"}"#)).expect("object accepted"),
			json!({"image": "alpine"})
		);
		let not_object = require_shell_params(params_frame("[1,2]")).expect_err("array rejected");
		assert_eq!(not_object.code(), Code::InvalidArgument);
		let garbage = require_shell_params(params_frame("{nope")).expect_err("garbage rejected");
		assert_eq!(garbage.code(), Code::InvalidArgument);
	}

	#[test]
	fn resize_bounds_match_the_ws_contract() {
		for (rows, cols) in [(1, 1), (24, 80), (65_535, 65_535)] {
			let (parsed_rows, parsed_cols) =
				resize_dims(pb::Resize { rows, cols }).expect("in-range resize accepted");
			assert_eq!(u32::from(parsed_rows), rows);
			assert_eq!(u32::from(parsed_cols), cols);
		}
		for (rows, cols) in [(0, 80), (24, 0), (65_536, 80), (24, 100_000)] {
			assert!(
				resize_dims(pb::Resize { rows, cols }).is_err(),
				"resize ({rows}, {cols}) must be rejected"
			);
		}
	}

	#[test]
	fn tail_bytes_keeps_the_last_lines() {
		let data = b"one\ntwo\nthree\n".to_vec();
		assert_eq!(tail_bytes(data.clone(), 0), b"");
		assert_eq!(tail_bytes(data.clone(), 1), b"three\n");
		assert_eq!(tail_bytes(data.clone(), 2), b"two\nthree\n");
		assert_eq!(tail_bytes(data.clone(), 3), data.as_slice());
		assert_eq!(tail_bytes(data.clone(), 10), data.as_slice());
		assert_eq!(tail_bytes(b"no-newline".to_vec(), 1), b"no-newline");
	}

	#[test]
	fn body_json_defaults_to_the_empty_document() {
		let restore: RestoreBody = parse_body_json("").expect("empty body defaults");
		assert_eq!(restore.name, None);
		let restore: RestoreBody =
			parse_body_json(r#"{"name":"copy"}"#).expect("named restore parses");
		assert_eq!(restore.name.as_deref(), Some("copy"));
		let fork: Result<ForkBody, Status> = parse_body_json("");
		assert!(fork.is_err(), "fork requires count");
		let garbage: Result<RestoreBody, Status> = parse_body_json("{nope");
		assert!(garbage.is_err());
	}
	#[tokio::test]
	async fn verified_artifact_range_streams_in_bounded_chunks() {
		let temp = tempfile::tempdir().expect("temp dir");
		let store = crate::function::artifact::ArtifactStore::open(temp.path()).expect("store");
		let bytes = (0..(ARTIFACT_CHUNK_SIZE * 3 + 17))
			.map(|index| (index % 251) as u8)
			.collect::<Vec<_>>();
		let stored = store.put(&bytes).expect("artifact");
		let reader = store
			.open_verified(&stored.digest, Some(stored.size))
			.expect("verified reader");
		let offset = 31_u64;
		let length = (ARTIFACT_CHUNK_SIZE * 2 + 7) as u64;
		let mut stream = stream_verified_artifact(reader, offset, length);
		let mut chunks = Vec::new();
		while let Some(chunk) = stream.next().await {
			chunks.push(chunk.expect("chunk"));
		}
		assert_eq!(chunks.len(), 3);
		assert!(
			chunks
				.iter()
				.all(|chunk| chunk.data.len() <= ARTIFACT_CHUNK_SIZE)
		);
		assert_eq!(chunks[0].offset, offset);
		assert_eq!(chunks[1].offset, offset + ARTIFACT_CHUNK_SIZE as u64);
		assert_eq!(chunks[2].offset, offset + (ARTIFACT_CHUNK_SIZE * 2) as u64);
		assert!(chunks[..2].iter().all(|chunk| !chunk.eof));
		assert!(chunks[2].eof);
		let streamed = chunks
			.into_iter()
			.flat_map(|chunk| chunk.data)
			.collect::<Vec<_>>();
		assert_eq!(streamed, bytes[offset as usize..(offset + length) as usize],);
	}

	#[tokio::test]
	async fn persisted_event_stream_pages_past_ten_thousand() {
		const EVENT_COUNT: u64 = 10_017;
		let page_loads = Arc::new(std::sync::atomic::AtomicUsize::new(0));
		let loads = Arc::clone(&page_loads);
		let mut stream = stream_call_event_pages(
			move |cursor| {
				loads.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
				let end = (cursor + 10_000).min(EVENT_COUNT);
				Ok((cursor + 1..=end)
					.map(|sequence| pb::CallEvent { sequence, ..Default::default() })
					.collect())
			},
			0,
		);
		let mut sequences = Vec::new();
		while let Some(event) = stream.next().await {
			sequences.push(event.expect("event").sequence);
		}
		assert_eq!(sequences.len(), EVENT_COUNT as usize);
		assert_eq!(sequences.first(), Some(&1));
		assert_eq!(sequences.last(), Some(&EVENT_COUNT));
		assert_eq!(page_loads.load(std::sync::atomic::Ordering::Relaxed), 2);
	}
	#[test]
	fn pending_restore_list_view_does_not_expose_a_serving_owner() {
		let mut pending = serde_json::Map::new();
		pending.insert("_mesh_restore_pending".to_owned(), json!({"kind": "replica"}));
		let record = CreateRecordWire {
			sid:             "sandbox".to_owned(),
			params:          pending,
			owner:           "candidate".to_owned(),
			epoch:           1,
			idempotency_key: "create".to_owned(),
			ha:              "async".to_owned(),
			restart_policy:  "none".to_owned(),
			created_at:      1.0,
		};

		let view = mesh_record_placeholder(&record, "default");
		assert_eq!(view.get("status").and_then(Value::as_str), Some("starting"));
		assert!(view.get("node").is_none());
	}
}
