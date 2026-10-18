//! Serialization compatibility with openraft 0.9.
//!
//! The fixtures are the exact JSON the openraft 0.9.25 build wrote to
//! `<data>/raft/` (log records, `vote.json`, `committed.bin` payloads,
//! `snapshot.bin` headers) and exchanged on `/cluster/v1/raft/*`. Persisted
//! state must keep decoding (existing data directories open and replay); the
//! RPC bodies document where the 0.10 wire format differs (drivers of the two
//! versions cannot replicate to each other).

use std::collections::{BTreeMap, BTreeSet};

use openraft::{
	EntryPayload,
	error::{Fatal, RaftError},
	raft::{
		AppendEntriesRequest, AppendEntriesResponse, SnapshotResponse, VoteRequest, VoteResponse,
	},
};
use serde::{Serialize, de::DeserializeOwned};

use super::{
	Op, Txn,
	log_store::{Record, RecordRef},
	machine::Command,
	raft::{LeaderId, LogEntry, LogId, RaftPeer, SnapshotMeta, StoredMembership, TypeConfig, Vote},
};
use crate::meta::NodeId;

type Membership = openraft::Membership<NodeId, RaftPeer>;

const BLANK: &str = r#"{"Entry":{"log_id":{"leader_id":{"term":3,"node_id":"0000000000001111"},"index":6},"payload":"Blank"}}"#;
const NORMAL: &str = r#"{"Entry":{"log_id":{"leader_id":{"term":3,"node_id":"0000000000001111"},"index":7},"payload":{"Normal":{"Txn":{"now_ms":1700000000000,"txn":{"compare":[],"success":[{"Put":{"key":"db/k","value":"7631","ttl_ms":500}}],"failure":[]}}}}}}"#;
const MEMBERSHIP: &str = r#"{"Entry":{"log_id":{"leader_id":{"term":3,"node_id":"0000000000001111"},"index":8},"payload":{"Membership":{"configs":[["0000000000001111","0000000000002222"]],"nodes":{"0000000000001111":{"cluster_url":"http://10.0.0.1:7100"},"0000000000002222":{"cluster_url":"http://10.0.0.2:7100"}}}}}}"#;
const TRUNCATE: &str = r#"{"Truncate":5}"#;
const PURGE: &str = r#"{"Purge":{"leader_id":{"term":2,"node_id":"0000000000001111"},"index":4}}"#;
const VOTE: &str = r#"{"leader_id":{"term":3,"node_id":"0000000000001111"},"committed":true}"#;
const VOTE_UNCOMMITTED: &str =
	r#"{"leader_id":{"term":4,"node_id":"0000000000002222"},"committed":false}"#;
const COMMITTED: &str = r#"{"leader_id":{"term":3,"node_id":"0000000000001111"},"index":7}"#;
const COMMITTED_NONE: &str = "null";
const SNAPSHOT_META: &str = r#"{"last_log_id":{"leader_id":{"term":3,"node_id":"0000000000001111"},"index":8},"last_membership":{"log_id":{"leader_id":{"term":3,"node_id":"0000000000001111"},"index":8},"membership":{"configs":[["0000000000001111","0000000000002222"]],"nodes":{"0000000000001111":{"cluster_url":"http://10.0.0.1:7100"},"0000000000002222":{"cluster_url":"http://10.0.0.2:7100"}}}},"snapshot_id":"3-8-0"}"#;
const APPEND_REQ: &str = r#"{"vote":{"leader_id":{"term":3,"node_id":"0000000000001111"},"committed":true},"prev_log_id":{"leader_id":{"term":3,"node_id":"0000000000001111"},"index":6},"entries":[{"log_id":{"leader_id":{"term":3,"node_id":"0000000000001111"},"index":7},"payload":{"Normal":{"Txn":{"now_ms":1700000000000,"txn":{"compare":[],"success":[{"Put":{"key":"db/k","value":"7631","ttl_ms":500}}],"failure":[]}}}}}],"leader_commit":{"leader_id":{"term":3,"node_id":"0000000000001111"},"index":6}}"#;
const APPEND_RESP_SUCCESS: &str = r#"{"Ok":"Success"}"#;
const APPEND_RESP_PARTIAL: &str =
	r#"{"Ok":{"PartialSuccess":{"leader_id":{"term":3,"node_id":"0000000000001111"},"index":7}}}"#;
const APPEND_RESP_CONFLICT: &str = r#"{"Ok":"Conflict"}"#;
const APPEND_RESP_HIGHER: &str = r#"{"Ok":{"HigherVote":{"leader_id":{"term":4,"node_id":"0000000000002222"},"committed":false}}}"#;
const VOTE_REQ: &str = r#"{"vote":{"leader_id":{"term":4,"node_id":"0000000000002222"},"committed":false},"last_log_id":{"leader_id":{"term":3,"node_id":"0000000000001111"},"index":8}}"#;
const VOTE_RESP: &str = r#"{"Ok":{"vote":{"leader_id":{"term":4,"node_id":"0000000000002222"},"committed":false},"vote_granted":true,"last_log_id":{"leader_id":{"term":3,"node_id":"0000000000001111"},"index":8}}}"#;
/// openraft 0.9's chunked `InstallSnapshotRequest`; 0.10 sends a whole
/// snapshot as a header line plus raw data instead (`network::SnapshotHeader`).
const SNAPSHOT_REQ: &str = r#"{"vote":{"leader_id":{"term":3,"node_id":"0000000000001111"},"committed":true},"meta":{"last_log_id":{"leader_id":{"term":3,"node_id":"0000000000001111"},"index":8},"last_membership":{"log_id":{"leader_id":{"term":3,"node_id":"0000000000001111"},"index":8},"membership":{"configs":[["0000000000001111","0000000000002222"]],"nodes":{"0000000000001111":{"cluster_url":"http://10.0.0.1:7100"},"0000000000002222":{"cluster_url":"http://10.0.0.2:7100"}}}},"snapshot_id":"3-8-0"},"offset":0,"data":[123,125],"done":true}"#;
const SNAPSHOT_RESP: &str =
	r#"{"Ok":{"vote":{"leader_id":{"term":3,"node_id":"0000000000001111"},"committed":true}}}"#;

const A: NodeId = NodeId(0x1111);
const B: NodeId = NodeId(0x2222);

fn log_id(term: u64, index: u64) -> LogId {
	LogId::new(LeaderId { term, node_id: A }, index)
}

fn membership() -> Membership {
	let nodes = BTreeMap::from([
		(A, RaftPeer { cluster_url: "http://10.0.0.1:7100".into() }),
		(B, RaftPeer { cluster_url: "http://10.0.0.2:7100".into() }),
	]);
	Membership::new(vec![BTreeSet::from([A, B])], nodes).unwrap()
}

fn blank_entry() -> LogEntry {
	LogEntry { log_id: log_id(3, 6), payload: EntryPayload::Blank }
}

fn txn_entry() -> LogEntry {
	LogEntry {
		log_id:  log_id(3, 7),
		payload: EntryPayload::Normal(Command::Txn {
			now_ms: 1_700_000_000_000,
			txn:    Txn {
				compare: vec![],
				success: vec![Op::Put {
					key:    "db/k".into(),
					value:  b"v1".to_vec(),
					ttl_ms: Some(500),
				}],
				failure: vec![],
			},
		}),
	}
}

fn membership_entry() -> LogEntry {
	LogEntry { log_id: log_id(3, 8), payload: EntryPayload::Membership(membership()) }
}

fn snapshot_meta() -> SnapshotMeta {
	SnapshotMeta {
		last_log_id:     Some(log_id(3, 8)),
		last_membership: StoredMembership::new(Some(log_id(3, 8)), membership()),
	}
}

fn decode<T: DeserializeOwned>(json: &str) -> T {
	serde_json::from_str(json).unwrap_or_else(|error| panic!("{json}: {error}"))
}

fn encode<T: Serialize + ?Sized>(value: &T) -> String {
	serde_json::to_string(value).unwrap()
}

/// Log records, the vote, and the committed id of 0.9 data directories
/// decode, and 0.10 writes them byte for byte as 0.9 did.
#[test]
fn persisted_log_vote_and_committed_are_unchanged() {
	let records = [
		(BLANK, Record::Entry(blank_entry())),
		(NORMAL, Record::Entry(txn_entry())),
		(MEMBERSHIP, Record::Entry(membership_entry())),
		(TRUNCATE, Record::Truncate(5)),
		(PURGE, Record::Purge(LogId::new(LeaderId { term: 2, node_id: A }, 4))),
	];
	for (json, record) in &records {
		assert_eq!(&decode::<Record>(json), record);
		let written = match record {
			Record::Entry(entry) => encode(&RecordRef::Entry(entry)),
			Record::Truncate(index) => encode(&RecordRef::Truncate(*index)),
			Record::Purge(log_id) => encode(&RecordRef::Purge(log_id)),
		};
		assert_eq!(&written, json);
	}

	for (json, vote) in [(VOTE, Vote::new_committed(3, A)), (VOTE_UNCOMMITTED, Vote::new(4, B))] {
		assert_eq!(decode::<Vote>(json), vote);
		assert_eq!(encode(&vote), json);
	}

	for (json, committed) in [(COMMITTED, Some(log_id(3, 7))), (COMMITTED_NONE, None)] {
		assert_eq!(decode::<Option<LogId>>(json), committed);
		assert_eq!(encode(&committed), json);
	}
}

/// A 0.9 `snapshot.bin` header decodes (its `snapshot_id` is ignored); 0.10
/// writes the reserved `snapshot_id` slot empty, which 0.9 decodes too.
#[test]
fn persisted_snapshot_meta_decodes() {
	assert_eq!(decode::<SnapshotMeta>(SNAPSHOT_META), snapshot_meta());
	assert_eq!(encode(&snapshot_meta()), SNAPSHOT_META.replace(r#""3-8-0""#, r#""""#));
}

/// `AppendEntries` and `Vote` bodies keep their 0.9 shapes (a 0.10 vote request
/// adds `leadership_transfer`, defaulted when absent and ignored by 0.9);
/// snapshot transfer changed, see [`SNAPSHOT_REQ`].
#[test]
fn rpc_bodies() {
	let append: AppendEntriesRequest<TypeConfig> = decode(APPEND_REQ);
	assert_eq!(append.vote, Vote::new_committed(3, A));
	assert_eq!(append.prev_log_id, Some(log_id(3, 6)));
	assert_eq!(append.entries, vec![txn_entry()]);
	assert_eq!(append.leader_commit, Some(log_id(3, 6)));
	assert_eq!(encode(&append), APPEND_REQ);

	type AppendReply = Result<AppendEntriesResponse<TypeConfig>, RaftError<TypeConfig>>;
	let replies: [(&str, AppendReply); 4] = [
		(APPEND_RESP_SUCCESS, Ok(AppendEntriesResponse::Success)),
		(APPEND_RESP_PARTIAL, Ok(AppendEntriesResponse::PartialSuccess(Some(log_id(3, 7))))),
		(APPEND_RESP_CONFLICT, Ok(AppendEntriesResponse::Conflict)),
		(APPEND_RESP_HIGHER, Ok(AppendEntriesResponse::HigherVote(Vote::new(4, B)))),
	];
	for (json, reply) in &replies {
		assert_eq!(&encode(&decode::<AppendReply>(json)), json);
		assert_eq!(&encode(reply), json);
	}

	let vote: VoteRequest<TypeConfig> = decode(VOTE_REQ);
	assert_eq!(vote, VoteRequest::new(Vote::new(4, B), Some(log_id(3, 8))));
	assert_eq!(
		encode(&vote),
		VOTE_REQ.replace(r"}}", r#"},"leadership_transfer":false}"#),
		"0.10 adds leadership_transfer"
	);
	let vote_reply: Result<VoteResponse<TypeConfig>, RaftError<TypeConfig>> = decode(VOTE_RESP);
	assert_eq!(
		vote_reply.as_ref().ok(),
		Some(&VoteResponse {
			vote:         Vote::new(4, B),
			vote_granted: true,
			last_log_id:  Some(log_id(3, 8)),
		})
	);
	assert_eq!(encode(&vote_reply), VOTE_RESP);

	let snapshot_reply: Result<SnapshotResponse<TypeConfig>, Fatal<TypeConfig>> =
		decode(SNAPSHOT_RESP);
	assert_eq!(
		snapshot_reply.as_ref().ok(),
		Some(&SnapshotResponse::new(Vote::new_committed(3, A)))
	);
	assert_eq!(encode(&snapshot_reply), SNAPSHOT_RESP);
	assert!(
		super::network::SnapshotHeader::decode(SNAPSHOT_REQ.into()).is_none(),
		"a 0.9 snapshot chunk is not a 0.10 snapshot body"
	);
}
