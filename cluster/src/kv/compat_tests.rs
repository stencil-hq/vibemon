//! Temporary: prints openraft 0.9 JSON fixtures.

use std::collections::{BTreeMap, BTreeSet};

use openraft::{
	CommittedLeaderId, Entry, EntryPayload, LogId, Membership, SnapshotMeta, StoredMembership, Vote,
	error::RaftError,
	raft::{
		AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
		VoteRequest, VoteResponse,
	},
};

use super::{
	Op, Txn,
	log_store::Record,
	machine::Command,
	raft::{RaftPeer, TypeConfig},
};
use crate::meta::NodeId;

const A: NodeId = NodeId(0x1111);
const B: NodeId = NodeId(0x2222);

fn log_id(term: u64, index: u64) -> LogId<NodeId> {
	LogId::new(CommittedLeaderId::new(term, A), index)
}

fn membership() -> Membership<NodeId, RaftPeer> {
	let nodes = BTreeMap::from([
		(A, RaftPeer { cluster_url: "http://10.0.0.1:7100".into() }),
		(B, RaftPeer { cluster_url: "http://10.0.0.2:7100".into() }),
	]);
	Membership::new(vec![BTreeSet::from([A, B])], nodes)
}

fn txn_entry() -> Entry<TypeConfig> {
	Entry {
		log_id:  log_id(3, 7),
		payload: EntryPayload::Normal(Command::Txn {
			now_ms: 1_700_000_000_000,
			txn:    Txn {
				compare: vec![],
				success: vec![Op::Put { key: "db/k".into(), value: b"v1".to_vec(), ttl_ms: Some(500) }],
				failure: vec![],
			},
		}),
	}
}

#[test]
fn print_fixtures() {
	let p = |name: &str, s: String| println!("FIXTURE {name} {s}");
	p(
		"blank",
		serde_json::to_string(&Record::Entry(Entry { log_id: log_id(3, 6), payload: EntryPayload::Blank }))
			.unwrap(),
	);
	p("normal", serde_json::to_string(&Record::Entry(txn_entry())).unwrap());
	p(
		"membership",
		serde_json::to_string(&Record::Entry(Entry {
			log_id:  log_id(3, 8),
			payload: EntryPayload::Membership(membership()),
		}))
		.unwrap(),
	);
	p("truncate", serde_json::to_string(&Record::Truncate(5)).unwrap());
	p("purge", serde_json::to_string(&Record::Purge(log_id(2, 4))).unwrap());
	p("vote", serde_json::to_string(&Vote::new_committed(3, A)).unwrap());
	p("vote_uncommitted", serde_json::to_string(&Vote::new(4, B)).unwrap());
	p("committed", serde_json::to_string(&Some(log_id(3, 7))).unwrap());
	p("committed_none", serde_json::to_string(&None::<LogId<NodeId>>).unwrap());
	let meta = SnapshotMeta {
		last_log_id:     Some(log_id(3, 8)),
		last_membership: StoredMembership::new(Some(log_id(3, 8)), membership()),
		snapshot_id:     "3-8-0".into(),
	};
	p("snapshot_meta", serde_json::to_string(&meta).unwrap());
	let append = AppendEntriesRequest::<TypeConfig> {
		vote:          Vote::new_committed(3, A),
		prev_log_id:   Some(log_id(3, 6)),
		entries:       vec![txn_entry()],
		leader_commit: Some(log_id(3, 6)),
	};
	p("append_req", serde_json::to_string(&append).unwrap());
	let ok = |r: AppendEntriesResponse<NodeId>| {
		serde_json::to_string(&Ok::<_, RaftError<NodeId>>(r)).unwrap()
	};
	p("append_resp_success", ok(AppendEntriesResponse::Success));
	p("append_resp_partial", ok(AppendEntriesResponse::PartialSuccess(Some(log_id(3, 7)))));
	p("append_resp_conflict", ok(AppendEntriesResponse::Conflict));
	p("append_resp_higher", ok(AppendEntriesResponse::HigherVote(Vote::new(4, B))));
	let vote = VoteRequest::new(Vote::new(4, B), Some(log_id(3, 8)));
	p("vote_req", serde_json::to_string(&vote).unwrap());
	let vote_resp = VoteResponse { vote: Vote::new(4, B), vote_granted: true, last_log_id: Some(log_id(3, 8)) };
	p("vote_resp", serde_json::to_string(&Ok::<_, RaftError<NodeId>>(vote_resp)).unwrap());
	let snap = InstallSnapshotRequest::<TypeConfig> {
		vote:   Vote::new_committed(3, A),
		meta:   meta.clone(),
		offset: 0,
		data:   b"{}".to_vec(),
		done:   true,
	};
	p("snapshot_req", serde_json::to_string(&snap).unwrap());
	let snap_resp = InstallSnapshotResponse { vote: Vote::new_committed(3, A) };
	p(
		"snapshot_resp",
		serde_json::to_string(&Ok::<_, RaftError<NodeId, openraft::error::InstallSnapshotError>>(
			snap_resp,
		))
		.unwrap(),
	);
}
