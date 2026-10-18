use std::{fs::OpenOptions, io::Write as _, path::Path};

use super::*;

/// Small segments so a few records span several files.
const SMALL: u64 = 256;

fn record(seq: u64, streams: &[&str]) -> Appended {
	Appended {
		seq,
		ts_ms: 1_000 + seq,
		streams: streams.iter().map(|s| (*s).to_owned()).collect(),
		value: format!("v{seq}").into_bytes(),
	}
}

/// Appends `range` one record per call (one applied entry each): all in
/// `all`, even ones also in `even`.
fn fill_range(journal: &mut Journal, range: Range<u64>) {
	for seq in range {
		let streams: &[&str] = if seq % 2 == 0 {
			&["all", "even"]
		} else {
			&["all"]
		};
		journal.append(&[record(seq, streams)]).unwrap();
	}
}

fn scan(
	journal: &Journal,
	stream: &str,
	after: Option<u64>,
	limit: Option<usize>,
	reverse: bool,
) -> Vec<u64> {
	let scan = Scan { stream: stream.into(), after, limit, reverse };
	journal
		.plan(&scan)
		.run()
		.unwrap()
		.into_iter()
		.map(|r| r.seq)
		.collect()
}

fn flush(journal: &mut Journal) {
	let plan = journal.flush_plan();
	plan.run().unwrap();
	journal.flushed(&plan).unwrap();
}

fn open(dir: &Path) -> Journal {
	Journal::open_with(dir, SMALL).unwrap()
}

/// `start..end` (keeps one-hole assertions readable for clippy).
const fn hole(start: u64, end: u64) -> Range<u64> {
	start..end
}

#[test]
fn scans_page_both_ways_over_memory_and_disk_indexes() {
	let dir = tempfile::tempdir().unwrap();
	let mut journal = open(dir.path());
	fill_range(&mut journal, 1..11);
	flush(&mut journal);
	fill_range(&mut journal, 11..21);
	assert!(journal.segments.len() > 3, "small segments roll");
	assert!(journal.segments.iter().any(Segment::indexed));
	assert!(journal.segments.iter().any(|s| !s.indexed()));

	assert_eq!(scan(&journal, "all", None, None, false), (1..21).collect::<Vec<_>>());
	assert_eq!(scan(&journal, "all", Some(0), Some(2), false), [1, 2]);
	assert_eq!(scan(&journal, "even", Some(5), Some(3), false), [6, 8, 10]);
	assert_eq!(scan(&journal, "even", Some(15), Some(4), true), [14, 12, 10, 8]);
	assert_eq!(scan(&journal, "all", None, Some(1), true), [20]);
	assert_eq!(scan(&journal, "all", Some(1), None, true), Vec::<u64>::new());
	assert_eq!(scan(&journal, "missing", None, None, false), Vec::<u64>::new());
	let first = journal.plan(&Scan {
		stream:  "even".into(),
		after:   None,
		limit:   Some(1),
		reverse: false,
	});
	let first = first.run().unwrap().remove(0);
	assert_eq!((first.seq, first.ts_ms, first.value), (2, 1_002, b"v2".to_vec()));
}

#[test]
fn reopen_truncates_a_torn_tail_and_replay_skips_present_records() {
	let dir = tempfile::tempdir().unwrap();
	let mut journal = Journal::open(dir.path()).unwrap();
	assert_eq!(journal.next, 1, "seqs start at 1");
	fill_range(&mut journal, 1..6);
	flush(&mut journal);
	fill_range(&mut journal, 6..9);
	let active = segment_path(dir.path(), journal.segments.last().unwrap().first);
	drop(journal);
	OpenOptions::new()
		.append(true)
		.open(&active)
		.unwrap()
		.write_all(b"torn bytes")
		.unwrap();

	let mut journal = Journal::open(dir.path()).unwrap();
	assert_eq!(journal.next, 9);
	// Log replay re-applies records the journal already holds.
	fill_range(&mut journal, 7..11);
	assert_eq!(scan(&journal, "all", None, None, false), (1..11).collect::<Vec<_>>());
	assert!(journal.complete());
}

#[test]
fn a_torn_sealed_segment_drops_the_unindexed_segments_after_it() {
	let dir = tempfile::tempdir().unwrap();
	let mut journal = open(dir.path());
	fill_range(&mut journal, 1..5);
	flush(&mut journal);
	fill_range(&mut journal, 5..13);
	let torn = journal.segments.iter().find(|s| !s.indexed()).unwrap();
	let (torn_first, torn_len) = (torn.first, torn.len);
	drop(journal);
	OpenOptions::new()
		.write(true)
		.open(segment_path(dir.path(), torn_first))
		.unwrap()
		.set_len(torn_len - 3)
		.unwrap();

	let journal = open(dir.path());
	// Everything after the tear is gone; replay appends it again from `next`.
	let kept = scan(&journal, "all", None, None, false);
	assert_eq!(kept, (1..journal.next).collect::<Vec<_>>());
	assert!(journal.next < 13);
	assert!(journal.complete());
}

#[test]
fn advance_records_a_hole_that_fill_and_settling_close() {
	let dir = tempfile::tempdir().unwrap();
	let mut journal = Journal::open(dir.path()).unwrap();
	fill_range(&mut journal, 1..5);
	journal.advance(10).unwrap();
	fill_range(&mut journal, 10..12);
	assert_eq!(journal.holes(), [hole(5, 10)]);
	// The hole survives a restart.
	drop(journal);
	let mut journal = Journal::open(dir.path()).unwrap();
	assert_eq!(journal.holes(), [hole(5, 10)]);

	// The peer kept records from 7 on: 5..7 expired everywhere.
	let copied = |records: Range<u64>, complete: bool| Copied {
		low: 7,
		complete,
		records: records.map(|seq| record(seq, &["all"])).collect(),
	};
	// A peer with holes of its own proves nothing about older records.
	journal.fill(5..10, &copied(7..9, false)).unwrap();
	assert_eq!(journal.holes(), [5..7, 9..10]);
	journal.fill(5..7, &copied(7..7, true)).unwrap();
	assert_eq!(journal.holes(), [hole(9, 10)]);
	let rest = copied(9..10, true);
	journal.fill(9..10, &rest).unwrap();
	assert!(journal.complete());
	assert_eq!(scan(&journal, "all", Some(2), None, false), [3, 4, 7, 8, 9, 10, 11]);
	// Overlapping copies are refused.
	assert!(journal.fill(9..10, &rest).is_err());
	drop(journal);
	assert!(Journal::open(dir.path()).unwrap().complete());
}

#[test]
fn prune_drops_old_indexed_segments_only() {
	let dir = tempfile::tempdir().unwrap();
	let mut journal = open(dir.path());
	fill_range(&mut journal, 1..13);
	flush(&mut journal);
	let indexed = journal.segments.iter().filter(|s| s.indexed()).count();
	assert!(indexed >= 2);
	// Record n is stamped 1000 + n: cut below the newest sealed one.
	let pruned = journal.prune(1_007).unwrap();
	assert!(pruned >= 1 && pruned < indexed);
	let kept = scan(&journal, "all", None, None, false);
	assert!(kept[0] > 1 && kept.ends_with(&[12]));
	assert!(journal.complete(), "pruned history is not a hole");
	journal.prune(u64::MAX).unwrap();
	assert_eq!(journal.segments.len(), 1, "the active segment stays");
}

#[test]
fn copy_plan_returns_the_range_and_the_oldest_seq() {
	let dir = tempfile::tempdir().unwrap();
	let mut journal = open(dir.path());
	fill_range(&mut journal, 1..11);
	flush(&mut journal);
	let copied = journal.copy_plan(3..7).run(usize::MAX).unwrap();
	assert_eq!((copied.low, copied.complete), (1, true));
	assert_eq!(copied.records.iter().map(|r| r.seq).collect::<Vec<_>>(), [3, 4, 5, 6]);
	assert_eq!(copied.records[1], record(4, &["all", "even"]));
	let capped = journal.copy_plan(1..11).run(1).unwrap();
	assert_eq!(capped.records.len(), 1);
}
