use std::collections::HashMap;

use parking_lot::Mutex;
use serde_json::Value;

use crate::error::Result;

/// Control half of a live persistent PTY attachment.
pub trait PtyControl: Send {
	fn write(&mut self, data: &[u8]) -> Result<()>;
	fn resize(&mut self, rows: u16, cols: u16) -> Result<()>;
	fn detach(&mut self) -> Result<()>;
}

/// Live output attachment. Session state and scrollback remain authoritative in
/// the guest agent.
pub struct PtyStream {
	pub session: Value,
	pub control: Box<dyn PtyControl>,
	pub stdout:  flume::Receiver<Vec<u8>>,
	pub exit:    flume::Receiver<super::ExecExit>,
}

/// Last-known metadata cache used only while the guest is unreachable during
/// suspension.
#[derive(Default)]
pub struct PtyCache {
	sessions: Mutex<HashMap<String, HashMap<String, Value>>>,
}

impl PtyCache {
	pub fn remember(&self, sandbox_id: &str, sessions: impl IntoIterator<Item = Value>) {
		let mut cache = self.sessions.lock();
		let values = cache.entry(sandbox_id.to_owned()).or_default();
		for session in sessions {
			if let Some(id) = session
				.get("session_id")
				.and_then(Value::as_str)
				.map(str::to_owned)
			{
				values.insert(id, session);
			}
		}
	}

	pub fn replace(&self, sandbox_id: &str, sessions: impl IntoIterator<Item = Value>) {
		self.sessions.lock().remove(sandbox_id);
		self.remember(sandbox_id, sessions);
	}

	pub fn suspended(&self, sandbox_id: &str) -> Vec<Value> {
		let mut values = self
			.sessions
			.lock()
			.get(sandbox_id)
			.map(|sessions| sessions.values().cloned().collect::<Vec<_>>())
			.unwrap_or_default();
		for session in &mut values {
			if let Some(object) = session.as_object_mut() {
				object.insert("suspended".to_owned(), Value::Bool(true));
				object.insert("attached_count".to_owned(), Value::from(0));
			}
		}
		values.sort_by_key(|session| {
			session
				.get("created_at_unix_millis")
				.and_then(Value::as_u64)
				.unwrap_or_default()
		});
		values
	}

	pub fn forget(&self, sandbox_id: &str, session_id: &str) {
		if let Some(sessions) = self.sessions.lock().get_mut(sandbox_id) {
			sessions.remove(session_id);
		}
	}
}
