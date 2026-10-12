//! `limit` + `nextToken` pagination with the cursor returned in
//! `X-Next-Token` (ARCHITECTURE.md §9.1).

use axum::{
	Json,
	http::{HeaderName, HeaderValue},
	response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};

use crate::{error::ApiError, store::Cursor};

/// Response header carrying the next page cursor.
pub const NEXT_TOKEN_HEADER: HeaderName = HeaderName::from_static("x-next-token");
/// Default and maximum page size.
pub const MAX_LIMIT: u32 = 100;

/// Query parameters shared by paginated lists.
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PageQuery {
	pub limit:      Option<i64>,
	pub next_token: Option<String>,
}

/// Validated page request.
#[derive(Debug, PartialEq, Eq)]
pub struct Page {
	pub limit: u32,
	pub after: Option<Cursor>,
}

impl PageQuery {
	/// Validates `limit` (1..=100, default 100) and decodes `nextToken`.
	pub fn page(&self) -> Result<Page, ApiError> {
		let limit = match self.limit {
			None => MAX_LIMIT,
			Some(n) if (1..=i64::from(MAX_LIMIT)).contains(&n) => {
				u32::try_from(n).unwrap_or(MAX_LIMIT)
			},
			Some(_) => {
				return Err(ApiError::bad_request(format!("limit must be between 1 and {MAX_LIMIT}")));
			},
		};
		let after = match self.next_token.as_deref().filter(|t| !t.is_empty()) {
			None => None,
			Some(token) => {
				Some(Cursor::decode(token).ok_or_else(|| ApiError::bad_request("Invalid next token"))?)
			},
		};
		Ok(Page { limit, after })
	}
}

/// Splits a `limit + 1` fetch into the page and the cursor of its last row
/// when more rows exist.
pub fn split_page<T>(
	mut rows: Vec<T>,
	limit: u32,
	cursor: impl Fn(&T) -> Cursor,
) -> (Vec<T>, Option<Cursor>) {
	let limit = limit as usize;
	if rows.len() > limit {
		rows.truncate(limit);
		let next = rows.last().map(&cursor);
		(rows, next)
	} else {
		(rows, None)
	}
}

/// JSON array response with `X-Next-Token` when another page exists.
pub fn paged_json<T: Serialize>(items: Vec<T>, next: Option<Cursor>) -> Response {
	let mut response = Json(items).into_response();
	if let Some(next) = next
		&& let Ok(value) = HeaderValue::from_str(&next.encode())
	{
		response.headers_mut().insert(NEXT_TOKEN_HEADER, value);
	}
	response
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn limit_bounds_and_token_validation() {
		assert_eq!(PageQuery::default().page().unwrap().limit, 100);
		assert_eq!(
			PageQuery { limit: Some(1), next_token: None }
				.page()
				.unwrap()
				.limit,
			1
		);
		assert!(
			PageQuery { limit: Some(0), next_token: None }
				.page()
				.is_err()
		);
		assert!(
			PageQuery { limit: Some(101), next_token: None }
				.page()
				.is_err()
		);
		assert!(
			PageQuery { limit: None, next_token: Some("@@".into()) }
				.page()
				.is_err()
		);
		let cursor = Cursor { created_at_us: 5, id: "x".into() };
		assert_eq!(
			PageQuery { limit: None, next_token: Some(cursor.encode()) }
				.page()
				.unwrap()
				.after,
			Some(cursor)
		);
	}

	#[test]
	fn split_page_reports_next_only_when_more_rows_exist() {
		let cursor = |n: &i64| Cursor { created_at_us: *n, id: n.to_string() };
		let (rows, next) = split_page(vec![1, 2, 3], 2, cursor);
		assert_eq!(rows, vec![1, 2]);
		assert_eq!(next.unwrap().created_at_us, 2);
		let (rows, next) = split_page(vec![1, 2], 2, cursor);
		assert_eq!(rows, vec![1, 2]);
		assert!(next.is_none());
	}
}
