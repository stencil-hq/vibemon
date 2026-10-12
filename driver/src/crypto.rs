//! Secrets derived from the cluster secret (ARCHITECTURE.md §4).
//!
//! Sandbox access tokens, the AES-256-GCM data key, JWT signing, signed
//! upload URLs, and the API-key / refresh-token / password primitives.

use std::fmt;

use aes_gcm::{
	Aes256Gcm, Nonce,
	aead::{Aead, KeyInit},
};
use argon2::{Argon2, PasswordHash, PasswordHasher, PasswordVerifier, password_hash::SaltString};
use hkdf::Hkdf;
use hmac::{Hmac, Mac};
use jsonwebtoken::{Algorithm, DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sha2::{Digest, Sha256};

type HmacSha256 = Hmac<Sha256>;

/// HKDF label for the envd/traffic token seed.
const LABEL_ENVD: &[u8] = b"envd-token";
/// HKDF label for the JWT (HS256) key.
const LABEL_JWT: &[u8] = b"jwt";
/// HKDF label for the AES-256-GCM data key.
const LABEL_DATA: &[u8] = b"data-key";
/// HKDF label for template file upload URL signatures.
const LABEL_UPLOAD: &[u8] = b"upload-url";
/// AES-GCM nonce length (prefixed to every ciphertext).
const NONCE_LEN: usize = 12;
/// Prefix of every team API key.
pub const API_KEY_PREFIX: &str = "e2b_";
/// Random bytes in an API key (rendered as 40 lowercase hex chars).
const API_KEY_BYTES: usize = 20;
/// JWT issuer stamped into every token we mint.
pub const JWT_ISSUER: &str = "vmon";
/// Audience of CLI/user access tokens.
pub const AUD_ACCESS: &str = "vmon-access";
/// Audience of volume-content tokens.
pub const AUD_VOLUME: &str = "vmon-volume";

/// Failure decrypting stored ciphertext (wrong key after a secret rotation,
/// truncation, or tampering).
#[derive(Debug, thiserror::Error)]
pub enum CryptoError {
	/// Ciphertext shorter than the nonce prefix.
	#[error("ciphertext too short")]
	Truncated,
	/// Authentication tag mismatch.
	#[error("ciphertext failed authentication")]
	Tampered,
	/// Decrypted bytes are not UTF-8 where text was expected.
	#[error("plaintext is not UTF-8")]
	NotUtf8,
}

/// Why a presented API key was rejected before any lookup (E2B messages).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApiKeyFormatError {
	/// Missing the `e2b_` prefix.
	MissingPrefix,
	/// Prefix present but the remainder is not hex.
	InvalidFormat,
}

/// Claims of a user access token (CLI login, `Authorization: Bearer`).
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct AccessClaims {
	/// User id.
	pub sub:   String,
	/// User email (informational).
	pub email: String,
	pub iss:   String,
	pub aud:   String,
	pub iat:   i64,
	pub exp:   i64,
}

/// Claims of a volume-content token (E2B `VolumeJWT`): scoped to one volume
/// of one team.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct VolumeClaims {
	/// Team id.
	pub sub:     String,
	pub teamid:  String,
	pub volid:   String,
	pub voltype: String,
	pub jti:     String,
	pub iss:     String,
	pub aud:     String,
	pub iat:     i64,
	pub nbf:     i64,
	pub exp:     i64,
}

/// Every key the driver derives from `--cluster-secret`. Built once by
/// [`crate::Driver::start`] and shared through [`crate::AppState`].
#[derive(Clone)]
pub struct Keys {
	envd_seed: [u8; 32],
	upload:    [u8; 32],
	jwt:       [u8; 32],
	data:      Aes256Gcm,
}

impl fmt::Debug for Keys {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.write_str("Keys(..)")
	}
}

fn hkdf32(secret: &[u8], label: &[u8]) -> [u8; 32] {
	let mut okm = [0u8; 32];
	Hkdf::<Sha256>::new(None, secret)
		.expand(label, &mut okm)
		.expect("32 bytes is a valid HKDF-SHA256 length");
	okm
}

fn hmac_hex(key: &[u8], parts: &[&[u8]]) -> String {
	let mut mac = <HmacSha256 as Mac>::new_from_slice(key).expect("HMAC accepts any key length");
	for part in parts {
		mac.update(part);
	}
	hex::encode(mac.finalize().into_bytes())
}

impl Keys {
	/// Derives all keys with HKDF-SHA256 from the shared cluster secret.
	pub fn derive(secret: &[u8]) -> Self {
		let data_key = hkdf32(secret, LABEL_DATA);
		Self {
			envd_seed: hkdf32(secret, LABEL_ENVD),
			upload:    hkdf32(secret, LABEL_UPLOAD),
			jwt:       hkdf32(secret, LABEL_JWT),
			data:      Aes256Gcm::new_from_slice(&data_key).expect("32-byte AES-256 key"),
		}
	}

	/// envd access token for a sandbox: `hex(HMAC-SHA256(seed, sandboxID))`.
	/// Deterministic, so paused sandboxes and resumes recompute it.
	pub fn envd_token(&self, sandbox_id: &str) -> String {
		hmac_hex(&self.envd_seed, &[sandbox_id.as_bytes()])
	}

	/// Traffic access token: `hex(HMAC-SHA256(seed, "sandbox-traffic-" + id))`.
	pub fn traffic_token(&self, sandbox_id: &str) -> String {
		hmac_hex(&self.envd_seed, &[b"sandbox-traffic-", sandbox_id.as_bytes()])
	}

	/// AES-256-GCM encrypts `plain`; output is `nonce(12) || ciphertext+tag`.
	pub fn encrypt(&self, plain: &[u8]) -> Vec<u8> {
		let nonce_bytes: [u8; NONCE_LEN] = rand::random();
		let sealed = self
			.data
			.encrypt(Nonce::from_slice(&nonce_bytes), plain)
			.expect("AES-GCM encryption of in-memory data cannot fail");
		let mut out = Vec::with_capacity(NONCE_LEN + sealed.len());
		out.extend_from_slice(&nonce_bytes);
		out.extend_from_slice(&sealed);
		out
	}

	/// Inverse of [`Keys::encrypt`].
	pub fn decrypt(&self, data: &[u8]) -> Result<Vec<u8>, CryptoError> {
		if data.len() < NONCE_LEN {
			return Err(CryptoError::Truncated);
		}
		let (nonce, sealed) = data.split_at(NONCE_LEN);
		self
			.data
			.decrypt(Nonce::from_slice(nonce), sealed)
			.map_err(|_| CryptoError::Tampered)
	}

	/// [`Keys::encrypt`] for text.
	pub fn encrypt_str(&self, plain: &str) -> Vec<u8> {
		self.encrypt(plain.as_bytes())
	}

	/// [`Keys::decrypt`] for text.
	pub fn decrypt_str(&self, data: &[u8]) -> Result<String, CryptoError> {
		String::from_utf8(self.decrypt(data)?).map_err(|_| CryptoError::NotUtf8)
	}

	/// Signs a template file upload URL: returns the query string
	/// `expires=<unix>&signature=<hex>` to append to `path`.
	pub fn sign_upload_url(&self, path: &str, expires_at_unix: i64) -> String {
		let signature = self.upload_signature(path, expires_at_unix);
		format!("expires={expires_at_unix}&signature={signature}")
	}

	/// Checks an upload URL signature produced by [`Keys::sign_upload_url`]
	/// and that it has not expired.
	pub fn verify_upload_url(&self, path: &str, expires_at_unix: i64, signature: &str) -> bool {
		if expires_at_unix < chrono::Utc::now().timestamp() {
			return false;
		}
		ct_eq(self.upload_signature(path, expires_at_unix).as_bytes(), signature.as_bytes())
	}

	fn upload_signature(&self, path: &str, expires_at_unix: i64) -> String {
		hmac_hex(&self.upload, &[path.as_bytes(), b"\n", expires_at_unix.to_string().as_bytes()])
	}

	/// Signs `claims` as an HS256 JWT.
	pub fn sign_jwt<C: Serialize>(&self, claims: &C) -> String {
		jsonwebtoken::encode(
			&Header::new(Algorithm::HS256),
			claims,
			&EncodingKey::from_secret(&self.jwt),
		)
		.expect("HS256 signing of serializable claims cannot fail")
	}

	/// Verifies an HS256 JWT (signature, `exp`/`nbf`, issuer, audience).
	pub fn verify_jwt<C: DeserializeOwned>(&self, token: &str, audience: &str) -> Option<C> {
		let mut validation = Validation::new(Algorithm::HS256);
		validation.leeway = 0;
		validation.validate_nbf = true;
		validation.set_audience(&[audience]);
		validation.set_issuer(&[JWT_ISSUER]);
		validation.set_required_spec_claims(&["exp", "sub", "aud", "iss"]);
		jsonwebtoken::decode::<C>(token, &DecodingKey::from_secret(&self.jwt), &validation)
			.ok()
			.map(|data| data.claims)
	}

	/// Mints a user access token valid for `ttl_secs`.
	pub fn access_token(&self, user_id: &str, email: &str, ttl_secs: i64) -> String {
		let now = chrono::Utc::now().timestamp();
		self.sign_jwt(&AccessClaims {
			sub:   user_id.to_owned(),
			email: email.to_owned(),
			iss:   JWT_ISSUER.into(),
			aud:   AUD_ACCESS.into(),
			iat:   now,
			exp:   now + ttl_secs,
		})
	}

	/// Verifies a user access token.
	pub fn verify_access_token(&self, token: &str) -> Option<AccessClaims> {
		self.verify_jwt(token, AUD_ACCESS)
	}

	/// Mints a volume-content token for `volume_id` of `team_id`.
	pub fn volume_token(&self, team_id: &str, volume_id: &str, ttl_secs: i64) -> String {
		let now = chrono::Utc::now().timestamp();
		self.sign_jwt(&VolumeClaims {
			sub:     team_id.to_owned(),
			teamid:  team_id.to_owned(),
			volid:   volume_id.to_owned(),
			voltype: "local".into(),
			jti:     uuid::Uuid::new_v4().to_string(),
			iss:     JWT_ISSUER.into(),
			aud:     AUD_VOLUME.into(),
			iat:     now,
			nbf:     now,
			exp:     now + ttl_secs,
		})
	}

	/// Verifies a volume-content token.
	pub fn verify_volume_token(&self, token: &str) -> Option<VolumeClaims> {
		self.verify_jwt(token, AUD_VOLUME)
	}
}

/// Constant-time byte comparison (length leaks, contents do not).
pub fn ct_eq(a: &[u8], b: &[u8]) -> bool {
	a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

/// Generates a fresh team API key (`e2b_` + 40 lowercase hex).
pub fn generate_api_key() -> String {
	let bytes: [u8; API_KEY_BYTES] = rand::random();
	format!("{API_KEY_PREFIX}{}", hex::encode(bytes))
}

/// Validates the key shape and returns its storage hash: hex SHA-256 of the
/// hex-decoded key bytes (upstream hashes the raw bytes, not the text).
pub fn hash_api_key(key: &str) -> Result<String, ApiKeyFormatError> {
	let body = key
		.strip_prefix(API_KEY_PREFIX)
		.ok_or(ApiKeyFormatError::MissingPrefix)?;
	let raw = hex::decode(body).map_err(|_| ApiKeyFormatError::InvalidFormat)?;
	if raw.is_empty() {
		return Err(ApiKeyFormatError::InvalidFormat);
	}
	Ok(sha256_hex(&raw))
}

/// Masking details of an API key: `(masked_prefix, masked_suffix)` = first 2
/// and last 4 characters after the `e2b_` prefix.
pub fn mask_api_key(key: &str) -> (String, String) {
	let body = key.strip_prefix(API_KEY_PREFIX).unwrap_or(key);
	let prefix: String = body.chars().take(2).collect();
	let suffix: String = body
		.chars()
		.skip(body.chars().count().saturating_sub(4))
		.collect();
	(prefix, suffix)
}

/// Length of the hex body of generated API keys
/// (`IdentifierMaskingDetails.valueLength`).
pub const API_KEY_VALUE_LEN: usize = API_KEY_BYTES * 2;

/// Lowercase hex SHA-256 (refresh tokens, API keys).
pub fn sha256_hex(data: &[u8]) -> String {
	hex::encode(Sha256::digest(data))
}

/// Fresh opaque refresh token (64 hex chars).
pub fn generate_refresh_token() -> String {
	hex::encode(rand::random::<[u8; 32]>())
}

/// Argon2id hash in PHC string form.
pub fn hash_password(password: &str) -> String {
	let salt = SaltString::encode_b64(&rand::random::<[u8; 16]>()).expect("16-byte salt encodes");
	Argon2::default()
		.hash_password(password.as_bytes(), &salt)
		.expect("argon2 hashing with default params cannot fail")
		.to_string()
}

/// Verifies `password` against a PHC hash from [`hash_password`].
pub fn verify_password(password: &str, phc: &str) -> bool {
	PasswordHash::new(phc).is_ok_and(|parsed| {
		Argon2::default()
			.verify_password(password.as_bytes(), &parsed)
			.is_ok()
	})
}

/// E2B webhook signature: `base64(SHA-256(secret + payload))` without padding
/// (sent as `e2b-signature`).
pub fn webhook_signature(secret: &str, payload: &[u8]) -> String {
	use base64::Engine;
	let mut hasher = Sha256::new();
	hasher.update(secret.as_bytes());
	hasher.update(payload);
	base64::engine::general_purpose::STANDARD_NO_PAD.encode(hasher.finalize())
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn sandbox_tokens_match_reference_construction() {
		let keys = Keys::derive(b"cluster-secret");
		let seed = hkdf32(b"cluster-secret", b"envd-token");
		let mut mac = <HmacSha256 as Mac>::new_from_slice(&seed).unwrap();
		mac.update(b"iabc");
		assert_eq!(keys.envd_token("iabc"), hex::encode(mac.finalize().into_bytes()));
		assert_eq!(keys.envd_token("iabc").len(), 64);
		assert_ne!(keys.envd_token("iabc"), keys.traffic_token("iabc"));
		assert_ne!(keys.envd_token("iabc"), Keys::derive(b"other").envd_token("iabc"));
	}

	#[test]
	fn encryption_round_trips_and_detects_tampering() {
		let keys = Keys::derive(b"s");
		let sealed = keys.encrypt_str("hello");
		assert_ne!(keys.encrypt_str("hello"), sealed, "nonces must differ");
		assert_eq!(keys.decrypt_str(&sealed).unwrap(), "hello");
		let mut bad = sealed.clone();
		*bad.last_mut().unwrap() ^= 1;
		assert!(matches!(keys.decrypt(&bad), Err(CryptoError::Tampered)));
		assert!(matches!(keys.decrypt(&sealed[..5]), Err(CryptoError::Truncated)));
		assert!(Keys::derive(b"rotated").decrypt(&sealed).is_err());
	}

	#[test]
	fn jwt_rejects_expired_wrong_audience_and_foreign_key() {
		let keys = Keys::derive(b"s");
		let token = keys.access_token("u1", "a@b.c", 60);
		assert_eq!(keys.verify_access_token(&token).unwrap().sub, "u1");
		assert!(
			keys.verify_volume_token(&token).is_none(),
			"access token must not pass as volume token"
		);
		assert!(Keys::derive(b"other").verify_access_token(&token).is_none());
		let expired = keys.access_token("u1", "a@b.c", -1);
		assert!(keys.verify_access_token(&expired).is_none());
		let volume = keys.volume_token("t1", "v1", 60);
		let claims = keys.verify_volume_token(&volume).unwrap();
		assert_eq!((claims.teamid.as_str(), claims.volid.as_str()), ("t1", "v1"));
		assert!(keys.verify_access_token(&volume).is_none());
	}

	#[test]
	fn upload_urls_bind_path_and_expiry() {
		let keys = Keys::derive(b"s");
		let exp = chrono::Utc::now().timestamp() + 60;
		let query = keys.sign_upload_url("/a", exp);
		let signature = query.split("signature=").nth(1).unwrap();
		assert!(keys.verify_upload_url("/a", exp, signature));
		assert!(!keys.verify_upload_url("/b", exp, signature));
		assert!(!keys.verify_upload_url("/a", exp + 1, signature));
		let past = chrono::Utc::now().timestamp() - 1;
		let sig_past = keys.sign_upload_url("/a", past);
		assert!(!keys.verify_upload_url("/a", past, sig_past.split("signature=").nth(1).unwrap()));
	}

	#[test]
	fn api_key_format_hash_and_mask() {
		let key = generate_api_key();
		assert_eq!(key.len(), 44);
		assert!(key.starts_with("e2b_"));
		let hash = hash_api_key(&key).unwrap();
		assert_eq!(hash, sha256_hex(&hex::decode(&key[4..]).unwrap()));
		assert_eq!(hash_api_key("sk_123"), Err(ApiKeyFormatError::MissingPrefix));
		assert_eq!(hash_api_key("e2b_zz"), Err(ApiKeyFormatError::InvalidFormat));
		assert_eq!(hash_api_key("e2b_"), Err(ApiKeyFormatError::InvalidFormat));
		assert_eq!(mask_api_key("e2b_abcdef0123"), ("ab".into(), "0123".into()));
	}

	#[test]
	fn passwords_verify_only_with_the_right_secret() {
		let phc = hash_password("hunter2");
		assert!(verify_password("hunter2", &phc));
		assert!(!verify_password("hunter3", &phc));
		assert!(!verify_password("hunter2", "not-a-phc"));
	}

	#[test]
	fn webhook_signature_matches_documented_algorithm() {
		// echo -n 'secret{"a":1}' | sha256sum | xxd -r -p | base64 (padding
		// stripped)
		let sig = webhook_signature("secret", br#"{"a":1}"#);
		assert!(!sig.ends_with('='));
		use base64::Engine;
		let expected =
			base64::engine::general_purpose::STANDARD.encode(Sha256::digest(br#"secret{"a":1}"#));
		assert_eq!(sig, expected.trim_end_matches('='));
	}
}
