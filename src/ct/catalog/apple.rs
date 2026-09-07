//! Apple CT log catalog source.
//!
//! Apple does not publish a detached signature for this catalog. Authenticity
//! rests on the TLS-authenticated fetch of `valid.apple.com`. Apple therefore
//! has no detached-signature verifier and is non-runtime-authoritative by
//! default. Apple-only logs reach the runtime only via an explicit
//! `custom_logs` or `static_logs` declaration.

use super::SignedCatalog;
use super::verify::VerifyError;

pub struct Apple;

impl SignedCatalog for Apple {
    fn name(&self) -> &'static str {
        "apple"
    }
    fn code_default_runtime_authoritative(&self) -> bool {
        false
    }
    fn list_url(&self) -> &'static str {
        "https://valid.apple.com/ct/log_list/current_log_list.json"
    }
    fn sig_url(&self) -> Option<&'static str> {
        // No detached signature exists. `fetch_and_verify` treats a `None`
        // sig_url as the documented unverified-source policy and never calls
        // `verify`.
        None
    }
    fn expected_key_fingerprint(&self) -> Option<&'static str> {
        None
    }
    fn verify(&self, _bytes: &[u8], _sig: &[u8]) -> Result<(), VerifyError> {
        // Unreachable in normal flow (sig_url() is None). Defensive: an Apple
        // catalog can never produce a verified signature.
        Err(VerifyError::BadSignature)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identifies_itself_as_apple() {
        assert_eq!(Apple.name(), "apple");
    }

    #[test]
    fn is_not_runtime_authoritative_by_default() {
        assert!(!Apple.code_default_runtime_authoritative());
    }

    #[test]
    fn list_url_points_at_apples_own_log_list() {
        assert_eq!(
            Apple.list_url(),
            "https://valid.apple.com/ct/log_list/current_log_list.json"
        );
    }

    #[test]
    fn has_no_detached_signature_or_expected_key() {
        assert!(Apple.sig_url().is_none());
        assert!(Apple.expected_key_fingerprint().is_none());
    }

    #[test]
    fn verify_always_fails_defensively() {
        assert!(matches!(
            Apple.verify(b"anything", b"anything"),
            Err(VerifyError::BadSignature)
        ));
    }
}
