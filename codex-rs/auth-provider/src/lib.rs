use std::sync::Arc;

use http::HeaderMap;

/// Applies resolved authentication state to outbound HTTP request headers.
///
/// Implementations must be cheap and non-blocking. Token refresh, task
/// registration, and other I/O must happen before request construction reaches
/// this trait.
pub trait AuthProvider: Send + Sync {
    fn add_auth_headers(&self, headers: &mut HeaderMap);

    fn to_auth_headers(&self) -> HeaderMap {
        let mut headers = HeaderMap::new();
        self.add_auth_headers(&mut headers);
        headers
    }
}

/// Shared auth handle passed through API clients.
pub type SharedAuthProvider = Arc<dyn AuthProvider>;
