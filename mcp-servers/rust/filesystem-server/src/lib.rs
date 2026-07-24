use crate::sandbox::Sandbox;
use crate::server::{AppContext, FilesystemServer};
use anyhow::{Context, Result};
use axum::extract::State;
use axum::http::{Request, StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use rmcp::transport;
use std::net::SocketAddr;
use std::sync::Arc;
use tracing_subscriber::EnvFilter;

pub mod sandbox;
pub mod server;
pub mod tools;

pub static DEFAULT_BIND_ADDRESS: &str = "127.0.0.1:8084";
pub static APP_NAME: &str = env!("CARGO_PKG_NAME");
pub static APP_VERSION: &str = env!("CARGO_PKG_VERSION");
pub static MAX_FILE_SIZE: u64 = 1024 * 1024;

pub fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::new("INFO"))
        .with_ansi(true)
        .try_init();
}

/// Bearer token required on all HTTP requests when authentication is enabled.
#[derive(Clone, Debug)]
pub struct AuthToken(String);

impl AuthToken {
    pub fn new(token: &str) -> Self {
        Self(token.to_string())
    }

    pub fn is_valid(&self, provided: &str) -> bool {
        let expected = self.0.as_bytes();
        let provided = provided.as_bytes();
        if expected.is_empty() || expected.len() != provided.len() {
            return false;
        }
        // Constant-time comparison so the token cannot be recovered one byte
        // at a time through response-time measurement.
        expected
            .iter()
            .zip(provided.iter())
            .fold(0u8, |acc, (a, b)| acc | (a ^ b))
            == 0
    }
}

/// Parse and validate the bind address. Binding to a non-loopback address
/// without an auth token would expose the MCP endpoint to the network, so
/// refuse to start.
pub fn resolve_bind_address(bind: &str, auth_enabled: bool) -> Result<SocketAddr> {
    let addr: SocketAddr = bind
        .parse()
        .with_context(|| format!("Invalid bind address '{}', expected IP:PORT", bind))?;
    if !addr.ip().is_loopback() && !auth_enabled {
        anyhow::bail!(
            "Refusing to bind to non-loopback address '{}' without an auth token. \
             Pass --auth-token (or set FILESYSTEM_SERVER_AUTH_TOKEN), or bind to a loopback address.",
            bind
        );
    }
    Ok(addr)
}

async fn require_bearer_token(
    State(token): State<Arc<AuthToken>>,
    request: Request<axum::body::Body>,
    next: Next,
) -> Response {
    let authorized = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .is_some_and(|provided| token.is_valid(provided));

    if authorized {
        next.run(request).await
    } else {
        (
            StatusCode::UNAUTHORIZED,
            [(header::WWW_AUTHENTICATE, "Bearer")],
            "Unauthorized",
        )
            .into_response()
    }
}

pub async fn build_router(roots: Vec<String>, auth_token: Option<String>) -> Result<axum::Router> {
    let sandbox = Arc::new(Sandbox::new(roots).await.context("Could not add roots")?);
    let ctx = Arc::new(AppContext { sandbox });

    let service = transport::streamable_http_server::StreamableHttpService::new(
        {
            let ctx = ctx.clone();
            move || Ok(FilesystemServer::new(ctx.clone()))
        },
        transport::streamable_http_server::session::local::LocalSessionManager::default().into(),
        Default::default(),
    );

    let mut router = axum::Router::new().nest_service("/mcp", service);
    if let Some(token) = auth_token {
        router = router.layer(axum::middleware::from_fn_with_state(
            Arc::new(AuthToken::new(&token)),
            require_bearer_token,
        ));
    }
    Ok(router)
}

pub fn print_startup_banner(roots: &[String], bind: &SocketAddr, auth_enabled: bool) {
    tracing::info!(
        "----------- MCP SERVER -----------
    App        :  {}
    Version    :  {}
    Roots      :  {:?}
    Transport  :  Streamable-HTTP
    Listening  :  http://{}/mcp
    Auth       :  {}
    ",
        APP_NAME,
        APP_VERSION,
        roots,
        bind,
        if auth_enabled {
            "bearer token required"
        } else {
            "disabled (loopback only)"
        },
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode, header};
    use tempfile::TempDir;
    use tower::ServiceExt;

    #[test]
    fn test_resolve_bind_address_loopback_without_token() {
        let addr = resolve_bind_address("127.0.0.1:8084", false).unwrap();
        assert!(addr.ip().is_loopback());
        assert_eq!(addr.port(), 8084);
    }

    #[test]
    fn test_resolve_bind_address_ipv6_loopback_without_token() {
        let addr = resolve_bind_address("[::1]:8084", false).unwrap();
        assert!(addr.ip().is_loopback());
    }

    #[test]
    fn test_resolve_bind_address_wildcard_requires_token() {
        // The old default of 0.0.0.0 with no authentication must refuse to
        // start.
        let result = resolve_bind_address("0.0.0.0:8084", false);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("non-loopback"));
    }

    #[test]
    fn test_resolve_bind_address_wildcard_with_token() {
        let addr = resolve_bind_address("0.0.0.0:8084", true).unwrap();
        assert_eq!(addr.port(), 8084);
    }

    #[test]
    fn test_resolve_bind_address_external_ip_requires_token() {
        assert!(resolve_bind_address("192.168.1.10:9000", false).is_err());
        assert!(resolve_bind_address("192.168.1.10:9000", true).is_ok());
    }

    #[test]
    fn test_resolve_bind_address_invalid() {
        assert!(resolve_bind_address("not-an-address", true).is_err());
    }

    #[test]
    fn test_auth_token_comparison() {
        let token = AuthToken::new("s3cret");
        assert!(token.is_valid("s3cret"));
        assert!(!token.is_valid("wrong"));
        assert!(!token.is_valid("s3cret-extra"));
        assert!(!token.is_valid(""));
    }

    #[test]
    fn test_auth_token_empty_never_valid() {
        // Security: an empty configured token must not authenticate the
        // empty bearer string.
        let token = AuthToken::new("");
        assert!(!token.is_valid(""));
    }

    async fn router_fixture(auth_token: Option<&str>) -> axum::Router {
        let temp_dir = TempDir::new().unwrap();
        let root = temp_dir.path().to_string_lossy().to_string();
        let router = build_router(vec![root], auth_token.map(str::to_string))
            .await
            .unwrap();
        // Leak the TempDir so the sandbox root outlives the fixture; the OS
        // reclaims it on process exit.
        std::mem::forget(temp_dir);
        router
    }

    fn mcp_request(auth_header: Option<&str>) -> Request<Body> {
        let mut builder = Request::builder().uri("/mcp");
        if let Some(value) = auth_header {
            builder = builder.header(header::AUTHORIZATION, value);
        }
        builder.body(Body::empty()).unwrap()
    }

    #[tokio::test]
    async fn test_router_without_token_allows_unauthenticated() {
        let router = router_fixture(None).await;
        let response = router.oneshot(mcp_request(None)).await.unwrap();
        assert_ne!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn test_router_with_token_rejects_missing_header() {
        let router = router_fixture(Some("s3cret")).await;
        let response = router.oneshot(mcp_request(None)).await.unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn test_router_with_token_rejects_wrong_token() {
        let router = router_fixture(Some("s3cret")).await;
        let response = router
            .oneshot(mcp_request(Some("Bearer wrong")))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn test_router_with_token_accepts_correct_token() {
        let router = router_fixture(Some("s3cret")).await;
        let response = router
            .oneshot(mcp_request(Some("Bearer s3cret")))
            .await
            .unwrap();
        assert_ne!(response.status(), StatusCode::UNAUTHORIZED);
    }
}
