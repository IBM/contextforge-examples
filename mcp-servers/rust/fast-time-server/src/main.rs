// fast-time-server - Ultra-fast MCP server for performance testing
//
// Copyright 2025
// SPDX-License-Identifier: Apache-2.0
//
// This server provides minimal, blazing-fast tools for load testing:
// - echo: Echoes back whatever you send it
// - flaky: Fails N times per key before succeeding (retry testing)
// - get_system_time: Returns current time in specified timezone
// - convert_time: Converts a time between IANA timezones
// - schema_error / schema_success: Output-schema validation fixtures
// - get_stats: Returns server statistics
// - verify-protocol: Reports the MCP protocol version of the current request
//
// Transport: Streamable HTTP (no auth) via the official MCP Rust SDK (rmcp).
// Dual-era by default: legacy 2025-11-25 (initialize handshake + sessions)
// and modern 2026-07-28 (stateless, per-request _meta) are served
// simultaneously on POST/DELETE /mcp.
// Default: http://127.0.0.1:9080/mcp

use axum::Router;
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::serve::ListenerExt;
#[cfg(test)]
use chrono::Offset;
use chrono::{DateTime, FixedOffset, SecondsFormat, TimeZone, Utc};
use chrono_tz::Tz;
use rand_distr::Distribution;
use rand_distr::Normal;
use rmcp::handler::server::router::tool::ToolRouter;
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::{
    CallToolResult, ContentBlock, Implementation, ProtocolVersion, ServerCapabilities, ServerInfo,
};
use rmcp::service::RequestContext;
use rmcp::transport::streamable_http_server::{
    StreamableHttpServerConfig, StreamableHttpService, session::local::LocalSessionManager,
};
use rmcp::{ErrorData as McpError, Json, RoleServer, ServerHandler, schemars};
use rmcp::{tool, tool_handler, tool_router};
use serde_json::json;
use std::borrow::Cow;
use std::collections::HashMap;
use std::env;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tracing::info;
use tracing::trace;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

const DEFAULT_BIND_ADDRESS: &str = "0.0.0.0:9080";
const APP_NAME: &str = "fast-time-server";
const APP_VERSION: &str = env!("CARGO_PKG_VERSION");
const MAX_DELAY_MS: u64 = 60_000;
/// The legacy revision negotiates via the `initialize` handshake and uses
/// `mcp-session-id` sessions; the modern revision declares the version
/// per-request in `_meta` and is served statelessly. Both are served at once.
const MCP_PROTOCOL_VERSION: &str = "2025-11-25";
const MCP_PROTOCOL_VERSION_MODERN: &str = "2026-07-28";
const SUPPORTED_PROTOCOL_VERSIONS: &[ProtocolVersion] =
    &[ProtocolVersion::V_2025_11_25, ProtocolVersion::V_2026_07_28];

// ============================================================================
// Delay Helpers
// ============================================================================

/// Compute the actual delay in ms, optionally sampling from a normal distribution.
/// Returns the mean unchanged when stddev is None, zero, or negative.
fn compute_delay(mean_ms: u64, stddev: Option<f64>) -> u64 {
    match stddev {
        Some(sd) if sd > 0.0 => {
            let dist = Normal::new(mean_ms as f64, sd)
                .unwrap_or_else(|_| Normal::new(mean_ms as f64, 0.0).unwrap());
            let sample = dist.sample(&mut rand::rng());
            sample.round().clamp(0.0, MAX_DELAY_MS as f64) as u64
        }
        _ => mean_ms,
    }
}

fn validate_delay(delay: Option<u64>) -> Result<Option<u64>, &'static str> {
    match delay {
        Some(ms) if ms > MAX_DELAY_MS => Err("delay exceeds the 60000 ms limit"),
        value => Ok(value),
    }
}

// ============================================================================
// Timezone Parsing
// ============================================================================

#[derive(Debug, Clone, Copy)]
enum ParsedTimezone {
    Fixed(FixedOffset),
    Named(Tz),
}

impl ParsedTimezone {
    fn format_utc(self, utc: DateTime<Utc>) -> String {
        match self {
            Self::Fixed(offset) if offset.local_minus_utc() == 0 => {
                utc.to_rfc3339_opts(SecondsFormat::Secs, true)
            }
            Self::Fixed(offset) => utc.with_timezone(&offset).to_rfc3339(),
            Self::Named(tz) => utc.with_timezone(&tz).to_rfc3339(),
        }
    }

    fn local_datetime_to_utc(self, naive: &chrono::NaiveDateTime) -> Option<DateTime<Utc>> {
        match self {
            Self::Fixed(offset) => offset
                .from_local_datetime(naive)
                .single()
                .map(|dt| dt.with_timezone(&Utc)),
            Self::Named(tz) => tz
                .from_local_datetime(naive)
                .single()
                .map(|dt| dt.with_timezone(&Utc)),
        }
    }

    #[cfg(test)]
    fn offset_seconds_at(self, utc: DateTime<Utc>) -> i32 {
        match self {
            Self::Fixed(offset) => offset.local_minus_utc(),
            Self::Named(tz) => utc.with_timezone(&tz).offset().fix().local_minus_utc(),
        }
    }
}

/// Parse an IANA timezone name or fixed UTC offset.
fn parse_timezone(tz: &str) -> Result<ParsedTimezone, String> {
    // Handle UTC explicitly
    if tz.eq_ignore_ascii_case("UTC") || tz.eq_ignore_ascii_case("GMT") {
        return Ok(ParsedTimezone::Fixed(FixedOffset::east_opt(0).unwrap()));
    }

    // Handle fixed offsets like "+05:30" or "-08:00"
    if tz.starts_with('+') || tz.starts_with('-') {
        return parse_offset(tz).map(ParsedTimezone::Fixed);
    }

    tz.parse::<Tz>()
        .map(ParsedTimezone::Named)
        .map_err(|_| format!("Unknown timezone: {}", tz))
}

/// Parse an input time string in the given offset, accepting RFC3339 and a
/// handful of common formats used by the Go fast-time-server port.
fn parse_time_in_timezone(
    time_str: &str,
    timezone: &ParsedTimezone,
) -> Result<DateTime<Utc>, String> {
    if let Ok(parsed) = DateTime::parse_from_rfc3339(time_str) {
        return Ok(parsed.with_timezone(&Utc));
    }
    for fmt in ["%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"] {
        if let Ok(naive) = chrono::NaiveDateTime::parse_from_str(time_str, fmt)
            && let Some(dt) = timezone.local_datetime_to_utc(&naive)
        {
            return Ok(dt);
        }
        if let Ok(date) = chrono::NaiveDate::parse_from_str(time_str, fmt)
            && let Some(naive) = date.and_hms_opt(0, 0, 0)
            && let Some(dt) = timezone.local_datetime_to_utc(&naive)
        {
            return Ok(dt);
        }
    }
    Err(format!("unrecognized time format: {}", time_str))
}

/// Parse an offset string like "+05:30" or "-08:00"
fn parse_offset(s: &str) -> Result<FixedOffset, String> {
    let (sign, rest) = if let Some(stripped) = s.strip_prefix('+') {
        (1, stripped)
    } else if let Some(stripped) = s.strip_prefix('-') {
        (-1, stripped)
    } else {
        return Err("Offset must start with + or -".to_string());
    };

    let parts: Vec<&str> = rest.split(':').collect();
    if parts.len() != 2 {
        return Err("Offset must be in format +HH:MM or -HH:MM".to_string());
    }

    let hours: i32 = parts[0].parse().map_err(|_| "Invalid hours in offset")?;
    let minutes: i32 = parts[1].parse().map_err(|_| "Invalid minutes in offset")?;

    let total_seconds = sign * (hours * 3600 + minutes * 60);

    FixedOffset::east_opt(total_seconds).ok_or_else(|| format!("Offset out of range: {}", s))
}

// ============================================================================
// MCP Server (official rmcp SDK)
// ============================================================================

/// Shared state, Arc-cloned into the single handler the service factory hands
/// to every session and stateless request.
#[derive(Default)]
struct SharedState {
    request_count: AtomicU64,
    /// Per-key attempt counter for the `flaky` test tool. Keyed by the caller-
    /// supplied `key` argument so back-to-back test sequences stay isolated;
    /// the gateway re-sends identical arguments on each retry, so all attempts
    /// of one logical call share a key and increment the same counter.
    flaky: Mutex<HashMap<String, u64>>,
}

struct FastTimeServer {
    state: Arc<SharedState>,
    tool_router: ToolRouter<Self>,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
struct EchoRequest {
    message: String,
    #[schemars(range(min = 0, max = 60000))]
    delay: Option<u64>,
    #[schemars(range(min = 0))]
    delay_stddev: Option<f64>,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
struct FlakyRequest {
    /// Unique key to track attempt count across retries
    key: String,
    /// Number of times to return isError=true before succeeding (default 0)
    #[schemars(range(min = 0))]
    fail_times: Option<u64>,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
struct GetSystemTimeRequest {
    timezone: Option<String>,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
struct ConvertTimeRequest {
    time: String,
    source_timezone: String,
    target_timezone: String,
}

#[derive(Debug, serde::Serialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
struct RecognitionResult {
    recognition_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    message: Option<String>,
}

#[derive(Debug, serde::Serialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
struct VerifyProtocolResult {
    protocol_version: String,
    transport: String,
}

/// Resolve the protocol version active for one request: the per-request
/// `_meta` version wins (modern, stateless era); otherwise fall back to the
/// version the session negotiated at `initialize` (legacy era).
fn protocol_report(
    meta_version: Option<ProtocolVersion>,
    negotiated: Option<ProtocolVersion>,
) -> VerifyProtocolResult {
    if let Some(version) = meta_version {
        return VerifyProtocolResult {
            protocol_version: version.to_string(),
            transport: "stateless".to_string(),
        };
    }
    VerifyProtocolResult {
        protocol_version: negotiated
            .map(|version| version.to_string())
            .unwrap_or_else(|| "unknown".to_string()),
        transport: "session".to_string(),
    }
}

#[tool_router]
impl FastTimeServer {
    fn new() -> Self {
        Self {
            state: Arc::new(SharedState::default()),
            tool_router: Self::tool_router(),
        }
    }

    #[tool(description = "Echo back the provided message.")]
    async fn echo(
        &self,
        Parameters(request): Parameters<EchoRequest>,
    ) -> Result<CallToolResult, McpError> {
        let delay = validate_delay(request.delay)
            .map_err(|message| McpError::invalid_params(message, None))?;

        self.state.request_count.fetch_add(1, Ordering::Relaxed);
        if let Some(ms) = delay
            && ms > 0
        {
            let actual_ms = compute_delay(ms, request.delay_stddev);
            tokio::time::sleep(std::time::Duration::from_millis(actual_ms)).await;
        }
        Ok(CallToolResult::success(vec![ContentBlock::text(
            request.message,
        )]))
    }

    #[tool(
        description = "Return isError=true for the first fail_times calls per key, then succeed (retry testing)."
    )]
    fn flaky(
        &self,
        Parameters(request): Parameters<FlakyRequest>,
    ) -> Result<CallToolResult, McpError> {
        let fail_times = request.fail_times.unwrap_or(0);

        self.state.request_count.fetch_add(1, Ordering::Relaxed);
        let mut state = self.state.flaky.lock().unwrap();
        let attempt = {
            let counter = state.entry(request.key.clone()).or_insert(0);
            *counter += 1;
            *counter
        };
        if attempt <= fail_times {
            Ok(CallToolResult::error(vec![ContentBlock::text(format!(
                "flaky transient failure (attempt {attempt}/{fail_times})"
            ))]))
        } else {
            state.remove(&request.key);
            Ok(CallToolResult::success(vec![ContentBlock::text(format!(
                "flaky recovered after {attempt} attempt(s)"
            ))]))
        }
    }

    #[tool(description = "Get current system time in the specified IANA timezone.")]
    fn get_system_time(
        &self,
        Parameters(request): Parameters<GetSystemTimeRequest>,
    ) -> Result<CallToolResult, McpError> {
        let timezone = request.timezone.as_deref().unwrap_or("UTC");

        self.state.request_count.fetch_add(1, Ordering::Relaxed);
        match parse_timezone(timezone) {
            Ok(timezone) => Ok(CallToolResult::success(vec![ContentBlock::text(
                timezone.format_utc(Utc::now()),
            )])),
            Err(err) => Ok(CallToolResult::error(vec![ContentBlock::text(format!(
                "Invalid timezone '{timezone}': {err}"
            ))])),
        }
    }

    #[tool(
        description = "Convert a time value from a source IANA timezone to a target IANA timezone."
    )]
    fn convert_time(
        &self,
        Parameters(request): Parameters<ConvertTimeRequest>,
    ) -> Result<CallToolResult, McpError> {
        self.state.request_count.fetch_add(1, Ordering::Relaxed);

        let source_timezone = match parse_timezone(&request.source_timezone) {
            Ok(timezone) => timezone,
            Err(err) => {
                return Ok(CallToolResult::error(vec![ContentBlock::text(format!(
                    "invalid source timezone: {err}"
                ))]));
            }
        };
        let target_timezone = match parse_timezone(&request.target_timezone) {
            Ok(timezone) => timezone,
            Err(err) => {
                return Ok(CallToolResult::error(vec![ContentBlock::text(format!(
                    "invalid target timezone: {err}"
                ))]));
            }
        };
        match parse_time_in_timezone(&request.time, &source_timezone) {
            Ok(parsed) => Ok(CallToolResult::success(vec![ContentBlock::text(
                target_timezone.format_utc(parsed),
            )])),
            Err(_) => Ok(CallToolResult::error(vec![ContentBlock::text(format!(
                "invalid time format: {}",
                request.time
            ))])),
        }
    }

    #[tool(
        description = "Always returns isError=true.",
        output_schema = rmcp::handler::server::tool::schema_for_output::<RecognitionResult>()
    )]
    fn schema_error(&self) -> Result<CallToolResult, McpError> {
        self.state.request_count.fetch_add(1, Ordering::Relaxed);
        Ok(CallToolResult::error(vec![ContentBlock::text(
            "You cannot send more than 200 points",
        )]))
    }

    #[tool(description = "Returns a JSON payload that conforms to the declared outputSchema.")]
    fn schema_success(&self) -> Result<Json<RecognitionResult>, McpError> {
        self.state.request_count.fetch_add(1, Ordering::Relaxed);
        Ok(Json(RecognitionResult {
            recognition_id: "rec-123".to_string(),
            message: Some("ok".to_string()),
        }))
    }

    #[tool(description = "Get server statistics including request count and uptime.")]
    fn get_stats(&self) -> Result<CallToolResult, McpError> {
        let count = self.state.request_count.load(Ordering::Relaxed);
        Ok(CallToolResult::success(vec![ContentBlock::text(format!(
            "{{\n  \"server\": \"{}\",\n  \"version\": \"{}\",\n  \"requests_handled\": {}\n}}",
            APP_NAME, APP_VERSION, count
        ))]))
    }

    #[tool(
        name = "verify-protocol",
        description = "Report the MCP protocol version active for the current request."
    )]
    fn verify_protocol(
        &self,
        context: RequestContext<RoleServer>,
    ) -> Result<Json<VerifyProtocolResult>, McpError> {
        let negotiated = context
            .peer
            .peer_info()
            .map(|info| info.protocol_version.clone());
        Ok(Json(protocol_report(
            context.meta.protocol_version(),
            negotiated,
        )))
    }
}

#[tool_handler(router = self.tool_router)]
impl ServerHandler for FastTimeServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(Implementation::new(APP_NAME, APP_VERSION))
            .with_protocol_version(ProtocolVersion::V_2025_11_25)
            .with_instructions("Ultra-fast MCP test server.".to_string())
    }

    fn supported_protocol_versions(&self) -> Cow<'static, [ProtocolVersion]> {
        Cow::Borrowed(SUPPORTED_PROTOCOL_VERSIONS)
    }
}

// ============================================================================
// Main Entry Point
// ============================================================================

fn build_router() -> Router {
    let server = Arc::new(FastTimeServer::new());
    let ct = tokio_util::sync::CancellationToken::new();
    let mcp_service = StreamableHttpService::new(
        move || Ok(server.clone()),
        LocalSessionManager::default().into(),
        StreamableHttpServerConfig::default()
            .with_json_response(true)
            // The pre-SDK server performed no Host validation; keep it open so
            // container and LAN benchmarks are not rejected as DNS rebinding.
            .with_allowed_hosts(Vec::<String>::new())
            .with_cancellation_token(ct.clone()),
    );

    Router::new()
        // Health & version
        .route("/health", axum::routing::get(health_handler))
        .route("/version", axum::routing::get(version_handler))
        // REST API for benchmarking (bypasses MCP session overhead)
        .route("/api/echo", axum::routing::post(rest_echo_handler))
        .route("/api/time", axum::routing::get(rest_time_handler))
        // MCP protocol endpoint (POST + DELETE; GET opens an SSE stream)
        .nest_service("/mcp", mcp_service)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize logging
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".to_string().into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    // Get bind address from environment or use default
    let bind_address =
        env::var("BIND_ADDRESS").unwrap_or_else(|_| DEFAULT_BIND_ADDRESS.to_string());

    info!("{} v{} starting...", APP_NAME, APP_VERSION);
    info!("Binding to: {}", bind_address);
    info!(
        "MCP protocol versions: {}, {}",
        MCP_PROTOCOL_VERSION, MCP_PROTOCOL_VERSION_MODERN
    );

    let router = build_router();

    // Bind and serve
    let tcp_listener = tokio::net::TcpListener::bind(&bind_address)
        .await?
        .tap_io(|tcp_stream| {
            if let Err(err) = tcp_stream.set_nodelay(true) {
                trace!("failed to set TCP_NODELAY on incoming connection: {err:#}");
            }
        });

    info!("MCP endpoint:   http://{}/mcp", bind_address);
    info!(
        "REST API:       http://{}/api/echo (POST), /api/time (GET)",
        bind_address
    );
    info!("Health check:   http://{}/health", bind_address);
    info!("Version info:   http://{}/version", bind_address);
    info!("");
    info!("Benchmark with:");
    info!("  hey -n 1000000 -c 200 -m POST -T 'application/json' \\");
    info!(
        "      -d '{{\"message\":\"hello\"}}' http://{}/api/echo",
        bind_address
    );

    axum::serve(tcp_listener, router)
        .with_graceful_shutdown(async move {
            tokio::signal::ctrl_c().await.unwrap();
            info!("Shutting down...");
        })
        .await?;

    Ok(())
}

// Health check handler
async fn health_handler() -> axum::Json<serde_json::Value> {
    axum::Json(json!({
        "status": "healthy",
        "server": APP_NAME,
        "version": APP_VERSION
    }))
}

// Version handler
async fn version_handler() -> axum::Json<serde_json::Value> {
    axum::Json(json!({
        "name": APP_NAME,
        "version": APP_VERSION,
        "mcp_version": MCP_PROTOCOL_VERSION,
        "mcp_versions": [MCP_PROTOCOL_VERSION, MCP_PROTOCOL_VERSION_MODERN]
    }))
}

// ============================================================================
// REST API Handlers (for benchmarking - bypasses MCP session overhead)
// ============================================================================

#[derive(Debug, serde::Deserialize)]
struct RestEchoRequest {
    message: String,
    #[serde(default)]
    delay: Option<u64>,
    #[serde(default)]
    delay_stddev: Option<f64>,
}

#[derive(Debug, serde::Deserialize)]
struct RestTimeQuery {
    #[serde(default)]
    tz: Option<String>,
}

// POST /api/echo - Simple echo for benchmarking
async fn rest_echo_handler(axum::Json(req): axum::Json<RestEchoRequest>) -> Response {
    let delay = match validate_delay(req.delay) {
        Ok(delay) => delay,
        Err(message) => {
            return (
                StatusCode::BAD_REQUEST,
                [(header::CONTENT_TYPE, "application/json")],
                serde_json::to_string(&json!({ "error": message })).unwrap_or_default(),
            )
                .into_response();
        }
    };
    if let Some(ms) = delay
        && ms > 0
    {
        let actual_ms = compute_delay(ms, req.delay_stddev);
        tokio::time::sleep(std::time::Duration::from_millis(actual_ms)).await;
    }
    axum::Json(json!({ "message": req.message })).into_response()
}

// GET /api/time?tz=America/New_York - Get time for benchmarking
async fn rest_time_handler(
    axum::extract::Query(query): axum::extract::Query<RestTimeQuery>,
) -> axum::Json<serde_json::Value> {
    let tz_name = query.tz.as_deref().unwrap_or("UTC");
    let now_utc = Utc::now();

    match parse_timezone(tz_name) {
        Ok(timezone) => axum::Json(json!({
            "time": timezone.format_utc(now_utc),
            "timezone": tz_name
        })),
        Err(e) => axum::Json(json!({
            "error": format!("Invalid timezone '{}': {}", tz_name, e)
        })),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body;
    use axum::http::{HeaderValue, Request, StatusCode};
    use tower::ServiceExt;

    const MCP_ACCEPT: &str = "application/json, text/event-stream";
    const SESSION_HEADER: &str = "mcp-session-id";
    const PROTOCOL_VERSION_HEADER: &str = "mcp-protocol-version";
    const PROTOCOL_VERSION_META_KEY: &str = "io.modelcontextprotocol/protocolVersion";
    const CLIENT_CAPABILITIES_META_KEY: &str = "io.modelcontextprotocol/clientCapabilities";

    #[test]
    fn test_parse_utc() {
        let timezone = parse_timezone("UTC").unwrap();
        let utc = DateTime::parse_from_rfc3339("2025-06-21T16:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        assert_eq!(timezone.offset_seconds_at(utc), 0);
    }

    #[test]
    fn test_parse_gmt() {
        let timezone = parse_timezone("GMT").unwrap();
        let utc = DateTime::parse_from_rfc3339("2025-06-21T16:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        assert_eq!(timezone.offset_seconds_at(utc), 0);
    }

    #[test]
    fn test_parse_dublin() {
        let timezone = parse_timezone("Europe/Dublin").unwrap();
        let utc = DateTime::parse_from_rfc3339("2025-01-21T16:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        assert_eq!(timezone.offset_seconds_at(utc), 0);
    }

    #[test]
    fn test_parse_new_york() {
        let timezone = parse_timezone("America/New_York").unwrap();
        let summer = DateTime::parse_from_rfc3339("2025-06-21T16:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let winter = DateTime::parse_from_rfc3339("2025-01-21T16:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        assert_eq!(timezone.offset_seconds_at(summer), -4 * 3600);
        assert_eq!(timezone.offset_seconds_at(winter), -5 * 3600);
    }

    #[test]
    fn test_parse_tokyo() {
        let timezone = parse_timezone("Asia/Tokyo").unwrap();
        let utc = DateTime::parse_from_rfc3339("2025-06-21T16:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        assert_eq!(timezone.offset_seconds_at(utc), 9 * 3600);
    }

    #[test]
    fn test_parse_fixed_offset_positive() {
        let offset = parse_offset("+05:30").unwrap();
        assert_eq!(offset.local_minus_utc(), 5 * 3600 + 30 * 60);
    }

    #[test]
    fn test_parse_fixed_offset_negative() {
        let offset = parse_offset("-08:00").unwrap();
        assert_eq!(offset.local_minus_utc(), -8 * 3600);
    }

    #[test]
    fn test_unknown_timezone() {
        let result = parse_timezone("Invalid/Timezone");
        assert!(result.is_err());
    }

    #[test]
    fn test_delay_validation_rejects_values_above_limit() {
        assert_eq!(validate_delay(Some(MAX_DELAY_MS)), Ok(Some(MAX_DELAY_MS)));
        assert!(validate_delay(Some(MAX_DELAY_MS + 1)).is_err());
    }

    #[test]
    fn test_supported_protocol_versions_advertises_exactly_two_eras() {
        let server = FastTimeServer::new();
        assert_eq!(
            server.supported_protocol_versions().as_ref(),
            [ProtocolVersion::V_2025_11_25, ProtocolVersion::V_2026_07_28]
        );
    }

    #[test]
    fn test_protocol_report_modern_meta_wins() {
        let report = protocol_report(Some(ProtocolVersion::V_2026_07_28), None);
        assert_eq!(report.protocol_version, "2026-07-28");
        assert_eq!(report.transport, "stateless");
    }

    #[test]
    fn test_protocol_report_legacy_falls_back_to_negotiated() {
        let report = protocol_report(None, Some(ProtocolVersion::V_2025_11_25));
        assert_eq!(report.protocol_version, "2025-11-25");
        assert_eq!(report.transport, "session");
    }

    #[test]
    fn test_protocol_report_without_any_version_is_unknown() {
        let report = protocol_report(None, None);
        assert_eq!(report.protocol_version, "unknown");
        assert_eq!(report.transport, "session");
    }

    // ========================================================================
    // HTTP integration helpers (tower oneshot against the real router)
    // ========================================================================

    fn mcp_post(body: serde_json::Value) -> Request<axum::body::Body> {
        Request::builder()
            .method("POST")
            .uri("http://localhost/mcp")
            .header(header::CONTENT_TYPE, "application/json")
            .header(header::ACCEPT, MCP_ACCEPT)
            .body(axum::body::Body::from(body.to_string()))
            .expect("request should build")
    }

    async fn response_text(response: Response) -> String {
        let bytes = body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("response body should be readable");
        String::from_utf8(bytes.to_vec()).expect("response body should be utf-8")
    }

    /// Legacy session requests are answered as SSE streams; the JSON-RPC
    /// message rides in the first non-empty `data:` line.
    fn parse_sse_json(text: &str) -> serde_json::Value {
        for line in text.lines() {
            if let Some(data) = line.strip_prefix("data:") {
                let data = data.trim();
                if !data.is_empty() {
                    return serde_json::from_str(data).expect("SSE data should be JSON");
                }
            }
        }
        panic!("no SSE data line in response body: {text:?}");
    }

    async fn oneshot(router: &Router, request: Request<axum::body::Body>) -> Response {
        tokio::time::timeout(
            std::time::Duration::from_secs(10),
            router.clone().oneshot(request),
        )
        .await
        .expect("request timed out")
        .expect("router should be infallible")
    }

    fn initialize_request(protocol_version: &str) -> serde_json::Value {
        json!({
            "jsonrpc": "2.0",
            "method": "initialize",
            "params": {
                "protocolVersion": protocol_version,
                "capabilities": {},
                "clientInfo": { "name": "test", "version": "1.0" }
            },
            "id": 1
        })
    }

    /// Run the legacy handshake and return the issued session id.
    async fn initialize_session(router: &Router) -> String {
        let response = oneshot(router, mcp_post(initialize_request(MCP_PROTOCOL_VERSION))).await;
        assert_eq!(response.status(), StatusCode::OK);
        let session_id = response
            .headers()
            .get(SESSION_HEADER)
            .expect("initialize should issue a session id")
            .to_str()
            .expect("session id should be ascii")
            .to_string();
        assert!(!session_id.is_empty());
        let body = parse_sse_json(&response_text(response).await);
        assert_eq!(body["result"]["protocolVersion"], MCP_PROTOCOL_VERSION);

        let mut initialized = mcp_post(json!({
            "jsonrpc": "2.0",
            "method": "notifications/initialized"
        }));
        initialized
            .headers_mut()
            .insert(SESSION_HEADER, HeaderValue::from_str(&session_id).unwrap());
        let response = oneshot(router, initialized).await;
        assert_eq!(response.status(), StatusCode::ACCEPTED);

        session_id
    }

    async fn legacy_tool_call(
        router: &Router,
        session_id: &str,
        name: &str,
        arguments: serde_json::Value,
        id: i64,
    ) -> serde_json::Value {
        let mut request = mcp_post(json!({
            "jsonrpc": "2.0",
            "method": "tools/call",
            "params": { "name": name, "arguments": arguments },
            "id": id
        }));
        request
            .headers_mut()
            .insert(SESSION_HEADER, HeaderValue::from_str(session_id).unwrap());
        let response = oneshot(router, request).await;
        assert_eq!(response.status(), StatusCode::OK);
        parse_sse_json(&response_text(response).await)
    }

    fn modern_request(method: &str, version: &str, id: i64) -> serde_json::Value {
        json!({
            "jsonrpc": "2.0",
            "method": method,
            "params": {
                "_meta": {
                    PROTOCOL_VERSION_META_KEY: version,
                    CLIENT_CAPABILITIES_META_KEY: {}
                }
            },
            "id": id
        })
    }

    /// The MCP-Protocol-Version header must mirror the version in `_meta`,
    /// and 2026-07-28 requests must carry SEP-2243 headers: `Mcp-Method`
    /// matching the body method, plus `Mcp-Name` for named methods.
    async fn modern_call(
        router: &Router,
        body: serde_json::Value,
    ) -> (StatusCode, serde_json::Value) {
        let version = body["params"]["_meta"][PROTOCOL_VERSION_META_KEY]
            .as_str()
            .expect("modern request should carry a version")
            .to_string();
        let method = body["method"].as_str().expect("request method").to_string();
        let name = body["params"]["name"].as_str().map(str::to_string);
        let mut request = mcp_post(body);
        let headers = request.headers_mut();
        headers.insert(
            PROTOCOL_VERSION_HEADER,
            HeaderValue::from_str(&version).unwrap(),
        );
        headers.insert("mcp-method", HeaderValue::from_str(&method).unwrap());
        if let Some(name) = name {
            headers.insert("mcp-name", HeaderValue::from_str(&name).unwrap());
        }
        let response = oneshot(router, request).await;
        let status = response.status();
        let text = response_text(response).await;
        let body = serde_json::from_str(&text).expect("modern responses should be JSON");
        (status, body)
    }

    async fn modern_tool_call(
        router: &Router,
        name: &str,
        arguments: serde_json::Value,
        id: i64,
    ) -> (StatusCode, serde_json::Value) {
        let mut request = modern_request("tools/call", MCP_PROTOCOL_VERSION_MODERN, id);
        request["params"]["name"] = json!(name);
        request["params"]["arguments"] = arguments;
        modern_call(router, request).await
    }

    // ========================================================================
    // Legacy era (2025-11-25): initialize handshake + mcp-session-id sessions
    // ========================================================================

    #[tokio::test]
    async fn test_initialize_issues_session_and_echoes_legacy_version() {
        let response = oneshot(
            &build_router(),
            mcp_post(initialize_request(MCP_PROTOCOL_VERSION)),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        assert!(response.headers().contains_key(SESSION_HEADER));
        let body = parse_sse_json(&response_text(response).await);
        let result = &body["result"];
        assert_eq!(result["protocolVersion"], MCP_PROTOCOL_VERSION);
        assert_eq!(result["serverInfo"]["name"], APP_NAME);
        assert!(result["capabilities"]["tools"].is_object());
    }

    #[tokio::test]
    async fn test_initialize_falls_back_to_legacy_for_unknown_version() {
        let response = oneshot(&build_router(), mcp_post(initialize_request("1999-01-01"))).await;
        assert_eq!(response.status(), StatusCode::OK);
        let body = parse_sse_json(&response_text(response).await);
        assert_eq!(body["result"]["protocolVersion"], MCP_PROTOCOL_VERSION);
    }

    #[tokio::test]
    async fn test_legacy_session_lifecycle() {
        let router = build_router();
        let session_id = initialize_session(&router).await;

        let mut list = mcp_post(json!({
            "jsonrpc": "2.0",
            "method": "tools/list",
            "id": 2
        }));
        list.headers_mut()
            .insert(SESSION_HEADER, HeaderValue::from_str(&session_id).unwrap());
        let response = oneshot(&router, list).await;
        assert_eq!(response.status(), StatusCode::OK);
        let body = parse_sse_json(&response_text(response).await);
        assert_eq!(body["result"]["tools"].as_array().map(Vec::len), Some(8));

        let response = oneshot(
            &router,
            mcp_post(json!({
                "jsonrpc": "2.0",
                "method": "tools/list",
                "id": 3
            })),
        )
        .await;
        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);

        let mut fake = mcp_post(json!({
            "jsonrpc": "2.0",
            "method": "tools/list",
            "id": 4
        }));
        fake.headers_mut()
            .insert(SESSION_HEADER, HeaderValue::from_static("fake-session"));
        let response = oneshot(&router, fake).await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);

        let delete = Request::builder()
            .method("DELETE")
            .uri("http://localhost/mcp")
            .header(SESSION_HEADER, HeaderValue::from_str(&session_id).unwrap())
            .body(axum::body::Body::empty())
            .expect("request should build");
        let response = oneshot(&router, delete).await;
        assert_eq!(response.status(), StatusCode::ACCEPTED);

        let mut gone = mcp_post(json!({
            "jsonrpc": "2.0",
            "method": "tools/list",
            "id": 5
        }));
        gone.headers_mut()
            .insert(SESSION_HEADER, HeaderValue::from_str(&session_id).unwrap());
        let response = oneshot(&router, gone).await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_legacy_verify_protocol_reports_session() {
        let router = build_router();
        let session_id = initialize_session(&router).await;
        let body = legacy_tool_call(&router, &session_id, "verify-protocol", json!({}), 10).await;
        let result = &body["result"];
        assert_eq!(result["isError"], false);
        assert_eq!(
            result["structuredContent"],
            json!({
                "protocolVersion": MCP_PROTOCOL_VERSION,
                "transport": "session"
            })
        );
        let text: serde_json::Value =
            serde_json::from_str(result["content"][0]["text"].as_str().expect("text content"))
                .expect("text content should mirror the structured payload");
        assert_eq!(text["protocolVersion"], MCP_PROTOCOL_VERSION);
        assert_eq!(text["transport"], "session");
    }

    #[tokio::test]
    async fn test_flaky_fails_then_succeeds() {
        let router = build_router();
        let session_id = initialize_session(&router).await;
        let key = "test-flaky-sdk";
        for attempt in 1..=2i64 {
            let body = legacy_tool_call(
                &router,
                &session_id,
                "flaky",
                json!({ "key": key, "fail_times": 2 }),
                100 + attempt,
            )
            .await;
            assert_eq!(
                body["result"]["isError"], true,
                "attempt {attempt} should be isError"
            );
        }
        let body = legacy_tool_call(
            &router,
            &session_id,
            "flaky",
            json!({ "key": key, "fail_times": 2 }),
            103,
        )
        .await;
        assert_eq!(body["result"]["isError"], false);
        assert!(
            body["result"]["content"][0]["text"]
                .as_str()
                .unwrap()
                .contains("flaky recovered after 3 attempt(s)"),
        );
    }

    #[tokio::test]
    async fn test_convert_time_matches_go_fast_time_dst_behavior() {
        let router = build_router();
        let session_id = initialize_session(&router).await;
        let body = legacy_tool_call(
            &router,
            &session_id,
            "convert_time",
            json!({
                "time": "2025-06-21T16:00:00Z",
                "source_timezone": "UTC",
                "target_timezone": "America/New_York"
            }),
            11,
        )
        .await;
        assert_eq!(
            body["result"]["content"][0]["text"],
            "2025-06-21T12:00:00-04:00"
        );
    }

    #[tokio::test]
    async fn test_convert_time_matches_go_fast_time_half_hour_zones() {
        let router = build_router();
        let session_id = initialize_session(&router).await;
        let body = legacy_tool_call(
            &router,
            &session_id,
            "convert_time",
            json!({
                "time": "2025-01-10 10:00:00",
                "source_timezone": "Asia/Kolkata",
                "target_timezone": "UTC"
            }),
            12,
        )
        .await;
        assert_eq!(body["result"]["content"][0]["text"], "2025-01-10T04:30:00Z");
    }

    #[tokio::test]
    async fn test_legacy_time_stats_and_schema_tools() {
        let router = build_router();
        let session_id = initialize_session(&router).await;

        let body = legacy_tool_call(&router, &session_id, "get_system_time", json!({}), 20).await;
        let text = body["result"]["content"][0]["text"].as_str().unwrap();
        assert!(text.ends_with('Z'), "UTC default should end with Z: {text}");

        let body = legacy_tool_call(
            &router,
            &session_id,
            "get_system_time",
            json!({"timezone": "Mars/Olympus"}),
            21,
        )
        .await;
        assert_eq!(body["result"]["isError"], true);
        assert!(
            body["result"]["content"][0]["text"]
                .as_str()
                .unwrap()
                .starts_with("Invalid timezone 'Mars/Olympus'")
        );

        let body = legacy_tool_call(&router, &session_id, "schema_success", json!({}), 22).await;
        assert_eq!(body["result"]["isError"], false);
        let expected = json!({ "recognitionId": "rec-123", "message": "ok" });
        assert_eq!(body["result"]["structuredContent"], expected);
        let text: serde_json::Value =
            serde_json::from_str(body["result"]["content"][0]["text"].as_str().unwrap())
                .expect("text content should mirror the structured payload");
        assert_eq!(text, expected);

        let body = legacy_tool_call(&router, &session_id, "schema_error", json!({}), 23).await;
        assert_eq!(body["result"]["isError"], true);
        assert_eq!(
            body["result"]["content"][0]["text"],
            "You cannot send more than 200 points"
        );

        let body = legacy_tool_call(&router, &session_id, "get_stats", json!({}), 24).await;
        let text = body["result"]["content"][0]["text"].as_str().unwrap();
        assert!(text.contains(r#""server": "fast-time-server""#));
        assert!(text.contains(r#""requests_handled": "#));
    }

    // ========================================================================
    // Modern era (2026-07-28): stateless, version in params._meta + header
    // ========================================================================

    #[tokio::test]
    async fn test_modern_verify_protocol_reports_stateless() {
        let (status, body) =
            modern_tool_call(&build_router(), "verify-protocol", json!({}), 1).await;
        assert_eq!(status, StatusCode::OK);
        let result = &body["result"];
        assert_eq!(result["resultType"], "complete");
        assert_eq!(result["isError"], false);
        assert_eq!(
            result["structuredContent"],
            json!({
                "protocolVersion": MCP_PROTOCOL_VERSION_MODERN,
                "transport": "stateless"
            })
        );
        let text: serde_json::Value =
            serde_json::from_str(result["content"][0]["text"].as_str().expect("text content"))
                .expect("text content should mirror the structured payload");
        assert_eq!(text["protocolVersion"], MCP_PROTOCOL_VERSION_MODERN);
        assert_eq!(text["transport"], "stateless");
    }

    #[tokio::test]
    async fn test_modern_tools_call_needs_no_session() {
        let (status, body) =
            modern_tool_call(&build_router(), "echo", json!({ "message": "hi" }), 2).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["result"]["content"][0]["text"], "hi");
        assert_eq!(body["result"]["isError"], false);
    }

    #[tokio::test]
    async fn test_modern_tools_list_schemas() {
        let (status, body) = modern_call(
            &build_router(),
            modern_request("tools/list", MCP_PROTOCOL_VERSION_MODERN, 3),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let tools = body["result"]["tools"].as_array().unwrap();
        assert_eq!(tools.len(), 8);

        let echo = tools.iter().find(|tool| tool["name"] == "echo").unwrap();
        assert_eq!(echo["description"], "Echo back the provided message.");
        assert_eq!(echo["inputSchema"]["type"], "object");
        assert_eq!(
            echo["inputSchema"]["properties"]["message"]["type"],
            "string"
        );
        assert!(
            echo["inputSchema"]["required"]
                .as_array()
                .unwrap()
                .contains(&json!("message"))
        );

        for name in ["schema_error", "schema_success"] {
            let tool = tools.iter().find(|tool| tool["name"] == name).unwrap();
            assert_eq!(
                tool["outputSchema"]["properties"]["recognitionId"]["type"], "string",
                "{name} should keep its outputSchema"
            );
            assert!(
                tool["outputSchema"]["required"]
                    .as_array()
                    .unwrap()
                    .contains(&json!("recognitionId"))
            );
        }

        let verify = tools
            .iter()
            .find(|tool| tool["name"] == "verify-protocol")
            .expect("verify-protocol should be listed");
        assert_eq!(
            verify["outputSchema"]["properties"]["protocolVersion"]["type"],
            "string"
        );
        assert_eq!(
            verify["outputSchema"]["properties"]["transport"]["type"],
            "string"
        );
    }

    #[tokio::test]
    async fn test_server_discover_lists_both_eras() {
        let (status, body) = modern_call(
            &build_router(),
            modern_request("server/discover", MCP_PROTOCOL_VERSION_MODERN, 4),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let result = &body["result"];
        assert_eq!(result["resultType"], "complete");
        assert_eq!(
            result["supportedVersions"],
            json!([MCP_PROTOCOL_VERSION, MCP_PROTOCOL_VERSION_MODERN])
        );
        assert!(result["capabilities"]["tools"].is_object());
        assert_eq!(result["cacheScope"], "private");
        assert_eq!(result["ttlMs"], 0);
        assert_eq!(
            result["_meta"]["io.modelcontextprotocol/serverInfo"]["name"],
            APP_NAME
        );
    }

    #[tokio::test]
    async fn test_modern_unsupported_version_rejected() {
        let (status, body) = modern_call(
            &build_router(),
            modern_request("tools/list", "2025-06-18", 5),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"]["code"], -32022);
        assert_eq!(body["error"]["message"], "Unsupported protocol version");
        assert_eq!(
            body["error"]["data"]["supported"],
            json!([MCP_PROTOCOL_VERSION, MCP_PROTOCOL_VERSION_MODERN])
        );
        assert_eq!(body["error"]["data"]["requested"], "2025-06-18");
    }

    #[tokio::test]
    async fn test_modern_header_mismatch_rejected() {
        let mut request = mcp_post(modern_request("tools/list", MCP_PROTOCOL_VERSION_MODERN, 6));
        request.headers_mut().insert(
            PROTOCOL_VERSION_HEADER,
            HeaderValue::from_static("2025-06-18"),
        );
        let response = oneshot(&build_router(), request).await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body: serde_json::Value = serde_json::from_str(&response_text(response).await).unwrap();
        assert_eq!(body["error"]["code"], -32020);
    }

    #[tokio::test]
    async fn test_modern_missing_client_capabilities_rejected() {
        let mut request = modern_request("tools/list", MCP_PROTOCOL_VERSION_MODERN, 7);
        request["params"]["_meta"]
            .as_object_mut()
            .unwrap()
            .remove(CLIENT_CAPABILITIES_META_KEY);
        let (status, body) = modern_call(&build_router(), request).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"]["code"], -32602);
    }

    #[tokio::test]
    async fn test_mcp_echo_rejects_delay_above_limit() {
        let (status, body) = modern_tool_call(
            &build_router(),
            "echo",
            json!({ "message": "hello", "delay": MAX_DELAY_MS + 1 }),
            8,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"]["code"], -32602);
        assert_eq!(body["error"]["message"], "delay exceeds the 60000 ms limit");
    }

    // ========================================================================
    // REST endpoints survive alongside the SDK service
    // ========================================================================

    #[tokio::test]
    async fn test_rest_and_meta_endpoints() {
        let router = build_router();
        let response = oneshot(
            &router,
            Request::builder()
                .method("GET")
                .uri("http://localhost/health")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let body: serde_json::Value = serde_json::from_str(&response_text(response).await).unwrap();
        assert_eq!(body["status"], "healthy");

        let response = oneshot(
            &router,
            Request::builder()
                .method("GET")
                .uri("http://localhost/version")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await;
        let body: serde_json::Value = serde_json::from_str(&response_text(response).await).unwrap();
        assert_eq!(
            body["mcp_versions"],
            json!([MCP_PROTOCOL_VERSION, MCP_PROTOCOL_VERSION_MODERN])
        );
        assert!(body.get("strict").is_none());

        let response = oneshot(
            &router,
            Request::builder()
                .method("POST")
                .uri("http://localhost/api/echo")
                .header(header::CONTENT_TYPE, "application/json")
                .body(axum::body::Body::from(r#"{"message":"hello"}"#))
                .unwrap(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let body: serde_json::Value = serde_json::from_str(&response_text(response).await).unwrap();
        assert_eq!(body["message"], "hello");

        let response = oneshot(
            &router,
            Request::builder()
                .method("GET")
                .uri("http://localhost/api/time?tz=America/New_York")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let body: serde_json::Value = serde_json::from_str(&response_text(response).await).unwrap();
        assert_eq!(body["timezone"], "America/New_York");
        assert!(
            body["time"].as_str().unwrap().ends_with("-04:00")
                || body["time"].as_str().unwrap().ends_with("-05:00")
        );
    }
}
