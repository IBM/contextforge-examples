// fast-time-server - Ultra-fast MCP server for performance testing
//
// Copyright 2025
// SPDX-License-Identifier: Apache-2.0
//
// This server provides minimal, blazing-fast tools for load testing:
// - echo: Echoes back whatever you send it
// - get_system_time: Returns current time in specified timezone
//
// Transport: Streamable HTTP (no auth)
// Default: http://127.0.0.1:9080/mcp

use axum::Router;
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::serve::ListenerExt;
#[cfg(test)]
use chrono::Offset;
use chrono::{DateTime, FixedOffset, SecondsFormat, TimeZone, Utc};
use chrono_tz::Tz;
use rand_distr::Distribution;
use rand_distr::Normal;
use serde_json::json;
use std::collections::{HashMap, HashSet};
use std::env;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex, RwLock};
use tracing::info;
use tracing::trace;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
use uuid::Uuid;

const DEFAULT_BIND_ADDRESS: &str = "0.0.0.0:9080";
const APP_NAME: &str = "fast-time-server";
const APP_VERSION: &str = env!("CARGO_PKG_VERSION");
// Legacy revisions (2025-11-25 and earlier) negotiate via the `initialize`
// handshake; modern revisions (2026-07-28 and later) declare the version
// per-request in `_meta`. The legacy revision is always served; the modern
// revision is enabled with `--protocol`.
const MCP_PROTOCOL_VERSION: &str = "2025-11-25";
const MCP_PROTOCOL_VERSION_MODERN: &str = "2026-07-28";
const SESSION_HEADER: &str = "mcp-session-id";
const PROTOCOL_VERSION_HEADER: &str = "mcp-protocol-version";
const PROTOCOL_VERSION_META_KEY: &str = "io.modelcontextprotocol/protocolVersion";
const SERVER_INFO_META_KEY: &str = "io.modelcontextprotocol/serverInfo";
const ERR_UNSUPPORTED_PROTOCOL_VERSION: i32 = -32022;
const ERR_HEADER_MISMATCH: i32 = -32020;
const MAX_ACTIVE_SESSIONS: usize = 10_000;
const MAX_DELAY_MS: u64 = 60_000;
static DIRECT_REQUEST_COUNT: AtomicU64 = AtomicU64::new(0);
static ACTIVE_SESSIONS: LazyLock<RwLock<HashSet<String>>> =
    LazyLock::new(|| RwLock::new(HashSet::new()));

/// Per-key attempt counter for the `flaky` test tool.  Keyed by the caller-
/// supplied `key` argument so back-to-back test sequences stay isolated; the
/// gateway re-sends identical arguments on each retry, so all attempts of one
/// logical call share a key and increment the same counter.
static FLAKY_STATE: LazyLock<Mutex<HashMap<String, u64>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

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
// Server Configuration & CLI
// ============================================================================

#[derive(Debug, Clone, PartialEq, Eq)]
struct ServerConfig {
    modern: bool,
    legacy: bool,
    strict: bool,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            modern: false,
            legacy: true,
            strict: false,
        }
    }
}

impl ServerConfig {
    fn supported_versions(&self) -> Vec<&'static str> {
        let mut versions = Vec::new();
        if self.modern {
            versions.push(MCP_PROTOCOL_VERSION_MODERN);
        }
        if self.legacy {
            versions.push(MCP_PROTOCOL_VERSION);
        }
        versions
    }
}

fn usage() -> String {
    format!(
        "Usage: {APP_NAME} [--protocol <VERSION>]... [--strict]\n\
         \n\
         Options:\n\
         \x20 --protocol <VERSION>  Also serve the given MCP protocol revision.\n\
         \x20                        Supported: {MCP_PROTOCOL_VERSION}, {MCP_PROTOCOL_VERSION_MODERN}.\n\
         \x20                        May be repeated. Default: {MCP_PROTOCOL_VERSION} only.\n\
         \x20 --strict             Serve exactly the revisions named with --protocol\n\
         \x20                        (default {MCP_PROTOCOL_VERSION}) and reject any\n\
         \x20                        non-conformant interaction — no fallback, no\n\
         \x20                        `initialize` handshake unless {MCP_PROTOCOL_VERSION}\n\
         \x20                        was explicitly enabled."
    )
}

fn parse_args(args: &[String]) -> Result<ServerConfig, String> {
    let mut modern = false;
    let mut legacy_explicit = false;
    let mut strict = false;
    let mut args = args.iter();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--strict" => strict = true,
            "--protocol" => {
                let version = args
                    .next()
                    .ok_or_else(|| "--protocol requires a version argument".to_string())?;
                match version.as_str() {
                    MCP_PROTOCOL_VERSION => legacy_explicit = true,
                    MCP_PROTOCOL_VERSION_MODERN => modern = true,
                    other => {
                        return Err(format!(
                            "unsupported protocol version '{other}' \
                             (supported: {MCP_PROTOCOL_VERSION}, {MCP_PROTOCOL_VERSION_MODERN})"
                        ));
                    }
                }
            }
            other => return Err(format!("unknown argument '{other}'")),
        }
    }
    // Non-strict servers always keep the legacy revision available so older
    // clients can fall back to it; strict servers serve exactly the revisions
    // named with --protocol and reject everything else.
    Ok(ServerConfig {
        modern,
        legacy: if strict { legacy_explicit } else { true },
        strict,
    })
}

// ============================================================================
// Main Entry Point
// ============================================================================

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

    // Parse CLI arguments
    let raw_args: Vec<String> = env::args().skip(1).collect();
    if raw_args.iter().any(|arg| arg == "-h" || arg == "--help") {
        println!("{}", usage());
        return Ok(());
    }
    let config = match parse_args(&raw_args) {
        Ok(config) => Arc::new(config),
        Err(err) => {
            eprintln!("error: {err}\n\n{}", usage());
            std::process::exit(2);
        }
    };

    // Get bind address from environment or use default
    let bind_address =
        env::var("BIND_ADDRESS").unwrap_or_else(|_| DEFAULT_BIND_ADDRESS.to_string());

    info!("{} v{} starting...", APP_NAME, APP_VERSION);
    info!("Binding to: {}", bind_address);
    info!(
        "MCP protocol versions: {}",
        config.supported_versions().join(", ")
    );
    if config.strict {
        info!("Strict negotiation: unsupported versions rejected (no fallback)");
    }

    // Build router with health check endpoint and REST API for benchmarking
    let router = Router::new()
        // Health & version
        .route("/health", axum::routing::get(health_handler))
        .route("/version", axum::routing::get(version_handler))
        // REST API for benchmarking (bypasses MCP session overhead)
        .route("/api/echo", axum::routing::post(rest_echo_handler))
        .route("/api/time", axum::routing::get(rest_time_handler))
        // MCP protocol endpoint
        .route(
            "/mcp",
            axum::routing::post(mcp_handler).delete(mcp_delete_handler),
        )
        .with_state(config);

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
async fn version_handler(
    axum::extract::State(config): axum::extract::State<Arc<ServerConfig>>,
) -> axum::Json<serde_json::Value> {
    axum::Json(json!({
        "name": APP_NAME,
        "version": APP_VERSION,
        "mcp_version": MCP_PROTOCOL_VERSION,
        "mcp_versions": config.supported_versions(),
        "strict": config.strict
    }))
}

// ============================================================================
// Fast Streamable HTTP MCP Handler
// ============================================================================

async fn mcp_delete_handler(headers: HeaderMap) -> StatusCode {
    let Some(session_id) = mcp_session_id(&headers) else {
        return StatusCode::BAD_REQUEST;
    };
    if remove_session(session_id) {
        StatusCode::OK
    } else {
        StatusCode::NOT_FOUND
    }
}

async fn mcp_handler(
    axum::extract::State(config): axum::extract::State<Arc<ServerConfig>>,
    headers: HeaderMap,
    axum::Json(req): axum::Json<serde_json::Value>,
) -> Response {
    let method = req
        .get("method")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    let id = req.get("id");

    if method != "initialize" {
        // A request carrying a per-request protocol version in `_meta` speaks
        // the modern era and is served statelessly, without a session.
        if let Some(requested) = modern_protocol_version(&req) {
            return mcp_modern_dispatch(&config, &headers, id, method, requested, &req).await;
        }
        let Err(status) = mcp_validate_active_session(&headers) else {
            if id.is_none() {
                return StatusCode::ACCEPTED.into_response();
            }
            return match method {
                "ping" => mcp_empty_result_response(id, false),
                "tools/list" => mcp_tools_list_response(id, false),
                "tools/call" => mcp_tools_call_response(id, &req, false).await,
                _ => mcp_error_response(id, -32601, "Method not found", None),
            };
        };
        if id.is_none() {
            return status.into_response();
        }
        return mcp_error_response_with_status(status, id, -32000, "Invalid session ID", None);
    }

    if id.is_none() {
        return StatusCode::ACCEPTED.into_response();
    }

    match method {
        "initialize" => mcp_initialize_response(id, &req, &config),
        _ => mcp_error_response(id, -32601, "Method not found", None),
    }
}

fn modern_protocol_version(req: &serde_json::Value) -> Option<&str> {
    req.get("params")?
        .get("_meta")?
        .get(PROTOCOL_VERSION_META_KEY)?
        .as_str()
}

async fn mcp_modern_dispatch(
    config: &ServerConfig,
    headers: &HeaderMap,
    id: Option<&serde_json::Value>,
    method: &str,
    requested: &str,
    req: &serde_json::Value,
) -> Response {
    // The mirrored MCP-Protocol-Version header must agree with the body; a
    // missing header is tolerated (the body `_meta` is authoritative here).
    if let Some(header_version) = headers
        .get(PROTOCOL_VERSION_HEADER)
        .and_then(|value| value.to_str().ok())
        && header_version != requested
    {
        return mcp_error_response_with_status(
            StatusCode::BAD_REQUEST,
            id,
            ERR_HEADER_MISMATCH,
            &format!(
                "Header mismatch: {PROTOCOL_VERSION_HEADER} header value \
                 '{header_version}' does not match body value '{requested}'"
            ),
            None,
        );
    }
    if !config.modern || requested != MCP_PROTOCOL_VERSION_MODERN {
        return mcp_error_response_with_status(
            StatusCode::BAD_REQUEST,
            id,
            ERR_UNSUPPORTED_PROTOCOL_VERSION,
            "Unsupported protocol version",
            Some(json!({
                "supported": config.supported_versions(),
                "requested": requested,
            })),
        );
    }
    let Some(id) = id else {
        return StatusCode::ACCEPTED.into_response();
    };
    match method {
        "ping" => mcp_empty_result_response(Some(id), true),
        "server/discover" => mcp_discover_response(Some(id), config),
        "tools/list" => mcp_tools_list_response(Some(id), true),
        "tools/call" => mcp_tools_call_response(Some(id), req, true).await,
        _ => mcp_error_response_with_status(
            StatusCode::NOT_FOUND,
            Some(id),
            -32601,
            "Method not found",
            None,
        ),
    }
}

fn mcp_discover_response(id: Option<&serde_json::Value>, config: &ServerConfig) -> Response {
    mcp_json_response(
        json!({
            "jsonrpc": "2.0",
            "id": id.cloned().unwrap_or(serde_json::Value::Null),
            "result": {
                "resultType": "complete",
                "supportedVersions": config.supported_versions(),
                "capabilities": { "tools": {} },
                "_meta": {
                    SERVER_INFO_META_KEY: { "name": APP_NAME, "version": APP_VERSION }
                },
                "instructions": "Ultra-fast MCP test server."
            }
        })
        .to_string(),
    )
}

fn mcp_json_response(body: String) -> Response {
    ([(header::CONTENT_TYPE, "application/json")], body).into_response()
}

fn mcp_id_json(id: Option<&serde_json::Value>) -> String {
    id.and_then(|value| serde_json::to_string(value).ok())
        .unwrap_or_else(|| "null".to_string())
}

fn mcp_initialize_response(
    id: Option<&serde_json::Value>,
    req: &serde_json::Value,
    config: &ServerConfig,
) -> Response {
    let requested = req
        .get("params")
        .and_then(|params| params.get("protocolVersion"))
        .and_then(serde_json::Value::as_str);
    if config.strict && (!config.legacy || requested != Some(MCP_PROTOCOL_VERSION)) {
        return mcp_error_response(
            id,
            -32602,
            "Unsupported protocol version",
            Some(json!({
                "supported": config.supported_versions(),
                "requested": requested,
            })),
        );
    }
    let session_id = Uuid::new_v4().to_string();
    let session_header = HeaderValue::from_str(&session_id)
        .unwrap_or_else(|_| HeaderValue::from_static("fast-time"));
    if !remember_session(session_id) {
        return mcp_error_response_with_status(
            StatusCode::SERVICE_UNAVAILABLE,
            id,
            -32000,
            "Maximum active sessions reached",
            Some(json!({ "max_sessions": MAX_ACTIVE_SESSIONS })),
        );
    }
    let mut response = mcp_json_response(format!(
        r#"{{"jsonrpc":"2.0","id":{},"result":{{"protocolVersion":"{}","capabilities":{{"tools":{{}}}},"serverInfo":{{"name":"{}","version":"{}"}},"instructions":"Ultra-fast MCP test server."}}}}"#,
        mcp_id_json(id),
        MCP_PROTOCOL_VERSION,
        APP_NAME,
        APP_VERSION
    ));
    response
        .headers_mut()
        .insert(SESSION_HEADER, session_header);
    response
}

fn remember_session(session_id: String) -> bool {
    if let Ok(mut sessions) = ACTIVE_SESSIONS.write() {
        remember_session_in(&mut sessions, session_id)
    } else {
        false
    }
}

fn remember_session_in(sessions: &mut HashSet<String>, session_id: String) -> bool {
    if sessions.len() >= MAX_ACTIVE_SESSIONS {
        return false;
    }
    sessions.insert(session_id)
}

fn remove_session(session_id: &str) -> bool {
    ACTIVE_SESSIONS
        .write()
        .map(|mut sessions| sessions.remove(session_id))
        .unwrap_or(false)
}

fn mcp_validate_active_session(headers: &HeaderMap) -> Result<(), StatusCode> {
    let Some(session_id) = mcp_session_id(headers) else {
        return Err(StatusCode::BAD_REQUEST);
    };
    if ACTIVE_SESSIONS
        .read()
        .map(|sessions| sessions.contains(session_id))
        .unwrap_or(false)
    {
        Ok(())
    } else {
        Err(StatusCode::NOT_FOUND)
    }
}

fn result_type_field(modern: bool) -> &'static str {
    if modern {
        r#""resultType":"complete","#
    } else {
        ""
    }
}

fn mcp_tools_list_response(id: Option<&serde_json::Value>, modern: bool) -> Response {
    mcp_json_response(format!(
        r#"{{"jsonrpc":"2.0","id":{},"result":{{{}"tools":[{{"name":"echo","description":"Echo back the provided message.","inputSchema":{{"type":"object","properties":{{"message":{{"type":"string"}},"delay":{{"type":"integer","minimum":0,"maximum":60000}},"delay_stddev":{{"type":"number","minimum":0}}}},"required":["message"]}}}},{{"name":"flaky","description":"Return isError=true for the first fail_times calls per key, then succeed (retry testing).","inputSchema":{{"type":"object","properties":{{"key":{{"type":"string","description":"Unique key to track attempt count across retries"}},"fail_times":{{"type":"integer","minimum":0,"description":"Number of times to return isError=true before succeeding (default 0)"}}}},"required":["key"]}}}},{{"name":"get_system_time","description":"Get current system time in the specified IANA timezone.","inputSchema":{{"type":"object","properties":{{"timezone":{{"type":"string"}}}}}}}},{{"name":"convert_time","description":"Convert a time value from a source IANA timezone to a target IANA timezone.","inputSchema":{{"type":"object","properties":{{"time":{{"type":"string"}},"source_timezone":{{"type":"string"}},"target_timezone":{{"type":"string"}}}},"required":["time","source_timezone","target_timezone"]}}}},{{"name":"schema_error","description":"Always returns isError=true.","inputSchema":{{"type":"object","properties":{{}}}},"outputSchema":{{"type":"object","properties":{{"recognitionId":{{"type":"string"}},"message":{{"type":"string"}}}},"required":["recognitionId"]}}}},{{"name":"schema_success","description":"Returns a JSON payload that conforms to the declared outputSchema.","inputSchema":{{"type":"object","properties":{{}}}},"outputSchema":{{"type":"object","properties":{{"recognitionId":{{"type":"string"}},"message":{{"type":"string"}}}},"required":["recognitionId"]}}}},{{"name":"get_stats","description":"Get server statistics including request count and uptime.","inputSchema":{{"type":"object","properties":{{}}}}}}]}}}}"#,
        mcp_id_json(id),
        result_type_field(modern)
    ))
}

fn mcp_session_id(headers: &HeaderMap) -> Option<&str> {
    headers.get(SESSION_HEADER)?.to_str().ok()
}

async fn mcp_tools_call_response(
    id: Option<&serde_json::Value>,
    req: &serde_json::Value,
    modern: bool,
) -> Response {
    let params = req.get("params").unwrap_or(&serde_json::Value::Null);
    let name = params
        .get("name")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    let arguments = params.get("arguments").unwrap_or(&serde_json::Value::Null);

    match name {
        "echo" => {
            let Some(arguments) = mcp_arguments_object(id, arguments) else {
                return mcp_invalid_params_response(id, "arguments must be an object");
            };
            let Some(message) = mcp_required_string(id, arguments, "message") else {
                return mcp_invalid_params_response(id, "message must be a string");
            };
            let Some(delay) = mcp_optional_u64(id, arguments, "delay") else {
                return mcp_invalid_params_response(id, "delay must be an unsigned integer");
            };
            let Some(delay_stddev) = mcp_optional_f64(id, arguments, "delay_stddev") else {
                return mcp_invalid_params_response(id, "delay_stddev must be a number");
            };
            let Ok(delay) = validate_delay(delay) else {
                return mcp_invalid_params_response(id, "delay exceeds the 60000 ms limit");
            };

            DIRECT_REQUEST_COUNT.fetch_add(1, Ordering::Relaxed);
            if let Some(ms) = delay
                && ms > 0
            {
                let actual_ms = compute_delay(ms, delay_stddev);
                tokio::time::sleep(std::time::Duration::from_millis(actual_ms)).await;
            }
            mcp_text_result_response(id, message, false, modern)
        }
        "flaky" => {
            let Some(arguments) = mcp_arguments_object(id, arguments) else {
                return mcp_invalid_params_response(id, "arguments must be an object");
            };
            let Some(key) = mcp_required_string(id, arguments, "key") else {
                return mcp_invalid_params_response(id, "key must be a string");
            };
            let Some(fail_times) = mcp_optional_u64(id, arguments, "fail_times") else {
                return mcp_invalid_params_response(id, "fail_times must be an unsigned integer");
            };
            let fail_times = fail_times.unwrap_or(0);

            DIRECT_REQUEST_COUNT.fetch_add(1, Ordering::Relaxed);
            let mut state = FLAKY_STATE.lock().unwrap();
            let attempt = {
                let counter = state.entry(key.to_string()).or_insert(0);
                *counter += 1;
                *counter
            };
            if attempt <= fail_times {
                mcp_text_result_response(
                    id,
                    &format!("flaky transient failure (attempt {attempt}/{fail_times})"),
                    true,
                    modern,
                )
            } else {
                state.remove(key);
                mcp_text_result_response(
                    id,
                    &format!("flaky recovered after {attempt} attempt(s)"),
                    false,
                    modern,
                )
            }
        }
        "get_system_time" => {
            let timezone = if arguments.is_null() {
                None
            } else {
                let Some(arguments) = mcp_arguments_object(id, arguments) else {
                    return mcp_invalid_params_response(id, "arguments must be an object");
                };
                let Some(timezone) = mcp_optional_string(id, arguments, "timezone") else {
                    return mcp_invalid_params_response(id, "timezone must be a string");
                };
                timezone
            };
            let timezone = timezone.unwrap_or("UTC");

            DIRECT_REQUEST_COUNT.fetch_add(1, Ordering::Relaxed);
            match parse_timezone(timezone) {
                Ok(timezone) => {
                    mcp_text_result_response(id, &timezone.format_utc(Utc::now()), false, modern)
                }
                Err(err) => mcp_text_result_response(
                    id,
                    &format!("Invalid timezone '{timezone}': {err}"),
                    true,
                    modern,
                ),
            }
        }
        "convert_time" => mcp_convert_time_response(id, arguments, modern),
        "schema_error" => {
            DIRECT_REQUEST_COUNT.fetch_add(1, Ordering::Relaxed);
            mcp_text_result_response(id, "You cannot send more than 200 points", true, modern)
        }
        "schema_success" => mcp_json_response({
            DIRECT_REQUEST_COUNT.fetch_add(1, Ordering::Relaxed);
            format!(
                r#"{{"jsonrpc":"2.0","id":{},"result":{{{}"content":[{{"type":"text","text":"{{\"recognitionId\":\"rec-123\",\"message\":\"ok\"}}"}}],"structuredContent":{{"recognitionId":"rec-123","message":"ok"}},"isError":false}}}}"#,
                mcp_id_json(id),
                result_type_field(modern)
            )
        }),
        "get_stats" => {
            let count = DIRECT_REQUEST_COUNT.load(Ordering::Relaxed);
            mcp_text_result_response(
                id,
                &format!(
                    "{{\n  \"server\": \"{}\",\n  \"version\": \"{}\",\n  \"requests_handled\": {}\n}}",
                    APP_NAME, APP_VERSION, count
                ),
                false,
                modern,
            )
        }
        _ => mcp_error_response(id, -32602, "Unknown tool", Some(json!({ "tool": name }))),
    }
}

fn mcp_convert_time_response(
    id: Option<&serde_json::Value>,
    arguments: &serde_json::Value,
    modern: bool,
) -> Response {
    let Some(arguments) = mcp_arguments_object(id, arguments) else {
        return mcp_invalid_params_response(id, "arguments must be an object");
    };
    let Some(time) = mcp_required_string(id, arguments, "time") else {
        return mcp_invalid_params_response(id, "time must be a string");
    };
    let Some(source_timezone) = mcp_required_string(id, arguments, "source_timezone") else {
        return mcp_invalid_params_response(id, "source_timezone must be a string");
    };
    let Some(target_timezone) = mcp_required_string(id, arguments, "target_timezone") else {
        return mcp_invalid_params_response(id, "target_timezone must be a string");
    };

    DIRECT_REQUEST_COUNT.fetch_add(1, Ordering::Relaxed);

    let source_timezone = match parse_timezone(source_timezone) {
        Ok(timezone) => timezone,
        Err(err) => {
            return mcp_text_result_response(
                id,
                &format!("invalid source timezone: {err}"),
                true,
                modern,
            );
        }
    };
    let target_timezone = match parse_timezone(target_timezone) {
        Ok(timezone) => timezone,
        Err(err) => {
            return mcp_text_result_response(
                id,
                &format!("invalid target timezone: {err}"),
                true,
                modern,
            );
        }
    };
    match parse_time_in_timezone(time, &source_timezone) {
        Ok(parsed) => {
            let converted = target_timezone.format_utc(parsed);
            mcp_text_result_response(id, &converted, false, modern)
        }
        Err(_) => {
            mcp_text_result_response(id, &format!("invalid time format: {time}"), true, modern)
        }
    }
}

fn mcp_arguments_object<'a>(
    _id: Option<&serde_json::Value>,
    value: &'a serde_json::Value,
) -> Option<&'a serde_json::Map<String, serde_json::Value>> {
    value.as_object()
}

fn mcp_required_string<'a>(
    _id: Option<&serde_json::Value>,
    arguments: &'a serde_json::Map<String, serde_json::Value>,
    field: &str,
) -> Option<&'a str> {
    arguments.get(field)?.as_str()
}

fn mcp_optional_string<'a>(
    _id: Option<&serde_json::Value>,
    arguments: &'a serde_json::Map<String, serde_json::Value>,
    field: &str,
) -> Option<Option<&'a str>> {
    match arguments.get(field) {
        Some(value) if value.is_null() => Some(None),
        Some(value) => value.as_str().map(Some),
        None => Some(None),
    }
}

fn mcp_optional_u64(
    _id: Option<&serde_json::Value>,
    arguments: &serde_json::Map<String, serde_json::Value>,
    field: &str,
) -> Option<Option<u64>> {
    match arguments.get(field) {
        Some(value) if value.is_null() => Some(None),
        Some(value) => value.as_u64().map(Some),
        None => Some(None),
    }
}

fn mcp_optional_f64(
    _id: Option<&serde_json::Value>,
    arguments: &serde_json::Map<String, serde_json::Value>,
    field: &str,
) -> Option<Option<f64>> {
    match arguments.get(field) {
        Some(value) if value.is_null() => Some(None),
        Some(value) => value.as_f64().map(Some),
        None => Some(None),
    }
}

fn mcp_text_result_response(
    id: Option<&serde_json::Value>,
    text: &str,
    is_error: bool,
    modern: bool,
) -> Response {
    let escaped = serde_json::to_string(text).unwrap_or_else(|_| "\"\"".to_string());
    mcp_json_response(format!(
        r#"{{"jsonrpc":"2.0","id":{},"result":{{{}"content":[{{"type":"text","text":{}}}],"isError":{}}}}}"#,
        mcp_id_json(id),
        result_type_field(modern),
        escaped,
        is_error
    ))
}

fn mcp_empty_result_response(id: Option<&serde_json::Value>, modern: bool) -> Response {
    let result = if modern {
        r#"{"resultType":"complete"}"#
    } else {
        "{}"
    };
    mcp_json_response(format!(
        r#"{{"jsonrpc":"2.0","id":{},"result":{}}}"#,
        mcp_id_json(id),
        result
    ))
}

fn mcp_error_response(
    id: Option<&serde_json::Value>,
    code: i32,
    message: &str,
    data: Option<serde_json::Value>,
) -> Response {
    mcp_error_response_with_status(StatusCode::OK, id, code, message, data)
}

fn mcp_error_response_with_status(
    status: StatusCode,
    id: Option<&serde_json::Value>,
    code: i32,
    message: &str,
    data: Option<serde_json::Value>,
) -> Response {
    let escaped_message = serde_json::to_string(message).unwrap_or_else(|_| "\"\"".to_string());
    let data = data
        .map(|value| format!(r#","data":{}"#, value))
        .unwrap_or_default();
    let mut response = mcp_json_response(format!(
        r#"{{"jsonrpc":"2.0","id":{},"error":{{"code":{},"message":{}{}}}}}"#,
        mcp_id_json(id),
        code,
        escaped_message,
        data
    ));
    *response.status_mut() = status;
    response
}

fn mcp_invalid_params_response(id: Option<&serde_json::Value>, message: &str) -> Response {
    mcp_error_response(id, -32602, message, None)
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

    fn default_state() -> axum::extract::State<Arc<ServerConfig>> {
        axum::extract::State(Arc::new(ServerConfig::default()))
    }

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
    fn test_server_advertises_latest_protocol() {
        assert_eq!(MCP_PROTOCOL_VERSION, "2025-11-25");
    }

    #[test]
    fn test_active_session_validation() {
        let session_id = "unit-test-session-validation";
        remove_session(session_id);

        let mut headers = HeaderMap::new();
        headers.insert(SESSION_HEADER, HeaderValue::from_static(session_id));
        assert_eq!(
            mcp_validate_active_session(&headers),
            Err(StatusCode::NOT_FOUND)
        );

        assert!(remember_session(session_id.to_string()));
        assert_eq!(mcp_validate_active_session(&headers), Ok(()));

        assert!(remove_session(session_id));
        assert_eq!(
            mcp_validate_active_session(&headers),
            Err(StatusCode::NOT_FOUND)
        );
    }

    #[test]
    fn test_session_cap_rejects_new_session_when_full() {
        let mut sessions = HashSet::with_capacity(MAX_ACTIVE_SESSIONS);
        for idx in 0..MAX_ACTIVE_SESSIONS {
            assert!(remember_session_in(
                &mut sessions,
                format!("test-session-{idx}")
            ));
        }

        assert!(!remember_session_in(
            &mut sessions,
            "overflow-session".to_string()
        ));
        assert_eq!(sessions.len(), MAX_ACTIVE_SESSIONS);
    }

    #[test]
    fn test_delay_validation_rejects_values_above_limit() {
        assert_eq!(validate_delay(Some(MAX_DELAY_MS)), Ok(Some(MAX_DELAY_MS)));
        assert!(validate_delay(Some(MAX_DELAY_MS + 1)).is_err());
    }

    async fn response_text(response: Response) -> String {
        let bytes = body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("response body should be readable");
        String::from_utf8(bytes.to_vec()).expect("response body should be utf-8")
    }

    async fn response_json(response: Response) -> serde_json::Value {
        serde_json::from_str(&response_text(response).await).expect("response body should be json")
    }

    async fn initialized_headers() -> HeaderMap {
        let response = mcp_handler(
            default_state(),
            HeaderMap::new(),
            axum::Json(json!({
                "jsonrpc": "2.0",
                "method": "initialize",
                "params": {
                    "protocolVersion": "2024-11-05",
                    "capabilities": {},
                    "clientInfo": {
                        "name": "go-parity",
                        "version": "1.0"
                    }
                },
                "id": 1
            })),
        )
        .await;
        let mut headers = HeaderMap::new();
        headers.insert(
            SESSION_HEADER,
            response
                .headers()
                .get(SESSION_HEADER)
                .expect("initialize should issue session id")
                .clone(),
        );
        headers
    }

    #[tokio::test]
    async fn test_version_endpoint_advertises_latest_protocol() {
        let version = version_handler(default_state()).await;
        assert_eq!(version.0["mcp_version"], MCP_PROTOCOL_VERSION);
    }

    #[tokio::test]
    async fn test_initialize_accepts_older_protocol_and_advertises_latest() {
        let response = mcp_handler(
            default_state(),
            HeaderMap::new(),
            axum::Json(json!({
                "jsonrpc": "2.0",
                "method": "initialize",
                "params": {
                    "protocolVersion": "2024-11-05",
                    "capabilities": {},
                    "clientInfo": {
                        "name": "go-compat-smoke",
                        "version": "1.0"
                    }
                },
                "id": 1
            })),
        )
        .await;

        assert_eq!(response.status(), StatusCode::OK);
        assert!(response.headers().contains_key(SESSION_HEADER));
        let session_id = response
            .headers()
            .get(SESSION_HEADER)
            .expect("initialize should issue session id")
            .to_str()
            .expect("session id should be ascii")
            .to_string();
        assert!(Uuid::parse_str(&session_id).is_ok());
        assert!(!session_id.starts_with("fast-time-"));
        let body = response_text(response).await;
        assert!(body.contains(r#""protocolVersion":"2025-11-25""#));
        assert!(remove_session(&session_id));
    }

    #[tokio::test]
    async fn test_flaky_fails_then_succeeds() {
        let key = format!("test-flaky-{}", uuid::Uuid::new_v4());
        // First two calls should be errors
        for attempt in 1..=2u64 {
            let response = mcp_handler(
                default_state(),
                initialized_headers().await,
                axum::Json(json!({
                    "jsonrpc": "2.0",
                    "method": "tools/call",
                    "params": {
                        "name": "flaky",
                        "arguments": {
                            "key": key,
                            "fail_times": 2
                        }
                    },
                    "id": 100 + attempt
                })),
            )
            .await;
            let body = response_json(response).await;
            assert_eq!(
                body["result"]["isError"], true,
                "attempt {attempt} should be isError"
            );
        }
        // Third call should succeed
        let response = mcp_handler(
            default_state(),
            initialized_headers().await,
            axum::Json(json!({
                "jsonrpc": "2.0",
                "method": "tools/call",
                "params": {
                    "name": "flaky",
                    "arguments": {
                        "key": key,
                        "fail_times": 2
                    }
                },
                "id": 103
            })),
        )
        .await;
        let body = response_json(response).await;
        assert_eq!(
            body["result"]["isError"], false,
            "third attempt should succeed"
        );
        assert!(
            body["result"]["content"][0]["text"]
                .as_str()
                .unwrap()
                .contains("flaky recovered after 3 attempt(s)"),
        );
    }

    #[tokio::test]
    async fn test_direct_mcp_session_lifecycle_matches_streamable_http() {
        let initialize = mcp_handler(
            default_state(),
            HeaderMap::new(),
            axum::Json(json!({
                "jsonrpc": "2.0",
                "method": "initialize",
                "params": {
                    "protocolVersion": "2024-11-05",
                    "capabilities": {},
                    "clientInfo": {
                        "name": "go-compat-smoke",
                        "version": "1.0"
                    }
                },
                "id": 1
            })),
        )
        .await;
        let session_id = initialize
            .headers()
            .get(SESSION_HEADER)
            .expect("initialize should issue session id")
            .clone();

        let mut valid_headers = HeaderMap::new();
        valid_headers.insert(SESSION_HEADER, session_id.clone());
        let valid = mcp_handler(
            default_state(),
            valid_headers.clone(),
            axum::Json(json!({
                "jsonrpc": "2.0",
                "method": "tools/list",
                "id": 2
            })),
        )
        .await;
        assert_eq!(valid.status(), StatusCode::OK);

        let ping = mcp_handler(
            default_state(),
            valid_headers.clone(),
            axum::Json(json!({
                "jsonrpc": "2.0",
                "method": "ping",
                "id": 6
            })),
        )
        .await;
        assert_eq!(ping.status(), StatusCode::OK);
        let ping_body = response_json(ping).await;
        assert_eq!(ping_body["result"], json!({}));

        let missing = mcp_handler(
            default_state(),
            HeaderMap::new(),
            axum::Json(json!({
                "jsonrpc": "2.0",
                "method": "tools/list",
                "id": 3
            })),
        )
        .await;
        assert_eq!(missing.status(), StatusCode::BAD_REQUEST);

        let mut fake_headers = HeaderMap::new();
        fake_headers.insert(SESSION_HEADER, HeaderValue::from_static("fake-session"));
        let fake = mcp_handler(
            default_state(),
            fake_headers,
            axum::Json(json!({
                "jsonrpc": "2.0",
                "method": "tools/list",
                "id": 4
            })),
        )
        .await;
        assert_eq!(fake.status(), StatusCode::NOT_FOUND);

        assert_eq!(
            mcp_delete_handler(valid_headers.clone()).await,
            StatusCode::OK
        );
        let deleted = mcp_handler(
            default_state(),
            valid_headers,
            axum::Json(json!({
                "jsonrpc": "2.0",
                "method": "tools/list",
                "id": 5
            })),
        )
        .await;
        assert_eq!(deleted.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_convert_time_matches_go_fast_time_dst_behavior() {
        let response = mcp_handler(
            default_state(),
            initialized_headers().await,
            axum::Json(json!({
                "jsonrpc": "2.0",
                "method": "tools/call",
                "params": {
                    "name": "convert_time",
                    "arguments": {
                        "time": "2025-06-21T16:00:00Z",
                        "source_timezone": "UTC",
                        "target_timezone": "America/New_York"
                    }
                },
                "id": 10
            })),
        )
        .await;

        assert_eq!(response.status(), StatusCode::OK);
        let body = response_json(response).await;
        assert_eq!(
            body["result"]["content"][0]["text"],
            "2025-06-21T12:00:00-04:00"
        );
    }

    #[tokio::test]
    async fn test_convert_time_matches_go_fast_time_half_hour_zones() {
        let response = mcp_handler(
            default_state(),
            initialized_headers().await,
            axum::Json(json!({
                "jsonrpc": "2.0",
                "method": "tools/call",
                "params": {
                    "name": "convert_time",
                    "arguments": {
                        "time": "2025-01-10 10:00:00",
                        "source_timezone": "Asia/Kolkata",
                        "target_timezone": "UTC"
                    }
                },
                "id": 11
            })),
        )
        .await;

        assert_eq!(response.status(), StatusCode::OK);
        let body = response_json(response).await;
        assert_eq!(body["result"]["content"][0]["text"], "2025-01-10T04:30:00Z");
    }

    #[tokio::test]
    async fn test_error_response_escapes_dynamic_message_text() {
        let response = mcp_error_response_with_status(
            StatusCode::BAD_REQUEST,
            Some(&json!(99)),
            -32602,
            r#"bad "message" } ,"injected":true"#,
            None,
        );

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = response_json(response).await;
        assert_eq!(
            body["error"]["message"],
            r#"bad "message" } ,"injected":true"#
        );
        assert!(body["error"].get("injected").is_none());
    }

    #[tokio::test]
    async fn test_mcp_echo_rejects_delay_above_limit() {
        let response = mcp_handler(
            default_state(),
            initialized_headers().await,
            axum::Json(json!({
                "jsonrpc": "2.0",
                "method": "tools/call",
                "params": {
                    "name": "echo",
                    "arguments": {
                        "message": "hello",
                        "delay": MAX_DELAY_MS + 1
                    }
                },
                "id": 12
            })),
        )
        .await;

        assert_eq!(response.status(), StatusCode::OK);
        let body = response_json(response).await;
        assert_eq!(body["error"]["code"], -32602);
        assert_eq!(body["error"]["message"], "delay exceeds the 60000 ms limit");
    }

    #[test]
    fn test_parse_args_defaults_to_legacy_only() {
        let config = parse_args(&[]).unwrap();
        assert_eq!(config, ServerConfig::default());
        assert_eq!(config.supported_versions(), vec!["2025-11-25"]);
    }

    #[test]
    fn test_parse_args_enables_modern_and_strict() {
        let args: Vec<String> = ["--protocol", "2026-07-28", "--strict"]
            .iter()
            .map(ToString::to_string)
            .collect();
        let config = parse_args(&args).unwrap();
        assert!(config.modern);
        assert!(config.strict);
        // Strict serves exactly the revisions named with --protocol.
        assert!(!config.legacy);
        assert_eq!(config.supported_versions(), vec!["2026-07-28"]);
    }

    #[test]
    fn test_parse_args_strict_with_explicit_versions_serves_both() {
        let args: Vec<String> = [
            "--protocol",
            "2026-07-28",
            "--protocol",
            "2025-11-25",
            "--strict",
        ]
        .iter()
        .map(ToString::to_string)
        .collect();
        let config = parse_args(&args).unwrap();
        assert_eq!(
            config.supported_versions(),
            vec!["2026-07-28", "2025-11-25"]
        );
    }

    #[test]
    fn test_parse_args_non_strict_keeps_legacy_fallback() {
        let args: Vec<String> = ["--protocol", "2026-07-28"]
            .iter()
            .map(ToString::to_string)
            .collect();
        let config = parse_args(&args).unwrap();
        assert!(config.legacy);
        assert_eq!(
            config.supported_versions(),
            vec!["2026-07-28", "2025-11-25"]
        );
    }

    #[test]
    fn test_parse_args_rejects_unknown_version_and_missing_value() {
        let bad: Vec<String> = ["--protocol", "1999-01-01"]
            .iter()
            .map(ToString::to_string)
            .collect();
        assert!(parse_args(&bad).is_err());
        let missing: Vec<String> = ["--protocol"].iter().map(ToString::to_string).collect();
        assert!(parse_args(&missing).is_err());
        let unknown: Vec<String> = ["--frobnicate"].iter().map(ToString::to_string).collect();
        assert!(parse_args(&unknown).is_err());
    }

    fn modern_state() -> axum::extract::State<Arc<ServerConfig>> {
        axum::extract::State(Arc::new(ServerConfig {
            modern: true,
            legacy: true,
            strict: false,
        }))
    }

    fn strict_state() -> axum::extract::State<Arc<ServerConfig>> {
        axum::extract::State(Arc::new(ServerConfig {
            modern: true,
            legacy: true,
            strict: true,
        }))
    }

    fn strict_modern_only_state() -> axum::extract::State<Arc<ServerConfig>> {
        axum::extract::State(Arc::new(ServerConfig {
            modern: true,
            legacy: false,
            strict: true,
        }))
    }

    fn modern_request(method: &str, version: &str, id: i64) -> serde_json::Value {
        json!({
            "jsonrpc": "2.0",
            "method": method,
            "params": {
                "_meta": {
                    "io.modelcontextprotocol/protocolVersion": version,
                    "io.modelcontextprotocol/clientCapabilities": {}
                }
            },
            "id": id
        })
    }

    #[tokio::test]
    async fn test_modern_request_rejected_when_modern_not_enabled() {
        let response = mcp_handler(
            default_state(),
            HeaderMap::new(),
            axum::Json(modern_request("tools/list", "2026-07-28", 1)),
        )
        .await;

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = response_json(response).await;
        assert_eq!(body["error"]["code"], -32022);
        assert_eq!(body["error"]["data"]["supported"], json!(["2025-11-25"]));
        assert_eq!(body["error"]["data"]["requested"], "2026-07-28");
    }

    #[tokio::test]
    async fn test_server_discover_lists_supported_versions() {
        let response = mcp_handler(
            modern_state(),
            HeaderMap::new(),
            axum::Json(modern_request("server/discover", "2026-07-28", 1)),
        )
        .await;

        assert_eq!(response.status(), StatusCode::OK);
        let body = response_json(response).await;
        let result = &body["result"];
        assert_eq!(result["resultType"], "complete");
        assert_eq!(
            result["supportedVersions"],
            json!(["2026-07-28", "2025-11-25"])
        );
        assert_eq!(
            result["_meta"]["io.modelcontextprotocol/serverInfo"]["name"],
            APP_NAME
        );
        assert!(result["capabilities"]["tools"].is_object());
    }

    #[tokio::test]
    async fn test_modern_tools_call_needs_no_session() {
        let mut request = modern_request("tools/call", "2026-07-28", 7);
        request["params"]["name"] = json!("echo");
        request["params"]["arguments"] = json!({ "message": "hi" });
        let response = mcp_handler(modern_state(), HeaderMap::new(), axum::Json(request)).await;

        assert_eq!(response.status(), StatusCode::OK);
        let body = response_json(response).await;
        assert_eq!(body["result"]["resultType"], "complete");
        assert_eq!(body["result"]["content"][0]["text"], "hi");
        assert_eq!(body["result"]["isError"], false);
    }

    #[tokio::test]
    async fn test_modern_version_mismatch_returns_unsupported_error() {
        let response = mcp_handler(
            modern_state(),
            HeaderMap::new(),
            axum::Json(modern_request("tools/list", "2025-06-18", 1)),
        )
        .await;

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = response_json(response).await;
        assert_eq!(body["error"]["code"], -32022);
        assert_eq!(
            body["error"]["data"]["supported"],
            json!(["2026-07-28", "2025-11-25"])
        );
    }

    #[tokio::test]
    async fn test_modern_header_mismatch_rejected() {
        let mut headers = HeaderMap::new();
        headers.insert(
            PROTOCOL_VERSION_HEADER,
            HeaderValue::from_static("2025-06-18"),
        );
        let response = mcp_handler(
            modern_state(),
            headers,
            axum::Json(modern_request("ping", "2026-07-28", 1)),
        )
        .await;

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = response_json(response).await;
        assert_eq!(body["error"]["code"], -32020);
    }

    #[tokio::test]
    async fn test_modern_unknown_method_is_not_found() {
        let response = mcp_handler(
            modern_state(),
            HeaderMap::new(),
            axum::Json(modern_request("resources/list", "2026-07-28", 1)),
        )
        .await;

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        let body = response_json(response).await;
        assert_eq!(body["error"]["code"], -32601);
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

    #[tokio::test]
    async fn test_strict_initialize_rejects_unsupported_version() {
        let response = mcp_handler(
            strict_state(),
            HeaderMap::new(),
            axum::Json(initialize_request("2024-11-05")),
        )
        .await;

        assert_eq!(response.status(), StatusCode::OK);
        assert!(response.headers().get(SESSION_HEADER).is_none());
        let body = response_json(response).await;
        assert_eq!(body["error"]["code"], -32602);
        assert_eq!(body["error"]["message"], "Unsupported protocol version");
        assert_eq!(
            body["error"]["data"]["supported"],
            json!(["2026-07-28", "2025-11-25"])
        );
        assert_eq!(body["error"]["data"]["requested"], "2024-11-05");
    }

    #[tokio::test]
    async fn test_strict_initialize_accepts_supported_version() {
        let response = mcp_handler(
            strict_state(),
            HeaderMap::new(),
            axum::Json(initialize_request("2025-11-25")),
        )
        .await;

        assert_eq!(response.status(), StatusCode::OK);
        assert!(response.headers().contains_key(SESSION_HEADER));
        let body = response_text(response).await;
        assert!(body.contains(r#""protocolVersion":"2025-11-25""#));
    }

    #[tokio::test]
    async fn test_strict_modern_only_rejects_all_initialize() {
        for version in ["2025-11-25", "2024-11-05"] {
            let response = mcp_handler(
                strict_modern_only_state(),
                HeaderMap::new(),
                axum::Json(initialize_request(version)),
            )
            .await;

            assert_eq!(response.status(), StatusCode::OK);
            assert!(response.headers().get(SESSION_HEADER).is_none());
            let body = response_json(response).await;
            assert_eq!(body["error"]["code"], -32602);
            assert_eq!(body["error"]["message"], "Unsupported protocol version");
            assert_eq!(
                body["error"]["data"]["supported"],
                json!(["2026-07-28"]),
                "initialize for {version} must be rejected naming only the strict set"
            );
        }
    }

    #[tokio::test]
    async fn test_non_strict_initialize_falls_back_to_legacy_version() {
        let response = mcp_handler(
            modern_state(),
            HeaderMap::new(),
            axum::Json(initialize_request("2024-11-05")),
        )
        .await;

        assert_eq!(response.status(), StatusCode::OK);
        let body = response_text(response).await;
        assert!(body.contains(r#""protocolVersion":"2025-11-25""#));
    }
}
