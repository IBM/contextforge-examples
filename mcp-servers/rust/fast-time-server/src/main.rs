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
// - whoami: Reflects the HTTP headers of the tool-call request
//
// Prompts seed conversations that drive those tools:
// - current_time / convert_time / server_diagnostics
//
// Resources mirror the same surface:
// - config://timezones, server://info, server://stats (static/dynamic docs)
// - time://now/{timezone} resource template (get_system_time as a resource)
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
use rmcp::handler::server::prompt::PromptContext;
use rmcp::handler::server::router::prompt::PromptRouter;
use rmcp::handler::server::router::tool::ToolRouter;
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::{
    CacheScope, CallToolResult, ContentBlock, GetPromptRequestParams, GetPromptResponse,
    Implementation, InitializeRequestParams, InitializeResult, ListPromptsResult,
    ListResourceTemplatesResult, ListResourcesResult, ListToolsResult, PaginatedRequestParams,
    PromptMessage, ProtocolVersion, ReadResourceRequestParams, ReadResourceResponse,
    ReadResourceResult, Resource, ResourceContents, ResourceTemplate, Role, ServerCapabilities,
    ServerConfig,
};
use rmcp::service::RequestContext;
use rmcp::transport::streamable_http_server::{
    StreamableHttpServerConfig, StreamableHttpService, session::local::LocalSessionManager,
};
use rmcp::{ErrorData as McpError, Json, RoleServer, ServerHandler, schemars};
use rmcp::{prompt, prompt_router, tool, tool_handler, tool_router};
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
#[cfg(test)]
const MCP_PROTOCOL_VERSION: &str = "2025-11-25";
#[cfg(test)]
const MCP_PROTOCOL_VERSION_MODERN: &str = "2026-07-28";
const SUPPORTED_PROTOCOL_VERSIONS: &[ProtocolVersion] =
    &[ProtocolVersion::V_2025_11_25, ProtocolVersion::V_2026_07_28];
/// Resource URIs and the RFC 6570 template kept in one place so
/// `resources/list` and `resources/read` cannot drift apart.
const TIMEZONES_RESOURCE_URI: &str = "config://timezones";
const SERVER_INFO_RESOURCE_URI: &str = "server://info";
const SERVER_STATS_RESOURCE_URI: &str = "server://stats";
const TIME_NOW_RESOURCE_PREFIX: &str = "time://now/";
const TIME_NOW_URI_TEMPLATE: &str = "time://now/{timezone}";

/// MCP era selection, chosen at startup via the `MCP_PROTOCOL_MODE`
/// environment variable: `legacy` serves only 2025-11-25, `modern` only
/// 2026-07-28, and `dual` (the default) serves both eras simultaneously.
/// The pre-rmcp server also accepted `--protocol`/`--strict` CLI flags; the
/// SDK rewrite dropped them, so the env var is the only mode selector.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProtocolMode {
    Legacy,
    Modern,
    Dual,
}

impl ProtocolMode {
    /// Parse the mode from an optional `MCP_PROTOCOL_MODE` value. Unset or
    /// empty preserves the dual-era default; anything else must be one of
    /// the three named modes or startup fails.
    fn parse(value: Option<&str>) -> anyhow::Result<Self> {
        match value.map(str::trim) {
            None | Some("") => Ok(Self::Dual),
            Some("legacy") => Ok(Self::Legacy),
            Some("modern") => Ok(Self::Modern),
            Some("dual") => Ok(Self::Dual),
            Some(other) => Err(anyhow::anyhow!(
                "invalid MCP_PROTOCOL_MODE '{other}': expected one of: legacy, modern, dual"
            )),
        }
    }

    fn from_env() -> anyhow::Result<Self> {
        Self::parse(env::var("MCP_PROTOCOL_MODE").ok().as_deref())
    }

    /// The revisions served in this mode, advertised via
    /// `supported_protocol_versions` so the SDK rejects requests for any
    /// other era.
    fn supported_versions(self) -> &'static [ProtocolVersion] {
        match self {
            Self::Legacy => &[ProtocolVersion::V_2025_11_25],
            Self::Modern => &[ProtocolVersion::V_2026_07_28],
            Self::Dual => SUPPORTED_PROTOCOL_VERSIONS,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Legacy => "legacy",
            Self::Modern => "modern",
            Self::Dual => "dual",
        }
    }
}

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
    prompt_router: PromptRouter<Self>,
    mode: ProtocolMode,
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

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
struct CurrentTimePromptArgs {
    /// IANA timezone name or fixed UTC offset (defaults to UTC)
    timezone: Option<String>,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
struct ConvertTimePromptArgs {
    /// Time value to convert, e.g. 2026-10-04T12:00:00Z or 2026-10-04 12:00:00
    time: String,
    /// IANA timezone name or fixed UTC offset the time is expressed in
    source_timezone: String,
    /// IANA timezone name or fixed UTC offset to convert the time to
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

/// Reflected HTTP headers for `whoami`: a lowercased name → value map with
/// `authorization` always present (`null` when absent). A typed newtype
/// instead of `serde_json::Value` so the SDK's auto-generated `outputSchema`
/// is a spec-valid object schema (`{"type":"object","additionalProperties":…}`)
/// — schemars renders `Value` as a typeless schema, which fails the MCP
/// conformance `tools-list` check.
#[derive(Debug, serde::Serialize, schemars::JsonSchema)]
#[serde(transparent)]
struct WhoamiResult(std::collections::BTreeMap<String, Option<String>>);

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

/// True when the active request speaks the modern 2026-07-28 revision: the
/// per-request `_meta` version is present and modern. Legacy-era requests
/// carry no `_meta` version and resolve to `false`.
fn is_modern_request(context: &RequestContext<RoleServer>) -> bool {
    context.meta.protocol_version() == Some(ProtocolVersion::V_2026_07_28)
}

#[tool_router]
impl FastTimeServer {
    fn new(mode: ProtocolMode) -> Self {
        Self {
            state: Arc::new(SharedState::default()),
            tool_router: Self::tool_router(),
            prompt_router: Self::prompt_router(),
            mode,
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

    /// Reflect the HTTP headers of the tool-call request so header-affecting
    /// gateway plugins (e.g. Vault `tool_pre_invoke`) can assert what the
    /// upstream actually received. Names are normalized to lowercase; the
    /// `authorization` key is always present so callers can distinguish
    /// "absent" from "not reflected". Values go only into the tool response —
    /// they are never logged.
    #[tool(
        description = "Reflect the HTTP headers received with this tool call as a lowercased JSON map, for header-propagation testing. The authorization key is null when the header is absent."
    )]
    fn whoami(&self, context: RequestContext<RoleServer>) -> Result<Json<WhoamiResult>, McpError> {
        self.state.request_count.fetch_add(1, Ordering::Relaxed);
        let mut headers = std::collections::BTreeMap::new();
        if let Some(parts) = context.extensions.get::<axum::http::request::Parts>() {
            for (name, value) in &parts.headers {
                let value = value
                    .to_str()
                    .map(str::to_owned)
                    .unwrap_or_else(|_| String::from_utf8_lossy(value.as_bytes()).into_owned());
                // http::HeaderName is already lowercase; first occurrence wins
                // for repeated headers.
                headers
                    .entry(name.as_str().to_lowercase())
                    .or_insert(Some(value));
            }
        }
        headers.entry("authorization".to_string()).or_insert(None);
        Ok(Json(WhoamiResult(headers)))
    }
}

/// Prompts mirror the tool surface: each renders a user message that asks
/// the model to drive one of this server's own tools, with arguments typed
/// by structs in the same style the tools use (the SDK derives the advertised
/// prompt arguments from the schema).
#[prompt_router]
impl FastTimeServer {
    #[prompt(
        description = "Ask the model to report the current time in a timezone using the get_system_time tool."
    )]
    fn current_time(
        &self,
        Parameters(args): Parameters<CurrentTimePromptArgs>,
    ) -> Result<Vec<PromptMessage>, McpError> {
        let timezone = args.timezone.unwrap_or_else(|| "UTC".to_string());
        Ok(vec![PromptMessage::new_text(
            Role::User,
            format!(
                "Call the get_system_time tool with {{\"timezone\": \"{timezone}\"}} and report the timestamp it returns, including its UTC offset."
            ),
        )])
    }

    #[prompt(
        name = "convert_time",
        description = "Ask the model to convert a time between timezones using the convert_time tool."
    )]
    fn convert_time_prompt(
        &self,
        Parameters(args): Parameters<ConvertTimePromptArgs>,
    ) -> Result<Vec<PromptMessage>, McpError> {
        Ok(vec![PromptMessage::new_text(
            Role::User,
            format!(
                "Call the convert_time tool with {{\"time\": \"{}\", \"source_timezone\": \"{}\", \"target_timezone\": \"{}\"}} and state the converted time.",
                args.time, args.source_timezone, args.target_timezone
            ),
        )])
    }

    #[prompt(
        description = "Ask the model for a server health report built from the get_stats and verify-protocol tools."
    )]
    fn server_diagnostics(&self) -> Result<Vec<PromptMessage>, McpError> {
        Ok(vec![PromptMessage::new_text(
            Role::User,
            "Call the get_stats tool, then the verify-protocol tool, and summarize the results: the number of requests handled so far, plus the MCP protocol version and transport that served the request.".to_string(),
        )])
    }
}

// ============================================================================
// Resources
// ============================================================================

/// The static registry behind `resources/list`; each entry mirrors a tool or
/// REST endpoint the server already exposes.
fn resource_catalog() -> Vec<Resource> {
    vec![
        Resource::new(TIMEZONES_RESOURCE_URI, "supported_timezones")
            .with_description(
                "Timezone formats accepted by the time tools, the time://now/{timezone} resource template, and /api/time.",
            )
            .with_mime_type("text/plain"),
        Resource::new(SERVER_INFO_RESOURCE_URI, "server_info")
            .with_description(
                "Server identity and the MCP protocol versions this instance serves (mirrors the /version endpoint).",
            )
            .with_mime_type("application/json"),
        Resource::new(SERVER_STATS_RESOURCE_URI, "server_stats")
            .with_description("Live request counter (mirrors the get_stats tool).")
            .with_mime_type("application/json"),
    ]
}

/// The single dynamic entry point: `time://now/{timezone}` resolves exactly
/// like the `get_system_time` tool and `/api/time`.
fn resource_templates() -> Vec<ResourceTemplate> {
    vec![ResourceTemplate::new(TIME_NOW_URI_TEMPLATE, "current_time")
        .with_description("Current time in an IANA timezone or fixed UTC offset (mirrors the get_system_time tool).")
        .with_mime_type("text/plain")]
}

/// Documented mirror of the values `parse_timezone` accepts.
fn timezones_document() -> String {
    [
        "fast-time-server accepted timezone formats",
        "=========================================",
        "",
        "The get_system_time and convert_time tools, the time://now/{timezone}",
        "resource template, and the /api/time endpoint all accept the same values:",
        "",
        "- UTC or GMT (case-insensitive)",
        "- IANA timezone names, e.g. America/New_York, Europe/London, Asia/Tokyo",
        "- Fixed UTC offsets as +HH:MM or -HH:MM, e.g. +05:30, -08:00",
        "",
        "Any other value is rejected with an \"Invalid timezone\" error.",
    ]
    .join("\n")
}

/// JSON mirror of the `/version` REST endpoint.
fn server_info_document(mode: ProtocolMode) -> String {
    json!({
        "name": APP_NAME,
        "version": APP_VERSION,
        "protocol_mode": mode.as_str(),
        "mcp_versions": mode
            .supported_versions()
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>(),
    })
    .to_string()
}

/// JSON mirror of the `get_stats` tool payload.
fn server_stats_document(requests_handled: u64) -> String {
    json!({
        "server": APP_NAME,
        "version": APP_VERSION,
        "requests_handled": requests_handled,
    })
    .to_string()
}

impl FastTimeServer {
    /// Resolve one resource URI to its contents, or a protocol error:
    /// `RESOURCE_NOT_FOUND` for unknown URIs (the SDK rewrites it to
    /// `invalid params` for 2026-07-28 peers) and `invalid params` with the
    /// same wording the tools use for bad timezones.
    fn read_resource_contents(&self, uri: &str) -> Result<ResourceContents, McpError> {
        match uri {
            TIMEZONES_RESOURCE_URI => Ok(ResourceContents::text(timezones_document(), uri)),
            SERVER_INFO_RESOURCE_URI => {
                Ok(ResourceContents::text(server_info_document(self.mode), uri)
                    .with_mime_type("application/json"))
            }
            SERVER_STATS_RESOURCE_URI => Ok(ResourceContents::text(
                server_stats_document(self.state.request_count.load(Ordering::Relaxed)),
                uri,
            )
            .with_mime_type("application/json")),
            _ => {
                let Some(timezone) = uri.strip_prefix(TIME_NOW_RESOURCE_PREFIX) else {
                    return Err(McpError::resource_not_found(
                        format!("unknown resource: {uri}"),
                        None,
                    ));
                };
                parse_timezone(timezone)
                    .map(|tz| ResourceContents::text(tz.format_utc(Utc::now()), uri))
                    .map_err(|err| {
                        McpError::invalid_params(
                            format!("Invalid timezone '{timezone}': {err}"),
                            None,
                        )
                    })
            }
        }
    }
}

#[tool_handler(router = self.tool_router)]
impl ServerHandler for FastTimeServer {
    /// Gate the legacy handshake to the era(s) the mode serves. The SDK's
    /// default negotiation accepts any known revision, so a single-era mode
    /// must reject handshakes for the other era itself. Unknown revisions
    /// keep the SDK's fallback-to-server-default behavior.
    async fn initialize(
        &self,
        request: InitializeRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<InitializeResult, McpError> {
        let supported = self.mode.supported_versions();
        if !supported.contains(&request.protocol_version)
            && ProtocolVersion::KNOWN_VERSIONS.contains(&request.protocol_version)
        {
            return Err(McpError::unsupported_protocol_version(
                request.protocol_version.clone(),
                supported,
            ));
        }
        context.peer.set_peer_info(request.clone());
        let mut info = self.get_info();
        if supported.contains(&request.protocol_version) {
            info.protocol_version = request.protocol_version;
        }
        Ok(info)
    }

    /// tools/list is cacheable at 2026-07-28, so the modern wire format
    /// requires the cache directives (`cacheScope`/`ttlMs`). Legacy-era
    /// requests keep the fields absent: the option stays `None` and the SDK
    /// strips `resultType` for legacy peers the same way. The prompts and
    /// resources handlers below follow this exact rule.
    async fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, McpError> {
        let mut result = ListToolsResult::with_all_items(self.tool_router.list_all());
        if is_modern_request(&context) {
            result = result.with_ttl_ms(0).with_cache_scope(CacheScope::Private);
        }
        Ok(result)
    }

    /// Hand-routed instead of `#[prompt_handler]`: that macro's generated
    /// `list_prompts` hardcodes `CacheScope::Public`, which would drift from
    /// `list_tools`; this keeps every list result on the same wire rules.
    async fn list_prompts(
        &self,
        _request: Option<PaginatedRequestParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<ListPromptsResult, McpError> {
        let mut result = ListPromptsResult::with_all_items(self.prompt_router.list_all());
        if is_modern_request(&context) {
            result = result.with_ttl_ms(0).with_cache_scope(CacheScope::Private);
        }
        Ok(result)
    }

    async fn get_prompt(
        &self,
        request: GetPromptRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<GetPromptResponse, McpError> {
        let prompt_context = PromptContext::new(self, request.name, request.arguments, context);
        self.prompt_router.get_prompt(prompt_context).await
    }

    async fn list_resources(
        &self,
        _request: Option<PaginatedRequestParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<ListResourcesResult, McpError> {
        let mut result = ListResourcesResult::with_all_items(resource_catalog());
        if is_modern_request(&context) {
            result = result.with_ttl_ms(0).with_cache_scope(CacheScope::Private);
        }
        Ok(result)
    }

    async fn list_resource_templates(
        &self,
        _request: Option<PaginatedRequestParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<ListResourceTemplatesResult, McpError> {
        let mut result = ListResourceTemplatesResult::with_all_items(resource_templates());
        if is_modern_request(&context) {
            result = result.with_ttl_ms(0).with_cache_scope(CacheScope::Private);
        }
        Ok(result)
    }

    /// Reads carry the same 2026-07-28 cache directives with `ttlMs: 0` —
    /// nothing this server returns is safe to cache (`time://now/*` and
    /// `server://stats` are live values, so the static documents stay
    /// uniform with them rather than special-cased).
    async fn read_resource(
        &self,
        request: ReadResourceRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<ReadResourceResponse, McpError> {
        let contents = self.read_resource_contents(&request.uri)?;
        let mut result = ReadResourceResult::new(vec![contents]);
        if is_modern_request(&context) {
            result = result.with_ttl_ms(0).with_cache_scope(CacheScope::Private);
        }
        Ok(result.into())
    }

    fn get_info(&self) -> ServerConfig {
        ServerConfig::new(
            ServerCapabilities::builder()
                .enable_tools()
                .enable_prompts()
                .enable_resources()
                .build(),
        )
        .with_server_info(Implementation::new(APP_NAME, APP_VERSION))
        .with_protocol_version(self.mode.supported_versions()[0].clone())
        .with_instructions("Ultra-fast MCP test server.".to_string())
    }

    fn supported_protocol_versions(&self) -> Cow<'static, [ProtocolVersion]> {
        Cow::Borrowed(self.mode.supported_versions())
    }
}

// ============================================================================
// Main Entry Point
// ============================================================================

fn build_router(mode: ProtocolMode) -> Router {
    let server = Arc::new(FastTimeServer::new(mode));
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
        .route(
            "/version",
            axum::routing::get(move || version_handler(mode)),
        )
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

    let mode = ProtocolMode::from_env()?;

    // Get bind address from environment or use default
    let bind_address =
        env::var("BIND_ADDRESS").unwrap_or_else(|_| DEFAULT_BIND_ADDRESS.to_string());

    info!("{} v{} starting...", APP_NAME, APP_VERSION);
    info!("Binding to: {}", bind_address);
    info!(
        "MCP protocol mode: {} (versions: {})",
        mode.as_str(),
        mode.supported_versions()
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(", ")
    );

    let router = build_router(mode);

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
async fn version_handler(mode: ProtocolMode) -> axum::Json<serde_json::Value> {
    let mcp_versions: Vec<String> = mode
        .supported_versions()
        .iter()
        .map(ToString::to_string)
        .collect();
    axum::Json(json!({
        "name": APP_NAME,
        "version": APP_VERSION,
        "protocol_mode": mode.as_str(),
        "mcp_versions": mcp_versions
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
        let server = FastTimeServer::new(ProtocolMode::Dual);
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

    /// Any JSON-RPC request on an established legacy session (SSE response).
    async fn legacy_jsonrpc(
        router: &Router,
        session_id: &str,
        method: &str,
        params: serde_json::Value,
        id: i64,
    ) -> serde_json::Value {
        let mut request = mcp_post(json!({
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
            "id": id
        }));
        request
            .headers_mut()
            .insert(SESSION_HEADER, HeaderValue::from_str(session_id).unwrap());
        let response = oneshot(router, request).await;
        assert_eq!(response.status(), StatusCode::OK);
        parse_sse_json(&response_text(response).await)
    }

    async fn legacy_tool_call(
        router: &Router,
        session_id: &str,
        name: &str,
        arguments: serde_json::Value,
        id: i64,
    ) -> serde_json::Value {
        legacy_jsonrpc(
            router,
            session_id,
            "tools/call",
            json!({ "name": name, "arguments": arguments }),
            id,
        )
        .await
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
    /// matching the body method, plus `Mcp-Name` carrying `params.name`
    /// (tools/call, prompts/get) or `params.uri` (resources/read) for
    /// named methods.
    async fn modern_call(
        router: &Router,
        body: serde_json::Value,
    ) -> (StatusCode, serde_json::Value) {
        let version = body["params"]["_meta"][PROTOCOL_VERSION_META_KEY]
            .as_str()
            .expect("modern request should carry a version")
            .to_string();
        let method = body["method"].as_str().expect("request method").to_string();
        let name = body["params"]["name"]
            .as_str()
            .or(body["params"]["uri"].as_str())
            .map(str::to_string);
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
            &build_router(ProtocolMode::Dual),
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
        assert!(result["capabilities"]["prompts"].is_object());
        assert!(result["capabilities"]["resources"].is_object());
    }

    #[tokio::test]
    async fn test_initialize_falls_back_to_legacy_for_unknown_version() {
        let response = oneshot(
            &build_router(ProtocolMode::Dual),
            mcp_post(initialize_request("1999-01-01")),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let body = parse_sse_json(&response_text(response).await);
        assert_eq!(body["result"]["protocolVersion"], MCP_PROTOCOL_VERSION);
    }

    #[tokio::test]
    async fn test_legacy_session_lifecycle() {
        let router = build_router(ProtocolMode::Dual);
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
        assert_eq!(body["result"]["tools"].as_array().map(Vec::len), Some(9));

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
        let router = build_router(ProtocolMode::Dual);
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
        let router = build_router(ProtocolMode::Dual);
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
        let router = build_router(ProtocolMode::Dual);
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
        let router = build_router(ProtocolMode::Dual);
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
        let router = build_router(ProtocolMode::Dual);
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

    #[tokio::test]
    async fn test_legacy_prompts_and_resources() {
        let router = build_router(ProtocolMode::Dual);
        let session_id = initialize_session(&router).await;

        // prompts/list: three tool-driven prompts, legacy era has no cache
        // directives.
        let body = legacy_jsonrpc(&router, &session_id, "prompts/list", json!({}), 50).await;
        let result = &body["result"];
        let prompts = result["prompts"].as_array().unwrap();
        assert_eq!(prompts.len(), 3);
        assert!(result.get("cacheScope").is_none());
        assert!(result.get("ttlMs").is_none());

        let current_time = prompts
            .iter()
            .find(|prompt| prompt["name"] == "current_time")
            .unwrap();
        let arguments = current_time["arguments"].as_array().unwrap();
        assert_eq!(arguments.len(), 1);
        assert_eq!(arguments[0]["name"], "timezone");
        assert_eq!(arguments[0]["required"], false);

        // prompts/get renders a user message that names the tool and args.
        let body = legacy_jsonrpc(
            &router,
            &session_id,
            "prompts/get",
            json!({ "name": "current_time", "arguments": { "timezone": "Asia/Tokyo" } }),
            51,
        )
        .await;
        let result = &body["result"];
        assert_eq!(result["messages"].as_array().map(Vec::len), Some(1));
        assert_eq!(result["messages"][0]["role"], "user");
        let text = result["messages"][0]["content"]["text"].as_str().unwrap();
        assert!(text.contains("get_system_time"));
        assert!(text.contains("Asia/Tokyo"));

        // prompts/get without optional arguments defaults to UTC.
        let body = legacy_jsonrpc(
            &router,
            &session_id,
            "prompts/get",
            json!({ "name": "current_time", "arguments": {} }),
            52,
        )
        .await;
        let text = body["result"]["messages"][0]["content"]["text"]
            .as_str()
            .unwrap();
        assert!(text.contains("UTC"));

        // Unknown prompt names are invalid-params errors.
        let body = legacy_jsonrpc(
            &router,
            &session_id,
            "prompts/get",
            json!({ "name": "does_not_exist" }),
            53,
        )
        .await;
        assert_eq!(body["error"]["code"], -32602);
        assert!(
            body["error"]["message"]
                .as_str()
                .unwrap()
                .contains("not found")
        );

        // resources/list: three static entries mirroring tools/endpoints.
        let body = legacy_jsonrpc(&router, &session_id, "resources/list", json!({}), 54).await;
        let result = &body["result"];
        let uris: Vec<&str> = result["resources"]
            .as_array()
            .unwrap()
            .iter()
            .map(|resource| resource["uri"].as_str().unwrap())
            .collect();
        assert_eq!(
            uris,
            ["config://timezones", "server://info", "server://stats"]
        );
        assert!(result.get("cacheScope").is_none());

        // resources/templates/list: the dynamic time template.
        let body = legacy_jsonrpc(
            &router,
            &session_id,
            "resources/templates/list",
            json!({}),
            55,
        )
        .await;
        let templates = body["result"]["resourceTemplates"].as_array().unwrap();
        assert_eq!(templates.len(), 1);
        assert_eq!(templates[0]["uriTemplate"], "time://now/{timezone}");

        // resources/read: static document.
        let body = legacy_jsonrpc(
            &router,
            &session_id,
            "resources/read",
            json!({ "uri": "config://timezones" }),
            56,
        )
        .await;
        let contents = &body["result"]["contents"][0];
        assert_eq!(contents["uri"], "config://timezones");
        assert_eq!(contents["mimeType"], "text/plain");
        assert!(
            contents["text"]
                .as_str()
                .unwrap()
                .contains("IANA timezone names")
        );

        // resources/read: server://info mirrors the /version endpoint.
        let body = legacy_jsonrpc(
            &router,
            &session_id,
            "resources/read",
            json!({ "uri": "server://info" }),
            57,
        )
        .await;
        let contents = &body["result"]["contents"][0];
        assert_eq!(contents["mimeType"], "application/json");
        let info: serde_json::Value =
            serde_json::from_str(contents["text"].as_str().unwrap()).unwrap();
        assert_eq!(info["name"], APP_NAME);
        assert_eq!(
            info["mcp_versions"],
            json!([MCP_PROTOCOL_VERSION, MCP_PROTOCOL_VERSION_MODERN])
        );

        // resources/read: the template URI resolves like get_system_time.
        let body = legacy_jsonrpc(
            &router,
            &session_id,
            "resources/read",
            json!({ "uri": "time://now/Asia/Tokyo" }),
            58,
        )
        .await;
        let text = body["result"]["contents"][0]["text"].as_str().unwrap();
        assert!(
            text.ends_with("+09:00"),
            "Tokyo time should be +09:00: {text}"
        );

        // Unknown URIs keep the legacy RESOURCE_NOT_FOUND code.
        let body = legacy_jsonrpc(
            &router,
            &session_id,
            "resources/read",
            json!({ "uri": "config://does-not-exist" }),
            59,
        )
        .await;
        assert_eq!(body["error"]["code"], -32002);

        // Bad timezones in template reads mirror the tool's error wording.
        let body = legacy_jsonrpc(
            &router,
            &session_id,
            "resources/read",
            json!({ "uri": "time://now/Mars/Olympus" }),
            60,
        )
        .await;
        assert_eq!(body["error"]["code"], -32602);
        assert!(
            body["error"]["message"]
                .as_str()
                .unwrap()
                .contains("Mars/Olympus")
        );
    }

    // ========================================================================
    // Modern era (2026-07-28): stateless, version in params._meta + header
    // ========================================================================

    #[tokio::test]
    async fn test_modern_verify_protocol_reports_stateless() {
        let (status, body) = modern_tool_call(
            &build_router(ProtocolMode::Dual),
            "verify-protocol",
            json!({}),
            1,
        )
        .await;
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
    async fn test_modern_whoami_reflects_headers_lowercased() {
        let mut request = modern_request("tools/call", MCP_PROTOCOL_VERSION_MODERN, 40);
        request["params"]["name"] = json!("whoami");
        request["params"]["arguments"] = json!({});
        let mut http = mcp_post(request);
        let headers = http.headers_mut();
        headers.insert(
            PROTOCOL_VERSION_HEADER,
            HeaderValue::from_static(MCP_PROTOCOL_VERSION_MODERN),
        );
        headers.insert("mcp-method", HeaderValue::from_static("tools/call"));
        headers.insert("mcp-name", HeaderValue::from_static("whoami"));
        headers.insert(
            header::AUTHORIZATION,
            HeaderValue::from_static("Bearer test-token"),
        );
        headers.insert("X-Vault-Tokens", HeaderValue::from_static("secret-token"));

        let response = oneshot(&build_router(ProtocolMode::Dual), http).await;
        assert_eq!(response.status(), StatusCode::OK);
        let body: serde_json::Value =
            serde_json::from_str(&response_text(response).await).expect("response should be JSON");
        let result = &body["result"];
        assert_eq!(result["isError"], false);
        let reflected = &result["structuredContent"];
        assert_eq!(reflected["authorization"], "Bearer test-token");
        assert_eq!(reflected["x-vault-tokens"], "secret-token");
        assert_eq!(reflected["mcp-method"], "tools/call");
        let text: serde_json::Value =
            serde_json::from_str(result["content"][0]["text"].as_str().expect("text content"))
                .expect("text content should mirror the structured payload");
        assert_eq!(text["authorization"], "Bearer test-token");
    }

    #[tokio::test]
    async fn test_legacy_whoami_reports_null_authorization_when_absent() {
        let router = build_router(ProtocolMode::Dual);
        let session_id = initialize_session(&router).await;
        let body = legacy_tool_call(&router, &session_id, "whoami", json!({}), 41).await;
        let result = &body["result"];
        assert_eq!(result["isError"], false);
        let reflected = &result["structuredContent"];
        assert!(reflected["authorization"].is_null());
        assert_eq!(reflected["mcp-session-id"], session_id.as_str());
        assert_eq!(reflected["content-type"], "application/json");
    }

    #[tokio::test]
    async fn test_modern_tools_call_needs_no_session() {
        let (status, body) = modern_tool_call(
            &build_router(ProtocolMode::Dual),
            "echo",
            json!({ "message": "hi" }),
            2,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["result"]["content"][0]["text"], "hi");
        assert_eq!(body["result"]["isError"], false);
    }

    #[tokio::test]
    async fn test_modern_tools_list_schemas() {
        let (status, body) = modern_call(
            &build_router(ProtocolMode::Dual),
            modern_request("tools/list", MCP_PROTOCOL_VERSION_MODERN, 3),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let tools = body["result"]["tools"].as_array().unwrap();
        assert_eq!(tools.len(), 9);

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
    async fn test_modern_prompts_and_resources_lists_include_cache_directives() {
        for method in ["prompts/list", "resources/list", "resources/templates/list"] {
            let (status, body) = modern_call(
                &build_router(ProtocolMode::Dual),
                modern_request(method, MCP_PROTOCOL_VERSION_MODERN, 60),
            )
            .await;
            assert_eq!(status, StatusCode::OK, "{method} should succeed");
            let result = &body["result"];
            assert_eq!(result["resultType"], "complete", "{method}");
            assert_eq!(result["cacheScope"], "private", "{method}");
            assert_eq!(result["ttlMs"], 0, "{method}");
        }
    }

    #[tokio::test]
    async fn test_modern_prompts_get_and_resources_read() {
        // prompts/get works statelessly; Mcp-Name mirrors params.name.
        let mut request = modern_request("prompts/get", MCP_PROTOCOL_VERSION_MODERN, 61);
        request["params"]["name"] = json!("convert_time");
        request["params"]["arguments"] = json!({
            "time": "2026-10-04T12:00:00Z",
            "source_timezone": "UTC",
            "target_timezone": "America/New_York"
        });
        let (status, body) = modern_call(&build_router(ProtocolMode::Dual), request).await;
        assert_eq!(status, StatusCode::OK);
        let text = body["result"]["messages"][0]["content"]["text"]
            .as_str()
            .unwrap();
        assert!(text.contains("convert_time"));
        assert!(text.contains("America/New_York"));

        // resources/read returns JSON content with the modern cache
        // directives (ttlMs 0: never cached).
        let mut request = modern_request("resources/read", MCP_PROTOCOL_VERSION_MODERN, 62);
        request["params"]["uri"] = json!("server://info");
        let (status, body) = modern_call(&build_router(ProtocolMode::Dual), request).await;
        assert_eq!(status, StatusCode::OK);
        let result = &body["result"];
        assert_eq!(result["resultType"], "complete");
        assert_eq!(result["cacheScope"], "private");
        assert_eq!(result["ttlMs"], 0);
        let contents = &result["contents"][0];
        assert_eq!(contents["uri"], "server://info");
        let info: serde_json::Value =
            serde_json::from_str(contents["text"].as_str().unwrap()).unwrap();
        assert_eq!(info["protocol_mode"], "dual");

        // At 2026-07-28 the SDK rewrites RESOURCE_NOT_FOUND to invalid params.
        let mut request = modern_request("resources/read", MCP_PROTOCOL_VERSION_MODERN, 63);
        request["params"]["uri"] = json!("config://does-not-exist");
        let (status, body) = modern_call(&build_router(ProtocolMode::Dual), request).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"]["code"], -32602);
    }

    #[tokio::test]
    async fn test_server_discover_lists_both_eras() {
        let (status, body) = modern_call(
            &build_router(ProtocolMode::Dual),
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
        assert!(result["capabilities"]["prompts"].is_object());
        assert!(result["capabilities"]["resources"].is_object());
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
            &build_router(ProtocolMode::Dual),
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
        let response = oneshot(&build_router(ProtocolMode::Dual), request).await;
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
        let (status, body) = modern_call(&build_router(ProtocolMode::Dual), request).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"]["code"], -32602);
    }

    #[tokio::test]
    async fn test_mcp_echo_rejects_delay_above_limit() {
        let (status, body) = modern_tool_call(
            &build_router(ProtocolMode::Dual),
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
        let router = build_router(ProtocolMode::Dual);
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

    // ========================================================================
    // Protocol mode selection (MCP_PROTOCOL_MODE) and cache directives
    // ========================================================================

    #[test]
    fn test_protocol_mode_parse_defaults_to_dual_and_accepts_all_modes() {
        assert_eq!(ProtocolMode::parse(None).unwrap(), ProtocolMode::Dual);
        assert_eq!(ProtocolMode::parse(Some("")).unwrap(), ProtocolMode::Dual);
        assert_eq!(
            ProtocolMode::parse(Some("legacy")).unwrap(),
            ProtocolMode::Legacy
        );
        assert_eq!(
            ProtocolMode::parse(Some(" modern ")).unwrap(),
            ProtocolMode::Modern
        );
        assert_eq!(
            ProtocolMode::parse(Some("dual")).unwrap(),
            ProtocolMode::Dual
        );
    }

    #[test]
    fn test_protocol_mode_parse_rejects_unknown_values_with_guidance() {
        let err = ProtocolMode::parse(Some("nightly"))
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("nightly"),
            "error should echo the bad value: {err}"
        );
        assert!(
            err.contains("legacy, modern, dual"),
            "error should list accepted values: {err}"
        );
    }

    #[tokio::test]
    async fn test_modern_tools_list_includes_cache_directives() {
        let (status, body) = modern_call(
            &build_router(ProtocolMode::Dual),
            modern_request("tools/list", MCP_PROTOCOL_VERSION_MODERN, 30),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let result = &body["result"];
        assert_eq!(result["resultType"], "complete");
        assert_eq!(result["cacheScope"], "private");
        assert_eq!(result["ttlMs"], 0);
    }

    #[tokio::test]
    async fn test_legacy_tools_list_omits_cache_directives() {
        let router = build_router(ProtocolMode::Dual);
        let session_id = initialize_session(&router).await;
        let mut list = mcp_post(json!({
            "jsonrpc": "2.0",
            "method": "tools/list",
            "id": 31
        }));
        list.headers_mut()
            .insert(SESSION_HEADER, HeaderValue::from_str(&session_id).unwrap());
        let response = oneshot(&router, list).await;
        assert_eq!(response.status(), StatusCode::OK);
        let body = parse_sse_json(&response_text(response).await);
        let result = &body["result"];
        assert!(result["tools"].is_array());
        assert!(
            result.get("cacheScope").is_none(),
            "legacy era must not emit cacheScope"
        );
        assert!(
            result.get("ttlMs").is_none(),
            "legacy era must not emit ttlMs"
        );
    }

    #[tokio::test]
    async fn test_legacy_mode_serves_only_the_legacy_era() {
        let router = build_router(ProtocolMode::Legacy);

        let session_id = initialize_session(&router).await;
        let body = legacy_tool_call(&router, &session_id, "verify-protocol", json!({}), 40).await;
        assert_eq!(
            body["result"]["structuredContent"]["protocolVersion"],
            MCP_PROTOCOL_VERSION
        );

        let (status, body) =
            modern_tool_call(&router, "echo", json!({ "message": "hi" }), 41).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"]["code"], -32022);
        assert_eq!(
            body["error"]["data"]["supported"],
            json!([MCP_PROTOCOL_VERSION])
        );
    }

    #[tokio::test]
    async fn test_modern_mode_serves_only_the_modern_era() {
        let router = build_router(ProtocolMode::Modern);

        let (status, body) =
            modern_tool_call(&router, "echo", json!({ "message": "hi" }), 42).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["result"]["content"][0]["text"], "hi");

        // The legacy handshake must be rejected: 2025-11-25 is a known
        // revision this mode does not serve.
        let response = oneshot(&router, mcp_post(initialize_request(MCP_PROTOCOL_VERSION))).await;
        let text = response_text(response).await;
        let body = serde_json::from_str(&text).unwrap_or_else(|_| parse_sse_json(&text));
        assert_eq!(body["error"]["code"], -32022);
        assert_eq!(
            body["error"]["data"]["supported"],
            json!([MCP_PROTOCOL_VERSION_MODERN])
        );
        let response = oneshot(&router, mcp_post(initialize_request(MCP_PROTOCOL_VERSION))).await;
        let body = parse_sse_json(&response_text(response).await);
        assert_ne!(body["result"]["protocolVersion"], MCP_PROTOCOL_VERSION);
    }

    #[tokio::test]
    async fn test_version_endpoint_reflects_protocol_mode() {
        for (mode, expected) in [
            (ProtocolMode::Legacy, json!([MCP_PROTOCOL_VERSION])),
            (ProtocolMode::Modern, json!([MCP_PROTOCOL_VERSION_MODERN])),
            (
                ProtocolMode::Dual,
                json!([MCP_PROTOCOL_VERSION, MCP_PROTOCOL_VERSION_MODERN]),
            ),
        ] {
            let response = oneshot(
                &build_router(mode),
                Request::builder()
                    .method("GET")
                    .uri("http://localhost/version")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await;
            assert_eq!(response.status(), StatusCode::OK);
            let body: serde_json::Value =
                serde_json::from_str(&response_text(response).await).unwrap();
            assert_eq!(body["mcp_versions"], expected);
            assert_eq!(body["protocol_mode"], mode.as_str());
        }
    }
}
