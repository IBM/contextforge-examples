# Fast Time Server (Rust)

> Author: Mihai Criveti

Ultra-fast MCP server written in Rust for performance testing and benchmarking. Built on the official [MCP Rust SDK](https://github.com/modelcontextprotocol/rust-sdk) (`rmcp`) with axum.

## Features

- **Blazing fast**: Native Rust performance with zero-copy where possible
- **Streamable HTTP**: MCP Streamable HTTP transport served by the SDK's `StreamableHttpService`
- **Dual-era MCP**: Legacy `2025-11-25` (initialize handshake + `mcp-session-id` sessions) and modern `2026-07-28` (stateless, per-request `_meta`) are served simultaneously on the same `/mcp` endpoint; `MCP_PROTOCOL_MODE=legacy|modern` restricts the server to a single era
- **Minimal overhead**: No auth, no database, pure compute
- **Tools**:
  - `echo` - Echoes back the provided message (with optional delay/jitter)
  - `flaky` - Fails N times per key before succeeding (retry testing)
  - `get_system_time` - Returns current time in specified timezone
  - `convert_time` - Converts a time between IANA timezones
  - `schema_error` / `schema_success` - Output-schema validation fixtures
  - `get_stats` - Returns server statistics
  - `verify-protocol` - Reports the MCP protocol version active for the current request
  - `whoami` - Reflects the HTTP headers received with the tool call as a lowercased JSON map

- **Prompts** (each seeds a conversation that drives the tools above):
  - `current_time` - Ask for the current time in a timezone (`get_system_time`)
  - `convert_time` - Convert a time between timezones (`convert_time`)
  - `server_diagnostics` - Health report from `get_stats` + `verify-protocol`
- **Resources** (mirroring the same surface):
  - `config://timezones` - The timezone formats every time entry point accepts
  - `server://info` - Server identity and protocol versions (mirrors `/version`)
  - `server://stats` - Live request counter (mirrors `get_stats`)
  - `time://now/{+timezone}` - Resource template mirroring `get_system_time` (RFC 6570 reserved expansion, so IANA names and `+HH:MM` offsets stay unencoded)
## Quick Start

```bash
# Build and run
make run

# Or release build for benchmarking
make run-release
```

Server starts at `http://localhost:9080/mcp`

## Testing

```bash
# List available tools
make test-tools

# Test echo
make test-echo

# Test time
make test-time
```

Or with curl, legacy era (`2025-11-25`): initialize a session, then send
requests with the `mcp-session-id` header. Session-mode POST responses are
`text/event-stream` (SSE); the JSON-RPC message rides in the `data:` line.

```bash
# Initialize session (response is SSE; session id comes back in a header)
curl -i -X POST http://localhost:9080/mcp \
  -H 'Content-Type: application/json' \
  -H 'Accept: application/json, text/event-stream' \
  -d '{"jsonrpc":"2.0","method":"initialize","params":{"protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"test","version":"1.0"}},"id":1}'

# Call echo tool (substitute the mcp-session-id from the initialize response)
curl -X POST http://localhost:9080/mcp \
  -H 'Content-Type: application/json' \
  -H 'Accept: application/json, text/event-stream' \
  -H 'mcp-session-id: <session-id>' \
  -d '{"jsonrpc":"2.0","method":"tools/call","params":{"name":"echo","arguments":{"message":"Hello!"}},"id":2}'

# Terminate the session
curl -X DELETE http://localhost:9080/mcp -H 'mcp-session-id: <session-id>'
```

### Modern Protocol (2026-07-28)

Modern requests are stateless: no handshake, no session. The protocol version
travels in `params._meta` plus the `MCP-Protocol-Version` header (the two must
agree), and every request must also mirror its method in the `Mcp-Method`
header — and its tool/prompt name in `Mcp-Name` for named methods — per the
2026-07-28 standard-headers rule (SEP-2243). Responses are plain
`application/json`.

```bash
# Discover supported versions and capabilities (no session needed)
curl -X POST http://localhost:9080/mcp \
  -H 'Content-Type: application/json' \
  -H 'Accept: application/json, text/event-stream' \
  -H 'MCP-Protocol-Version: 2026-07-28' \
  -H 'Mcp-Method: server/discover' \
  -d '{"jsonrpc":"2.0","method":"server/discover","params":{"_meta":{"io.modelcontextprotocol/protocolVersion":"2026-07-28","io.modelcontextprotocol/clientCapabilities":{}}},"id":1}'

# Call a tool directly - no initialize, no session
curl -X POST http://localhost:9080/mcp \
  -H 'Content-Type: application/json' \
  -H 'Accept: application/json, text/event-stream' \
  -H 'MCP-Protocol-Version: 2026-07-28' \
  -H 'Mcp-Method: tools/call' \
  -H 'Mcp-Name: echo' \
  -d '{"jsonrpc":"2.0","method":"tools/call","params":{"name":"echo","arguments":{"message":"Hello!"},"_meta":{"io.modelcontextprotocol/protocolVersion":"2026-07-28","io.modelcontextprotocol/clientCapabilities":{}}},"id":2}'
```

A request for an unsupported version is rejected with HTTP 400 and an
`UnsupportedProtocolVersionError` (`-32022`) whose `data.supported` lists
exactly the two served eras (`2025-11-25`, `2026-07-28`). A mismatching
`MCP-Protocol-Version` header is rejected with `HeaderMismatch` (`-32020`).

### verify-protocol

The `verify-protocol` tool reports which era served the current request. It
returns both text content and structured content:

- Modern (stateless) requests: the version comes from the request's own
  `_meta` → `{"protocolVersion": "2026-07-28", "transport": "stateless"}`
- Legacy (session) requests: the version is the one negotiated at `initialize`
  → `{"protocolVersion": "2025-11-25", "transport": "session"}`

### whoami

The `whoami` tool reflects the HTTP headers of the tool-call request so
header-affecting gateway plugins (e.g. Vault `tool_pre_invoke`) can assert
what the upstream actually received. It returns both text content and
structured content: a JSON object mapping each received header name
(lowercased) to its value. The `authorization` key is always present — `null`
when the header was absent — and header values appear only in the tool
response, never in server logs. Example:

```json
{
  "authorization": "Bearer <vault-token>",
  "content-type": "application/json",
  "mcp-session-id": "…"
}
```

## Prompts

Each prompt renders a user message that asks the model to drive one of this
server's own tools, with the same arguments the tools accept:

| Name | Arguments | Drives |
|------|-----------|-------|
| `current_time` | `timezone` (optional, default `UTC`) | `get_system_time` |
| `convert_time` | `time`, `source_timezone`, `target_timezone` (all required) | `convert_time` |
| `server_diagnostics` | none | `get_stats` + `verify-protocol` |

Modern era (`2026-07-28`, stateless — note `Mcp-Name` carries the prompt name):

```bash
curl -s -X POST http://localhost:9080/mcp \
  -H 'Content-Type: application/json' \
  -H 'Accept: application/json, text/event-stream' \
  -H 'MCP-Protocol-Version: 2026-07-28' \
  -H 'Mcp-Method: prompts/get' \
  -H 'Mcp-Name: current_time' \
  -d '{"jsonrpc":"2.0","method":"prompts/get","params":{"name":"current_time","arguments":{"timezone":"Asia/Tokyo"},"_meta":{"io.modelcontextprotocol/protocolVersion":"2026-07-28","io.modelcontextprotocol/clientCapabilities":{}}},"id":1}'
```

On the legacy era the same request is sent with an `mcp-session-id` header
after `initialize`, exactly like `tools/call`.

## Resources

Resources mirror the server's existing surface so tool and resource clients
see the same data:

| URI | MIME type | Mirrors |
|-----|-----------|---------|
| `config://timezones` | `text/plain` | The timezone formats `parse_timezone` accepts |
| `server://info` | `application/json` | The `/version` REST endpoint |
| `server://stats` | `application/json` | The `get_stats` tool |
| `time://now/{+timezone}` (template) | `text/plain` | The `get_system_time` tool and `/api/time` |

`resources/read` resolves template URIs exactly like `get_system_time`:
unknown URIs fail with `RESOURCE_NOT_FOUND` on the legacy era (rewritten to
invalid params at `2026-07-28`), and a bad timezone reports the same
"Invalid timezone" wording as the tools.

```bash
# List resources and templates (stateless, 2026-07-28)
curl -s -X POST http://localhost:9080/mcp \
  -H 'Content-Type: application/json' \
  -H 'Accept: application/json, text/event-stream' \
  -H 'MCP-Protocol-Version: 2026-07-28' \
  -H 'Mcp-Method: resources/list' \
  -d '{"jsonrpc":"2.0","method":"resources/list","params":{"_meta":{"io.modelcontextprotocol/protocolVersion":"2026-07-28","io.modelcontextprotocol/clientCapabilities":{}}},"id":1}'

# Read a template instance (Mcp-Name carries the URI for resources/read)
curl -s -X POST http://localhost:9080/mcp \
  -H 'Content-Type: application/json' \
  -H 'Accept: application/json, text/event-stream' \
  -H 'MCP-Protocol-Version: 2026-07-28' \
  -H 'Mcp-Method: resources/read' \
  -H 'Mcp-Name: time://now/Asia/Tokyo' \
  -d '{"jsonrpc":"2.0","method":"resources/read","params":{"uri":"time://now/Asia/Tokyo","_meta":{"io.modelcontextprotocol/protocolVersion":"2026-07-28","io.modelcontextprotocol/clientCapabilities":{}}},"id":2}'
```

### 2026-07-28 conformance

Prompts and resources follow the same wire rules the tools already use:

- `prompts/list`, `resources/list` and `resources/templates/list` responses
  carry `resultType: "complete"` plus the SEP-2549 cache directives
  (`cacheScope: "private"`, `ttlMs: 0`) on the modern era, and omit all
  three on legacy sessions.
- `resources/read` responses carry the same directives with `ttlMs: 0`
  (nothing this server returns is safe to cache: the time and stats
  resources are live values).
- Both capabilities are advertised in `initialize` (legacy) and
  `server/discover` (modern).

### SSE Streaming

The `/mcp` endpoint itself speaks SSE — there is no separate `/sse` endpoint:

- Legacy session POST responses (including `initialize`) are SSE streams
  carrying the JSON-RPC response, so the server can interleave progress and
  other notifications with the result.
- `GET /mcp` with `Accept: text/event-stream` and a valid `mcp-session-id`
  opens a standalone stream for server-initiated messages; `Last-Event-ID`
  resumes a broken stream.
- Modern stateless requests return plain `application/json` responses (the
  server is configured with `json_response`), falling back to SSE only if a
  handler emits intermediate messages.

## Benchmarking

The server includes REST API endpoints that bypass MCP session overhead for accurate benchmarking:

```bash
# Install hey
go install github.com/rakyll/hey@latest

# Run full benchmark (1M requests, 200 concurrent)
make bench

# Quick benchmark (100K requests)
make bench-quick

# Individual endpoints
make bench-echo   # POST /api/echo
make bench-time   # GET /api/time
```

### Benchmark Results (REST API with hey)

On a typical development machine:

| Endpoint | Requests/sec | Latency p99 |
|----------|-------------|-------------|
| `/api/echo` | ~175,000 | 6ms |
| `/api/time` | ~181,000 | 6ms |

## Locust Load Testing (MCP Protocol)

For proper MCP protocol testing with session management, use Locust:

```bash
# Install locust
pip install locust

# Start the server
make run-release

# In another terminal - Web UI (recommended)
make locust-ui
# Open http://localhost:8089, select user classes

# Headless test (100 users, 60s)
make locust

# Stress test (500 users, 120s)
make locust-stress

# Compare MCP vs REST performance
make locust-compare
```

### User Classes

| Class | Weight | Description |
|-------|--------|-------------|
| `RustMCPUser` | 10 | MCP protocol via Streamable HTTP |
| `RustMCPStressUser` | 1 | High-frequency MCP stress test |
| `RustRESTUser` | 5 | REST API baseline comparison |

## Docker

```bash
# Build image
make docker-build

# Run container
make docker-run
```

## Endpoints

### REST API (for benchmarking)

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/echo` | POST | Echo `{"message":"..."}` - pure performance test |
| `/api/time` | GET | Get time, optional `?tz=America/New_York` |
| `/health` | GET | Health check |
| `/version` | GET | Version info and supported MCP protocol versions |

### MCP Protocol

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/mcp` | POST | MCP JSON-RPC. Legacy (`2025-11-25`): `initialize` handshake + `mcp-session-id` sessions, SSE responses. Modern (`2026-07-28`): stateless requests with version in `params._meta` + `MCP-Protocol-Version`/`Mcp-Method` headers, JSON responses, including `server/discover` |
| `/mcp` | GET | Open a standalone SSE stream for a legacy session (resume with `Last-Event-ID`) |
| `/mcp` | DELETE | Terminate a legacy session |

## Environment Variables


| Variable | Default | Description |
|----------|---------|-------------|
| `BIND_ADDRESS` | `0.0.0.0:9080` | Address to bind to |
| `RUST_LOG` | `info` | Log level (trace, debug, info, warn, error) |
| `MCP_PROTOCOL_MODE` | `dual` | MCP era(s) to serve: `legacy` (2025-11-25 only), `modern` (2026-07-28 only), or `dual` (both). Any other value fails startup with an error listing the accepted values |

The `MCP_PROTOCOL_MODE` variable selects which MCP revision(s) the server
speaks, without rebuilding or editing the entrypoint:

| `MCP_PROTOCOL_MODE` | Serves | Behavior |
|---------------------|--------|----------|
| `legacy` | 2025-11-25 only | `initialize` handshake + sessions; modern stateless requests are rejected with `-32022` |
| `modern` | 2026-07-28 only | Stateless per-request era; `initialize` proposing 2025-11-25 is rejected with `-32022` |
| `dual` (default) | both | Legacy and modern served simultaneously, as documented above |

The published container image sets `ENV MCP_PROTOCOL_MODE=dual`, so it serves
both eras out of the box; operators can restrict it with
`docker run -e MCP_PROTOCOL_MODE=modern ...`.

## Supported Timezones

The `get_system_time` tool supports:

- UTC, GMT
- IANA timezone names (e.g., `America/New_York`, `Europe/London`, `Asia/Tokyo`)
- Fixed offsets (e.g., `+05:30`, `-08:00`)

## Comparison with Go Server

This server is designed to be compared with the Go `fast-time-server` for benchmarking purposes. Both implement similar functionality with the same transport (streamable HTTP).

## License

Apache-2.0
