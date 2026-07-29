# Fast Time Server (Rust)

> Author: Mihai Criveti

Ultra-fast MCP server written in Rust for performance testing and benchmarking. Built on the official [MCP Rust SDK](https://github.com/modelcontextprotocol/rust-sdk) (`rmcp`) with axum.

## Features

- **Blazing fast**: Native Rust performance with zero-copy where possible
- **Streamable HTTP**: MCP Streamable HTTP transport served by the SDK's `StreamableHttpService`
- **Dual-era MCP**: Legacy `2025-11-25` (initialize handshake + `mcp-session-id` sessions) and modern `2026-07-28` (stateless, per-request `_meta`) are served simultaneously on the same `/mcp` endpoint — no flags, no modes
- **Minimal overhead**: No auth, no database, pure compute
- **Tools**:
  - `echo` - Echoes back the provided message (with optional delay/jitter)
  - `flaky` - Fails N times per key before succeeding (retry testing)
  - `get_system_time` - Returns current time in specified timezone
  - `convert_time` - Converts a time between IANA timezones
  - `schema_error` / `schema_success` - Output-schema validation fixtures
  - `get_stats` - Returns server statistics
  - `verify-protocol` - Reports the MCP protocol version active for the current request

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

## Supported Timezones

The `get_system_time` tool supports:

- UTC, GMT
- IANA timezone names (e.g., `America/New_York`, `Europe/London`, `Asia/Tokyo`)
- Fixed offsets (e.g., `+05:30`, `-08:00`)

## Comparison with Go Server

This server is designed to be compared with the Go `fast-time-server` for benchmarking purposes. Both implement similar functionality with the same transport (streamable HTTP).

## License

Apache-2.0
