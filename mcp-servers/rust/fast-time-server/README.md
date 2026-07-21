# Fast Time Server (Rust)

> Author: Mihai Criveti

Ultra-fast MCP server written in Rust for performance testing and benchmarking. Hand-rolled on axum with no SDK in the hot path.

## Features

- **Blazing fast**: Native Rust performance with zero-copy where possible
- **Streamable HTTP**: Modern HTTP transport with streaming support
- **Dual-era MCP**: Serves legacy `2025-11-25` (initialize handshake + sessions) by default, and optionally the modern `2026-07-28` revision (stateless, per-request `_meta`) via `--protocol` — see [Command-Line Flags](#command-line-flags)
- **Minimal overhead**: No auth, no database, pure compute
- **Tools**:
  - `echo` - Echoes back the provided message (with optional delay/jitter)
  - `flaky` - Fails N times per key before succeeding (retry testing)
  - `get_system_time` - Returns current time in specified timezone
  - `convert_time` - Converts a time between IANA timezones
  - `schema_error` / `schema_success` - Output-schema validation fixtures
  - `get_stats` - Returns server statistics

## Quick Start

```bash
# Build and run (legacy 2025-11-25 only)
make run

# Or release build for benchmarking
make run-release

# Also serve the modern 2026-07-28 revision
cargo run -- --protocol 2026-07-28

# Reject version fallback during initialize (strict negotiation)
cargo run -- --protocol 2026-07-28 --strict
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

Or with curl:

```bash
# Initialize session
SESSION_RESPONSE=$(curl -s -X POST http://localhost:9080/mcp \
  -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"test","version":"1.0"}},"id":1}')

# Extract session ID from response header (if using httpie or similar)
# Or parse from mcp-session-id header

# List tools
curl -X POST http://localhost:9080/mcp \
  -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","method":"tools/list","id":1}'

# Call echo tool
curl -X POST http://localhost:9080/mcp \
  -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","method":"tools/call","params":{"name":"echo","arguments":{"message":"Hello!"}},"id":1}'

# Call get_system_time tool
curl -X POST http://localhost:9080/mcp \
  -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","method":"tools/call","params":{"name":"get_system_time","arguments":{"timezone":"America/New_York"}},"id":1}'
```

### Modern Protocol (2026-07-28)

Start the server with `--protocol 2026-07-28` and skip the handshake entirely —
modern requests are stateless and carry their protocol version in
`params._meta` (plus the `MCP-Protocol-Version` header):

```bash
# Discover supported versions and capabilities (no session needed)
curl -X POST http://localhost:9080/mcp \
  -H 'Content-Type: application/json' \
  -H 'MCP-Protocol-Version: 2026-07-28' \
  -d '{"jsonrpc":"2.0","method":"server/discover","params":{"_meta":{"io.modelcontextprotocol/protocolVersion":"2026-07-28","io.modelcontextprotocol/clientCapabilities":{}}},"id":1}'

# Call a tool directly - no initialize, no session
curl -X POST http://localhost:9080/mcp \
  -H 'Content-Type: application/json' \
  -H 'MCP-Protocol-Version: 2026-07-28' \
  -d '{"jsonrpc":"2.0","method":"tools/call","params":{"name":"echo","arguments":{"message":"Hello!"},"_meta":{"io.modelcontextprotocol/protocolVersion":"2026-07-28","io.modelcontextprotocol/clientCapabilities":{}}},"id":2}'
```

A request for an unsupported version is rejected with HTTP 400 and an
`UnsupportedProtocolVersionError` (`-32022`) whose `data.supported` lists the
versions the server speaks.


### SSE Streaming Transport

The server supports Server-Sent Events (SSE) for streaming MCP protocol messages. Per the MCP SSE specification, clients connect to the SSE endpoint first to receive the POST endpoint URL, then initialize the session:

```bash
# Step 1: Connect to SSE endpoint (no session required)
curl -N http://localhost:9080/sse

# Expected output:
# event: endpoint
# data: /mcp
#
# : (keep-alive comments every 15 seconds)

# Step 2: Initialize session via the endpoint from SSE
curl -X POST http://localhost:9080/mcp \
  -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"test","version":"1.0"}},"id":1}'

# Response includes mcp-session-id header for subsequent requests
```

The SSE endpoint immediately sends an "endpoint" event with the POST endpoint URL (`/mcp`), then maintains the connection with periodic keep-alive comments.

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
| `/version` | GET | Version info, supported MCP protocol versions, strict mode |

### MCP Protocol

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/mcp` | POST | MCP JSON-RPC. Legacy (`2025-11-25`): `initialize` handshake + `mcp-session-id` sessions. Modern (`2026-07-28`, if enabled): stateless requests with version in `params._meta`, including `server/discover` |
| `/mcp` | DELETE | Terminate a legacy session |
| `/sse` | GET | Server-Sent Events streaming transport |

## Command-Line Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--protocol <VERSION>` | `2025-11-25` only | Also serve the given MCP protocol revision. Supported: `2025-11-25`, `2026-07-28`. May be repeated. |
| `--strict` | off | Serve exactly the revisions named with `--protocol` and reject any non-conformant interaction — no fallback, and no `initialize` handshake at all unless `2025-11-25` was explicitly enabled. |

Without arguments the server speaks only the legacy `2025-11-25` revision
(`initialize` handshake + sessions). With `--protocol 2026-07-28` it becomes
dual-era: legacy `initialize` traffic is served as before, and requests that
carry `io.modelcontextprotocol/protocolVersion` in `params._meta` are served
statelessly per the modern revision, including the mandatory `server/discover`
method. Unsupported modern versions are rejected with HTTP 400 and an
`UnsupportedProtocolVersionError` (`-32022`) listing the supported versions;
a mismatching `MCP-Protocol-Version` header is rejected with `HeaderMismatch`
(`-32020`).

`--strict` makes the served set exact. `--protocol 2026-07-28 --strict` runs a
pure `2026-07-28` server: every `initialize` call — including ones naming
`2025-11-25` — is rejected with JSON-RPC error `-32602` whose `data.supported`
lists only the configured revisions, and `server/discover` advertises only
`2026-07-28`. To run a strict server that still accepts the legacy handshake,
enable both revisions explicitly:
`--protocol 2025-11-25 --protocol 2026-07-28 --strict`.

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
