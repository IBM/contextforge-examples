#!/bin/sh
# Bobshell container entrypoint.
#
# Wires up the two supported configurations, both provided through Docker
# Compose mechanisms:
#   secrets: /run/secrets/bob_api_key -> exported as BOB_API_KEY
#   configs: /etc/bob/mcp.json        -> ~/.bob/settings/mcp.json (user-global)
set -eu

BOB_STATE_DIR="${HOME}/.bob"

# API key: an explicit BOB_API_KEY env var wins; otherwise read the mounted
# Docker secret. Whitespace/newlines around the key are stripped.
if [ -z "${BOB_API_KEY:-}" ] && [ -r /run/secrets/bob_api_key ]; then
    BOB_API_KEY="$(tr -d '[:space:]' < /run/secrets/bob_api_key)"
    export BOB_API_KEY
fi

# MCP servers: seed the user-global config from the mounted Docker config,
# unless one already exists (e.g. a persisted state volume or an image
# baked with its own mcp.json).
if [ -r /etc/bob/mcp.json ] && [ ! -e "${BOB_STATE_DIR}/settings/mcp.json" ]; then
    mkdir -p "${BOB_STATE_DIR}/settings"
    cp /etc/bob/mcp.json "${BOB_STATE_DIR}/settings/mcp.json"
fi

# Default: interactive Bob Shell session. The flags keep a fresh container
# (e.g. empty volume over $HOME) non-interactive: accept the IBM license and
# mark the current folder trusted.
if [ "$#" -eq 0 ]; then
    set -- bob chat --accept-license --trust
fi

# Allow `docker run <image> --version`-style invocations: a leading flag
# means the user wants bob itself with those flags.
if [ "${1#-}" != "$1" ]; then
    set -- bob "$@"
fi

exec "$@"
