# Bobshell Container

[IBM Bob Shell](https://bob.ibm.com/docs/shell) (the `bob` agentic CLI) packaged
as a container image on `registry.access.redhat.com/ubi10/nodejs-24`.

- Bob Shell installed via the [official installer](https://bob.ibm.com/docs/shell/getting-started/install-and-setup)
  (SHA256-verified, `npm install -g`).
- IBM license pre-accepted and `/workspace` pre-trusted at image build time, so
  `bob` never blocks on first-run prompts (see
  [trusted folders](https://bob.ibm.com/docs/shell/security/trusted-folders)).
- Runs as the non-root default user (UID 1001).
- Defaults to an interactive `bob chat` session when the container starts.

## Configuration

The container accepts two configurations:

| Configuration | Mechanism | Container location | Effect |
| ------------- | --------- | ------------------ | ------ |
| Bobshell API secret | Compose `secrets:` (or `BOB_API_KEY` env var) | `/run/secrets/bob_api_key` | Exported as `BOB_API_KEY`; used for [API key authentication](https://bob.ibm.com/docs/shell/getting-started/install-and-setup#api-key-authentication) |
| MCP configuration | Compose `configs:` | `/etc/bob/mcp.json` | Installed as `~/.bob/settings/mcp.json` (user-global [`mcpServers`](https://bob.ibm.com/docs/shell/configuration/mcp/mcp-bobshell) config) unless one already exists |

An API key (scope **Inference**) is created in the
[Bob web portal](https://bob.ibm.com/login).

## Quick start (Docker Compose)

```bash
cd infra/bobshell

# Provide the API secret and a project directory
mkdir -p secrets workspace
printf '%s' 'your-bob-api-key' > secrets/bob_api_key.txt
# Change permission on the secrets directory:
chmod 700 secrets

# Change permissions on the secrets file:
chmod 600 secrets/bob_api_key.txt
# Edit the sample MCP configuration to taste
$EDITOR mcp.json

# Interactive session (image is pulled or built locally)
docker compose run --rm bobshell

# Headless task
docker compose run --rm bobshell bob run --accept-license --trust "Explain this project"

# Build locally instead of pulling the published image
docker compose build
```

`docker-compose.yml` mounts `./workspace` at `/workspace` (the pre-trusted
working directory) and persists `~/.bob` (sessions, tasks, settings) in the
`bob-state` named volume.

## Plain Docker

```bash
docker build -f Containerfile -t bobshell .

# Interactive
docker run --rm -it \
  -e BOB_API_KEY="your-bob-api-key" \
  -v "$PWD/workspace:/workspace" \
  bobshell

# Headless, with MCP config mounted
docker run --rm \
  -e BOB_API_KEY="your-bob-api-key" \
  -v "$PWD/mcp.json:/etc/bob/mcp.json:ro" \
  -v "$PWD/workspace:/workspace" \
  bobshell bob run --accept-license --trust "Summarize this repo"
```

Any command after the image name replaces the default `bob chat` invocation;
a leading flag (e.g. `--version`) is passed to `bob` itself.

## Version pinning

The image installs the latest Bob Shell release by default. Pin a version at
build time:

```bash
docker build --build-arg BOB_VERSION=2.0.5 -t bobshell .
```

## Publishing (CI)

`.github/workflows/build-bobshell.yml` builds `linux/amd64` and `linux/arm64`
images and publishes a multi-arch manifest to
`ghcr.io/ibm/cfex-bobshell` on:

- pushes to `main` touching `infra/bobshell/**` (tagged with the commit SHA and `latest`),
- **release publication** (additionally tagged with the release tag),
- manual `workflow_dispatch`.

## Security notes

- Never commit `secrets/` or set your API key in any tracked file; the local
  `.gitignore` excludes them, and `.dockerignore` keeps them out of the build
  context.
- The pre-trust entry covers `/workspace` only; Bob Shell's folder-trust
  protections remain intact for everything else.
- Run the container with least privilege: it needs no host access beyond the
  mounted workspace and outbound network to `bob.ibm.com`.
