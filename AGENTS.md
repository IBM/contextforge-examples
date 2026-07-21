# AGENTS.md

Guidance for AI coding agents working in this repository.

## Repository Overview

`contextforge-examples` contains lightly supported example assets for
ContextForge: MCP servers (e.g. `mcp-servers/rust/fast-time-server`), plugins,
and related tooling. Each example is self-contained — Rust servers under
`mcp-servers/rust/` are standalone Cargo workspaces, not part of a repo-wide
workspace.

## Commits: Always Sign Off

**Every commit in this repository MUST be signed off.** Always pass
`--signoff` (or `-s`) when committing:

```bash
git commit --signoff -m "Your commit message"
```

This appends a `Signed-off-by: Name <email>` trailer certifying the
Developer Certificate of Origin (see [DCO.txt](DCO.txt) and
[CONTRIBUTING.md](CONTRIBUTING.md#legal)). Never amend away or drop existing
sign-off trailers. If you create a commit without a sign-off, fix it
immediately:

```bash
git commit --amend --signoff --no-edit
```

## Working in This Repo

- Keep changes scoped to the example you are modifying; examples are
  independent of one another.
- For Rust servers: `cargo fmt`, `cargo clippy --all-targets`, and
  `cargo test` must all pass before committing.
- Match the existing plain, imperative commit-message style
  (e.g. "Add flaky tool to fast-time-server for retry testing").
- Do not commit build artifacts (`target/`, `node_modules/`) or agent state
  directories (`.omo/`, `.bob/` — already gitignored).
