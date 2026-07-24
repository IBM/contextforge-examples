use anyhow::{Context, Result};
use clap::Parser;
use filesystem_server::{
    DEFAULT_BIND_ADDRESS, build_router, init_tracing, print_startup_banner, resolve_bind_address,
};
use tokio::net::TcpListener;

#[derive(Parser, Debug)]
struct Args {
    #[arg(long = "roots", value_delimiter = ' ')]
    roots: Vec<String>,

    /// IP address and port the HTTP server binds to. Non-loopback addresses
    /// require --auth-token.
    #[arg(long = "bind", default_value = DEFAULT_BIND_ADDRESS)]
    bind: String,

    /// Bearer token clients must send in the Authorization header. Required
    /// when binding to a non-loopback address.
    #[arg(long = "auth-token", env = "FILESYSTEM_SERVER_AUTH_TOKEN")]
    auth_token: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    init_tracing();

    let bind_addr = resolve_bind_address(&args.bind, args.auth_token.is_some())?;
    let auth_enabled = args.auth_token.is_some();
    let roots = args.roots.clone();
    let router = build_router(args.roots, args.auth_token).await?;
    print_startup_banner(&roots, &bind_addr, auth_enabled);

    let listener = TcpListener::bind(bind_addr)
        .await
        .context("Failed to bind to port")?;

    axum::serve(listener, router)
        .with_graceful_shutdown(async {
            tokio::signal::ctrl_c().await.unwrap();
        })
        .await?;

    Ok(())
}
