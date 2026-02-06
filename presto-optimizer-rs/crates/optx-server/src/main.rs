//! # optx-server: HTTP Service for the Cascades Query Optimizer
//!
//! This binary crate provides an HTTP server that exposes the Cascades optimizer
//! as a network service. It is designed to be called by the Presto Java coordinator
//! during query planning, enabling Rust-based optimization without JNI.
//!
//! ## Architecture
//!
//! ```text
//! Presto Java Coordinator
//!   |
//!   | HTTP POST /optimize (Substrait protobuf)
//!   v
//! optx-server (this binary)
//!   |
//!   +-> Substrait consumer (deserialize plan into memo)
//!   +-> Cascades search (explore + implement + cost)
//!   +-> Substrait producer (serialize optimized plan)
//!   |
//!   | HTTP response (optimized Substrait protobuf)
//!   v
//! Presto Java Coordinator
//! ```
//!
//! ## Endpoints
//!
//! - `GET  /health`                - Health check
//! - `GET  /rules`                 - List active optimization rules
//! - `POST /optimize`              - Optimize a Substrait plan (protobuf binary)
//! - `POST /optimize/json`         - Optimize a Substrait plan (JSON encoding)
//! - `POST /optimize/join-graph`   - Optimize join ordering via join-graph JSON protocol
//! - `POST /rules/configure`       - Enable/disable rules (placeholder)
//!
//! ## Configuration
//!
//! The server listens on `0.0.0.0:3000` by default. Logging is controlled by the
//! `RUST_LOG` environment variable (defaults to `optx=debug`).

mod join_graph;
mod routes;
mod state;

use axum::routing::{get, post};
use axum::Router;
use std::sync::Arc;
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() {
    // Initialize structured logging with tracing. The default filter shows debug-level
    // messages from the optx crates; override with RUST_LOG env var for more/less detail.
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("optx=debug".parse().unwrap()))
        .init();

    // Create shared application state (rule registry, cost model, catalog).
    // This is shared across all request handlers via Arc.
    let state = Arc::new(state::AppState::new());

    // Build the Axum router with all endpoints.
    let app = Router::new()
        .route("/health", get(routes::health))
        .route("/rules", get(routes::list_rules))
        .route("/optimize", post(routes::optimize_proto))
        .route("/optimize/json", post(routes::optimize_json))
        .route("/optimize/join-graph", post(join_graph::optimize_join_graph))
        .route("/rules/configure", post(routes::configure_rules))
        .layer(CorsLayer::permissive()) // Allow cross-origin requests (for dev/debug UIs)
        .layer(TraceLayer::new_for_http()) // Log all HTTP requests
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    tracing::info!("optx-server listening on http://0.0.0.0:3000");
    axum::serve(listener, app).await.unwrap();
}
