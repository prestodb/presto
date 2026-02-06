//! # HTTP Route Handlers
//!
//! This module defines the Axum route handlers for the optimizer service.
//!
//! ## Optimization Pipeline
//!
//! The core optimization flow (`run_optimization`) is shared between the protobuf
//! and JSON endpoints:
//!
//! 1. **Consume**: Deserialize the Substrait plan into a Memo.
//! 2. **Configure**: Set up the Cascades search with rules, cost model, and catalog.
//! 3. **Optimize**: Run the two-phase Cascades search (explore + implement).
//! 4. **Produce**: Serialize the optimized PlanNode back to a Substrait plan.
//!
//! ## Error Handling
//!
//! Errors are returned as HTTP status codes with descriptive messages:
//! - 400 Bad Request: malformed input (invalid protobuf, unsupported Substrait features)
//! - 500 Internal Server Error: optimization failure (no valid plan found, encoding error)

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use prost::Message;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use substrait::proto;

use optx_core::memo::Memo;
use optx_core::properties::PhysicalPropertySet;
use optx_core::search::{CascadesSearch, SearchConfig};
use optx_substrait::{consumer, producer};

use crate::state::AppState;

/// GET /health
pub async fn health() -> impl IntoResponse {
    Json(HealthResponse {
        status: "ok".to_string(),
    })
}

#[derive(Serialize)]
pub struct HealthResponse {
    pub status: String,
}

/// GET /rules — list active rules.
pub async fn list_rules(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let rules: Vec<RuleInfo> = state
        .rule_registry
        .active_rules(state.config.source_type.as_deref())
        .iter()
        .map(|r| RuleInfo {
            name: r.name().to_string(),
            rule_type: format!("{:?}", r.rule_type()),
        })
        .collect();

    Json(RulesResponse { rules })
}

#[derive(Serialize)]
pub struct RulesResponse {
    pub rules: Vec<RuleInfo>,
}

#[derive(Serialize)]
pub struct RuleInfo {
    pub name: String,
    pub rule_type: String,
}

/// POST /optimize — accept a Substrait plan (protobuf), optimize it, return optimized plan.
pub async fn optimize_proto(
    State(state): State<Arc<AppState>>,
    body: axum::body::Bytes,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let plan = proto::Plan::decode(&body[..])
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("Invalid protobuf: {}", e)))?;

    let optimized = run_optimization(&state, &plan)?;

    let mut buf = Vec::new();
    optimized
        .encode(&mut buf)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("Encode error: {}", e)))?;

    Ok((StatusCode::OK, buf))
}

/// POST /optimize/json — accept and return Substrait plan as JSON.
pub async fn optimize_json(
    State(state): State<Arc<AppState>>,
    Json(plan): Json<proto::Plan>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let optimized = run_optimization(&state, &plan)?;
    Ok(Json(optimized))
}

/// POST /rules/configure — enable/disable rules.
pub async fn configure_rules(
    State(_state): State<Arc<AppState>>,
    Json(req): Json<ConfigureRulesRequest>,
) -> impl IntoResponse {
    // In a full implementation, we'd modify the rule registry.
    // For now, just acknowledge the request.
    Json(serde_json::json!({
        "status": "ok",
        "enabled": req.enable,
        "disabled": req.disable,
    }))
}

/// Request body for the rule configuration endpoint.
///
/// Allows clients to enable or disable specific rules by name, optionally
/// scoped to a particular data source type.
#[derive(Deserialize)]
pub struct ConfigureRulesRequest {
    /// Rules to enable (by name).
    #[serde(default)]
    pub enable: Vec<String>,
    /// Rules to disable (by name).
    #[serde(default)]
    pub disable: Vec<String>,
    /// Optional source type to scope the configuration (e.g., "hive", "iceberg").
    pub source: Option<String>,
}

/// Core optimization logic shared between proto and JSON endpoints.
///
/// This function encapsulates the entire optimization pipeline:
/// 1. Create a fresh Memo for this request (no cross-request state sharing).
/// 2. Consume the Substrait plan into the Memo.
/// 3. Run the Cascades search starting from the root group with "any" properties
///    (no specific sort or distribution requirements for the top-level output).
/// 4. Produce a Substrait plan from the optimized result.
fn run_optimization(
    state: &AppState,
    plan: &proto::Plan,
) -> Result<proto::Plan, (StatusCode, String)> {
    // Each optimization request gets a fresh memo -- there is no cross-request
    // memoization because different queries have different plan structures.
    let mut memo = Memo::new();

    // Step 1: Deserialize the Substrait plan into logical operators in the memo.
    let root_group = consumer::consume_plan(plan, &mut memo)
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("Failed to consume plan: {}", e)))?;

    // Step 2: Configure the search engine with limits and the active rule set.
    let config = SearchConfig {
        max_memo_groups: state.config.max_memo_groups,
        max_iterations: 1_000_000,
        source_type: state.config.source_type.clone(),
    };

    let mut search = CascadesSearch::new(
        memo,
        state.rule_registry.clone(),
        state.cost_model.clone(),
        state.catalog.clone(),
        config,
    );

    // Step 3: Run the Cascades optimization. "any" properties means the top-level
    // output has no specific ordering or distribution requirements.
    let optimized_plan = search
        .optimize(root_group, &PhysicalPropertySet::any())
        .ok_or_else(|| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Optimization failed: no valid plan found".to_string(),
            )
        })?;

    // Step 4: Serialize the optimized plan back to Substrait for the caller.
    Ok(producer::produce_plan(&optimized_plan))
}
