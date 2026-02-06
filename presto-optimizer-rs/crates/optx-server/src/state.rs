//! # Application State
//!
//! This module defines the shared state that is available to all HTTP request handlers.
//! The state is created once at server startup and shared via `Arc` across all
//! concurrent requests.
//!
//! ## Components
//!
//! - **Rule Registry**: The set of transformation and implementation rules. Shared
//!   (not cloned per request) because rules are stateless.
//! - **Cost Model**: The function that estimates the cost of physical operators.
//!   Shared because it is also stateless.
//! - **Catalog**: Provides table metadata and statistics. In the current implementation,
//!   this is an empty in-memory catalog. In production, it would be populated from
//!   the Presto coordinator's metadata or from statistics sent with the plan.
//! - **Optimizer Config**: Tuning knobs like search time limits and memo size limits.

use optx_core::catalog::{Catalog, InMemoryCatalog};
use optx_core::cost::{CostModel, DefaultCostModel};
use optx_core::rule::RuleRegistry;
use std::sync::Arc;

/// Server-level optimizer configuration.
///
/// These settings control resource limits for the optimization search.
/// They apply to all optimization requests handled by this server instance.
pub struct OptimizerConfig {
    /// Maximum wall-clock time for a single optimization (not currently enforced;
    /// the iteration limit is the primary safety mechanism).
    pub max_search_time_ms: u64,
    /// Maximum number of groups in the memo table before the search is halted.
    pub max_memo_groups: usize,
    /// Optional connector/source type for selecting connector-specific rules.
    pub source_type: Option<String>,
}

impl Default for OptimizerConfig {
    fn default() -> Self {
        Self {
            max_search_time_ms: 5000,
            max_memo_groups: 100_000,
            source_type: None,
        }
    }
}

/// Shared application state, accessible by all request handlers via Axum's State extractor.
///
/// All fields are wrapped in Arc so they can be shared across concurrent requests
/// without cloning the underlying data.
pub struct AppState {
    /// All transformation and implementation rules available to the optimizer.
    pub rule_registry: Arc<RuleRegistry>,
    /// The cost model used to score physical plan alternatives.
    pub cost_model: Arc<dyn CostModel>,
    /// Catalog providing table metadata and statistics for cost estimation.
    pub catalog: Arc<dyn Catalog>,
    /// Optimizer configuration (limits, source type).
    pub config: OptimizerConfig,
}

impl AppState {
    /// Create a new AppState with the default configuration.
    ///
    /// Initializes the rule registry with all built-in rules, the default
    /// cost model (CPU/memory/network weighted), and an empty in-memory catalog.
    pub fn new() -> Self {
        let registry = optx_rules::default_rule_registry();
        Self {
            rule_registry: Arc::new(registry),
            cost_model: Arc::new(DefaultCostModel::default()),
            catalog: Arc::new(InMemoryCatalog::new()),
            config: OptimizerConfig::default(),
        }
    }
}
