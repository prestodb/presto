//! # Built-in Optimization Rules
//!
//! This crate provides the default set of optimization rules for the Cascades search
//! engine. Rules are divided into two categories:
//!
//! ## Transformation Rules (Logical -> Logical)
//!
//! These rules expand the search space by generating equivalent logical alternatives:
//!
//! - **`JoinCommutativityRule`**: Swaps the sides of inner and cross joins
//!   (A JOIN B -> B JOIN A). Enables the cost model to choose which side is smaller.
//! - **`JoinAssociativityRule`**: Placeholder for changing join grouping
//!   ((A JOIN B) JOIN C -> A JOIN (B JOIN C)). Currently a no-op due to rule
//!   interface limitations; commutativity compensates in practice.
//! - **`PredicatePushdownRule`**: Merges filter predicates into join conditions,
//!   enabling earlier data reduction.
//! - **`ProjectionPushdownRule`**: Pushes column requirements into table scans,
//!   reducing the amount of data read from storage.
//!
//! ## Implementation Rules (Logical -> Physical)
//!
//! These rules produce physical operator alternatives that the cost model scores:
//!
//! - **`ImplHashJoinRule`**: Implements a join as a hash join (build-left and build-right).
//! - **`ImplMergeJoinRule`**: Implements a join as a merge join (inner equi-joins only).
//! - **`ImplNestedLoopJoinRule`**: Implements a join as a nested loop join (universal fallback).
//! - **`ImplSeqScanRule`**: Implements a scan as a sequential (full) table scan.
//! - **`ImplHashAggregateRule`**: Implements aggregation using a hash table.
//! - **`ImplStreamAggregateRule`**: Implements aggregation using sorted-stream processing.
//! - **`ImplSortRule`**: Implements a logical sort as a physical sort operator.

pub mod enforcer;
pub mod impl_agg;
pub mod impl_join;
pub mod impl_scan;
pub mod join_associativity;
pub mod join_commutativity;
pub mod predicate_pushdown;
pub mod projection_pushdown;

use optx_core::rule::RuleRegistry;

/// Create a default rule registry with all built-in rules.
///
/// This is the standard configuration for the optimizer. Connector-specific rules
/// can be added to the returned registry via `add_source_rule_set()`.
pub fn default_rule_registry() -> RuleRegistry {
    let mut registry = RuleRegistry::new();

    // Transformation rules: expand the logical search space.
    registry.add_rule(Box::new(join_commutativity::JoinCommutativityRule));
    registry.add_rule(Box::new(join_associativity::JoinAssociativityRule));
    registry.add_rule(Box::new(predicate_pushdown::PredicatePushdownRule));
    registry.add_rule(Box::new(projection_pushdown::ProjectionPushdownRule));

    // Implementation rules: map logical operators to physical alternatives.
    registry.add_rule(Box::new(impl_join::ImplHashJoinRule));
    registry.add_rule(Box::new(impl_join::ImplMergeJoinRule));
    registry.add_rule(Box::new(impl_join::ImplNestedLoopJoinRule));
    registry.add_rule(Box::new(impl_scan::ImplSeqScanRule));
    registry.add_rule(Box::new(impl_agg::ImplHashAggregateRule));
    registry.add_rule(Box::new(impl_agg::ImplStreamAggregateRule));
    registry.add_rule(Box::new(enforcer::ImplSortRule));

    registry
}
