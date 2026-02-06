//! # Cost Model
//!
//! This module defines the cost abstraction and a default cost model for the optimizer.
//!
//! ## Multi-Dimensional Cost Model
//!
//! Query execution cost is not a single number -- it depends on CPU time, memory
//! consumption, and network transfer. The `DefaultCostModel` uses a weighted sum of
//! these three dimensions to collapse them into a single comparable `Cost` value:
//!
//! ```text
//! total_cost = cpu_weight * cpu_cost + memory_weight * memory_cost + network_weight * network_cost
//! ```
//!
//! The default weights (1.0, 1.0, 10.0) reflect the typical Presto assumption that
//! network I/O is the most expensive dimension in a distributed system. These weights
//! can be tuned per deployment.
//!
//! ## Cost Accumulation
//!
//! Costs are **additive**: the total cost of a plan is the sum of its local cost
//! (computed by the cost model for the operator) plus the accumulated costs of all
//! child plans. This bottom-up accumulation happens during the `implement_group`
//! phase of the Cascades search.
//!
//! ## Pluggable Design
//!
//! The `CostModel` trait allows replacing the default model with a custom one (e.g.,
//! a model trained on production query execution data or one that accounts for
//! connector-specific costs).

use crate::expr::*;
use crate::stats::Statistics;
use crate::properties::PhysicalPropertySet;
use serde::{Deserialize, Serialize};

/// Cost is a single comparable value representing the estimated expense of a plan.
///
/// Although the cost model considers multiple dimensions (CPU, memory, network),
/// they are collapsed into a single `total` for comparison. This simplification
/// enables a straightforward "cheapest wins" comparison in the Cascades search.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct Cost {
    /// The total weighted cost. Lower is better. `f64::MAX` represents infinity
    /// (an infeasible or not-yet-costed plan).
    pub total: f64,
}

impl Cost {
    pub fn zero() -> Self {
        Self { total: 0.0 }
    }

    pub fn new(total: f64) -> Self {
        Self { total }
    }

    pub fn infinite() -> Self {
        Self { total: f64::MAX }
    }

    pub fn is_infinite(&self) -> bool {
        self.total == f64::MAX
    }
}

/// Epsilon-based equality to handle floating-point imprecision in cost comparisons.
impl PartialEq for Cost {
    fn eq(&self, other: &Self) -> bool {
        (self.total - other.total).abs() < f64::EPSILON
    }
}

impl PartialOrd for Cost {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.total.partial_cmp(&other.total)
    }
}

/// Trait for pluggable cost models.
pub trait CostModel: Send + Sync {
    fn compute_cost(
        &self,
        op: &PhysicalOp,
        input_stats: &[&Statistics],
        children_costs: &[Cost],
        required_props: &PhysicalPropertySet,
    ) -> Cost;
}

/// Default cost model inspired by Presto's multi-dimensional cost model.
///
/// Each physical operator's cost is computed as a weighted sum of three dimensions:
/// - **CPU**: proportional to the number of rows processed or comparisons made.
/// - **Memory**: proportional to the amount of data held in memory (e.g., hash table).
/// - **Network**: proportional to the bytes transferred across nodes (e.g., exchanges).
///
/// The default weights make network 10x more expensive than CPU or memory,
/// reflecting the reality that data shuffling dominates cost in distributed execution.
pub struct DefaultCostModel {
    /// Weight for CPU-bound operations (row processing, comparisons).
    pub cpu_weight: f64,
    /// Weight for memory-bound operations (hash table construction, buffering).
    pub memory_weight: f64,
    /// Weight for network-bound operations (exchange/shuffle between nodes).
    pub network_weight: f64,
}

impl Default for DefaultCostModel {
    fn default() -> Self {
        Self {
            cpu_weight: 1.0,
            memory_weight: 1.0,
            network_weight: 10.0,
        }
    }
}

impl CostModel for DefaultCostModel {
    fn compute_cost(
        &self,
        op: &PhysicalOp,
        input_stats: &[&Statistics],
        children_costs: &[Cost],
        _required_props: &PhysicalPropertySet,
    ) -> Cost {
        let children_total: f64 = children_costs.iter().map(|c| c.total).sum();

        let local_cost = match op {
            // SeqScan: full table scan. CPU cost is proportional to the number of rows
            // because every row must be read and evaluated against any pushed-down predicate.
            PhysicalOp::SeqScan { .. } => {
                let rows = input_stats.first().map(|s| s.row_count).unwrap_or(1000.0);
                self.cpu_weight * rows
            }
            // HashJoin: builds a hash table on the build side, then probes with the other.
            // Build cost includes CPU (hashing each row) + memory (storing the hash table).
            // Probe cost is CPU-only (one hash lookup per probe row).
            // Choosing the smaller side as the build side is critical for minimizing memory.
            PhysicalOp::HashJoin { build_side, .. } => {
                if input_stats.len() < 2 {
                    return Cost::new(children_total + 1000.0);
                }
                let (build_stats, probe_stats) = match build_side {
                    BuildSide::Left => (input_stats[0], input_stats[1]),
                    BuildSide::Right => (input_stats[1], input_stats[0]),
                };
                let build_cost = self.cpu_weight * build_stats.row_count
                    + self.memory_weight * build_stats.total_size_bytes;
                let probe_cost = self.cpu_weight * probe_stats.row_count;
                build_cost + probe_cost
            }
            // MergeJoin: merges two pre-sorted streams. Linear O(n + m) since each
            // pointer advances at most once per row. No extra memory needed beyond buffers.
            PhysicalOp::MergeJoin { .. } => {
                if input_stats.len() < 2 {
                    return Cost::new(children_total + 1000.0);
                }
                let left_rows = input_stats[0].row_count;
                let right_rows = input_stats[1].row_count;
                self.cpu_weight * (left_rows + right_rows)
            }
            // NestedLoopJoin: O(n * m) -- for every left row, scans all right rows.
            // This is the most expensive join but works for any join condition, including
            // non-equi predicates and cross joins.
            PhysicalOp::NestedLoopJoin { .. } => {
                if input_stats.len() < 2 {
                    return Cost::new(children_total + 1000.0);
                }
                let left_rows = input_stats[0].row_count;
                let right_rows = input_stats[1].row_count;
                self.cpu_weight * left_rows * right_rows
            }
            // HashAggregate: builds a hash table keyed by group-by columns.
            // CPU cost for hashing + memory cost for storing all groups.
            // The memory estimate (rows * 100 bytes) is a rough per-row hash entry size.
            PhysicalOp::HashAggregate { .. } => {
                let rows = input_stats.first().map(|s| s.row_count).unwrap_or(1000.0);
                self.cpu_weight * rows + self.memory_weight * rows * 100.0
            }
            // StreamAggregate: processes pre-sorted input in a streaming fashion.
            // No hash table needed, so no memory cost -- just CPU to scan rows.
            // Requires input sorted on group-by columns (enforced via child properties).
            PhysicalOp::StreamAggregate { .. } => {
                let rows = input_stats.first().map(|s| s.row_count).unwrap_or(1000.0);
                self.cpu_weight * rows
            }
            // Sort: O(n log n) comparison-based sort. Memory cost accounts for
            // materializing all rows in a sort buffer.
            PhysicalOp::SortOp { .. } => {
                let rows = input_stats.first().map(|s| s.row_count).unwrap_or(1000.0);
                let n_log_n = if rows > 1.0 {
                    rows * rows.log2()
                } else {
                    1.0
                };
                self.cpu_weight * n_log_n + self.memory_weight * rows * 100.0
            }
            // Exchange: shuffles data across network between worker nodes.
            // Cost is dominated by the bytes transferred, weighted by the network factor.
            PhysicalOp::Exchange { .. } => {
                let rows = input_stats.first().map(|s| s.row_count).unwrap_or(1000.0);
                let size = input_stats.first().map(|s| s.total_size_bytes).unwrap_or(rows * 100.0);
                self.network_weight * size
            }
        };

        // Total cost = sum of children costs + this operator's local cost.
        Cost::new(children_total + local_cost)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stats::Statistics;

    #[test]
    fn test_hash_join_cost() {
        let model = DefaultCostModel::default();
        let small = Statistics::new(100.0, 10000.0);
        let large = Statistics::new(1_000_000.0, 100_000_000.0);

        let op_small_build = PhysicalOp::HashJoin {
            join_type: JoinType::Inner,
            build_side: BuildSide::Left,
            condition: Expr::Literal(ScalarValue::Bool(true)),
        };
        let op_large_build = PhysicalOp::HashJoin {
            join_type: JoinType::Inner,
            build_side: BuildSide::Right,
            condition: Expr::Literal(ScalarValue::Bool(true)),
        };

        let cost_small_build = model.compute_cost(
            &op_small_build,
            &[&small, &large],
            &[Cost::zero(), Cost::zero()],
            &PhysicalPropertySet::any(),
        );
        let cost_large_build = model.compute_cost(
            &op_large_build,
            &[&small, &large],
            &[Cost::zero(), Cost::zero()],
            &PhysicalPropertySet::any(),
        );

        // Building on the small side should be cheaper
        assert!(cost_small_build.total < cost_large_build.total);
    }
}
