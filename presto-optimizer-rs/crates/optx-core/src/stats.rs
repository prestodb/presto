//! # Statistics for Cost-Based Optimization
//!
//! This module defines the statistics structures and derivation formulas used by the
//! cost model to estimate the expense of query plans. Accurate statistics are crucial
//! for the optimizer to make good decisions (e.g., choosing which table to use as the
//! build side of a hash join, or whether an index scan is cheaper than a sequential scan).
//!
//! ## Statistics Hierarchy
//!
//! - **Table-level**: row count and total size in bytes.
//! - **Column-level**: number of distinct values (NDV), null fraction, min/max values,
//!   average row size, and optional histograms for range selectivity estimation.
//!
//! ## Derivation Formulas
//!
//! Statistics for intermediate plan nodes are derived bottom-up:
//!
//! - **Filter**: output_rows = input_rows * selectivity. Column NDVs are scaled
//!   proportionally to the row reduction ratio.
//! - **Join**: output_rows = |left| * |right| / max(NDV_left_key, NDV_right_key).
//!   This is the standard equi-join cardinality formula assuming uniform distribution.
//! - **Aggregate**: output_rows = product of NDVs of group-by columns, capped by input rows.
//!   This reflects the worst case where every combination of group-by values exists.
//!
//! ## Selectivity Estimation
//!
//! - **Equality**: 1 / NDV (uniform distribution assumption).
//! - **Range**: fixed 1/3 heuristic (could be improved with histogram-based estimation).
//! - **Default**: 0.1 (10%) when no better estimate is available.

use crate::expr::ScalarValue;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Statistics for a relation (or group in the memo).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Statistics {
    pub row_count: f64,
    pub total_size_bytes: f64,
    pub column_stats: HashMap<String, ColumnStatistics>,
}

impl Statistics {
    pub fn new(row_count: f64, total_size_bytes: f64) -> Self {
        Self {
            row_count,
            total_size_bytes,
            column_stats: HashMap::new(),
        }
    }

    pub fn with_column(mut self, name: impl Into<String>, stats: ColumnStatistics) -> Self {
        self.column_stats.insert(name.into(), stats);
        self
    }
}

/// Per-column statistics used for selectivity estimation and cost modeling.
///
/// These statistics are typically gathered by ANALYZE TABLE and stored in the catalog.
/// The optimizer uses them to estimate the output cardinality of operators.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnStatistics {
    /// Number of distinct values (NDV). Used for equality selectivity: sel = 1/NDV.
    pub distinct_count: f64,
    /// Fraction of rows that are NULL [0.0, 1.0].
    pub null_fraction: f64,
    /// Minimum value in the column (if known). Used for range selectivity.
    pub min_value: Option<ScalarValue>,
    /// Maximum value in the column (if known). Used for range selectivity.
    pub max_value: Option<ScalarValue>,
    /// Average size of a single value in bytes. Used for size estimation.
    pub avg_row_size: f64,
    /// Optional equi-depth histogram for more accurate range selectivity estimation.
    pub histogram: Option<Histogram>,
}

impl ColumnStatistics {
    pub fn new(distinct_count: f64, null_fraction: f64) -> Self {
        Self {
            distinct_count,
            null_fraction,
            min_value: None,
            max_value: None,
            avg_row_size: 8.0,
            histogram: None,
        }
    }
}

/// Equi-depth histogram for range selectivity estimation.
///
/// Each bucket contains approximately the same number of rows. This allows
/// more accurate selectivity estimation for range predicates (e.g., `WHERE x > 100`)
/// compared to the uniform distribution assumption.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Histogram {
    pub buckets: Vec<HistogramBucket>,
}

/// A single bucket in an equi-depth histogram.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistogramBucket {
    /// Lower bound of the bucket range (inclusive).
    pub lower: f64,
    /// Upper bound of the bucket range (inclusive).
    pub upper: f64,
    /// Estimated number of rows in this bucket.
    pub count: f64,
    /// Estimated number of distinct values in this bucket.
    pub distinct: f64,
}

/// Derive statistics for join output.
///
/// Uses the standard cardinality estimation formula for equi-joins:
///
/// ```text
/// |A JOIN B| = |A| * |B| / max(NDV(A.key), NDV(B.key))
/// ```
///
/// For multi-column joins, selectivities are multiplied (independence assumption):
/// each additional join column further reduces the output cardinality.
///
/// When NDV information is unavailable for a column, we conservatively use the
/// relation's row count as the NDV (assuming all values are distinct).
pub fn derive_join_stats(left: &Statistics, right: &Statistics, join_columns: &[(String, String)]) -> Statistics {
    // Accumulate the selectivity for each equi-join column pair.
    // For a single-column equi-join: selectivity = 1 / max(NDV_left, NDV_right).
    // For multi-column: multiply selectivities (independence assumption).
    let mut selectivity = 1.0_f64;

    for (left_col, right_col) in join_columns {
        let left_ndv = left
            .column_stats
            .get(left_col)
            .map(|s| s.distinct_count)
            .unwrap_or(left.row_count);
        let right_ndv = right
            .column_stats
            .get(right_col)
            .map(|s| s.distinct_count)
            .unwrap_or(right.row_count);
        // Use max(NDV) as the denominator. This is the standard "containment"
        // assumption: the smaller domain is fully contained in the larger one.
        let max_ndv = left_ndv.max(right_ndv).max(1.0);
        selectivity /= max_ndv;
    }

    // Output row count = cross product * selectivity, floored at 1.
    let row_count = (left.row_count * right.row_count * selectivity).max(1.0);

    // Estimate output row width as the sum of both sides' average row widths.
    let avg_row_size_left = if left.row_count > 0.0 {
        left.total_size_bytes / left.row_count
    } else {
        100.0
    };
    let avg_row_size_right = if right.row_count > 0.0 {
        right.total_size_bytes / right.row_count
    } else {
        100.0
    };
    let total_size_bytes = row_count * (avg_row_size_left + avg_row_size_right);

    // Propagate column-level statistics from both sides.
    // NDV is capped by the output row count (can't have more distinct values than rows).
    let mut column_stats = HashMap::new();
    for (name, stats) in &left.column_stats {
        let mut cs = stats.clone();
        cs.distinct_count = cs.distinct_count.min(row_count);
        column_stats.insert(name.clone(), cs);
    }
    for (name, stats) in &right.column_stats {
        let mut cs = stats.clone();
        cs.distinct_count = cs.distinct_count.min(row_count);
        column_stats.insert(name.clone(), cs);
    }

    Statistics {
        row_count,
        total_size_bytes,
        column_stats,
    }
}

/// Derive statistics for filter output.
///
/// Applies the given selectivity to the input statistics:
/// - Output rows = input rows * selectivity (floored at 1).
/// - Output size is scaled proportionally.
/// - Column NDVs are scaled by the same ratio, under the assumption that
///   filtering reduces distinct values proportionally to the row reduction.
///   This can overestimate NDV for highly selective filters on the filtered
///   column itself, but is a reasonable default.
pub fn derive_filter_stats(input: &Statistics, selectivity: f64) -> Statistics {
    let row_count = (input.row_count * selectivity).max(1.0);
    // Ratio of output to input rows, used to scale column-level statistics.
    let ratio = if input.row_count > 0.0 {
        row_count / input.row_count
    } else {
        1.0
    };

    let mut column_stats = HashMap::new();
    for (name, stats) in &input.column_stats {
        let mut cs = stats.clone();
        // Scale NDV by the row reduction ratio, clamped to [1, row_count].
        cs.distinct_count = (cs.distinct_count * ratio).max(1.0).min(row_count);
        column_stats.insert(name.clone(), cs);
    }

    Statistics {
        row_count,
        total_size_bytes: input.total_size_bytes * ratio,
        column_stats,
    }
}

/// Derive statistics for aggregate output.
///
/// The number of output groups is estimated as the product of NDVs of all group-by
/// columns, representing the worst case where every combination of group-by values
/// exists. This is capped by the input row count (can't have more groups than input rows).
///
/// For a global aggregate (no group-by columns), the output is always 1 row.
///
/// Column-level statistics are not propagated because the aggregate output columns
/// are aggregate functions (SUM, COUNT, etc.) whose statistics are not easily derived.
pub fn derive_aggregate_stats(input: &Statistics, group_by_cols: &[String]) -> Statistics {
    // Start with 1 and multiply by each group-by column's NDV.
    // This gives the maximum possible number of distinct groups.
    let mut row_count = 1.0_f64;
    for col in group_by_cols {
        let ndv = input
            .column_stats
            .get(col)
            .map(|s| s.distinct_count)
            .unwrap_or(input.row_count);
        row_count *= ndv;
    }
    // Cap by input rows (can't produce more groups than input rows) and floor at 1.
    row_count = row_count.min(input.row_count).max(1.0);

    Statistics {
        row_count,
        total_size_bytes: row_count * 100.0, // rough estimate: 100 bytes per output row
        column_stats: HashMap::new(),
    }
}

/// Default filter selectivity when we can't determine it.
pub const DEFAULT_FILTER_SELECTIVITY: f64 = 0.1;

/// Estimate selectivity for an equality predicate: `sel = 1 / NDV`.
///
/// This assumes a uniform distribution of values. If column statistics are
/// unavailable, falls back to the default selectivity of 0.1 (10%).
pub fn equality_selectivity(stats: &Statistics, col_name: &str) -> f64 {
    stats
        .column_stats
        .get(col_name)
        .map(|cs| 1.0 / cs.distinct_count.max(1.0))
        .unwrap_or(DEFAULT_FILTER_SELECTIVITY)
}
