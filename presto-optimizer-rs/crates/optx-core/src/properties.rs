//! # Physical and Logical Properties
//!
//! Properties describe characteristics of a query plan's output. They are central to
//! the Cascades framework's handling of "interesting orders" and data distributions.
//!
//! ## Logical Properties
//!
//! Logical properties are shared by all expressions in a group because they describe
//! the *what* (e.g., which columns are produced, estimated cardinality). They are
//! derived once per group during statistics derivation.
//!
//! ## Physical Properties
//!
//! Physical properties describe *how* data is organized at runtime:
//! - **Sort order**: specifies that the output is sorted by certain columns in a
//!   particular direction. Required by operators like MergeJoin and StreamAggregate.
//! - **Distribution**: specifies how data is partitioned across nodes (e.g., hash
//!   partitioned on join keys, broadcast, round-robin). Required for distributed
//!   execution planning.
//!
//! ## Property Enforcement
//!
//! When a parent operator requires properties that its child doesn't natively provide,
//! the optimizer can insert **enforcer** operators (e.g., Sort, Exchange) to satisfy
//! them. The cost of enforcement is factored into the plan comparison so that the
//! optimizer only adds enforcers when they're cheaper than alternative plans that
//! natively provide the required properties.
//!
//! ## The "Any" Property Set
//!
//! `PhysicalPropertySet::any()` represents "no requirements" -- the parent accepts
//! any sort order and any distribution. This is the default for most operators.

use crate::expr::{ColumnRef, Distribution, SortKey};
use serde::{Deserialize, Serialize};

/// Logical properties are derived from the logical content of a group.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LogicalProperties {
    /// Output columns of this group.
    pub output_columns: Vec<ColumnRef>,
    /// Estimated row count (from statistics).
    pub row_count: Option<f64>,
}

/// Physical properties describe how data is physically organized.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PhysicalPropertySet {
    pub sort_order: Option<Vec<SortKey>>,
    pub distribution: Option<Distribution>,
}

impl PhysicalPropertySet {
    pub fn any() -> Self {
        Self {
            sort_order: None,
            distribution: None,
        }
    }

    pub fn with_sort(order: Vec<SortKey>) -> Self {
        Self {
            sort_order: Some(order),
            distribution: None,
        }
    }

    pub fn with_distribution(dist: Distribution) -> Self {
        Self {
            sort_order: None,
            distribution: Some(dist),
        }
    }

    /// Check if this property set is satisfied by the given properties.
    ///
    /// A property requirement is satisfied if:
    /// - **Sort**: the provided sort order is a prefix of (or equal to) the required
    ///   sort order. For example, if we require `ORDER BY a, b` and the child provides
    ///   `ORDER BY a, b, c`, the requirement is satisfied (extra sort columns are fine).
    /// - **Distribution**: exact match is required (e.g., Hash([a]) != Hash([b])).
    /// - `None` (no requirement) is always satisfied regardless of what is provided.
    pub fn satisfied_by(&self, other: &PhysicalPropertySet) -> bool {
        let sort_ok = match (&self.sort_order, &other.sort_order) {
            (None, _) => true,
            (Some(_), None) => false,
            (Some(required), Some(provided)) => {
                // Provided order must be a prefix of or equal to required.
                // This allows "ORDER BY a, b" to satisfy "ORDER BY a, b, c".
                required.len() <= provided.len()
                    && required.iter().zip(provided.iter()).all(|(r, p)| r == p)
            }
        };
        let dist_ok = match (&self.distribution, &other.distribution) {
            (None, _) => true,
            (Some(_), None) => false,
            (Some(required), Some(provided)) => required == provided,
        };
        sort_ok && dist_ok
    }
}
