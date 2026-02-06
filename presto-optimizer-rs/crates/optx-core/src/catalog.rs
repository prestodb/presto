//! # Catalog Interface
//!
//! The catalog provides the optimizer with metadata about the database schema:
//! table column definitions and table-level statistics. This information is essential
//! for cost-based optimization because the cost model needs row counts, column NDVs,
//! and size estimates to compare alternative plans.
//!
//! ## Trait Design
//!
//! The `Catalog` trait is intentionally minimal and behind a trait object (`dyn Catalog`)
//! so that different backends can provide metadata. In production, the catalog would
//! be backed by Presto's `ConnectorMetadata` and `ConnectorTableLayoutHandle`. For
//! testing and development, the `InMemoryCatalog` provides a simple HashMap-based
//! implementation that can be populated programmatically.
//!
//! ## Key Lookups
//!
//! Tables are identified by `TableRef` (schema + name). The catalog returns:
//! - `get_table_stats`: Row count, total size, and per-column statistics (NDV, null
//!   fraction, etc.). Returns `None` if the table is unknown.
//! - `get_table_columns`: Column definitions. Returns `None` if the table is unknown.

use crate::expr::{ColumnRef, TableRef};
use crate::stats::Statistics;
use std::collections::HashMap;

/// Catalog provides schema and statistics information.
pub trait Catalog: Send + Sync {
    fn get_table_stats(&self, table: &TableRef) -> Option<Statistics>;
    fn get_table_columns(&self, table: &TableRef) -> Option<Vec<ColumnRef>>;
}

/// In-memory catalog for testing and development.
///
/// Tables are keyed by their fully-qualified name (`schema.table`). This catalog
/// is populated programmatically and does not persist across restarts. In production,
/// it would be replaced by a catalog backed by Presto's connector metadata.
#[derive(Debug, Clone, Default)]
pub struct InMemoryCatalog {
    /// Table-level statistics keyed by "schema.table".
    pub table_stats: HashMap<String, Statistics>,
    /// Column definitions keyed by "schema.table".
    pub table_columns: HashMap<String, Vec<ColumnRef>>,
}

impl InMemoryCatalog {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_table(
        &mut self,
        table: &TableRef,
        columns: Vec<ColumnRef>,
        stats: Statistics,
    ) {
        let key = format!("{}.{}", table.schema, table.name);
        self.table_columns.insert(key.clone(), columns);
        self.table_stats.insert(key, stats);
    }
}

impl Catalog for InMemoryCatalog {
    fn get_table_stats(&self, table: &TableRef) -> Option<Statistics> {
        let key = format!("{}.{}", table.schema, table.name);
        self.table_stats.get(&key).cloned()
    }

    fn get_table_columns(&self, table: &TableRef) -> Option<Vec<ColumnRef>> {
        let key = format!("{}.{}", table.schema, table.name);
        self.table_columns.get(&key).cloned()
    }
}
