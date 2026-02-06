//! # Scan Implementation Rule
//!
//! This module provides the implementation rule that maps a logical Scan to a
//! physical SeqScan (sequential/full table scan).
//!
//! ## Sequential Scan
//!
//! A sequential scan reads every row from the table and evaluates any pushed-down
//! predicate. In a lakehouse environment (Hive, Iceberg, Delta Lake), this is the
//! only scan strategy because Parquet/ORC files do not have traditional database
//! indexes (B-tree, hash, etc.).
//!
//! ## Lakehouse-Specific Optimizations
//!
//! While there is no IndexScan for lakehouse tables, several connector-level
//! optimizations can reduce I/O without changing the physical operator model:
//!
//! - **Partition pruning**: Hive/Iceberg partition predicates eliminate entire
//!   directories/files from the scan. Handled by the connector's split manager.
//! - **Min/max file statistics**: Parquet/ORC footer stats (min, max per column
//!   per row group) allow skipping row groups that cannot match the predicate.
//! - **Bloom filters**: Parquet bloom filters on high-cardinality columns can
//!   skip row groups for equality predicates.
//! - **Delete file handling**: Iceberg v2 position/equality delete files are
//!   applied at the connector level during scan.

use optx_core::expr::*;
use optx_core::memo::{GroupId, Memo, MemoExpr};
use optx_core::pattern::{OpMatcher, Pattern};
use optx_core::rule::{OptContext, Rule, RuleType};

/// Implement logical scan as a sequential (full) table scan.
///
/// This is the default scan implementation. It directly translates the logical
/// Scan's table, columns, and predicate into a physical SeqScan with the same fields.
pub struct ImplSeqScanRule;

impl Rule for ImplSeqScanRule {
    fn name(&self) -> &str {
        "ImplSeqScan"
    }

    fn rule_type(&self) -> RuleType {
        RuleType::Implementation
    }

    fn pattern(&self) -> Pattern {
        Pattern::Operator(OpMatcher::LogicalOp(LogicalOpKind::Scan), vec![])
    }

    fn apply(
        &self,
        expr: &MemoExpr,
        _memo: &Memo,
        _ctx: &OptContext,
    ) -> Vec<(Operator, Vec<GroupId>)> {
        let Operator::Logical(LogicalOp::Scan {
            table,
            columns,
            predicate,
        }) = &expr.op
        else {
            return vec![];
        };

        vec![(
            Operator::Physical(PhysicalOp::SeqScan {
                table: table.clone(),
                columns: columns.clone(),
                predicate: predicate.clone(),
            }),
            vec![],
        )]
    }
}
