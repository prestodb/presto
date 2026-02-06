//! # Projection Pushdown Rule
//!
//! This rule implements column pruning by pushing projection requirements down to
//! table scans. When a Project node only references a subset of the columns produced
//! by a child Scan, this rule creates a new Scan that reads only those columns.
//!
//! ## Why This Helps
//!
//! - **Reduces I/O**: columnar storage formats (ORC, Parquet) can skip entire columns
//!   that are not needed, dramatically reducing the amount of data read from disk.
//! - **Reduces memory**: fewer columns means smaller row representations throughout
//!   the plan, reducing memory pressure on hash tables, sort buffers, and network
//!   transfers.
//! - **Reduces network transfer**: in distributed execution, only needed columns are
//!   shuffled between nodes.
//!
//! ## Current Scope
//!
//! This rule currently handles the simple case of Project directly over Scan. A more
//! complete implementation would push projections through joins, filters, and
//! aggregates to prune columns at every level of the plan.

use optx_core::expr::*;
use optx_core::memo::{Memo, MemoExpr};
use optx_core::pattern::{OpMatcher, Pattern};
use optx_core::rule::{OptContext, Rule, RuleResult, RuleType};

/// Push projections closer to table scans to reduce intermediate data.
///
/// When a Project sits on top of a Scan, creates a narrower Scan that only
/// reads the columns referenced by the projection expressions.
pub struct ProjectionPushdownRule;

impl Rule for ProjectionPushdownRule {
    fn name(&self) -> &str {
        "ProjectionPushdown"
    }

    fn rule_type(&self) -> RuleType {
        RuleType::Transformation
    }

    fn pattern(&self) -> Pattern {
        // Match: Project(Any)
        Pattern::Operator(
            OpMatcher::LogicalOp(LogicalOpKind::Project),
            vec![Pattern::Any],
        )
    }

    fn apply(
        &self,
        expr: &MemoExpr,
        memo: &Memo,
        _ctx: &OptContext,
    ) -> Vec<RuleResult> {
        let Operator::Logical(LogicalOp::Project { exprs, aliases: _ }) = &expr.op else {
            return vec![];
        };

        if expr.children.is_empty() {
            return vec![];
        }

        let child_group = memo.group(expr.children[0]);

        // Check if child is a scan — if so, try to push column pruning into scan
        for &child_eid in &child_group.logical_exprs {
            let child_expr = memo.expr(child_eid);
            if let Operator::Logical(LogicalOp::Scan {
                table,
                columns: _,
                predicate,
            }) = &child_expr.op
            {
                // Extract columns referenced by the projection
                let needed_cols: Vec<ColumnRef> = exprs
                    .iter()
                    .flat_map(|e| e.columns().into_iter().cloned())
                    .collect();

                if needed_cols.is_empty() {
                    continue;
                }

                // Create a new scan with only the needed columns
                let new_scan = Operator::Logical(LogicalOp::Scan {
                    table: table.clone(),
                    columns: needed_cols,
                    predicate: predicate.clone(),
                });

                // The projection stays on top but the scan is now narrower
                return vec![RuleResult::Substitution(new_scan, vec![])];
            }
        }

        vec![]
    }
}
