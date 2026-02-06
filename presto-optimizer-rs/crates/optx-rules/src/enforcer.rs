//! # Sort Enforcer / Implementation Rule
//!
//! This module provides the implementation rule for the logical Sort operator.
//!
//! ## Enforcers in the Cascades Framework
//!
//! An "enforcer" is a physical operator whose sole purpose is to satisfy a required
//! physical property that the child plan doesn't natively provide. The Sort operator
//! is the canonical enforcer: when a parent requires sorted output (e.g., ORDER BY
//! or MergeJoin inputs), but the child's best plan is unsorted, the optimizer
//! considers adding a Sort on top.
//!
//! ## How It Works
//!
//! 1. A logical Sort node appears in the plan (from an ORDER BY clause).
//! 2. This rule maps it to a physical SortOp.
//! 3. Additionally, the search engine's `try_sort_enforcer` method can inject Sort
//!    enforcers even when there is no explicit logical Sort, whenever a parent
//!    operator requires sorted child output.
//!
//! ## Cost Considerations
//!
//! The sort cost is O(n log n) CPU + O(n) memory for n input rows. The optimizer
//! compares this against plans that natively provide the sort order (e.g., an index
//! scan or a prior sort), choosing whichever is cheaper overall.

use optx_core::expr::*;
use optx_core::memo::{Memo, MemoExpr};
use optx_core::pattern::{OpMatcher, Pattern};
use optx_core::rule::{OptContext, Rule, RuleResult, RuleType};

/// Implement logical sort as a physical sort operator.
///
/// This is both an implementation rule (for explicit ORDER BY) and conceptually
/// an enforcer (for implicit sort requirements from operators like MergeJoin).
pub struct ImplSortRule;

impl Rule for ImplSortRule {
    fn name(&self) -> &str {
        "ImplSort"
    }

    fn rule_type(&self) -> RuleType {
        RuleType::Implementation
    }

    fn pattern(&self) -> Pattern {
        Pattern::Operator(
            OpMatcher::LogicalOp(LogicalOpKind::Sort),
            vec![Pattern::Any],
        )
    }

    fn apply(
        &self,
        expr: &MemoExpr,
        _memo: &Memo,
        _ctx: &OptContext,
    ) -> Vec<RuleResult> {
        let Operator::Logical(LogicalOp::Sort { order }) = &expr.op else {
            return vec![];
        };

        vec![RuleResult::Substitution(
            Operator::Physical(PhysicalOp::SortOp {
                order: order.clone(),
            }),
            expr.children.clone(),
        )]
    }
}
