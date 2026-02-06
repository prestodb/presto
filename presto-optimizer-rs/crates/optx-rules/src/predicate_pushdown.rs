//! # Predicate Pushdown Rule
//!
//! Predicate pushdown is one of the most impactful optimization rules because it
//! reduces the amount of data flowing through the plan as early as possible.
//!
//! ## What It Does
//!
//! When a Filter sits on top of a Join, this rule merges the filter predicate into
//! the join condition. The combined condition allows the join operator to evaluate
//! the predicate during join execution rather than in a separate pass afterward.
//!
//! ```text
//! Before: Filter(pred, Join(A, B, cond))
//! After:  Join(A, B, cond AND pred)
//! ```
//!
//! ## Why This Helps
//!
//! - **Reduces intermediate data**: rows are filtered during the join rather than
//!   after it, producing fewer output rows for upstream operators.
//! - **Enables better cost estimates**: the merged condition gives the join operator
//!   a more selective predicate, leading to more accurate cardinality estimates.
//! - **Prerequisite for other optimizations**: with the predicate on the join, the
//!   cost model can accurately compare hash join vs merge join alternatives.
//!
//! ## Memo-Based Approach
//!
//! In a traditional rewrite-based optimizer, predicate pushdown would physically
//! restructure the plan tree. In our Cascades/memo-based approach, we instead
//! create a *new equivalent expression* in the join's group with the merged
//! condition. The original Filter-over-Join plan remains in the memo as an
//! alternative, and the cost model chooses the cheaper option.

use optx_core::expr::*;
use optx_core::memo::{GroupId, Memo, MemoExpr};
use optx_core::pattern::{OpMatcher, Pattern};
use optx_core::rule::{OptContext, Rule, RuleType};

/// Push filter predicates into join conditions.
///
/// Matches a Filter on top of a Join and produces a new Join with the filter
/// predicate merged into the join condition.
pub struct PredicatePushdownRule;

impl Rule for PredicatePushdownRule {
    fn name(&self) -> &str {
        "PredicatePushdown"
    }

    fn rule_type(&self) -> RuleType {
        RuleType::Transformation
    }

    fn pattern(&self) -> Pattern {
        // Match: Filter(Join(A, B))
        Pattern::Operator(
            OpMatcher::LogicalOp(LogicalOpKind::Filter),
            vec![Pattern::Operator(
                OpMatcher::LogicalOp(LogicalOpKind::Join),
                vec![Pattern::Any, Pattern::Any],
            )],
        )
    }

    fn apply(
        &self,
        expr: &MemoExpr,
        memo: &Memo,
        _ctx: &OptContext,
    ) -> Vec<(Operator, Vec<GroupId>)> {
        let Operator::Logical(LogicalOp::Filter { predicate }) = &expr.op else {
            return vec![];
        };

        if expr.children.is_empty() {
            return vec![];
        }

        let join_group = memo.group(expr.children[0]);
        let join_expr = join_group.logical_exprs.iter().find_map(|&eid| {
            let e = memo.expr(eid);
            if let Operator::Logical(LogicalOp::Join {
                join_type,
                condition,
            }) = &e.op
            {
                Some((eid, *join_type, condition.clone(), e.children.clone()))
            } else {
                None
            }
        });

        let Some((_join_eid, join_type, join_condition, join_children)) = join_expr else {
            return vec![];
        };

        if join_children.len() != 2 {
            return vec![];
        }

        // Merge the filter predicate into the join condition
        let merged_condition = match (&join_condition, predicate) {
            (existing, new_pred) => {
                let mut conjuncts = existing.conjuncts().into_iter().cloned().collect::<Vec<_>>();
                conjuncts.extend(new_pred.conjuncts().into_iter().cloned());
                if conjuncts.len() == 1 {
                    conjuncts.into_iter().next().unwrap()
                } else {
                    Expr::And(conjuncts)
                }
            }
        };

        let new_join = Operator::Logical(LogicalOp::Join {
            join_type,
            condition: merged_condition,
        });

        vec![(new_join, join_children)]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rule_metadata() {
        let rule = PredicatePushdownRule;
        assert_eq!(rule.name(), "PredicatePushdown");
        assert_eq!(rule.rule_type(), RuleType::Transformation);
    }
}
