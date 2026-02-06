//! # Join Commutativity Rule
//!
//! This rule implements the algebraic identity: `A JOIN B = B JOIN A` for symmetric
//! join types (inner joins and cross joins).
//!
//! ## Why Commutativity Matters
//!
//! In a cost-based optimizer, the order of join inputs significantly affects performance:
//!
//! - **Hash join build side**: The smaller relation should be the build side to minimize
//!   memory usage. Commutativity lets the optimizer consider both orientations and pick
//!   the one where the smaller table is built into the hash table.
//!
//! - **Join reordering**: Combined with associativity, commutativity enables exploring
//!   different join orderings for multi-table queries. Even without full associativity
//!   support, swapping join sides at each level of a left-deep tree explores many
//!   useful alternatives.
//!
//! ## Applicability
//!
//! This rule only fires for `Inner` and `Cross` joins. Left, Right, Semi, and Anti
//! joins have fixed left/right semantics (e.g., a left join preserves all left rows)
//! and cannot be commuted without changing the query semantics.
//!
//! ## Condition Swapping
//!
//! When swapping `A.x = B.y` to `B.y = A.x`, the condition is also swapped for
//! consistency. While equality is symmetric, maintaining the convention helps with
//! downstream processing and debugging.

use optx_core::expr::*;
use optx_core::memo::{GroupId, Memo, MemoExpr};
use optx_core::pattern::{OpMatcher, Pattern};
use optx_core::rule::{OptContext, Rule, RuleType};

/// Join commutativity: A JOIN B -> B JOIN A.
///
/// Generates one alternative where the left and right children are swapped.
/// Only applies to inner and cross joins (symmetric join types).
pub struct JoinCommutativityRule;

impl Rule for JoinCommutativityRule {
    fn name(&self) -> &str {
        "JoinCommutativity"
    }

    fn rule_type(&self) -> RuleType {
        RuleType::Transformation
    }

    fn pattern(&self) -> Pattern {
        Pattern::Operator(
            OpMatcher::LogicalOp(LogicalOpKind::Join),
            vec![Pattern::Any, Pattern::Any],
        )
    }

    fn apply(
        &self,
        expr: &MemoExpr,
        _memo: &Memo,
        _ctx: &OptContext,
    ) -> Vec<(Operator, Vec<GroupId>)> {
        let Operator::Logical(LogicalOp::Join {
            join_type,
            condition,
        }) = &expr.op
        else {
            return vec![];
        };

        // Only commute inner and cross joins (left/right/semi have fixed semantics)
        if !matches!(join_type, JoinType::Inner | JoinType::Cross) {
            return vec![];
        }

        if expr.children.len() != 2 {
            return vec![];
        }

        // Swap children
        let new_children = vec![expr.children[1], expr.children[0]];
        let new_op = Operator::Logical(LogicalOp::Join {
            join_type: *join_type,
            condition: swap_condition_sides(condition),
        });

        vec![(new_op, new_children)]
    }
}

/// Swap the sides of an equi-join condition.
/// For A.x = B.y, produce B.y = A.x (semantically the same, but tracks the swap).
fn swap_condition_sides(expr: &Expr) -> Expr {
    match expr {
        Expr::BinaryOp {
            op: BinaryOp::Eq,
            left,
            right,
        } => Expr::BinaryOp {
            op: BinaryOp::Eq,
            left: right.clone(),
            right: left.clone(),
        },
        Expr::And(conjuncts) => {
            Expr::And(conjuncts.iter().map(swap_condition_sides).collect())
        }
        other => other.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_swap_condition() {
        let cond = Expr::BinaryOp {
            op: BinaryOp::Eq,
            left: Box::new(Expr::Column(ColumnRef {
                table: Some("a".into()),
                name: "x".into(),
                index: 0,
            })),
            right: Box::new(Expr::Column(ColumnRef {
                table: Some("b".into()),
                name: "y".into(),
                index: 0,
            })),
        };
        let swapped = swap_condition_sides(&cond);
        match swapped {
            Expr::BinaryOp { left, right, .. } => {
                assert!(matches!(left.as_ref(), Expr::Column(c) if c.table.as_deref() == Some("b")));
                assert!(matches!(right.as_ref(), Expr::Column(c) if c.table.as_deref() == Some("a")));
            }
            _ => panic!("Expected BinaryOp"),
        }
    }
}
