//! # Join Implementation Rules
//!
//! This module provides three implementation rules that map a logical Join to
//! physical join operators. Each physical join has different cost characteristics
//! and applicability constraints:
//!
//! ## Hash Join (`ImplHashJoinRule`)
//!
//! The workhorse join algorithm for most queries. Builds a hash table on one side
//! (the "build side") and probes it with rows from the other side. Produces two
//! alternatives per join (build-left and build-right) so the cost model can pick
//! the cheaper option (typically building on the smaller side).
//!
//! **Requires**: at least one equi-join predicate (e.g., `A.id = B.id`).
//! **Cost**: O(build_rows) memory + O(build_rows + probe_rows) CPU.
//!
//! ## Merge Join (`ImplMergeJoinRule`)
//!
//! Merges two pre-sorted streams by advancing pointers. Very efficient when both
//! inputs are already sorted. However, in a lakehouse environment (Hive/Iceberg/Delta),
//! data from Parquet/ORC files is almost never pre-sorted, so the optimizer must add
//! Sort enforcers on both inputs. This makes merge join rarely win over hash join
//! in practice, unless the data happens to be sorted (e.g., from a bucketed Hive table
//! or a preceding Sort operator).
//!
//! **Velox/native execution only** -- Presto's Java engine has no merge join operator.
//! The `MergeJoinNode` plan node exists at the SPI level, but only the Velox C++
//! engine implements the physical operator.
//!
//! **Requires**: inner join with equi-join predicates.
//! **Cost**: O(left_rows + right_rows) CPU, minimal memory.
//!
//! ## Nested Loop Join (`ImplNestedLoopJoinRule`)
//!
//! The universal fallback: for each left row, scans all right rows. Always
//! applicable (works with any join condition, including non-equi predicates and
//! cross joins), but O(n * m) makes it prohibitively expensive for large inputs.
//! The cost model typically chooses this only for very small joins or when no
//! equi-join predicate exists.
//!
//! **Requires**: nothing (always applicable).
//! **Cost**: O(left_rows * right_rows) CPU.

use optx_core::expr::*;
use optx_core::memo::{Memo, MemoExpr};
use optx_core::pattern::{OpMatcher, Pattern};
use optx_core::rule::{OptContext, Rule, RuleResult, RuleType};

/// Implement logical join as a hash join.
///
/// Generates two alternatives: build on left side and build on right side.
/// The cost model will select the cheaper one (typically building on the smaller side).
pub struct ImplHashJoinRule;

impl Rule for ImplHashJoinRule {
    fn name(&self) -> &str {
        "ImplHashJoin"
    }

    fn rule_type(&self) -> RuleType {
        RuleType::Implementation
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
    ) -> Vec<RuleResult> {
        let Operator::Logical(LogicalOp::Join {
            join_type,
            condition,
        }) = &expr.op
        else {
            return vec![];
        };

        if expr.children.len() != 2 {
            return vec![];
        }

        // Hash join requires at least one equi-join predicate
        let has_equi = has_equi_predicate(condition);
        if !has_equi && !matches!(join_type, JoinType::Cross) {
            return vec![];
        }

        let mut results = Vec::new();

        // Build on right (typical: smaller table on build side)
        results.push(RuleResult::Substitution(
            Operator::Physical(PhysicalOp::HashJoin {
                join_type: *join_type,
                build_side: BuildSide::Right,
                condition: condition.clone(),
            }),
            expr.children.clone(),
        ));

        // Build on left
        results.push(RuleResult::Substitution(
            Operator::Physical(PhysicalOp::HashJoin {
                join_type: *join_type,
                build_side: BuildSide::Left,
                condition: condition.clone(),
            }),
            expr.children.clone(),
        ));

        results
    }
}

/// Implement logical join as a merge join (requires sorted input).
///
/// Only fires for inner joins with equi-join predicates. The search engine will
/// derive sort requirements for both children, potentially adding sort enforcers
/// if the children don't natively produce sorted output.
pub struct ImplMergeJoinRule;

impl Rule for ImplMergeJoinRule {
    fn name(&self) -> &str {
        "ImplMergeJoin"
    }

    fn rule_type(&self) -> RuleType {
        RuleType::Implementation
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
    ) -> Vec<RuleResult> {
        let Operator::Logical(LogicalOp::Join {
            join_type,
            condition,
        }) = &expr.op
        else {
            return vec![];
        };

        // Merge join only for inner joins with equi predicates
        if *join_type != JoinType::Inner {
            return vec![];
        }
        if !has_equi_predicate(condition) {
            return vec![];
        }

        vec![RuleResult::Substitution(
            Operator::Physical(PhysicalOp::MergeJoin {
                join_type: *join_type,
                condition: condition.clone(),
            }),
            expr.children.clone(),
        )]
    }
}

/// Implement logical join as a nested loop join (universal fallback).
///
/// Always applicable regardless of join type or condition, but has O(n * m) cost.
/// The cost model will typically prefer hash join or merge join when applicable.
pub struct ImplNestedLoopJoinRule;

impl Rule for ImplNestedLoopJoinRule {
    fn name(&self) -> &str {
        "ImplNestedLoopJoin"
    }

    fn rule_type(&self) -> RuleType {
        RuleType::Implementation
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
    ) -> Vec<RuleResult> {
        let Operator::Logical(LogicalOp::Join {
            join_type,
            condition,
        }) = &expr.op
        else {
            return vec![];
        };

        // NLJ is always applicable but expensive
        vec![RuleResult::Substitution(
            Operator::Physical(PhysicalOp::NestedLoopJoin {
                join_type: *join_type,
                condition: condition.clone(),
            }),
            expr.children.clone(),
        )]
    }
}

/// Check if an expression contains at least one equi-join predicate (col = col).
///
/// This is used as a gate for hash join and merge join applicability. These
/// algorithms require at least one equality comparison between columns from
/// the two sides to function correctly.
fn has_equi_predicate(expr: &Expr) -> bool {
    match expr {
        Expr::BinaryOp {
            op: BinaryOp::Eq,
            left,
            right,
        } => matches!(
            (left.as_ref(), right.as_ref()),
            (Expr::Column(_), Expr::Column(_))
        ),
        Expr::And(conjuncts) => conjuncts.iter().any(has_equi_predicate),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_has_equi_predicate() {
        let equi = Expr::BinaryOp {
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
        assert!(has_equi_predicate(&equi));

        let non_equi = Expr::BinaryOp {
            op: BinaryOp::Lt,
            left: Box::new(Expr::Column(ColumnRef {
                table: Some("a".into()),
                name: "x".into(),
                index: 0,
            })),
            right: Box::new(Expr::Literal(ScalarValue::Int64(10))),
        };
        assert!(!has_equi_predicate(&non_equi));
    }
}
