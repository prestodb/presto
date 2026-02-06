//! # Aggregate Implementation Rules
//!
//! This module provides two implementation rules for the logical Aggregate operator:
//!
//! ## Hash Aggregate (`ImplHashAggregateRule`)
//!
//! Uses a hash table keyed by the group-by columns. Each input row is hashed and
//! routed to the corresponding bucket, where the aggregate accumulators are updated.
//! Works with any input ordering.
//!
//! **Cost trade-off**: O(n) CPU + O(groups) memory. The memory cost can be significant
//! for high-cardinality group-by columns (many distinct groups), but is the go-to
//! choice when the input is not pre-sorted.
//!
//! ## Stream Aggregate (`ImplStreamAggregateRule`)
//!
//! Processes input rows in a single pass, detecting group boundaries from the sorted
//! order. When the group-by columns change, the current group is finalized and emitted.
//!
//! **Cost trade-off**: O(n) CPU + O(1) memory (only the current group's accumulators).
//! Cheaper than hash aggregate but requires input sorted on the group-by columns.
//! The search engine will derive sort requirements and potentially add a Sort enforcer;
//! the total cost (sort + stream aggregate) may be cheaper than hash aggregate for
//! inputs that are already partially sorted or for very large aggregations.

use optx_core::expr::*;
use optx_core::memo::{Memo, MemoExpr};
use optx_core::pattern::{OpMatcher, Pattern};
use optx_core::rule::{OptContext, Rule, RuleResult, RuleType};

/// Implement logical aggregate as a hash aggregate.
///
/// Always applicable. Uses a hash table to group rows, trading memory for the
/// ability to process unsorted input.
pub struct ImplHashAggregateRule;

impl Rule for ImplHashAggregateRule {
    fn name(&self) -> &str {
        "ImplHashAggregate"
    }

    fn rule_type(&self) -> RuleType {
        RuleType::Implementation
    }

    fn pattern(&self) -> Pattern {
        Pattern::Operator(
            OpMatcher::LogicalOp(LogicalOpKind::Aggregate),
            vec![Pattern::Any],
        )
    }

    fn apply(
        &self,
        expr: &MemoExpr,
        _memo: &Memo,
        _ctx: &OptContext,
    ) -> Vec<RuleResult> {
        let Operator::Logical(LogicalOp::Aggregate {
            group_by,
            aggregates,
        }) = &expr.op
        else {
            return vec![];
        };

        vec![RuleResult::Substitution(
            Operator::Physical(PhysicalOp::HashAggregate {
                group_by: group_by.clone(),
                aggregates: aggregates.clone(),
            }),
            expr.children.clone(),
        )]
    }
}

/// Implement logical aggregate as a stream aggregate (requires sorted input).
///
/// Uses constant memory by processing rows in sorted order. The search engine
/// derives sort requirements on the group-by columns and will add a Sort enforcer
/// if the child doesn't naturally provide sorted output.
pub struct ImplStreamAggregateRule;

impl Rule for ImplStreamAggregateRule {
    fn name(&self) -> &str {
        "ImplStreamAggregate"
    }

    fn rule_type(&self) -> RuleType {
        RuleType::Implementation
    }

    fn pattern(&self) -> Pattern {
        Pattern::Operator(
            OpMatcher::LogicalOp(LogicalOpKind::Aggregate),
            vec![Pattern::Any],
        )
    }

    fn apply(
        &self,
        expr: &MemoExpr,
        _memo: &Memo,
        _ctx: &OptContext,
    ) -> Vec<RuleResult> {
        let Operator::Logical(LogicalOp::Aggregate {
            group_by,
            aggregates,
        }) = &expr.op
        else {
            return vec![];
        };

        vec![RuleResult::Substitution(
            Operator::Physical(PhysicalOp::StreamAggregate {
                group_by: group_by.clone(),
                aggregates: aggregates.clone(),
            }),
            expr.children.clone(),
        )]
    }
}
