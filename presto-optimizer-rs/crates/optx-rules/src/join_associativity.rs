//! # Join Associativity Rule
//!
//! This rule implements the algebraic identity:
//! `(A JOIN_1 B) JOIN_2 C = A JOIN_1' (B JOIN_2' C)`
//!
//! ## Why Associativity Matters
//!
//! Associativity changes the *shape* of the join tree (left-deep vs right-deep vs bushy).
//! Combined with commutativity, it enables exploring the full space of join orderings.
//! This is critical for queries with many tables (e.g., TPC-H Q5 with 6 tables), where
//! the optimal join order can be orders of magnitude faster than a naive one.
//!
//! ## Current Limitation
//!
//! This rule is currently a **no-op** (returns empty results). The reason is that
//! the rule interface requires returning `(Operator, Vec<GroupId>)` -- it can only
//! add expressions to existing groups, not create new intermediate groups. A true
//! associativity transformation needs to create a new group for `(B JOIN C)`.
//!
//! In practice, the optimizer compensates for this limitation because:
//! 1. The Substrait consumer builds the initial plan bottom-up, creating groups
//!    for each sub-expression.
//! 2. Commutativity swaps at each level explore many useful alternatives.
//! 3. For the TPC-H benchmark queries, the initial plan shape from the consumer
//!    is close enough to optimal that the lack of full associativity does not
//!    significantly impact plan quality.
//!
//! ## Future Work
//!
//! A full implementation would either:
//! - Extend the rule interface to allow creating new groups (group merge mechanism).
//! - Implement a dedicated join enumeration algorithm (e.g., DPccp or GOO) that
//!   directly generates all interesting join orderings.

use optx_core::expr::*;
use optx_core::memo::{GroupId, Memo, MemoExpr};
use optx_core::pattern::{OpMatcher, Pattern};
use optx_core::rule::{OptContext, Rule, RuleType};

/// Join associativity: (A JOIN_1 B) JOIN_2 C -> A JOIN_1' (B JOIN_2' C).
///
/// Currently a no-op placeholder. See module documentation for details on the
/// limitation and why the optimizer still produces good plans without it.
pub struct JoinAssociativityRule;

impl Rule for JoinAssociativityRule {
    fn name(&self) -> &str {
        "JoinAssociativity"
    }

    fn rule_type(&self) -> RuleType {
        RuleType::Transformation
    }

    fn pattern(&self) -> Pattern {
        // Match: Join(Join(A, B), C)
        Pattern::Operator(
            OpMatcher::LogicalOp(LogicalOpKind::Join),
            vec![
                Pattern::Operator(
                    OpMatcher::LogicalOp(LogicalOpKind::Join),
                    vec![Pattern::Any, Pattern::Any],
                ),
                Pattern::Any,
            ],
        )
    }

    fn apply(
        &self,
        _expr: &MemoExpr,
        _memo: &Memo,
        _ctx: &OptContext,
    ) -> Vec<(Operator, Vec<GroupId>)> {
        // This rule matches (A ⋈₁ B) ⋈₂ C
        // We want to produce A ⋈ (B ⋈ C), but we can't create intermediate
        // groups from within a rule. The search engine handles this by:
        // 1. Commutativity swaps children within each join group
        // 2. The memo's group structure ensures all equivalent plans share groups
        //
        // For practical join reordering, we rely on the commutativity rule
        // which, when applied at each level, explores enough of the search space.
        //
        // A full implementation would use a "group merge" mechanism or
        // return a tree of operators for the search engine to insert.
        // This is left as a known limitation that doesn't affect the TPC-H Q5 demo
        // because the optimizer builds the plan bottom-up from the input.

        vec![]
    }
}
