//! # Declarative Pattern Matching for Optimization Rules
//!
//! This module provides a simple pattern language for matching expressions in the memo.
//! Each optimization rule declares a `Pattern` that describes the shape of expressions
//! it can transform. Before applying a rule, the search engine checks the pattern to
//! avoid calling `apply()` on non-matching expressions.
//!
//! ## Pattern Language
//!
//! - `Pattern::Operator(matcher, children)`: matches an expression whose operator
//!   satisfies `matcher` and whose children match the given child patterns. This is
//!   used for structural matching (e.g., "a Filter on top of a Join with two children").
//!
//! - `Pattern::Any`: matches any expression or group. Used as a wildcard for children
//!   that the rule doesn't inspect. This is the most common child pattern.
//!
//! - `Pattern::Leaf`: matches only leaf expressions (those with no children, like Scan).
//!
//! ## Group-Level Matching
//!
//! When a child pattern is not `Any`, the matcher checks all expressions in the child
//! group (both logical and physical). A child pattern matches if *any* expression in
//! the child group satisfies it. This is correct because all expressions in a group
//! are logically equivalent.
//!
//! ## Convenience Constructors
//!
//! Common patterns have named constructors (e.g., `Pattern::join()`, `Pattern::filter()`)
//! to reduce boilerplate in rule definitions.

use crate::expr::{LogicalOpKind, PhysicalOpKind, Operator};
use crate::memo::{ExprId, Memo};

/// Pattern for matching expressions in the memo.
#[derive(Debug, Clone)]
pub enum Pattern {
    /// Match an operator with child patterns.
    Operator(OpMatcher, Vec<Pattern>),
    /// Match any subtree (group).
    Any,
    /// Match a leaf node (no children).
    Leaf,
}

/// Matcher for operator types (without data).
#[derive(Debug, Clone)]
pub enum OpMatcher {
    LogicalOp(LogicalOpKind),
    PhysicalOp(PhysicalOpKind),
    AnyLogical,
    AnyPhysical,
}

impl Pattern {
    /// Create a pattern that matches a logical join with two any-children.
    pub fn join() -> Self {
        Pattern::Operator(
            OpMatcher::LogicalOp(LogicalOpKind::Join),
            vec![Pattern::Any, Pattern::Any],
        )
    }

    /// Create a pattern that matches a logical join where the left child is also a join.
    pub fn join_join_left() -> Self {
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

    /// Match a logical scan.
    pub fn scan() -> Self {
        Pattern::Operator(OpMatcher::LogicalOp(LogicalOpKind::Scan), vec![])
    }

    /// Match a logical filter with one child.
    pub fn filter() -> Self {
        Pattern::Operator(
            OpMatcher::LogicalOp(LogicalOpKind::Filter),
            vec![Pattern::Any],
        )
    }

    /// Match a logical aggregate.
    pub fn aggregate() -> Self {
        Pattern::Operator(
            OpMatcher::LogicalOp(LogicalOpKind::Aggregate),
            vec![Pattern::Any],
        )
    }

    /// Match a logical sort.
    pub fn sort() -> Self {
        Pattern::Operator(
            OpMatcher::LogicalOp(LogicalOpKind::Sort),
            vec![Pattern::Any],
        )
    }

    /// Match a filter on top of a join.
    pub fn filter_join() -> Self {
        Pattern::Operator(
            OpMatcher::LogicalOp(LogicalOpKind::Filter),
            vec![Pattern::Operator(
                OpMatcher::LogicalOp(LogicalOpKind::Join),
                vec![Pattern::Any, Pattern::Any],
            )],
        )
    }

    /// Match a project with one child.
    pub fn project() -> Self {
        Pattern::Operator(
            OpMatcher::LogicalOp(LogicalOpKind::Project),
            vec![Pattern::Any],
        )
    }
}

/// Check if a memo expression matches a pattern.
pub fn matches(memo: &Memo, expr_id: ExprId, pattern: &Pattern) -> bool {
    let expr = memo.expr(expr_id);
    match pattern {
        Pattern::Any => true,
        Pattern::Leaf => expr.children.is_empty(),
        Pattern::Operator(matcher, child_patterns) => {
            // Check operator type matches
            let op_matches = match (&expr.op, matcher) {
                (Operator::Logical(l), OpMatcher::LogicalOp(kind)) => l.kind() == *kind,
                (Operator::Physical(p), OpMatcher::PhysicalOp(kind)) => p.kind() == *kind,
                (Operator::Logical(_), OpMatcher::AnyLogical) => true,
                (Operator::Physical(_), OpMatcher::AnyPhysical) => true,
                _ => false,
            };
            if !op_matches {
                return false;
            }

            // Check children count
            if expr.children.len() != child_patterns.len() {
                return false;
            }

            // Check each child pattern against the child group.
            // For each child group, at least one expression in the group must match.
            for (child_gid, child_pattern) in expr.children.iter().zip(child_patterns.iter()) {
                match child_pattern {
                    Pattern::Any => continue,
                    _ => {
                        let group = memo.group(*child_gid);
                        let any_match = group
                            .logical_exprs
                            .iter()
                            .chain(group.physical_exprs.iter())
                            .any(|&eid| matches(memo, eid, child_pattern));
                        if !any_match {
                            return false;
                        }
                    }
                }
            }

            true
        }
    }
}
