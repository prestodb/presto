//! # Join Associativity Rule
//!
//! This rule implements the algebraic identity for inner joins:
//! `(A JOIN_1 B) JOIN_2 C = A JOIN_1 (B JOIN_2 C)`
//!
//! ## Why Associativity Matters
//!
//! Associativity changes the *shape* of the join tree (left-deep vs right-deep vs bushy).
//! Combined with commutativity, it enables exploring the full space of join orderings.
//! This is critical for queries with many tables (e.g., TPC-H Q5 with 6 tables), where
//! the optimal join order can be orders of magnitude faster than a naive one.
//!
//! For example, consider a 3-table join `(A ⋈ B) ⋈ C`:
//! - Without associativity: only commutativity swaps are explored (A⋈B)⋈C vs C⋈(A⋈B).
//! - With associativity: the optimizer also considers A⋈(B⋈C), which may be much cheaper
//!   if B and C are small and A is large.
//!
//! ## Implementation
//!
//! The rule uses [`RuleResult::NewChildren`] with [`RuleChild::NewExpr`] to create
//! new intermediate groups. For example, transforming `(A ⋈₁ B) ⋈₂ C → A ⋈₁ (B ⋈₂ C)`
//! requires creating a new memo group for `B ⋈₂ C`. The search engine materializes
//! this new group via `memo.add_expr()`, which also handles deduplication (if `B ⋈ C`
//! already exists from a different rule application, the existing group is reused).
//!
//! ## Condition Handling
//!
//! For inner joins, predicates can be freely redistributed as long as each predicate
//! is placed on a join where both sides' columns are available. The rule splits the
//! outer join's condition into:
//!
//! 1. **B-C predicates**: conjuncts whose columns all come from the inner join's right
//!    child (B) and the outer right child (C). These move to the new inner join.
//! 2. **Remaining predicates**: conjuncts that reference the inner join's left child (A).
//!    These stay on the new outer join, combined with the original inner condition.
//!
//! The transformation only fires if there is at least one B-C predicate (otherwise the
//! new inner join would be a cross join, which is rarely useful).
//!
//! ## Both Orientations
//!
//! The rule tries both orientations of the inner join expression:
//! - `(A ⋈₁ B) ⋈₂ C → A ⋈ (B ⋈ C)` when `cond₂` connects B and C
//! - `(A ⋈₁ B) ⋈₂ C → B ⋈ (A ⋈ C)` when `cond₂` connects A and C
//!
//! This avoids relying on commutativity of the inner join to fire first.
//!
//! ## Applicability
//!
//! Currently only fires for inner joins. Outer join associativity has ordering
//! constraints (e.g., `(A LEFT JOIN B) LEFT JOIN C ≠ A LEFT JOIN (B LEFT JOIN C)`)
//! that require additional validity checks.
//!
//! ## Future Work
//!
//! - **Right-to-left associativity**: `A ⋈ (B ⋈ C) → (A ⋈ B) ⋈ C`. This is the
//!   reverse direction. Currently not needed because the initial plan is left-deep,
//!   so left-to-right is the primary exploration direction. Adding a separate rule
//!   with a `Join(Any, Join(Any, Any))` pattern would complete the bidirectional search.
//! - **Outer join support**: Left/right/full outer joins have validity constraints on
//!   reordering. A correct implementation would check the "associativity matrix" from
//!   the literature (e.g., Moerkotte & Neumann, "Analysis of Two Existing and One New
//!   Dynamic Programming Algorithm for the Generation of Optimal Bushy Join Trees").
//! - **Multi-column join conditions**: When a single join edge has multiple equality
//!   predicates (e.g., `A.x = B.x AND A.y = B.y`), all of them should be handled as
//!   a unit rather than split across different joins.
//! - **DPccp algorithm**: For very large join graphs (>10 tables), a dedicated dynamic
//!   programming algorithm like DPccp may be more efficient than Cascades-style rule
//!   application for exploring all orderings.

use optx_core::expr::*;
use optx_core::memo::{GroupId, Memo, MemoExpr};
use optx_core::pattern::{OpMatcher, Pattern};
use optx_core::rule::{OptContext, Rule, RuleChild, RuleResult, RuleType};
use std::collections::HashSet;

/// Join associativity: `(A ⋈₁ B) ⋈₂ C → A ⋈ (B ⋈ C)`.
///
/// Transforms left-deep join trees into bushy or right-deep trees by reassociating
/// the join grouping. Combined with commutativity (which swaps children at each level),
/// this rule enables the optimizer to explore the full space of join orderings.
///
/// Only applies to inner joins. Uses [`RuleResult::NewChildren`] to create the new
/// intermediate group for the reassociated subtree.
pub struct JoinAssociativityRule;

impl Rule for JoinAssociativityRule {
    fn name(&self) -> &str {
        "JoinAssociativity"
    }

    fn rule_type(&self) -> RuleType {
        RuleType::Transformation
    }

    fn pattern(&self) -> Pattern {
        // Match: Join(Join(A, B), C) — left child is itself a join.
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
        expr: &MemoExpr,
        memo: &Memo,
        _ctx: &OptContext,
    ) -> Vec<RuleResult> {
        // Match: (inner_group) ⋈_outer_cond (c_group)
        let Operator::Logical(LogicalOp::Join {
            join_type: outer_join_type,
            condition: outer_cond,
        }) = &expr.op
        else {
            return vec![];
        };

        // Only inner joins for now. Outer join associativity has validity constraints
        // that require checking the "associativity matrix" from the literature.
        if *outer_join_type != JoinType::Inner {
            return vec![];
        }

        if expr.children.len() != 2 {
            return vec![];
        }

        let inner_group_id = expr.children[0]; // group containing (A ⋈ B)
        let c_group = expr.children[1]; // group containing C

        // Find an inner join expression in the inner group.
        let inner_group = memo.group(inner_group_id);
        let inner_join_info = inner_group.logical_exprs.iter().find_map(|&eid| {
            let e = memo.expr(eid);
            if let Operator::Logical(LogicalOp::Join {
                join_type,
                condition,
            }) = &e.op
            {
                if *join_type == JoinType::Inner && e.children.len() == 2 {
                    Some((e.children[0], e.children[1], condition.clone()))
                } else {
                    None
                }
            } else {
                None
            }
        });

        let Some((left_group, right_group, inner_cond)) = inner_join_info else {
            return vec![];
        };

        // Collect table names reachable from each child group.
        // These are used to determine which predicates can be pushed to the new inner join.
        let left_tables = collect_table_names(memo, left_group);
        let right_tables = collect_table_names(memo, right_group);
        let c_tables = collect_table_names(memo, c_group);

        let mut results = Vec::new();

        // Try transformation 1: left ⋈ (right ⋈ C)
        // The outer condition's predicates connecting "right" tables with C go to the new inner join.
        if let Some(result) = try_reassociate(
            &outer_cond,
            &inner_cond,
            left_group,
            right_group,
            c_group,
            &left_tables,
            &right_tables,
            &c_tables,
        ) {
            results.push(result);
        }

        // Try transformation 2: right ⋈ (left ⋈ C)
        // The outer condition's predicates connecting "left" tables with C go to the new inner join.
        // The inner condition is also swapped (it was left ⋈ right, now used as right ⋈ (left ⋈ C)).
        if let Some(result) = try_reassociate(
            &outer_cond,
            &inner_cond,
            right_group,
            left_group,
            c_group,
            &right_tables,
            &left_tables,
            &c_tables,
        ) {
            results.push(result);
        }

        results
    }
}

/// Attempt the associativity transformation:
/// `(stay_group ⋈_inner_cond move_group) ⋈_outer_cond c_group`
/// → `stay_group ⋈_new_outer (move_group ⋈_new_inner c_group)`
///
/// Returns `Some(RuleResult)` if the outer condition has at least one predicate
/// connecting `move_group` tables with `c_group` tables (avoiding a cross join).
/// Returns `None` if no valid transformation is possible.
fn try_reassociate(
    outer_cond: &Expr,
    inner_cond: &Expr,
    stay_group: GroupId,
    move_group: GroupId,
    c_group: GroupId,
    _stay_tables: &HashSet<String>,
    move_tables: &HashSet<String>,
    c_tables: &HashSet<String>,
) -> Option<RuleResult> {
    // Split the outer condition into predicates that go to the new inner join
    // (connecting move_group with c_group) vs. those that stay on the outer join.
    let outer_conjuncts = outer_cond.conjuncts();
    let mut new_inner_preds = Vec::new();
    let mut remaining_outer_preds = Vec::new();

    for pred in &outer_conjuncts {
        let tables_in_pred = referenced_tables(pred);

        // Check if all referenced tables are in move_group or c_group (not stay_group).
        let all_in_move_or_c = tables_in_pred
            .iter()
            .all(|t| move_tables.contains(t) || c_tables.contains(t));
        let has_move = tables_in_pred.iter().any(|t| move_tables.contains(t));
        let has_c = tables_in_pred.iter().any(|t| c_tables.contains(t));

        if all_in_move_or_c && has_move && has_c {
            // This predicate connects move_group with c_group — push to new inner join.
            new_inner_preds.push((*pred).clone());
        } else {
            // This predicate references stay_group — keep on the outer join.
            remaining_outer_preds.push((*pred).clone());
        }
    }

    // Must have at least one predicate for the new inner join to avoid a cross join.
    if new_inner_preds.is_empty() {
        return None;
    }

    // Build the new inner join condition (move ⋈ C).
    let new_inner_cond = make_conjunction(new_inner_preds);

    // Build the new outer join condition: original inner_cond + remaining outer predicates.
    // The inner_cond connects stay_group with move_group; since move_group is now inside
    // the new inner join, its columns are still available at the outer join level.
    let mut outer_parts: Vec<Expr> = inner_cond.conjuncts().into_iter().cloned().collect();
    outer_parts.extend(remaining_outer_preds);
    let new_outer_cond = make_conjunction(outer_parts);

    Some(RuleResult::NewChildren(
        Operator::Logical(LogicalOp::Join {
            join_type: JoinType::Inner,
            condition: new_outer_cond,
        }),
        vec![
            RuleChild::Group(stay_group),
            RuleChild::NewExpr(
                Operator::Logical(LogicalOp::Join {
                    join_type: JoinType::Inner,
                    condition: new_inner_cond,
                }),
                vec![RuleChild::Group(move_group), RuleChild::Group(c_group)],
            ),
        ],
    ))
}

/// Combine a list of predicates into a single expression using AND.
/// If there's only one predicate, returns it directly (no wrapping).
fn make_conjunction(preds: Vec<Expr>) -> Expr {
    debug_assert!(!preds.is_empty(), "make_conjunction called with empty preds");
    if preds.len() == 1 {
        preds.into_iter().next().unwrap()
    } else {
        Expr::And(preds)
    }
}

/// Collect all table names referenced by column expressions in an expression tree.
fn referenced_tables(expr: &Expr) -> HashSet<String> {
    expr.columns()
        .into_iter()
        .filter_map(|c| c.table.clone())
        .collect()
}

/// Recursively collect all table names reachable from a memo group.
///
/// Walks the logical expressions in the group, looking for Scan operators to find
/// table names. Recurses into child groups for non-leaf operators (joins, filters, etc.).
///
/// This is used to determine which predicates reference which side of a join,
/// enabling correct condition redistribution during associativity.
fn collect_table_names(memo: &Memo, group_id: GroupId) -> HashSet<String> {
    let mut tables = HashSet::new();
    collect_table_names_recursive(memo, group_id, &mut tables, &mut HashSet::new());
    tables
}

/// Helper for `collect_table_names` with cycle detection via `visited` set.
fn collect_table_names_recursive(
    memo: &Memo,
    group_id: GroupId,
    tables: &mut HashSet<String>,
    visited: &mut HashSet<GroupId>,
) {
    if !visited.insert(group_id) {
        return; // Already visited this group (cycle protection).
    }

    for &expr_id in &memo.group(group_id).logical_exprs {
        let expr = memo.expr(expr_id);
        match &expr.op {
            Operator::Logical(LogicalOp::Scan { table, .. }) => {
                tables.insert(table.name.clone());
            }
            _ => {
                // Recurse into children to find scans deeper in the tree.
                for &child_gid in &expr.children {
                    collect_table_names_recursive(memo, child_gid, tables, visited);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use optx_core::memo::Memo;

    /// Helper to create a simple equi-join condition: left_table.left_col = right_table.right_col
    fn eq_cond(left_table: &str, left_col: &str, right_table: &str, right_col: &str) -> Expr {
        Expr::BinaryOp {
            op: BinaryOp::Eq,
            left: Box::new(Expr::Column(ColumnRef {
                table: Some(left_table.into()),
                name: left_col.into(),
                index: 0,
            })),
            right: Box::new(Expr::Column(ColumnRef {
                table: Some(right_table.into()),
                name: right_col.into(),
                index: 0,
            })),
        }
    }

    /// Helper to create a scan operator for a table.
    fn scan_op(schema: &str, name: &str) -> Operator {
        Operator::Logical(LogicalOp::Scan {
            table: TableRef {
                schema: schema.into(),
                name: name.into(),
            },
            columns: vec![],
            predicate: None,
        })
    }

    #[test]
    fn test_collect_table_names() {
        let mut memo = Memo::new();
        let (g_a, _) = memo.add_expr(scan_op("s", "A"), vec![]);
        let (g_b, _) = memo.add_expr(scan_op("s", "B"), vec![]);

        let join_op = Operator::Logical(LogicalOp::Join {
            join_type: JoinType::Inner,
            condition: eq_cond("A", "x", "B", "y"),
        });
        let (g_ab, _) = memo.add_expr(join_op, vec![g_a, g_b]);

        let tables = collect_table_names(&memo, g_ab);
        assert!(tables.contains("A"));
        assert!(tables.contains("B"));
        assert_eq!(tables.len(), 2);

        let tables_a = collect_table_names(&memo, g_a);
        assert_eq!(tables_a, HashSet::from(["A".to_string()]));
    }

    #[test]
    fn test_referenced_tables() {
        let cond = eq_cond("orders", "o_custkey", "customer", "c_custkey");
        let tables = referenced_tables(&cond);
        assert!(tables.contains("orders"));
        assert!(tables.contains("customer"));
        assert_eq!(tables.len(), 2);
    }

    #[test]
    fn test_try_reassociate_valid() {
        // Setup: (A ⋈_{A.x=B.y} B) ⋈_{B.z=C.w} C
        // Expect: A ⋈_{A.x=B.y} (B ⋈_{B.z=C.w} C)
        let inner_cond = eq_cond("A", "x", "B", "y");
        let outer_cond = eq_cond("B", "z", "C", "w");

        let stay_tables = HashSet::from(["A".to_string()]);
        let move_tables = HashSet::from(["B".to_string()]);
        let c_tables = HashSet::from(["C".to_string()]);

        let result = try_reassociate(
            &outer_cond,
            &inner_cond,
            0, // stay_group (A)
            1, // move_group (B)
            2, // c_group (C)
            &stay_tables,
            &move_tables,
            &c_tables,
        );

        assert!(result.is_some(), "Expected valid reassociation");
        match result.unwrap() {
            RuleResult::NewChildren(op, children) => {
                // Outer join condition should be the inner condition (A.x = B.y)
                if let Operator::Logical(LogicalOp::Join { condition, .. }) = &op {
                    assert_eq!(*condition, inner_cond);
                } else {
                    panic!("Expected logical join operator");
                }
                assert_eq!(children.len(), 2);
                // First child is stay_group (A)
                assert!(matches!(&children[0], RuleChild::Group(0)));
                // Second child is new expr (B ⋈ C)
                assert!(matches!(&children[1], RuleChild::NewExpr(..)));
            }
            _ => panic!("Expected NewChildren result"),
        }
    }

    #[test]
    fn test_try_reassociate_no_bc_predicate() {
        // Setup: (A ⋈_{A.x=B.y} B) ⋈_{A.z=C.w} C
        // The outer condition connects A (stay) with C, not B (move) with C.
        // So this orientation should NOT produce a result.
        let inner_cond = eq_cond("A", "x", "B", "y");
        let outer_cond = eq_cond("A", "z", "C", "w");

        let stay_tables = HashSet::from(["A".to_string()]);
        let move_tables = HashSet::from(["B".to_string()]);
        let c_tables = HashSet::from(["C".to_string()]);

        let result = try_reassociate(
            &outer_cond,
            &inner_cond,
            0,
            1,
            2,
            &stay_tables,
            &move_tables,
            &c_tables,
        );

        assert!(result.is_none(), "Should not reassociate when outer cond connects stay with C");
    }

    #[test]
    fn test_try_reassociate_flipped_orientation() {
        // Same setup but try the flipped orientation:
        // (A ⋈_{A.x=B.y} B) ⋈_{A.z=C.w} C
        // With stay=B, move=A: A connects with C via outer condition → valid!
        let inner_cond = eq_cond("A", "x", "B", "y");
        let outer_cond = eq_cond("A", "z", "C", "w");

        let stay_tables = HashSet::from(["B".to_string()]);
        let move_tables = HashSet::from(["A".to_string()]);
        let c_tables = HashSet::from(["C".to_string()]);

        let result = try_reassociate(
            &outer_cond,
            &inner_cond,
            1, // stay=B
            0, // move=A
            2, // C
            &stay_tables,
            &move_tables,
            &c_tables,
        );

        assert!(result.is_some(), "Flipped orientation should work when outer connects A with C");
    }

    #[test]
    fn test_make_conjunction_single() {
        let pred = eq_cond("A", "x", "B", "y");
        let result = make_conjunction(vec![pred.clone()]);
        assert_eq!(result, pred);
    }

    #[test]
    fn test_make_conjunction_multiple() {
        let pred1 = eq_cond("A", "x", "B", "y");
        let pred2 = eq_cond("A", "z", "C", "w");
        let result = make_conjunction(vec![pred1.clone(), pred2.clone()]);
        assert!(matches!(result, Expr::And(ref v) if v.len() == 2));
    }
}
