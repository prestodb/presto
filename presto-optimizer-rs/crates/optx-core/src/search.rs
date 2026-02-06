//! # Cascades Search Algorithm
//!
//! This module implements the core Cascades-style top-down query optimization search.
//! The Cascades framework (originally proposed by Goetz Graefe) is a cost-based optimizer
//! that uses memoization to efficiently explore the space of equivalent query plans.
//!
//! ## How It Works
//!
//! The search follows a **two-phase, top-down** approach for each group in the memo:
//!
//! 1. **Explore Phase** (`explore_group`): Apply all *transformation rules* (logical-to-logical)
//!    to generate alternative logical expressions. For example, join commutativity
//!    transforms `A JOIN B` into `B JOIN A`, and both alternatives live in the same memo group.
//!    This phase runs once per group and expands the search space.
//!
//! 2. **Implement Phase** (`implement_group`): Apply all *implementation rules*
//!    (logical-to-physical) to map each logical expression to one or more physical operators.
//!    For example, a logical Join may become a HashJoin, MergeJoin, or NestedLoopJoin.
//!    Each physical alternative is costed, and the cheapest plan for the required physical
//!    properties is recorded as the group's "winner."
//!
//! ## Cost Comparison
//!
//! Cost comparison is bottom-up within the top-down recursion: when implementing a physical
//! operator, we first recursively optimize all child groups, then compute the local cost
//! (CPU + memory + network) and add it to the children's costs. The plan with the lowest
//! total cost becomes the winner for that group and property set.
//!
//! ## Property Enforcement
//!
//! Some physical operators require specific child properties (e.g., MergeJoin requires
//! sorted input). When a required property cannot be satisfied natively, the optimizer
//! inserts **enforcer** operators (e.g., a Sort node) and costs the alternative.
//!
//! ## Termination
//!
//! The search terminates when all groups are explored and implemented, or when
//! the iteration budget (`max_iterations`) is exhausted as a safety mechanism.

use crate::catalog::Catalog;
use crate::cost::{Cost, CostModel};
use crate::expr::*;
use crate::memo::{GroupId, Memo, PlanNode, Winner};
use crate::pattern::matches;
use crate::properties::PhysicalPropertySet;
use crate::rule::{OptContext, RuleRegistry};
use crate::stats::{self, Statistics};
use std::sync::Arc;
use tracing::{debug, trace};

/// Configuration knobs for the Cascades search algorithm.
///
/// These limits prevent runaway optimization for pathologically large queries.
/// In production, `max_iterations` is the primary safety valve; `max_memo_groups`
/// prevents unbounded memory growth.
pub struct SearchConfig {
    /// Upper bound on the number of groups the memo may contain.
    pub max_memo_groups: usize,
    /// Upper bound on the total number of rule applications (transformation + implementation).
    pub max_iterations: usize,
    /// Optional connector/source type name used to select connector-specific rules.
    pub source_type: Option<String>,
}

impl Default for SearchConfig {
    fn default() -> Self {
        Self {
            max_memo_groups: 100_000,
            max_iterations: 1_000_000,
            source_type: None,
        }
    }
}

/// The Cascades search engine.
///
/// Owns the memo table and orchestrates the explore/implement phases of optimization.
/// The search is stateful: `iterations` tracks total rule applications across the
/// entire optimization to enforce the budget in `SearchConfig`.
pub struct CascadesSearch {
    /// The memo table storing all groups and expressions in the search space.
    pub memo: Memo,
    /// Registry of transformation and implementation rules to apply.
    pub rule_registry: Arc<RuleRegistry>,
    /// Cost model used to estimate the expense of physical operators.
    pub cost_model: Arc<dyn CostModel>,
    /// Catalog providing table metadata and statistics.
    pub catalog: Arc<dyn Catalog>,
    /// Configuration limits for the search.
    pub config: SearchConfig,
    /// Running count of rule applications (shared across all groups).
    iterations: usize,
}

impl CascadesSearch {
    pub fn new(
        memo: Memo,
        rule_registry: Arc<RuleRegistry>,
        cost_model: Arc<dyn CostModel>,
        catalog: Arc<dyn Catalog>,
        config: SearchConfig,
    ) -> Self {
        Self {
            memo,
            rule_registry,
            cost_model,
            catalog,
            config,
            iterations: 0,
        }
    }

    /// Run the Cascades optimization starting from the root group.
    pub fn optimize(
        &mut self,
        root_group: GroupId,
        required_props: &PhysicalPropertySet,
    ) -> Option<PlanNode> {
        debug!(
            "Starting Cascades optimization: root_group={}, groups={}, exprs={}",
            root_group,
            self.memo.num_groups(),
            self.memo.num_exprs()
        );

        self.optimize_group(root_group, required_props);

        let plan = self.memo.extract_best_plan(root_group, required_props);
        if let Some(ref p) = plan {
            debug!("Optimization complete: cost={:.1}, iterations={}", p.cost.total, self.iterations);
        } else {
            debug!("Optimization failed: no valid plan found");
        }
        plan
    }

    /// Top-down optimization of a group with required physical properties.
    ///
    /// This is the heart of the Cascades algorithm. For a given group and a set of
    /// required physical properties (e.g., "output must be sorted by col X"), we:
    ///
    /// 1. Check the memoized winner cache -- if we already found the best plan for
    ///    these exact properties, skip all work (this is what makes Cascades efficient).
    /// 2. Explore the group to generate all logically equivalent alternatives.
    /// 3. Implement the group to find the cheapest physical realization.
    fn optimize_group(&mut self, group_id: GroupId, required_props: &PhysicalPropertySet) {
        // Memoization check: avoid redundant work if we already solved this subproblem.
        // The key insight is that (group_id, required_props) uniquely identifies a
        // subproblem in the optimization, analogous to a DP table entry.
        if self.memo.group(group_id).best_plan.contains_key(required_props) {
            return;
        }

        if self.iterations >= self.config.max_iterations {
            debug!("Hit iteration limit");
            return;
        }

        // Phase 1: Explore the group -- apply transformation rules to expand the
        // logical search space with equivalent alternatives (e.g., join reorderings).
        self.explore_group(group_id);

        // Phase 2: Implement -- apply implementation rules to create physical operators,
        // recursively optimize children, compute costs, and record the winner.
        self.implement_group(group_id, required_props);
    }

    /// Explore a group: apply all transformation rules to generate logical alternatives.
    ///
    /// Exploration is idempotent per group -- once a group is marked `explored`, we
    /// never re-explore it. This is safe because transformation rules only add new
    /// expressions to existing groups (they never create new groups), so a single
    /// pass over all logical expressions is sufficient.
    ///
    /// The exploration proceeds as follows:
    /// 1. Snapshot the current logical expressions (because rules may add new ones).
    /// 2. For each expression, try every transformation rule.
    /// 3. If the rule's pattern matches and it hasn't been applied before, fire it.
    /// 4. New equivalent expressions are added to the same group.
    /// 5. Recursively explore child groups so that alternatives propagate bottom-up.
    fn explore_group(&mut self, group_id: GroupId) {
        if self.memo.group(group_id).explored {
            return;
        }
        self.memo.group_mut(group_id).explored = true;

        // Snapshot: we iterate over a frozen copy of logical_exprs because rule
        // application may add new expressions to this group during the loop.
        let logical_exprs: Vec<_> = self.memo.group(group_id).logical_exprs.clone();
        let source = self.config.source_type.clone();

        for expr_id in logical_exprs {
            // Collect rule metadata up front to avoid borrowing conflicts with the memo.
            // We need the name, hash, and pattern to decide whether to apply each rule.
            let rules: Vec<_> = self
                .rule_registry
                .transformation_rules(source.as_deref())
                .into_iter()
                .map(|r| (r.name().to_string(), r.rule_hash(), r.pattern().clone()))
                .collect();

            for (rule_name, rule_hash, pattern) in &rules {
                // Skip rules already applied to this expression (prevents infinite loops
                // like commutativity: A JOIN B -> B JOIN A -> A JOIN B -> ...).
                if self.memo.rule_applied(expr_id, *rule_hash) {
                    continue;
                }
                // Skip rules whose structural pattern doesn't match the expression.
                if !matches(&self.memo, expr_id, pattern) {
                    continue;
                }

                self.iterations += 1;
                if self.iterations >= self.config.max_iterations {
                    return;
                }

                trace!("Applying transformation rule '{}' to expr {}", rule_name, expr_id);

                let ctx = OptContext {
                    catalog: self.catalog.as_ref(),
                };

                // Re-locate the rule by hash to call apply(). This two-pass approach
                // (collect metadata, then find-and-apply) avoids holding a borrow on
                // the rule registry while also borrowing the memo.
                let results: Vec<_> = {
                    let rule = self
                        .rule_registry
                        .transformation_rules(source.as_deref())
                        .into_iter()
                        .find(|r| r.rule_hash() == *rule_hash)
                        .unwrap();
                    rule.apply(self.memo.expr(expr_id), &self.memo, &ctx)
                };

                // Mark the rule as applied *before* inserting results, so that even
                // if a result is the same expression, we won't re-apply this rule.
                self.memo.mark_rule_applied(expr_id, *rule_hash);

                // Insert each new equivalent expression into the same group.
                // The memo handles deduplication internally.
                for (new_op, new_children) in results {
                    let new_expr_id =
                        self.memo
                            .add_expr_to_group(group_id, new_op, new_children);
                    trace!("  Created new expr {} in group {}", new_expr_id, group_id);
                }

                // Recursively explore child groups so that their alternatives are
                // available when we later implement this group's expressions.
                let children: Vec<_> = self.memo.expr(expr_id).children.clone();
                for child_gid in children {
                    self.explore_group(child_gid);
                }
            }
        }
    }

    /// Implement a group: apply implementation rules and find the cheapest physical plan.
    ///
    /// For each logical expression in the group, we try every implementation rule.
    /// Each rule may produce one or more physical alternatives (e.g., ImplHashJoinRule
    /// produces two: build-left and build-right). For each physical alternative:
    ///
    /// 1. Derive what physical properties each child must satisfy (e.g., MergeJoin
    ///    requires both children sorted on join keys).
    /// 2. Recursively optimize each child group with those required properties.
    /// 3. If all children are feasible (have a winner), compute the total cost.
    /// 4. If this cost beats the current best, record it as the new winner.
    ///
    /// After considering all implementation rules, we also check whether adding an
    /// enforcer (e.g., a Sort operator) on top of an unsorted plan might be cheaper
    /// than any plan that natively satisfies the sort requirement.
    fn implement_group(&mut self, group_id: GroupId, required_props: &PhysicalPropertySet) {
        let logical_exprs: Vec<_> = self.memo.group(group_id).logical_exprs.clone();
        let source = self.config.source_type.clone();
        let mut best_cost = Cost::infinite();

        // Statistics must be available before costing; derive them lazily.
        self.derive_group_stats(group_id);

        for expr_id in &logical_exprs {
            let rules: Vec<_> = self
                .rule_registry
                .implementation_rules(source.as_deref())
                .into_iter()
                .map(|r| (r.name().to_string(), r.rule_hash(), r.pattern().clone()))
                .collect();

            for (rule_name, rule_hash, pattern) in &rules {
                if self.memo.rule_applied(*expr_id, *rule_hash) {
                    continue;
                }
                if !matches(&self.memo, *expr_id, pattern) {
                    continue;
                }

                self.iterations += 1;
                if self.iterations >= self.config.max_iterations {
                    return;
                }

                trace!("Applying implementation rule '{}' to expr {}", rule_name, expr_id);

                let ctx = OptContext {
                    catalog: self.catalog.as_ref(),
                };
                let results: Vec<_> = {
                    let rule = self
                        .rule_registry
                        .implementation_rules(source.as_deref())
                        .into_iter()
                        .find(|r| r.rule_hash() == *rule_hash)
                        .unwrap();
                    rule.apply(self.memo.expr(*expr_id), &self.memo, &ctx)
                };

                self.memo.mark_rule_applied(*expr_id, *rule_hash);

                for (phys_op, phys_children) in results {
                    let phys_expr_id =
                        self.memo
                            .add_expr_to_group(group_id, phys_op, phys_children.clone());

                    // Determine what physical properties each child must provide.
                    // For example, MergeJoin requires both children sorted on join keys,
                    // while HashJoin has no ordering requirement on its children.
                    let child_required = self.derive_child_required_props(
                        phys_expr_id,
                        required_props,
                    );

                    let mut child_costs = Vec::new();
                    let mut child_stats_list = Vec::new();
                    let mut feasible = true;

                    // Recursively optimize each child with its required properties.
                    // If any child cannot produce a valid plan, this physical alternative
                    // is infeasible and we skip it.
                    for (i, &child_gid) in phys_children.iter().enumerate() {
                        let child_props = child_required
                            .get(i)
                            .cloned()
                            .unwrap_or_else(PhysicalPropertySet::any);

                        self.optimize_group(child_gid, &child_props);

                        if let Some(winner) = self.memo.group(child_gid).best_plan.get(&child_props)
                        {
                            child_costs.push(winner.cost);
                            child_stats_list.push(
                                self.memo.group(child_gid).stats.clone().unwrap_or_else(|| {
                                    Statistics::new(1000.0, 100000.0)
                                }),
                            );
                        } else {
                            feasible = false;
                            break;
                        }
                    }

                    if !feasible {
                        continue;
                    }

                    let stats_refs: Vec<&Statistics> = child_stats_list.iter().collect();

                    // Compute total cost = local operator cost + sum of children costs.
                    // The cost model uses child statistics (row counts, sizes) to estimate
                    // the local cost of this physical operator.
                    let cost = self.cost_model.compute_cost(
                        match &self.memo.expr(phys_expr_id).op {
                            Operator::Physical(p) => p,
                            _ => continue,
                        },
                        &stats_refs,
                        &child_costs,
                        required_props,
                    );

                    // Update the group's winner if this plan is cheaper.
                    if cost.total < best_cost.total {
                        best_cost = cost;
                        self.memo.group_mut(group_id).best_plan.insert(
                            required_props.clone(),
                            Winner {
                                expr_id: phys_expr_id,
                                cost,
                            },
                        );
                        trace!(
                            "  New best for group {} with props {:?}: cost={:.1}",
                            group_id,
                            required_props,
                            cost.total
                        );
                    }
                }
            }
        }

        // After exhausting all implementation rules, check if an enforcer (e.g., adding
        // a sort on top of an unsorted plan) would be cheaper than any natively-sorted plan.
        if required_props.sort_order.is_some() {
            self.try_sort_enforcer(group_id, required_props, &mut best_cost);
        }
    }

    /// Try adding a sort enforcer to satisfy sort requirements.
    ///
    /// This implements the "enforcer" pattern from Cascades: instead of requiring
    /// every physical operator to produce sorted output, we consider adding an
    /// explicit Sort on top of the best unsorted plan. If that's cheaper than any
    /// plan that natively provides the sort order, we use the enforcer instead.
    fn try_sort_enforcer(
        &mut self,
        group_id: GroupId,
        required_props: &PhysicalPropertySet,
        best_cost: &mut Cost,
    ) {
        let any_props = PhysicalPropertySet::any();

        // Optimize the group without sort requirement
        if !self.memo.group(group_id).best_plan.contains_key(&any_props) {
            // Already optimized above or there's nothing to enforce on
            return;
        }

        if let Some(winner) = self.memo.group(group_id).best_plan.get(&any_props).cloned() {
            // Add a sort enforcer on top
            if let Some(ref sort_order) = required_props.sort_order {
                let sort_op = Operator::Physical(PhysicalOp::SortOp {
                    order: sort_order.clone(),
                });
                let sort_expr_id =
                    self.memo
                        .add_expr_to_group(group_id, sort_op, vec![group_id]);

                // Cost of sort on top of the unsorted plan
                let group_stats = self
                    .memo
                    .group(group_id)
                    .stats
                    .clone()
                    .unwrap_or_else(|| Statistics::new(1000.0, 100000.0));
                let sort_cost = self.cost_model.compute_cost(
                    &PhysicalOp::SortOp {
                        order: sort_order.clone(),
                    },
                    &[&group_stats],
                    &[winner.cost],
                    required_props,
                );

                if sort_cost.total < best_cost.total {
                    *best_cost = sort_cost;
                    self.memo.group_mut(group_id).best_plan.insert(
                        required_props.clone(),
                        Winner {
                            expr_id: sort_expr_id,
                            cost: sort_cost,
                        },
                    );
                }
            }
        }
    }

    /// Derive the required physical properties for each child of a physical operator.
    ///
    /// Different physical operators impose different requirements on their children.
    /// For example:
    /// - MergeJoin requires both children sorted on the join key columns.
    /// - StreamAggregate requires its input sorted on the group-by columns.
    /// - HashJoin, SeqScan, etc. have no ordering requirements ("any" properties).
    ///
    /// These child requirements drive the recursive optimization: when we optimize
    /// a child group, we optimize it for the specific properties this parent demands.
    fn derive_child_required_props(
        &self,
        expr_id: crate::memo::ExprId,
        _parent_required: &PhysicalPropertySet,
    ) -> Vec<PhysicalPropertySet> {
        let expr = self.memo.expr(expr_id);
        match &expr.op {
            Operator::Physical(PhysicalOp::MergeJoin { condition, .. }) => {
                // Merge join requires both sides sorted on join keys
                let sort_keys = self.extract_join_sort_keys(condition);
                vec![
                    PhysicalPropertySet::with_sort(sort_keys.clone()),
                    PhysicalPropertySet::with_sort(sort_keys),
                ]
            }
            Operator::Physical(PhysicalOp::StreamAggregate { group_by, .. }) => {
                // Stream aggregate requires input sorted on group-by keys
                let sort_keys: Vec<SortKey> = group_by
                    .iter()
                    .map(|e| SortKey {
                        expr: e.clone(),
                        ascending: true,
                        nulls_first: false,
                    })
                    .collect();
                vec![PhysicalPropertySet::with_sort(sort_keys)]
            }
            _ => {
                // Most physical operators don't require specific child properties
                expr.children.iter().map(|_| PhysicalPropertySet::any()).collect()
            }
        }
    }

    /// Extract sort keys from an equi-join condition for use in merge join ordering.
    /// For `A.x = B.y`, the sort key is the left-hand column in ascending order.
    fn extract_join_sort_keys(&self, condition: &Expr) -> Vec<SortKey> {
        match condition {
            Expr::BinaryOp {
                op: BinaryOp::Eq,
                left,
                ..
            } => {
                vec![SortKey {
                    expr: *left.clone(),
                    ascending: true,
                    nulls_first: false,
                }]
            }
            _ => vec![],
        }
    }

    /// Derive statistics for a group from its logical expressions.
    ///
    /// Statistics are derived lazily (only when needed for costing) and cached in the
    /// group. We use the first logical expression as the representative -- all logical
    /// expressions in a group are equivalent, so they share the same statistics.
    fn derive_group_stats(&mut self, group_id: GroupId) {
        if self.memo.group(group_id).stats.is_some() {
            return;
        }

        // Use the first logical expression as the representative to derive stats.
        // Since all expressions in a group are logically equivalent, any one suffices.
        let logical_exprs = self.memo.group(group_id).logical_exprs.clone();
        if let Some(&expr_id) = logical_exprs.first() {
            let stats = self.derive_expr_stats(expr_id);
            self.memo.group_mut(group_id).stats = Some(stats);
        }
    }

    /// Derive statistics for a single expression by dispatching on operator type.
    ///
    /// Each operator type has its own derivation formula:
    /// - **Scan**: Fetches base table stats from the catalog.
    /// - **Filter**: Applies estimated selectivity to the child's row count.
    /// - **Join**: Uses the standard formula |A JOIN B| = |A| * |B| / max(ndv_A, ndv_B).
    /// - **Aggregate**: Estimates output rows as the product of group-by column NDVs.
    /// - **Project/Sort/Limit**: Passes through child stats (row count unchanged).
    ///
    /// When catalog stats are unavailable, sensible defaults (1000 rows, 100KB) are used
    /// so that the optimizer can still make relative cost comparisons.
    fn derive_expr_stats(&mut self, expr_id: crate::memo::ExprId) -> Statistics {
        let expr = self.memo.expr(expr_id);
        let op = expr.op.clone();
        let children = expr.children.clone();

        match &op {
            Operator::Logical(LogicalOp::Scan { table, .. }) => {
                // Get stats from catalog
                self.catalog
                    .get_table_stats(table)
                    .unwrap_or_else(|| Statistics::new(1000.0, 100000.0))
            }
            Operator::Logical(LogicalOp::Filter { predicate }) => {
                if let Some(&child_gid) = children.first() {
                    self.derive_group_stats(child_gid);
                    let child_stats = self
                        .memo
                        .group(child_gid)
                        .stats
                        .clone()
                        .unwrap_or_else(|| Statistics::new(1000.0, 100000.0));
                    let selectivity = self.estimate_selectivity(predicate, &child_stats);
                    stats::derive_filter_stats(&child_stats, selectivity)
                } else {
                    Statistics::new(100.0, 10000.0)
                }
            }
            Operator::Logical(LogicalOp::Join { condition, .. }) => {
                if children.len() >= 2 {
                    self.derive_group_stats(children[0]);
                    self.derive_group_stats(children[1]);
                    let left_stats = self
                        .memo
                        .group(children[0])
                        .stats
                        .clone()
                        .unwrap_or_else(|| Statistics::new(1000.0, 100000.0));
                    let right_stats = self
                        .memo
                        .group(children[1])
                        .stats
                        .clone()
                        .unwrap_or_else(|| Statistics::new(1000.0, 100000.0));

                    let join_cols = self.extract_equi_join_columns(condition);
                    stats::derive_join_stats(&left_stats, &right_stats, &join_cols)
                } else {
                    Statistics::new(10000.0, 1000000.0)
                }
            }
            Operator::Logical(LogicalOp::Aggregate { group_by, .. }) => {
                if let Some(&child_gid) = children.first() {
                    self.derive_group_stats(child_gid);
                    let child_stats = self
                        .memo
                        .group(child_gid)
                        .stats
                        .clone()
                        .unwrap_or_else(|| Statistics::new(1000.0, 100000.0));
                    let group_cols: Vec<String> = group_by
                        .iter()
                        .filter_map(|e| match e {
                            Expr::Column(c) => Some(c.name.clone()),
                            _ => None,
                        })
                        .collect();
                    stats::derive_aggregate_stats(&child_stats, &group_cols)
                } else {
                    Statistics::new(100.0, 10000.0)
                }
            }
            Operator::Logical(LogicalOp::Project { .. })
            | Operator::Logical(LogicalOp::Sort { .. })
            | Operator::Logical(LogicalOp::Limit { .. }) => {
                // Pass through child stats
                if let Some(&child_gid) = children.first() {
                    self.derive_group_stats(child_gid);
                    self.memo
                        .group(child_gid)
                        .stats
                        .clone()
                        .unwrap_or_else(|| Statistics::new(1000.0, 100000.0))
                } else {
                    Statistics::new(1000.0, 100000.0)
                }
            }
            _ => Statistics::new(1000.0, 100000.0),
        }
    }

    /// Estimate the selectivity of a predicate expression.
    ///
    /// Selectivity is a value in [0, 1] representing the fraction of input rows that
    /// pass the predicate. The estimation uses standard heuristics:
    ///
    /// - **Equality (col = value)**: selectivity = 1 / NDV(col). This assumes a uniform
    ///   distribution of values. If NDV is unknown, falls back to 0.1 (10%).
    /// - **Range (col < value, etc.)**: uses a fixed estimate of 0.33 (1/3 of rows).
    ///   A more sophisticated model could use histograms for tighter bounds.
    /// - **AND (conjunction)**: assumes predicate independence and multiplies selectivities.
    ///   This can underestimate for correlated predicates but is standard practice.
    /// - **OR (disjunction)**: uses the inclusion-exclusion formula:
    ///   sel(A OR B) = 1 - (1 - sel(A)) * (1 - sel(B)).
    /// - **Unknown predicates**: fall back to the default selectivity of 0.1.
    fn estimate_selectivity(&self, expr: &Expr, stats: &Statistics) -> f64 {
        match expr {
            Expr::BinaryOp {
                op: BinaryOp::Eq,
                left,
                right,
            } => {
                // Equality selectivity: 1/NDV assumes uniform value distribution.
                if let Expr::Column(c) = left.as_ref() {
                    return stats::equality_selectivity(stats, &c.name);
                }
                if let Expr::Column(c) = right.as_ref() {
                    return stats::equality_selectivity(stats, &c.name);
                }
                stats::DEFAULT_FILTER_SELECTIVITY
            }
            Expr::BinaryOp {
                op: BinaryOp::Lt | BinaryOp::LtEq | BinaryOp::Gt | BinaryOp::GtEq,
                ..
            } => {
                // Range predicates: fixed 1/3 heuristic (could be refined with histograms).
                0.33
            }
            Expr::And(conjuncts) => {
                // Independence assumption: P(A AND B) = P(A) * P(B).
                conjuncts
                    .iter()
                    .map(|c| self.estimate_selectivity(c, stats))
                    .product()
            }
            Expr::Or(disjuncts) => {
                // Inclusion-exclusion: P(A OR B) = 1 - (1 - P(A)) * (1 - P(B)).
                let product: f64 = disjuncts
                    .iter()
                    .map(|d| 1.0 - self.estimate_selectivity(d, stats))
                    .product();
                1.0 - product
            }
            _ => stats::DEFAULT_FILTER_SELECTIVITY,
        }
    }

    /// Extract equi-join column pairs from a join condition.
    ///
    /// Given a condition like `A.x = B.y AND A.z = B.w`, returns
    /// `[("x", "y"), ("z", "w")]`. These pairs are used to estimate join selectivity
    /// via the NDV-based formula.
    fn extract_equi_join_columns(&self, condition: &Expr) -> Vec<(String, String)> {
        let mut pairs = Vec::new();
        self.collect_equi_join_columns(condition, &mut pairs);
        pairs
    }

    fn collect_equi_join_columns(&self, expr: &Expr, pairs: &mut Vec<(String, String)>) {
        match expr {
            Expr::BinaryOp {
                op: BinaryOp::Eq,
                left,
                right,
            } => {
                if let (Expr::Column(l), Expr::Column(r)) = (left.as_ref(), right.as_ref()) {
                    pairs.push((l.name.clone(), r.name.clone()));
                }
            }
            Expr::And(conjuncts) => {
                for c in conjuncts {
                    self.collect_equi_join_columns(c, pairs);
                }
            }
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::InMemoryCatalog;
    use crate::cost::DefaultCostModel;
    use crate::stats::ColumnStatistics;

    #[test]
    fn test_search_config_defaults() {
        let config = SearchConfig::default();
        assert_eq!(config.max_memo_groups, 100_000);
        assert_eq!(config.max_iterations, 1_000_000);
        assert!(config.source_type.is_none());
    }
}
