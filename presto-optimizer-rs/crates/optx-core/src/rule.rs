//! # Rule System
//!
//! This module defines the rule trait and rule registry that drive the Cascades optimizer.
//!
//! ## Rule Types
//!
//! There are two kinds of optimization rules:
//!
//! - **Transformation rules** (`RuleType::Transformation`): Rewrite a logical operator
//!   into an equivalent logical operator. They expand the search space by generating
//!   alternatives. For example, join commutativity (A JOIN B -> B JOIN A) or predicate
//!   pushdown (Filter over Join -> Join with merged condition).
//!
//! - **Implementation rules** (`RuleType::Implementation`): Map a logical operator to
//!   one or more physical operators. For example, a logical Join may be implemented as
//!   a HashJoin, MergeJoin, or NestedLoopJoin. These rules produce the candidates that
//!   are costed during the implement phase.
//!
//! ## Pattern Matching
//!
//! Each rule declares a `Pattern` that describes the structure it matches against.
//! Before applying a rule, the search engine checks whether the current memo expression
//! matches the rule's pattern. This avoids calling `apply` on non-matching expressions.
//!
//! ## Rule Deduplication
//!
//! Each rule has a `rule_hash()` fingerprint. The memo tracks which rules have already
//! been applied to each expression to prevent infinite loops (e.g., commutativity
//! swapping back and forth) and redundant work.
//!
//! ## Rule Registry
//!
//! The `RuleRegistry` collects all rules and can include connector-specific rule sets
//! (e.g., a Hive connector might add an `ImplHiveScan` rule). Rules are keyed by
//! `source_type` so that connector-specific rules only fire for their data source.

use crate::expr::Operator;
use crate::memo::{GroupId, Memo};
use crate::pattern::Pattern;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};

/// Classification of optimization rules.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuleType {
    /// Logical → Logical transformation (e.g., join commutativity).
    Transformation,
    /// Logical → Physical implementation (e.g., join → hash join).
    Implementation,
}

/// Context passed to rules during application.
pub struct OptContext<'a> {
    pub catalog: &'a dyn crate::catalog::Catalog,
}

/// A child reference in a rule result — either an existing group or a new sub-expression.
///
/// This is the mechanism that enables rules like join associativity, which need to
/// create new intermediate groups. For example, the transformation:
///
/// ```text
/// (A ⋈ B) ⋈ C  →  A ⋈ (B ⋈ C)
/// ```
///
/// requires creating a *new group* for `B ⋈ C`. The rule expresses this by returning
/// `RuleChild::NewExpr(Join, [Group(B), Group(C)])`, and the search engine creates
/// the new group when inserting the rule result into the memo.
#[derive(Debug, Clone)]
pub enum RuleChild {
    /// Reference to an existing group in the memo.
    Group(GroupId),
    /// A new sub-expression that the search engine should place in a new group.
    /// The children of this expression may themselves be existing groups or new expressions.
    NewExpr(Operator, Vec<RuleChild>),
}

/// Result of applying a rule to an expression.
///
/// Rules can produce two kinds of results:
///
/// - `Substitution`: A new expression using existing child groups. This is the common
///   case for commutativity, implementation rules, etc. It is backward-compatible with
///   the original `(Operator, Vec<GroupId>)` return type.
///
/// - `NewChildren`: A new expression where some children are new sub-expressions
///   that need their own groups. The search engine materializes these bottom-up by
///   calling `memo.add_expr()` for each `RuleChild::NewExpr` node, creating new groups
///   as needed. Memo deduplication ensures that if the new expression already exists
///   in another group, the existing group is reused.
#[derive(Debug, Clone)]
pub enum RuleResult {
    /// Add a new expression to the current group referencing existing child groups.
    Substitution(Operator, Vec<GroupId>),
    /// Add a new expression where some children may be new sub-expressions.
    /// The search engine creates new groups for `RuleChild::NewExpr` nodes.
    NewChildren(Operator, Vec<RuleChild>),
}

/// A rule transforms or implements expressions.
pub trait Rule: Send + Sync {
    /// Unique name of this rule.
    fn name(&self) -> &str;

    /// Whether this rule is a transformation or implementation rule.
    fn rule_type(&self) -> RuleType;

    /// Pattern that this rule matches against.
    fn pattern(&self) -> Pattern;

    /// Apply the rule to a matching expression, producing new operators.
    ///
    /// Returns a list of [`RuleResult`]s. Most rules return `RuleResult::Substitution`
    /// (referencing existing child groups). Rules that need new intermediate groups
    /// (e.g., join associativity) return `RuleResult::NewChildren`.
    fn apply(
        &self,
        expr: &crate::memo::MemoExpr,
        memo: &Memo,
        ctx: &OptContext,
    ) -> Vec<RuleResult>;

    /// Hash for fingerprinting (to avoid re-applying rules).
    fn rule_hash(&self) -> u64 {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        self.name().hash(&mut hasher);
        hasher.finish()
    }
}

/// A named set of rules (e.g., for a specific connector).
pub struct RuleSet {
    pub name: String,
    pub rules: Vec<Box<dyn Rule>>,
}

/// Registry of optimization rules.
pub struct RuleRegistry {
    pub base_rules: Vec<Box<dyn Rule>>,
    pub source_rules: HashMap<String, RuleSet>,
}

impl RuleRegistry {
    pub fn new() -> Self {
        Self {
            base_rules: Vec::new(),
            source_rules: HashMap::new(),
        }
    }

    pub fn add_rule(&mut self, rule: Box<dyn Rule>) {
        self.base_rules.push(rule);
    }

    pub fn add_source_rule_set(&mut self, name: impl Into<String>, rule_set: RuleSet) {
        self.source_rules.insert(name.into(), rule_set);
    }

    /// Get all active rules for a given source type.
    pub fn active_rules(&self, source: Option<&str>) -> Vec<&dyn Rule> {
        let mut rules: Vec<&dyn Rule> = self.base_rules.iter().map(|r| r.as_ref()).collect();
        if let Some(source) = source {
            if let Some(rs) = self.source_rules.get(source) {
                rules.extend(rs.rules.iter().map(|r| r.as_ref()));
            }
        }
        rules
    }

    /// Get all transformation rules.
    pub fn transformation_rules(&self, source: Option<&str>) -> Vec<&dyn Rule> {
        self.active_rules(source)
            .into_iter()
            .filter(|r| r.rule_type() == RuleType::Transformation)
            .collect()
    }

    /// Get all implementation rules.
    pub fn implementation_rules(&self, source: Option<&str>) -> Vec<&dyn Rule> {
        self.active_rules(source)
            .into_iter()
            .filter(|r| r.rule_type() == RuleType::Implementation)
            .collect()
    }
}

impl Default for RuleRegistry {
    fn default() -> Self {
        Self::new()
    }
}
