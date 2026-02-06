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
use crate::memo::Memo;
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

/// A rule transforms or implements expressions.
pub trait Rule: Send + Sync {
    /// Unique name of this rule.
    fn name(&self) -> &str;

    /// Whether this rule is a transformation or implementation rule.
    fn rule_type(&self) -> RuleType;

    /// Pattern that this rule matches against.
    fn pattern(&self) -> Pattern;

    /// Apply the rule to a matching expression, producing new operators.
    /// Returns a list of (operator, children) pairs.
    fn apply(
        &self,
        expr: &crate::memo::MemoExpr,
        memo: &Memo,
        ctx: &OptContext,
    ) -> Vec<(Operator, Vec<crate::memo::GroupId>)>;

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
