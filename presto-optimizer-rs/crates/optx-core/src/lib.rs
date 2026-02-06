//! # optx-core: Cascades Query Optimizer Core
//!
//! This crate implements the core data structures and algorithms for a Cascades-style
//! cost-based query optimizer. It is the foundation of the Presto Rust optimizer.
//!
//! ## Module Overview
//!
//! - **`memo`**: The Memo table -- the central data structure that compactly represents
//!   the entire search space of equivalent query plans using groups and expressions.
//! - **`expr`**: Expression and operator type definitions (logical, physical, scalar).
//! - **`search`**: The Cascades top-down search algorithm with memoization.
//! - **`rule`**: The Rule trait and RuleRegistry for transformation and implementation rules.
//! - **`pattern`**: Declarative pattern matching for rule applicability checks.
//! - **`cost`**: Cost model trait and default implementation (CPU/memory/network weighted).
//! - **`stats`**: Statistics structures and derivation formulas for cardinality estimation.
//! - **`properties`**: Physical and logical property definitions (sort order, distribution).
//! - **`catalog`**: Catalog trait for accessing table metadata and statistics.

pub mod catalog;
pub mod cost;
pub mod expr;
pub mod memo;
pub mod pattern;
pub mod properties;
pub mod rule;
pub mod search;
pub mod stats;
