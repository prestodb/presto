//! # Expression and Operator Types
//!
//! This module defines the complete type system for the optimizer's plan representation.
//! It is organized into three layers:
//!
//! ## Scalar Expressions (`Expr`)
//! Scalar expressions represent computations on individual rows: column references,
//! literal values, arithmetic operations, comparisons, boolean logic, and function calls.
//! They appear inside predicates, projections, join conditions, and sort keys.
//!
//! ## Logical Operators (`LogicalOp`)
//! Logical operators describe *what* to compute without specifying *how*. A logical
//! `Join` says "combine these two relations on this condition" but does not specify
//! whether to use a hash join, merge join, or nested loop join. Transformation rules
//! operate on logical operators to generate equivalent alternatives (e.g., reordering joins).
//!
//! ## Physical Operators (`PhysicalOp`)
//! Physical operators describe *how* to execute a computation. They are produced by
//! implementation rules from logical operators. Each physical operator has well-defined
//! cost characteristics (e.g., HashJoin has O(n) build + O(m) probe, while NestedLoopJoin
//! has O(n*m) cost).
//!
//! ## Unified `Operator` Enum
//! The `Operator` enum wraps both logical and physical operators so that the memo can
//! store them uniformly. The `OpKind` discriminant allows pattern matching on operator
//! type without inspecting the operator's data fields.

use ordered_float::OrderedFloat;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::hash::{Hash, Hasher};

/// Reference to a table in the catalog.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TableRef {
    pub schema: String,
    pub name: String,
}

impl fmt::Display for TableRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}", self.schema, self.name)
    }
}

/// Reference to a column.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ColumnRef {
    pub table: Option<String>,
    pub name: String,
    pub index: u32,
}

impl fmt::Display for ColumnRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(ref t) = self.table {
            write!(f, "{}.{}", t, self.name)
        } else {
            write!(f, "{}", self.name)
        }
    }
}

/// Scalar value for expressions.
///
/// Represents constant values that appear in SQL queries (e.g., `WHERE x = 42`).
/// Uses `OrderedFloat` for `f64` so that floating-point values can be used as
/// hash map keys and in Eq/Hash comparisons (needed for memo deduplication).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ScalarValue {
    /// SQL NULL value.
    Null,
    /// Boolean true/false.
    Bool(bool),
    /// 64-bit signed integer.
    Int64(i64),
    /// 64-bit floating point, wrapped in OrderedFloat for Eq/Hash support.
    Float64(OrderedFloat<f64>),
    /// UTF-8 string.
    Utf8(String),
    /// Date as days since Unix epoch (1970-01-01).
    Date(i32),
}

impl PartialEq for ScalarValue {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Null, Self::Null) => true,
            (Self::Bool(a), Self::Bool(b)) => a == b,
            (Self::Int64(a), Self::Int64(b)) => a == b,
            (Self::Float64(a), Self::Float64(b)) => a == b,
            (Self::Utf8(a), Self::Utf8(b)) => a == b,
            (Self::Date(a), Self::Date(b)) => a == b,
            _ => false,
        }
    }
}

impl Eq for ScalarValue {}

impl Hash for ScalarValue {
    fn hash<H: Hasher>(&self, state: &mut H) {
        std::mem::discriminant(self).hash(state);
        match self {
            Self::Null => {}
            Self::Bool(v) => v.hash(state),
            Self::Int64(v) => v.hash(state),
            Self::Float64(v) => v.hash(state),
            Self::Utf8(v) => v.hash(state),
            Self::Date(v) => v.hash(state),
        }
    }
}

/// Scalar expressions used in predicates, projections, join conditions, etc.
///
/// This is a recursive tree representing a single scalar computation. Expressions
/// are stored inside operator nodes (e.g., a Filter's predicate, a Join's condition,
/// a Project's output expressions). They are also used in the `SortKey` definition.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Expr {
    /// Reference to a column by name and ordinal index.
    Column(ColumnRef),
    /// Constant literal value.
    Literal(ScalarValue),
    /// Binary operation (e.g., `a + b`, `x = y`, `price > 100`).
    BinaryOp {
        op: BinaryOp,
        left: Box<Expr>,
        right: Box<Expr>,
    },
    /// Unary operation (e.g., `NOT flag`, `-value`, `IS NULL`).
    UnaryOp {
        op: UnaryOp,
        operand: Box<Expr>,
    },
    /// Named function call (e.g., `UPPER(name)`, `ABS(value)`).
    Function {
        name: String,
        args: Vec<Expr>,
    },
    /// Conjunction (AND) of multiple predicates. Stored as a flat list to simplify
    /// predicate decomposition and pushdown (avoiding nested binary AND trees).
    And(Vec<Expr>),
    /// Disjunction (OR) of multiple predicates.
    Or(Vec<Expr>),
}

impl Expr {
    /// Return all column references in this expression.
    pub fn columns(&self) -> Vec<&ColumnRef> {
        let mut cols = Vec::new();
        self.collect_columns(&mut cols);
        cols
    }

    fn collect_columns<'a>(&'a self, out: &mut Vec<&'a ColumnRef>) {
        match self {
            Expr::Column(c) => out.push(c),
            Expr::Literal(_) => {}
            Expr::BinaryOp { left, right, .. } => {
                left.collect_columns(out);
                right.collect_columns(out);
            }
            Expr::UnaryOp { operand, .. } => operand.collect_columns(out),
            Expr::Function { args, .. } => {
                for a in args {
                    a.collect_columns(out);
                }
            }
            Expr::And(exprs) | Expr::Or(exprs) => {
                for e in exprs {
                    e.collect_columns(out);
                }
            }
        }
    }

    /// Check if this expression references columns from a given table.
    pub fn references_table(&self, table: &str) -> bool {
        self.columns()
            .iter()
            .any(|c| c.table.as_deref() == Some(table))
    }

    /// Flatten AND-chains: (A AND (B AND C)) → And([A, B, C]).
    pub fn conjuncts(&self) -> Vec<&Expr> {
        match self {
            Expr::And(exprs) => exprs.iter().flat_map(|e| e.conjuncts()).collect(),
            other => vec![other],
        }
    }
}

/// Binary operators for comparison and arithmetic.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum BinaryOp {
    /// Equality comparison (`=`). Used heavily in join conditions and filters.
    Eq,
    /// Inequality comparison (`<>` or `!=`).
    NotEq,
    /// Less than (`<`).
    Lt,
    /// Less than or equal (`<=`).
    LtEq,
    /// Greater than (`>`).
    Gt,
    /// Greater than or equal (`>=`).
    GtEq,
    /// Addition (`+`).
    Add,
    /// Subtraction (`-`).
    Sub,
    /// Multiplication (`*`).
    Mul,
    /// Division (`/`).
    Div,
}

/// Unary operators for boolean logic and null checks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum UnaryOp {
    /// Boolean negation (`NOT`).
    Not,
    /// Arithmetic negation (unary minus).
    Neg,
    /// Null check (`IS NULL`).
    IsNull,
    /// Non-null check (`IS NOT NULL`).
    IsNotNull,
}

/// SQL join types.
///
/// The join type affects which rows are produced and which optimization rules apply.
/// For example, only Inner and Cross joins are commutative; Left and Semi joins have
/// fixed left/right semantics and cannot be swapped.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum JoinType {
    /// Inner join: only matching rows from both sides.
    Inner,
    /// Left outer join: all rows from left, matching from right (or NULLs).
    Left,
    /// Right outer join: all rows from right, matching from left (or NULLs).
    Right,
    /// Full outer join: all rows from both sides, NULLs where no match.
    Full,
    /// Semi join: left rows that have at least one match on the right (no right columns).
    Semi,
    /// Anti join: left rows that have no match on the right.
    Anti,
    /// Cross join: Cartesian product of both sides (no condition).
    Cross,
}

/// Aggregate expression.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct AggExpr {
    pub func: AggFunc,
    pub arg: Expr,
    pub distinct: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum AggFunc {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

/// Sort key.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SortKey {
    pub expr: Expr,
    pub ascending: bool,
    pub nulls_first: bool,
}

/// Data distribution strategy for exchange (shuffle) operators.
///
/// In a distributed query engine like Presto, data must be redistributed between
/// stages to satisfy the requirements of downstream operators (e.g., a hash join
/// needs both sides hash-partitioned on the join key).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Distribution {
    /// All data on a single node (coordinator). Used for final aggregation.
    Single,
    /// Every row replicated to all nodes. Used for small dimension tables in joins.
    Broadcast,
    /// Hash-partitioned on the given expressions. Ensures co-located data for joins.
    Hash(Vec<Expr>),
    /// Round-robin distribution for load balancing (no data-locality guarantees).
    RoundRobin,
}

/// Build side for hash joins.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum BuildSide {
    Left,
    Right,
}

/// Logical operators -- represent *what* to compute, not *how*.
///
/// These operators are the input to the optimizer. Transformation rules rewrite them
/// into equivalent logical alternatives (e.g., join reordering), and implementation
/// rules map them to physical operators (e.g., logical Join -> HashJoin).
///
/// Children of logical operators are referenced by group ID in the memo, not stored
/// inline here. The children are managed by the `MemoExpr` wrapper.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum LogicalOp {
    /// Table scan: reads rows from a base table with optional column pruning and
    /// pushed-down filter predicate. This is always a leaf node (no children).
    Scan {
        table: TableRef,
        columns: Vec<ColumnRef>,
        predicate: Option<Expr>,
    },
    /// Filter: applies a predicate to its single child, discarding non-matching rows.
    /// The optimizer may merge this into a join condition via predicate pushdown.
    Filter {
        predicate: Expr,
    },
    /// Projection: computes a set of output expressions from its child's columns.
    /// Column pruning (projection pushdown) narrows the scan to only needed columns.
    Project {
        exprs: Vec<Expr>,
        aliases: Vec<String>,
    },
    /// Join: combines two child relations using the given join type and condition.
    /// This is the primary target for rewrite rules (commutativity, associativity)
    /// and has the most implementation alternatives (hash, merge, nested loop).
    Join {
        join_type: JoinType,
        condition: Expr,
    },
    /// Aggregate: groups rows by `group_by` expressions and computes aggregate
    /// functions (COUNT, SUM, AVG, MIN, MAX) over each group.
    Aggregate {
        group_by: Vec<Expr>,
        aggregates: Vec<AggExpr>,
    },
    /// Sort: orders the output by the given sort keys. May be eliminated if the
    /// child already provides the required ordering.
    Sort {
        order: Vec<SortKey>,
    },
    /// Limit: returns at most `count` rows starting from `offset`. Typically
    /// appears at the top of the plan for `LIMIT` / `OFFSET` clauses.
    Limit {
        offset: u64,
        count: u64,
    },
}

/// Physical operators -- represent *how* to execute a computation.
///
/// Each physical operator has well-defined execution semantics, resource requirements,
/// and cost characteristics. The cost model uses these to estimate the expense of
/// alternative plans.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum PhysicalOp {
    /// Sequential (full) table scan: reads every row from the table.
    /// O(n) where n is the number of rows. No prerequisites.
    ///
    /// In a lakehouse environment (Hive/Iceberg/Delta), this is the only scan strategy.
    /// Lakehouse tables stored as Parquet/ORC files do not have traditional database
    /// indexes. Instead, query engines rely on file-level optimizations like partition
    /// pruning, min/max column statistics, and bloom filters -- but these are handled
    /// at the connector level, not as separate physical operators.
    SeqScan {
        table: TableRef,
        columns: Vec<ColumnRef>,
        predicate: Option<Expr>,
    },
    /// Hash join: builds a hash table on the build side, probes with the other side.
    /// O(build + probe) with O(build) memory. Requires equi-join predicates.
    /// `build_side` determines which child is materialized into the hash table.
    HashJoin {
        join_type: JoinType,
        build_side: BuildSide,
        condition: Expr,
    },
    /// Merge join: merges two pre-sorted streams. O(n + m) with minimal memory.
    /// Requires both inputs sorted on the join key (enforced via child properties).
    ///
    /// **Velox/native execution only.** Presto's Java engine does not implement merge
    /// join. In a lakehouse environment, data from Parquet/ORC is never pre-sorted
    /// (unless explicitly written with sorted bucketing), so the optimizer must add
    /// Sort enforcers on both inputs. This means merge join rarely wins over hash join
    /// unless the data happens to already be sorted or the hash table would be too
    /// large to fit in memory.
    MergeJoin {
        join_type: JoinType,
        condition: Expr,
    },
    /// Nested loop join: for each left row, scans all right rows. O(n * m).
    /// The universal fallback -- works for any join condition including non-equi.
    NestedLoopJoin {
        join_type: JoinType,
        condition: Expr,
    },
    /// Hash aggregate: builds a hash table keyed by group-by columns.
    /// O(n) time, O(groups) memory. Works for any input ordering.
    HashAggregate {
        group_by: Vec<Expr>,
        aggregates: Vec<AggExpr>,
    },
    /// Stream aggregate: processes pre-sorted input in a single pass.
    /// O(n) time, O(1) memory. Requires input sorted on group-by columns.
    ///
    /// **Velox/native execution only.** Like merge join, this operator requires
    /// pre-sorted input which is uncommon in lakehouse environments. The cost model
    /// will add Sort enforcers, making hash aggregate typically cheaper unless the
    /// data is already sorted (e.g., from a preceding Sort or MergeJoin output).
    StreamAggregate {
        group_by: Vec<Expr>,
        aggregates: Vec<AggExpr>,
    },
    /// Sort operator: materializes all input rows and sorts them.
    /// O(n log n) time, O(n) memory. Used as an enforcer when a parent
    /// requires sorted input that the child doesn't natively provide.
    SortOp {
        order: Vec<SortKey>,
    },
    /// Exchange (shuffle): redistributes data across worker nodes.
    /// Cost is dominated by network transfer (bytes sent).
    Exchange {
        distribution: Distribution,
    },
}

/// Unified operator enum.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Operator {
    Logical(LogicalOp),
    Physical(PhysicalOp),
}

impl Operator {
    pub fn is_logical(&self) -> bool {
        matches!(self, Operator::Logical(_))
    }

    pub fn is_physical(&self) -> bool {
        matches!(self, Operator::Physical(_))
    }

    pub fn kind(&self) -> OpKind {
        match self {
            Operator::Logical(l) => OpKind::Logical(l.kind()),
            Operator::Physical(p) => OpKind::Physical(p.kind()),
        }
    }
}

/// Kind discriminant for pattern matching (without data).
///
/// `OpKind` strips away all the fields of an operator and retains only its
/// discriminant (e.g., "this is a Join" or "this is a HashJoin"). This is used
/// by the pattern matching system to quickly check whether a rule's pattern
/// matches an expression without inspecting operator-specific fields.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OpKind {
    Logical(LogicalOpKind),
    Physical(PhysicalOpKind),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum LogicalOpKind {
    Scan,
    Filter,
    Project,
    Join,
    Aggregate,
    Sort,
    Limit,
}

impl LogicalOp {
    pub fn kind(&self) -> LogicalOpKind {
        match self {
            LogicalOp::Scan { .. } => LogicalOpKind::Scan,
            LogicalOp::Filter { .. } => LogicalOpKind::Filter,
            LogicalOp::Project { .. } => LogicalOpKind::Project,
            LogicalOp::Join { .. } => LogicalOpKind::Join,
            LogicalOp::Aggregate { .. } => LogicalOpKind::Aggregate,
            LogicalOp::Sort { .. } => LogicalOpKind::Sort,
            LogicalOp::Limit { .. } => LogicalOpKind::Limit,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum PhysicalOpKind {
    SeqScan,
    HashJoin,
    MergeJoin,
    NestedLoopJoin,
    HashAggregate,
    StreamAggregate,
    SortOp,
    Exchange,
}

impl PhysicalOp {
    pub fn kind(&self) -> PhysicalOpKind {
        match self {
            PhysicalOp::SeqScan { .. } => PhysicalOpKind::SeqScan,
            PhysicalOp::HashJoin { .. } => PhysicalOpKind::HashJoin,
            PhysicalOp::MergeJoin { .. } => PhysicalOpKind::MergeJoin,
            PhysicalOp::NestedLoopJoin { .. } => PhysicalOpKind::NestedLoopJoin,
            PhysicalOp::HashAggregate { .. } => PhysicalOpKind::HashAggregate,
            PhysicalOp::StreamAggregate { .. } => PhysicalOpKind::StreamAggregate,
            PhysicalOp::SortOp { .. } => PhysicalOpKind::SortOp,
            PhysicalOp::Exchange { .. } => PhysicalOpKind::Exchange,
        }
    }
}
