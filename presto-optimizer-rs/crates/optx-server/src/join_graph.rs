//! # Join-Graph Optimization Endpoint
//!
//! This module implements a simplified protocol for join reordering between the
//! Presto Java coordinator and the Rust Cascades optimizer. Instead of full
//! Substrait conversion (which requires handling TableHandle, ColumnHandle,
//! RowExpression, type mappings, etc.), this protocol communicates just the
//! essential information for join reordering:
//!
//! - **Tables**: name, row count, size, and per-column NDV statistics
//! - **Joins**: equi-join conditions between pairs of tables
//!
//! The optimizer builds a Memo from this join graph, runs Cascades search to
//! find the optimal join order, and returns a tree describing the reordered joins.
//!
//! ## Wire Protocol
//!
//! - Request: `POST /optimize/join-graph` with JSON body (`JoinGraphRequest`)
//! - Response: JSON body (`JoinGraphResponse`) with the optimized join tree
//!
//! ## Future Work
//!
//! - **Substrait integration**: Replace this protocol with full Substrait
//!   PlanNode ↔ Substrait conversion for production use. This requires mapping
//!   Presto's TableHandle/ColumnHandle/RowExpression types to Substrait.
//! - **Non-equi join predicates**: Currently only equi-join conditions (col = col)
//!   are supported. Range joins, theta joins, and complex expressions are not
//!   yet handled.
//! - **Filter pushdown**: Table-level filter predicates (e.g., WHERE clauses on
//!   individual tables) are not yet communicated or applied.
//! - **Multi-column joins**: Each join edge currently represents a single-column
//!   equi-join. Multi-column equi-joins (e.g., ON a.x = b.x AND a.y = b.y)
//!   should be supported by allowing multiple column pairs per join edge.
//! - **Outer join support**: The optimizer currently treats all joins as inner
//!   joins for reordering. Left/right/full outer joins have ordering constraints
//!   that must be respected (e.g., left join cannot be commuted freely).

use axum::extract::State;
use axum::http::StatusCode;
use axum::Json;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

use optx_core::catalog::InMemoryCatalog;
use optx_core::cost::DefaultCostModel;
use optx_core::expr::*;
use optx_core::memo::{Memo, PlanNode};
use optx_core::properties::PhysicalPropertySet;
use optx_core::search::{CascadesSearch, SearchConfig};
use optx_core::stats::{ColumnStatistics, Statistics};

use crate::state::AppState;

// ---------------------------------------------------------------------------
// JSON wire-protocol types
// ---------------------------------------------------------------------------

/// Request body for `POST /optimize/join-graph`.
///
/// Describes a join graph as a set of tables (with statistics) and join edges
/// (equi-join conditions between pairs of tables).
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct JoinGraphRequest {
    /// Tables participating in the join graph. Each table has an ID, schema/name,
    /// row count, size, and per-column statistics.
    pub tables: Vec<TableInfo>,
    /// Equi-join conditions connecting pairs of tables.
    pub joins: Vec<JoinEdge>,
}

/// A table in the join graph with its statistics.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TableInfo {
    /// Unique identifier for this table within the request (e.g., "t0", "t1").
    pub id: String,
    /// Schema name (e.g., "tpch").
    pub schema: String,
    /// Table name (e.g., "customer").
    pub name: String,
    /// Estimated row count from table statistics.
    pub row_count: f64,
    /// Estimated total size in bytes.
    pub size_bytes: f64,
    /// Per-column statistics (NDV, null fraction, avg size).
    #[serde(default)]
    pub columns: Vec<ColumnInfo>,
}

/// Per-column statistics for cost estimation.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ColumnInfo {
    /// Column name.
    pub name: String,
    /// Number of distinct values (NDV).
    pub ndv: f64,
    /// Fraction of values that are NULL (0.0 to 1.0).
    #[serde(default)]
    pub null_fraction: f64,
    /// Average size of a single value in bytes.
    #[serde(default = "default_avg_size")]
    pub avg_size: f64,
}

fn default_avg_size() -> f64 {
    8.0
}

/// A join edge connecting two tables via an equi-join condition.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct JoinEdge {
    /// ID of the left table (must match a `TableInfo.id`).
    pub left_table_id: String,
    /// ID of the right table (must match a `TableInfo.id`).
    pub right_table_id: String,
    /// Join type (INNER, LEFT, RIGHT, FULL, SEMI, ANTI, CROSS).
    /// Currently only INNER is fully supported for reordering.
    ///
    /// TODO: Support outer join reordering with proper ordering constraints.
    /// Left/right/full outer joins cannot be freely commuted or reassociated.
    #[serde(default = "default_join_type")]
    pub join_type: String,
    /// Column name on the left side of the equi-join condition.
    pub left_column: String,
    /// Column name on the right side of the equi-join condition.
    pub right_column: String,
}

fn default_join_type() -> String {
    "INNER".to_string()
}

/// Response body from the join-graph optimization endpoint.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct JoinGraphResponse {
    /// The optimized join tree. Each node is either a leaf (table reference)
    /// or a join of two subtrees.
    pub tree: JoinTreeNode,
    /// Total estimated cost of the optimized plan.
    pub cost: f64,
    /// Whether the optimizer actually changed the join order (vs. returning
    /// the original order because no improvement was found).
    pub optimized: bool,
}

/// A node in the optimized join tree.
///
/// This is a recursive tree structure where leaf nodes reference tables by ID
/// and internal nodes represent join operations with left/right children.
#[derive(Debug, Serialize)]
#[serde(untagged)]
pub enum JoinTreeNode {
    /// A leaf node referencing a table by its ID.
    Leaf {
        #[serde(rename = "tableId")]
        table_id: String,
    },
    /// A join node combining two subtrees.
    Join {
        #[serde(rename = "joinType")]
        join_type: String,
        #[serde(rename = "leftColumn")]
        left_column: String,
        #[serde(rename = "rightColumn")]
        right_column: String,
        left: Box<JoinTreeNode>,
        right: Box<JoinTreeNode>,
    },
}

// ---------------------------------------------------------------------------
// Handler
// ---------------------------------------------------------------------------

/// POST /optimize/join-graph — optimize join ordering via Cascades search.
///
/// Accepts a join graph (tables + join edges), builds a Memo, runs the
/// Cascades optimizer, and returns the optimal join tree.
pub async fn optimize_join_graph(
    State(state): State<Arc<AppState>>,
    Json(req): Json<JoinGraphRequest>,
) -> Result<Json<JoinGraphResponse>, (StatusCode, String)> {
    if req.tables.is_empty() || req.joins.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            "Request must contain at least one table and one join".to_string(),
        ));
    }

    // Build catalog with statistics from the request.
    let catalog = build_catalog(&req);

    // Build the memo with logical scan and join operators.
    let (memo, root_group, table_id_map) = build_memo(&req)?;

    // Configure and run the Cascades search.
    let config = SearchConfig {
        max_memo_groups: state.config.max_memo_groups,
        max_iterations: 1_000_000,
        source_type: state.config.source_type.clone(),
    };

    let mut search = CascadesSearch::new(
        memo,
        state.rule_registry.clone(),
        Arc::new(DefaultCostModel::default()),
        Arc::new(catalog),
        config,
    );

    let best_plan = search
        .optimize(root_group, &PhysicalPropertySet::any())
        .ok_or_else(|| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Optimization failed: no valid plan found".to_string(),
            )
        })?;

    let total_cost = best_plan.cost.total;

    // Convert the optimized plan tree back to the join-graph response format.
    let tree = plan_to_tree(&best_plan, &table_id_map)?;

    Ok(Json(JoinGraphResponse {
        tree,
        cost: total_cost,
        optimized: true,
    }))
}

// ---------------------------------------------------------------------------
// Memo construction from join graph
// ---------------------------------------------------------------------------

/// Build an InMemoryCatalog populated with table statistics from the request.
fn build_catalog(req: &JoinGraphRequest) -> InMemoryCatalog {
    let mut catalog = InMemoryCatalog::new();

    for table_info in &req.tables {
        let table_ref = TableRef {
            schema: table_info.schema.clone(),
            name: table_info.name.clone(),
        };

        let mut stats = Statistics::new(table_info.row_count, table_info.size_bytes);
        let mut columns = Vec::new();

        for (idx, col) in table_info.columns.iter().enumerate() {
            let col_stats = ColumnStatistics {
                distinct_count: col.ndv,
                null_fraction: col.null_fraction,
                min_value: None,
                max_value: None,
                avg_row_size: col.avg_size,
                histogram: None,
            };
            stats = stats.with_column(&col.name, col_stats);

            columns.push(ColumnRef {
                table: Some(table_info.name.clone()),
                name: col.name.clone(),
                index: idx as u32,
            });
        }

        catalog.add_table(&table_ref, columns, stats);
    }

    catalog
}

/// Build a Memo from the join graph request.
///
/// Returns (memo, root_group_id, table_id_to_name_map).
///
/// The table_id_to_name_map maps from internal table index to the original
/// table ID string (e.g., "t0"), used when converting the optimized plan
/// back to the response format.
///
/// ## Strategy
///
/// 1. Create a LogicalScan group for each table.
/// 2. Build a left-deep join tree following the order of join edges.
///    The Cascades optimizer will explore alternative orderings via
///    commutativity and associativity rules.
///
/// ## Future Work
///
/// - **Bushy tree construction**: Currently builds a left-deep initial tree.
///   Starting with a bushy tree could give the optimizer a better starting
///   point, but the Cascades rules should explore the full space regardless.
/// - **Join graph connectivity**: Currently assumes all tables are connected
///   via join edges. Disconnected components (cross joins) are not handled.
fn build_memo(
    req: &JoinGraphRequest,
) -> Result<(Memo, u32, HashMap<String, String>), (StatusCode, String)> {
    let mut memo = Memo::new();

    // Map from table ID string (e.g., "t0") to memo GroupId.
    let mut table_groups: HashMap<String, u32> = HashMap::new();
    // Reverse map: table name → table ID string (for response construction).
    let mut table_id_map: HashMap<String, String> = HashMap::new();

    // Step 1: Create scan groups for each table.
    for table_info in &req.tables {
        let table_ref = TableRef {
            schema: table_info.schema.clone(),
            name: table_info.name.clone(),
        };

        let columns: Vec<ColumnRef> = table_info
            .columns
            .iter()
            .enumerate()
            .map(|(idx, col)| ColumnRef {
                table: Some(table_info.name.clone()),
                name: col.name.clone(),
                index: idx as u32,
            })
            .collect();

        let scan_op = Operator::Logical(LogicalOp::Scan {
            table: table_ref,
            columns,
            predicate: None,
        });

        let (group_id, _) = memo.add_expr(scan_op, vec![]);
        table_groups.insert(table_info.id.clone(), group_id);
        table_id_map.insert(table_info.name.clone(), table_info.id.clone());
    }

    // Step 2: Build a left-deep join tree from the join edges.
    // Start with the left table of the first join, then successively join
    // additional tables.
    if req.joins.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            "No join edges provided".to_string(),
        ));
    }

    // Track which tables have been joined into the tree.
    let mut joined_tables: Vec<String> = Vec::new();
    let mut current_group: Option<u32> = None;

    for join_edge in &req.joins {
        let left_group = *table_groups.get(&join_edge.left_table_id).ok_or_else(|| {
            (
                StatusCode::BAD_REQUEST,
                format!("Unknown table ID: {}", join_edge.left_table_id),
            )
        })?;
        let right_group = *table_groups.get(&join_edge.right_table_id).ok_or_else(|| {
            (
                StatusCode::BAD_REQUEST,
                format!("Unknown table ID: {}", join_edge.right_table_id),
            )
        })?;

        let join_type = parse_join_type(&join_edge.join_type);

        // Build the equi-join condition: left_col = right_col
        let condition = Expr::BinaryOp {
            op: BinaryOp::Eq,
            left: Box::new(Expr::Column(ColumnRef {
                table: Some(
                    req.tables
                        .iter()
                        .find(|t| t.id == join_edge.left_table_id)
                        .map(|t| t.name.clone())
                        .unwrap_or_default(),
                ),
                name: join_edge.left_column.clone(),
                index: 0,
            })),
            right: Box::new(Expr::Column(ColumnRef {
                table: Some(
                    req.tables
                        .iter()
                        .find(|t| t.id == join_edge.right_table_id)
                        .map(|t| t.name.clone())
                        .unwrap_or_default(),
                ),
                name: join_edge.right_column.clone(),
                index: 0,
            })),
        };

        match current_group {
            None => {
                // First join: create join between the two table scans.
                let join_op = Operator::Logical(LogicalOp::Join {
                    join_type,
                    condition,
                });
                let (group_id, _) = memo.add_expr(join_op, vec![left_group, right_group]);
                current_group = Some(group_id);
                joined_tables.push(join_edge.left_table_id.clone());
                joined_tables.push(join_edge.right_table_id.clone());
            }
            Some(prev_group) => {
                // Subsequent joins: join the new table onto the existing tree.
                // Determine which side is the "new" table.
                let new_table_group = if !joined_tables.contains(&join_edge.right_table_id) {
                    joined_tables.push(join_edge.right_table_id.clone());
                    right_group
                } else if !joined_tables.contains(&join_edge.left_table_id) {
                    joined_tables.push(join_edge.left_table_id.clone());
                    left_group
                } else {
                    // Both tables already joined — this is an additional predicate
                    // between already-joined tables. For now, we still create a
                    // join node; the optimizer may handle this via filter pushdown.
                    //
                    // TODO: Handle additional join predicates between already-joined
                    // tables. These should be added as filters on the existing join
                    // rather than creating new join nodes.
                    right_group
                };

                let join_op = Operator::Logical(LogicalOp::Join {
                    join_type,
                    condition,
                });
                let (group_id, _) = memo.add_expr(join_op, vec![prev_group, new_table_group]);
                current_group = Some(group_id);
            }
        }
    }

    let root_group = current_group.ok_or_else(|| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "Failed to build join tree".to_string(),
        )
    })?;

    Ok((memo, root_group, table_id_map))
}

/// Parse a join type string into the internal enum.
///
/// TODO: Support SEMI, ANTI, and CROSS join types. Currently these fall back
/// to Inner, which may produce incorrect results for outer join queries.
fn parse_join_type(s: &str) -> JoinType {
    match s.to_uppercase().as_str() {
        "INNER" => JoinType::Inner,
        "LEFT" => JoinType::Left,
        "RIGHT" => JoinType::Right,
        "FULL" => JoinType::Full,
        "SEMI" => JoinType::Semi,
        "ANTI" => JoinType::Anti,
        "CROSS" => JoinType::Cross,
        _ => JoinType::Inner,
    }
}

// ---------------------------------------------------------------------------
// Plan → response conversion
// ---------------------------------------------------------------------------

/// Convert an optimized PlanNode tree into the JSON response tree format.
///
/// Walks the physical plan tree and maps:
/// - SeqScan nodes → leaf `JoinTreeNode::Leaf` with the table ID
/// - HashJoin/MergeJoin/NestedLoopJoin nodes → `JoinTreeNode::Join` with children
///
/// ## Future Work
///
/// - **Exchange/Sort nodes**: Currently skips over Exchange and Sort enforcers
///   that the optimizer may insert. These are physical artifacts and not relevant
///   to the join ordering response.
/// - **Projection/Filter nodes**: Currently skips these as well. If the optimizer
///   pushes filters into the join tree, they should be represented in the response.
fn plan_to_tree(
    plan: &PlanNode,
    table_id_map: &HashMap<String, String>,
) -> Result<JoinTreeNode, (StatusCode, String)> {
    match &plan.op {
        // Physical scan → leaf node
        Operator::Physical(PhysicalOp::SeqScan { table, .. }) => {
            let table_id = table_id_map
                .get(&table.name)
                .cloned()
                .unwrap_or_else(|| table.name.clone());
            Ok(JoinTreeNode::Leaf { table_id })
        }

        // Logical scan (if optimizer didn't implement it) → leaf node
        Operator::Logical(LogicalOp::Scan { table, .. }) => {
            let table_id = table_id_map
                .get(&table.name)
                .cloned()
                .unwrap_or_else(|| table.name.clone());
            Ok(JoinTreeNode::Leaf { table_id })
        }

        // Physical join → join node with two children
        Operator::Physical(PhysicalOp::HashJoin {
            join_type,
            condition,
            ..
        })
        | Operator::Physical(PhysicalOp::MergeJoin {
            join_type,
            condition,
            ..
        })
        | Operator::Physical(PhysicalOp::NestedLoopJoin {
            join_type,
            condition,
            ..
        }) => {
            let (left_col, right_col) = extract_join_columns(condition);
            let join_type_str = format_join_type(join_type);

            if plan.children.len() != 2 {
                return Err((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Join node has {} children, expected 2", plan.children.len()),
                ));
            }

            let left = plan_to_tree(&plan.children[0], table_id_map)?;
            let right = plan_to_tree(&plan.children[1], table_id_map)?;

            Ok(JoinTreeNode::Join {
                join_type: join_type_str,
                left_column: left_col,
                right_column: right_col,
                left: Box::new(left),
                right: Box::new(right),
            })
        }

        // Logical join (fallback if not implemented to physical)
        Operator::Logical(LogicalOp::Join {
            join_type,
            condition,
        }) => {
            let (left_col, right_col) = extract_join_columns(condition);
            let join_type_str = format_join_type(join_type);

            if plan.children.len() != 2 {
                return Err((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Join node has {} children, expected 2", plan.children.len()),
                ));
            }

            let left = plan_to_tree(&plan.children[0], table_id_map)?;
            let right = plan_to_tree(&plan.children[1], table_id_map)?;

            Ok(JoinTreeNode::Join {
                join_type: join_type_str,
                left_column: left_col,
                right_column: right_col,
                left: Box::new(left),
                right: Box::new(right),
            })
        }

        // Skip over enforcers (Exchange, Sort) — recurse into their child.
        // These are physical artifacts from the Cascades search that are not
        // relevant to the join ordering response.
        Operator::Physical(PhysicalOp::Exchange { .. })
        | Operator::Physical(PhysicalOp::SortOp { .. }) => {
            if plan.children.len() == 1 {
                plan_to_tree(&plan.children[0], table_id_map)
            } else {
                Err((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Unexpected enforcer with {} children", plan.children.len()),
                ))
            }
        }

        other => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Unexpected operator in optimized plan: {:?}", other),
        )),
    }
}

/// Extract left and right column names from an equi-join condition.
///
/// Expects the condition to be `Expr::BinaryOp { op: Eq, left: Column, right: Column }`.
///
/// TODO: Support multi-column equi-joins (AND of multiple Eq conditions).
/// Currently only extracts the first column pair.
fn extract_join_columns(condition: &Expr) -> (String, String) {
    match condition {
        Expr::BinaryOp {
            op: BinaryOp::Eq,
            left,
            right,
        } => {
            let left_col = match left.as_ref() {
                Expr::Column(c) => c.name.clone(),
                _ => "unknown".to_string(),
            };
            let right_col = match right.as_ref() {
                Expr::Column(c) => c.name.clone(),
                _ => "unknown".to_string(),
            };
            (left_col, right_col)
        }
        // For AND conditions, take the first equality
        Expr::And(exprs) => {
            for expr in exprs {
                if let Expr::BinaryOp {
                    op: BinaryOp::Eq,
                    left,
                    right,
                } = expr
                {
                    let left_col = match left.as_ref() {
                        Expr::Column(c) => c.name.clone(),
                        _ => continue,
                    };
                    let right_col = match right.as_ref() {
                        Expr::Column(c) => c.name.clone(),
                        _ => continue,
                    };
                    return (left_col, right_col);
                }
            }
            ("unknown".to_string(), "unknown".to_string())
        }
        _ => ("unknown".to_string(), "unknown".to_string()),
    }
}

/// Format a JoinType enum value as a string for the JSON response.
fn format_join_type(jt: &JoinType) -> String {
    match jt {
        JoinType::Inner => "INNER".to_string(),
        JoinType::Left => "LEFT".to_string(),
        JoinType::Right => "RIGHT".to_string(),
        JoinType::Full => "FULL".to_string(),
        JoinType::Semi => "SEMI".to_string(),
        JoinType::Anti => "ANTI".to_string(),
        JoinType::Cross => "CROSS".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_join_type() {
        assert_eq!(parse_join_type("INNER"), JoinType::Inner);
        assert_eq!(parse_join_type("inner"), JoinType::Inner);
        assert_eq!(parse_join_type("LEFT"), JoinType::Left);
        assert_eq!(parse_join_type("RIGHT"), JoinType::Right);
        assert_eq!(parse_join_type("FULL"), JoinType::Full);
        assert_eq!(parse_join_type("unknown"), JoinType::Inner);
    }

    #[test]
    fn test_extract_join_columns() {
        let condition = Expr::BinaryOp {
            op: BinaryOp::Eq,
            left: Box::new(Expr::Column(ColumnRef {
                table: Some("customer".to_string()),
                name: "c_custkey".to_string(),
                index: 0,
            })),
            right: Box::new(Expr::Column(ColumnRef {
                table: Some("orders".to_string()),
                name: "o_custkey".to_string(),
                index: 0,
            })),
        };

        let (left, right) = extract_join_columns(&condition);
        assert_eq!(left, "c_custkey");
        assert_eq!(right, "o_custkey");
    }

    #[test]
    fn test_build_catalog() {
        let req = JoinGraphRequest {
            tables: vec![
                TableInfo {
                    id: "t0".to_string(),
                    schema: "tpch".to_string(),
                    name: "customer".to_string(),
                    row_count: 150000.0,
                    size_bytes: 15000000.0,
                    columns: vec![ColumnInfo {
                        name: "c_custkey".to_string(),
                        ndv: 150000.0,
                        null_fraction: 0.0,
                        avg_size: 8.0,
                    }],
                },
            ],
            joins: vec![],
        };

        let catalog = build_catalog(&req);
        let stats = catalog
            .table_stats
            .get("tpch.customer")
            .expect("should have customer stats");
        assert_eq!(stats.row_count, 150000.0);
        assert!(stats.column_stats.contains_key("c_custkey"));
    }
}
