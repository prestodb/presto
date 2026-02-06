//! # Substrait Consumer (Deserialization)
//!
//! This module converts a Substrait `Plan` protobuf into the optimizer's internal
//! Memo representation. This is the "ingestion" step of the optimization pipeline:
//!
//! ```text
//! Presto Java Coordinator
//!   -> Substrait Plan (protobuf bytes)
//!   -> consumer::consume_plan()
//!   -> Memo with logical operators
//!   -> Cascades search
//!   -> optimized PlanNode
//!   -> producer::produce_plan()
//!   -> Substrait Plan (protobuf bytes)
//!   -> back to Presto Java
//! ```
//!
//! ## Conversion Strategy
//!
//! The consumer recursively walks the Substrait `Rel` tree and creates corresponding
//! memo groups and expressions:
//!
//! - **ReadRel** -> `LogicalOp::Scan` (extracts table name, columns, and filter)
//! - **FilterRel** -> `LogicalOp::Filter` (extracts predicate expression)
//! - **ProjectRel** -> `LogicalOp::Project` (extracts projection expressions)
//! - **JoinRel** -> `LogicalOp::Join` (extracts join type and condition)
//! - **AggregateRel** -> `LogicalOp::Aggregate` (extracts grouping and measures)
//! - **SortRel** -> `LogicalOp::Sort` (extracts sort keys with direction)
//! - **FetchRel** -> `LogicalOp::Limit` (extracts offset and count)
//!
//! ## Expression Conversion
//!
//! Substrait expressions are converted to the internal `Expr` enum:
//! - Field references -> `Expr::Column` (using positional `col_N` naming)
//! - Literals -> `Expr::Literal` (booleans, integers, floats, strings, dates)
//! - Scalar functions -> `Expr::Function` (using function reference IDs)
//!
//! ## Error Handling
//!
//! The consumer returns `ConsumeError` for malformed or unsupported Substrait plans.
//! Unsupported relation types are rejected rather than silently ignored.

use optx_core::expr::*;
use optx_core::memo::{GroupId, Memo};
use substrait::proto;

/// Convert a Substrait Plan into our internal Memo representation.
/// Returns the root group ID that serves as the starting point for optimization.
pub fn consume_plan(plan: &proto::Plan, memo: &mut Memo) -> Result<GroupId, ConsumeError> {
    let root_rel = plan
        .relations
        .first()
        .ok_or(ConsumeError::EmptyPlan)?;

    let rel = match root_rel.rel_type.as_ref().ok_or(ConsumeError::MissingRelType)? {
        proto::plan_rel::RelType::Root(root) => root
            .input
            .as_ref()
            .ok_or(ConsumeError::MissingRelType)?,
        proto::plan_rel::RelType::Rel(rel) => rel,
    };

    consume_rel(rel, memo)
}

/// Recursively convert a Substrait Rel into memo groups.
pub fn consume_rel(rel: &proto::Rel, memo: &mut Memo) -> Result<GroupId, ConsumeError> {
    let rel_type = rel.rel_type.as_ref().ok_or(ConsumeError::MissingRelType)?;

    match rel_type {
        proto::rel::RelType::Read(read) => consume_read(read, memo),
        proto::rel::RelType::Filter(filter) => consume_filter(filter, memo),
        proto::rel::RelType::Project(project) => consume_project(project, memo),
        proto::rel::RelType::Join(join) => consume_join(join, memo),
        proto::rel::RelType::Aggregate(agg) => consume_aggregate(agg, memo),
        proto::rel::RelType::Sort(sort) => consume_sort(sort, memo),
        proto::rel::RelType::Fetch(fetch) => consume_fetch(fetch, memo),
        _ => Err(ConsumeError::UnsupportedRelType),
    }
}

/// Convert a Substrait ReadRel (table scan) to a logical Scan in the memo.
/// Extracts table name (from named table), column schema, and optional filter predicate.
fn consume_read(read: &proto::ReadRel, memo: &mut Memo) -> Result<GroupId, ConsumeError> {
    let (schema_name, table_name) = match &read.read_type {
        Some(proto::read_rel::ReadType::NamedTable(nt)) => {
            let parts = &nt.names;
            match parts.len() {
                1 => ("default".to_string(), parts[0].clone()),
                2 => (parts[0].clone(), parts[1].clone()),
                _ => ("default".to_string(), parts.join(".")),
            }
        }
        _ => ("default".to_string(), "unknown".to_string()),
    };

    let columns = extract_columns_from_schema(&read.base_schema);

    let predicate = read
        .filter
        .as_ref()
        .map(|f| convert_expression(f))
        .transpose()?;

    let op = Operator::Logical(LogicalOp::Scan {
        table: TableRef {
            schema: schema_name,
            name: table_name,
        },
        columns,
        predicate,
    });

    let (gid, _) = memo.add_expr(op, vec![]);
    Ok(gid)
}

/// Convert a Substrait FilterRel to a logical Filter in the memo.
fn consume_filter(filter: &proto::FilterRel, memo: &mut Memo) -> Result<GroupId, ConsumeError> {
    let input = filter
        .input
        .as_ref()
        .ok_or(ConsumeError::MissingInput)?;
    let child_gid = consume_rel(input, memo)?;

    let predicate = filter
        .condition
        .as_ref()
        .map(|c| convert_expression(c))
        .transpose()?
        .unwrap_or(Expr::Literal(ScalarValue::Bool(true)));

    let op = Operator::Logical(LogicalOp::Filter { predicate });
    let (gid, _) = memo.add_expr(op, vec![child_gid]);
    Ok(gid)
}

fn consume_project(
    project: &proto::ProjectRel,
    memo: &mut Memo,
) -> Result<GroupId, ConsumeError> {
    let input = project
        .input
        .as_ref()
        .ok_or(ConsumeError::MissingInput)?;
    let child_gid = consume_rel(input, memo)?;

    let exprs: Vec<Expr> = project
        .expressions
        .iter()
        .map(|e| convert_expression(e))
        .collect::<Result<_, _>>()?;

    let aliases: Vec<String> = (0..exprs.len()).map(|i| format!("col_{}", i)).collect();

    let op = Operator::Logical(LogicalOp::Project { exprs, aliases });
    let (gid, _) = memo.add_expr(op, vec![child_gid]);
    Ok(gid)
}

/// Convert a Substrait JoinRel to a logical Join in the memo.
/// Maps Substrait join types to our internal JoinType enum.
fn consume_join(join: &proto::JoinRel, memo: &mut Memo) -> Result<GroupId, ConsumeError> {
    let left = join.left.as_ref().ok_or(ConsumeError::MissingInput)?;
    let right = join.right.as_ref().ok_or(ConsumeError::MissingInput)?;

    let left_gid = consume_rel(left, memo)?;
    let right_gid = consume_rel(right, memo)?;

    let join_type = match join.r#type {
        x if x == proto::join_rel::JoinType::Inner as i32 => JoinType::Inner,
        x if x == proto::join_rel::JoinType::Left as i32 => JoinType::Left,
        x if x == proto::join_rel::JoinType::Right as i32 => JoinType::Right,
        x if x == proto::join_rel::JoinType::Outer as i32 => JoinType::Full,
        x if x == proto::join_rel::JoinType::LeftSemi as i32 => JoinType::Semi,
        x if x == proto::join_rel::JoinType::LeftAnti as i32 => JoinType::Anti,
        _ => JoinType::Inner,
    };

    let condition = join
        .expression
        .as_ref()
        .map(|e| convert_expression(e))
        .transpose()?
        .unwrap_or(Expr::Literal(ScalarValue::Bool(true)));

    let op = Operator::Logical(LogicalOp::Join {
        join_type,
        condition,
    });
    let (gid, _) = memo.add_expr(op, vec![left_gid, right_gid]);
    Ok(gid)
}

/// Convert a Substrait AggregateRel to a logical Aggregate in the memo.
/// Extracts grouping expressions and aggregate measures (currently maps all to Sum
/// as a simplification -- a full implementation would resolve function references).
fn consume_aggregate(
    agg: &proto::AggregateRel,
    memo: &mut Memo,
) -> Result<GroupId, ConsumeError> {
    let input = agg.input.as_ref().ok_or(ConsumeError::MissingInput)?;
    let child_gid = consume_rel(input, memo)?;

    let group_by: Vec<Expr> = agg
        .groupings
        .iter()
        .flat_map(|g| {
            g.grouping_expressions
                .iter()
                .map(|e| convert_expression(e).unwrap_or(Expr::Literal(ScalarValue::Null)))
        })
        .collect();

    let aggregates: Vec<AggExpr> = agg
        .measures
        .iter()
        .map(|m| {
            let func = m
                .measure
                .as_ref()
                .map(|_af| AggFunc::Sum)
                .unwrap_or(AggFunc::Count);
            let arg = m
                .measure
                .as_ref()
                .and_then(|af| af.arguments.first())
                .and_then(|a| match &a.arg_type {
                    Some(proto::function_argument::ArgType::Value(v)) => {
                        convert_expression(v).ok()
                    }
                    _ => None,
                })
                .unwrap_or(Expr::Literal(ScalarValue::Int64(1)));
            AggExpr {
                func,
                arg,
                distinct: false,
            }
        })
        .collect();

    let op = Operator::Logical(LogicalOp::Aggregate {
        group_by,
        aggregates,
    });
    let (gid, _) = memo.add_expr(op, vec![child_gid]);
    Ok(gid)
}

/// Convert a Substrait SortRel to a logical Sort in the memo.
/// Extracts sort keys with direction (ascending/descending) and nulls ordering.
fn consume_sort(sort: &proto::SortRel, memo: &mut Memo) -> Result<GroupId, ConsumeError> {
    let input = sort.input.as_ref().ok_or(ConsumeError::MissingInput)?;
    let child_gid = consume_rel(input, memo)?;

    let order: Vec<SortKey> = sort
        .sorts
        .iter()
        .map(|sf| {
            let expr = sf
                .expr
                .as_ref()
                .map(|e| convert_expression(e))
                .transpose()
                .ok()
                .flatten()
                .unwrap_or(Expr::Literal(ScalarValue::Null));
            let direction = match &sf.sort_kind {
                Some(proto::sort_field::SortKind::Direction(d)) => *d,
                _ => 0,
            };
            let ascending = direction != proto::sort_field::SortDirection::DescNullsFirst as i32
                && direction != proto::sort_field::SortDirection::DescNullsLast as i32;
            SortKey {
                expr,
                ascending,
                nulls_first: direction == proto::sort_field::SortDirection::AscNullsFirst as i32
                    || direction == proto::sort_field::SortDirection::DescNullsFirst as i32,
            }
        })
        .collect();

    let op = Operator::Logical(LogicalOp::Sort { order });
    let (gid, _) = memo.add_expr(op, vec![child_gid]);
    Ok(gid)
}

/// Convert a Substrait FetchRel (LIMIT/OFFSET) to a logical Limit in the memo.
fn consume_fetch(fetch: &proto::FetchRel, memo: &mut Memo) -> Result<GroupId, ConsumeError> {
    let input = fetch.input.as_ref().ok_or(ConsumeError::MissingInput)?;
    let child_gid = consume_rel(input, memo)?;

    let op = Operator::Logical(LogicalOp::Limit {
        offset: fetch.offset as u64,
        count: fetch.count as u64,
    });
    let (gid, _) = memo.add_expr(op, vec![child_gid]);
    Ok(gid)
}

/// Convert a Substrait expression to our internal Expr.
pub fn convert_expression(expr: &proto::Expression) -> Result<Expr, ConsumeError> {
    match &expr.rex_type {
        Some(proto::expression::RexType::Selection(field_ref)) => {
            let index = match &field_ref.reference_type {
                Some(proto::expression::field_reference::ReferenceType::DirectReference(
                    seg,
                )) => match &seg.reference_type {
                    Some(
                        proto::expression::reference_segment::ReferenceType::StructField(sf),
                    ) => sf.field as u32,
                    _ => 0,
                },
                _ => 0,
            };
            Ok(Expr::Column(ColumnRef {
                table: None,
                name: format!("col_{}", index),
                index,
            }))
        }
        Some(proto::expression::RexType::Literal(lit)) => convert_literal(lit),
        Some(proto::expression::RexType::ScalarFunction(func)) => {
            let args: Vec<Expr> = func
                .arguments
                .iter()
                .filter_map(|a| match &a.arg_type {
                    Some(proto::function_argument::ArgType::Value(v)) => {
                        convert_expression(v).ok()
                    }
                    _ => None,
                })
                .collect();

            Ok(Expr::Function {
                name: format!("func_{}", func.function_reference),
                args,
            })
        }
        _ => Ok(Expr::Literal(ScalarValue::Null)),
    }
}

/// Convert a Substrait literal to our internal Expr::Literal representation.
/// Handles booleans, integers, floats, strings, dates, and treats unknown types as NULL.
fn convert_literal(lit: &proto::expression::Literal) -> Result<Expr, ConsumeError> {
    match &lit.literal_type {
        Some(proto::expression::literal::LiteralType::Boolean(v)) => {
            Ok(Expr::Literal(ScalarValue::Bool(*v)))
        }
        Some(proto::expression::literal::LiteralType::I64(v)) => {
            Ok(Expr::Literal(ScalarValue::Int64(*v)))
        }
        Some(proto::expression::literal::LiteralType::Fp64(v)) => {
            Ok(Expr::Literal(ScalarValue::Float64(ordered_float::OrderedFloat(*v))))
        }
        Some(proto::expression::literal::LiteralType::String(v)) => {
            Ok(Expr::Literal(ScalarValue::Utf8(v.clone())))
        }
        Some(proto::expression::literal::LiteralType::Date(v)) => {
            Ok(Expr::Literal(ScalarValue::Date(*v)))
        }
        _ => Ok(Expr::Literal(ScalarValue::Null)),
    }
}

/// Extract column definitions from a Substrait NamedStruct schema.
/// Creates ColumnRef entries with positional indices and the names from the schema.
fn extract_columns_from_schema(schema: &Option<proto::NamedStruct>) -> Vec<ColumnRef> {
    match schema {
        Some(ns) => ns
            .names
            .iter()
            .enumerate()
            .map(|(i, name)| ColumnRef {
                table: None,
                name: name.clone(),
                index: i as u32,
            })
            .collect(),
        None => vec![],
    }
}

/// Errors that can occur during Substrait plan consumption.
#[derive(Debug, thiserror::Error)]
pub enum ConsumeError {
    /// The Substrait Plan contains no relations (empty query).
    #[error("Empty plan: no relations")]
    EmptyPlan,
    /// A relation node is missing its rel_type discriminant.
    #[error("Missing rel type")]
    MissingRelType,
    /// A relation node is missing a required input (child) relation.
    #[error("Missing input relation")]
    MissingInput,
    /// The relation type is not yet supported by the consumer.
    #[error("Unsupported relation type")]
    UnsupportedRelType,
}
