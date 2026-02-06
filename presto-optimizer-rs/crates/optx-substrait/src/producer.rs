//! # Substrait Producer (Serialization)
//!
//! This module converts an optimized `PlanNode` tree (the output of the Cascades search)
//! back into a Substrait `Plan` protobuf for transmission to the Presto Java coordinator.
//!
//! ## Conversion Strategy
//!
//! The producer recursively walks the `PlanNode` tree and creates corresponding
//! Substrait `Rel` nodes:
//!
//! - **Physical scan operators** (SeqScan) -> `ReadRel` with named table
//! - **Physical join operators** (HashJoin, MergeJoin, NestedLoopJoin) -> `JoinRel`
//! - **Physical aggregate operators** (HashAggregate, StreamAggregate) -> `AggregateRel`
//! - **Physical sort** (SortOp) -> `SortRel`
//! - **Exchange** -> passes through to child (exchange is a Presto-internal concept)
//!
//! Logical operators that weren't converted to physical (e.g., in a partially-optimized
//! plan) are also handled as a fallback.
//!
//! ## Expression Conversion
//!
//! Internal `Expr` values are converted to Substrait expressions:
//! - Column references -> field references (by positional index)
//! - Literals -> Substrait literal types
//! - Binary operations -> scalar functions with numeric function references
//! - AND conjunctions -> scalar function with reference 100
//!
//! ## Function References
//!
//! Binary operators are mapped to numeric function references (Eq=1, NotEq=2, etc.).
//! In a full implementation, these would be registered in the Substrait extension
//! declarations. The current mapping is sufficient for roundtrip fidelity.

use optx_core::expr::*;
use optx_core::memo::PlanNode;
use substrait::proto;

/// Convert an optimized PlanNode tree to a Substrait Plan.
///
/// Wraps the plan in a `PlanRel::Root` with Substrait version metadata.
pub fn produce_plan(plan: &PlanNode) -> proto::Plan {
    let rel = produce_rel(plan);
    proto::Plan {
        version: Some(proto::Version {
            major_number: 0,
            minor_number: 46,
            patch_number: 0,
            ..Default::default()
        }),
        relations: vec![proto::PlanRel {
            rel_type: Some(proto::plan_rel::RelType::Root(proto::RelRoot {
                input: Some(rel),
                names: vec![],
            })),
        }],
        ..Default::default()
    }
}

/// Recursively convert a PlanNode to a Substrait Rel.
fn produce_rel(node: &PlanNode) -> proto::Rel {
    match &node.op {
        Operator::Physical(pop) => produce_physical_rel(pop, &node.children),
        Operator::Logical(lop) => produce_logical_rel(lop, &node.children),
    }
}

/// Convert a physical operator to a Substrait Rel.
/// Different physical join types (hash, merge, nested loop) all map to JoinRel
/// because Substrait doesn't distinguish physical join algorithms.
fn produce_physical_rel(op: &PhysicalOp, children: &[PlanNode]) -> proto::Rel {
    match op {
        PhysicalOp::SeqScan {
            table,
            columns,
            predicate,
        } => make_read_rel(table, columns, predicate),
        PhysicalOp::HashJoin {
            join_type,
            condition,
            ..
        }
        | PhysicalOp::MergeJoin {
            join_type,
            condition,
        }
        | PhysicalOp::NestedLoopJoin {
            join_type,
            condition,
        } => {
            let left = children.first().map(produce_rel);
            let right = children.get(1).map(produce_rel);
            proto::Rel {
                rel_type: Some(proto::rel::RelType::Join(Box::new(proto::JoinRel {
                    common: None,
                    left: left.map(Box::new),
                    right: right.map(Box::new),
                    expression: Some(Box::new(produce_expression(condition))),
                    post_join_filter: None,
                    r#type: convert_join_type(join_type) as i32,
                    advanced_extension: None,
                }))),
            }
        }
        PhysicalOp::HashAggregate {
            group_by,
            aggregates,
        }
        | PhysicalOp::StreamAggregate {
            group_by,
            aggregates,
        } => {
            let input = children.first().map(produce_rel);
            proto::Rel {
                rel_type: Some(proto::rel::RelType::Aggregate(Box::new(
                    proto::AggregateRel {
                        common: None,
                        input: input.map(Box::new),
                        groupings: vec![proto::aggregate_rel::Grouping {
                            grouping_expressions: group_by
                                .iter()
                                .map(produce_expression)
                                .collect(),
                            expression_references: vec![],
                        }],
                        measures: aggregates
                            .iter()
                            .map(|agg| proto::aggregate_rel::Measure {
                                measure: Some(proto::AggregateFunction {
                                    function_reference: match agg.func {
                                        AggFunc::Count => 0,
                                        AggFunc::Sum => 1,
                                        AggFunc::Avg => 2,
                                        AggFunc::Min => 3,
                                        AggFunc::Max => 4,
                                    },
                                    arguments: vec![proto::FunctionArgument {
                                        arg_type: Some(
                                            proto::function_argument::ArgType::Value(
                                                produce_expression(&agg.arg),
                                            ),
                                        ),
                                    }],
                                    ..Default::default()
                                }),
                                filter: None,
                            })
                            .collect(),
                        grouping_expressions: vec![],
                        advanced_extension: None,
                    },
                ))),
            }
        }
        PhysicalOp::SortOp { order } => {
            let input = children.first().map(produce_rel);
            proto::Rel {
                rel_type: Some(proto::rel::RelType::Sort(Box::new(proto::SortRel {
                    common: None,
                    input: input.map(Box::new),
                    sorts: order.iter().map(produce_sort_field).collect(),
                    advanced_extension: None,
                }))),
            }
        }
        PhysicalOp::Exchange { .. } => {
            children
                .first()
                .map(produce_rel)
                .unwrap_or(empty_rel())
        }
    }
}

/// Convert a logical operator to a Substrait Rel (fallback for unimplemented operators).
fn produce_logical_rel(op: &LogicalOp, children: &[PlanNode]) -> proto::Rel {
    match op {
        LogicalOp::Scan {
            table,
            columns,
            predicate,
        } => make_read_rel(table, columns, predicate),
        LogicalOp::Filter { predicate } => {
            let input = children.first().map(produce_rel);
            proto::Rel {
                rel_type: Some(proto::rel::RelType::Filter(Box::new(proto::FilterRel {
                    common: None,
                    input: input.map(Box::new),
                    condition: Some(Box::new(produce_expression(predicate))),
                    advanced_extension: None,
                }))),
            }
        }
        LogicalOp::Join {
            join_type,
            condition,
        } => {
            let left = children.first().map(produce_rel);
            let right = children.get(1).map(produce_rel);
            proto::Rel {
                rel_type: Some(proto::rel::RelType::Join(Box::new(proto::JoinRel {
                    common: None,
                    left: left.map(Box::new),
                    right: right.map(Box::new),
                    expression: Some(Box::new(produce_expression(condition))),
                    post_join_filter: None,
                    r#type: convert_join_type(join_type) as i32,
                    advanced_extension: None,
                }))),
            }
        }
        _ => empty_rel(),
    }
}

/// Create a Substrait ReadRel from a table reference, column list, and optional predicate.
/// The table is identified by a two-part name (schema, table) in the NamedTable field.
fn make_read_rel(table: &TableRef, columns: &[ColumnRef], predicate: &Option<Expr>) -> proto::Rel {
    let names: Vec<String> = columns.iter().map(|c| c.name.clone()).collect();
    proto::Rel {
        rel_type: Some(proto::rel::RelType::Read(Box::new(proto::ReadRel {
            common: None,
            read_type: Some(proto::read_rel::ReadType::NamedTable(
                proto::read_rel::NamedTable {
                    names: vec![table.schema.clone(), table.name.clone()],
                    advanced_extension: None,
                },
            )),
            base_schema: if !names.is_empty() {
                Some(proto::NamedStruct {
                    names,
                    r#struct: None,
                })
            } else {
                None
            },
            filter: predicate.as_ref().map(|p| Box::new(produce_expression(p))),
            best_effort_filter: None,
            projection: None,
            advanced_extension: None,
        }))),
    }
}

/// Convert an internal SortKey to a Substrait SortField with direction encoding.
fn produce_sort_field(sk: &SortKey) -> proto::SortField {
    let direction = if sk.ascending {
        if sk.nulls_first {
            proto::sort_field::SortDirection::AscNullsFirst
        } else {
            proto::sort_field::SortDirection::AscNullsLast
        }
    } else if sk.nulls_first {
        proto::sort_field::SortDirection::DescNullsFirst
    } else {
        proto::sort_field::SortDirection::DescNullsLast
    };

    proto::SortField {
        expr: Some(produce_expression(&sk.expr)),
        sort_kind: Some(proto::sort_field::SortKind::Direction(direction as i32)),
    }
}

/// Convert our internal expression to a Substrait expression.
pub fn produce_expression(expr: &Expr) -> proto::Expression {
    match expr {
        Expr::Column(col) => proto::Expression {
            rex_type: Some(proto::expression::RexType::Selection(Box::new(
                proto::expression::FieldReference {
                    reference_type: Some(
                        proto::expression::field_reference::ReferenceType::DirectReference(
                            proto::expression::ReferenceSegment {
                                reference_type: Some(
                                    proto::expression::reference_segment::ReferenceType::StructField(
                                        Box::new(proto::expression::reference_segment::StructField {
                                            field: col.index as i32,
                                            child: None,
                                        }),
                                    ),
                                ),
                            },
                        ),
                    ),
                    root_type: None,
                },
            ))),
        },
        Expr::Literal(scalar) => {
            let literal_type = match scalar {
                ScalarValue::Null => None,
                ScalarValue::Bool(v) => {
                    Some(proto::expression::literal::LiteralType::Boolean(*v))
                }
                ScalarValue::Int64(v) => {
                    Some(proto::expression::literal::LiteralType::I64(*v))
                }
                ScalarValue::Float64(v) => {
                    Some(proto::expression::literal::LiteralType::Fp64(v.into_inner()))
                }
                ScalarValue::Utf8(v) => {
                    Some(proto::expression::literal::LiteralType::String(v.clone()))
                }
                ScalarValue::Date(v) => {
                    Some(proto::expression::literal::LiteralType::Date(*v))
                }
            };
            proto::Expression {
                rex_type: Some(proto::expression::RexType::Literal(
                    proto::expression::Literal {
                        nullable: false,
                        type_variation_reference: 0,
                        literal_type,
                    },
                )),
            }
        }
        Expr::BinaryOp { op, left, right } => {
            let func_ref = match op {
                BinaryOp::Eq => 1,
                BinaryOp::NotEq => 2,
                BinaryOp::Lt => 3,
                BinaryOp::LtEq => 4,
                BinaryOp::Gt => 5,
                BinaryOp::GtEq => 6,
                BinaryOp::Add => 7,
                BinaryOp::Sub => 8,
                BinaryOp::Mul => 9,
                BinaryOp::Div => 10,
            };
            proto::Expression {
                rex_type: Some(proto::expression::RexType::ScalarFunction(
                    proto::expression::ScalarFunction {
                        function_reference: func_ref,
                        arguments: vec![
                            proto::FunctionArgument {
                                arg_type: Some(proto::function_argument::ArgType::Value(
                                    produce_expression(left),
                                )),
                            },
                            proto::FunctionArgument {
                                arg_type: Some(proto::function_argument::ArgType::Value(
                                    produce_expression(right),
                                )),
                            },
                        ],
                        ..Default::default()
                    },
                )),
            }
        }
        Expr::And(conjuncts) => {
            if conjuncts.len() == 1 {
                return produce_expression(&conjuncts[0]);
            }
            let args: Vec<proto::FunctionArgument> = conjuncts
                .iter()
                .map(|c| proto::FunctionArgument {
                    arg_type: Some(proto::function_argument::ArgType::Value(
                        produce_expression(c),
                    )),
                })
                .collect();
            proto::Expression {
                rex_type: Some(proto::expression::RexType::ScalarFunction(
                    proto::expression::ScalarFunction {
                        function_reference: 100, // AND
                        arguments: args,
                        ..Default::default()
                    },
                )),
            }
        }
        _ => proto::Expression {
            rex_type: Some(proto::expression::RexType::Literal(
                proto::expression::Literal {
                    nullable: true,
                    type_variation_reference: 0,
                    literal_type: None,
                },
            )),
        },
    }
}

/// Map internal JoinType to Substrait JoinType enum.
/// Note: Cross join maps to Inner in Substrait (Substrait doesn't have a separate cross type).
fn convert_join_type(jt: &JoinType) -> proto::join_rel::JoinType {
    match jt {
        JoinType::Inner => proto::join_rel::JoinType::Inner,
        JoinType::Left => proto::join_rel::JoinType::Left,
        JoinType::Right => proto::join_rel::JoinType::Right,
        JoinType::Full => proto::join_rel::JoinType::Outer,
        JoinType::Semi => proto::join_rel::JoinType::LeftSemi,
        JoinType::Anti => proto::join_rel::JoinType::LeftAnti,
        JoinType::Cross => proto::join_rel::JoinType::Inner,
    }
}

/// Create an empty Rel (used as fallback for unsupported operator types).
fn empty_rel() -> proto::Rel {
    proto::Rel { rel_type: None }
}
