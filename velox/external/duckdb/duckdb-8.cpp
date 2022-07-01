// See https://raw.githubusercontent.com/cwida/duckdb/master/LICENSE for licensing information

#include "duckdb.hpp"
#include "duckdb-internal.hpp"
#ifndef DUCKDB_AMALGAMATION
#error header mismatch
#endif















namespace duckdb {

PragmaHandler::PragmaHandler(ClientContext &context) : context(context) {
}

void PragmaHandler::HandlePragmaStatementsInternal(vector<unique_ptr<SQLStatement>> &statements) {
	vector<unique_ptr<SQLStatement>> new_statements;
	for (idx_t i = 0; i < statements.size(); i++) {
		if (statements[i]->type == StatementType::PRAGMA_STATEMENT) {
			// PRAGMA statement: check if we need to replace it by a new set of statements
			PragmaHandler handler(context);
			auto new_query = handler.HandlePragma(statements[i].get()); //*((PragmaStatement &)*statements[i]).info
			if (!new_query.empty()) {
				// this PRAGMA statement gets replaced by a new query string
				// push the new query string through the parser again and add it to the transformer
				Parser parser(context.GetParserOptions());
				parser.ParseQuery(new_query);
				// insert the new statements and remove the old statement
				// FIXME: off by one here maybe?
				for (idx_t j = 0; j < parser.statements.size(); j++) {
					new_statements.push_back(move(parser.statements[j]));
				}
				continue;
			}
		}
		new_statements.push_back(move(statements[i]));
	}
	statements = move(new_statements);
}

void PragmaHandler::HandlePragmaStatements(ClientContextLock &lock, vector<unique_ptr<SQLStatement>> &statements) {
	// first check if there are any pragma statements
	bool found_pragma = false;
	for (idx_t i = 0; i < statements.size(); i++) {
		if (statements[i]->type == StatementType::PRAGMA_STATEMENT) {
			found_pragma = true;
			break;
		}
	}
	if (!found_pragma) {
		// no pragmas: skip this step
		return;
	}
	context.RunFunctionInTransactionInternal(lock, [&]() { HandlePragmaStatementsInternal(statements); });
}

string PragmaHandler::HandlePragma(SQLStatement *statement) { // PragmaInfo &info
	auto info = *((PragmaStatement &)*statement).info;
	auto entry =
	    Catalog::GetCatalog(context).GetEntry<PragmaFunctionCatalogEntry>(context, DEFAULT_SCHEMA, info.name, false);
	string error;
	idx_t bound_idx = Function::BindFunction(entry->name, entry->functions, info, error);
	if (bound_idx == DConstants::INVALID_INDEX) {
		throw BinderException(error);
	}
	auto &bound_function = entry->functions[bound_idx];
	if (bound_function.query) {
		QueryErrorContext error_context(statement, statement->stmt_location);
		Binder::BindNamedParameters(bound_function.named_parameters, info.named_parameters, error_context,
		                            bound_function.name);
		FunctionParameters parameters {info.parameters, info.named_parameters};
		return bound_function.query(context, parameters);
	}
	return string();
}

} // namespace duckdb













namespace duckdb {

FlattenDependentJoins::FlattenDependentJoins(Binder &binder, const vector<CorrelatedColumnInfo> &correlated,
                                             bool any_join)
    : binder(binder), correlated_columns(correlated), any_join(any_join) {
	for (idx_t i = 0; i < correlated_columns.size(); i++) {
		auto &col = correlated_columns[i];
		correlated_map[col.binding] = i;
		delim_types.push_back(col.type);
	}
}

bool FlattenDependentJoins::DetectCorrelatedExpressions(LogicalOperator *op) {
	D_ASSERT(op);
	// check if this entry has correlated expressions
	HasCorrelatedExpressions visitor(correlated_columns);
	visitor.VisitOperator(*op);
	bool has_correlation = visitor.has_correlated_expressions;
	// now visit the children of this entry and check if they have correlated expressions
	for (auto &child : op->children) {
		// we OR the property with its children such that has_correlation is true if either
		// (1) this node has a correlated expression or
		// (2) one of its children has a correlated expression
		if (DetectCorrelatedExpressions(child.get())) {
			has_correlation = true;
		}
	}
	// set the entry in the map
	has_correlated_expressions[op] = has_correlation;
	return has_correlation;
}

unique_ptr<LogicalOperator> FlattenDependentJoins::PushDownDependentJoin(unique_ptr<LogicalOperator> plan) {
	bool propagate_null_values = true;
	auto result = PushDownDependentJoinInternal(move(plan), propagate_null_values);
	if (!replacement_map.empty()) {
		// check if we have to replace any COUNT aggregates into "CASE WHEN X IS NULL THEN 0 ELSE COUNT END"
		RewriteCountAggregates aggr(replacement_map);
		aggr.VisitOperator(*result);
	}
	return result;
}

bool SubqueryDependentFilter(Expression *expr) {
	if (expr->expression_class == ExpressionClass::BOUND_CONJUNCTION &&
	    expr->GetExpressionType() == ExpressionType::CONJUNCTION_AND) {
		auto bound_conjuction = (BoundConjunctionExpression *)expr;
		for (auto &child : bound_conjuction->children) {
			if (SubqueryDependentFilter(child.get())) {
				return true;
			}
		}
	}
	if (expr->expression_class == ExpressionClass::BOUND_SUBQUERY) {
		return true;
	}
	return false;
}
unique_ptr<LogicalOperator> FlattenDependentJoins::PushDownDependentJoinInternal(unique_ptr<LogicalOperator> plan,
                                                                                 bool &parent_propagate_null_values) {
	// first check if the logical operator has correlated expressions
	auto entry = has_correlated_expressions.find(plan.get());
	D_ASSERT(entry != has_correlated_expressions.end());
	if (!entry->second) {
		// we reached a node without correlated expressions
		// we can eliminate the dependent join now and create a simple cross product
		auto cross_product = make_unique<LogicalCrossProduct>();
		// now create the duplicate eliminated scan for this node
		auto delim_index = binder.GenerateTableIndex();
		this->base_binding = ColumnBinding(delim_index, 0);
		auto delim_scan = make_unique<LogicalDelimGet>(delim_index, delim_types);
		cross_product->children.push_back(move(delim_scan));
		cross_product->children.push_back(move(plan));
		return move(cross_product);
	}
	switch (plan->type) {
	case LogicalOperatorType::LOGICAL_UNNEST:
	case LogicalOperatorType::LOGICAL_FILTER: {
		// filter
		// first we flatten the dependent join in the child of the filter
		for (auto &expr : plan->expressions) {
			any_join |= SubqueryDependentFilter(expr.get());
		}
		plan->children[0] = PushDownDependentJoinInternal(move(plan->children[0]), parent_propagate_null_values);

		// then we replace any correlated expressions with the corresponding entry in the correlated_map
		RewriteCorrelatedExpressions rewriter(base_binding, correlated_map);
		rewriter.VisitOperator(*plan);
		return plan;
	}
	case LogicalOperatorType::LOGICAL_PROJECTION: {
		// projection
		// first we flatten the dependent join in the child of the projection
		for (auto &expr : plan->expressions) {
			parent_propagate_null_values &= expr->PropagatesNullValues();
		}
		plan->children[0] = PushDownDependentJoinInternal(move(plan->children[0]), parent_propagate_null_values);

		// then we replace any correlated expressions with the corresponding entry in the correlated_map
		RewriteCorrelatedExpressions rewriter(base_binding, correlated_map);
		rewriter.VisitOperator(*plan);
		// now we add all the columns of the delim_scan to the projection list
		auto proj = (LogicalProjection *)plan.get();
		for (idx_t i = 0; i < correlated_columns.size(); i++) {
			auto colref = make_unique<BoundColumnRefExpression>(
			    correlated_columns[i].type, ColumnBinding(base_binding.table_index, base_binding.column_index + i));
			plan->expressions.push_back(move(colref));
		}

		base_binding.table_index = proj->table_index;
		this->delim_offset = base_binding.column_index = plan->expressions.size() - correlated_columns.size();
		this->data_offset = 0;
		return plan;
	}
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
		auto &aggr = (LogicalAggregate &)*plan;
		// aggregate and group by
		// first we flatten the dependent join in the child of the projection
		for (auto &expr : plan->expressions) {
			parent_propagate_null_values &= expr->PropagatesNullValues();
		}
		plan->children[0] = PushDownDependentJoinInternal(move(plan->children[0]), parent_propagate_null_values);
		// then we replace any correlated expressions with the corresponding entry in the correlated_map
		RewriteCorrelatedExpressions rewriter(base_binding, correlated_map);
		rewriter.VisitOperator(*plan);
		// now we add all the columns of the delim_scan to the grouping operators AND the projection list
		for (idx_t i = 0; i < correlated_columns.size(); i++) {
			auto colref = make_unique<BoundColumnRefExpression>(
			    correlated_columns[i].type, ColumnBinding(base_binding.table_index, base_binding.column_index + i));
			for (auto &set : aggr.grouping_sets) {
				set.insert(aggr.groups.size());
			}
			aggr.groups.push_back(move(colref));
		}
		if (aggr.groups.size() == correlated_columns.size()) {
			// we have to perform a LEFT OUTER JOIN between the result of this aggregate and the delim scan
			// FIXME: this does not always have to be a LEFT OUTER JOIN, depending on whether aggr.expressions return
			// NULL or a value
			unique_ptr<LogicalComparisonJoin> join = make_unique<LogicalComparisonJoin>(JoinType::INNER);
			for (auto &aggr_exp : aggr.expressions) {
				auto b_aggr_exp = (BoundAggregateExpression *)aggr_exp.get();
				if (!b_aggr_exp->PropagatesNullValues() || any_join || !parent_propagate_null_values) {
					join = make_unique<LogicalComparisonJoin>(JoinType::LEFT);
					break;
				}
			}
			auto left_index = binder.GenerateTableIndex();
			auto delim_scan = make_unique<LogicalDelimGet>(left_index, delim_types);
			join->children.push_back(move(delim_scan));
			join->children.push_back(move(plan));
			for (idx_t i = 0; i < correlated_columns.size(); i++) {
				JoinCondition cond;
				cond.left =
				    make_unique<BoundColumnRefExpression>(correlated_columns[i].type, ColumnBinding(left_index, i));
				cond.right = make_unique<BoundColumnRefExpression>(
				    correlated_columns[i].type,
				    ColumnBinding(aggr.group_index, (aggr.groups.size() - correlated_columns.size()) + i));
				cond.comparison = ExpressionType::COMPARE_NOT_DISTINCT_FROM;
				join->conditions.push_back(move(cond));
			}
			// for any COUNT aggregate we replace references to the column with: CASE WHEN COUNT(*) IS NULL THEN 0
			// ELSE COUNT(*) END
			for (idx_t i = 0; i < aggr.expressions.size(); i++) {
				D_ASSERT(aggr.expressions[i]->GetExpressionClass() == ExpressionClass::BOUND_AGGREGATE);
				auto bound = (BoundAggregateExpression *)&*aggr.expressions[i];
				vector<LogicalType> arguments;
				if (bound->function == CountFun::GetFunction() || bound->function == CountStarFun::GetFunction()) {
					// have to replace this ColumnBinding with the CASE expression
					replacement_map[ColumnBinding(aggr.aggregate_index, i)] = i;
				}
			}
			// now we update the delim_index

			base_binding.table_index = left_index;
			this->delim_offset = base_binding.column_index = 0;
			this->data_offset = 0;
			return move(join);
		} else {
			// update the delim_index
			base_binding.table_index = aggr.group_index;
			this->delim_offset = base_binding.column_index = aggr.groups.size() - correlated_columns.size();
			this->data_offset = aggr.groups.size();
			return plan;
		}
	}
	case LogicalOperatorType::LOGICAL_CROSS_PRODUCT: {
		// cross product
		// push into both sides of the plan
		bool left_has_correlation = has_correlated_expressions.find(plan->children[0].get())->second;
		bool right_has_correlation = has_correlated_expressions.find(plan->children[1].get())->second;
		if (!right_has_correlation) {
			// only left has correlation: push into left
			plan->children[0] = PushDownDependentJoinInternal(move(plan->children[0]), parent_propagate_null_values);
			return plan;
		}
		if (!left_has_correlation) {
			// only right has correlation: push into right
			plan->children[1] = PushDownDependentJoinInternal(move(plan->children[1]), parent_propagate_null_values);
			return plan;
		}
		// both sides have correlation
		// turn into an inner join
		auto join = make_unique<LogicalComparisonJoin>(JoinType::INNER);
		plan->children[0] = PushDownDependentJoinInternal(move(plan->children[0]), parent_propagate_null_values);
		auto left_binding = this->base_binding;
		plan->children[1] = PushDownDependentJoinInternal(move(plan->children[1]), parent_propagate_null_values);
		// add the correlated columns to the join conditions
		for (idx_t i = 0; i < correlated_columns.size(); i++) {
			JoinCondition cond;
			cond.left = make_unique<BoundColumnRefExpression>(
			    correlated_columns[i].type, ColumnBinding(left_binding.table_index, left_binding.column_index + i));
			cond.right = make_unique<BoundColumnRefExpression>(
			    correlated_columns[i].type, ColumnBinding(base_binding.table_index, base_binding.column_index + i));
			cond.comparison = ExpressionType::COMPARE_NOT_DISTINCT_FROM;
			join->conditions.push_back(move(cond));
		}
		join->children.push_back(move(plan->children[0]));
		join->children.push_back(move(plan->children[1]));
		return move(join);
	}
	case LogicalOperatorType::LOGICAL_ANY_JOIN:
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN: {
		auto &join = (LogicalJoin &)*plan;
		D_ASSERT(plan->children.size() == 2);
		// check the correlated expressions in the children of the join
		bool left_has_correlation = has_correlated_expressions.find(plan->children[0].get())->second;
		bool right_has_correlation = has_correlated_expressions.find(plan->children[1].get())->second;

		if (join.join_type == JoinType::INNER) {
			// inner join
			if (!right_has_correlation) {
				// only left has correlation: push into left
				plan->children[0] =
				    PushDownDependentJoinInternal(move(plan->children[0]), parent_propagate_null_values);
				return plan;
			}
			if (!left_has_correlation) {
				// only right has correlation: push into right
				plan->children[1] =
				    PushDownDependentJoinInternal(move(plan->children[1]), parent_propagate_null_values);
				return plan;
			}
		} else if (join.join_type == JoinType::LEFT) {
			// left outer join
			if (!right_has_correlation) {
				// only left has correlation: push into left
				plan->children[0] =
				    PushDownDependentJoinInternal(move(plan->children[0]), parent_propagate_null_values);
				return plan;
			}
		} else if (join.join_type == JoinType::RIGHT) {
			// left outer join
			if (!left_has_correlation) {
				// only right has correlation: push into right
				plan->children[1] =
				    PushDownDependentJoinInternal(move(plan->children[1]), parent_propagate_null_values);
				return plan;
			}
		} else if (join.join_type == JoinType::MARK) {
			if (right_has_correlation) {
				throw Exception("MARK join with correlation in RHS not supported");
			}
			// push the child into the LHS
			plan->children[0] = PushDownDependentJoinInternal(move(plan->children[0]), parent_propagate_null_values);
			// rewrite expressions in the join conditions
			RewriteCorrelatedExpressions rewriter(base_binding, correlated_map);
			rewriter.VisitOperator(*plan);
			return plan;
		} else {
			throw Exception("Unsupported join type for flattening correlated subquery");
		}
		// both sides have correlation
		// push into both sides
		plan->children[0] = PushDownDependentJoinInternal(move(plan->children[0]), parent_propagate_null_values);
		auto left_binding = this->base_binding;
		plan->children[1] = PushDownDependentJoinInternal(move(plan->children[1]), parent_propagate_null_values);
		auto right_binding = this->base_binding;
		// NOTE: for OUTER JOINS it matters what the BASE BINDING is after the join
		// for the LEFT OUTER JOIN, we want the LEFT side to be the base binding after we push
		// because the RIGHT binding might contain NULL values
		if (join.join_type == JoinType::LEFT) {
			this->base_binding = left_binding;
		} else if (join.join_type == JoinType::RIGHT) {
			this->base_binding = right_binding;
		}
		// add the correlated columns to the join conditions
		for (idx_t i = 0; i < correlated_columns.size(); i++) {
			auto left = make_unique<BoundColumnRefExpression>(
			    correlated_columns[i].type, ColumnBinding(left_binding.table_index, left_binding.column_index + i));
			auto right = make_unique<BoundColumnRefExpression>(
			    correlated_columns[i].type, ColumnBinding(right_binding.table_index, right_binding.column_index + i));

			if (join.type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
				JoinCondition cond;
				cond.left = move(left);
				cond.right = move(right);
				cond.comparison = ExpressionType::COMPARE_NOT_DISTINCT_FROM;

				auto &comparison_join = (LogicalComparisonJoin &)join;
				comparison_join.conditions.push_back(move(cond));
			} else {
				auto &any_join = (LogicalAnyJoin &)join;
				auto comparison = make_unique<BoundComparisonExpression>(ExpressionType::COMPARE_NOT_DISTINCT_FROM,
				                                                         move(left), move(right));
				auto conjunction = make_unique<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_AND,
				                                                           move(comparison), move(any_join.condition));
				any_join.condition = move(conjunction);
			}
		}
		// then we replace any correlated expressions with the corresponding entry in the correlated_map
		RewriteCorrelatedExpressions rewriter(right_binding, correlated_map);
		rewriter.VisitOperator(*plan);
		return plan;
	}
	case LogicalOperatorType::LOGICAL_LIMIT: {
		auto &limit = (LogicalLimit &)*plan;
		if (limit.offset_val > 0) {
			throw ParserException("OFFSET not supported in correlated subquery");
		}
		if (limit.limit) {
			throw ParserException("Non-constant limit not supported in correlated subquery");
		}
		plan->children[0] = PushDownDependentJoinInternal(move(plan->children[0]), parent_propagate_null_values);
		if (limit.limit_val == 0) {
			// limit = 0 means we return zero columns here
			return plan;
		} else {
			// limit > 0 does nothing
			return move(plan->children[0]);
		}
	}
	case LogicalOperatorType::LOGICAL_LIMIT_PERCENT: {
		throw ParserException("Limit percent operator not supported in correlated subquery");
	}
	case LogicalOperatorType::LOGICAL_WINDOW: {
		auto &window = (LogicalWindow &)*plan;
		// push into children
		plan->children[0] = PushDownDependentJoinInternal(move(plan->children[0]), parent_propagate_null_values);
		// add the correlated columns to the PARTITION BY clauses in the Window
		for (auto &expr : window.expressions) {
			D_ASSERT(expr->GetExpressionClass() == ExpressionClass::BOUND_WINDOW);
			auto &w = (BoundWindowExpression &)*expr;
			for (idx_t i = 0; i < correlated_columns.size(); i++) {
				w.partitions.push_back(make_unique<BoundColumnRefExpression>(
				    correlated_columns[i].type,
				    ColumnBinding(base_binding.table_index, base_binding.column_index + i)));
			}
		}
		return plan;
	}
	case LogicalOperatorType::LOGICAL_EXCEPT:
	case LogicalOperatorType::LOGICAL_INTERSECT:
	case LogicalOperatorType::LOGICAL_UNION: {
		auto &setop = (LogicalSetOperation &)*plan;
		// set operator, push into both children
		plan->children[0] = PushDownDependentJoin(move(plan->children[0]));
		plan->children[1] = PushDownDependentJoin(move(plan->children[1]));
		// we have to refer to the setop index now
		base_binding.table_index = setop.table_index;
		base_binding.column_index = setop.column_count;
		setop.column_count += correlated_columns.size();
		return plan;
	}
	case LogicalOperatorType::LOGICAL_DISTINCT:
		plan->children[0] = PushDownDependentJoin(move(plan->children[0]));
		return plan;
	case LogicalOperatorType::LOGICAL_EXPRESSION_GET: {
		// expression get
		// first we flatten the dependent join in the child
		plan->children[0] = PushDownDependentJoinInternal(move(plan->children[0]), parent_propagate_null_values);
		// then we replace any correlated expressions with the corresponding entry in the correlated_map
		RewriteCorrelatedExpressions rewriter(base_binding, correlated_map);
		rewriter.VisitOperator(*plan);
		// now we add all the correlated columns to each of the expressions of the expression scan
		auto expr_get = (LogicalExpressionGet *)plan.get();
		for (idx_t i = 0; i < correlated_columns.size(); i++) {
			for (auto &expr_list : expr_get->expressions) {
				auto colref = make_unique<BoundColumnRefExpression>(
				    correlated_columns[i].type, ColumnBinding(base_binding.table_index, base_binding.column_index + i));
				expr_list.push_back(move(colref));
			}
			expr_get->expr_types.push_back(correlated_columns[i].type);
		}

		base_binding.table_index = expr_get->table_index;
		this->delim_offset = base_binding.column_index = expr_get->expr_types.size() - correlated_columns.size();
		this->data_offset = 0;
		return plan;
	}
	case LogicalOperatorType::LOGICAL_ORDER_BY:
		throw ParserException("ORDER BY not supported in correlated subquery");
	default:
		throw InternalException("Logical operator type \"%s\" for dependent join", LogicalOperatorToString(plan->type));
	}
}

} // namespace duckdb





#include <algorithm>

namespace duckdb {

HasCorrelatedExpressions::HasCorrelatedExpressions(const vector<CorrelatedColumnInfo> &correlated)
    : has_correlated_expressions(false), correlated_columns(correlated) {
}

void HasCorrelatedExpressions::VisitOperator(LogicalOperator &op) {
	//! The HasCorrelatedExpressions does not recursively visit logical operators, it only visits the current one
	VisitOperatorExpressions(op);
}

unique_ptr<Expression> HasCorrelatedExpressions::VisitReplace(BoundColumnRefExpression &expr,
                                                              unique_ptr<Expression> *expr_ptr) {
	if (expr.depth == 0) {
		return nullptr;
	}
	// correlated column reference
	D_ASSERT(expr.depth == 1);
	has_correlated_expressions = true;
	return nullptr;
}

unique_ptr<Expression> HasCorrelatedExpressions::VisitReplace(BoundSubqueryExpression &expr,
                                                              unique_ptr<Expression> *expr_ptr) {
	if (!expr.IsCorrelated()) {
		return nullptr;
	}
	// check if the subquery contains any of the correlated expressions that we are concerned about in this node
	for (idx_t i = 0; i < correlated_columns.size(); i++) {
		if (std::find(expr.binder->correlated_columns.begin(), expr.binder->correlated_columns.end(),
		              correlated_columns[i]) != expr.binder->correlated_columns.end()) {
			has_correlated_expressions = true;
			break;
		}
	}
	return nullptr;
}

} // namespace duckdb









namespace duckdb {

RewriteCorrelatedExpressions::RewriteCorrelatedExpressions(ColumnBinding base_binding,
                                                           column_binding_map_t<idx_t> &correlated_map)
    : base_binding(base_binding), correlated_map(correlated_map) {
}

void RewriteCorrelatedExpressions::VisitOperator(LogicalOperator &op) {
	VisitOperatorExpressions(op);
}

unique_ptr<Expression> RewriteCorrelatedExpressions::VisitReplace(BoundColumnRefExpression &expr,
                                                                  unique_ptr<Expression> *expr_ptr) {
	if (expr.depth == 0) {
		return nullptr;
	}
	// correlated column reference
	// replace with the entry referring to the duplicate eliminated scan
	// if this assertion occurs it generally means the correlated expressions were not propagated correctly
	// through different binders
	D_ASSERT(expr.depth == 1);
	auto entry = correlated_map.find(expr.binding);
	D_ASSERT(entry != correlated_map.end());

	expr.binding = ColumnBinding(base_binding.table_index, base_binding.column_index + entry->second);
	expr.depth = 0;
	return nullptr;
}

unique_ptr<Expression> RewriteCorrelatedExpressions::VisitReplace(BoundSubqueryExpression &expr,
                                                                  unique_ptr<Expression> *expr_ptr) {
	if (!expr.IsCorrelated()) {
		return nullptr;
	}
	// subquery detected within this subquery
	// recursively rewrite it using the RewriteCorrelatedRecursive class
	RewriteCorrelatedRecursive rewrite(expr, base_binding, correlated_map);
	rewrite.RewriteCorrelatedSubquery(expr);
	return nullptr;
}

RewriteCorrelatedExpressions::RewriteCorrelatedRecursive::RewriteCorrelatedRecursive(
    BoundSubqueryExpression &parent, ColumnBinding base_binding, column_binding_map_t<idx_t> &correlated_map)
    : parent(parent), base_binding(base_binding), correlated_map(correlated_map) {
}

void RewriteCorrelatedExpressions::RewriteCorrelatedRecursive::RewriteCorrelatedSubquery(
    BoundSubqueryExpression &expr) {
	// rewrite the binding in the correlated list of the subquery)
	for (auto &corr : expr.binder->correlated_columns) {
		auto entry = correlated_map.find(corr.binding);
		if (entry != correlated_map.end()) {
			corr.binding = ColumnBinding(base_binding.table_index, base_binding.column_index + entry->second);
		}
	}
	// now rewrite any correlated BoundColumnRef expressions inside the subquery
	ExpressionIterator::EnumerateQueryNodeChildren(*expr.subquery,
	                                               [&](Expression &child) { RewriteCorrelatedExpressions(child); });
}

void RewriteCorrelatedExpressions::RewriteCorrelatedRecursive::RewriteCorrelatedExpressions(Expression &child) {
	if (child.type == ExpressionType::BOUND_COLUMN_REF) {
		// bound column reference
		auto &bound_colref = (BoundColumnRefExpression &)child;
		if (bound_colref.depth == 0) {
			// not a correlated column, ignore
			return;
		}
		// correlated column
		// check the correlated map
		auto entry = correlated_map.find(bound_colref.binding);
		if (entry != correlated_map.end()) {
			// we found the column in the correlated map!
			// update the binding and reduce the depth by 1
			bound_colref.binding = ColumnBinding(base_binding.table_index, base_binding.column_index + entry->second);
			bound_colref.depth--;
		}
	} else if (child.type == ExpressionType::SUBQUERY) {
		// we encountered another subquery: rewrite recursively
		D_ASSERT(child.GetExpressionClass() == ExpressionClass::BOUND_SUBQUERY);
		auto &bound_subquery = (BoundSubqueryExpression &)child;
		RewriteCorrelatedRecursive rewrite(bound_subquery, base_binding, correlated_map);
		rewrite.RewriteCorrelatedSubquery(bound_subquery);
	}
}

RewriteCountAggregates::RewriteCountAggregates(column_binding_map_t<idx_t> &replacement_map)
    : replacement_map(replacement_map) {
}

unique_ptr<Expression> RewriteCountAggregates::VisitReplace(BoundColumnRefExpression &expr,
                                                            unique_ptr<Expression> *expr_ptr) {
	auto entry = replacement_map.find(expr.binding);
	if (entry != replacement_map.end()) {
		// reference to a COUNT(*) aggregate
		// replace this with CASE WHEN COUNT(*) IS NULL THEN 0 ELSE COUNT(*) END
		auto is_null = make_unique<BoundOperatorExpression>(ExpressionType::OPERATOR_IS_NULL, LogicalType::BOOLEAN);
		is_null->children.push_back(expr.Copy());
		auto check = move(is_null);
		auto result_if_true = make_unique<BoundConstantExpression>(Value::Numeric(expr.return_type, 0));
		auto result_if_false = move(*expr_ptr);
		return make_unique<BoundCaseExpression>(move(check), move(result_if_true), move(result_if_false));
	}
	return nullptr;
}

} // namespace duckdb













namespace duckdb {

Binding::Binding(BindingType binding_type, const string &alias, vector<LogicalType> coltypes, vector<string> colnames,
                 idx_t index)
    : binding_type(binding_type), alias(alias), index(index), types(move(coltypes)), names(move(colnames)) {
	D_ASSERT(types.size() == names.size());
	for (idx_t i = 0; i < names.size(); i++) {
		auto &name = names[i];
		D_ASSERT(!name.empty());
		if (name_map.find(name) != name_map.end()) {
			throw BinderException("table \"%s\" has duplicate column name \"%s\"", alias, name);
		}
		name_map[name] = i;
	}
}

bool Binding::TryGetBindingIndex(const string &column_name, column_t &result) {
	auto entry = name_map.find(column_name);
	if (entry == name_map.end()) {
		return false;
	}
	auto column_info = entry->second;
	result = column_info;
	return true;
}

column_t Binding::GetBindingIndex(const string &column_name) {
	column_t result;
	if (!TryGetBindingIndex(column_name, result)) {
		throw InternalException("Binding index for column \"%s\" not found", column_name);
	}
	return result;
}

bool Binding::HasMatchingBinding(const string &column_name) {
	column_t result;
	return TryGetBindingIndex(column_name, result);
}

string Binding::ColumnNotFoundError(const string &column_name) const {
	return StringUtil::Format("Values list \"%s\" does not have a column named \"%s\"", alias, column_name);
}

BindResult Binding::Bind(ColumnRefExpression &colref, idx_t depth) {
	column_t column_index;
	bool success = false;
	success = TryGetBindingIndex(colref.GetColumnName(), column_index);
	if (!success) {
		return BindResult(ColumnNotFoundError(colref.GetColumnName()));
	}
	ColumnBinding binding;
	binding.table_index = index;
	binding.column_index = column_index;
	LogicalType sql_type = types[column_index];
	if (colref.alias.empty()) {
		colref.alias = names[column_index];
	}
	return BindResult(make_unique<BoundColumnRefExpression>(colref.GetName(), sql_type, binding, depth));
}

StandardEntry *Binding::GetStandardEntry() {
	return nullptr;
}

EntryBinding::EntryBinding(const string &alias, vector<LogicalType> types_p, vector<string> names_p, idx_t index,
                           StandardEntry &entry)
    : Binding(BindingType::CATALOG_ENTRY, alias, move(types_p), move(names_p), index), entry(entry) {
}

StandardEntry *EntryBinding::GetStandardEntry() {
	return &this->entry;
}

TableBinding::TableBinding(const string &alias, vector<LogicalType> types_p, vector<string> names_p, LogicalGet &get,
                           idx_t index, bool add_row_id)
    : Binding(BindingType::TABLE, alias, move(types_p), move(names_p), index), get(get) {
	if (add_row_id) {
		if (name_map.find("rowid") == name_map.end()) {
			name_map["rowid"] = COLUMN_IDENTIFIER_ROW_ID;
		}
	}
}

static void BakeTableName(ParsedExpression &expr, const string &table_name) {
	if (expr.type == ExpressionType::COLUMN_REF) {
		auto &colref = (ColumnRefExpression &)expr;
		D_ASSERT(!colref.IsQualified());
		auto &col_names = colref.column_names;
		col_names.insert(col_names.begin(), table_name);
	}
	ParsedExpressionIterator::EnumerateChildren(
	    expr, [&](const ParsedExpression &child) { BakeTableName((ParsedExpression &)child, table_name); });
}

unique_ptr<ParsedExpression> TableBinding::ExpandGeneratedColumn(const string &column_name) {
	auto catalog_entry = GetStandardEntry();
	D_ASSERT(catalog_entry); // Should only be called on a TableBinding

	D_ASSERT(catalog_entry->type == CatalogType::TABLE_ENTRY);
	auto table_entry = (TableCatalogEntry *)catalog_entry;

	// Get the index of the generated column
	auto column_index = GetBindingIndex(column_name);
	D_ASSERT(table_entry->columns[column_index].Generated());
	// Get a copy of the generated column
	auto expression = table_entry->columns[column_index].GeneratedExpression().Copy();
	BakeTableName(*expression, alias);
	return (expression);
}

BindResult TableBinding::Bind(ColumnRefExpression &colref, idx_t depth) {
	auto &column_name = colref.GetColumnName();
	column_t column_index;
	bool success = false;
	success = TryGetBindingIndex(column_name, column_index);
	if (!success) {
		return BindResult(ColumnNotFoundError(column_name));
	}
#ifdef DEBUG
	auto entry = GetStandardEntry();
	if (entry) {
		D_ASSERT(entry->type == CatalogType::TABLE_ENTRY);
		auto table_entry = (TableCatalogEntry *)entry;
		//! Either there is no table, or the columns category has to be standard
		if (column_index != COLUMN_IDENTIFIER_ROW_ID) {
			D_ASSERT(table_entry->columns[column_index].Category() == TableColumnType::STANDARD);
		}
	}
#endif /* DEBUG */
	// fetch the type of the column
	LogicalType col_type;
	if (column_index == COLUMN_IDENTIFIER_ROW_ID) {
		// row id: BIGINT type
		col_type = LogicalType::BIGINT;
	} else {
		// normal column: fetch type from base column
		col_type = types[column_index];
		if (colref.alias.empty()) {
			colref.alias = names[column_index];
		}
	}

	auto &column_ids = get.column_ids;
	// check if the entry already exists in the column list for the table
	ColumnBinding binding;

	binding.column_index = column_ids.size();
	for (idx_t i = 0; i < column_ids.size(); i++) {
		if (column_ids[i] == column_index) {
			binding.column_index = i;
			break;
		}
	}
	if (binding.column_index == column_ids.size()) {
		// column binding not found: add it to the list of bindings
		column_ids.push_back(column_index);
	}
	binding.table_index = index;
	return BindResult(make_unique<BoundColumnRefExpression>(colref.GetName(), col_type, binding, depth));
}

StandardEntry *TableBinding::GetStandardEntry() {
	return get.GetTable();
}

string TableBinding::ColumnNotFoundError(const string &column_name) const {
	return StringUtil::Format("Table \"%s\" does not have a column named \"%s\"", alias, column_name);
}

MacroBinding::MacroBinding(vector<LogicalType> types_p, vector<string> names_p, string macro_name_p)
    : Binding(BindingType::MACRO, MacroBinding::MACRO_NAME, move(types_p), move(names_p), -1),
      macro_name(move(macro_name_p)) {
}

BindResult MacroBinding::Bind(ColumnRefExpression &colref, idx_t depth) {
	column_t column_index;
	if (!TryGetBindingIndex(colref.GetColumnName(), column_index)) {
		throw InternalException("Column %s not found in macro", colref.GetColumnName());
	}
	ColumnBinding binding;
	binding.table_index = index;
	binding.column_index = column_index;

	// we are binding a parameter to create the macro, no arguments are supplied
	return BindResult(make_unique<BoundColumnRefExpression>(colref.GetName(), types[column_index], binding, depth));
}

unique_ptr<ParsedExpression> MacroBinding::ParamToArg(ColumnRefExpression &colref) {
	column_t column_index;
	if (!TryGetBindingIndex(colref.GetColumnName(), column_index)) {
		throw InternalException("Column %s not found in macro", colref.GetColumnName());
	}
	auto arg = arguments[column_index]->Copy();
	arg->alias = colref.alias;
	return arg;
}

} // namespace duckdb



namespace duckdb {

void TableFilterSet::PushFilter(idx_t column_index, unique_ptr<TableFilter> filter) {
	auto entry = filters.find(column_index);
	if (entry == filters.end()) {
		// no filter yet: push the filter directly
		filters[column_index] = move(filter);
	} else {
		// there is already a filter: AND it together
		if (entry->second->filter_type == TableFilterType::CONJUNCTION_AND) {
			auto &and_filter = (ConjunctionAndFilter &)*entry->second;
			and_filter.child_filters.push_back(move(filter));
		} else {
			auto and_filter = make_unique<ConjunctionAndFilter>();
			and_filter->child_filters.push_back(move(entry->second));
			and_filter->child_filters.push_back(move(filter));
			filters[column_index] = move(and_filter);
		}
	}
}

} // namespace duckdb



namespace duckdb {

Block::Block(Allocator &allocator, block_id_t id)
    : FileBuffer(allocator, FileBufferType::BLOCK, Storage::BLOCK_ALLOC_SIZE), id(id) {
}

Block::Block(FileBuffer &source, block_id_t id) : FileBuffer(source, FileBufferType::BLOCK), id(id) {
	D_ASSERT(GetMallocedSize() == Storage::BLOCK_ALLOC_SIZE);
	D_ASSERT(size == Storage::BLOCK_SIZE);
}

} // namespace duckdb



namespace duckdb {

BufferHandle::BufferHandle(shared_ptr<BlockHandle> handle, FileBuffer *node) : handle(move(handle)), node(node) {
}

BufferHandle::~BufferHandle() {
	auto &buffer_manager = BufferManager::GetBufferManager(handle->db);
	buffer_manager.Unpin(handle);
}

data_ptr_t BufferHandle::Ptr() {
	return node->buffer;
}

} // namespace duckdb






namespace duckdb {

ManagedBuffer::ManagedBuffer(DatabaseInstance &db, idx_t size, bool can_destroy, block_id_t id)
    : FileBuffer(Allocator::Get(db), FileBufferType::MANAGED_BUFFER, size), db(db), can_destroy(can_destroy), id(id) {
	D_ASSERT(id >= MAXIMUM_BLOCK);
	D_ASSERT(size >= Storage::BLOCK_SIZE);
}

} // namespace duckdb







namespace duckdb {

BlockHandle::BlockHandle(DatabaseInstance &db, block_id_t block_id_p)
    : db(db), readers(0), block_id(block_id_p), buffer(nullptr), eviction_timestamp(0), can_destroy(false) {
	eviction_timestamp = 0;
	state = BlockState::BLOCK_UNLOADED;
	memory_usage = Storage::BLOCK_ALLOC_SIZE;
}

BlockHandle::BlockHandle(DatabaseInstance &db, block_id_t block_id_p, unique_ptr<FileBuffer> buffer_p,
                         bool can_destroy_p, idx_t block_size)
    : db(db), readers(0), block_id(block_id_p), eviction_timestamp(0), can_destroy(can_destroy_p) {
	D_ASSERT(block_size >= Storage::BLOCK_SIZE);
	buffer = move(buffer_p);
	state = BlockState::BLOCK_LOADED;
	memory_usage = block_size + Storage::BLOCK_HEADER_SIZE;
}

BlockHandle::~BlockHandle() {
	auto &buffer_manager = BufferManager::GetBufferManager(db);
	// no references remain to this block: erase
	if (state == BlockState::BLOCK_LOADED) {
		// the block is still loaded in memory: erase it
		buffer.reset();
		buffer_manager.current_memory -= memory_usage;
	}
	buffer_manager.UnregisterBlock(block_id, can_destroy);
}

unique_ptr<BufferHandle> BlockHandle::Load(shared_ptr<BlockHandle> &handle) {
	if (handle->state == BlockState::BLOCK_LOADED) {
		// already loaded
		D_ASSERT(handle->buffer);
		return make_unique<BufferHandle>(handle, handle->buffer.get());
	}

	auto &buffer_manager = BufferManager::GetBufferManager(handle->db);
	auto &block_manager = BlockManager::GetBlockManager(handle->db);
	if (handle->block_id < MAXIMUM_BLOCK) {
		auto block = make_unique<Block>(Allocator::Get(handle->db), handle->block_id);
		block_manager.Read(*block);
		handle->buffer = move(block);
	} else {
		if (handle->can_destroy) {
			return nullptr;
		} else {
			handle->buffer = buffer_manager.ReadTemporaryBuffer(handle->block_id);
		}
	}
	handle->state = BlockState::BLOCK_LOADED;
	return make_unique<BufferHandle>(handle, handle->buffer.get());
}

void BlockHandle::Unload() {
	auto &buffer_manager = BufferManager::GetBufferManager(db);
	if (state == BlockState::BLOCK_UNLOADED) {
		// already unloaded: nothing to do
		return;
	}
	D_ASSERT(CanUnload());
	D_ASSERT(memory_usage >= Storage::BLOCK_ALLOC_SIZE);

	if (block_id >= MAXIMUM_BLOCK && !can_destroy) {
		// temporary block that cannot be destroyed: write to temporary file
		buffer_manager.WriteTemporaryBuffer((ManagedBuffer &)*buffer);
	}
	buffer.reset();
	buffer_manager.current_memory -= memory_usage;
	state = BlockState::BLOCK_UNLOADED;
}

bool BlockHandle::CanUnload() {
	if (state == BlockState::BLOCK_UNLOADED) {
		// already unloaded
		return false;
	}
	if (readers > 0) {
		// there are active readers
		return false;
	}
	auto &buffer_manager = BufferManager::GetBufferManager(db);
	if (block_id >= MAXIMUM_BLOCK && !can_destroy && buffer_manager.temp_directory.empty()) {
		// in order to unload this block we need to write it to a temporary buffer
		// however, no temporary directory is specified!
		// hence we cannot unload the block
		return false;
	}
	return true;
}

struct BufferEvictionNode {
	BufferEvictionNode(weak_ptr<BlockHandle> handle_p, idx_t timestamp_p)
	    : handle(move(handle_p)), timestamp(timestamp_p) {
		D_ASSERT(!handle.expired());
	}

	weak_ptr<BlockHandle> handle;
	idx_t timestamp;

	bool CanUnload(BlockHandle &handle_p) {
		if (timestamp != handle_p.eviction_timestamp) {
			// handle was used in between
			return false;
		}
		return handle_p.CanUnload();
	}

	shared_ptr<BlockHandle> TryGetBlockHandle() {
		auto handle_p = handle.lock();
		if (!handle_p) {
			// BlockHandle has been destroyed
			return nullptr;
		}
		if (!CanUnload(*handle_p)) {
			// handle was used in between
			return nullptr;
		}
		// this is the latest node in the queue with this handle
		return handle_p;
	}
};

typedef duckdb_moodycamel::ConcurrentQueue<unique_ptr<BufferEvictionNode>> eviction_queue_t;

struct EvictionQueue {
	eviction_queue_t q;
};

class TemporaryDirectoryHandle {
public:
	TemporaryDirectoryHandle(DatabaseInstance &db, string path_p) : db(db), temp_directory(move(path_p)) {
		auto &fs = FileSystem::GetFileSystem(db);
		if (!temp_directory.empty()) {
			fs.CreateDirectory(temp_directory);
		}
	}
	~TemporaryDirectoryHandle() {
		auto &fs = FileSystem::GetFileSystem(db);
		if (!temp_directory.empty()) {
			fs.RemoveDirectory(temp_directory);
		}
	}

private:
	DatabaseInstance &db;
	string temp_directory;
};

void BufferManager::SetTemporaryDirectory(string new_dir) {
	if (temp_directory_handle) {
		throw NotImplementedException("Cannot switch temporary directory after the current one has been used");
	}
	this->temp_directory = move(new_dir);
}

BufferManager::BufferManager(DatabaseInstance &db, string tmp, idx_t maximum_memory)
    : db(db), current_memory(0), maximum_memory(maximum_memory), temp_directory(move(tmp)),
      queue(make_unique<EvictionQueue>()), temporary_id(MAXIMUM_BLOCK) {
}

BufferManager::~BufferManager() {
}

shared_ptr<BlockHandle> BufferManager::RegisterBlock(block_id_t block_id) {
	lock_guard<mutex> lock(blocks_lock);
	// check if the block already exists
	auto entry = blocks.find(block_id);
	if (entry != blocks.end()) {
		// already exists: check if it hasn't expired yet
		auto existing_ptr = entry->second.lock();
		if (existing_ptr) {
			//! it hasn't! return it
			return existing_ptr;
		}
	}
	// create a new block pointer for this block
	auto result = make_shared<BlockHandle>(db, block_id);
	// register the block pointer in the set of blocks as a weak pointer
	blocks[block_id] = weak_ptr<BlockHandle>(result);
	return result;
}

shared_ptr<BlockHandle> BufferManager::ConvertToPersistent(BlockManager &block_manager, block_id_t block_id,
                                                           shared_ptr<BlockHandle> old_block) {

	// pin the old block to ensure we have it loaded in memory
	auto old_handle = Pin(old_block);
	D_ASSERT(old_block->state == BlockState::BLOCK_LOADED);
	D_ASSERT(old_block->buffer);

	// register a block with the new block id
	auto new_block = RegisterBlock(block_id);
	D_ASSERT(new_block->state == BlockState::BLOCK_UNLOADED);
	D_ASSERT(new_block->readers == 0);

#ifdef DEBUG
	lock_guard<mutex> b_lock(blocks_lock);
#endif

	// move the data from the old block into data for the new block
	new_block->state = BlockState::BLOCK_LOADED;
	new_block->buffer = make_unique<Block>(*old_block->buffer, block_id);

	// clear the old buffer and unload it
	old_handle.reset();
	old_block->buffer.reset();
	old_block->state = BlockState::BLOCK_UNLOADED;
	old_block->memory_usage = 0;
	old_block.reset();

	// persist the new block to disk
	block_manager.Write(*new_block->buffer, block_id);

	AddToEvictionQueue(new_block);

	return new_block;
}

shared_ptr<BlockHandle> BufferManager::RegisterMemory(idx_t block_size, bool can_destroy) {
	auto alloc_size = block_size + Storage::BLOCK_HEADER_SIZE;
	// first evict blocks until we have enough memory to store this buffer
	if (!EvictBlocks(alloc_size, maximum_memory)) {
		throw OutOfMemoryException("could not allocate block of %lld bytes%s", alloc_size, InMemoryWarning());
	}

	// allocate the buffer
	auto temp_id = ++temporary_id;
	auto buffer = make_unique<ManagedBuffer>(db, block_size, can_destroy, temp_id);

	// create a new block pointer for this block
	return make_shared<BlockHandle>(db, temp_id, move(buffer), can_destroy, block_size);
}

unique_ptr<BufferHandle> BufferManager::Allocate(idx_t block_size) {
	auto block = RegisterMemory(block_size, true);
	return Pin(block);
}

void BufferManager::ReAllocate(shared_ptr<BlockHandle> &handle, idx_t block_size) {
	D_ASSERT(block_size >= Storage::BLOCK_SIZE);
	lock_guard<mutex> lock(handle->lock);
	D_ASSERT(handle->state == BlockState::BLOCK_LOADED);
	auto alloc_size = block_size + Storage::BLOCK_HEADER_SIZE;
	int64_t required_memory = alloc_size - handle->memory_usage;
	if (required_memory == 0) {
		return;
	} else if (required_memory > 0) {
		// evict blocks until we have space to resize this block
		if (!EvictBlocks(required_memory, maximum_memory)) {
			throw OutOfMemoryException("failed to resize block from %lld to %lld%s", handle->memory_usage, alloc_size,
			                           InMemoryWarning());
		}
	} else {
		// no need to evict blocks
		current_memory -= idx_t(-required_memory);
	}

	// resize and adjust current memory
	handle->buffer->Resize(block_size);
	handle->memory_usage = alloc_size;
}

unique_ptr<BufferHandle> BufferManager::Pin(shared_ptr<BlockHandle> &handle) {
	idx_t required_memory;
	{
		// lock the block
		lock_guard<mutex> lock(handle->lock);
		// check if the block is already loaded
		if (handle->state == BlockState::BLOCK_LOADED) {
			// the block is loaded, increment the reader count and return a pointer to the handle
			handle->readers++;
			return handle->Load(handle);
		}
		required_memory = handle->memory_usage;
	}
	// evict blocks until we have space for the current block
	if (!EvictBlocks(required_memory, maximum_memory)) {
		throw OutOfMemoryException("failed to pin block of size %lld%s", required_memory, InMemoryWarning());
	}
	// lock the handle again and repeat the check (in case anybody loaded in the mean time)
	lock_guard<mutex> lock(handle->lock);
	// check if the block is already loaded
	if (handle->state == BlockState::BLOCK_LOADED) {
		// the block is loaded, increment the reader count and return a pointer to the handle
		handle->readers++;
		current_memory -= required_memory;
		return handle->Load(handle);
	}
	// now we can actually load the current block
	D_ASSERT(handle->readers == 0);
	handle->readers = 1;
	return handle->Load(handle);
}

void BufferManager::AddToEvictionQueue(shared_ptr<BlockHandle> &handle) {
	D_ASSERT(handle->readers == 0);
	handle->eviction_timestamp++;
	PurgeQueue();
	queue->q.enqueue(make_unique<BufferEvictionNode>(weak_ptr<BlockHandle>(handle), handle->eviction_timestamp));
}

void BufferManager::Unpin(shared_ptr<BlockHandle> &handle) {
	lock_guard<mutex> lock(handle->lock);
	D_ASSERT(handle->readers > 0);
	handle->readers--;
	if (handle->readers == 0) {
		AddToEvictionQueue(handle);
	}
}

bool BufferManager::EvictBlocks(idx_t extra_memory, idx_t memory_limit) {
	PurgeQueue();

	unique_ptr<BufferEvictionNode> node;
	current_memory += extra_memory;
	while (current_memory > memory_limit) {
		// get a block to unpin from the queue
		if (!queue->q.try_dequeue(node)) {
			current_memory -= extra_memory;
			return false;
		}
		// get a reference to the underlying block pointer
		auto handle = node->TryGetBlockHandle();
		if (!handle) {
			continue;
		}
		// we might be able to free this block: grab the mutex and check if we can free it
		lock_guard<mutex> lock(handle->lock);
		if (!node->CanUnload(*handle)) {
			// something changed in the mean-time, bail out
			continue;
		}
		// hooray, we can unload the block
		// release the memory and mark the block as unloaded
		handle->Unload();
	}
	return true;
}

void BufferManager::PurgeQueue() {
	unique_ptr<BufferEvictionNode> node;
	while (true) {
		if (!queue->q.try_dequeue(node)) {
			break;
		}
		auto handle = node->TryGetBlockHandle();
		if (!handle) {
			continue;
		} else {
			queue->q.enqueue(move(node));
			break;
		}
	}
}

void BufferManager::UnregisterBlock(block_id_t block_id, bool can_destroy) {
	if (block_id >= MAXIMUM_BLOCK) {
		// in-memory buffer: destroy the buffer
		if (!can_destroy) {
			// buffer could have been offloaded to disk: remove the file
			DeleteTemporaryFile(block_id);
		}
	} else {
		lock_guard<mutex> lock(blocks_lock);
		// on-disk block: erase from list of blocks in manager
		blocks.erase(block_id);
	}
}
void BufferManager::SetLimit(idx_t limit) {
	lock_guard<mutex> l_lock(limit_lock);
	// try to evict until the limit is reached
	if (!EvictBlocks(0, limit)) {
		throw OutOfMemoryException(
		    "Failed to change memory limit to %lld: could not free up enough memory for the new limit%s", limit,
		    InMemoryWarning());
	}
	idx_t old_limit = maximum_memory;
	// set the global maximum memory to the new limit if successful
	maximum_memory = limit;
	// evict again
	if (!EvictBlocks(0, limit)) {
		// failed: go back to old limit
		maximum_memory = old_limit;
		throw OutOfMemoryException(
		    "Failed to change memory limit to %lld: could not free up enough memory for the new limit%s", limit,
		    InMemoryWarning());
	}
}

string BufferManager::GetTemporaryPath(block_id_t id) {
	auto &fs = FileSystem::GetFileSystem(db);
	return fs.JoinPath(temp_directory, to_string(id) + ".block");
}

void BufferManager::RequireTemporaryDirectory() {
	if (temp_directory.empty()) {
		throw Exception(
		    "Out-of-memory: cannot write buffer because no temporary directory is specified!\nTo enable "
		    "temporary buffer eviction set a temporary directory using PRAGMA temp_directory='/path/to/tmp.tmp'");
	}
	lock_guard<mutex> temp_handle_guard(temp_handle_lock);
	if (!temp_directory_handle) {
		// temp directory has not been created yet: initialize it
		temp_directory_handle = make_unique<TemporaryDirectoryHandle>(db, temp_directory);
	}
}

void BufferManager::WriteTemporaryBuffer(ManagedBuffer &buffer) {
	RequireTemporaryDirectory();

	D_ASSERT(buffer.size >= Storage::BLOCK_SIZE);
	// get the path to write to
	auto path = GetTemporaryPath(buffer.id);
	// create the file and write the size followed by the buffer contents
	auto &fs = FileSystem::GetFileSystem(db);
	auto handle = fs.OpenFile(path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE);
	handle->Write(&buffer.size, sizeof(idx_t), 0);
	buffer.Write(*handle, sizeof(idx_t));
}

unique_ptr<FileBuffer> BufferManager::ReadTemporaryBuffer(block_id_t id) {
	D_ASSERT(!temp_directory.empty());
	D_ASSERT(temp_directory_handle.get());
	idx_t block_size;
	// open the temporary file and read the size
	auto path = GetTemporaryPath(id);
	auto &fs = FileSystem::GetFileSystem(db);
	auto handle = fs.OpenFile(path, FileFlags::FILE_FLAGS_READ);
	handle->Read(&block_size, sizeof(idx_t), 0);

	// now allocate a buffer of this size and read the data into that buffer
	auto buffer = make_unique<ManagedBuffer>(db, block_size, false, id);
	buffer->Read(*handle, sizeof(idx_t));

	handle.reset();
	DeleteTemporaryFile(id);
	return move(buffer);
}

void BufferManager::DeleteTemporaryFile(block_id_t id) {
	if (temp_directory.empty() || !temp_directory_handle) {
		return;
	}
	auto &fs = FileSystem::GetFileSystem(db);
	auto path = GetTemporaryPath(id);
	if (fs.FileExists(path)) {
		fs.RemoveFile(path);
	}
}

string BufferManager::InMemoryWarning() {
	if (!temp_directory.empty()) {
		return "";
	}
	return "\nDatabase is launched in in-memory mode and no temporary directory is specified."
	       "\nUnused blocks cannot be offloaded to disk."
	       "\n\nLaunch the database with a persistent storage back-end"
	       "\nOr set PRAGMA temp_directory='/path/to/tmp.tmp'";
}

} // namespace duckdb















namespace duckdb {

TableDataReader::TableDataReader(MetaBlockReader &reader, BoundCreateTableInfo &info) : reader(reader), info(info) {
	info.data = make_unique<PersistentTableData>(info.Base().columns.size());
}

void TableDataReader::ReadTableData() {
	auto &columns = info.Base().columns;
	D_ASSERT(columns.size() > 0);

	// deserialize the total table statistics
	info.data->column_stats.reserve(columns.size());
	for (idx_t i = 0; i < columns.size(); i++) {
		auto &col = columns[i];
		// Have to use 'Generated()' here, storage_oid is uninitialized here
		if (col.Generated()) {
			continue;
		}
		info.data->column_stats.push_back(BaseStatistics::Deserialize(reader, columns[i].Type()));
	}

	// deserialize each of the individual row groups
	auto row_group_count = reader.Read<uint64_t>();
	info.data->row_groups.reserve(row_group_count);
	for (idx_t i = 0; i < row_group_count; i++) {
		auto row_group_pointer = RowGroup::Deserialize(reader, columns);
		info.data->row_groups.push_back(move(row_group_pointer));
	}
}

} // namespace duckdb








namespace duckdb {

TableDataWriter::TableDataWriter(DatabaseInstance &, CheckpointManager &checkpoint_manager, TableCatalogEntry &table,
                                 MetaBlockWriter &meta_writer)
    : checkpoint_manager(checkpoint_manager), table(table), meta_writer(meta_writer) {
}

TableDataWriter::~TableDataWriter() {
}

BlockPointer TableDataWriter::WriteTableData() {
	// start scanning the table and append the data to the uncompressed segments
	return table.storage->Checkpoint(*this);
}

CompressionType TableDataWriter::GetColumnCompressionType(idx_t i) {
	return table.columns[i].CompressionType();
}

} // namespace duckdb





namespace duckdb {

WriteOverflowStringsToDisk::WriteOverflowStringsToDisk(DatabaseInstance &db)
    : db(db), block_id(INVALID_BLOCK), offset(0) {
}

WriteOverflowStringsToDisk::~WriteOverflowStringsToDisk() {
	auto &block_manager = BlockManager::GetBlockManager(db);
	if (offset > 0) {
		block_manager.Write(*handle->node, block_id);
	}
}

void WriteOverflowStringsToDisk::WriteString(string_t string, block_id_t &result_block, int32_t &result_offset) {
	auto &buffer_manager = BufferManager::GetBufferManager(db);
	auto &block_manager = BlockManager::GetBlockManager(db);
	if (!handle) {
		handle = buffer_manager.Allocate(Storage::BLOCK_SIZE);
	}
	// first write the length of the string
	if (block_id == INVALID_BLOCK || offset + 2 * sizeof(uint32_t) >= STRING_SPACE) {
		AllocateNewBlock(block_manager.GetFreeBlockId());
	}
	result_block = block_id;
	result_offset = offset;

	// GZIP the string
	auto uncompressed_size = string.GetSize();
	MiniZStream s;
	size_t compressed_size = 0;
	compressed_size = s.MaxCompressedLength(uncompressed_size);
	auto compressed_buf = unique_ptr<data_t[]>(new data_t[compressed_size]);
	s.Compress((const char *)string.GetDataUnsafe(), uncompressed_size, (char *)compressed_buf.get(), &compressed_size);
	string_t compressed_string((const char *)compressed_buf.get(), compressed_size);

	// store sizes
	Store<uint32_t>(compressed_size, handle->node->buffer + offset);
	Store<uint32_t>(uncompressed_size, handle->node->buffer + offset + sizeof(uint32_t));

	// now write the remainder of the string
	offset += 2 * sizeof(uint32_t);
	auto strptr = compressed_string.GetDataUnsafe();
	uint32_t remaining = compressed_size;
	while (remaining > 0) {
		uint32_t to_write = MinValue<uint32_t>(remaining, STRING_SPACE - offset);
		if (to_write > 0) {
			memcpy(handle->node->buffer + offset, strptr, to_write);

			remaining -= to_write;
			offset += to_write;
			strptr += to_write;
		}
		if (remaining > 0) {
			// there is still remaining stuff to write
			// first get the new block id and write it to the end of the previous block
			auto new_block_id = block_manager.GetFreeBlockId();
			Store<block_id_t>(new_block_id, handle->node->buffer + offset);
			// now write the current block to disk and allocate a new block
			AllocateNewBlock(new_block_id);
		}
	}
}

void WriteOverflowStringsToDisk::AllocateNewBlock(block_id_t new_block_id) {
	auto &block_manager = BlockManager::GetBlockManager(db);
	if (block_id != INVALID_BLOCK) {
		// there is an old block, write it first
		block_manager.Write(*handle->node, block_id);
	}
	offset = 0;
	block_id = new_block_id;
}

} // namespace duckdb


























namespace duckdb {

void ReorderTableEntries(vector<TableCatalogEntry *> &tables);

CheckpointManager::CheckpointManager(DatabaseInstance &db) : db(db) {
}

void CheckpointManager::CreateCheckpoint() {
	auto &config = DBConfig::GetConfig(db);
	auto &storage_manager = StorageManager::GetStorageManager(db);
	if (storage_manager.InMemory()) {
		return;
	}
	// assert that the checkpoint manager hasn't been used before
	D_ASSERT(!metadata_writer);

	auto &block_manager = BlockManager::GetBlockManager(db);
	block_manager.StartCheckpoint();

	//! Set up the writers for the checkpoints
	metadata_writer = make_unique<MetaBlockWriter>(db);
	tabledata_writer = make_unique<MetaBlockWriter>(db);

	// get the id of the first meta block
	block_id_t meta_block = metadata_writer->block->id;

	vector<SchemaCatalogEntry *> schemas;
	// we scan the set of committed schemas
	auto &catalog = Catalog::GetCatalog(db);
	catalog.schemas->Scan([&](CatalogEntry *entry) { schemas.push_back((SchemaCatalogEntry *)entry); });
	// write the actual data into the database
	// write the amount of schemas
	metadata_writer->Write<uint32_t>(schemas.size());
	for (auto &schema : schemas) {
		WriteSchema(*schema);
	}
	FlushPartialSegments();
	// flush the meta data to disk
	metadata_writer->Flush();
	tabledata_writer->Flush();

	// write a checkpoint flag to the WAL
	// this protects against the rare event that the database crashes AFTER writing the file, but BEFORE truncating the
	// WAL we write an entry CHECKPOINT "meta_block_id" into the WAL upon loading, if we see there is an entry
	// CHECKPOINT "meta_block_id", and the id MATCHES the head idin the file we know that the database was successfully
	// checkpointed, so we know that we should avoid replaying the WAL to avoid duplicating data
	auto wal = storage_manager.GetWriteAheadLog();
	wal->WriteCheckpoint(meta_block);
	wal->Flush();

	if (config.checkpoint_abort == CheckpointAbort::DEBUG_ABORT_BEFORE_HEADER) {
		throw IOException("Checkpoint aborted before header write because of PRAGMA checkpoint_abort flag");
	}

	// finally write the updated header
	DatabaseHeader header;
	header.meta_block = meta_block;
	block_manager.WriteHeader(header);

	if (config.checkpoint_abort == CheckpointAbort::DEBUG_ABORT_BEFORE_TRUNCATE) {
		throw IOException("Checkpoint aborted before truncate because of PRAGMA checkpoint_abort flag");
	}

	// truncate the WAL
	wal->Truncate(0);

	// mark all blocks written as part of the metadata as modified
	for (auto &block_id : metadata_writer->written_blocks) {
		block_manager.MarkBlockAsModified(block_id);
	}
	for (auto &block_id : tabledata_writer->written_blocks) {
		block_manager.MarkBlockAsModified(block_id);
	}
}

void CheckpointManager::LoadFromStorage() {
	auto &block_manager = BlockManager::GetBlockManager(db);
	block_id_t meta_block = block_manager.GetMetaBlock();
	if (meta_block < 0) {
		// storage is empty
		return;
	}

	Connection con(db);
	con.BeginTransaction();
	// create the MetaBlockReader to read from the storage
	MetaBlockReader reader(db, meta_block);
	uint32_t schema_count = reader.Read<uint32_t>();
	for (uint32_t i = 0; i < schema_count; i++) {
		ReadSchema(*con.context, reader);
	}
	con.Commit();
}

//===--------------------------------------------------------------------===//
// Schema
//===--------------------------------------------------------------------===//
void CheckpointManager::WriteSchema(SchemaCatalogEntry &schema) {
	// write the schema data
	schema.Serialize(*metadata_writer);
	// then, we fetch the tables/views/sequences information
	vector<TableCatalogEntry *> tables;
	vector<ViewCatalogEntry *> views;
	schema.Scan(CatalogType::TABLE_ENTRY, [&](CatalogEntry *entry) {
		if (entry->internal) {
			return;
		}
		if (entry->type == CatalogType::TABLE_ENTRY) {
			tables.push_back((TableCatalogEntry *)entry);
		} else if (entry->type == CatalogType::VIEW_ENTRY) {
			views.push_back((ViewCatalogEntry *)entry);
		} else {
			throw NotImplementedException("Catalog type for entries");
		}
	});
	vector<SequenceCatalogEntry *> sequences;
	schema.Scan(CatalogType::SEQUENCE_ENTRY, [&](CatalogEntry *entry) {
		if (entry->internal) {
			return;
		}
		sequences.push_back((SequenceCatalogEntry *)entry);
	});

	vector<TypeCatalogEntry *> custom_types;
	schema.Scan(CatalogType::TYPE_ENTRY, [&](CatalogEntry *entry) {
		if (entry->internal) {
			return;
		}
		custom_types.push_back((TypeCatalogEntry *)entry);
	});

	vector<ScalarMacroCatalogEntry *> macros;
	schema.Scan(CatalogType::SCALAR_FUNCTION_ENTRY, [&](CatalogEntry *entry) {
		if (entry->internal) {
			return;
		}
		if (entry->type == CatalogType::MACRO_ENTRY) {
			macros.push_back((ScalarMacroCatalogEntry *)entry);
		}
	});

	vector<TableMacroCatalogEntry *> table_macros;
	schema.Scan(CatalogType::TABLE_FUNCTION_ENTRY, [&](CatalogEntry *entry) {
		if (entry->internal) {
			return;
		}
		if (entry->type == CatalogType::TABLE_MACRO_ENTRY) {
			table_macros.push_back((TableMacroCatalogEntry *)entry);
		}
	});

	FieldWriter writer(*metadata_writer);
	writer.WriteField<uint32_t>(custom_types.size());
	writer.WriteField<uint32_t>(sequences.size());
	writer.WriteField<uint32_t>(tables.size());
	writer.WriteField<uint32_t>(views.size());
	writer.WriteField<uint32_t>(macros.size());
	writer.WriteField<uint32_t>(table_macros.size());
	writer.Finalize();

	// write the custom_types
	for (auto &custom_type : custom_types) {
		WriteType(*custom_type);
	}

	// write the sequences
	for (auto &seq : sequences) {
		WriteSequence(*seq);
	}
	// reorder tables because of foreign key constraint
	ReorderTableEntries(tables);
	// now write the tables
	for (auto &table : tables) {
		WriteTable(*table);
	}
	// now write the views
	for (auto &view : views) {
		WriteView(*view);
	}

	// finally write the macro's
	for (auto &macro : macros) {
		WriteMacro(*macro);
	}

	// finally write the macro's
	for (auto &macro : table_macros) {
		WriteTableMacro(*macro);
	}
}

void CheckpointManager::ReadSchema(ClientContext &context, MetaBlockReader &reader) {
	auto &catalog = Catalog::GetCatalog(db);

	// read the schema and create it in the catalog
	auto info = SchemaCatalogEntry::Deserialize(reader);
	// we set create conflict to ignore to ignore the failure of recreating the main schema
	info->on_conflict = OnCreateConflict::IGNORE_ON_CONFLICT;
	catalog.CreateSchema(context, info.get());

	// first read all the counts
	FieldReader field_reader(reader);
	uint32_t enum_count = field_reader.ReadRequired<uint32_t>();
	uint32_t seq_count = field_reader.ReadRequired<uint32_t>();
	uint32_t table_count = field_reader.ReadRequired<uint32_t>();
	uint32_t view_count = field_reader.ReadRequired<uint32_t>();
	uint32_t macro_count = field_reader.ReadRequired<uint32_t>();
	uint32_t table_macro_count = field_reader.ReadRequired<uint32_t>();
	field_reader.Finalize();

	// now read the enums
	for (uint32_t i = 0; i < enum_count; i++) {
		ReadType(context, reader);
	}

	// read the sequences
	for (uint32_t i = 0; i < seq_count; i++) {
		ReadSequence(context, reader);
	}
	// read the table count and recreate the tables
	for (uint32_t i = 0; i < table_count; i++) {
		ReadTable(context, reader);
	}
	// now read the views
	for (uint32_t i = 0; i < view_count; i++) {
		ReadView(context, reader);
	}

	// finally read the macro's
	for (uint32_t i = 0; i < macro_count; i++) {
		ReadMacro(context, reader);
	}

	for (uint32_t i = 0; i < table_macro_count; i++) {
		ReadTableMacro(context, reader);
	}
}

//===--------------------------------------------------------------------===//
// Views
//===--------------------------------------------------------------------===//
void CheckpointManager::WriteView(ViewCatalogEntry &view) {
	view.Serialize(*metadata_writer);
}

void CheckpointManager::ReadView(ClientContext &context, MetaBlockReader &reader) {
	auto info = ViewCatalogEntry::Deserialize(reader);

	auto &catalog = Catalog::GetCatalog(db);
	catalog.CreateView(context, info.get());
}

//===--------------------------------------------------------------------===//
// Sequences
//===--------------------------------------------------------------------===//
void CheckpointManager::WriteSequence(SequenceCatalogEntry &seq) {
	seq.Serialize(*metadata_writer);
}

void CheckpointManager::ReadSequence(ClientContext &context, MetaBlockReader &reader) {
	auto info = SequenceCatalogEntry::Deserialize(reader);

	auto &catalog = Catalog::GetCatalog(db);
	catalog.CreateSequence(context, info.get());
}

//===--------------------------------------------------------------------===//
// Custom Types
//===--------------------------------------------------------------------===//
void CheckpointManager::WriteType(TypeCatalogEntry &table) {
	table.Serialize(*metadata_writer);
}

void CheckpointManager::ReadType(ClientContext &context, MetaBlockReader &reader) {
	auto info = TypeCatalogEntry::Deserialize(reader);

	auto &catalog = Catalog::GetCatalog(db);
	catalog.CreateType(context, info.get());
}

//===--------------------------------------------------------------------===//
// Macro's
//===--------------------------------------------------------------------===//
void CheckpointManager::WriteMacro(ScalarMacroCatalogEntry &macro) {
	macro.Serialize(*metadata_writer);
}

void CheckpointManager::ReadMacro(ClientContext &context, MetaBlockReader &reader) {
	auto info = ScalarMacroCatalogEntry::Deserialize(reader);
	auto &catalog = Catalog::GetCatalog(db);
	catalog.CreateFunction(context, info.get());
}

void CheckpointManager::WriteTableMacro(TableMacroCatalogEntry &macro) {
	macro.Serialize(*metadata_writer);
}

void CheckpointManager::ReadTableMacro(ClientContext &context, MetaBlockReader &reader) {
	auto info = TableMacroCatalogEntry::Deserialize(reader);
	auto &catalog = Catalog::GetCatalog(db);
	catalog.CreateFunction(context, info.get());
}

//===--------------------------------------------------------------------===//
// Table Metadata
//===--------------------------------------------------------------------===//
void CheckpointManager::WriteTable(TableCatalogEntry &table) {
	// write the table meta data
	table.Serialize(*metadata_writer);
	// now we need to write the table data
	TableDataWriter writer(db, *this, table, *tabledata_writer);
	auto pointer = writer.WriteTableData();

	//! write the block pointer for the table info
	metadata_writer->Write<block_id_t>(pointer.block_id);
	metadata_writer->Write<uint64_t>(pointer.offset);
}

void CheckpointManager::ReadTable(ClientContext &context, MetaBlockReader &reader) {
	// deserialize the table meta data
	auto info = TableCatalogEntry::Deserialize(reader);
	// bind the info
	auto binder = Binder::CreateBinder(context);
	auto bound_info = binder->BindCreateTableInfo(move(info));

	// now read the actual table data and place it into the create table info
	auto block_id = reader.Read<block_id_t>();
	auto offset = reader.Read<uint64_t>();
	MetaBlockReader table_data_reader(db, block_id);
	table_data_reader.offset = offset;
	TableDataReader data_reader(table_data_reader, *bound_info);
	data_reader.ReadTableData();

	// finally create the table in the catalog
	auto &catalog = Catalog::GetCatalog(db);
	catalog.CreateTable(context, bound_info.get());
}

//===--------------------------------------------------------------------===//
// Partial Blocks
//===--------------------------------------------------------------------===//
bool CheckpointManager::GetPartialBlock(ColumnSegment *segment, idx_t segment_size, block_id_t &block_id,
                                        uint32_t &offset_in_block, PartialBlock *&partial_block_ptr,
                                        unique_ptr<PartialBlock> &owned_partial_block) {
	auto entry = partially_filled_blocks.lower_bound(segment_size);
	if (entry == partially_filled_blocks.end()) {
		return false;
	}
	// found a partially filled block! fill in the info
	auto partial_block = move(entry->second);
	partial_block_ptr = partial_block.get();
	block_id = partial_block->block_id;
	offset_in_block = Storage::BLOCK_SIZE - entry->first;
	partially_filled_blocks.erase(entry);
	PartialColumnSegment partial_segment;
	partial_segment.segment = segment;
	partial_segment.offset_in_block = offset_in_block;
	partial_block->segments.push_back(partial_segment);

	D_ASSERT(offset_in_block > 0);
	D_ASSERT(ValueIsAligned(offset_in_block));

	// check if the block is STILL partially filled after adding the segment_size
	auto new_size = AlignValue(offset_in_block + segment_size);
	if (new_size <= CheckpointManager::PARTIAL_BLOCK_THRESHOLD) {
		// the block is still partially filled: add it to the partially_filled_blocks list
		auto new_space_left = Storage::BLOCK_SIZE - new_size;
		partially_filled_blocks.insert(make_pair(new_space_left, move(partial_block)));
		// should not write the block yet: perhaps more columns will be added
	} else {
		// we are done with this block after the current write: write it to disk
		owned_partial_block = move(partial_block);
	}
	return true;
}

void CheckpointManager::RegisterPartialBlock(ColumnSegment *segment, idx_t segment_size, block_id_t block_id) {
	D_ASSERT(segment_size <= CheckpointManager::PARTIAL_BLOCK_THRESHOLD);
	auto partial_block = make_unique<PartialBlock>();
	partial_block->block_id = block_id;
	partial_block->block = segment->block;

	PartialColumnSegment partial_segment;
	partial_segment.segment = segment;
	partial_segment.offset_in_block = 0;
	partial_block->segments.push_back(partial_segment);
	auto space_left = Storage::BLOCK_SIZE - AlignValue(segment_size);
	partially_filled_blocks.insert(make_pair(space_left, move(partial_block)));
}

void CheckpointManager::FlushPartialSegments() {
	for (auto &entry : partially_filled_blocks) {
		entry.second->FlushToDisk(db);
	}
}

void PartialBlock::FlushToDisk(DatabaseInstance &db) {
	auto &buffer_manager = BufferManager::GetBufferManager(db);
	auto &block_manager = BlockManager::GetBlockManager(db);

	// the data for the block might already exists in-memory of our block
	// instead of copying the data we alter some metadata so the buffer points to an on-disk block
	block = buffer_manager.ConvertToPersistent(block_manager, block_id, move(block));

	// now set this block as the block for all segments
	for (auto &seg : segments) {
		seg.segment->ConvertToPersistent(block, block_id, seg.offset_in_block);
	}
}

} // namespace duckdb












#include <functional>

namespace duckdb {

// Note that optimizations in scanning only work if this value is equal to STANDARD_VECTOR_SIZE, however we keep them
// separated to prevent the code from break on lower vector sizes
static constexpr const idx_t BITPACKING_WIDTH_GROUP_SIZE = 1024;

struct EmptyBitpackingWriter {
	template <class T>
	static void Operation(T *values, bool *validity, bitpacking_width_t width, idx_t count, void *data_ptr) {
	}
};

template <class T>
struct BitpackingState {
public:
	BitpackingState() : compression_buffer_idx(0), total_size(0), data_ptr(nullptr) {
	}

	T compression_buffer[BITPACKING_WIDTH_GROUP_SIZE];
	bool compression_buffer_validity[BITPACKING_WIDTH_GROUP_SIZE];
	idx_t compression_buffer_idx;
	idx_t total_size;
	void *data_ptr;

public:
	template <class OP>
	void Flush() {
		bitpacking_width_t width = BitpackingPrimitives::MinimumBitWidth<T>(compression_buffer, compression_buffer_idx);
		OP::Operation(compression_buffer, compression_buffer_validity, width, compression_buffer_idx, data_ptr);
		total_size += (BITPACKING_WIDTH_GROUP_SIZE * width) / 8 + sizeof(bitpacking_width_t);
		compression_buffer_idx = 0;
	}

	template <class OP = EmptyBitpackingWriter>
	void Update(T *data, ValidityMask &validity, idx_t idx) {

		if (validity.RowIsValid(idx)) {
			compression_buffer_validity[compression_buffer_idx] = true;
			compression_buffer[compression_buffer_idx++] = data[idx];
		} else {
			// We write zero for easy bitwidth analysis of the compression buffer later
			compression_buffer_validity[compression_buffer_idx] = false;
			compression_buffer[compression_buffer_idx++] = 0;
		}

		if (compression_buffer_idx == BITPACKING_WIDTH_GROUP_SIZE) {
			// Calculate bitpacking width;
			Flush<OP>();
		}
	}
};

//===--------------------------------------------------------------------===//
// Analyze
//===--------------------------------------------------------------------===//
template <class T>
struct BitpackingAnalyzeState : public AnalyzeState {
	BitpackingState<T> state;
};

template <class T>
unique_ptr<AnalyzeState> BitpackingInitAnalyze(ColumnData &col_data, PhysicalType type) {
	return make_unique<BitpackingAnalyzeState<T>>();
}

template <class T>
bool BitpackingAnalyze(AnalyzeState &state, Vector &input, idx_t count) {
	auto &analyze_state = (BitpackingAnalyzeState<T> &)state;
	VectorData vdata;
	input.Orrify(count, vdata);

	auto data = (T *)vdata.data;
	for (idx_t i = 0; i < count; i++) {
		auto idx = vdata.sel->get_index(i);
		analyze_state.state.template Update<EmptyBitpackingWriter>(data, vdata.validity, idx);
	}

	return true;
}

template <class T>
idx_t BitpackingFinalAnalyze(AnalyzeState &state) {
	auto &bitpacking_state = (BitpackingAnalyzeState<T> &)state;
	bitpacking_state.state.template Flush<EmptyBitpackingWriter>();
	return bitpacking_state.state.total_size;
}

//===--------------------------------------------------------------------===//
// Compress
//===--------------------------------------------------------------------===//
template <class T>
struct BitpackingCompressState : public CompressionState {
public:
	explicit BitpackingCompressState(ColumnDataCheckpointer &checkpointer) : checkpointer(checkpointer) {
		auto &db = checkpointer.GetDatabase();
		auto &type = checkpointer.GetType();
		auto &config = DBConfig::GetConfig(db);
		function = config.GetCompressionFunction(CompressionType::COMPRESSION_BITPACKING, type.InternalType());
		CreateEmptySegment(checkpointer.GetRowGroup().start);

		state.data_ptr = (void *)this;
	}

	ColumnDataCheckpointer &checkpointer;
	CompressionFunction *function;
	unique_ptr<ColumnSegment> current_segment;
	unique_ptr<BufferHandle> handle;

	// Ptr to next free spot in segment;
	data_ptr_t data_ptr;
	// Ptr to next free spot for storing bitwidths (growing downwards).
	data_ptr_t width_ptr;

	BitpackingState<T> state;

public:
	struct BitpackingWriter {
		template <class VALUE_TYPE>
		static void Operation(VALUE_TYPE *values, bool *validity, bitpacking_width_t width, idx_t count,
		                      void *data_ptr) {
			auto state = (BitpackingCompressState<T> *)data_ptr;

			if (state->RemainingSize() < (width * BITPACKING_WIDTH_GROUP_SIZE) / 8 + sizeof(bitpacking_width_t)) {
				// Segment is full
				auto row_start = state->current_segment->start + state->current_segment->count;
				state->FlushSegment();
				state->CreateEmptySegment(row_start);
			}

			for (idx_t i = 0; i < count; i++) {
				if (validity[i]) {
					NumericStatistics::Update<T>(state->current_segment->stats, values[i]);
				}
			}

			state->WriteValues(values, width, count);
		}
	};

	// Space remaining between the width_ptr growing down and data ptr growing up
	idx_t RemainingSize() {
		return width_ptr - data_ptr;
	}

	void CreateEmptySegment(idx_t row_start) {
		auto &db = checkpointer.GetDatabase();
		auto &type = checkpointer.GetType();
		auto compressed_segment = ColumnSegment::CreateTransientSegment(db, type, row_start);
		compressed_segment->function = function;
		current_segment = move(compressed_segment);
		auto &buffer_manager = BufferManager::GetBufferManager(db);
		handle = buffer_manager.Pin(current_segment->block);

		data_ptr = handle->Ptr() + current_segment->GetBlockOffset() + BitpackingPrimitives::BITPACKING_HEADER_SIZE;
		width_ptr =
		    handle->Ptr() + current_segment->GetBlockOffset() + Storage::BLOCK_SIZE - sizeof(bitpacking_width_t);
	}

	void Append(VectorData &vdata, idx_t count) {
		// TODO Optimization: avoid use of compression buffer if we can compress straight to result vector
		auto data = (T *)vdata.data;

		for (idx_t i = 0; i < count; i++) {
			auto idx = vdata.sel->get_index(i);
			state.template Update<BitpackingCompressState<T>::BitpackingWriter>(data, vdata.validity, idx);
		}
	}

	void WriteValues(T *values, bitpacking_width_t width, idx_t count) {
		// TODO we can optimize this by stopping early if count < BITPACKING_WIDTH_GROUP_SIZE
		BitpackingPrimitives::PackBuffer<T, false>(data_ptr, values, count, width);
		data_ptr += (BITPACKING_WIDTH_GROUP_SIZE * width) / 8;

		Store<bitpacking_width_t>(width, width_ptr);
		width_ptr -= sizeof(bitpacking_width_t);

		current_segment->count += count;
	}

	void FlushSegment() {
		auto &state = checkpointer.GetCheckpointState();

		// Compact the segment by moving the widths next to the data.
		idx_t minimal_widths_offset = AlignValue(data_ptr - handle->node->buffer);
		idx_t widths_size = handle->node->buffer + Storage::BLOCK_SIZE - width_ptr - 1;
		idx_t total_segment_size = minimal_widths_offset + widths_size;
		memmove(handle->node->buffer + minimal_widths_offset, width_ptr + 1, widths_size);

		// Store the offset of the first width (which is at the highest address).
		Store<idx_t>(minimal_widths_offset + widths_size - 1, handle->node->buffer);
		handle.reset();

		state.FlushSegment(move(current_segment), total_segment_size);
	}

	void Finalize() {
		state.template Flush<BitpackingCompressState<T>::BitpackingWriter>();
		FlushSegment();
		current_segment.reset();
	}
};

template <class T>
unique_ptr<CompressionState> BitpackingInitCompression(ColumnDataCheckpointer &checkpointer,
                                                       unique_ptr<AnalyzeState> state) {
	return make_unique<BitpackingCompressState<T>>(checkpointer);
}

template <class T>
void BitpackingCompress(CompressionState &state_p, Vector &scan_vector, idx_t count) {
	auto &state = (BitpackingCompressState<T> &)state_p;
	VectorData vdata;
	scan_vector.Orrify(count, vdata);
	state.Append(vdata, count);
}

template <class T>
void BitpackingFinalizeCompress(CompressionState &state_p) {
	auto &state = (BitpackingCompressState<T> &)state_p;
	state.Finalize();
}

//===--------------------------------------------------------------------===//
// Scan
//===--------------------------------------------------------------------===//
template <class T>
struct BitpackingScanState : public SegmentScanState {
public:
	explicit BitpackingScanState(ColumnSegment &segment) {
		auto &buffer_manager = BufferManager::GetBufferManager(segment.db);
		handle = buffer_manager.Pin(segment.block);

		current_width_group_ptr =
		    handle->node->buffer + segment.GetBlockOffset() + BitpackingPrimitives::BITPACKING_HEADER_SIZE;

		// load offset to bitpacking widths pointer
		auto bitpacking_widths_offset = Load<idx_t>(handle->node->buffer + segment.GetBlockOffset());
		bitpacking_width_ptr = handle->node->buffer + segment.GetBlockOffset() + bitpacking_widths_offset;

		// load the bitwidth of the first vector
		LoadCurrentBitWidth();
	}

	unique_ptr<BufferHandle> handle;

	void (*decompress_function)(data_ptr_t, data_ptr_t, bitpacking_width_t, bool skip_sign_extension);
	T decompression_buffer[BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE];

	idx_t position_in_group = 0;
	data_ptr_t current_width_group_ptr;
	data_ptr_t bitpacking_width_ptr;
	bitpacking_width_t current_width;

public:
	void LoadCurrentBitWidth() {
		D_ASSERT(bitpacking_width_ptr > handle->node->buffer &&
		         bitpacking_width_ptr < handle->node->buffer + Storage::BLOCK_SIZE);
		current_width = Load<bitpacking_width_t>(bitpacking_width_ptr);
		LoadDecompressFunction();
	}

	void Skip(ColumnSegment &segment, idx_t skip_count) {
		while (skip_count > 0) {
			if (position_in_group + skip_count < BITPACKING_WIDTH_GROUP_SIZE) {
				// We're not leaving this bitpacking group, we can perform all skips.
				position_in_group += skip_count;
				break;
			} else {
				// The skip crosses the current bitpacking group, we skip the remainder of this group.
				auto skipping = BITPACKING_WIDTH_GROUP_SIZE - position_in_group;
				position_in_group = 0;
				current_width_group_ptr += (current_width * BITPACKING_WIDTH_GROUP_SIZE) / 8;

				// Update width pointer and load new width
				bitpacking_width_ptr -= sizeof(bitpacking_width_t);
				LoadCurrentBitWidth();

				skip_count -= skipping;
			}
		}
	}

	void LoadDecompressFunction() {
		decompress_function = &BitpackingPrimitives::UnPackBlock<T>;
	}
};

template <class T>
unique_ptr<SegmentScanState> BitpackingInitScan(ColumnSegment &segment) {
	auto result = make_unique<BitpackingScanState<T>>(segment);
	return move(result);
}

//===--------------------------------------------------------------------===//
// Scan base data
//===--------------------------------------------------------------------===//
template <class T>
void BitpackingScanPartial(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result,
                           idx_t result_offset) {
	auto &scan_state = (BitpackingScanState<T> &)*state.scan_state;

	T *result_data = FlatVector::GetData<T>(result);
	result.SetVectorType(VectorType::FLAT_VECTOR);

	// Fast path for when no compression was used, we can do a single memcopy
	if (STANDARD_VECTOR_SIZE == BITPACKING_WIDTH_GROUP_SIZE) {
		if (scan_state.current_width == sizeof(T) * 8 && scan_count <= BITPACKING_WIDTH_GROUP_SIZE &&
		    scan_state.position_in_group == 0) {

			memcpy(result_data + result_offset, scan_state.current_width_group_ptr, scan_count * sizeof(T));
			scan_state.current_width_group_ptr += scan_count * sizeof(T);
			scan_state.bitpacking_width_ptr -= sizeof(bitpacking_width_t);
			scan_state.LoadCurrentBitWidth();
			return;
		}
	}

	// Determine if we can skip sign extension during compression
	auto &nstats = (NumericStatistics &)*segment.stats.statistics;
	bool skip_sign_extend = std::is_signed<T>::value && nstats.min >= 0;

	idx_t scanned = 0;

	while (scanned < scan_count) {
		// Exhausted this width group, move pointers to next group and load bitwidth for next group.
		if (scan_state.position_in_group >= BITPACKING_WIDTH_GROUP_SIZE) {
			scan_state.position_in_group = 0;
			scan_state.bitpacking_width_ptr -= sizeof(bitpacking_width_t);
			scan_state.current_width_group_ptr += (scan_state.current_width * BITPACKING_WIDTH_GROUP_SIZE) / 8;
			scan_state.LoadCurrentBitWidth();
		}

		idx_t offset_in_compression_group =
		    scan_state.position_in_group % BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE;

		idx_t to_scan = MinValue<idx_t>(scan_count - scanned, BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE -
		                                                          offset_in_compression_group);

		// Calculate start of compression algorithm group
		data_ptr_t current_position_ptr =
		    scan_state.current_width_group_ptr + scan_state.position_in_group * scan_state.current_width / 8;
		data_ptr_t decompression_group_start_pointer =
		    current_position_ptr - offset_in_compression_group * scan_state.current_width / 8;

		T *current_result_ptr = result_data + result_offset + scanned;

		if (to_scan == BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE && offset_in_compression_group == 0) {
			// Decompress directly into result vector
			scan_state.decompress_function((data_ptr_t)current_result_ptr, decompression_group_start_pointer,
			                               scan_state.current_width, skip_sign_extend);
		} else {
			// Decompress compression algorithm to buffer
			scan_state.decompress_function((data_ptr_t)scan_state.decompression_buffer,
			                               decompression_group_start_pointer, scan_state.current_width,
			                               skip_sign_extend);

			memcpy(current_result_ptr, scan_state.decompression_buffer + offset_in_compression_group,
			       to_scan * sizeof(T));
		}

		scanned += to_scan;
		scan_state.position_in_group += to_scan;
	}
}

template <class T>
void BitpackingScan(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result) {
	BitpackingScanPartial<T>(segment, state, scan_count, result, 0);
}

//===--------------------------------------------------------------------===//
// Fetch
//===--------------------------------------------------------------------===//
template <class T>
void BitpackingFetchRow(ColumnSegment &segment, ColumnFetchState &state, row_t row_id, Vector &result,
                        idx_t result_idx) {
	BitpackingScanState<T> scan_state(segment);
	scan_state.Skip(segment, row_id);
	auto result_data = FlatVector::GetData<T>(result);
	T *current_result_ptr = result_data + result_idx;

	// TODO clean up, is reused in partialscan
	idx_t offset_in_compression_group =
	    scan_state.position_in_group % BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE;

	data_ptr_t decompression_group_start_pointer =
	    scan_state.current_width_group_ptr +
	    (scan_state.position_in_group - offset_in_compression_group) * scan_state.current_width / 8;

	auto &nstats = (NumericStatistics &)*segment.stats.statistics;
	bool skip_sign_extend = std::is_signed<T>::value && nstats.min >= 0;

	scan_state.decompress_function((data_ptr_t)scan_state.decompression_buffer, decompression_group_start_pointer,
	                               scan_state.current_width, skip_sign_extend);

	*current_result_ptr = *(T *)(scan_state.decompression_buffer + offset_in_compression_group);
}
template <class T>
void BitpackingSkip(ColumnSegment &segment, ColumnScanState &state, idx_t skip_count) {
	auto &scan_state = (BitpackingScanState<T> &)*state.scan_state;
	scan_state.Skip(segment, skip_count);
}

//===--------------------------------------------------------------------===//
// Get Function
//===--------------------------------------------------------------------===//
template <class T>
CompressionFunction GetBitpackingFunction(PhysicalType data_type) {
	return CompressionFunction(CompressionType::COMPRESSION_BITPACKING, data_type, BitpackingInitAnalyze<T>,
	                           BitpackingAnalyze<T>, BitpackingFinalAnalyze<T>, BitpackingInitCompression<T>,
	                           BitpackingCompress<T>, BitpackingFinalizeCompress<T>, BitpackingInitScan<T>,
	                           BitpackingScan<T>, BitpackingScanPartial<T>, BitpackingFetchRow<T>, BitpackingSkip<T>);
}

CompressionFunction BitpackingFun::GetFunction(PhysicalType type) {
	switch (type) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		return GetBitpackingFunction<int8_t>(type);
	case PhysicalType::INT16:
		return GetBitpackingFunction<int16_t>(type);
	case PhysicalType::INT32:
		return GetBitpackingFunction<int32_t>(type);
	case PhysicalType::INT64:
		return GetBitpackingFunction<int64_t>(type);
	case PhysicalType::UINT8:
		return GetBitpackingFunction<uint8_t>(type);
	case PhysicalType::UINT16:
		return GetBitpackingFunction<uint16_t>(type);
	case PhysicalType::UINT32:
		return GetBitpackingFunction<uint32_t>(type);
	case PhysicalType::UINT64:
		return GetBitpackingFunction<uint64_t>(type);
	default:
		throw InternalException("Unsupported type for Bitpacking");
	}
}

bool BitpackingFun::TypeIsSupported(PhysicalType type) {
	switch (type) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
	case PhysicalType::INT16:
	case PhysicalType::INT32:
	case PhysicalType::INT64:
	case PhysicalType::UINT8:
	case PhysicalType::UINT16:
	case PhysicalType::UINT32:
	case PhysicalType::UINT64:
		return true;
	default:
		return false;
	}
}

} // namespace duckdb










namespace duckdb {

struct StringHash {
	std::size_t operator()(const string &k) const {
		return Hash(k.c_str(), k.size());
	}
};

// Abstract class for keeping compression state either for compression or size analysis
class DictionaryCompressionState : public CompressionState {
public:
	bool UpdateState(Vector &scan_vector, idx_t count) {
		VectorData vdata;
		scan_vector.Orrify(count, vdata);
		auto data = (string_t *)vdata.data;
		Verify();

		for (idx_t i = 0; i < count; i++) {
			auto idx = vdata.sel->get_index(i);
			size_t string_size = 0;
			bool new_string = false;
			auto row_is_valid = vdata.validity.RowIsValid(idx);

			if (row_is_valid) {
				string_size = data[idx].GetSize();
				if (string_size >= StringUncompressed::STRING_BLOCK_LIMIT) {
					// Big strings not implemented for dictionary compression
					return false;
				}
				new_string = !LookupString(data[idx]);
			}

			bool fits = HasEnoughSpace(new_string, string_size);
			if (!fits) {
				Flush();
				new_string = true;
				D_ASSERT(HasEnoughSpace(new_string, string_size));
			}

			if (!row_is_valid) {
				AddNull();
			} else if (new_string) {
				AddNewString(data[idx]);
			} else {
				AddLastLookup();
			}

			Verify();
		}

		return true;
	}

protected:
	// Should verify the State
	virtual void Verify() = 0;
	// Performs a lookup of str, storing the result internally
	virtual bool LookupString(string_t str) = 0;
	// Add the most recently looked up str to compression state
	virtual void AddLastLookup() = 0;
	// Add string to the state that is known to not be seen yet
	virtual void AddNewString(string_t str) = 0;
	// Add a null value to the compression state
	virtual void AddNull() = 0;
	// Check if we have enough space to add a string
	virtual bool HasEnoughSpace(bool new_string, size_t string_size) = 0;
	// Flush the segment to disk if compressing or reset the counters if analyzing
	virtual void Flush(bool final = false) = 0;
};

typedef struct {
	uint32_t dict_size;
	uint32_t dict_end;
	uint32_t index_buffer_offset;
	uint32_t index_buffer_count;
	uint32_t bitpacking_width;
} dictionary_compression_header_t;

struct DictionaryCompressionStorage {
	static constexpr float MINIMUM_COMPRESSION_RATIO = 1.2;
	static constexpr uint16_t DICTIONARY_HEADER_SIZE = sizeof(dictionary_compression_header_t);
	static constexpr size_t COMPACTION_FLUSH_LIMIT = (size_t)Storage::BLOCK_SIZE / 5 * 4;

	static unique_ptr<AnalyzeState> StringInitAnalyze(ColumnData &col_data, PhysicalType type);
	static bool StringAnalyze(AnalyzeState &state_p, Vector &input, idx_t count);
	static idx_t StringFinalAnalyze(AnalyzeState &state_p);

	static unique_ptr<CompressionState> InitCompression(ColumnDataCheckpointer &checkpointer,
	                                                    unique_ptr<AnalyzeState> state);
	static void Compress(CompressionState &state_p, Vector &scan_vector, idx_t count);
	static void FinalizeCompress(CompressionState &state_p);

	static unique_ptr<SegmentScanState> StringInitScan(ColumnSegment &segment);
	template <bool ALLOW_DICT_VECTORS>
	static void StringScanPartial(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result,
	                              idx_t result_offset);
	static void StringScan(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result);
	static void StringFetchRow(ColumnSegment &segment, ColumnFetchState &state, row_t row_id, Vector &result,
	                           idx_t result_idx);

	static bool HasEnoughSpace(idx_t current_count, idx_t index_count, idx_t dict_size,
	                           bitpacking_width_t packing_width);
	static idx_t RequiredSpace(idx_t current_count, idx_t index_count, idx_t dict_size,
	                           bitpacking_width_t packing_width);

	static StringDictionaryContainer GetDictionary(ColumnSegment &segment, BufferHandle &handle);
	static void SetDictionary(ColumnSegment &segment, BufferHandle &handle, StringDictionaryContainer container);
	static string_t FetchStringFromDict(ColumnSegment &segment, StringDictionaryContainer dict, data_ptr_t baseptr,
	                                    int32_t dict_offset, uint16_t string_len);
	static uint16_t GetStringLength(uint32_t *index_buffer_ptr, sel_t index);
};

// Dictionary compression uses a combination of bitpacking and a dictionary to compress string segments. The data is
// stored across three buffers: the index buffer, the selection buffer and the dictionary. Firstly the Index buffer
// contains the offsets into the dictionary which are also used to determine the string lengths. Each value in the
// dictionary gets a single unique index in the index buffer. Secondly, the selection buffer maps the tuples to an index
// in the index buffer. The selection buffer is compressed with bitpacking. Finally, the dictionary contains simply all
// the unique strings without lenghts or null termination as we can deduce the lengths from the index buffer. The
// addition of the selection buffer is done for two reasons: firstly, to allow the scan to emit dictionary vectors by
// scanning the whole dictionary at once and then scanning the selection buffer for each emitted vector. Secondly, it
// allows for efficient bitpacking compression as the selection values should remain relatively small.
struct DictionaryCompressionCompressState : public DictionaryCompressionState {
	explicit DictionaryCompressionCompressState(ColumnDataCheckpointer &checkpointer) : checkpointer(checkpointer) {
		auto &db = checkpointer.GetDatabase();
		auto &config = DBConfig::GetConfig(db);
		function = config.GetCompressionFunction(CompressionType::COMPRESSION_DICTIONARY, PhysicalType::VARCHAR);
		CreateEmptySegment(checkpointer.GetRowGroup().start);
	}

	void CreateEmptySegment(idx_t row_start) {
		auto &db = checkpointer.GetDatabase();
		auto &type = checkpointer.GetType();
		auto compressed_segment = ColumnSegment::CreateTransientSegment(db, type, row_start);
		current_segment = move(compressed_segment);

		current_segment->function = function;

		// Reset the buffers and string map
		current_string_map.clear();
		index_buffer.clear();
		index_buffer.push_back(0); // Reserve index 0 for null strings
		selection_buffer.clear();

		current_width = 0;
		next_width = 0;

		// Reset the pointers into the current segment
		auto &buffer_manager = BufferManager::GetBufferManager(current_segment->db);
		current_handle = buffer_manager.Pin(current_segment->block);
		current_dictionary = DictionaryCompressionStorage::GetDictionary(*current_segment, *current_handle);
		current_end_ptr = current_handle->node->buffer + current_dictionary.end;
	}

	ColumnDataCheckpointer &checkpointer;
	CompressionFunction *function;

	// State regarding current segment
	unique_ptr<ColumnSegment> current_segment;
	unique_ptr<BufferHandle> current_handle;
	StringDictionaryContainer current_dictionary;
	data_ptr_t current_end_ptr;

	// Buffers and map for current segment
	std::unordered_map<string, uint32_t, StringHash> current_string_map;
	std::vector<uint32_t> index_buffer;
	std::vector<uint32_t> selection_buffer;

	bitpacking_width_t current_width = 0;
	bitpacking_width_t next_width = 0;

	// Result of latest LookupString call
	uint32_t latest_lookup_result;

	void Verify() override {
		current_dictionary.Verify();
		D_ASSERT(current_segment->count == selection_buffer.size());
		D_ASSERT(DictionaryCompressionStorage::HasEnoughSpace(current_segment->count.load(), index_buffer.size(),
		                                                      current_dictionary.size, current_width));
		D_ASSERT(current_dictionary.end == Storage::BLOCK_SIZE);
		D_ASSERT(index_buffer.size() == current_string_map.size() + 1); // +1 is for null value
	}

	bool LookupString(string_t str) override {
		auto search = current_string_map.find(str.GetString());
		auto has_result = search != current_string_map.end();

		if (has_result) {
			latest_lookup_result = search->second;
		}
		return has_result;
	}

	void AddNewString(string_t str) override {
		UncompressedStringStorage::UpdateStringStats(current_segment->stats, str);

		// Copy string to dict
		current_dictionary.size += str.GetSize();
		auto dict_pos = current_end_ptr - current_dictionary.size;
		memcpy(dict_pos, str.GetDataUnsafe(), str.GetSize());
		current_dictionary.Verify();
		D_ASSERT(current_dictionary.end == Storage::BLOCK_SIZE);

		// Update buffers and map
		index_buffer.push_back(current_dictionary.size);
		selection_buffer.push_back(index_buffer.size() - 1);
		current_string_map.insert({str.GetString(), index_buffer.size() - 1});
		DictionaryCompressionStorage::SetDictionary(*current_segment, *current_handle, current_dictionary);

		current_width = next_width;
		current_segment->count++;
	}

	void AddNull() override {
		selection_buffer.push_back(0);
		current_segment->count++;
	}

	void AddLastLookup() override {
		selection_buffer.push_back(latest_lookup_result);
		current_segment->count++;
	}

	bool HasEnoughSpace(bool new_string, size_t string_size) override {
		if (new_string) {
			next_width = BitpackingPrimitives::MinimumBitWidth(index_buffer.size() - 1 + new_string);
			return DictionaryCompressionStorage::HasEnoughSpace(current_segment->count.load() + 1,
			                                                    index_buffer.size() + 1,
			                                                    current_dictionary.size + string_size, next_width);
		} else {
			return DictionaryCompressionStorage::HasEnoughSpace(current_segment->count.load() + 1, index_buffer.size(),
			                                                    current_dictionary.size, current_width);
		}
	}

	void Flush(bool final = false) override {
		auto next_start = current_segment->start + current_segment->count;

		auto segment_size = Finalize();
		auto &state = checkpointer.GetCheckpointState();
		state.FlushSegment(move(current_segment), segment_size);

		if (!final) {
			CreateEmptySegment(next_start);
		}
	}

	idx_t Finalize() {
		auto &buffer_manager = BufferManager::GetBufferManager(current_segment->db);
		auto handle = buffer_manager.Pin(current_segment->block);
		D_ASSERT(current_dictionary.end == Storage::BLOCK_SIZE);

		// calculate sizes
		auto compressed_selection_buffer_size =
		    BitpackingPrimitives::GetRequiredSize<sel_t>(current_segment->count, current_width);
		auto index_buffer_size = index_buffer.size() * sizeof(uint32_t);
		auto total_size = DictionaryCompressionStorage::DICTIONARY_HEADER_SIZE + compressed_selection_buffer_size +
		                  index_buffer_size + current_dictionary.size;

		// calculate ptr and offsets
		auto base_ptr = handle->node->buffer;
		auto header_ptr = (dictionary_compression_header_t *)base_ptr;
		auto compressed_selection_buffer_offset = DictionaryCompressionStorage::DICTIONARY_HEADER_SIZE;
		auto index_buffer_offset = compressed_selection_buffer_offset + compressed_selection_buffer_size;

		// Write compressed selection buffer
		BitpackingPrimitives::PackBuffer<sel_t, false>(base_ptr + compressed_selection_buffer_offset,
		                                               (sel_t *)(selection_buffer.data()), current_segment->count,
		                                               current_width);

		// Write the index buffer
		memcpy(base_ptr + index_buffer_offset, index_buffer.data(), index_buffer_size);

		// Store sizes and offsets in segment header
		Store<uint32_t>(index_buffer_offset, (data_ptr_t)&header_ptr->index_buffer_offset);
		Store<uint32_t>(index_buffer.size(), (data_ptr_t)&header_ptr->index_buffer_count);
		Store<uint32_t>((uint32_t)current_width, (data_ptr_t)&header_ptr->bitpacking_width);

		D_ASSERT(current_width == BitpackingPrimitives::MinimumBitWidth(index_buffer.size() - 1));
		D_ASSERT(DictionaryCompressionStorage::HasEnoughSpace(current_segment->count, index_buffer.size(),
		                                                      current_dictionary.size, current_width));
		D_ASSERT((uint64_t)*max_element(std::begin(selection_buffer), std::end(selection_buffer)) ==
		         index_buffer.size() - 1);

		if (total_size >= DictionaryCompressionStorage::COMPACTION_FLUSH_LIMIT) {
			// the block is full enough, don't bother moving around the dictionary
			return Storage::BLOCK_SIZE;
		}
		// the block has space left: figure out how much space we can save
		auto move_amount = Storage::BLOCK_SIZE - total_size;
		// move the dictionary so it lines up exactly with the offsets
		auto new_dictionary_offset = index_buffer_offset + index_buffer_size;
		memmove(base_ptr + new_dictionary_offset, base_ptr + current_dictionary.end - current_dictionary.size,
		        current_dictionary.size);
		current_dictionary.end -= move_amount;
		D_ASSERT(current_dictionary.end == total_size);
		// write the new dictionary (with the updated "end")
		DictionaryCompressionStorage::SetDictionary(*current_segment, *handle, current_dictionary);
		return total_size;
	}
};

//===--------------------------------------------------------------------===//
// Analyze
//===--------------------------------------------------------------------===//
struct DictionaryCompressionAnalyzeState : public AnalyzeState, DictionaryCompressionState {
	DictionaryCompressionAnalyzeState()
	    : segment_count(0), current_tuple_count(0), current_unique_count(0), current_dict_size(0), current_width(0),
	      next_width(0) {
	}

	size_t segment_count;
	idx_t current_tuple_count;
	idx_t current_unique_count;
	size_t current_dict_size;
	std::unordered_set<string, StringHash> current_set;
	bitpacking_width_t current_width;
	bitpacking_width_t next_width;

	bool LookupString(string_t str) override {
		return current_set.count(str.GetString());
	}

	void AddNewString(string_t str) override {
		current_tuple_count++;
		current_unique_count++;
		current_dict_size += str.GetSize();
		current_set.insert(str.GetString());
		current_width = next_width;
	}

	void AddLastLookup() override {
		current_tuple_count++;
	}

	void AddNull() override {
		current_tuple_count++;
	}

	bool HasEnoughSpace(bool new_string, size_t string_size) override {
		if (new_string) {
			next_width =
			    BitpackingPrimitives::MinimumBitWidth(current_unique_count + 2); // 1 for null, one for new string
			return DictionaryCompressionStorage::HasEnoughSpace(current_tuple_count + 1, current_unique_count + 1,
			                                                    current_dict_size + string_size, next_width);
		} else {
			return DictionaryCompressionStorage::HasEnoughSpace(current_tuple_count + 1, current_unique_count,
			                                                    current_dict_size, current_width);
		}
	}

	void Flush(bool final = false) override {
		segment_count++;
		current_tuple_count = 0;
		current_unique_count = 0;
		current_dict_size = 0;
		current_set.clear();
	}
	void Verify() override {};
};

unique_ptr<AnalyzeState> DictionaryCompressionStorage::StringInitAnalyze(ColumnData &col_data, PhysicalType type) {
	return make_unique<DictionaryCompressionAnalyzeState>();
}

bool DictionaryCompressionStorage::StringAnalyze(AnalyzeState &state_p, Vector &input, idx_t count) {
	auto &state = (DictionaryCompressionAnalyzeState &)state_p;
	return state.UpdateState(input, count);
}

idx_t DictionaryCompressionStorage::StringFinalAnalyze(AnalyzeState &state_p) {
	auto &state = (DictionaryCompressionAnalyzeState &)state_p;

	auto width = BitpackingPrimitives::MinimumBitWidth(state.current_unique_count + 1);
	auto req_space =
	    RequiredSpace(state.current_tuple_count, state.current_unique_count, state.current_dict_size, width);

	return MINIMUM_COMPRESSION_RATIO * (state.segment_count * Storage::BLOCK_SIZE + req_space);
}

//===--------------------------------------------------------------------===//
// Compress
//===--------------------------------------------------------------------===//
unique_ptr<CompressionState> DictionaryCompressionStorage::InitCompression(ColumnDataCheckpointer &checkpointer,
                                                                           unique_ptr<AnalyzeState> state) {
	return make_unique<DictionaryCompressionCompressState>(checkpointer);
}

void DictionaryCompressionStorage::Compress(CompressionState &state_p, Vector &scan_vector, idx_t count) {
	auto &state = (DictionaryCompressionCompressState &)state_p;
	state.UpdateState(scan_vector, count);
}

void DictionaryCompressionStorage::FinalizeCompress(CompressionState &state_p) {
	auto &state = (DictionaryCompressionCompressState &)state_p;
	state.Flush(true);
}

//===--------------------------------------------------------------------===//
// Scan
//===--------------------------------------------------------------------===//
struct CompressedStringScanState : public StringScanState {
	unique_ptr<BufferHandle> handle;
	buffer_ptr<Vector> dictionary;
	bitpacking_width_t current_width;
	buffer_ptr<SelectionVector> sel_vec;
	idx_t sel_vec_size = 0;
};

unique_ptr<SegmentScanState> DictionaryCompressionStorage::StringInitScan(ColumnSegment &segment) {
	auto state = make_unique<CompressedStringScanState>();
	auto &buffer_manager = BufferManager::GetBufferManager(segment.db);
	state->handle = buffer_manager.Pin(segment.block);

	auto baseptr = state->handle->node->buffer + segment.GetBlockOffset();

	// Load header values
	auto dict = DictionaryCompressionStorage::GetDictionary(segment, *(state->handle));
	auto header_ptr = (dictionary_compression_header_t *)baseptr;
	auto index_buffer_offset = Load<uint32_t>((data_ptr_t)&header_ptr->index_buffer_offset);
	auto index_buffer_count = Load<uint32_t>((data_ptr_t)&header_ptr->index_buffer_count);
	state->current_width = (bitpacking_width_t)(Load<uint32_t>((data_ptr_t)&header_ptr->bitpacking_width));

	auto index_buffer_ptr = (uint32_t *)(baseptr + index_buffer_offset);

	state->dictionary = make_buffer<Vector>(segment.type, index_buffer_count);
	auto dict_child_data = FlatVector::GetData<string_t>(*(state->dictionary));

	for (uint32_t i = 0; i < index_buffer_count; i++) {
		// NOTE: the passing of dict_child_vector, will not be used, its for big strings
		uint16_t str_len = GetStringLength(index_buffer_ptr, i);
		dict_child_data[i] = FetchStringFromDict(segment, dict, baseptr, index_buffer_ptr[i], str_len);
	}

	return move(state);
}

//===--------------------------------------------------------------------===//
// Scan base data
//===--------------------------------------------------------------------===//
template <bool ALLOW_DICT_VECTORS>
void DictionaryCompressionStorage::StringScanPartial(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count,
                                                     Vector &result, idx_t result_offset) {
	// clear any previously locked buffers and get the primary buffer handle
	auto &scan_state = (CompressedStringScanState &)*state.scan_state;
	auto start = segment.GetRelativeIndex(state.row_index);

	auto baseptr = scan_state.handle->node->buffer + segment.GetBlockOffset();
	auto dict = DictionaryCompressionStorage::GetDictionary(segment, *scan_state.handle);

	auto header_ptr = (dictionary_compression_header_t *)baseptr;
	auto index_buffer_offset = Load<uint32_t>((data_ptr_t)&header_ptr->index_buffer_offset);
	auto index_buffer_ptr = (uint32_t *)(baseptr + index_buffer_offset);

	auto base_data = (data_ptr_t)(baseptr + DICTIONARY_HEADER_SIZE);
	auto result_data = FlatVector::GetData<string_t>(result);

	if (!ALLOW_DICT_VECTORS || scan_count != STANDARD_VECTOR_SIZE ||
	    start % BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE != 0) {
		// Emit regular vector

		// Handling non-bitpacking-group-aligned start values;
		idx_t start_offset = start % BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE;

		// We will scan in blocks of BITPACKING_ALGORITHM_GROUP_SIZE, so we may scan some extra values.
		idx_t decompress_count = BitpackingPrimitives::RoundUpToAlgorithmGroupSize(scan_count + start_offset);

		// Create a decompression buffer of sufficient size if we don't already have one.
		if (!scan_state.sel_vec || scan_state.sel_vec_size < decompress_count) {
			scan_state.sel_vec_size = decompress_count;
			scan_state.sel_vec = make_buffer<SelectionVector>(decompress_count);
		}

		data_ptr_t src = &base_data[((start - start_offset) * scan_state.current_width) / 8];
		sel_t *sel_vec_ptr = scan_state.sel_vec->data();

		BitpackingPrimitives::UnPackBuffer<sel_t>((data_ptr_t)sel_vec_ptr, src, decompress_count,
		                                          scan_state.current_width);

		for (idx_t i = 0; i < scan_count; i++) {
			// Lookup dict offset in index buffer
			auto string_number = scan_state.sel_vec->get_index(i + start_offset);
			auto dict_offset = index_buffer_ptr[string_number];
			uint16_t str_len = GetStringLength(index_buffer_ptr, string_number);
			result_data[result_offset + i] = FetchStringFromDict(segment, dict, baseptr, dict_offset, str_len);
		}

	} else {
		D_ASSERT(start % BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE == 0);
		D_ASSERT(scan_count == STANDARD_VECTOR_SIZE);
		D_ASSERT(result_offset == 0);

		idx_t decompress_count = BitpackingPrimitives::RoundUpToAlgorithmGroupSize(scan_count);

		// Create a selection vector of sufficient size if we don't already have one.
		if (!scan_state.sel_vec || scan_state.sel_vec_size < decompress_count) {
			scan_state.sel_vec_size = decompress_count;
			scan_state.sel_vec = make_buffer<SelectionVector>(decompress_count);
		}

		// Scanning 1024 values, emitting a dict vector
		data_ptr_t dst = (data_ptr_t)(scan_state.sel_vec->data());
		data_ptr_t src = (data_ptr_t)&base_data[(start * scan_state.current_width) / 8];

		BitpackingPrimitives::UnPackBuffer<sel_t>(dst, src, scan_count, scan_state.current_width);

		result.Slice(*(scan_state.dictionary), *scan_state.sel_vec, scan_count);
	}
}

void DictionaryCompressionStorage::StringScan(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count,
                                              Vector &result) {
	StringScanPartial<true>(segment, state, scan_count, result, 0);
}

//===--------------------------------------------------------------------===//
// Fetch
//===--------------------------------------------------------------------===//
void DictionaryCompressionStorage::StringFetchRow(ColumnSegment &segment, ColumnFetchState &state, row_t row_id,
                                                  Vector &result, idx_t result_idx) {
	// fetch a single row from the string segment
	// first pin the main buffer if it is not already pinned
	auto primary_id = segment.block->BlockId();

	BufferHandle *handle_ptr;
	auto entry = state.handles.find(primary_id);
	if (entry == state.handles.end()) {
		// not pinned yet: pin it
		auto &buffer_manager = BufferManager::GetBufferManager(segment.db);
		auto handle = buffer_manager.Pin(segment.block);
		handle_ptr = handle.get();
		state.handles[primary_id] = move(handle);
	} else {
		// already pinned: use the pinned handle
		handle_ptr = entry->second.get();
	}

	auto baseptr = handle_ptr->node->buffer + segment.GetBlockOffset();
	auto header_ptr = (dictionary_compression_header_t *)baseptr;
	auto dict = DictionaryCompressionStorage::GetDictionary(segment, *handle_ptr);
	auto index_buffer_offset = Load<uint32_t>((data_ptr_t)&header_ptr->index_buffer_offset);
	auto width = (bitpacking_width_t)(Load<uint32_t>((data_ptr_t)&header_ptr->bitpacking_width));
	auto index_buffer_ptr = (uint32_t *)(baseptr + index_buffer_offset);
	auto base_data = (data_ptr_t)(baseptr + DICTIONARY_HEADER_SIZE);
	auto result_data = FlatVector::GetData<string_t>(result);

	// Handling non-bitpacking-group-aligned start values;
	idx_t start_offset = row_id % BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE;

	// Decompress part of selection buffer we need for this value.
	sel_t decompression_buffer[BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE];
	data_ptr_t src = (data_ptr_t)&base_data[((row_id - start_offset) * width) / 8];
	BitpackingPrimitives::UnPackBuffer<sel_t>((data_ptr_t)decompression_buffer, src,
	                                          BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE, width);

	auto selection_value = decompression_buffer[start_offset];
	auto dict_offset = index_buffer_ptr[selection_value];
	uint16_t str_len = GetStringLength(index_buffer_ptr, selection_value);

	result_data[result_idx] = FetchStringFromDict(segment, dict, baseptr, dict_offset, str_len);
}

//===--------------------------------------------------------------------===//
// Helper Functions
//===--------------------------------------------------------------------===//
bool DictionaryCompressionStorage::HasEnoughSpace(idx_t current_count, idx_t index_count, idx_t dict_size,
                                                  bitpacking_width_t packing_width) {
	return RequiredSpace(current_count, index_count, dict_size, packing_width) <= Storage::BLOCK_SIZE;
}

idx_t DictionaryCompressionStorage::RequiredSpace(idx_t current_count, idx_t index_count, idx_t dict_size,
                                                  bitpacking_width_t packing_width) {
	idx_t base_space = DICTIONARY_HEADER_SIZE + dict_size;
	idx_t string_number_space = BitpackingPrimitives::GetRequiredSize<sel_t>(current_count, packing_width);
	idx_t index_space = index_count * sizeof(uint32_t);

	idx_t used_space = base_space + index_space + string_number_space;

	return used_space;
}

StringDictionaryContainer DictionaryCompressionStorage::GetDictionary(ColumnSegment &segment, BufferHandle &handle) {
	auto header_ptr = (dictionary_compression_header_t *)(handle.node->buffer + segment.GetBlockOffset());
	StringDictionaryContainer container;
	container.size = Load<uint32_t>((data_ptr_t)&header_ptr->dict_size);
	container.end = Load<uint32_t>((data_ptr_t)&header_ptr->dict_end);
	return container;
}

void DictionaryCompressionStorage::SetDictionary(ColumnSegment &segment, BufferHandle &handle,
                                                 StringDictionaryContainer container) {
	auto header_ptr = (dictionary_compression_header_t *)(handle.node->buffer + segment.GetBlockOffset());
	Store<uint32_t>(container.size, (data_ptr_t)&header_ptr->dict_size);
	Store<uint32_t>(container.end, (data_ptr_t)&header_ptr->dict_end);
}

string_t DictionaryCompressionStorage::FetchStringFromDict(ColumnSegment &segment, StringDictionaryContainer dict,
                                                           data_ptr_t baseptr, int32_t dict_offset,
                                                           uint16_t string_len) {
	D_ASSERT(dict_offset >= 0 && dict_offset <= Storage::BLOCK_SIZE);

	if (dict_offset == 0) {
		return string_t(nullptr, 0);
	}
	// normal string: read string from this block
	auto dict_end = baseptr + dict.end;
	auto dict_pos = dict_end - dict_offset;

	auto str_ptr = (char *)(dict_pos);
	return string_t(str_ptr, string_len);
}

uint16_t DictionaryCompressionStorage::GetStringLength(uint32_t *index_buffer_ptr, sel_t index) {
	if (index == 0) {
		return 0;
	} else {
		return index_buffer_ptr[index] - index_buffer_ptr[index - 1];
	}
}

//===--------------------------------------------------------------------===//
// Get Function
//===--------------------------------------------------------------------===//
CompressionFunction DictionaryCompressionFun::GetFunction(PhysicalType data_type) {
	return CompressionFunction(
	    CompressionType::COMPRESSION_DICTIONARY, data_type, DictionaryCompressionStorage ::StringInitAnalyze,
	    DictionaryCompressionStorage::StringAnalyze, DictionaryCompressionStorage::StringFinalAnalyze,
	    DictionaryCompressionStorage::InitCompression, DictionaryCompressionStorage::Compress,
	    DictionaryCompressionStorage::FinalizeCompress, DictionaryCompressionStorage::StringInitScan,
	    DictionaryCompressionStorage::StringScan, DictionaryCompressionStorage::StringScanPartial<false>,
	    DictionaryCompressionStorage::StringFetchRow, UncompressedFunctions::EmptySkip);
}

bool DictionaryCompressionFun::TypeIsSupported(PhysicalType type) {
	return type == PhysicalType::VARCHAR;
}
} // namespace duckdb












namespace duckdb {

//===--------------------------------------------------------------------===//
// Analyze
//===--------------------------------------------------------------------===//
struct FixedSizeAnalyzeState : public AnalyzeState {
	FixedSizeAnalyzeState() : count(0) {
	}

	idx_t count;
};

unique_ptr<AnalyzeState> FixedSizeInitAnalyze(ColumnData &col_data, PhysicalType type) {
	return make_unique<FixedSizeAnalyzeState>();
}

bool FixedSizeAnalyze(AnalyzeState &state_p, Vector &input, idx_t count) {
	auto &state = (FixedSizeAnalyzeState &)state_p;
	state.count += count;
	return true;
}

template <class T>
idx_t FixedSizeFinalAnalyze(AnalyzeState &state_p) {
	auto &state = (FixedSizeAnalyzeState &)state_p;
	return sizeof(T) * state.count;
}

//===--------------------------------------------------------------------===//
// Compress
//===--------------------------------------------------------------------===//
UncompressedCompressState::UncompressedCompressState(ColumnDataCheckpointer &checkpointer)
    : checkpointer(checkpointer) {
	CreateEmptySegment(checkpointer.GetRowGroup().start);
}

void UncompressedCompressState::CreateEmptySegment(idx_t row_start) {
	auto &db = checkpointer.GetDatabase();
	auto &type = checkpointer.GetType();
	auto compressed_segment = ColumnSegment::CreateTransientSegment(db, type, row_start);
	if (type.InternalType() == PhysicalType::VARCHAR) {
		auto &state = (UncompressedStringSegmentState &)*compressed_segment->GetSegmentState();
		state.overflow_writer = make_unique<WriteOverflowStringsToDisk>(db);
	}
	current_segment = move(compressed_segment);
}

void UncompressedCompressState::FlushSegment(idx_t segment_size) {
	auto &state = checkpointer.GetCheckpointState();
	state.FlushSegment(move(current_segment), segment_size);
}

void UncompressedCompressState::Finalize(idx_t segment_size) {
	FlushSegment(segment_size);
	current_segment.reset();
}

unique_ptr<CompressionState> UncompressedFunctions::InitCompression(ColumnDataCheckpointer &checkpointer,
                                                                    unique_ptr<AnalyzeState> state) {
	return make_unique<UncompressedCompressState>(checkpointer);
}

void UncompressedFunctions::Compress(CompressionState &state_p, Vector &data, idx_t count) {
	auto &state = (UncompressedCompressState &)state_p;
	VectorData vdata;
	data.Orrify(count, vdata);

	ColumnAppendState append_state;
	idx_t offset = 0;
	while (count > 0) {
		idx_t appended = state.current_segment->Append(append_state, vdata, offset, count);
		if (appended == count) {
			// appended everything: finished
			return;
		}
		auto next_start = state.current_segment->start + state.current_segment->count;
		// the segment is full: flush it to disk
		state.FlushSegment(state.current_segment->FinalizeAppend());

		// now create a new segment and continue appending
		state.CreateEmptySegment(next_start);
		offset += appended;
		count -= appended;
	}
}

void UncompressedFunctions::FinalizeCompress(CompressionState &state_p) {
	auto &state = (UncompressedCompressState &)state_p;
	state.Finalize(state.current_segment->FinalizeAppend());
}

//===--------------------------------------------------------------------===//
// Scan
//===--------------------------------------------------------------------===//
struct FixedSizeScanState : public SegmentScanState {
	unique_ptr<BufferHandle> handle;
};

unique_ptr<SegmentScanState> FixedSizeInitScan(ColumnSegment &segment) {
	auto result = make_unique<FixedSizeScanState>();
	auto &buffer_manager = BufferManager::GetBufferManager(segment.db);
	result->handle = buffer_manager.Pin(segment.block);
	return move(result);
}

//===--------------------------------------------------------------------===//
// Scan base data
//===--------------------------------------------------------------------===//
template <class T>
void FixedSizeScanPartial(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result,
                          idx_t result_offset) {
	auto &scan_state = (FixedSizeScanState &)*state.scan_state;
	auto start = segment.GetRelativeIndex(state.row_index);

	auto data = scan_state.handle->node->buffer + segment.GetBlockOffset();
	auto source_data = data + start * sizeof(T);

	// copy the data from the base table
	result.SetVectorType(VectorType::FLAT_VECTOR);
	memcpy(FlatVector::GetData(result) + result_offset * sizeof(T), source_data, scan_count * sizeof(T));
}

template <class T>
void FixedSizeScan(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result) {
	auto &scan_state = (FixedSizeScanState &)*state.scan_state;
	auto start = segment.GetRelativeIndex(state.row_index);

	auto data = scan_state.handle->node->buffer + segment.GetBlockOffset();
	auto source_data = data + start * sizeof(T);

	result.SetVectorType(VectorType::FLAT_VECTOR);
	if (std::is_same<T, list_entry_t>()) {
		// list columns are modified in-place during the scans to correct the offsets
		// so we can't do a zero-copy there
		memcpy(FlatVector::GetData(result), source_data, scan_count * sizeof(T));
	} else {
		FlatVector::SetData(result, source_data);
	}
}

//===--------------------------------------------------------------------===//
// Fetch
//===--------------------------------------------------------------------===//
template <class T>
void FixedSizeFetchRow(ColumnSegment &segment, ColumnFetchState &state, row_t row_id, Vector &result,
                       idx_t result_idx) {
	auto &buffer_manager = BufferManager::GetBufferManager(segment.db);
	auto handle = buffer_manager.Pin(segment.block);

	// first fetch the data from the base table
	auto data_ptr = handle->node->buffer + segment.GetBlockOffset() + row_id * sizeof(T);

	memcpy(FlatVector::GetData(result) + result_idx * sizeof(T), data_ptr, sizeof(T));
}

//===--------------------------------------------------------------------===//
// Append
//===--------------------------------------------------------------------===//
template <class T>
static void AppendLoop(SegmentStatistics &stats, data_ptr_t target, idx_t target_offset, VectorData &adata,
                       idx_t offset, idx_t count) {
	auto sdata = (T *)adata.data;
	auto tdata = (T *)target;
	if (!adata.validity.AllValid()) {
		for (idx_t i = 0; i < count; i++) {
			auto source_idx = adata.sel->get_index(offset + i);
			auto target_idx = target_offset + i;
			bool is_null = !adata.validity.RowIsValid(source_idx);
			if (!is_null) {
				NumericStatistics::Update<T>(stats, sdata[source_idx]);
				tdata[target_idx] = sdata[source_idx];
			} else {
				// we insert a NullValue<T> in the null gap for debuggability
				// this value should never be used or read anywhere
				tdata[target_idx] = NullValue<T>();
			}
		}
	} else {
		for (idx_t i = 0; i < count; i++) {
			auto source_idx = adata.sel->get_index(offset + i);
			auto target_idx = target_offset + i;
			NumericStatistics::Update<T>(stats, sdata[source_idx]);
			tdata[target_idx] = sdata[source_idx];
		}
	}
}

template <>
void AppendLoop<list_entry_t>(SegmentStatistics &stats, data_ptr_t target, idx_t target_offset, VectorData &adata,
                              idx_t offset, idx_t count) {
	auto sdata = (list_entry_t *)adata.data;
	auto tdata = (list_entry_t *)target;
	for (idx_t i = 0; i < count; i++) {
		auto source_idx = adata.sel->get_index(offset + i);
		auto target_idx = target_offset + i;
		tdata[target_idx] = sdata[source_idx];
	}
}

template <class T>
idx_t FixedSizeAppend(ColumnSegment &segment, SegmentStatistics &stats, VectorData &data, idx_t offset, idx_t count) {
	auto &buffer_manager = BufferManager::GetBufferManager(segment.db);
	auto handle = buffer_manager.Pin(segment.block);
	D_ASSERT(segment.GetBlockOffset() == 0);

	auto target_ptr = handle->node->buffer;
	idx_t max_tuple_count = Storage::BLOCK_SIZE / sizeof(T);
	idx_t copy_count = MinValue<idx_t>(count, max_tuple_count - segment.count);

	AppendLoop<T>(stats, target_ptr, segment.count, data, offset, copy_count);
	segment.count += copy_count;
	return copy_count;
}

template <class T>
idx_t FixedSizeFinalizeAppend(ColumnSegment &segment, SegmentStatistics &stats) {
	return segment.count * sizeof(T);
}

//===--------------------------------------------------------------------===//
// Get Function
//===--------------------------------------------------------------------===//
template <class T>
CompressionFunction FixedSizeGetFunction(PhysicalType data_type) {
	return CompressionFunction(CompressionType::COMPRESSION_UNCOMPRESSED, data_type, FixedSizeInitAnalyze,
	                           FixedSizeAnalyze, FixedSizeFinalAnalyze<T>, UncompressedFunctions::InitCompression,
	                           UncompressedFunctions::Compress, UncompressedFunctions::FinalizeCompress,
	                           FixedSizeInitScan, FixedSizeScan<T>, FixedSizeScanPartial<T>, FixedSizeFetchRow<T>,
	                           UncompressedFunctions::EmptySkip, nullptr, FixedSizeAppend<T>,
	                           FixedSizeFinalizeAppend<T>, nullptr);
}

CompressionFunction FixedSizeUncompressed::GetFunction(PhysicalType data_type) {
	switch (data_type) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		return FixedSizeGetFunction<int8_t>(data_type);
	case PhysicalType::INT16:
		return FixedSizeGetFunction<int16_t>(data_type);
	case PhysicalType::INT32:
		return FixedSizeGetFunction<int32_t>(data_type);
	case PhysicalType::INT64:
		return FixedSizeGetFunction<int64_t>(data_type);
	case PhysicalType::UINT8:
		return FixedSizeGetFunction<uint8_t>(data_type);
	case PhysicalType::UINT16:
		return FixedSizeGetFunction<uint16_t>(data_type);
	case PhysicalType::UINT32:
		return FixedSizeGetFunction<uint32_t>(data_type);
	case PhysicalType::UINT64:
		return FixedSizeGetFunction<uint64_t>(data_type);
	case PhysicalType::INT128:
		return FixedSizeGetFunction<hugeint_t>(data_type);
	case PhysicalType::FLOAT:
		return FixedSizeGetFunction<float>(data_type);
	case PhysicalType::DOUBLE:
		return FixedSizeGetFunction<double>(data_type);
	case PhysicalType::INTERVAL:
		return FixedSizeGetFunction<interval_t>(data_type);
	case PhysicalType::LIST:
		return FixedSizeGetFunction<list_entry_t>(data_type);
	default:
		throw InternalException("Unsupported type for FixedSizeUncompressed::GetFunction");
	}
}

} // namespace duckdb









namespace duckdb {

//===--------------------------------------------------------------------===//
// Scan
//===--------------------------------------------------------------------===//
unique_ptr<SegmentScanState> ConstantInitScan(ColumnSegment &segment) {
	return nullptr;
}

//===--------------------------------------------------------------------===//
// Scan base data
//===--------------------------------------------------------------------===//
void ConstantScanFunctionValidity(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result) {
	auto &validity = (ValidityStatistics &)*segment.stats.statistics;
	if (validity.has_null) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
		ConstantVector::SetNull(result, true);
	}
}

template <class T>
void ConstantScanFunction(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result) {
	auto &nstats = (NumericStatistics &)*segment.stats.statistics;

	auto data = FlatVector::GetData<T>(result);
	data[0] = nstats.min.GetValueUnsafe<T>();
	result.SetVectorType(VectorType::CONSTANT_VECTOR);
}

//===--------------------------------------------------------------------===//
// Scan Partial
//===--------------------------------------------------------------------===//
void ConstantFillFunctionValidity(ColumnSegment &segment, Vector &result, idx_t start_idx, idx_t count) {
	auto &validity = (ValidityStatistics &)*segment.stats.statistics;
	if (validity.has_null) {
		auto &mask = FlatVector::Validity(result);
		for (idx_t i = 0; i < count; i++) {
			mask.SetInvalid(start_idx + i);
		}
	}
}

template <class T>
void ConstantFillFunction(ColumnSegment &segment, Vector &result, idx_t start_idx, idx_t count) {
	auto &nstats = (NumericStatistics &)*segment.stats.statistics;

	auto data = FlatVector::GetData<T>(result);
	auto constant_value = nstats.min.GetValueUnsafe<T>();
	for (idx_t i = 0; i < count; i++) {
		data[start_idx + i] = constant_value;
	}
}

void ConstantScanPartialValidity(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result,
                                 idx_t result_offset) {
	ConstantFillFunctionValidity(segment, result, result_offset, scan_count);
}

template <class T>
void ConstantScanPartial(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result,
                         idx_t result_offset) {
	ConstantFillFunction<T>(segment, result, result_offset, scan_count);
}

//===--------------------------------------------------------------------===//
// Fetch
//===--------------------------------------------------------------------===//
void ConstantFetchRowValidity(ColumnSegment &segment, ColumnFetchState &state, row_t row_id, Vector &result,
                              idx_t result_idx) {
	ConstantFillFunctionValidity(segment, result, result_idx, 1);
}

template <class T>
void ConstantFetchRow(ColumnSegment &segment, ColumnFetchState &state, row_t row_id, Vector &result, idx_t result_idx) {
	ConstantFillFunction<T>(segment, result, result_idx, 1);
}

//===--------------------------------------------------------------------===//
// Get Function
//===--------------------------------------------------------------------===//
CompressionFunction ConstantGetFunctionValidity(PhysicalType data_type) {
	D_ASSERT(data_type == PhysicalType::BIT);
	return CompressionFunction(CompressionType::COMPRESSION_CONSTANT, data_type, nullptr, nullptr, nullptr, nullptr,
	                           nullptr, nullptr, ConstantInitScan, ConstantScanFunctionValidity,
	                           ConstantScanPartialValidity, ConstantFetchRowValidity, UncompressedFunctions::EmptySkip);
}

template <class T>
CompressionFunction ConstantGetFunction(PhysicalType data_type) {
	return CompressionFunction(CompressionType::COMPRESSION_CONSTANT, data_type, nullptr, nullptr, nullptr, nullptr,
	                           nullptr, nullptr, ConstantInitScan, ConstantScanFunction<T>, ConstantScanPartial<T>,
	                           ConstantFetchRow<T>, UncompressedFunctions::EmptySkip);
}

CompressionFunction ConstantFun::GetFunction(PhysicalType data_type) {
	switch (data_type) {
	case PhysicalType::BIT:
		return ConstantGetFunctionValidity(data_type);
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		return ConstantGetFunction<int8_t>(data_type);
	case PhysicalType::INT16:
		return ConstantGetFunction<int16_t>(data_type);
	case PhysicalType::INT32:
		return ConstantGetFunction<int32_t>(data_type);
	case PhysicalType::INT64:
		return ConstantGetFunction<int64_t>(data_type);
	case PhysicalType::UINT8:
		return ConstantGetFunction<uint8_t>(data_type);
	case PhysicalType::UINT16:
		return ConstantGetFunction<uint16_t>(data_type);
	case PhysicalType::UINT32:
		return ConstantGetFunction<uint32_t>(data_type);
	case PhysicalType::UINT64:
		return ConstantGetFunction<uint64_t>(data_type);
	case PhysicalType::INT128:
		return ConstantGetFunction<hugeint_t>(data_type);
	case PhysicalType::FLOAT:
		return ConstantGetFunction<float>(data_type);
	case PhysicalType::DOUBLE:
		return ConstantGetFunction<double>(data_type);
	default:
		throw InternalException("Unsupported type for ConstantUncompressed::GetFunction");
	}
}

bool ConstantFun::TypeIsSupported(PhysicalType type) {
	switch (type) {
	case PhysicalType::BIT:
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
	case PhysicalType::INT16:
	case PhysicalType::INT32:
	case PhysicalType::INT64:
	case PhysicalType::UINT8:
	case PhysicalType::UINT16:
	case PhysicalType::UINT32:
	case PhysicalType::UINT64:
	case PhysicalType::INT128:
	case PhysicalType::FLOAT:
	case PhysicalType::DOUBLE:
		return true;
	default:
		throw InternalException("Unsupported type for constant function");
	}
}

} // namespace duckdb








#include <functional>

namespace duckdb {

using rle_count_t = uint16_t;

//===--------------------------------------------------------------------===//
// Analyze
//===--------------------------------------------------------------------===//
struct EmptyRLEWriter {
	template <class VALUE_TYPE>
	static void Operation(VALUE_TYPE value, rle_count_t count, void *dataptr, bool is_null) {
	}
};

template <class T>
struct RLEState {
	RLEState() : seen_count(0), last_value(NullValue<T>()), last_seen_count(0), dataptr(nullptr) {
	}

	idx_t seen_count;
	T last_value;
	rle_count_t last_seen_count;
	void *dataptr;
	bool all_null = true;

public:
	template <class OP>
	void Flush() {
		OP::template Operation<T>(last_value, last_seen_count, dataptr, all_null);
	}

	template <class OP = EmptyRLEWriter>
	void Update(T *data, ValidityMask &validity, idx_t idx) {
		if (validity.RowIsValid(idx)) {
			all_null = false;
			if (seen_count == 0) {
				// no value seen yet
				// assign the current value, and set the seen_count to 1
				// note that we increment last_seen_count rather than setting it to 1
				// this is intentional: this is the first VALID value we see
				// but it might not be the first value in case of nulls!
				last_value = data[idx];
				seen_count = 1;
				last_seen_count++;
			} else if (last_value == data[idx]) {
				// the last value is identical to this value: increment the last_seen_count
				last_seen_count++;
			} else {
				// the values are different
				// issue the callback on the last value
				Flush<OP>();

				// increment the seen_count and put the new value into the RLE slot
				last_value = data[idx];
				seen_count++;
				last_seen_count = 1;
			}
		} else {
			// NULL value: we merely increment the last_seen_count
			last_seen_count++;
		}
		if (last_seen_count == NumericLimits<rle_count_t>::Maximum()) {
			// we have seen the same value so many times in a row we are at the limit of what fits in our count
			// write away the value and move to the next value
			Flush<OP>();
			last_seen_count = 0;
			seen_count++;
		}
	}
};

template <class T>
struct RLEAnalyzeState : public AnalyzeState {
	RLEAnalyzeState() {
	}

	RLEState<T> state;
};

template <class T>
unique_ptr<AnalyzeState> RLEInitAnalyze(ColumnData &col_data, PhysicalType type) {
	return make_unique<RLEAnalyzeState<T>>();
}

template <class T>
bool RLEAnalyze(AnalyzeState &state, Vector &input, idx_t count) {
	auto &rle_state = (RLEAnalyzeState<T> &)state;
	VectorData vdata;
	input.Orrify(count, vdata);

	auto data = (T *)vdata.data;
	for (idx_t i = 0; i < count; i++) {
		auto idx = vdata.sel->get_index(i);
		rle_state.state.Update(data, vdata.validity, idx);
	}
	return true;
}

template <class T>
idx_t RLEFinalAnalyze(AnalyzeState &state) {
	auto &rle_state = (RLEAnalyzeState<T> &)state;
	return (sizeof(rle_count_t) + sizeof(T)) * rle_state.state.seen_count;
}

//===--------------------------------------------------------------------===//
// Compress
//===--------------------------------------------------------------------===//
struct RLEConstants {
	static constexpr const idx_t RLE_HEADER_SIZE = sizeof(uint64_t);
};

template <class T>
struct RLECompressState : public CompressionState {
	struct RLEWriter {
		template <class VALUE_TYPE>
		static void Operation(VALUE_TYPE value, rle_count_t count, void *dataptr, bool is_null) {
			auto state = (RLECompressState<T> *)dataptr;
			state->WriteValue(value, count, is_null);
		}
	};

	static idx_t MaxRLECount() {
		auto entry_size = sizeof(T) + sizeof(rle_count_t);
		auto entry_count = (Storage::BLOCK_SIZE - RLEConstants::RLE_HEADER_SIZE) / entry_size;
		auto max_vector_count = entry_count / STANDARD_VECTOR_SIZE;
		return max_vector_count * STANDARD_VECTOR_SIZE;
	}

	explicit RLECompressState(ColumnDataCheckpointer &checkpointer_p) : checkpointer(checkpointer_p) {
		auto &db = checkpointer.GetDatabase();
		auto &type = checkpointer.GetType();
		auto &config = DBConfig::GetConfig(db);
		function = config.GetCompressionFunction(CompressionType::COMPRESSION_RLE, type.InternalType());
		CreateEmptySegment(checkpointer.GetRowGroup().start);

		state.dataptr = (void *)this;
		max_rle_count = MaxRLECount();
	}

	void CreateEmptySegment(idx_t row_start) {
		auto &db = checkpointer.GetDatabase();
		auto &type = checkpointer.GetType();
		auto column_segment = ColumnSegment::CreateTransientSegment(db, type, row_start);
		column_segment->function = function;
		current_segment = move(column_segment);
		auto &buffer_manager = BufferManager::GetBufferManager(db);
		handle = buffer_manager.Pin(current_segment->block);
	}

	void Append(VectorData &vdata, idx_t count) {
		auto data = (T *)vdata.data;
		for (idx_t i = 0; i < count; i++) {
			auto idx = vdata.sel->get_index(i);
			state.template Update<RLECompressState<T>::RLEWriter>(data, vdata.validity, idx);
		}
	}

	void WriteValue(T value, rle_count_t count, bool is_null) {
		// write the RLE entry
		auto handle_ptr = handle->Ptr() + RLEConstants::RLE_HEADER_SIZE;
		auto data_pointer = (T *)handle_ptr;
		auto index_pointer = (rle_count_t *)(handle_ptr + max_rle_count * sizeof(T));
		data_pointer[entry_count] = value;
		index_pointer[entry_count] = count;
		entry_count++;

		// update meta data
		if (!is_null) {
			NumericStatistics::Update<T>(current_segment->stats, value);
		}
		current_segment->count += count;

		if (entry_count == max_rle_count) {
			// we have finished writing this segment: flush it and create a new segment
			auto row_start = current_segment->start + current_segment->count;
			FlushSegment();
			CreateEmptySegment(row_start);
			entry_count = 0;
		}
	}

	void FlushSegment() {
		// flush the segment
		// we compact the segment by moving the counts so they are directly next to the values
		idx_t counts_size = sizeof(rle_count_t) * entry_count;
		idx_t original_rle_offset = RLEConstants::RLE_HEADER_SIZE + max_rle_count * sizeof(T);
		idx_t minimal_rle_offset = AlignValue(RLEConstants::RLE_HEADER_SIZE + sizeof(T) * entry_count);
		idx_t total_segment_size = minimal_rle_offset + counts_size;
		memmove(handle->node->buffer + minimal_rle_offset, handle->node->buffer + original_rle_offset, counts_size);
		// store the final RLE offset within the segment
		Store<uint64_t>(minimal_rle_offset, handle->node->buffer);
		handle.reset();

		auto &state = checkpointer.GetCheckpointState();
		state.FlushSegment(move(current_segment), total_segment_size);
	}

	void Finalize() {
		state.template Flush<RLECompressState<T>::RLEWriter>();

		FlushSegment();
		current_segment.reset();
	}

	ColumnDataCheckpointer &checkpointer;
	CompressionFunction *function;
	unique_ptr<ColumnSegment> current_segment;
	unique_ptr<BufferHandle> handle;

	RLEState<T> state;
	idx_t entry_count = 0;
	idx_t max_rle_count;
};

template <class T>
unique_ptr<CompressionState> RLEInitCompression(ColumnDataCheckpointer &checkpointer, unique_ptr<AnalyzeState> state) {
	return make_unique<RLECompressState<T>>(checkpointer);
}

template <class T>
void RLECompress(CompressionState &state_p, Vector &scan_vector, idx_t count) {
	auto &state = (RLECompressState<T> &)state_p;
	VectorData vdata;
	scan_vector.Orrify(count, vdata);

	state.Append(vdata, count);
}

template <class T>
void RLEFinalizeCompress(CompressionState &state_p) {
	auto &state = (RLECompressState<T> &)state_p;
	state.Finalize();
}

//===--------------------------------------------------------------------===//
// Scan
//===--------------------------------------------------------------------===//
template <class T>
struct RLEScanState : public SegmentScanState {
	explicit RLEScanState(ColumnSegment &segment) {
		auto &buffer_manager = BufferManager::GetBufferManager(segment.db);
		handle = buffer_manager.Pin(segment.block);
		entry_pos = 0;
		position_in_entry = 0;
		rle_count_offset = Load<uint64_t>(handle->node->buffer + segment.GetBlockOffset());
		D_ASSERT(rle_count_offset <= Storage::BLOCK_SIZE);
	}

	void Skip(ColumnSegment &segment, idx_t skip_count) {
		auto data = handle->node->buffer + segment.GetBlockOffset();
		auto index_pointer = (rle_count_t *)(data + rle_count_offset);

		for (idx_t i = 0; i < skip_count; i++) {
			// assign the current value
			position_in_entry++;
			if (position_in_entry >= index_pointer[entry_pos]) {
				// handled all entries in this RLE value
				// move to the next entry
				entry_pos++;
				position_in_entry = 0;
			}
		}
	}

	unique_ptr<BufferHandle> handle;
	uint32_t rle_offset;
	idx_t entry_pos;
	idx_t position_in_entry;
	uint32_t rle_count_offset;
};

template <class T>
unique_ptr<SegmentScanState> RLEInitScan(ColumnSegment &segment) {
	auto result = make_unique<RLEScanState<T>>(segment);
	return move(result);
}

//===--------------------------------------------------------------------===//
// Scan base data
//===--------------------------------------------------------------------===//
template <class T>
void RLESkip(ColumnSegment &segment, ColumnScanState &state, idx_t skip_count) {
	auto &scan_state = (RLEScanState<T> &)*state.scan_state;
	scan_state.Skip(segment, skip_count);
}

template <class T>
void RLEScanPartial(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result,
                    idx_t result_offset) {
	auto &scan_state = (RLEScanState<T> &)*state.scan_state;

	auto data = scan_state.handle->node->buffer + segment.GetBlockOffset();
	auto data_pointer = (T *)(data + RLEConstants::RLE_HEADER_SIZE);
	auto index_pointer = (rle_count_t *)(data + scan_state.rle_count_offset);

	auto result_data = FlatVector::GetData<T>(result);
	result.SetVectorType(VectorType::FLAT_VECTOR);
	for (idx_t i = 0; i < scan_count; i++) {
		// assign the current value
		result_data[result_offset + i] = data_pointer[scan_state.entry_pos];
		scan_state.position_in_entry++;
		if (scan_state.position_in_entry >= index_pointer[scan_state.entry_pos]) {
			// handled all entries in this RLE value
			// move to the next entry
			scan_state.entry_pos++;
			scan_state.position_in_entry = 0;
		}
	}
}

template <class T>
void RLEScan(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result) {
	// FIXME: emit constant vector if repetition of single value is >= scan_count
	RLEScanPartial<T>(segment, state, scan_count, result, 0);
}

//===--------------------------------------------------------------------===//
// Fetch
//===--------------------------------------------------------------------===//
template <class T>
void RLEFetchRow(ColumnSegment &segment, ColumnFetchState &state, row_t row_id, Vector &result, idx_t result_idx) {
	RLEScanState<T> scan_state(segment);
	scan_state.Skip(segment, row_id);

	auto data = scan_state.handle->node->buffer + segment.GetBlockOffset();
	auto data_pointer = (T *)(data + RLEConstants::RLE_HEADER_SIZE);
	auto result_data = FlatVector::GetData<T>(result);
	result_data[result_idx] = data_pointer[scan_state.entry_pos];
}

//===--------------------------------------------------------------------===//
// Get Function
//===--------------------------------------------------------------------===//
template <class T>
CompressionFunction GetRLEFunction(PhysicalType data_type) {
	return CompressionFunction(CompressionType::COMPRESSION_RLE, data_type, RLEInitAnalyze<T>, RLEAnalyze<T>,
	                           RLEFinalAnalyze<T>, RLEInitCompression<T>, RLECompress<T>, RLEFinalizeCompress<T>,
	                           RLEInitScan<T>, RLEScan<T>, RLEScanPartial<T>, RLEFetchRow<T>, RLESkip<T>);
}

CompressionFunction RLEFun::GetFunction(PhysicalType type) {
	switch (type) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		return GetRLEFunction<int8_t>(type);
	case PhysicalType::INT16:
		return GetRLEFunction<int16_t>(type);
	case PhysicalType::INT32:
		return GetRLEFunction<int32_t>(type);
	case PhysicalType::INT64:
		return GetRLEFunction<int64_t>(type);
	case PhysicalType::INT128:
		return GetRLEFunction<hugeint_t>(type);
	case PhysicalType::UINT8:
		return GetRLEFunction<uint8_t>(type);
	case PhysicalType::UINT16:
		return GetRLEFunction<uint16_t>(type);
	case PhysicalType::UINT32:
		return GetRLEFunction<uint32_t>(type);
	case PhysicalType::UINT64:
		return GetRLEFunction<uint64_t>(type);
	case PhysicalType::FLOAT:
		return GetRLEFunction<float>(type);
	case PhysicalType::DOUBLE:
		return GetRLEFunction<double>(type);
	default:
		throw InternalException("Unsupported type for RLE");
	}
}

bool RLEFun::TypeIsSupported(PhysicalType type) {
	switch (type) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
	case PhysicalType::INT16:
	case PhysicalType::INT32:
	case PhysicalType::INT64:
	case PhysicalType::INT128:
	case PhysicalType::UINT8:
	case PhysicalType::UINT16:
	case PhysicalType::UINT32:
	case PhysicalType::UINT64:
	case PhysicalType::FLOAT:
	case PhysicalType::DOUBLE:
		return true;
	default:
		return false;
	}
}

} // namespace duckdb




namespace duckdb {

//===--------------------------------------------------------------------===//
// Storage Class
//===--------------------------------------------------------------------===//
UncompressedStringSegmentState::~UncompressedStringSegmentState() {
	while (head) {
		// prevent deep recursion here
		head = move(head->next);
	}
}

//===--------------------------------------------------------------------===//
// Analyze
//===--------------------------------------------------------------------===//
struct StringAnalyzeState : public AnalyzeState {
	StringAnalyzeState() : count(0), total_string_size(0), overflow_strings(0) {
	}

	idx_t count;
	idx_t total_string_size;
	idx_t overflow_strings;
};

unique_ptr<AnalyzeState> UncompressedStringStorage::StringInitAnalyze(ColumnData &col_data, PhysicalType type) {
	return make_unique<StringAnalyzeState>();
}

bool UncompressedStringStorage::StringAnalyze(AnalyzeState &state_p, Vector &input, idx_t count) {
	auto &state = (StringAnalyzeState &)state_p;
	VectorData vdata;
	input.Orrify(count, vdata);

	state.count += count;
	auto data = (string_t *)vdata.data;
	for (idx_t i = 0; i < count; i++) {
		auto idx = vdata.sel->get_index(i);
		if (vdata.validity.RowIsValid(idx)) {
			auto string_size = data[idx].GetSize();
			state.total_string_size += string_size;
			if (string_size >= StringUncompressed::STRING_BLOCK_LIMIT) {
				state.overflow_strings++;
			}
		}
	}
	return true;
}

idx_t UncompressedStringStorage::StringFinalAnalyze(AnalyzeState &state_p) {
	auto &state = (StringAnalyzeState &)state_p;
	return state.count * sizeof(int32_t) + state.total_string_size + state.overflow_strings * BIG_STRING_MARKER_SIZE;
}

//===--------------------------------------------------------------------===//
// Scan
//===--------------------------------------------------------------------===//
unique_ptr<SegmentScanState> UncompressedStringStorage::StringInitScan(ColumnSegment &segment) {
	auto result = make_unique<StringScanState>();
	auto &buffer_manager = BufferManager::GetBufferManager(segment.db);
	result->handle = buffer_manager.Pin(segment.block);
	return move(result);
}

//===--------------------------------------------------------------------===//
// Scan base data
//===--------------------------------------------------------------------===//
void UncompressedStringStorage::StringScanPartial(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count,
                                                  Vector &result, idx_t result_offset) {
	// clear any previously locked buffers and get the primary buffer handle
	auto &scan_state = (StringScanState &)*state.scan_state;
	auto start = segment.GetRelativeIndex(state.row_index);

	auto baseptr = scan_state.handle->node->buffer + segment.GetBlockOffset();
	auto dict = GetDictionary(segment, *scan_state.handle);
	auto base_data = (int32_t *)(baseptr + DICTIONARY_HEADER_SIZE);
	auto result_data = FlatVector::GetData<string_t>(result);

	int32_t previous_offset = start > 0 ? base_data[start - 1] : 0;

	for (idx_t i = 0; i < scan_count; i++) {
		// std::abs used since offsets can be negative to indicate big strings
		uint32_t string_length = std::abs(base_data[start + i]) - std::abs(previous_offset);
		result_data[result_offset + i] =
		    FetchStringFromDict(segment, dict, result, baseptr, base_data[start + i], string_length);
		previous_offset = base_data[start + i];
	}
}

void UncompressedStringStorage::StringScan(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count,
                                           Vector &result) {
	StringScanPartial(segment, state, scan_count, result, 0);
}

//===--------------------------------------------------------------------===//
// Fetch
//===--------------------------------------------------------------------===//
void UncompressedStringStorage::StringFetchRow(ColumnSegment &segment, ColumnFetchState &state, row_t row_id,
                                               Vector &result, idx_t result_idx) {
	// fetch a single row from the string segment
	// first pin the main buffer if it is not already pinned
	auto primary_id = segment.block->BlockId();

	BufferHandle *handle_ptr;
	auto entry = state.handles.find(primary_id);
	if (entry == state.handles.end()) {
		// not pinned yet: pin it
		auto &buffer_manager = BufferManager::GetBufferManager(segment.db);
		auto handle = buffer_manager.Pin(segment.block);
		handle_ptr = handle.get();
		state.handles[primary_id] = move(handle);
	} else {
		// already pinned: use the pinned handle
		handle_ptr = entry->second.get();
	}
	auto baseptr = handle_ptr->node->buffer + segment.GetBlockOffset();
	auto dict = GetDictionary(segment, *handle_ptr);
	auto base_data = (int32_t *)(baseptr + DICTIONARY_HEADER_SIZE);
	auto result_data = FlatVector::GetData<string_t>(result);

	auto dict_offset = base_data[row_id];
	uint32_t string_length;
	if ((idx_t)row_id == 0) {
		// edge case where this is the first string in the dict
		string_length = std::abs(dict_offset);
	} else {
		string_length = std::abs(dict_offset) - std::abs(base_data[row_id - 1]);
	}
	result_data[result_idx] = FetchStringFromDict(segment, dict, result, baseptr, dict_offset, string_length);
}

//===--------------------------------------------------------------------===//
// Append
//===--------------------------------------------------------------------===//
unique_ptr<CompressedSegmentState> UncompressedStringStorage::StringInitSegment(ColumnSegment &segment,
                                                                                block_id_t block_id) {
	auto &buffer_manager = BufferManager::GetBufferManager(segment.db);
	if (block_id == INVALID_BLOCK) {
		auto handle = buffer_manager.Pin(segment.block);
		StringDictionaryContainer dictionary;
		dictionary.size = 0;
		dictionary.end = Storage::BLOCK_SIZE;
		SetDictionary(segment, *handle, dictionary);
	}
	return make_unique<UncompressedStringSegmentState>();
}

idx_t UncompressedStringStorage::FinalizeAppend(ColumnSegment &segment, SegmentStatistics &stats) {
	auto &buffer_manager = BufferManager::GetBufferManager(segment.db);
	auto handle = buffer_manager.Pin(segment.block);
	auto dict = GetDictionary(segment, *handle);
	D_ASSERT(dict.end == Storage::BLOCK_SIZE);
	// compute the total size required to store this segment
	auto offset_size = DICTIONARY_HEADER_SIZE + segment.count * sizeof(int32_t);
	auto total_size = offset_size + dict.size;
	if (total_size >= COMPACTION_FLUSH_LIMIT) {
		// the block is full enough, don't bother moving around the dictionary
		return Storage::BLOCK_SIZE;
	}
	// the block has space left: figure out how much space we can save
	auto move_amount = Storage::BLOCK_SIZE - total_size;
	// move the dictionary so it lines up exactly with the offsets
	memmove(handle->node->buffer + offset_size, handle->node->buffer + dict.end - dict.size, dict.size);
	dict.end -= move_amount;
	D_ASSERT(dict.end == total_size);
	// write the new dictionary (with the updated "end")
	SetDictionary(segment, *handle, dict);
	return total_size;
}

//===--------------------------------------------------------------------===//
// Get Function
//===--------------------------------------------------------------------===//
CompressionFunction StringUncompressed::GetFunction(PhysicalType data_type) {
	D_ASSERT(data_type == PhysicalType::VARCHAR);
	return CompressionFunction(CompressionType::COMPRESSION_UNCOMPRESSED, data_type,
	                           UncompressedStringStorage::StringInitAnalyze, UncompressedStringStorage::StringAnalyze,
	                           UncompressedStringStorage::StringFinalAnalyze, UncompressedFunctions::InitCompression,
	                           UncompressedFunctions::Compress, UncompressedFunctions::FinalizeCompress,
	                           UncompressedStringStorage::StringInitScan, UncompressedStringStorage::StringScan,
	                           UncompressedStringStorage::StringScanPartial, UncompressedStringStorage::StringFetchRow,
	                           UncompressedFunctions::EmptySkip, UncompressedStringStorage::StringInitSegment,
	                           UncompressedStringStorage::StringAppend, UncompressedStringStorage::FinalizeAppend);
}

//===--------------------------------------------------------------------===//
// Helper Functions
//===--------------------------------------------------------------------===//
void UncompressedStringStorage::SetDictionary(ColumnSegment &segment, BufferHandle &handle,
                                              StringDictionaryContainer container) {
	auto startptr = handle.node->buffer + segment.GetBlockOffset();
	Store<uint32_t>(container.size, startptr);
	Store<uint32_t>(container.end, startptr + sizeof(uint32_t));
}

StringDictionaryContainer UncompressedStringStorage::GetDictionary(ColumnSegment &segment, BufferHandle &handle) {
	auto startptr = handle.node->buffer + segment.GetBlockOffset();
	StringDictionaryContainer container;
	container.size = Load<uint32_t>(startptr);
	container.end = Load<uint32_t>(startptr + sizeof(uint32_t));
	return container;
}

idx_t UncompressedStringStorage::RemainingSpace(ColumnSegment &segment, BufferHandle &handle) {
	auto dictionary = GetDictionary(segment, handle);
	D_ASSERT(dictionary.end == Storage::BLOCK_SIZE);
	idx_t used_space = dictionary.size + segment.count * sizeof(int32_t) + DICTIONARY_HEADER_SIZE;
	D_ASSERT(Storage::BLOCK_SIZE >= used_space);
	return Storage::BLOCK_SIZE - used_space;
}

void UncompressedStringStorage::WriteString(ColumnSegment &segment, string_t string, block_id_t &result_block,
                                            int32_t &result_offset) {
	auto &state = (UncompressedStringSegmentState &)*segment.GetSegmentState();
	if (state.overflow_writer) {
		// overflow writer is set: write string there
		state.overflow_writer->WriteString(string, result_block, result_offset);
	} else {
		// default overflow behavior: use in-memory buffer to store the overflow string
		WriteStringMemory(segment, string, result_block, result_offset);
	}
}

void UncompressedStringStorage::WriteStringMemory(ColumnSegment &segment, string_t string, block_id_t &result_block,
                                                  int32_t &result_offset) {
	uint32_t total_length = string.GetSize() + sizeof(uint32_t);
	shared_ptr<BlockHandle> block;
	unique_ptr<BufferHandle> handle;

	auto &buffer_manager = BufferManager::GetBufferManager(segment.db);
	auto &state = (UncompressedStringSegmentState &)*segment.GetSegmentState();
	// check if the string fits in the current block
	if (!state.head || state.head->offset + total_length >= state.head->size) {
		// string does not fit, allocate space for it
		// create a new string block
		idx_t alloc_size = MaxValue<idx_t>(total_length, Storage::BLOCK_SIZE);
		auto new_block = make_unique<StringBlock>();
		new_block->offset = 0;
		new_block->size = alloc_size;
		// allocate an in-memory buffer for it
		block = buffer_manager.RegisterMemory(alloc_size, false);
		handle = buffer_manager.Pin(block);
		state.overflow_blocks[block->BlockId()] = new_block.get();
		new_block->block = move(block);
		new_block->next = move(state.head);
		state.head = move(new_block);
	} else {
		// string fits, copy it into the current block
		handle = buffer_manager.Pin(state.head->block);
	}

	result_block = state.head->block->BlockId();
	result_offset = state.head->offset;

	// copy the string and the length there
	auto ptr = handle->node->buffer + state.head->offset;
	Store<uint32_t>(string.GetSize(), ptr);
	ptr += sizeof(uint32_t);
	memcpy(ptr, string.GetDataUnsafe(), string.GetSize());
	state.head->offset += total_length;
}

string_t UncompressedStringStorage::ReadOverflowString(ColumnSegment &segment, Vector &result, block_id_t block,
                                                       int32_t offset) {
	D_ASSERT(block != INVALID_BLOCK);
	D_ASSERT(offset < Storage::BLOCK_SIZE);

	auto &buffer_manager = BufferManager::GetBufferManager(segment.db);
	auto &state = (UncompressedStringSegmentState &)*segment.GetSegmentState();
	if (block < MAXIMUM_BLOCK) {
		// read the overflow string from disk
		// pin the initial handle and read the length
		auto block_handle = buffer_manager.RegisterBlock(block);
		auto handle = buffer_manager.Pin(block_handle);

		// read header
		uint32_t compressed_size = Load<uint32_t>(handle->node->buffer + offset);
		uint32_t uncompressed_size = Load<uint32_t>(handle->node->buffer + offset + sizeof(uint32_t));
		uint32_t remaining = compressed_size;
		offset += 2 * sizeof(uint32_t);

		data_ptr_t decompression_ptr;
		std::unique_ptr<data_t[]> decompression_buffer;

		// If string is in single block we decompress straight from it, else we copy first
		if (remaining <= Storage::BLOCK_SIZE - sizeof(block_id_t) - offset) {
			decompression_ptr = handle->node->buffer + offset;
		} else {
			decompression_buffer = std::unique_ptr<data_t[]>(new data_t[compressed_size]);
			auto target_ptr = decompression_buffer.get();

			// now append the string to the single buffer
			while (remaining > 0) {
				idx_t to_write = MinValue<idx_t>(remaining, Storage::BLOCK_SIZE - sizeof(block_id_t) - offset);
				memcpy(target_ptr, handle->node->buffer + offset, to_write);

				remaining -= to_write;
				offset += to_write;
				target_ptr += to_write;
				if (remaining > 0) {
					// read the next block
					block_id_t next_block = Load<block_id_t>(handle->node->buffer + offset);
					block_handle = buffer_manager.RegisterBlock(next_block);
					handle = buffer_manager.Pin(block_handle);
					offset = 0;
				}
			}
			decompression_ptr = decompression_buffer.get();
		}

		// overflow strings on disk are gzipped, decompress here
		auto decompressed_target_handle =
		    buffer_manager.Allocate(MaxValue<idx_t>(Storage::BLOCK_SIZE, uncompressed_size));
		auto decompressed_target_ptr = decompressed_target_handle->node->buffer;
		MiniZStream s;
		s.Decompress((const char *)decompression_ptr, compressed_size, (char *)decompressed_target_ptr,
		             uncompressed_size);

		auto final_buffer = decompressed_target_handle->node->buffer;
		StringVector::AddHandle(result, move(decompressed_target_handle));
		return ReadString(final_buffer, 0, uncompressed_size);
	} else {
		// read the overflow string from memory
		// first pin the handle, if it is not pinned yet
		auto entry = state.overflow_blocks.find(block);
		D_ASSERT(entry != state.overflow_blocks.end());
		auto handle = buffer_manager.Pin(entry->second->block);
		auto final_buffer = handle->node->buffer;
		StringVector::AddHandle(result, move(handle));
		return ReadStringWithLength(final_buffer, offset);
	}
}

string_t UncompressedStringStorage::ReadString(data_ptr_t target, int32_t offset, uint32_t string_length) {
	auto ptr = target + offset;
	auto str_ptr = (char *)(ptr);
	return string_t(str_ptr, string_length);
}

string_t UncompressedStringStorage::ReadStringWithLength(data_ptr_t target, int32_t offset) {
	auto ptr = target + offset;
	auto str_length = Load<uint32_t>(ptr);
	auto str_ptr = (char *)(ptr + sizeof(uint32_t));
	return string_t(str_ptr, str_length);
}

void UncompressedStringStorage::WriteStringMarker(data_ptr_t target, block_id_t block_id, int32_t offset) {
	memcpy(target, &block_id, sizeof(block_id_t));
	target += sizeof(block_id_t);
	memcpy(target, &offset, sizeof(int32_t));
}

void UncompressedStringStorage::ReadStringMarker(data_ptr_t target, block_id_t &block_id, int32_t &offset) {
	memcpy(&block_id, target, sizeof(block_id_t));
	target += sizeof(block_id_t);
	memcpy(&offset, target, sizeof(int32_t));
}

string_location_t UncompressedStringStorage::FetchStringLocation(StringDictionaryContainer dict, data_ptr_t baseptr,
                                                                 int32_t dict_offset) {
	D_ASSERT(dict_offset >= -1 * Storage::BLOCK_SIZE && dict_offset <= Storage::BLOCK_SIZE);
	if (dict_offset < 0) {
		string_location_t result;
		ReadStringMarker(baseptr + dict.end - (-1 * dict_offset), result.block_id, result.offset);
		return result;
	} else {
		return string_location_t(INVALID_BLOCK, dict_offset);
	}
}

string_t UncompressedStringStorage::FetchStringFromDict(ColumnSegment &segment, StringDictionaryContainer dict,
                                                        Vector &result, data_ptr_t baseptr, int32_t dict_offset,
                                                        uint32_t string_length) {
	// fetch base data
	D_ASSERT(dict_offset <= Storage::BLOCK_SIZE);
	string_location_t location = FetchStringLocation(dict, baseptr, dict_offset);
	return FetchString(segment, dict, result, baseptr, location, string_length);
}

string_t UncompressedStringStorage::FetchString(ColumnSegment &segment, StringDictionaryContainer dict, Vector &result,
                                                data_ptr_t baseptr, string_location_t location,
                                                uint32_t string_length) {
	if (location.block_id != INVALID_BLOCK) {
		// big string marker: read from separate block
		return ReadOverflowString(segment, result, location.block_id, location.offset);
	} else {
		if (location.offset == 0) {
			return string_t(nullptr, 0);
		}
		// normal string: read string from this block
		auto dict_end = baseptr + dict.end;
		auto dict_pos = dict_end - location.offset;

		auto str_ptr = (char *)(dict_pos);
		return string_t(str_ptr, string_length);
	}
}

} // namespace duckdb



namespace duckdb {

CompressionFunction UncompressedFun::GetFunction(PhysicalType type) {
	switch (type) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
	case PhysicalType::INT16:
	case PhysicalType::INT32:
	case PhysicalType::INT64:
	case PhysicalType::INT128:
	case PhysicalType::UINT8:
	case PhysicalType::UINT16:
	case PhysicalType::UINT32:
	case PhysicalType::UINT64:
	case PhysicalType::FLOAT:
	case PhysicalType::DOUBLE:
	case PhysicalType::LIST:
	case PhysicalType::INTERVAL:
		return FixedSizeUncompressed::GetFunction(type);
	case PhysicalType::BIT:
		return ValidityUncompressed::GetFunction(type);
	case PhysicalType::VARCHAR:
		return StringUncompressed::GetFunction(type);
	default:
		throw InternalException("Unsupported type for Uncompressed");
	}
}

bool UncompressedFun::TypeIsSupported(PhysicalType type) {
	return true;
}

} // namespace duckdb










namespace duckdb {

//===--------------------------------------------------------------------===//
// Mask constants
//===--------------------------------------------------------------------===//
// LOWER_MASKS contains masks with all the lower bits set until a specific value
// LOWER_MASKS[0] has the 0 lowest bits set, i.e.:
// 0b0000000000000000000000000000000000000000000000000000000000000000,
// LOWER_MASKS[10] has the 10 lowest bits set, i.e.:
// 0b0000000000000000000000000000000000000000000000000000000111111111,
// etc...
// 0b0000000000000000000000000000000000000001111111111111111111111111,
// ...
// 0b0000000000000000000001111111111111111111111111111111111111111111,
// until LOWER_MASKS[64], which has all bits set:
// 0b1111111111111111111111111111111111111111111111111111111111111111
// generated with this python snippet:
// for i in range(65):
//   print(hex(int((64 - i) * '0' + i * '1', 2)) + ",")
const validity_t ValidityUncompressed::LOWER_MASKS[] = {0x0,
                                                        0x1,
                                                        0x3,
                                                        0x7,
                                                        0xf,
                                                        0x1f,
                                                        0x3f,
                                                        0x7f,
                                                        0xff,
                                                        0x1ff,
                                                        0x3ff,
                                                        0x7ff,
                                                        0xfff,
                                                        0x1fff,
                                                        0x3fff,
                                                        0x7fff,
                                                        0xffff,
                                                        0x1ffff,
                                                        0x3ffff,
                                                        0x7ffff,
                                                        0xfffff,
                                                        0x1fffff,
                                                        0x3fffff,
                                                        0x7fffff,
                                                        0xffffff,
                                                        0x1ffffff,
                                                        0x3ffffff,
                                                        0x7ffffff,
                                                        0xfffffff,
                                                        0x1fffffff,
                                                        0x3fffffff,
                                                        0x7fffffff,
                                                        0xffffffff,
                                                        0x1ffffffff,
                                                        0x3ffffffff,
                                                        0x7ffffffff,
                                                        0xfffffffff,
                                                        0x1fffffffff,
                                                        0x3fffffffff,
                                                        0x7fffffffff,
                                                        0xffffffffff,
                                                        0x1ffffffffff,
                                                        0x3ffffffffff,
                                                        0x7ffffffffff,
                                                        0xfffffffffff,
                                                        0x1fffffffffff,
                                                        0x3fffffffffff,
                                                        0x7fffffffffff,
                                                        0xffffffffffff,
                                                        0x1ffffffffffff,
                                                        0x3ffffffffffff,
                                                        0x7ffffffffffff,
                                                        0xfffffffffffff,
                                                        0x1fffffffffffff,
                                                        0x3fffffffffffff,
                                                        0x7fffffffffffff,
                                                        0xffffffffffffff,
                                                        0x1ffffffffffffff,
                                                        0x3ffffffffffffff,
                                                        0x7ffffffffffffff,
                                                        0xfffffffffffffff,
                                                        0x1fffffffffffffff,
                                                        0x3fffffffffffffff,
                                                        0x7fffffffffffffff,
                                                        0xffffffffffffffff};

// UPPER_MASKS contains masks with all the highest bits set until a specific value
// UPPER_MASKS[0] has the 0 highest bits set, i.e.:
// 0b0000000000000000000000000000000000000000000000000000000000000000,
// UPPER_MASKS[10] has the 10 highest bits set, i.e.:
// 0b1111111111110000000000000000000000000000000000000000000000000000,
// etc...
// 0b1111111111111111111111110000000000000000000000000000000000000000,
// ...
// 0b1111111111111111111111111111111111111110000000000000000000000000,
// until UPPER_MASKS[64], which has all bits set:
// 0b1111111111111111111111111111111111111111111111111111111111111111
// generated with this python snippet:
// for i in range(65):
//   print(hex(int(i * '1' + (64 - i) * '0', 2)) + ",")
const validity_t ValidityUncompressed::UPPER_MASKS[] = {0x0,
                                                        0x8000000000000000,
                                                        0xc000000000000000,
                                                        0xe000000000000000,
                                                        0xf000000000000000,
                                                        0xf800000000000000,
                                                        0xfc00000000000000,
                                                        0xfe00000000000000,
                                                        0xff00000000000000,
                                                        0xff80000000000000,
                                                        0xffc0000000000000,
                                                        0xffe0000000000000,
                                                        0xfff0000000000000,
                                                        0xfff8000000000000,
                                                        0xfffc000000000000,
                                                        0xfffe000000000000,
                                                        0xffff000000000000,
                                                        0xffff800000000000,
                                                        0xffffc00000000000,
                                                        0xffffe00000000000,
                                                        0xfffff00000000000,
                                                        0xfffff80000000000,
                                                        0xfffffc0000000000,
                                                        0xfffffe0000000000,
                                                        0xffffff0000000000,
                                                        0xffffff8000000000,
                                                        0xffffffc000000000,
                                                        0xffffffe000000000,
                                                        0xfffffff000000000,
                                                        0xfffffff800000000,
                                                        0xfffffffc00000000,
                                                        0xfffffffe00000000,
                                                        0xffffffff00000000,
                                                        0xffffffff80000000,
                                                        0xffffffffc0000000,
                                                        0xffffffffe0000000,
                                                        0xfffffffff0000000,
                                                        0xfffffffff8000000,
                                                        0xfffffffffc000000,
                                                        0xfffffffffe000000,
                                                        0xffffffffff000000,
                                                        0xffffffffff800000,
                                                        0xffffffffffc00000,
                                                        0xffffffffffe00000,
                                                        0xfffffffffff00000,
                                                        0xfffffffffff80000,
                                                        0xfffffffffffc0000,
                                                        0xfffffffffffe0000,
                                                        0xffffffffffff0000,
                                                        0xffffffffffff8000,
                                                        0xffffffffffffc000,
                                                        0xffffffffffffe000,
                                                        0xfffffffffffff000,
                                                        0xfffffffffffff800,
                                                        0xfffffffffffffc00,
                                                        0xfffffffffffffe00,
                                                        0xffffffffffffff00,
                                                        0xffffffffffffff80,
                                                        0xffffffffffffffc0,
                                                        0xffffffffffffffe0,
                                                        0xfffffffffffffff0,
                                                        0xfffffffffffffff8,
                                                        0xfffffffffffffffc,
                                                        0xfffffffffffffffe,
                                                        0xffffffffffffffff};

//===--------------------------------------------------------------------===//
// Analyze
//===--------------------------------------------------------------------===//
struct ValidityAnalyzeState : public AnalyzeState {
	ValidityAnalyzeState() : count(0) {
	}

	idx_t count;
};

unique_ptr<AnalyzeState> ValidityInitAnalyze(ColumnData &col_data, PhysicalType type) {
	return make_unique<ValidityAnalyzeState>();
}

bool ValidityAnalyze(AnalyzeState &state_p, Vector &input, idx_t count) {
	auto &state = (ValidityAnalyzeState &)state_p;
	state.count += count;
	return true;
}

idx_t ValidityFinalAnalyze(AnalyzeState &state_p) {
	auto &state = (ValidityAnalyzeState &)state_p;
	return (state.count + 7) / 8;
}

//===--------------------------------------------------------------------===//
// Scan
//===--------------------------------------------------------------------===//
struct ValidityScanState : public SegmentScanState {
	unique_ptr<BufferHandle> handle;
};

unique_ptr<SegmentScanState> ValidityInitScan(ColumnSegment &segment) {
	auto result = make_unique<ValidityScanState>();
	auto &buffer_manager = BufferManager::GetBufferManager(segment.db);
	result->handle = buffer_manager.Pin(segment.block);
	return move(result);
}

//===--------------------------------------------------------------------===//
// Scan base data
//===--------------------------------------------------------------------===//
void ValidityScanPartial(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result,
                         idx_t result_offset) {
	auto start = segment.GetRelativeIndex(state.row_index);

	static_assert(sizeof(validity_t) == sizeof(uint64_t), "validity_t should be 64-bit");
	auto &scan_state = (ValidityScanState &)*state.scan_state;

	auto &result_mask = FlatVector::Validity(result);
	auto buffer_ptr = scan_state.handle->node->buffer + segment.GetBlockOffset();
	auto input_data = (validity_t *)buffer_ptr;

#ifdef DEBUG
	// this method relies on all the bits we are going to write to being set to valid
	for (idx_t i = 0; i < scan_count; i++) {
		D_ASSERT(result_mask.RowIsValid(result_offset + i));
	}
#endif
#if STANDARD_VECTOR_SIZE < 128
	// fallback for tiny vector sizes
	// the bitwise ops we use below don't work if the vector size is too small
	ValidityMask source_mask(input_data);
	for (idx_t i = 0; i < scan_count; i++) {
		if (!source_mask.RowIsValid(start + i)) {
			if (result_mask.AllValid()) {
				result_mask.Initialize(MaxValue<idx_t>(STANDARD_VECTOR_SIZE, result_offset + scan_count));
			}
			result_mask.SetInvalid(result_offset + i);
		}
	}
#else
	// the code below does what the fallback code above states, but using bitwise ops:
	auto result_data = (validity_t *)result_mask.GetData();

	// set up the initial positions
	// we need to find the validity_entry to modify, together with the bit-index WITHIN the validity entry
	idx_t result_entry = result_offset / ValidityMask::BITS_PER_VALUE;
	idx_t result_idx = result_offset - result_entry * ValidityMask::BITS_PER_VALUE;

	// same for the input: find the validity_entry we are pulling from, together with the bit-index WITHIN that entry
	idx_t input_entry = start / ValidityMask::BITS_PER_VALUE;
	idx_t input_idx = start - input_entry * ValidityMask::BITS_PER_VALUE;

	// now start the bit games
	idx_t pos = 0;
	while (pos < scan_count) {
		// these are the current validity entries we are dealing with
		idx_t current_result_idx = result_entry;
		idx_t offset;
		validity_t input_mask = input_data[input_entry];

		// construct the mask to AND together with the result
		if (result_idx < input_idx) {
			// we have to shift the input RIGHT if the result_idx is smaller than the input_idx
			auto shift_amount = input_idx - result_idx;
			D_ASSERT(shift_amount > 0 && shift_amount <= ValidityMask::BITS_PER_VALUE);

			input_mask = input_mask >> shift_amount;

			// now the upper "shift_amount" bits are set to 0
			// we need them to be set to 1
			// otherwise the subsequent bitwise & will modify values outside of the range of values we want to alter
			input_mask |= ValidityUncompressed::UPPER_MASKS[shift_amount];

			// after this, we move to the next input_entry
			offset = ValidityMask::BITS_PER_VALUE - input_idx;
			input_entry++;
			input_idx = 0;
			result_idx += offset;
		} else if (result_idx > input_idx) {
			// we have to shift the input LEFT if the result_idx is bigger than the input_idx
			auto shift_amount = result_idx - input_idx;
			D_ASSERT(shift_amount > 0 && shift_amount <= ValidityMask::BITS_PER_VALUE);

			// to avoid overflows, we set the upper "shift_amount" values to 0 first
			input_mask = (input_mask & ~ValidityUncompressed::UPPER_MASKS[shift_amount]) << shift_amount;

			// now the lower "shift_amount" bits are set to 0
			// we need them to be set to 1
			// otherwise the subsequent bitwise & will modify values outside of the range of values we want to alter
			input_mask |= ValidityUncompressed::LOWER_MASKS[shift_amount];

			// after this, we move to the next result_entry
			offset = ValidityMask::BITS_PER_VALUE - result_idx;
			result_entry++;
			result_idx = 0;
			input_idx += offset;
		} else {
			// if the input_idx is equal to result_idx they are already aligned
			// we just move to the next entry for both after this
			offset = ValidityMask::BITS_PER_VALUE - result_idx;
			input_entry++;
			result_entry++;
			result_idx = input_idx = 0;
		}
		// now we need to check if we should include the ENTIRE mask
		// OR if we need to mask from the right side
		pos += offset;
		if (pos > scan_count) {
			// we need to set any bits that are past the scan_count on the right-side to 1
			// this is required so we don't influence any bits that are not part of the scan
			input_mask |= ValidityUncompressed::UPPER_MASKS[pos - scan_count];
		}
		// now finally we can merge the input mask with the result mask
		if (input_mask != ValidityMask::ValidityBuffer::MAX_ENTRY) {
			if (!result_data) {
				result_mask.Initialize(MaxValue<idx_t>(STANDARD_VECTOR_SIZE, result_offset + scan_count));
				result_data = (validity_t *)result_mask.GetData();
			}
			result_data[current_result_idx] &= input_mask;
		}
	}
#endif

#ifdef DEBUG
	// verify that we actually accomplished the bitwise ops equivalent that we wanted to do
	ValidityMask input_mask(input_data);
	for (idx_t i = 0; i < scan_count; i++) {
		D_ASSERT(result_mask.RowIsValid(result_offset + i) == input_mask.RowIsValid(start + i));
	}
#endif
}

void ValidityScan(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result) {
	result.Normalify(scan_count);

	auto start = segment.GetRelativeIndex(state.row_index);
	if (start % ValidityMask::BITS_PER_VALUE == 0) {
		auto &scan_state = (ValidityScanState &)*state.scan_state;

		// aligned scan: no need to do anything fancy
		// note: this is only an optimization which avoids having to do messy bitshifting in the common case
		// it is not required for correctness
		auto &result_mask = FlatVector::Validity(result);
		auto buffer_ptr = scan_state.handle->node->buffer + segment.GetBlockOffset();
		auto input_data = (validity_t *)buffer_ptr;
		auto result_data = (validity_t *)result_mask.GetData();
		idx_t start_offset = start / ValidityMask::BITS_PER_VALUE;
		idx_t entry_scan_count = (scan_count + ValidityMask::BITS_PER_VALUE - 1) / ValidityMask::BITS_PER_VALUE;
		for (idx_t i = 0; i < entry_scan_count; i++) {
			auto input_entry = input_data[start_offset + i];
			if (!result_data && input_entry == ValidityMask::ValidityBuffer::MAX_ENTRY) {
				continue;
			}
			if (!result_data) {
				result_mask.Initialize(MaxValue<idx_t>(STANDARD_VECTOR_SIZE, scan_count));
				result_data = (validity_t *)result_mask.GetData();
			}
			result_data[i] = input_entry;
		}
	} else {
		// unaligned scan: fall back to scan_partial which does bitshift tricks
		ValidityScanPartial(segment, state, scan_count, result, 0);
	}
}

//===--------------------------------------------------------------------===//
// Fetch
//===--------------------------------------------------------------------===//
void ValidityFetchRow(ColumnSegment &segment, ColumnFetchState &state, row_t row_id, Vector &result, idx_t result_idx) {
	D_ASSERT(row_id >= 0 && row_id < row_t(segment.count));
	auto &buffer_manager = BufferManager::GetBufferManager(segment.db);
	auto handle = buffer_manager.Pin(segment.block);
	auto dataptr = handle->node->buffer + segment.GetBlockOffset();
	ValidityMask mask((validity_t *)dataptr);
	auto &result_mask = FlatVector::Validity(result);
	if (!mask.RowIsValidUnsafe(row_id)) {
		result_mask.SetInvalid(result_idx);
	}
}

//===--------------------------------------------------------------------===//
// Append
//===--------------------------------------------------------------------===//
unique_ptr<CompressedSegmentState> ValidityInitSegment(ColumnSegment &segment, block_id_t block_id) {
	auto &buffer_manager = BufferManager::GetBufferManager(segment.db);
	if (block_id == INVALID_BLOCK) {
		auto handle = buffer_manager.Pin(segment.block);
		memset(handle->node->buffer, 0xFF, Storage::BLOCK_SIZE);
	}
	return nullptr;
}

idx_t ValidityAppend(ColumnSegment &segment, SegmentStatistics &stats, VectorData &data, idx_t offset, idx_t vcount) {
	D_ASSERT(segment.GetBlockOffset() == 0);
	auto &validity_stats = (ValidityStatistics &)*stats.statistics;

	auto max_tuples = Storage::BLOCK_SIZE / ValidityMask::STANDARD_MASK_SIZE * STANDARD_VECTOR_SIZE;
	idx_t append_count = MinValue<idx_t>(vcount, max_tuples - segment.count);
	if (data.validity.AllValid()) {
		// no null values: skip append
		segment.count += append_count;
		validity_stats.has_no_null = true;
		return append_count;
	}
	auto &buffer_manager = BufferManager::GetBufferManager(segment.db);
	auto handle = buffer_manager.Pin(segment.block);

	ValidityMask mask((validity_t *)handle->node->buffer);
	for (idx_t i = 0; i < append_count; i++) {
		auto idx = data.sel->get_index(offset + i);
		if (!data.validity.RowIsValidUnsafe(idx)) {
			mask.SetInvalidUnsafe(segment.count + i);
			validity_stats.has_null = true;
		} else {
			validity_stats.has_no_null = true;
		}
	}
	segment.count += append_count;
	return append_count;
}

idx_t ValidityFinalizeAppend(ColumnSegment &segment, SegmentStatistics &stats) {
	return ((segment.count + STANDARD_VECTOR_SIZE - 1) / STANDARD_VECTOR_SIZE) * ValidityMask::STANDARD_MASK_SIZE;
}

void ValidityRevertAppend(ColumnSegment &segment, idx_t start_row) {
	idx_t start_bit = start_row - segment.start;

	auto &buffer_manager = BufferManager::GetBufferManager(segment.db);
	auto handle = buffer_manager.Pin(segment.block);
	idx_t revert_start;
	if (start_bit % 8 != 0) {
		// handle sub-bit stuff (yay)
		idx_t byte_pos = start_bit / 8;
		idx_t bit_start = byte_pos * 8;
		idx_t bit_end = (byte_pos + 1) * 8;
		ValidityMask mask((validity_t *)handle->node->buffer + byte_pos);
		for (idx_t i = start_bit; i < bit_end; i++) {
			mask.SetValid(i - bit_start);
		}
		revert_start = bit_end / 8;
	} else {
		revert_start = start_bit / 8;
	}
	// for the rest, we just memset
	memset(handle->node->buffer + revert_start, 0xFF, Storage::BLOCK_SIZE - revert_start);
}

//===--------------------------------------------------------------------===//
// Get Function
//===--------------------------------------------------------------------===//
CompressionFunction ValidityUncompressed::GetFunction(PhysicalType data_type) {
	D_ASSERT(data_type == PhysicalType::BIT);
	return CompressionFunction(CompressionType::COMPRESSION_UNCOMPRESSED, data_type, ValidityInitAnalyze,
	                           ValidityAnalyze, ValidityFinalAnalyze, UncompressedFunctions::InitCompression,
	                           UncompressedFunctions::Compress, UncompressedFunctions::FinalizeCompress,
	                           ValidityInitScan, ValidityScan, ValidityScanPartial, ValidityFetchRow,
	                           UncompressedFunctions::EmptySkip, ValidityInitSegment, ValidityAppend,
	                           ValidityFinalizeAppend, ValidityRevertAppend);
}

} // namespace duckdb





















namespace duckdb {

DataTable::DataTable(DatabaseInstance &db, const string &schema, const string &table,
                     vector<ColumnDefinition> column_definitions_p, unique_ptr<PersistentTableData> data)
    : info(make_shared<DataTableInfo>(db, schema, table)), column_definitions(move(column_definitions_p)), db(db),
      total_rows(0), is_root(true) {
	// initialize the table with the existing data from disk, if any
	this->row_groups = make_shared<SegmentTree>();
	auto types = GetTypes();
	if (data && !data->row_groups.empty()) {
		for (auto &row_group_pointer : data->row_groups) {
			auto new_row_group = make_unique<RowGroup>(db, *info, types, row_group_pointer);
			auto row_group_count = new_row_group->start + new_row_group->count;
			if (row_group_count > total_rows) {
				total_rows = row_group_count;
			}
			row_groups->AppendSegment(move(new_row_group));
		}
		column_stats.reserve(data->column_stats.size());
		for (auto &stats : data->column_stats) {
			column_stats.push_back(make_shared<ColumnStatistics>(move(stats)));
		}
		if (column_stats.size() != types.size()) { // LCOV_EXCL_START
			throw IOException("Table statistics column count is not aligned with table column count. Corrupt file?");
		} // LCOV_EXCL_STOP
	}
	if (column_stats.empty()) {
		D_ASSERT(total_rows == 0);

		AppendRowGroup(0);
		for (auto &type : types) {
			column_stats.push_back(ColumnStatistics::CreateEmptyStats(type));
		}
	} else {
		D_ASSERT(column_stats.size() == types.size());
		D_ASSERT(row_groups->GetRootSegment() != nullptr);
	}
}

void DataTable::AppendRowGroup(idx_t start_row) {
	auto types = GetTypes();
	auto new_row_group = make_unique<RowGroup>(db, *info, start_row, 0);
	new_row_group->InitializeEmpty(types);
	row_groups->AppendSegment(move(new_row_group));
}

DataTable::DataTable(ClientContext &context, DataTable &parent, ColumnDefinition &new_column, Expression *default_value)
    : info(parent.info), db(parent.db), total_rows(parent.total_rows.load()), is_root(true) {
	for (auto &column_def : parent.column_definitions) {
		column_definitions.emplace_back(column_def.Copy());
	}
	// prevent any new tuples from being added to the parent
	lock_guard<mutex> parent_lock(parent.append_lock);
	// add the new column to this DataTable
	auto new_column_type = new_column.Type();
	auto new_column_idx = parent.column_definitions.size();

	// set up the statistics
	for (idx_t i = 0; i < parent.column_stats.size(); i++) {
		column_stats.push_back(parent.column_stats[i]);
	}
	column_stats.push_back(ColumnStatistics::CreateEmptyStats(new_column_type));

	// add the column definitions from this DataTable
	column_definitions.emplace_back(new_column.Copy());

	auto &transaction = Transaction::GetTransaction(context);

	ExpressionExecutor executor;
	DataChunk dummy_chunk;
	Vector result(new_column_type);
	if (!default_value) {
		FlatVector::Validity(result).SetAllInvalid(STANDARD_VECTOR_SIZE);
	} else {
		executor.AddExpression(*default_value);
	}

	// fill the column with its DEFAULT value, or NULL if none is specified
	auto new_stats = make_unique<SegmentStatistics>(new_column.Type());
	this->row_groups = make_shared<SegmentTree>();
	auto current_row_group = (RowGroup *)parent.row_groups->GetRootSegment();
	while (current_row_group) {
		auto new_row_group = current_row_group->AddColumn(context, new_column, executor, default_value, result);
		// merge in the statistics
		column_stats[new_column_idx]->stats->Merge(*new_row_group->GetStatistics(new_column_idx));

		row_groups->AppendSegment(move(new_row_group));
		current_row_group = (RowGroup *)current_row_group->next.get();
	}

	// also add this column to client local storage
	transaction.storage.AddColumn(&parent, this, new_column, default_value);

	// this table replaces the previous table, hence the parent is no longer the root DataTable
	parent.is_root = false;
}

DataTable::DataTable(ClientContext &context, DataTable &parent, idx_t removed_column)
    : info(parent.info), db(parent.db), total_rows(parent.total_rows.load()), is_root(true) {
	// prevent any new tuples from being added to the parent
	lock_guard<mutex> parent_lock(parent.append_lock);

	for (auto &column_def : parent.column_definitions) {
		column_definitions.emplace_back(column_def.Copy());
	}
	// first check if there are any indexes that exist that point to the removed column
	info->indexes.Scan([&](Index &index) {
		for (auto &column_id : index.column_ids) {
			if (column_id == removed_column) {
				throw CatalogException("Cannot drop this column: an index depends on it!");
			} else if (column_id > removed_column) {
				throw CatalogException("Cannot drop this column: an index depends on a column after it!");
			}
		}
		return false;
	});

	// erase the stats from this DataTable
	for (idx_t i = 0; i < parent.column_stats.size(); i++) {
		if (i != removed_column) {
			column_stats.push_back(parent.column_stats[i]);
		}
	}

	// erase the column definitions from this DataTable
	D_ASSERT(removed_column < column_definitions.size());
	column_definitions.erase(column_definitions.begin() + removed_column);

	storage_t storage_idx = 0;
	for (idx_t i = 0; i < column_definitions.size(); i++) {
		auto &col = column_definitions[i];
		col.SetOid(i);
		if (col.Generated()) {
			continue;
		}
		col.SetStorageOid(storage_idx++);
	}

	// alter the row_groups and remove the column from each of them
	this->row_groups = make_shared<SegmentTree>();
	auto current_row_group = (RowGroup *)parent.row_groups->GetRootSegment();
	while (current_row_group) {
		auto new_row_group = current_row_group->RemoveColumn(removed_column);
		row_groups->AppendSegment(move(new_row_group));
		current_row_group = (RowGroup *)current_row_group->next.get();
	}

	// this table replaces the previous table, hence the parent is no longer the root DataTable
	parent.is_root = false;
}

DataTable::DataTable(ClientContext &context, DataTable &parent, idx_t changed_idx, const LogicalType &target_type,
                     vector<column_t> bound_columns, Expression &cast_expr)
    : info(parent.info), db(parent.db), total_rows(parent.total_rows.load()), is_root(true) {
	// prevent any tuples from being added to the parent
	lock_guard<mutex> lock(append_lock);
	for (auto &column_def : parent.column_definitions) {
		column_definitions.emplace_back(column_def.Copy());
	}
	// first check if there are any indexes that exist that point to the changed column
	info->indexes.Scan([&](Index &index) {
		for (auto &column_id : index.column_ids) {
			if (column_id == changed_idx) {
				throw CatalogException("Cannot change the type of this column: an index depends on it!");
			}
		}
		return false;
	});

	// change the type in this DataTable
	column_definitions[changed_idx].SetType(target_type);

	// set up the statistics for the table
	// the column that had its type changed will have the new statistics computed during conversion
	for (idx_t i = 0; i < column_definitions.size(); i++) {
		if (i == changed_idx) {
			column_stats.push_back(ColumnStatistics::CreateEmptyStats(column_definitions[i].Type()));
		} else {
			column_stats.push_back(parent.column_stats[i]);
		}
	}

	// scan the original table, and fill the new column with the transformed value
	auto &transaction = Transaction::GetTransaction(context);

	vector<LogicalType> scan_types;
	for (idx_t i = 0; i < bound_columns.size(); i++) {
		if (bound_columns[i] == COLUMN_IDENTIFIER_ROW_ID) {
			scan_types.emplace_back(LogicalType::ROW_TYPE);
		} else {
			scan_types.push_back(parent.column_definitions[bound_columns[i]].Type());
		}
	}
	DataChunk scan_chunk;
	scan_chunk.Initialize(scan_types);

	ExpressionExecutor executor;
	executor.AddExpression(cast_expr);

	TableScanState scan_state;
	scan_state.column_ids = bound_columns;
	scan_state.max_row = total_rows;

	// now alter the type of the column within all of the row_groups individually
	this->row_groups = make_shared<SegmentTree>();
	auto current_row_group = (RowGroup *)parent.row_groups->GetRootSegment();
	while (current_row_group) {
		auto new_row_group =
		    current_row_group->AlterType(context, target_type, changed_idx, executor, scan_state, scan_chunk);
		column_stats[changed_idx]->stats->Merge(*new_row_group->GetStatistics(changed_idx));
		row_groups->AppendSegment(move(new_row_group));
		current_row_group = (RowGroup *)current_row_group->next.get();
	}

	transaction.storage.ChangeType(&parent, this, changed_idx, target_type, bound_columns, cast_expr);

	// this table replaces the previous table, hence the parent is no longer the root DataTable
	parent.is_root = false;
}

vector<LogicalType> DataTable::GetTypes() {
	vector<LogicalType> types;
	for (auto &it : column_definitions) {
		types.push_back(it.Type());
	}
	return types;
}

//===--------------------------------------------------------------------===//
// Scan
//===--------------------------------------------------------------------===//
void DataTable::InitializeScan(TableScanState &state, const vector<column_t> &column_ids,
                               TableFilterSet *table_filters) {
	// initialize a column scan state for each column
	// initialize the chunk scan state
	auto row_group = (RowGroup *)row_groups->GetRootSegment();
	state.column_ids = column_ids;
	state.max_row = total_rows;
	state.table_filters = table_filters;
	if (table_filters) {
		D_ASSERT(!table_filters->filters.empty());
		state.adaptive_filter = make_unique<AdaptiveFilter>(table_filters);
	}
	while (row_group && !row_group->InitializeScan(state.row_group_scan_state)) {
		row_group = (RowGroup *)row_group->next.get();
	}
}

void DataTable::InitializeScan(Transaction &transaction, TableScanState &state, const vector<column_t> &column_ids,
                               TableFilterSet *table_filters) {
	InitializeScan(state, column_ids, table_filters);
	transaction.storage.InitializeScan(this, state.local_state, table_filters);
}

void DataTable::InitializeScanWithOffset(TableScanState &state, const vector<column_t> &column_ids, idx_t start_row,
                                         idx_t end_row) {

	auto row_group = (RowGroup *)row_groups->GetSegment(start_row);
	state.column_ids = column_ids;
	state.max_row = end_row;
	state.table_filters = nullptr;
	idx_t start_vector = (start_row - row_group->start) / STANDARD_VECTOR_SIZE;
	if (!row_group->InitializeScanWithOffset(state.row_group_scan_state, start_vector)) {
		throw InternalException("Failed to initialize row group scan with offset");
	}
}

bool DataTable::InitializeScanInRowGroup(TableScanState &state, const vector<column_t> &column_ids,
                                         TableFilterSet *table_filters, RowGroup *row_group, idx_t vector_index,
                                         idx_t max_row) {
	state.column_ids = column_ids;
	state.max_row = max_row;
	state.table_filters = table_filters;
	if (table_filters) {
		D_ASSERT(!table_filters->filters.empty());
		state.adaptive_filter = make_unique<AdaptiveFilter>(table_filters);
	}
	return row_group->InitializeScanWithOffset(state.row_group_scan_state, vector_index);
}

idx_t DataTable::MaxThreads(ClientContext &context) {
	idx_t parallel_scan_vector_count = RowGroup::ROW_GROUP_VECTOR_COUNT;
	if (ClientConfig::GetConfig(context).verify_parallelism) {
		parallel_scan_vector_count = 1;
	}
	idx_t parallel_scan_tuple_count = STANDARD_VECTOR_SIZE * parallel_scan_vector_count;

	return total_rows / parallel_scan_tuple_count + 1;
}

void DataTable::InitializeParallelScan(ClientContext &context, ParallelTableScanState &state) {
	state.current_row_group = (RowGroup *)row_groups->GetRootSegment();
	state.transaction_local_data = false;
	// figure out the max row we can scan for both the regular and the transaction-local storage
	state.max_row = total_rows;
	state.vector_index = 0;
	state.local_state.max_index = 0;
	auto &transaction = Transaction::GetTransaction(context);
	transaction.storage.InitializeScan(this, state.local_state, nullptr);
}

bool DataTable::NextParallelScan(ClientContext &context, ParallelTableScanState &state, TableScanState &scan_state,
                                 const vector<column_t> &column_ids) {
	while (state.current_row_group) {
		idx_t vector_index;
		idx_t max_row;
		if (ClientConfig::GetConfig(context).verify_parallelism) {
			vector_index = state.vector_index;
			max_row = state.current_row_group->start +
			          MinValue<idx_t>(state.current_row_group->count,
			                          STANDARD_VECTOR_SIZE * state.vector_index + STANDARD_VECTOR_SIZE);
			D_ASSERT(vector_index * STANDARD_VECTOR_SIZE < state.current_row_group->count);
		} else {
			vector_index = 0;
			max_row = state.current_row_group->start + state.current_row_group->count;
		}
		max_row = MinValue<idx_t>(max_row, state.max_row);
		bool need_to_scan;
		if (state.current_row_group->count == 0) {
			need_to_scan = false;
		} else {
			need_to_scan = InitializeScanInRowGroup(scan_state, column_ids, scan_state.table_filters,
			                                        state.current_row_group, vector_index, max_row);
		}
		if (ClientConfig::GetConfig(context).verify_parallelism) {
			state.vector_index++;
			if (state.vector_index * STANDARD_VECTOR_SIZE >= state.current_row_group->count) {
				state.current_row_group = (RowGroup *)state.current_row_group->next.get();
				state.vector_index = 0;
			}
		} else {
			state.current_row_group = (RowGroup *)state.current_row_group->next.get();
		}
		if (!need_to_scan) {
			// filters allow us to skip this row group: move to the next row group
			continue;
		}
		return true;
	}
	if (!state.transaction_local_data) {
		auto &transaction = Transaction::GetTransaction(context);
		// create a task for scanning the local data
		scan_state.row_group_scan_state.max_row = 0;
		scan_state.max_row = 0;
		transaction.storage.InitializeScan(this, scan_state.local_state, scan_state.table_filters);
		scan_state.local_state.max_index = state.local_state.max_index;
		scan_state.local_state.last_chunk_count = state.local_state.last_chunk_count;
		state.transaction_local_data = true;
		return true;
	} else {
		// finished all scans: no more scans remaining
		return false;
	}
}

void DataTable::Scan(Transaction &transaction, DataChunk &result, TableScanState &state, vector<column_t> &column_ids) {
	// scan the persistent segments
	if (ScanBaseTable(transaction, result, state)) {
		D_ASSERT(result.size() > 0);
		return;
	}

	// scan the transaction-local segments
	transaction.storage.Scan(state.local_state, column_ids, result);
}

bool DataTable::ScanBaseTable(Transaction &transaction, DataChunk &result, TableScanState &state) {
	auto current_row_group = state.row_group_scan_state.row_group;
	while (current_row_group) {
		current_row_group->Scan(transaction, state.row_group_scan_state, result);
		if (result.size() > 0) {
			return true;
		} else {
			do {
				current_row_group = state.row_group_scan_state.row_group = (RowGroup *)current_row_group->next.get();
				if (current_row_group) {
					bool scan_row_group = current_row_group->InitializeScan(state.row_group_scan_state);
					if (scan_row_group) {
						// skip this row group
						break;
					}
				}
			} while (current_row_group);
		}
	}
	return false;
}

//===--------------------------------------------------------------------===//
// Fetch
//===--------------------------------------------------------------------===//
void DataTable::Fetch(Transaction &transaction, DataChunk &result, const vector<column_t> &column_ids,
                      Vector &row_identifiers, idx_t fetch_count, ColumnFetchState &state) {
	// figure out which row_group to fetch from
	auto row_ids = FlatVector::GetData<row_t>(row_identifiers);
	idx_t count = 0;
	for (idx_t i = 0; i < fetch_count; i++) {
		auto row_id = row_ids[i];
		auto row_group = (RowGroup *)row_groups->GetSegment(row_id);
		if (!row_group->Fetch(transaction, row_id - row_group->start)) {
			continue;
		}
		row_group->FetchRow(transaction, state, column_ids, row_id, result, count);
		count++;
	}
	result.SetCardinality(count);
}

//===--------------------------------------------------------------------===//
// Append
//===--------------------------------------------------------------------===//
static void VerifyNotNullConstraint(TableCatalogEntry &table, Vector &vector, idx_t count, const string &col_name) {
	if (VectorOperations::HasNull(vector, count)) {
		throw ConstraintException("NOT NULL constraint failed: %s.%s", table.name, col_name);
	}
}

// To avoid throwing an error at SELECT, instead this moves the error detection to INSERT
static void VerifyGeneratedExpressionSuccess(TableCatalogEntry &table, DataChunk &chunk, Expression &expr,
                                             column_t index) {
	auto &col = table.columns[index];
	D_ASSERT(col.Generated());
	ExpressionExecutor executor(expr);
	Vector result(col.Type());
	try {
		executor.ExecuteExpression(chunk, result);
	} catch (std::exception &ex) {
		throw ConstraintException("Incorrect %s value for generated column \"%s\"", col.Type().ToString(), col.Name());
	}
}

static void VerifyCheckConstraint(TableCatalogEntry &table, Expression &expr, DataChunk &chunk) {
	ExpressionExecutor executor(expr);
	Vector result(LogicalType::INTEGER);
	try {
		executor.ExecuteExpression(chunk, result);
	} catch (std::exception &ex) {
		throw ConstraintException("CHECK constraint failed: %s (Error: %s)", table.name, ex.what());
	} catch (...) { // LCOV_EXCL_START
		throw ConstraintException("CHECK constraint failed: %s (Unknown Error)", table.name);
	} // LCOV_EXCL_STOP
	VectorData vdata;
	result.Orrify(chunk.size(), vdata);

	auto dataptr = (int32_t *)vdata.data;
	for (idx_t i = 0; i < chunk.size(); i++) {
		auto idx = vdata.sel->get_index(i);
		if (vdata.validity.RowIsValid(idx) && dataptr[idx] == 0) {
			throw ConstraintException("CHECK constraint failed: %s", table.name);
		}
	}
}

static bool IsForeignKeyIndex(const vector<idx_t> &fk_keys, Index &index, ForeignKeyType fk_type) {
	if (fk_type == ForeignKeyType::FK_TYPE_PRIMARY_KEY_TABLE ? !index.IsUnique() : !index.IsForeign()) {
		return false;
	}
	if (fk_keys.size() != index.column_ids.size()) {
		return false;
	}
	for (auto &fk_key : fk_keys) {
		bool is_found = false;
		for (auto &index_key : index.column_ids) {
			if (fk_key == index_key) {
				is_found = true;
				break;
			}
		}
		if (!is_found) {
			return false;
		}
	}
	return true;
}

Index *TableIndexList::FindForeignKeyIndex(const vector<idx_t> &fk_keys, ForeignKeyType fk_type) {
	Index *result = nullptr;
	Scan([&](Index &index) {
		if (IsForeignKeyIndex(fk_keys, index, fk_type)) {
			result = &index;
		}
		return false;
	});
	return result;
}

static void VerifyForeignKeyConstraint(const BoundForeignKeyConstraint &bfk, ClientContext &context, DataChunk &chunk,
                                       bool is_append) {
	const vector<idx_t> *src_keys_ptr = &bfk.info.fk_keys;
	const vector<idx_t> *dst_keys_ptr = &bfk.info.pk_keys;
	if (!is_append) {
		src_keys_ptr = &bfk.info.pk_keys;
		dst_keys_ptr = &bfk.info.fk_keys;
	}

	auto table_entry_ptr =
	    Catalog::GetCatalog(context).GetEntry<TableCatalogEntry>(context, bfk.info.schema, bfk.info.table);
	if (table_entry_ptr == nullptr) {
		throw InternalException("Can't find table \"%s\" in foreign key constraint", bfk.info.table);
	}

	// make the data chunk to check
	vector<LogicalType> types;
	for (idx_t i = 0; i < table_entry_ptr->columns.size(); i++) {
		types.emplace_back(table_entry_ptr->columns[i].Type());
	}
	DataChunk dst_chunk;
	dst_chunk.InitializeEmpty(types);
	for (idx_t i = 0; i < src_keys_ptr->size(); i++) {
		dst_chunk.data[(*dst_keys_ptr)[i]].Reference(chunk.data[(*src_keys_ptr)[i]]);
	}
	dst_chunk.SetCardinality(chunk.size());
	auto data_table = table_entry_ptr->storage.get();

	idx_t count = dst_chunk.size();
	if (count <= 0) {
		return;
	}

	// we need to look at the error messages concurrently in data table's index and transaction local storage's index
	vector<string> err_msgs, tran_err_msgs;
	err_msgs.resize(count);
	tran_err_msgs.resize(count);

	auto fk_type = is_append ? ForeignKeyType::FK_TYPE_PRIMARY_KEY_TABLE : ForeignKeyType::FK_TYPE_FOREIGN_KEY_TABLE;
	// check whether or not the chunk can be inserted or deleted into the referenced table' storage
	auto index = data_table->info->indexes.FindForeignKeyIndex(*dst_keys_ptr, fk_type);
	if (!index) {
		throw InternalException("Internal Foreign Key error: could not find index to verify...");
	}
	if (is_append) {
		index->VerifyAppendForeignKey(dst_chunk, err_msgs.data());
	} else {
		index->VerifyDeleteForeignKey(dst_chunk, err_msgs.data());
	}
	// check whether or not the chunk can be inserted or deleted into the referenced table' transaction local storage
	auto &transaction = Transaction::GetTransaction(context);
	bool transaction_check = transaction.storage.Find(data_table);
	if (transaction_check) {
		vector<unique_ptr<Index>> &transact_index_vec = transaction.storage.GetIndexes(data_table);
		for (idx_t i = 0; i < transact_index_vec.size(); i++) {
			if (IsForeignKeyIndex(*dst_keys_ptr, *transact_index_vec[i], fk_type)) {
				if (is_append) {
					transact_index_vec[i]->VerifyAppendForeignKey(dst_chunk, tran_err_msgs.data());
				} else {
					transact_index_vec[i]->VerifyDeleteForeignKey(dst_chunk, tran_err_msgs.data());
				}
			}
		}
	}

	// we need to look at the error messages concurrently in data table's index and transaction local storage's index
	for (idx_t i = 0; i < count; i++) {
		if (!transaction_check) {
			// if there is no transaction-local data we only need to check if there is an error message in the main
			// index
			if (!err_msgs[i].empty()) {
				throw ConstraintException(err_msgs[i]);
			} else {
				continue;
			}
		}
		if (is_append) {
			// if we are appending we need to check to ensure the foreign key exists in either the transaction-local
			// storage or the main table
			if (!err_msgs[i].empty() && !tran_err_msgs[i].empty()) {
				throw ConstraintException(err_msgs[i]);
			} else {
				continue;
			}
		}
		// if we are deleting we need to ensure the foreign key DOES NOT exist in EITHER the transaction-local storage
		// OR the main table
		if (!err_msgs[i].empty() || !tran_err_msgs[i].empty()) {
			string &err_msg = err_msgs[i];
			if (err_msg.empty()) {
				err_msg = tran_err_msgs[i];
			}
			throw ConstraintException(err_msg);
		}
	}
}

static void VerifyAppendForeignKeyConstraint(const BoundForeignKeyConstraint &bfk, ClientContext &context,
                                             DataChunk &chunk) {
	VerifyForeignKeyConstraint(bfk, context, chunk, true);
}

static void VerifyDeleteForeignKeyConstraint(const BoundForeignKeyConstraint &bfk, ClientContext &context,
                                             DataChunk &chunk) {
	VerifyForeignKeyConstraint(bfk, context, chunk, false);
}

void DataTable::VerifyAppendConstraints(TableCatalogEntry &table, ClientContext &context, DataChunk &chunk) {
	auto binder = Binder::CreateBinder(context);
	auto bound_columns = unordered_set<column_t>();
	CheckBinder generated_check_binder(*binder, context, table.name, table.columns, bound_columns);
	for (idx_t i = 0; i < table.columns.size(); i++) {
		auto &col = table.columns[i];
		if (!col.Generated()) {
			continue;
		}
		D_ASSERT(col.Type().id() != LogicalTypeId::ANY);
		generated_check_binder.target_type = col.Type();
		auto to_be_bound_expression = col.GeneratedExpression().Copy();
		auto bound_expression = generated_check_binder.Bind(to_be_bound_expression);
		VerifyGeneratedExpressionSuccess(table, chunk, *bound_expression, i);
	}
	for (idx_t i = 0; i < table.bound_constraints.size(); i++) {
		auto &base_constraint = table.constraints[i];
		auto &constraint = table.bound_constraints[i];
		switch (base_constraint->type) {
		case ConstraintType::NOT_NULL: {
			auto &not_null = *reinterpret_cast<BoundNotNullConstraint *>(constraint.get());
			VerifyNotNullConstraint(table, chunk.data[not_null.index], chunk.size(),
			                        table.columns[not_null.index].Name());
			break;
		}
		case ConstraintType::CHECK: {
			auto &check = *reinterpret_cast<BoundCheckConstraint *>(constraint.get());
			VerifyCheckConstraint(table, *check.expression, chunk);
			break;
		}
		case ConstraintType::UNIQUE: {
			//! check whether or not the chunk can be inserted into the indexes
			info->indexes.Scan([&](Index &index) {
				index.VerifyAppend(chunk);
				return false;
			});
			break;
		}
		case ConstraintType::FOREIGN_KEY: {
			auto &bfk = *reinterpret_cast<BoundForeignKeyConstraint *>(constraint.get());
			if (bfk.info.type == ForeignKeyType::FK_TYPE_FOREIGN_KEY_TABLE ||
			    bfk.info.type == ForeignKeyType::FK_TYPE_SELF_REFERENCE_TABLE) {
				VerifyAppendForeignKeyConstraint(bfk, context, chunk);
			}
			break;
		}
		default:
			throw NotImplementedException("Constraint type not implemented!");
		}
	}
}

void DataTable::Append(TableCatalogEntry &table, ClientContext &context, DataChunk &chunk) {
	if (chunk.size() == 0) {
		return;
	}
	// FIXME: could be an assertion instead?
	if (chunk.ColumnCount() != table.StandardColumnCount()) {
		throw InternalException("Mismatch in column count for append");
	}
	if (!is_root) {
		throw TransactionException("Transaction conflict: adding entries to a table that has been altered!");
	}

	chunk.Verify();

	// verify any constraints on the new chunk
	VerifyAppendConstraints(table, context, chunk);

	// append to the transaction local data
	auto &transaction = Transaction::GetTransaction(context);
	transaction.storage.Append(this, chunk);
}

void DataTable::InitializeAppend(Transaction &transaction, TableAppendState &state, idx_t append_count) {
	// obtain the append lock for this table
	state.append_lock = unique_lock<mutex>(append_lock);
	if (!is_root) {
		throw TransactionException("Transaction conflict: adding entries to a table that has been altered!");
	}
	state.row_start = total_rows;
	state.current_row = state.row_start;
	state.remaining_append_count = append_count;

	// start writing to the row_groups
	lock_guard<mutex> row_group_lock(row_groups->node_lock);
	auto last_row_group = (RowGroup *)row_groups->GetLastSegment();
	D_ASSERT(total_rows == last_row_group->start + last_row_group->count);
	last_row_group->InitializeAppend(transaction, state.row_group_append_state, state.remaining_append_count);
	total_rows += append_count;
}

void DataTable::Append(Transaction &transaction, DataChunk &chunk, TableAppendState &state) {
	D_ASSERT(is_root);
	D_ASSERT(chunk.ColumnCount() == column_definitions.size());
	chunk.Verify();

	idx_t append_count = chunk.size();
	idx_t remaining = chunk.size();
	while (true) {
		auto current_row_group = state.row_group_append_state.row_group;
		// check how much we can fit into the current row_group
		idx_t append_count =
		    MinValue<idx_t>(remaining, RowGroup::ROW_GROUP_SIZE - state.row_group_append_state.offset_in_row_group);
		if (append_count > 0) {
			current_row_group->Append(state.row_group_append_state, chunk, append_count);
			// merge the stats
			lock_guard<mutex> stats_guard(stats_lock);
			for (idx_t i = 0; i < column_definitions.size(); i++) {
				column_stats[i]->stats->Merge(*current_row_group->GetStatistics(i));
			}
		}
		state.remaining_append_count -= append_count;
		remaining -= append_count;
		if (remaining > 0) {
			// we expect max 1 iteration of this loop (i.e. a single chunk should never overflow more than one
			// row_group)
			D_ASSERT(chunk.size() == remaining + append_count);
			// slice the input chunk
			if (remaining < chunk.size()) {
				SelectionVector sel(STANDARD_VECTOR_SIZE);
				for (idx_t i = 0; i < remaining; i++) {
					sel.set_index(i, append_count + i);
				}
				chunk.Slice(sel, remaining);
			}
			// append a new row_group
			AppendRowGroup(current_row_group->start + current_row_group->count);
			// set up the append state for this row_group
			lock_guard<mutex> row_group_lock(row_groups->node_lock);
			auto last_row_group = (RowGroup *)row_groups->GetLastSegment();
			last_row_group->InitializeAppend(transaction, state.row_group_append_state, state.remaining_append_count);
			continue;
		} else {
			break;
		}
	}
	state.current_row += append_count;
	for (idx_t col_idx = 0; col_idx < column_stats.size(); col_idx++) {
		auto type = chunk.data[col_idx].GetType().InternalType();
		if (type == PhysicalType::LIST || type == PhysicalType::STRUCT) {
			continue;
		}
		column_stats[col_idx]->stats->UpdateDistinctStatistics(chunk.data[col_idx], chunk.size());
	}
}

void DataTable::ScanTableSegment(idx_t row_start, idx_t count, const std::function<void(DataChunk &chunk)> &function) {
	idx_t end = row_start + count;

	vector<column_t> column_ids;
	vector<LogicalType> types;
	for (idx_t i = 0; i < this->column_definitions.size(); i++) {
		auto &col = this->column_definitions[i];
		column_ids.push_back(i);
		types.push_back(col.Type());
	}
	DataChunk chunk;
	chunk.Initialize(types);

	CreateIndexScanState state;

	idx_t row_start_aligned = row_start / STANDARD_VECTOR_SIZE * STANDARD_VECTOR_SIZE;
	InitializeScanWithOffset(state, column_ids, row_start_aligned, row_start + count);

	idx_t current_row = row_start_aligned;
	while (current_row < end) {
		ScanCreateIndex(state, chunk, TableScanType::TABLE_SCAN_COMMITTED_ROWS);
		if (chunk.size() == 0) {
			break;
		}
		idx_t end_row = current_row + chunk.size();
		// figure out if we need to write the entire chunk or just part of it
		idx_t chunk_start = MaxValue<idx_t>(current_row, row_start);
		idx_t chunk_end = MinValue<idx_t>(end_row, end);
		D_ASSERT(chunk_start < chunk_end);
		idx_t chunk_count = chunk_end - chunk_start;
		if (chunk_count != chunk.size()) {
			// need to slice the chunk before insert
			auto start_in_chunk = chunk_start % STANDARD_VECTOR_SIZE;
			SelectionVector sel(start_in_chunk, chunk_count);
			chunk.Slice(sel, chunk_count);
			chunk.Verify();
		}
		function(chunk);
		chunk.Reset();
		current_row = end_row;
	}
}

void DataTable::WriteToLog(WriteAheadLog &log, idx_t row_start, idx_t count) {
	log.WriteSetTable(info->schema, info->table);
	ScanTableSegment(row_start, count, [&](DataChunk &chunk) { log.WriteInsert(chunk); });
}

void DataTable::CommitAppend(transaction_t commit_id, idx_t row_start, idx_t count) {
	lock_guard<mutex> lock(append_lock);

	auto row_group = (RowGroup *)row_groups->GetSegment(row_start);
	idx_t current_row = row_start;
	idx_t remaining = count;
	while (true) {
		idx_t start_in_row_group = current_row - row_group->start;
		idx_t append_count = MinValue<idx_t>(row_group->count - start_in_row_group, remaining);

		row_group->CommitAppend(commit_id, start_in_row_group, append_count);

		current_row += append_count;
		remaining -= append_count;
		if (remaining == 0) {
			break;
		}
		row_group = (RowGroup *)row_group->next.get();
	}
	info->cardinality += count;
}

void DataTable::RevertAppendInternal(idx_t start_row, idx_t count) {
	if (count == 0) {
		// nothing to revert!
		return;
	}
	if (total_rows != start_row + count) {
		// interleaved append: don't do anything
		// in this case the rows will stay as "inserted by transaction X", but will never be committed
		// they will never be used by any other transaction and will essentially leave a gap
		// this situation is rare, and as such we don't care about optimizing it (yet?)
		// it only happens if C1 appends a lot of data -> C2 appends a lot of data -> C1 rolls back
		return;
	}
	// adjust the cardinality
	info->cardinality = start_row;
	total_rows = start_row;
	D_ASSERT(is_root);
	// revert appends made to row_groups
	lock_guard<mutex> tree_lock(row_groups->node_lock);
	// find the segment index that the current row belongs to
	idx_t segment_index = row_groups->GetSegmentIndex(start_row);
	auto segment = row_groups->nodes[segment_index].node;
	auto &info = (RowGroup &)*segment;

	// remove any segments AFTER this segment: they should be deleted entirely
	if (segment_index < row_groups->nodes.size() - 1) {
		row_groups->nodes.erase(row_groups->nodes.begin() + segment_index + 1, row_groups->nodes.end());
	}
	info.next = nullptr;
	info.RevertAppend(start_row);
}

void DataTable::RevertAppend(idx_t start_row, idx_t count) {
	lock_guard<mutex> lock(append_lock);

	if (!info->indexes.Empty()) {
		idx_t current_row_base = start_row;
		row_t row_data[STANDARD_VECTOR_SIZE];
		Vector row_identifiers(LogicalType::ROW_TYPE, (data_ptr_t)row_data);
		ScanTableSegment(start_row, count, [&](DataChunk &chunk) {
			for (idx_t i = 0; i < chunk.size(); i++) {
				row_data[i] = current_row_base + i;
			}
			info->indexes.Scan([&](Index &index) {
				index.Delete(chunk, row_identifiers);
				return false;
			});
			current_row_base += chunk.size();
		});
	}
	RevertAppendInternal(start_row, count);
}

//===--------------------------------------------------------------------===//
// Indexes
//===--------------------------------------------------------------------===//
bool DataTable::AppendToIndexes(TableAppendState &state, DataChunk &chunk, row_t row_start) {
	D_ASSERT(is_root);
	if (info->indexes.Empty()) {
		return true;
	}
	// first generate the vector of row identifiers
	Vector row_identifiers(LogicalType::ROW_TYPE);
	VectorOperations::GenerateSequence(row_identifiers, chunk.size(), row_start, 1);

	vector<Index *> already_appended;
	bool append_failed = false;
	// now append the entries to the indices
	info->indexes.Scan([&](Index &index) {
		if (!index.Append(chunk, row_identifiers)) {
			append_failed = true;
			return true;
		}
		already_appended.push_back(&index);
		return false;
	});

	if (append_failed) {
		// constraint violation!
		// remove any appended entries from previous indexes (if any)

		for (auto *index : already_appended) {
			index->Delete(chunk, row_identifiers);
		}

		return false;
	}
	return true;
}

void DataTable::RemoveFromIndexes(TableAppendState &state, DataChunk &chunk, row_t row_start) {
	D_ASSERT(is_root);
	if (info->indexes.Empty()) {
		return;
	}
	// first generate the vector of row identifiers
	Vector row_identifiers(LogicalType::ROW_TYPE);
	VectorOperations::GenerateSequence(row_identifiers, chunk.size(), row_start, 1);

	// now remove the entries from the indices
	RemoveFromIndexes(state, chunk, row_identifiers);
}

void DataTable::RemoveFromIndexes(TableAppendState &state, DataChunk &chunk, Vector &row_identifiers) {
	D_ASSERT(is_root);
	info->indexes.Scan([&](Index &index) {
		index.Delete(chunk, row_identifiers);
		return false;
	});
}

void DataTable::RemoveFromIndexes(Vector &row_identifiers, idx_t count) {
	D_ASSERT(is_root);
	auto row_ids = FlatVector::GetData<row_t>(row_identifiers);

	// figure out which row_group to fetch from
	auto row_group = (RowGroup *)row_groups->GetSegment(row_ids[0]);
	auto row_group_vector_idx = (row_ids[0] - row_group->start) / STANDARD_VECTOR_SIZE;
	auto base_row_id = row_group_vector_idx * STANDARD_VECTOR_SIZE + row_group->start;

	// create a selection vector from the row_ids
	SelectionVector sel(STANDARD_VECTOR_SIZE);
	for (idx_t i = 0; i < count; i++) {
		auto row_in_vector = row_ids[i] - base_row_id;
		D_ASSERT(row_in_vector < STANDARD_VECTOR_SIZE);
		sel.set_index(i, row_in_vector);
	}

	// now fetch the columns from that row_group
	// FIXME: we do not need to fetch all columns, only the columns required by the indices!
	TableScanState state;
	state.max_row = total_rows;
	auto types = GetTypes();
	for (idx_t i = 0; i < types.size(); i++) {
		state.column_ids.push_back(i);
	}
	DataChunk result;
	result.Initialize(types);

	row_group->InitializeScanWithOffset(state.row_group_scan_state, row_group_vector_idx);
	row_group->ScanCommitted(state.row_group_scan_state, result,
	                         TableScanType::TABLE_SCAN_COMMITTED_ROWS_DISALLOW_UPDATES);
	result.Slice(sel, count);

	info->indexes.Scan([&](Index &index) {
		index.Delete(result, row_identifiers);
		return false;
	});
}

void DataTable::VerifyDeleteConstraints(TableCatalogEntry &table, ClientContext &context, DataChunk &chunk) {
	for (auto &constraint : table.bound_constraints) {
		switch (constraint->type) {
		case ConstraintType::NOT_NULL:
		case ConstraintType::CHECK:
		case ConstraintType::UNIQUE:
			break;
		case ConstraintType::FOREIGN_KEY: {
			auto &bfk = *reinterpret_cast<BoundForeignKeyConstraint *>(constraint.get());
			if (bfk.info.type == ForeignKeyType::FK_TYPE_PRIMARY_KEY_TABLE ||
			    bfk.info.type == ForeignKeyType::FK_TYPE_SELF_REFERENCE_TABLE) {
				VerifyDeleteForeignKeyConstraint(bfk, context, chunk);
			}
			break;
		}
		default:
			throw NotImplementedException("Constraint type not implemented!");
		}
	}
}

//===--------------------------------------------------------------------===//
// Delete
//===--------------------------------------------------------------------===//
idx_t DataTable::Delete(TableCatalogEntry &table, ClientContext &context, Vector &row_identifiers, idx_t count) {
	D_ASSERT(row_identifiers.GetType().InternalType() == ROW_TYPE);
	if (count == 0) {
		return 0;
	}

	auto &transaction = Transaction::GetTransaction(context);

	row_identifiers.Normalify(count);
	auto ids = FlatVector::GetData<row_t>(row_identifiers);
	auto first_id = ids[0];

	// verify any constraints on the delete rows
	DataChunk verify_chunk;
	if (first_id >= MAX_ROW_ID) {
		transaction.storage.FetchChunk(this, row_identifiers, count, verify_chunk);
	} else {
		ColumnFetchState fetch_state;
		vector<column_t> col_ids;
		vector<LogicalType> types;
		for (idx_t i = 0; i < column_definitions.size(); i++) {
			col_ids.push_back(column_definitions[i].StorageOid());
			types.emplace_back(column_definitions[i].Type());
		}
		verify_chunk.Initialize(types);
		Fetch(transaction, verify_chunk, col_ids, row_identifiers, count, fetch_state);
	}
	VerifyDeleteConstraints(table, context, verify_chunk);

	if (first_id >= MAX_ROW_ID) {
		// deletion is in transaction-local storage: push delete into local chunk collection
		return transaction.storage.Delete(this, row_identifiers, count);
	} else {
		idx_t delete_count = 0;
		// delete is in the row groups
		// we need to figure out for each id to which row group it belongs
		// usually all (or many) ids belong to the same row group
		// we iterate over the ids and check for every id if it belongs to the same row group as their predecessor
		idx_t pos = 0;
		do {
			idx_t start = pos;
			auto row_group = (RowGroup *)row_groups->GetSegment(ids[pos]);
			for (pos++; pos < count; pos++) {
				D_ASSERT(ids[pos] >= 0);
				// check if this id still belongs to this row group
				if (idx_t(ids[pos]) < row_group->start) {
					// id is before row_group start -> it does not
					break;
				}
				if (idx_t(ids[pos]) >= row_group->start + row_group->count) {
					// id is after row group end -> it does not
					break;
				}
			}
			delete_count += row_group->Delete(transaction, this, ids + start, pos - start);
		} while (pos < count);
		return delete_count;
	}
}

//===--------------------------------------------------------------------===//
// Update
//===--------------------------------------------------------------------===//
static void CreateMockChunk(vector<LogicalType> &types, const vector<column_t> &column_ids, DataChunk &chunk,
                            DataChunk &mock_chunk) {
	// construct a mock DataChunk
	mock_chunk.InitializeEmpty(types);
	for (column_t i = 0; i < column_ids.size(); i++) {
		mock_chunk.data[column_ids[i]].Reference(chunk.data[i]);
	}
	mock_chunk.SetCardinality(chunk.size());
}

static bool CreateMockChunk(TableCatalogEntry &table, const vector<column_t> &column_ids,
                            unordered_set<column_t> &desired_column_ids, DataChunk &chunk, DataChunk &mock_chunk) {
	idx_t found_columns = 0;
	// check whether the desired columns are present in the UPDATE clause
	for (column_t i = 0; i < column_ids.size(); i++) {
		if (desired_column_ids.find(column_ids[i]) != desired_column_ids.end()) {
			found_columns++;
		}
	}
	if (found_columns == 0) {
		// no columns were found: no need to check the constraint again
		return false;
	}
	if (found_columns != desired_column_ids.size()) {
		// FIXME: not all columns in UPDATE clause are present!
		// this should not be triggered at all as the binder should add these columns
		throw InternalException("Not all columns required for the CHECK constraint are present in the UPDATED chunk!");
	}
	// construct a mock DataChunk
	auto types = table.GetTypes();
	CreateMockChunk(types, column_ids, chunk, mock_chunk);
	return true;
}

void DataTable::VerifyUpdateConstraints(TableCatalogEntry &table, DataChunk &chunk,
                                        const vector<column_t> &column_ids) {
	for (auto &constraint : table.bound_constraints) {
		switch (constraint->type) {
		case ConstraintType::NOT_NULL: {
			auto &not_null = *reinterpret_cast<BoundNotNullConstraint *>(constraint.get());
			// check if the constraint is in the list of column_ids
			for (idx_t i = 0; i < column_ids.size(); i++) {
				if (column_ids[i] == not_null.index) {
					// found the column id: check the data in
					VerifyNotNullConstraint(table, chunk.data[i], chunk.size(), table.columns[not_null.index].Name());
					break;
				}
			}
			break;
		}
		case ConstraintType::CHECK: {
			auto &check = *reinterpret_cast<BoundCheckConstraint *>(constraint.get());

			DataChunk mock_chunk;
			if (CreateMockChunk(table, column_ids, check.bound_columns, chunk, mock_chunk)) {
				VerifyCheckConstraint(table, *check.expression, mock_chunk);
			}
			break;
		}
		case ConstraintType::UNIQUE:
		case ConstraintType::FOREIGN_KEY:
			break;
		default:
			throw NotImplementedException("Constraint type not implemented!");
		}
	}
	// update should not be called for indexed columns!
	// instead update should have been rewritten to delete + update on higher layer
#ifdef DEBUG
	info->indexes.Scan([&](Index &index) {
		D_ASSERT(!index.IndexIsUpdated(column_ids));
		return false;
	});

#endif
}

void DataTable::Update(TableCatalogEntry &table, ClientContext &context, Vector &row_ids,
                       const vector<column_t> &column_ids, DataChunk &updates) {
	D_ASSERT(row_ids.GetType().InternalType() == ROW_TYPE);

	auto count = updates.size();
	updates.Verify();
	if (count == 0) {
		return;
	}

	if (!is_root) {
		throw TransactionException("Transaction conflict: cannot update a table that has been altered!");
	}

	// first verify that no constraints are violated
	VerifyUpdateConstraints(table, updates, column_ids);

	// now perform the actual update
	auto &transaction = Transaction::GetTransaction(context);

	updates.Normalify();
	row_ids.Normalify(count);
	auto ids = FlatVector::GetData<row_t>(row_ids);
	auto first_id = FlatVector::GetValue<row_t>(row_ids, 0);
	if (first_id >= MAX_ROW_ID) {
		// update is in transaction-local storage: push update into local storage
		transaction.storage.Update(this, row_ids, column_ids, updates);
		return;
	}

	// update is in the row groups
	// we need to figure out for each id to which row group it belongs
	// usually all (or many) ids belong to the same row group
	// we iterate over the ids and check for every id if it belongs to the same row group as their predecessor
	idx_t pos = 0;
	do {
		idx_t start = pos;
		auto row_group = (RowGroup *)row_groups->GetSegment(ids[pos]);
		row_t base_id =
		    row_group->start + ((ids[pos] - row_group->start) / STANDARD_VECTOR_SIZE * STANDARD_VECTOR_SIZE);
		for (pos++; pos < count; pos++) {
			D_ASSERT(ids[pos] >= 0);
			// check if this id still belongs to this vector
			if (ids[pos] < base_id) {
				// id is before vector start -> it does not
				break;
			}
			if (ids[pos] >= base_id + STANDARD_VECTOR_SIZE) {
				// id is after vector end -> it does not
				break;
			}
		}
		row_group->Update(transaction, updates, ids, start, pos - start, column_ids);

		lock_guard<mutex> stats_guard(stats_lock);
		for (idx_t i = 0; i < column_ids.size(); i++) {
			auto column_id = column_ids[i];
			column_stats[column_id]->stats->Merge(*row_group->GetStatistics(column_id));
		}
	} while (pos < count);
}

void DataTable::UpdateColumn(TableCatalogEntry &table, ClientContext &context, Vector &row_ids,
                             const vector<column_t> &column_path, DataChunk &updates) {
	D_ASSERT(row_ids.GetType().InternalType() == ROW_TYPE);
	D_ASSERT(updates.ColumnCount() == 1);
	updates.Verify();
	if (updates.size() == 0) {
		return;
	}

	if (!is_root) {
		throw TransactionException("Transaction conflict: cannot update a table that has been altered!");
	}

	// now perform the actual update
	auto &transaction = Transaction::GetTransaction(context);

	updates.Normalify();
	row_ids.Normalify(updates.size());
	auto first_id = FlatVector::GetValue<row_t>(row_ids, 0);
	if (first_id >= MAX_ROW_ID) {
		throw NotImplementedException("Cannot update a column-path on transaction local data");
	}
	// find the row_group this id belongs to
	auto primary_column_idx = column_path[0];
	auto row_group = (RowGroup *)row_groups->GetSegment(first_id);
	row_group->UpdateColumn(transaction, updates, row_ids, column_path);

	lock_guard<mutex> stats_guard(stats_lock);
	column_stats[primary_column_idx]->stats->Merge(*row_group->GetStatistics(primary_column_idx));
}

//===--------------------------------------------------------------------===//
// Create Index Scan
//===--------------------------------------------------------------------===//
void DataTable::InitializeCreateIndexScan(CreateIndexScanState &state, const vector<column_t> &column_ids) {
	// we grab the append lock to make sure nothing is appended until AFTER we finish the index scan
	state.append_lock = std::unique_lock<mutex>(append_lock);
	state.delete_lock = std::unique_lock<mutex>(row_groups->node_lock);

	InitializeScan(state, column_ids);
}

bool DataTable::ScanCreateIndex(CreateIndexScanState &state, DataChunk &result, TableScanType type) {
	auto current_row_group = state.row_group_scan_state.row_group;
	while (current_row_group) {
		current_row_group->ScanCommitted(state.row_group_scan_state, result, type);
		if (result.size() > 0) {
			return true;
		} else {
			current_row_group = state.row_group_scan_state.row_group = (RowGroup *)current_row_group->next.get();
			if (current_row_group) {
				current_row_group->InitializeScan(state.row_group_scan_state);
			}
		}
	}
	return false;
}

void DataTable::AddIndex(unique_ptr<Index> index, const vector<unique_ptr<Expression>> &expressions) {
	DataChunk result;
	result.Initialize(index->logical_types);

	DataChunk intermediate;
	vector<LogicalType> intermediate_types;
	auto column_ids = index->column_ids;
	column_ids.push_back(COLUMN_IDENTIFIER_ROW_ID);
	for (auto &id : index->column_ids) {
		auto &col = column_definitions[id];
		intermediate_types.push_back(col.Type());
	}
	intermediate_types.emplace_back(LogicalType::ROW_TYPE);
	intermediate.Initialize(intermediate_types);

	// initialize an index scan
	CreateIndexScanState state;
	InitializeCreateIndexScan(state, column_ids);

	if (!is_root) {
		throw TransactionException("Transaction conflict: cannot add an index to a table that has been altered!");
	}

	// now start incrementally building the index
	{
		IndexLock lock;
		index->InitializeLock(lock);
		ExpressionExecutor executor(expressions);
		while (true) {
			intermediate.Reset();
			// scan a new chunk from the table to index
			ScanCreateIndex(state, intermediate, TableScanType::TABLE_SCAN_COMMITTED_ROWS_OMIT_PERMANENTLY_DELETED);
			if (intermediate.size() == 0) {
				// finished scanning for index creation
				// release all locks
				break;
			}
			// resolve the expressions for this chunk
			executor.Execute(intermediate, result);

			// insert into the index
			if (!index->Insert(lock, result, intermediate.data[intermediate.ColumnCount() - 1])) {
				throw ConstraintException(
				    "Cant create unique index, table contains duplicate data on indexed column(s)");
			}
		}
	}
	info->indexes.AddIndex(move(index));
}

unique_ptr<BaseStatistics> DataTable::GetStatistics(ClientContext &context, column_t column_id) {
	if (column_id == COLUMN_IDENTIFIER_ROW_ID) {
		return nullptr;
	}
	lock_guard<mutex> stats_guard(stats_lock);
	return column_stats[column_id]->stats->Copy();
}

//===--------------------------------------------------------------------===//
// Checkpoint
//===--------------------------------------------------------------------===//
BlockPointer DataTable::Checkpoint(TableDataWriter &writer) {
	// checkpoint each individual row group
	// FIXME: we might want to combine adjacent row groups in case they have had deletions...
	vector<unique_ptr<BaseStatistics>> global_stats;
	for (idx_t i = 0; i < column_definitions.size(); i++) {
		global_stats.push_back(column_stats[i]->stats->Copy());
	}

	auto row_group = (RowGroup *)row_groups->GetRootSegment();
	vector<RowGroupPointer> row_group_pointers;
	while (row_group) {
		auto pointer = row_group->Checkpoint(writer, global_stats);
		row_group_pointers.push_back(move(pointer));
		row_group = (RowGroup *)row_group->next.get();
	}
	// store the current position in the metadata writer
	// this is where the row groups for this table start
	auto &meta_writer = writer.GetMetaWriter();
	auto pointer = meta_writer.GetBlockPointer();

	for (auto &stats : global_stats) {
		stats->Serialize(meta_writer);
	}
	// now start writing the row group pointers to disk
	meta_writer.Write<uint64_t>(row_group_pointers.size());
	for (auto &row_group_pointer : row_group_pointers) {
		RowGroup::Serialize(row_group_pointer, meta_writer);
	}
	return pointer;
}

void DataTable::CommitDropColumn(idx_t index) {
	auto segment = (RowGroup *)row_groups->GetRootSegment();
	while (segment) {
		segment->CommitDropColumn(index);
		segment = (RowGroup *)segment->next.get();
	}
}

idx_t DataTable::GetTotalRows() {
	return total_rows;
}

void DataTable::CommitDropTable() {
	// commit a drop of this table: mark all blocks as modified so they can be reclaimed later on
	auto segment = (RowGroup *)row_groups->GetRootSegment();
	while (segment) {
		segment->CommitDrop();
		segment = (RowGroup *)segment->next.get();
	}
}

//===--------------------------------------------------------------------===//
// GetStorageInfo
//===--------------------------------------------------------------------===//
vector<vector<Value>> DataTable::GetStorageInfo() {
	vector<vector<Value>> result;

	auto row_group = (RowGroup *)row_groups->GetRootSegment();
	idx_t row_group_index = 0;
	while (row_group) {
		row_group->GetStorageInfo(row_group_index, result);
		row_group_index++;

		row_group = (RowGroup *)row_group->next.get();
	}

	return result;
}

} // namespace duckdb







namespace duckdb {

Index::Index(IndexType type, const vector<column_t> &column_ids_p,
             const vector<unique_ptr<Expression>> &unbound_expressions, IndexConstraintType constraint_type_p)
    : type(type), column_ids(column_ids_p), constraint_type(constraint_type_p) {
	for (auto &expr : unbound_expressions) {
		types.push_back(expr->return_type.InternalType());
		logical_types.push_back(expr->return_type);
		auto unbound_expression = expr->Copy();
		bound_expressions.push_back(BindExpression(unbound_expression->Copy()));
		this->unbound_expressions.emplace_back(move(unbound_expression));
	}
	for (auto &bound_expr : bound_expressions) {
		executor.AddExpression(*bound_expr);
	}
	for (auto column_id : column_ids) {
		column_id_set.insert(column_id);
	}
}

void Index::InitializeLock(IndexLock &state) {
	state.index_lock = unique_lock<mutex>(lock);
}

bool Index::Append(DataChunk &entries, Vector &row_identifiers) {
	IndexLock state;
	InitializeLock(state);
	return Append(state, entries, row_identifiers);
}

void Index::Delete(DataChunk &entries, Vector &row_identifiers) {
	IndexLock state;
	InitializeLock(state);
	Delete(state, entries, row_identifiers);
}

void Index::ExecuteExpressions(DataChunk &input, DataChunk &result) {
	executor.Execute(input, result);
}

unique_ptr<Expression> Index::BindExpression(unique_ptr<Expression> expr) {
	if (expr->type == ExpressionType::BOUND_COLUMN_REF) {
		auto &bound_colref = (BoundColumnRefExpression &)*expr;
		return make_unique<BoundReferenceExpression>(expr->return_type, column_ids[bound_colref.binding.column_index]);
	}
	ExpressionIterator::EnumerateChildren(*expr,
	                                      [&](unique_ptr<Expression> &expr) { expr = BindExpression(move(expr)); });
	return expr;
}

bool Index::IndexIsUpdated(const vector<column_t> &column_ids) const {
	for (auto &column : column_ids) {
		if (column_id_set.find(column) != column_id_set.end()) {
			return true;
		}
	}
	return false;
}

} // namespace duckdb











namespace duckdb {

LocalTableStorage::LocalTableStorage(DataTable &table) : table(table), active_scans(0) {
	Clear();
}

LocalTableStorage::~LocalTableStorage() {
}

void LocalTableStorage::InitializeScan(LocalScanState &state, TableFilterSet *table_filters) {
	state.table_filters = table_filters;
	state.chunk_index = 0;
	if (collection.ChunkCount() == 0) {
		// nothing to scan
		state.max_index = 0;
		state.last_chunk_count = 0;
		return;
	}
	state.SetStorage(shared_from_this());

	state.max_index = collection.ChunkCount() - 1;
	state.last_chunk_count = collection.Chunks().back()->size();
}

idx_t LocalTableStorage::EstimatedSize() {
	idx_t appended_rows = collection.Count() - deleted_rows;
	if (appended_rows == 0) {
		return 0;
	}
	idx_t row_size = 0;
	for (auto &type : collection.Types()) {
		row_size += GetTypeIdSize(type.InternalType());
	}
	return appended_rows * row_size;
}

LocalScanState::~LocalScanState() {
	SetStorage(nullptr);
}

void LocalScanState::SetStorage(shared_ptr<LocalTableStorage> new_storage) {
	if (storage) {
		D_ASSERT(storage->active_scans > 0);
		storage->active_scans--;
	}
	storage = move(new_storage);
	if (storage) {
		storage->active_scans++;
	}
}

void LocalTableStorage::Clear() {
	collection.Reset();
	deleted_entries.clear();
	indexes.clear();
	deleted_rows = 0;
	table.info->indexes.Scan([&](Index &index) {
		D_ASSERT(index.type == IndexType::ART);
		auto &art = (ART &)index;
		if (art.constraint_type != IndexConstraintType::NONE) {
			// unique index: create a local ART index that maintains the same unique constraint
			vector<unique_ptr<Expression>> unbound_expressions;
			for (auto &expr : art.unbound_expressions) {
				unbound_expressions.push_back(expr->Copy());
			}
			indexes.push_back(make_unique<ART>(art.column_ids, move(unbound_expressions), art.constraint_type));
		}
		return false;
	});
}

void LocalStorage::InitializeScan(DataTable *table, LocalScanState &state, TableFilterSet *table_filters) {
	auto entry = table_storage.find(table);
	if (entry == table_storage.end()) {
		// no local storage for table: set scan to nullptr
		state.SetStorage(nullptr);
		return;
	}
	auto storage = entry->second.get();
	storage->InitializeScan(state, table_filters);
}

void LocalStorage::Scan(LocalScanState &state, const vector<column_t> &column_ids, DataChunk &result) {
	auto storage = state.GetStorage();
	if (!storage || state.chunk_index > state.max_index) {
		// nothing left to scan
		result.Reset();
		return;
	}
	auto &chunk = storage->collection.GetChunk(state.chunk_index);
	idx_t chunk_count = state.chunk_index == state.max_index ? state.last_chunk_count : chunk.size();
	idx_t count = chunk_count;

	// first create a selection vector from the deleted entries (if any)
	SelectionVector valid_sel(STANDARD_VECTOR_SIZE);
	auto entry = storage->deleted_entries.find(state.chunk_index);
	if (entry != storage->deleted_entries.end()) {
		// deleted entries! create a selection vector
		auto deleted = entry->second.get();
		idx_t new_count = 0;
		for (idx_t i = 0; i < count; i++) {
			if (!deleted[i]) {
				valid_sel.set_index(new_count++, i);
			}
		}
		if (new_count == 0 && count > 0) {
			// all entries in this chunk were deleted: continue to next chunk
			state.chunk_index++;
			Scan(state, column_ids, result);
			return;
		}
		count = new_count;
	}

	SelectionVector sel;
	if (count != chunk_count) {
		sel.Initialize(valid_sel);
	} else {
		sel.Initialize(nullptr);
	}
	// now scan the vectors of the chunk
	for (idx_t i = 0; i < column_ids.size(); i++) {
		auto id = column_ids[i];
		if (id == COLUMN_IDENTIFIER_ROW_ID) {
			// row identifier: return a sequence of rowids starting from MAX_ROW_ID plus the row offset in the chunk
			result.data[i].Sequence(MAX_ROW_ID + state.chunk_index * STANDARD_VECTOR_SIZE, 1);
		} else {
			result.data[i].Reference(chunk.data[id]);
		}
		idx_t approved_tuple_count = count;
		if (state.table_filters) {
			auto column_filters = state.table_filters->filters.find(i);
			if (column_filters != state.table_filters->filters.end()) {
				//! We have filters to apply here
				auto &mask = FlatVector::Validity(result.data[i]);
				ColumnSegment::FilterSelection(sel, result.data[i], *column_filters->second, approved_tuple_count,
				                               mask);
				count = approved_tuple_count;
			}
		}
	}
	if (count == 0) {
		// all entries in this chunk were filtered:: Continue on next chunk
		state.chunk_index++;
		Scan(state, column_ids, result);
		return;
	}
	if (count == chunk_count) {
		result.SetCardinality(count);
	} else {
		result.Slice(sel, count);
	}
	state.chunk_index++;
}

void LocalStorage::Append(DataTable *table, DataChunk &chunk) {
	auto entry = table_storage.find(table);
	LocalTableStorage *storage;
	if (entry == table_storage.end()) {
		auto new_storage = make_shared<LocalTableStorage>(*table);
		storage = new_storage.get();
		table_storage.insert(make_pair(table, move(new_storage)));
	} else {
		storage = entry->second.get();
	}
	// append to unique indices (if any)
	if (!storage->indexes.empty()) {
		idx_t base_id = MAX_ROW_ID + storage->collection.Count();

		// first generate the vector of row identifiers
		Vector row_ids(LogicalType::ROW_TYPE);
		VectorOperations::GenerateSequence(row_ids, chunk.size(), base_id, 1);

		// now append the entries to the indices
		for (auto &index : storage->indexes) {
			if (!index->Append(chunk, row_ids)) {
				throw ConstraintException("PRIMARY KEY or UNIQUE constraint violated: duplicated key");
			}
		}
	}
	//! Append to the chunk
	storage->collection.Append(chunk);
	if (storage->active_scans == 0 && storage->collection.Count() >= RowGroup::ROW_GROUP_SIZE * 2) {
		// flush to base storage
		Flush(*table, *storage);
	}
}

LocalTableStorage *LocalStorage::GetStorage(DataTable *table) {
	auto entry = table_storage.find(table);
	D_ASSERT(entry != table_storage.end());
	return entry->second.get();
}

idx_t LocalStorage::EstimatedSize() {
	idx_t estimated_size = 0;
	for (auto &storage : table_storage) {
		estimated_size += storage.second->EstimatedSize();
	}
	return estimated_size;
}

static idx_t GetChunk(Vector &row_ids) {
	auto ids = FlatVector::GetData<row_t>(row_ids);
	auto first_id = ids[0] - MAX_ROW_ID;

	return first_id / STANDARD_VECTOR_SIZE;
}

idx_t LocalStorage::Delete(DataTable *table, Vector &row_ids, idx_t count) {
	auto storage = GetStorage(table);
	// figure out the chunk from which these row ids came
	idx_t chunk_idx = GetChunk(row_ids);
	D_ASSERT(chunk_idx < storage->collection.ChunkCount());

	// delete from unique indices (if any)
	if (!storage->indexes.empty()) {
		// Index::Delete assumes that ALL rows are being deleted, so
		// Slice out the rows that are being deleted from the storage Chunk
		auto &chunk = storage->collection.GetChunk(chunk_idx);

		VectorData row_ids_data;
		row_ids.Orrify(count, row_ids_data);
		auto row_identifiers = (const row_t *)row_ids_data.data;
		SelectionVector sel(count);
		for (idx_t i = 0; i < count; ++i) {
			const auto idx = row_ids_data.sel->get_index(i);
			sel.set_index(i, row_identifiers[idx] - MAX_ROW_ID);
		}

		DataChunk deleted;
		deleted.InitializeEmpty(chunk.GetTypes());
		deleted.Slice(chunk, sel, count);
		for (auto &index : storage->indexes) {
			index->Delete(deleted, row_ids);
		}
	}

	// get a pointer to the deleted entries for this chunk
	bool *deleted;
	auto entry = storage->deleted_entries.find(chunk_idx);
	if (entry == storage->deleted_entries.end()) {
		// nothing deleted yet, add the deleted entries
		auto del_entries = unique_ptr<bool[]>(new bool[STANDARD_VECTOR_SIZE]);
		memset(del_entries.get(), 0, sizeof(bool) * STANDARD_VECTOR_SIZE);
		deleted = del_entries.get();
		storage->deleted_entries.insert(make_pair(chunk_idx, move(del_entries)));
	} else {
		deleted = entry->second.get();
	}

	// now actually mark the entries as deleted in the deleted vector
	idx_t base_index = MAX_ROW_ID + chunk_idx * STANDARD_VECTOR_SIZE;

	idx_t deleted_count = 0;
	auto ids = FlatVector::GetData<row_t>(row_ids);
	for (idx_t i = 0; i < count; i++) {
		auto id = ids[i] - base_index;
		if (!deleted[id]) {
			deleted_count++;
		}
		deleted[id] = true;
	}
	storage->deleted_rows += deleted_count;
	return deleted_count;
}

template <class T>
static void TemplatedUpdateLoop(Vector &data_vector, Vector &update_vector, Vector &row_ids, idx_t count,
                                idx_t base_index) {
	VectorData udata;
	update_vector.Orrify(count, udata);

	auto target = FlatVector::GetData<T>(data_vector);
	auto &mask = FlatVector::Validity(data_vector);
	auto ids = FlatVector::GetData<row_t>(row_ids);
	auto updates = (T *)udata.data;

	for (idx_t i = 0; i < count; i++) {
		auto uidx = udata.sel->get_index(i);

		auto id = ids[i] - base_index;
		target[id] = updates[uidx];
		mask.Set(id, udata.validity.RowIsValid(uidx));
	}
}

static void UpdateChunk(Vector &data, Vector &updates, Vector &row_ids, idx_t count, idx_t base_index) {
	D_ASSERT(data.GetType() == updates.GetType());
	D_ASSERT(row_ids.GetType() == LogicalType::ROW_TYPE);

	switch (data.GetType().InternalType()) {
	case PhysicalType::INT8:
		TemplatedUpdateLoop<int8_t>(data, updates, row_ids, count, base_index);
		break;
	case PhysicalType::UINT8:
		TemplatedUpdateLoop<uint8_t>(data, updates, row_ids, count, base_index);
		break;
	case PhysicalType::INT16:
		TemplatedUpdateLoop<int16_t>(data, updates, row_ids, count, base_index);
		break;
	case PhysicalType::UINT16:
		TemplatedUpdateLoop<uint16_t>(data, updates, row_ids, count, base_index);
		break;
	case PhysicalType::INT32:
		TemplatedUpdateLoop<int32_t>(data, updates, row_ids, count, base_index);
		break;
	case PhysicalType::UINT32:
		TemplatedUpdateLoop<uint32_t>(data, updates, row_ids, count, base_index);
		break;
	case PhysicalType::INT64:
		TemplatedUpdateLoop<int64_t>(data, updates, row_ids, count, base_index);
		break;
	case PhysicalType::UINT64:
		TemplatedUpdateLoop<uint64_t>(data, updates, row_ids, count, base_index);
		break;
	case PhysicalType::FLOAT:
		TemplatedUpdateLoop<float>(data, updates, row_ids, count, base_index);
		break;
	case PhysicalType::DOUBLE:
		TemplatedUpdateLoop<double>(data, updates, row_ids, count, base_index);
		break;
	case PhysicalType::VARCHAR:
		TemplatedUpdateLoop<string_t>(data, updates, row_ids, count, base_index);
		break;
	default:
		throw Exception("Unsupported type for in-place update: " + TypeIdToString(data.GetType().InternalType()));
	}
}

void LocalStorage::Update(DataTable *table, Vector &row_ids, const vector<column_t> &column_ids, DataChunk &data) {
	auto storage = GetStorage(table);
	// figure out the chunk from which these row ids came
	idx_t chunk_idx = GetChunk(row_ids);
	D_ASSERT(chunk_idx < storage->collection.ChunkCount());

	idx_t base_index = MAX_ROW_ID + chunk_idx * STANDARD_VECTOR_SIZE;

	// now perform the actual update
	auto &chunk = storage->collection.GetChunk(chunk_idx);
	for (idx_t i = 0; i < column_ids.size(); i++) {
		auto col_idx = column_ids[i];
		UpdateChunk(chunk.data[col_idx], data.data[i], row_ids, data.size(), base_index);
	}
}

template <class T>
bool LocalStorage::ScanTableStorage(DataTable &table, LocalTableStorage &storage, T &&fun) {
	vector<column_t> column_ids;
	column_ids.reserve(table.column_definitions.size());
	for (idx_t i = 0; i < table.column_definitions.size(); i++) {
		column_ids.push_back(i);
	}

	DataChunk chunk;
	chunk.Initialize(table.GetTypes());

	// initialize the scan
	LocalScanState state;
	storage.InitializeScan(state);

	while (true) {
		Scan(state, column_ids, chunk);
		if (chunk.size() == 0) {
			return true;
		}
		if (!fun(chunk)) {
			return false;
		}
	}
}

void LocalStorage::Flush(DataTable &table, LocalTableStorage &storage) {
	if (storage.collection.Count() <= storage.deleted_rows) {
		return;
	}
	idx_t append_count = storage.collection.Count() - storage.deleted_rows;
	TableAppendState append_state;
	table.InitializeAppend(transaction, append_state, append_count);

	bool constraint_violated = false;
	ScanTableStorage(table, storage, [&](DataChunk &chunk) -> bool {
		// append this chunk to the indexes of the table
		if (!table.AppendToIndexes(append_state, chunk, append_state.current_row)) {
			constraint_violated = true;
			return false;
		}
		// append to base table
		table.Append(transaction, chunk, append_state);
		return true;
	});
	if (constraint_violated) {
		// need to revert the append
		row_t current_row = append_state.row_start;
		// remove the data from the indexes, if there are any indexes
		ScanTableStorage(table, storage, [&](DataChunk &chunk) -> bool {
			// append this chunk to the indexes of the table
			table.RemoveFromIndexes(append_state, chunk, current_row);

			current_row += chunk.size();
			if (current_row >= append_state.current_row) {
				// finished deleting all rows from the index: abort now
				return false;
			}
			return true;
		});
		table.RevertAppendInternal(append_state.row_start, append_count);
		storage.Clear();
		throw ConstraintException("PRIMARY KEY or UNIQUE constraint violated: duplicated key");
	}
	storage.Clear();
	transaction.PushAppend(&table, append_state.row_start, append_count);
}

void LocalStorage::Commit(LocalStorage::CommitState &commit_state, Transaction &transaction, WriteAheadLog *log,
                          transaction_t commit_id) {
	// commit local storage, iterate over all entries in the table storage map
	for (auto &entry : table_storage) {
		auto table = entry.first;
		auto storage = entry.second.get();
		Flush(*table, *storage);
	}
	// finished commit: clear local storage
	table_storage.clear();
}

void LocalStorage::AddColumn(DataTable *old_dt, DataTable *new_dt, ColumnDefinition &new_column,
                             Expression *default_value) {
	// check if there are any pending appends for the old version of the table
	auto entry = table_storage.find(old_dt);
	if (entry == table_storage.end()) {
		return;
	}
	// take over the storage from the old entry
	auto new_storage = move(entry->second);

	// now add the new column filled with the default value to all chunks
	const auto &new_column_type = new_column.Type();
	ExpressionExecutor executor;
	DataChunk dummy_chunk;
	if (default_value) {
		executor.AddExpression(*default_value);
	}

	new_storage->collection.Types().push_back(new_column_type);
	for (idx_t chunk_idx = 0; chunk_idx < new_storage->collection.ChunkCount(); chunk_idx++) {
		auto &chunk = new_storage->collection.GetChunk(chunk_idx);
		Vector result(new_column_type);
		if (default_value) {
			dummy_chunk.SetCardinality(chunk.size());
			executor.ExecuteExpression(dummy_chunk, result);
		} else {
			FlatVector::Validity(result).SetAllInvalid(chunk.size());
		}
		result.Normalify(chunk.size());
		chunk.data.push_back(move(result));
	}

	table_storage.erase(entry);
	table_storage[new_dt] = move(new_storage);
}

void LocalStorage::ChangeType(DataTable *old_dt, DataTable *new_dt, idx_t changed_idx, const LogicalType &target_type,
                              const vector<column_t> &bound_columns, Expression &cast_expr) {
	// check if there are any pending appends for the old version of the table
	auto entry = table_storage.find(old_dt);
	if (entry == table_storage.end()) {
		return;
	}
	throw NotImplementedException("FIXME: ALTER TYPE with transaction local data not currently supported");
}

void LocalStorage::FetchChunk(DataTable *table, Vector &row_ids, idx_t count, DataChunk &dst_chunk) {
	auto storage = GetStorage(table);
	idx_t chunk_idx = GetChunk(row_ids);
	auto &chunk = storage->collection.GetChunk(chunk_idx);

	VectorData row_ids_data;
	row_ids.Orrify(count, row_ids_data);
	auto row_identifiers = (const row_t *)row_ids_data.data;
	SelectionVector sel(count);
	for (idx_t i = 0; i < count; ++i) {
		const auto idx = row_ids_data.sel->get_index(i);
		sel.set_index(i, row_identifiers[idx] - MAX_ROW_ID);
	}

	dst_chunk.InitializeEmpty(chunk.GetTypes());
	dst_chunk.Slice(chunk, sel, count);
}

vector<unique_ptr<Index>> &LocalStorage::GetIndexes(DataTable *table) {
	auto storage = GetStorage(table);

	return storage->indexes;
}

} // namespace duckdb



#include <cstring>

namespace duckdb {

MetaBlockReader::MetaBlockReader(DatabaseInstance &db, block_id_t block_id)
    : db(db), handle(nullptr), offset(0), next_block(-1) {
	ReadNewBlock(block_id);
}

MetaBlockReader::~MetaBlockReader() {
}

void MetaBlockReader::ReadData(data_ptr_t buffer, idx_t read_size) {
	while (offset + read_size > handle->node->size) {
		// cannot read entire entry from block
		// first read what we can from this block
		idx_t to_read = handle->node->size - offset;
		if (to_read > 0) {
			memcpy(buffer, handle->node->buffer + offset, to_read);
			read_size -= to_read;
			buffer += to_read;
		}
		// then move to the next block
		ReadNewBlock(next_block);
	}
	// we have enough left in this block to read from the buffer
	memcpy(buffer, handle->node->buffer + offset, read_size);
	offset += read_size;
}

void MetaBlockReader::ReadNewBlock(block_id_t id) {
	auto &block_manager = BlockManager::GetBlockManager(db);
	auto &buffer_manager = BufferManager::GetBufferManager(db);

	block_manager.MarkBlockAsModified(id);
	block = buffer_manager.RegisterBlock(id);
	handle = buffer_manager.Pin(block);

	next_block = Load<block_id_t>(handle->node->buffer);
	D_ASSERT(next_block >= -1);
	offset = sizeof(block_id_t);
}

} // namespace duckdb


#include <cstring>

namespace duckdb {

MetaBlockWriter::MetaBlockWriter(DatabaseInstance &db, block_id_t initial_block_id) : db(db) {
	if (initial_block_id == INVALID_BLOCK) {
		initial_block_id = GetNextBlockId();
	}
	auto &block_manager = BlockManager::GetBlockManager(db);
	block = block_manager.CreateBlock(initial_block_id);
	Store<block_id_t>(-1, block->buffer);
	offset = sizeof(block_id_t);
}

MetaBlockWriter::~MetaBlockWriter() {
	if (Exception::UncaughtException()) {
		return;
	}
	try {
		Flush();
	} catch (...) {
	}
}

block_id_t MetaBlockWriter::GetNextBlockId() {
	auto &block_manager = BlockManager::GetBlockManager(db);
	return block_manager.GetFreeBlockId();
}

BlockPointer MetaBlockWriter::GetBlockPointer() {
	BlockPointer pointer;
	pointer.block_id = block->id;
	pointer.offset = offset;
	return pointer;
}

void MetaBlockWriter::Flush() {
	written_blocks.insert(block->id);
	if (offset > sizeof(block_id_t)) {
		auto &block_manager = BlockManager::GetBlockManager(db);
		block_manager.Write(*block);
		offset = sizeof(block_id_t);
	}
}

void MetaBlockWriter::WriteData(const_data_ptr_t buffer, idx_t write_size) {
	while (offset + write_size > block->size) {
		// we need to make a new block
		// first copy what we can
		D_ASSERT(offset <= block->size);
		idx_t copy_amount = block->size - offset;
		if (copy_amount > 0) {
			memcpy(block->buffer + offset, buffer, copy_amount);
			buffer += copy_amount;
			offset += copy_amount;
			write_size -= copy_amount;
		}
		// now we need to get a new block id
		block_id_t new_block_id = GetNextBlockId();
		// write the block id of the new block to the start of the current block
		Store<block_id_t>(new_block_id, block->buffer);
		// first flush the old block
		Flush();
		// now update the block id of the lbock
		block->id = new_block_id;
		Store<block_id_t>(-1, block->buffer);
	}
	memcpy(block->buffer + offset, buffer, write_size);
	offset += write_size;
}

} // namespace duckdb











#include <algorithm>
#include <cstring>

namespace duckdb {

const char MainHeader::MAGIC_BYTES[] = "DUCK";

void MainHeader::Serialize(Serializer &ser) {
	ser.WriteData((data_ptr_t)MAGIC_BYTES, MAGIC_BYTE_SIZE);
	ser.Write<uint64_t>(version_number);
	FieldWriter writer(ser);
	for (idx_t i = 0; i < FLAG_COUNT; i++) {
		writer.WriteField<uint64_t>(flags[i]);
	}
	writer.Finalize();
}

void MainHeader::CheckMagicBytes(FileHandle &handle) {
	data_t magic_bytes[MAGIC_BYTE_SIZE];
	if (handle.GetFileSize() < MainHeader::MAGIC_BYTE_SIZE + MainHeader::MAGIC_BYTE_OFFSET) {
		throw IOException("The file is not a valid DuckDB database file!");
	}
	handle.Read(magic_bytes, MainHeader::MAGIC_BYTE_SIZE, MainHeader::MAGIC_BYTE_OFFSET);
	if (memcmp(magic_bytes, MainHeader::MAGIC_BYTES, MainHeader::MAGIC_BYTE_SIZE) != 0) {
		throw IOException("The file is not a valid DuckDB database file!");
	}
}

MainHeader MainHeader::Deserialize(Deserializer &source) {
	data_t magic_bytes[MAGIC_BYTE_SIZE];
	MainHeader header;
	source.ReadData(magic_bytes, MainHeader::MAGIC_BYTE_SIZE);
	if (memcmp(magic_bytes, MainHeader::MAGIC_BYTES, MainHeader::MAGIC_BYTE_SIZE) != 0) {
		throw IOException("The file is not a valid DuckDB database file!");
	}
	header.version_number = source.Read<uint64_t>();
	// read the flags
	FieldReader reader(source);
	for (idx_t i = 0; i < FLAG_COUNT; i++) {
		header.flags[i] = reader.ReadRequired<uint64_t>();
	}
	reader.Finalize();
	return header;
}

void DatabaseHeader::Serialize(Serializer &ser) {
	ser.Write<uint64_t>(iteration);
	ser.Write<block_id_t>(meta_block);
	ser.Write<block_id_t>(free_list);
	ser.Write<uint64_t>(block_count);
}

DatabaseHeader DatabaseHeader::Deserialize(Deserializer &source) {
	DatabaseHeader header;
	header.iteration = source.Read<uint64_t>();
	header.meta_block = source.Read<block_id_t>();
	header.free_list = source.Read<block_id_t>();
	header.block_count = source.Read<uint64_t>();
	return header;
}

template <class T>
void SerializeHeaderStructure(T header, data_ptr_t ptr) {
	BufferedSerializer ser(ptr, Storage::FILE_HEADER_SIZE);
	header.Serialize(ser);
}

template <class T>
T DeserializeHeaderStructure(data_ptr_t ptr) {
	BufferedDeserializer source(ptr, Storage::FILE_HEADER_SIZE);
	return T::Deserialize(source);
}

SingleFileBlockManager::SingleFileBlockManager(DatabaseInstance &db, string path_p, bool read_only, bool create_new,
                                               bool use_direct_io)
    : db(db), path(move(path_p)),
      header_buffer(Allocator::Get(db), FileBufferType::MANAGED_BUFFER, Storage::FILE_HEADER_SIZE), iteration_count(0),
      read_only(read_only), use_direct_io(use_direct_io) {
	uint8_t flags;
	FileLockType lock;
	if (read_only) {
		D_ASSERT(!create_new);
		flags = FileFlags::FILE_FLAGS_READ;
		lock = FileLockType::READ_LOCK;
	} else {
		flags = FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_READ;
		lock = FileLockType::WRITE_LOCK;
		if (create_new) {
			flags |= FileFlags::FILE_FLAGS_FILE_CREATE;
		}
	}
	if (use_direct_io) {
		flags |= FileFlags::FILE_FLAGS_DIRECT_IO;
	}
	// open the RDBMS handle
	auto &fs = FileSystem::GetFileSystem(db);
	handle = fs.OpenFile(path, flags, lock);
	if (create_new) {
		// if we create a new file, we fill the metadata of the file
		// first fill in the new header
		header_buffer.Clear();

		MainHeader main_header;
		main_header.version_number = VERSION_NUMBER;
		memset(main_header.flags, 0, sizeof(uint64_t) * 4);

		SerializeHeaderStructure<MainHeader>(main_header, header_buffer.buffer);
		// now write the header to the file
		header_buffer.ChecksumAndWrite(*handle, 0);
		header_buffer.Clear();

		// write the database headers
		// initialize meta_block and free_list to INVALID_BLOCK because the database file does not contain any actual
		// content yet
		DatabaseHeader h1, h2;
		// header 1
		h1.iteration = 0;
		h1.meta_block = INVALID_BLOCK;
		h1.free_list = INVALID_BLOCK;
		h1.block_count = 0;
		SerializeHeaderStructure<DatabaseHeader>(h1, header_buffer.buffer);
		header_buffer.ChecksumAndWrite(*handle, Storage::FILE_HEADER_SIZE);
		// header 2
		h2.iteration = 0;
		h2.meta_block = INVALID_BLOCK;
		h2.free_list = INVALID_BLOCK;
		h2.block_count = 0;
		SerializeHeaderStructure<DatabaseHeader>(h2, header_buffer.buffer);
		header_buffer.ChecksumAndWrite(*handle, Storage::FILE_HEADER_SIZE * 2);
		// ensure that writing to disk is completed before returning
		handle->Sync();
		// we start with h2 as active_header, this way our initial write will be in h1
		iteration_count = 0;
		active_header = 1;
		max_block = 0;
	} else {
		MainHeader::CheckMagicBytes(*handle);
		// otherwise, we check the metadata of the file
		header_buffer.ReadAndChecksum(*handle, 0);
		MainHeader header = DeserializeHeaderStructure<MainHeader>(header_buffer.buffer);
		// check the version number
		if (header.version_number != VERSION_NUMBER) {
			throw IOException(
			    "Trying to read a database file with version number %lld, but we can only read version %lld.\n"
			    "The database file was created with an %s version of DuckDB.\n\n"
			    "The storage of DuckDB is not yet stable; newer versions of DuckDB cannot read old database files and "
			    "vice versa.\n"
			    "The storage will be stabilized when version 1.0 releases.\n\n"
			    "For now, we recommend that you load the database file in a supported version of DuckDB, and use the "
			    "EXPORT DATABASE command "
			    "followed by IMPORT DATABASE on the current version of DuckDB.",
			    header.version_number, VERSION_NUMBER, VERSION_NUMBER > header.version_number ? "older" : "newer");
		}

		// read the database headers from disk
		DatabaseHeader h1, h2;
		header_buffer.ReadAndChecksum(*handle, Storage::FILE_HEADER_SIZE);
		h1 = DeserializeHeaderStructure<DatabaseHeader>(header_buffer.buffer);
		header_buffer.ReadAndChecksum(*handle, Storage::FILE_HEADER_SIZE * 2);
		h2 = DeserializeHeaderStructure<DatabaseHeader>(header_buffer.buffer);
		// check the header with the highest iteration count
		if (h1.iteration > h2.iteration) {
			// h1 is active header
			active_header = 0;
			Initialize(h1);
		} else {
			// h2 is active header
			active_header = 1;
			Initialize(h2);
		}
	}
}

void SingleFileBlockManager::Initialize(DatabaseHeader &header) {
	free_list_id = header.free_list;
	meta_block = header.meta_block;
	iteration_count = header.iteration;
	max_block = header.block_count;
}

void SingleFileBlockManager::LoadFreeList() {
	if (read_only) {
		// no need to load free list for read only db
		return;
	}
	if (free_list_id == INVALID_BLOCK) {
		// no free list
		return;
	}
	MetaBlockReader reader(db, free_list_id);
	auto free_list_count = reader.Read<uint64_t>();
	free_list.clear();
	for (idx_t i = 0; i < free_list_count; i++) {
		free_list.insert(reader.Read<block_id_t>());
	}
	auto multi_use_blocks_count = reader.Read<uint64_t>();
	multi_use_blocks.clear();
	for (idx_t i = 0; i < multi_use_blocks_count; i++) {
		auto block_id = reader.Read<block_id_t>();
		auto usage_count = reader.Read<uint32_t>();
		multi_use_blocks[block_id] = usage_count;
	}
}

void SingleFileBlockManager::StartCheckpoint() {
}

bool SingleFileBlockManager::IsRootBlock(block_id_t root) {
	return root == meta_block;
}

block_id_t SingleFileBlockManager::GetFreeBlockId() {
	block_id_t block;
	if (!free_list.empty()) {
		// free list is non empty
		// take an entry from the free list
		block = *free_list.begin();
		// erase the entry from the free list again
		free_list.erase(free_list.begin());
	} else {
		block = max_block++;
	}
	return block;
}

void SingleFileBlockManager::MarkBlockAsModified(block_id_t block_id) {
	D_ASSERT(block_id >= 0);

	// check if the block is a multi-use block
	auto entry = multi_use_blocks.find(block_id);
	if (entry != multi_use_blocks.end()) {
		// it is! reduce the reference count of the block
		entry->second--;
		// check the reference count: is the block still a multi-use block?
		if (entry->second <= 1) {
			// no longer a multi-use block!
			multi_use_blocks.erase(entry);
		}
		return;
	}
	modified_blocks.insert(block_id);
}

void SingleFileBlockManager::IncreaseBlockReferenceCount(block_id_t block_id) {
	D_ASSERT(free_list.find(block_id) == free_list.end());
	auto entry = multi_use_blocks.find(block_id);
	if (entry != multi_use_blocks.end()) {
		entry->second++;
	} else {
		multi_use_blocks[block_id] = 2;
	}
}

block_id_t SingleFileBlockManager::GetMetaBlock() {
	return meta_block;
}

unique_ptr<Block> SingleFileBlockManager::CreateBlock(block_id_t block_id) {
	return make_unique<Block>(Allocator::Get(db), block_id);
}

void SingleFileBlockManager::Read(Block &block) {
	D_ASSERT(block.id >= 0);
	D_ASSERT(std::find(free_list.begin(), free_list.end(), block.id) == free_list.end());
	block.ReadAndChecksum(*handle, BLOCK_START + block.id * Storage::BLOCK_ALLOC_SIZE);
}

void SingleFileBlockManager::Write(FileBuffer &buffer, block_id_t block_id) {
	D_ASSERT(block_id >= 0);
	buffer.ChecksumAndWrite(*handle, BLOCK_START + block_id * Storage::BLOCK_ALLOC_SIZE);
}

vector<block_id_t> SingleFileBlockManager::GetFreeListBlocks() {
	vector<block_id_t> free_list_blocks;

	if (!free_list.empty() || !multi_use_blocks.empty() || !modified_blocks.empty()) {
		// there are blocks in the free list or multi_use_blocks
		// figure out how many blocks we need to write these to the file
		auto free_list_size = sizeof(uint64_t) + sizeof(block_id_t) * (free_list.size() + modified_blocks.size());
		auto multi_use_blocks_size =
		    sizeof(uint64_t) + (sizeof(block_id_t) + sizeof(uint32_t)) * multi_use_blocks.size();
		auto total_size = free_list_size + multi_use_blocks_size;
		// because of potential alignment issues and needing to store a next pointer in a block we subtract
		// a bit from the max block size
		auto space_in_block = Storage::BLOCK_SIZE - 4 * sizeof(block_id_t);
		auto total_blocks = (total_size + space_in_block - 1) / space_in_block;
		auto &config = DBConfig::GetConfig(db);
		if (config.debug_many_free_list_blocks) {
			total_blocks++;
		}
		D_ASSERT(total_size > 0);
		D_ASSERT(total_blocks > 0);

		// reserve the blocks that we are going to write
		// since these blocks are no longer free we cannot just include them in the free list!
		for (idx_t i = 0; i < total_blocks; i++) {
			auto block_id = GetFreeBlockId();
			free_list_blocks.push_back(block_id);
		}
	}

	return free_list_blocks;
}

class FreeListBlockWriter : public MetaBlockWriter {
public:
	FreeListBlockWriter(DatabaseInstance &db_p, vector<block_id_t> &free_list_blocks_p)
	    : MetaBlockWriter(db_p, free_list_blocks_p[0]), free_list_blocks(free_list_blocks_p), index(1) {
	}

	vector<block_id_t> &free_list_blocks;
	idx_t index;

protected:
	block_id_t GetNextBlockId() override {
		if (index >= free_list_blocks.size()) {
			throw InternalException(
			    "Free List Block Writer ran out of blocks, this means not enough blocks were allocated up front");
		}
		return free_list_blocks[index++];
	}
};

void SingleFileBlockManager::WriteHeader(DatabaseHeader header) {
	// set the iteration count
	header.iteration = ++iteration_count;

	vector<block_id_t> free_list_blocks = GetFreeListBlocks();

	// now handle the free list
	// add all modified blocks to the free list: they can now be written to again
	for (auto &block : modified_blocks) {
		free_list.insert(block);
	}
	modified_blocks.clear();

	if (!free_list_blocks.empty()) {
		// there are blocks to write, either in the free_list or in the modified_blocks
		// we write these blocks specifically to the free_list_blocks
		// a normal MetaBlockWriter will fetch blocks to use from the free_list
		// but since we are WRITING the free_list, this behavior is sub-optimal

		FreeListBlockWriter writer(db, free_list_blocks);

		D_ASSERT(writer.block->id == free_list_blocks[0]);
		header.free_list = writer.block->id;
		for (auto &block_id : free_list_blocks) {
			modified_blocks.insert(block_id);
		}

		writer.Write<uint64_t>(free_list.size());
		for (auto &block_id : free_list) {
			writer.Write<block_id_t>(block_id);
		}
		writer.Write<uint64_t>(multi_use_blocks.size());
		for (auto &entry : multi_use_blocks) {
			writer.Write<block_id_t>(entry.first);
			writer.Write<uint32_t>(entry.second);
		}
		writer.Flush();
	} else {
		// no blocks in the free list
		header.free_list = INVALID_BLOCK;
	}
	header.block_count = max_block;

	auto &config = DBConfig::GetConfig(db);
	if (config.checkpoint_abort == CheckpointAbort::DEBUG_ABORT_AFTER_FREE_LIST_WRITE) {
		throw IOException("Checkpoint aborted after free list write because of PRAGMA checkpoint_abort flag");
	}

	if (!use_direct_io) {
		// if we are not using Direct IO we need to fsync BEFORE we write the header to ensure that all the previous
		// blocks are written as well
		handle->Sync();
	}
	// set the header inside the buffer
	header_buffer.Clear();
	Store<DatabaseHeader>(header, header_buffer.buffer);
	// now write the header to the file, active_header determines whether we write to h1 or h2
	// note that if active_header is h1 we write to h2, and vice versa
	header_buffer.ChecksumAndWrite(*handle,
	                               active_header == 1 ? Storage::FILE_HEADER_SIZE : Storage::FILE_HEADER_SIZE * 2);
	// switch active header to the other header
	active_header = 1 - active_header;
	//! Ensure the header write ends up on disk
	handle->Sync();
}

} // namespace duckdb











namespace duckdb {

BaseStatistics::BaseStatistics(LogicalType type, StatisticsType stats_type) : type(move(type)), stats_type(stats_type) {
}

BaseStatistics::~BaseStatistics() {
}

void BaseStatistics::InitializeBase() {
	validity_stats = make_unique<ValidityStatistics>(false);
	if (stats_type == GLOBAL_STATS) {
		distinct_stats = make_unique<DistinctStatistics>();
	}
}

bool BaseStatistics::CanHaveNull() const {
	if (!validity_stats) {
		// we don't know
		// solid maybe
		return true;
	}
	return ((ValidityStatistics &)*validity_stats).has_null;
}

bool BaseStatistics::CanHaveNoNull() const {
	if (!validity_stats) {
		// we don't know
		// solid maybe
		return true;
	}
	return ((ValidityStatistics &)*validity_stats).has_no_null;
}

void BaseStatistics::UpdateDistinctStatistics(Vector &v, idx_t count) {
	if (!distinct_stats) {
		return;
	}
	auto &d_stats = (DistinctStatistics &)*distinct_stats;
	d_stats.Update(v, count);
}

void MergeInternal(unique_ptr<BaseStatistics> &orig, const unique_ptr<BaseStatistics> &other) {
	if (other) {
		if (orig) {
			orig->Merge(*other);
		} else {
			orig = other->Copy();
		}
	}
}

void BaseStatistics::Merge(const BaseStatistics &other) {
	D_ASSERT(type == other.type);
	MergeInternal(validity_stats, other.validity_stats);
	if (stats_type == GLOBAL_STATS) {
		MergeInternal(distinct_stats, other.distinct_stats);
	}
}

unique_ptr<BaseStatistics> BaseStatistics::CreateEmpty(LogicalType type, StatisticsType stats_type) {
	unique_ptr<BaseStatistics> result;
	switch (type.InternalType()) {
	case PhysicalType::BIT:
		return make_unique<ValidityStatistics>(false, false);
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
	case PhysicalType::INT16:
	case PhysicalType::INT32:
	case PhysicalType::INT64:
	case PhysicalType::UINT8:
	case PhysicalType::UINT16:
	case PhysicalType::UINT32:
	case PhysicalType::UINT64:
	case PhysicalType::INT128:
	case PhysicalType::FLOAT:
	case PhysicalType::DOUBLE:
		result = make_unique<NumericStatistics>(move(type), stats_type);
		break;
	case PhysicalType::VARCHAR:
		result = make_unique<StringStatistics>(move(type), stats_type);
		break;
	case PhysicalType::STRUCT:
		result = make_unique<StructStatistics>(move(type));
		break;
	case PhysicalType::LIST:
		result = make_unique<ListStatistics>(move(type));
		break;
	case PhysicalType::INTERVAL:
	default:
		result = make_unique<BaseStatistics>(move(type), stats_type);
	}
	result->InitializeBase();
	return result;
}

unique_ptr<BaseStatistics> BaseStatistics::Copy() const {
	auto result = make_unique<BaseStatistics>(type, stats_type);
	result->CopyBase(*this);
	return result;
}

void BaseStatistics::CopyBase(const BaseStatistics &orig) {
	if (orig.validity_stats) {
		validity_stats = orig.validity_stats->Copy();
	}
	if (orig.distinct_stats) {
		distinct_stats = orig.distinct_stats->Copy();
	}
}

void BaseStatistics::Serialize(Serializer &serializer) const {
	FieldWriter writer(serializer);
	ValidityStatistics(CanHaveNull(), CanHaveNoNull()).Serialize(writer);
	Serialize(writer);
	auto ptype = type.InternalType();
	if (ptype != PhysicalType::BIT) {
		writer.WriteField<StatisticsType>(stats_type);
		writer.WriteOptional<BaseStatistics>(distinct_stats);
	}
	writer.Finalize();
}

void BaseStatistics::Serialize(FieldWriter &writer) const {
}

unique_ptr<BaseStatistics> BaseStatistics::Deserialize(Deserializer &source, LogicalType type) {
	FieldReader reader(source);
	auto validity_stats = ValidityStatistics::Deserialize(reader);
	unique_ptr<BaseStatistics> result;
	auto ptype = type.InternalType();
	switch (ptype) {
	case PhysicalType::BIT:
		result = ValidityStatistics::Deserialize(reader);
		break;
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
	case PhysicalType::INT16:
	case PhysicalType::INT32:
	case PhysicalType::INT64:
	case PhysicalType::UINT8:
	case PhysicalType::UINT16:
	case PhysicalType::UINT32:
	case PhysicalType::UINT64:
	case PhysicalType::INT128:
	case PhysicalType::FLOAT:
	case PhysicalType::DOUBLE:
		result = NumericStatistics::Deserialize(reader, move(type));
		break;
	case PhysicalType::VARCHAR:
		result = StringStatistics::Deserialize(reader, move(type));
		break;
	case PhysicalType::STRUCT:
		result = StructStatistics::Deserialize(reader, move(type));
		break;
	case PhysicalType::LIST:
		result = ListStatistics::Deserialize(reader, move(type));
		break;
	case PhysicalType::INTERVAL:
		result = make_unique<BaseStatistics>(move(type), StatisticsType::LOCAL_STATS);
		break;
	default:
		throw InternalException("Unimplemented type for statistics deserialization");
	}

	if (ptype != PhysicalType::BIT) {
		result->validity_stats = move(validity_stats);
		result->stats_type = reader.ReadField<StatisticsType>(StatisticsType::LOCAL_STATS);
		result->distinct_stats = reader.ReadOptional<DistinctStatistics>(nullptr);
	}

	reader.Finalize();
	return result;
}

string BaseStatistics::ToString() const {
	return StringUtil::Format("%s%s", validity_stats ? validity_stats->ToString() : "",
	                          distinct_stats ? distinct_stats->ToString() : "");
}

void BaseStatistics::Verify(Vector &vector, const SelectionVector &sel, idx_t count) const {
	D_ASSERT(vector.GetType() == this->type);
	if (validity_stats) {
		validity_stats->Verify(vector, sel, count);
	}
}

void BaseStatistics::Verify(Vector &vector, idx_t count) const {
	auto sel = FlatVector::IncrementalSelectionVector();
	Verify(vector, *sel, count);
}

} // namespace duckdb


namespace duckdb {

ColumnStatistics::ColumnStatistics(unique_ptr<BaseStatistics> stats_p) : stats(move(stats_p)) {
}

shared_ptr<ColumnStatistics> ColumnStatistics::CreateEmptyStats(const LogicalType &type) {
	auto col_stats = BaseStatistics::CreateEmpty(type, StatisticsType::GLOBAL_STATS);
	return make_shared<ColumnStatistics>(move(col_stats));
}

} // namespace duckdb





#include <math.h>

namespace duckdb {

DistinctStatistics::DistinctStatistics()
    : BaseStatistics(LogicalType::INVALID, StatisticsType::LOCAL_STATS), log(make_unique<HyperLogLog>()),
      sample_count(0), total_count(0) {
}

DistinctStatistics::DistinctStatistics(unique_ptr<HyperLogLog> log, idx_t sample_count, idx_t total_count)
    : BaseStatistics(LogicalType::INVALID, StatisticsType::LOCAL_STATS), log(move(log)), sample_count(sample_count),
      total_count(total_count) {
}

unique_ptr<BaseStatistics> DistinctStatistics::Copy() const {
	return make_unique<DistinctStatistics>(log->Copy(), sample_count, total_count);
}

void DistinctStatistics::Merge(const BaseStatistics &other_p) {
	BaseStatistics::Merge(other_p);
	auto &other = (const DistinctStatistics &)other_p;
	log->Merge(*other.log);
	sample_count += other.sample_count;
	total_count += other.total_count;
}

void DistinctStatistics::Serialize(Serializer &serializer) const {
	FieldWriter writer(serializer);
	Serialize(writer);
	writer.Finalize();
}

void DistinctStatistics::Serialize(FieldWriter &writer) const {
	writer.WriteField<idx_t>(sample_count);
	writer.WriteField<idx_t>(total_count);
	log->Serialize(writer);
}

unique_ptr<DistinctStatistics> DistinctStatistics::Deserialize(Deserializer &source) {
	FieldReader reader(source);
	auto result = Deserialize(reader);
	reader.Finalize();
	return result;
}

unique_ptr<DistinctStatistics> DistinctStatistics::Deserialize(FieldReader &reader) {
	auto sample_count = reader.ReadRequired<idx_t>();
	auto total_count = reader.ReadRequired<idx_t>();
	return make_unique<DistinctStatistics>(HyperLogLog::Deserialize(reader), sample_count, total_count);
}

void DistinctStatistics::Update(Vector &v, idx_t count) {
	VectorData vdata;
	v.Orrify(count, vdata);
	Update(vdata, v.GetType(), count);
}

void DistinctStatistics::Update(VectorData &vdata, const LogicalType &type, idx_t count) {
	if (count == 0) {
		return;
	}
	total_count += count;
	count = MinValue<idx_t>(idx_t(SAMPLE_RATE * MaxValue<idx_t>(STANDARD_VECTOR_SIZE, count)), count);
	sample_count += count;

	uint64_t indices[STANDARD_VECTOR_SIZE];
	uint8_t counts[STANDARD_VECTOR_SIZE];

	HyperLogLog::ProcessEntries(vdata, type, indices, counts, count);
	log->AddToLog(vdata, count, indices, counts);
}

string DistinctStatistics::ToString() const {
	return StringUtil::Format("[Approx Unique: %s]", to_string(GetCount()));
}

idx_t DistinctStatistics::GetCount() const {
	if (sample_count == 0 || total_count == 0) {
		return 0;
	}

	double u = MinValue<idx_t>(log->Count(), sample_count);
	double s = sample_count;
	double n = total_count;

	// Assume this proportion of the the sampled values occurred only once
	double u1 = pow(u / s, 2) * u;

	// Estimate total uniques using Good Turing Estimation
	idx_t estimate = u + u1 / s * (n - s);
	return MinValue<idx_t>(estimate, total_count);
}

} // namespace duckdb






namespace duckdb {

ListStatistics::ListStatistics(LogicalType type_p) : BaseStatistics(move(type_p), StatisticsType::LOCAL_STATS) {
	D_ASSERT(type.InternalType() == PhysicalType::LIST);
	InitializeBase();
	auto &child_type = ListType::GetChildType(type);
	child_stats = BaseStatistics::CreateEmpty(child_type, StatisticsType::LOCAL_STATS);
}

void ListStatistics::Merge(const BaseStatistics &other_p) {
	BaseStatistics::Merge(other_p);

	auto &other = (const ListStatistics &)other_p;
	if (child_stats && other.child_stats) {
		child_stats->Merge(*other.child_stats);
	} else {
		child_stats.reset();
	}
}

// LCOV_EXCL_START
FilterPropagateResult ListStatistics::CheckZonemap(ExpressionType comparison_type, const Value &constant) const {
	throw InternalException("List zonemaps are not supported yet");
}
// LCOV_EXCL_STOP

unique_ptr<BaseStatistics> ListStatistics::Copy() const {
	auto result = make_unique<ListStatistics>(type);
	result->CopyBase(*this);

	result->child_stats = child_stats ? child_stats->Copy() : nullptr;
	return move(result);
}

void ListStatistics::Serialize(FieldWriter &writer) const {
	writer.WriteSerializable(*child_stats);
}

unique_ptr<BaseStatistics> ListStatistics::Deserialize(FieldReader &reader, LogicalType type) {
	D_ASSERT(type.InternalType() == PhysicalType::LIST);
	auto result = make_unique<ListStatistics>(move(type));
	auto &child_type = ListType::GetChildType(result->type);
	result->child_stats = reader.ReadRequiredSerializable<BaseStatistics>(child_type);
	return move(result);
}

string ListStatistics::ToString() const {
	return StringUtil::Format("[%s]%s", child_stats ? child_stats->ToString() : "No Stats", BaseStatistics::ToString());
}

void ListStatistics::Verify(Vector &vector, const SelectionVector &sel, idx_t count) const {
	BaseStatistics::Verify(vector, sel, count);

	if (child_stats) {
		auto &child_entry = ListVector::GetEntry(vector);
		VectorData vdata;
		vector.Orrify(count, vdata);

		auto list_data = (list_entry_t *)vdata.data;
		idx_t total_list_count = 0;
		for (idx_t i = 0; i < count; i++) {
			auto idx = sel.get_index(i);
			auto index = vdata.sel->get_index(idx);
			auto list = list_data[index];
			if (vdata.validity.RowIsValid(index)) {
				for (idx_t list_idx = 0; list_idx < list.length; list_idx++) {
					total_list_count++;
				}
			}
		}
		SelectionVector list_sel(total_list_count);
		idx_t list_count = 0;
		for (idx_t i = 0; i < count; i++) {
			auto idx = sel.get_index(i);
			auto index = vdata.sel->get_index(idx);
			auto list = list_data[index];
			if (vdata.validity.RowIsValid(index)) {
				for (idx_t list_idx = 0; list_idx < list.length; list_idx++) {
					list_sel.set_index(list_count++, list.offset + list_idx);
				}
			}
		}

		child_stats->Verify(child_entry, list_sel, list_count);
	}
}

} // namespace duckdb






namespace duckdb {

template <>
void NumericStatistics::Update<interval_t>(SegmentStatistics &stats, interval_t new_value) {
}

template <>
void NumericStatistics::Update<list_entry_t>(SegmentStatistics &stats, list_entry_t new_value) {
}

NumericStatistics::NumericStatistics(LogicalType type_p, StatisticsType stats_type)
    : BaseStatistics(move(type_p), stats_type) {
	InitializeBase();
	min = Value::MaximumValue(type);
	max = Value::MinimumValue(type);
}

NumericStatistics::NumericStatistics(LogicalType type_p, Value min_p, Value max_p, StatisticsType stats_type)
    : BaseStatistics(move(type_p), stats_type), min(move(min_p)), max(move(max_p)) {
	InitializeBase();
}

void NumericStatistics::Merge(const BaseStatistics &other_p) {
	BaseStatistics::Merge(other_p);
	auto &other = (const NumericStatistics &)other_p;
	if (other.min.IsNull() || min.IsNull()) {
		min = Value(type);
	} else if (other.min < min) {
		min = other.min;
	}
	if (other.max.IsNull() || max.IsNull()) {
		max = Value(type);
	} else if (other.max > max) {
		max = other.max;
	}
}

FilterPropagateResult NumericStatistics::CheckZonemap(ExpressionType comparison_type, const Value &constant) const {
	if (constant.IsNull()) {
		return FilterPropagateResult::FILTER_ALWAYS_FALSE;
	}
	if (min.IsNull() || max.IsNull()) {
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}
	switch (comparison_type) {
	case ExpressionType::COMPARE_EQUAL:
		if (constant == min && constant == max) {
			return FilterPropagateResult::FILTER_ALWAYS_TRUE;
		} else if (constant >= min && constant <= max) {
			return FilterPropagateResult::NO_PRUNING_POSSIBLE;
		} else {
			return FilterPropagateResult::FILTER_ALWAYS_FALSE;
		}
	case ExpressionType::COMPARE_NOTEQUAL:
		if (constant < min || constant > max) {
			return FilterPropagateResult::FILTER_ALWAYS_TRUE;
		} else if (min == max && min == constant) {
			// corner case of a cluster with one numeric equal to the target constant
			return FilterPropagateResult::FILTER_ALWAYS_FALSE;
		}
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		// X >= C
		// this can be true only if max(X) >= C
		// if min(X) >= C, then this is always true
		if (min >= constant) {
			return FilterPropagateResult::FILTER_ALWAYS_TRUE;
		} else if (max >= constant) {
			return FilterPropagateResult::NO_PRUNING_POSSIBLE;
		} else {
			return FilterPropagateResult::FILTER_ALWAYS_FALSE;
		}
	case ExpressionType::COMPARE_GREATERTHAN:
		// X > C
		// this can be true only if max(X) > C
		// if min(X) > C, then this is always true
		if (min > constant) {
			return FilterPropagateResult::FILTER_ALWAYS_TRUE;
		} else if (max > constant) {
			return FilterPropagateResult::NO_PRUNING_POSSIBLE;
		} else {
			return FilterPropagateResult::FILTER_ALWAYS_FALSE;
		}
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		// X <= C
		// this can be true only if min(X) <= C
		// if max(X) <= C, then this is always true
		if (max <= constant) {
			return FilterPropagateResult::FILTER_ALWAYS_TRUE;
		} else if (min <= constant) {
			return FilterPropagateResult::NO_PRUNING_POSSIBLE;
		} else {
			return FilterPropagateResult::FILTER_ALWAYS_FALSE;
		}
	case ExpressionType::COMPARE_LESSTHAN:
		// X < C
		// this can be true only if min(X) < C
		// if max(X) < C, then this is always true
		if (max < constant) {
			return FilterPropagateResult::FILTER_ALWAYS_TRUE;
		} else if (min < constant) {
			return FilterPropagateResult::NO_PRUNING_POSSIBLE;
		} else {
			return FilterPropagateResult::FILTER_ALWAYS_FALSE;
		}
	default:
		throw InternalException("Expression type in zonemap check not implemented");
	}
}

unique_ptr<BaseStatistics> NumericStatistics::Copy() const {
	auto result = make_unique<NumericStatistics>(type, min, max, stats_type);
	result->CopyBase(*this);
	return move(result);
}

bool NumericStatistics::IsConstant() const {
	return max <= min;
}

void NumericStatistics::Serialize(FieldWriter &writer) const {
	writer.WriteSerializable(min);
	writer.WriteSerializable(max);
}

unique_ptr<BaseStatistics> NumericStatistics::Deserialize(FieldReader &reader, LogicalType type) {
	auto min = reader.ReadRequiredSerializable<Value, Value>();
	auto max = reader.ReadRequiredSerializable<Value, Value>();
	return make_unique_base<BaseStatistics, NumericStatistics>(move(type), min, max, StatisticsType::LOCAL_STATS);
}

string NumericStatistics::ToString() const {
	return StringUtil::Format("[Min: %s, Max: %s]%s", min.ToString(), max.ToString(), BaseStatistics::ToString());
}

template <class T>
void NumericStatistics::TemplatedVerify(Vector &vector, const SelectionVector &sel, idx_t count) const {
	VectorData vdata;
	vector.Orrify(count, vdata);

	auto data = (T *)vdata.data;
	for (idx_t i = 0; i < count; i++) {
		auto idx = sel.get_index(i);
		auto index = vdata.sel->get_index(idx);
		if (!vdata.validity.RowIsValid(index)) {
			continue;
		}
		if (!min.IsNull() && LessThan::Operation(data[index], min.GetValueUnsafe<T>())) { // LCOV_EXCL_START
			throw InternalException("Statistics mismatch: value is smaller than min.\nStatistics: %s\nVector: %s",
			                        ToString(), vector.ToString(count));
		} // LCOV_EXCL_STOP
		if (!max.IsNull() && GreaterThan::Operation(data[index], max.GetValueUnsafe<T>())) {
			throw InternalException("Statistics mismatch: value is bigger than max.\nStatistics: %s\nVector: %s",
			                        ToString(), vector.ToString(count));
		}
	}
}

void NumericStatistics::Verify(Vector &vector, const SelectionVector &sel, idx_t count) const {
	BaseStatistics::Verify(vector, sel, count);

	switch (type.InternalType()) {
	case PhysicalType::BOOL:
		break;
	case PhysicalType::INT8:
		TemplatedVerify<int8_t>(vector, sel, count);
		break;
	case PhysicalType::INT16:
		TemplatedVerify<int16_t>(vector, sel, count);
		break;
	case PhysicalType::INT32:
		TemplatedVerify<int32_t>(vector, sel, count);
		break;
	case PhysicalType::INT64:
		TemplatedVerify<int64_t>(vector, sel, count);
		break;
	case PhysicalType::UINT8:
		TemplatedVerify<uint8_t>(vector, sel, count);
		break;
	case PhysicalType::UINT16:
		TemplatedVerify<uint16_t>(vector, sel, count);
		break;
	case PhysicalType::UINT32:
		TemplatedVerify<uint32_t>(vector, sel, count);
		break;
	case PhysicalType::UINT64:
		TemplatedVerify<uint64_t>(vector, sel, count);
		break;
	case PhysicalType::INT128:
		TemplatedVerify<hugeint_t>(vector, sel, count);
		break;
	case PhysicalType::FLOAT:
		TemplatedVerify<float>(vector, sel, count);
		break;
	case PhysicalType::DOUBLE:
		TemplatedVerify<double>(vector, sel, count);
		break;
	default:
		throw InternalException("Unsupported type %s for numeric statistics verify", type.ToString());
	}
}

} // namespace duckdb




namespace duckdb {

SegmentStatistics::SegmentStatistics(LogicalType type) : type(move(type)) {
	Reset();
}

SegmentStatistics::SegmentStatistics(LogicalType type, unique_ptr<BaseStatistics> stats)
    : type(move(type)), statistics(move(stats)) {
	if (!statistics) {
		Reset();
	}
}

void SegmentStatistics::Reset() {
	statistics = BaseStatistics::CreateEmpty(type, StatisticsType::LOCAL_STATS);
}

} // namespace duckdb






namespace duckdb {

StringStatistics::StringStatistics(LogicalType type_p, StatisticsType stats_type)
    : BaseStatistics(move(type_p), stats_type) {
	InitializeBase();
	for (idx_t i = 0; i < MAX_STRING_MINMAX_SIZE; i++) {
		min[i] = 0xFF;
		max[i] = 0;
	}
	max_string_length = 0;
	has_unicode = false;
	has_overflow_strings = false;
}

unique_ptr<BaseStatistics> StringStatistics::Copy() const {
	auto result = make_unique<StringStatistics>(type, stats_type);
	result->CopyBase(*this);

	memcpy(result->min, min, MAX_STRING_MINMAX_SIZE);
	memcpy(result->max, max, MAX_STRING_MINMAX_SIZE);
	result->has_unicode = has_unicode;
	result->max_string_length = max_string_length;
	return move(result);
}

void StringStatistics::Serialize(FieldWriter &writer) const {
	writer.WriteBlob(min, MAX_STRING_MINMAX_SIZE);
	writer.WriteBlob(max, MAX_STRING_MINMAX_SIZE);
	writer.WriteField<bool>(has_unicode);
	writer.WriteField<uint32_t>(max_string_length);
	writer.WriteField<bool>(has_overflow_strings);
}

unique_ptr<BaseStatistics> StringStatistics::Deserialize(FieldReader &reader, LogicalType type) {
	auto stats = make_unique<StringStatistics>(move(type), StatisticsType::LOCAL_STATS);
	reader.ReadBlob(stats->min, MAX_STRING_MINMAX_SIZE);
	reader.ReadBlob(stats->max, MAX_STRING_MINMAX_SIZE);
	stats->has_unicode = reader.ReadRequired<bool>();
	stats->max_string_length = reader.ReadRequired<uint32_t>();
	stats->has_overflow_strings = reader.ReadRequired<bool>();
	return move(stats);
}

static int StringValueComparison(const_data_ptr_t data, idx_t len, const_data_ptr_t comparison) {
	D_ASSERT(len <= StringStatistics::MAX_STRING_MINMAX_SIZE);
	for (idx_t i = 0; i < len; i++) {
		if (data[i] < comparison[i]) {
			return -1;
		} else if (data[i] > comparison[i]) {
			return 1;
		}
	}
	return 0;
}

static void ConstructValue(const_data_ptr_t data, idx_t size, data_t target[]) {
	idx_t value_size =
	    size > StringStatistics::MAX_STRING_MINMAX_SIZE ? StringStatistics::MAX_STRING_MINMAX_SIZE : size;
	memcpy(target, data, value_size);
	for (idx_t i = value_size; i < StringStatistics::MAX_STRING_MINMAX_SIZE; i++) {
		target[i] = '\0';
	}
}

void StringStatistics::Update(const string_t &value) {
	auto data = (const_data_ptr_t)value.GetDataUnsafe();
	auto size = value.GetSize();

	//! we can only fit 8 bytes, so we might need to trim our string
	// construct the value
	data_t target[MAX_STRING_MINMAX_SIZE];
	ConstructValue(data, size, target);

	// update the min and max
	if (StringValueComparison(target, MAX_STRING_MINMAX_SIZE, min) < 0) {
		memcpy(min, target, MAX_STRING_MINMAX_SIZE);
	}
	if (StringValueComparison(target, MAX_STRING_MINMAX_SIZE, max) > 0) {
		memcpy(max, target, MAX_STRING_MINMAX_SIZE);
	}
	if (size > max_string_length) {
		max_string_length = size;
	}
	if (type.id() == LogicalTypeId::VARCHAR && !has_unicode) {
		auto unicode = Utf8Proc::Analyze((const char *)data, size);
		if (unicode == UnicodeType::UNICODE) {
			has_unicode = true;
		} else if (unicode == UnicodeType::INVALID) {
			throw InternalException("Invalid unicode detected in segment statistics update!");
		}
	}
}

void StringStatistics::Merge(const BaseStatistics &other_p) {
	BaseStatistics::Merge(other_p);
	auto &other = (const StringStatistics &)other_p;
	if (StringValueComparison(other.min, MAX_STRING_MINMAX_SIZE, min) < 0) {
		memcpy(min, other.min, MAX_STRING_MINMAX_SIZE);
	}
	if (StringValueComparison(other.max, MAX_STRING_MINMAX_SIZE, max) > 0) {
		memcpy(max, other.max, MAX_STRING_MINMAX_SIZE);
	}
	has_unicode = has_unicode || other.has_unicode;
	max_string_length = MaxValue<uint32_t>(max_string_length, other.max_string_length);
	has_overflow_strings = has_overflow_strings || other.has_overflow_strings;
}

FilterPropagateResult StringStatistics::CheckZonemap(ExpressionType comparison_type, const string &constant) const {
	auto data = (const_data_ptr_t)constant.c_str();
	auto size = constant.size();

	idx_t value_size = size > MAX_STRING_MINMAX_SIZE ? MAX_STRING_MINMAX_SIZE : size;
	int min_comp = StringValueComparison(data, value_size, min);
	int max_comp = StringValueComparison(data, value_size, max);
	switch (comparison_type) {
	case ExpressionType::COMPARE_EQUAL:
		if (min_comp >= 0 && max_comp <= 0) {
			return FilterPropagateResult::NO_PRUNING_POSSIBLE;
		} else {
			return FilterPropagateResult::FILTER_ALWAYS_FALSE;
		}
	case ExpressionType::COMPARE_NOTEQUAL:
		if (min_comp < 0 || max_comp > 0) {
			return FilterPropagateResult::FILTER_ALWAYS_TRUE;
		}
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
	case ExpressionType::COMPARE_GREATERTHAN:
		if (max_comp <= 0) {
			return FilterPropagateResult::NO_PRUNING_POSSIBLE;
		} else {
			return FilterPropagateResult::FILTER_ALWAYS_FALSE;
		}
	case ExpressionType::COMPARE_LESSTHAN:
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		if (min_comp >= 0) {
			return FilterPropagateResult::NO_PRUNING_POSSIBLE;
		} else {
			return FilterPropagateResult::FILTER_ALWAYS_FALSE;
		}
	default:
		throw InternalException("Expression type not implemented for string statistics zone map");
	}
}

static idx_t GetValidMinMaxSubstring(const_data_ptr_t data) {
	for (idx_t i = 0; i < StringStatistics::MAX_STRING_MINMAX_SIZE; i++) {
		if (data[i] == '\0') {
			return i;
		}
		if ((data[i] & 0x80) != 0) {
			return i;
		}
	}
	return StringStatistics::MAX_STRING_MINMAX_SIZE;
}

string StringStatistics::ToString() const {
	idx_t min_len = GetValidMinMaxSubstring(min);
	idx_t max_len = GetValidMinMaxSubstring(max);
	return StringUtil::Format("[Min: %s, Max: %s, Has Unicode: %s, Max String Length: %lld]%s",
	                          string((const char *)min, min_len), string((const char *)max, max_len),
	                          has_unicode ? "true" : "false", max_string_length, BaseStatistics::ToString());
}

void StringStatistics::Verify(Vector &vector, const SelectionVector &sel, idx_t count) const {
	BaseStatistics::Verify(vector, sel, count);

	string_t min_string((const char *)min, MAX_STRING_MINMAX_SIZE);
	string_t max_string((const char *)max, MAX_STRING_MINMAX_SIZE);

	VectorData vdata;
	vector.Orrify(count, vdata);
	auto data = (string_t *)vdata.data;
	for (idx_t i = 0; i < count; i++) {
		auto idx = sel.get_index(i);
		auto index = vdata.sel->get_index(idx);
		if (!vdata.validity.RowIsValid(index)) {
			continue;
		}
		auto value = data[index];
		auto data = value.GetDataUnsafe();
		auto len = value.GetSize();
		// LCOV_EXCL_START
		if (len > max_string_length) {
			throw InternalException(
			    "Statistics mismatch: string value exceeds maximum string length.\nStatistics: %s\nVector: %s",
			    ToString(), vector.ToString(count));
		}
		if (type.id() == LogicalTypeId::VARCHAR && !has_unicode) {
			auto unicode = Utf8Proc::Analyze(data, len);
			if (unicode == UnicodeType::UNICODE) {
				throw InternalException("Statistics mismatch: string value contains unicode, but statistics says it "
				                        "shouldn't.\nStatistics: %s\nVector: %s",
				                        ToString(), vector.ToString(count));
			} else if (unicode == UnicodeType::INVALID) {
				throw InternalException("Invalid unicode detected in vector: %s", vector.ToString(count));
			}
		}
		if (StringValueComparison((const_data_ptr_t)data, MinValue<idx_t>(len, MAX_STRING_MINMAX_SIZE), min) < 0) {
			throw InternalException("Statistics mismatch: value is smaller than min.\nStatistics: %s\nVector: %s",
			                        ToString(), vector.ToString(count));
		}
		if (StringValueComparison((const_data_ptr_t)data, MinValue<idx_t>(len, MAX_STRING_MINMAX_SIZE), max) > 0) {
			throw InternalException("Statistics mismatch: value is bigger than max.\nStatistics: %s\nVector: %s",
			                        ToString(), vector.ToString(count));
		}
		// LCOV_EXCL_STOP
	}
}

} // namespace duckdb





namespace duckdb {

StructStatistics::StructStatistics(LogicalType type_p) : BaseStatistics(move(type_p), StatisticsType::LOCAL_STATS) {
	D_ASSERT(type.InternalType() == PhysicalType::STRUCT);
	InitializeBase();

	auto &child_types = StructType::GetChildTypes(type);
	child_stats.resize(child_types.size());
	for (idx_t i = 0; i < child_types.size(); i++) {
		child_stats[i] = BaseStatistics::CreateEmpty(child_types[i].second, StatisticsType::LOCAL_STATS);
	}
}

void StructStatistics::Merge(const BaseStatistics &other_p) {
	BaseStatistics::Merge(other_p);

	auto &other = (const StructStatistics &)other_p;
	D_ASSERT(other.child_stats.size() == child_stats.size());
	for (idx_t i = 0; i < child_stats.size(); i++) {
		if (child_stats[i] && other.child_stats[i]) {
			child_stats[i]->Merge(*other.child_stats[i]);
		} else {
			child_stats[i].reset();
		}
	}
}

// LCOV_EXCL_START
FilterPropagateResult StructStatistics::CheckZonemap(ExpressionType comparison_type, const Value &constant) const {
	throw InternalException("Struct zonemaps are not supported yet");
}
// LCOV_EXCL_STOP

unique_ptr<BaseStatistics> StructStatistics::Copy() const {
	auto result = make_unique<StructStatistics>(type);
	result->CopyBase(*this);

	for (idx_t i = 0; i < child_stats.size(); i++) {
		result->child_stats[i] = child_stats[i] ? child_stats[i]->Copy() : nullptr;
	}
	return move(result);
}

void StructStatistics::Serialize(FieldWriter &writer) const {
	writer.WriteField<uint32_t>(child_stats.size());
	auto &serializer = writer.GetSerializer();
	for (idx_t i = 0; i < child_stats.size(); i++) {
		serializer.Write<bool>(child_stats[i] ? true : false);
		if (child_stats[i]) {
			child_stats[i]->Serialize(serializer);
		}
	}
}

unique_ptr<BaseStatistics> StructStatistics::Deserialize(FieldReader &reader, LogicalType type) {
	D_ASSERT(type.InternalType() == PhysicalType::STRUCT);
	auto result = make_unique<StructStatistics>(move(type));
	auto &child_types = StructType::GetChildTypes(result->type);

	auto child_type_count = reader.ReadRequired<uint32_t>();
	if (child_types.size() != child_type_count) {
		throw InternalException("Struct stats deserialization failure: child count does not match type count!");
	}
	auto &source = reader.GetSource();
	for (idx_t i = 0; i < child_types.size(); i++) {
		auto has_child = source.Read<bool>();
		if (has_child) {
			result->child_stats[i] = BaseStatistics::Deserialize(source, child_types[i].second);
		} else {
			result->child_stats[i].reset();
		}
	}
	return move(result);
}

string StructStatistics::ToString() const {
	string result;
	result += " {";
	auto &child_types = StructType::GetChildTypes(type);
	for (idx_t i = 0; i < child_types.size(); i++) {
		if (i > 0) {
			result += ", ";
		}
		result += child_types[i].first + ": " + (child_stats[i] ? child_stats[i]->ToString() : "No Stats");
	}
	result += "}";
	result += BaseStatistics::ToString();
	return result;
}

void StructStatistics::Verify(Vector &vector, const SelectionVector &sel, idx_t count) const {
	BaseStatistics::Verify(vector, sel, count);

	auto &child_entries = StructVector::GetEntries(vector);
	for (idx_t i = 0; i < child_entries.size(); i++) {
		if (child_stats[i]) {
			child_stats[i]->Verify(*child_entries[i], sel, count);
		}
	}
}

} // namespace duckdb







namespace duckdb {

ValidityStatistics::ValidityStatistics(bool has_null, bool has_no_null)
    : BaseStatistics(LogicalType(LogicalTypeId::VALIDITY), StatisticsType::LOCAL_STATS), has_null(has_null),
      has_no_null(has_no_null) {
}

unique_ptr<BaseStatistics> ValidityStatistics::Combine(const unique_ptr<BaseStatistics> &lstats,
                                                       const unique_ptr<BaseStatistics> &rstats) {
	if (!lstats && !rstats) {
		return nullptr;
	} else if (!lstats) {
		return rstats->Copy();
	} else if (!rstats) {
		return lstats->Copy();
	} else {
		auto &l = (ValidityStatistics &)*lstats;
		auto &r = (ValidityStatistics &)*rstats;
		return make_unique<ValidityStatistics>(l.has_null || r.has_null, l.has_no_null || r.has_no_null);
	}
}

bool ValidityStatistics::IsConstant() const {
	if (!has_null) {
		return true;
	}
	if (!has_no_null) {
		return true;
	}
	return false;
}

void ValidityStatistics::Merge(const BaseStatistics &other_p) {
	auto &other = (ValidityStatistics &)other_p;
	has_null = has_null || other.has_null;
	has_no_null = has_no_null || other.has_no_null;
}

unique_ptr<BaseStatistics> ValidityStatistics::Copy() const {
	return make_unique<ValidityStatistics>(has_null, has_no_null);
}

void ValidityStatistics::Serialize(FieldWriter &writer) const {
	writer.WriteField<bool>(has_null);
	writer.WriteField<bool>(has_no_null);
}

unique_ptr<ValidityStatistics> ValidityStatistics::Deserialize(FieldReader &reader) {
	bool has_null = reader.ReadRequired<bool>();
	bool has_no_null = reader.ReadRequired<bool>();
	return make_unique<ValidityStatistics>(has_null, has_no_null);
}

void ValidityStatistics::Verify(Vector &vector, const SelectionVector &sel, idx_t count) const {
	if (has_null && has_no_null) {
		// nothing to verify
		return;
	}
	VectorData vdata;
	vector.Orrify(count, vdata);
	for (idx_t i = 0; i < count; i++) {
		auto idx = sel.get_index(i);
		auto index = vdata.sel->get_index(idx);
		bool row_is_valid = vdata.validity.RowIsValid(index);
		if (row_is_valid && !has_no_null) {
			throw InternalException(
			    "Statistics mismatch: vector labeled as having only NULL values, but vector contains valid values: %s",
			    vector.ToString(count));
		}
		if (!row_is_valid && !has_null) {
			throw InternalException(
			    "Statistics mismatch: vector labeled as not having NULL values, but vector contains null values: %s",
			    vector.ToString(count));
		}
	}
}

string ValidityStatistics::ToString() const {
	auto has_n = has_null ? "true" : "false";
	auto has_n_n = has_no_null ? "true" : "false";
	return StringUtil::Format("[Has Null: %s, Has No Null: %s]", has_n, has_n_n);
}

} // namespace duckdb


namespace duckdb {

const uint64_t VERSION_NUMBER = 35;

} // namespace duckdb




namespace duckdb {

StorageLockKey::StorageLockKey(StorageLock &lock, StorageLockType type) : lock(lock), type(type) {
}

StorageLockKey::~StorageLockKey() {
	if (type == StorageLockType::EXCLUSIVE) {
		lock.ReleaseExclusiveLock();
	} else {
		D_ASSERT(type == StorageLockType::SHARED);
		lock.ReleaseSharedLock();
	}
}

StorageLock::StorageLock() : read_count(0) {
}

unique_ptr<StorageLockKey> StorageLock::GetExclusiveLock() {
	exclusive_lock.lock();
	while (read_count != 0) {
	}
	return make_unique<StorageLockKey>(*this, StorageLockType::EXCLUSIVE);
}

unique_ptr<StorageLockKey> StorageLock::GetSharedLock() {
	exclusive_lock.lock();
	read_count++;
	exclusive_lock.unlock();
	return make_unique<StorageLockKey>(*this, StorageLockType::SHARED);
}

void StorageLock::ReleaseExclusiveLock() {
	exclusive_lock.unlock();
}

void StorageLock::ReleaseSharedLock() {
	read_count--;
}

} // namespace duckdb

















namespace duckdb {

StorageManager::StorageManager(DatabaseInstance &db, string path, bool read_only)
    : db(db), path(move(path)), wal(db), read_only(read_only) {
}

StorageManager::~StorageManager() {
}

StorageManager &StorageManager::GetStorageManager(ClientContext &context) {
	return StorageManager::GetStorageManager(*context.db);
}

BufferManager &BufferManager::GetBufferManager(ClientContext &context) {
	return BufferManager::GetBufferManager(*context.db);
}

ObjectCache &ObjectCache::GetObjectCache(ClientContext &context) {
	return context.db->GetObjectCache();
}

bool ObjectCache::ObjectCacheEnabled(ClientContext &context) {
	return context.db->config.object_cache_enable;
}

bool StorageManager::InMemory() {
	return path.empty() || path == ":memory:";
}

void StorageManager::Initialize() {
	bool in_memory = InMemory();
	if (in_memory && read_only) {
		throw CatalogException("Cannot launch in-memory database in read-only mode!");
	}

	// first initialize the base system catalogs
	// these are never written to the WAL
	Connection con(db);
	con.BeginTransaction();

	auto &config = DBConfig::GetConfig(db);
	auto &catalog = Catalog::GetCatalog(*con.context);

	// create the default schema
	CreateSchemaInfo info;
	info.schema = DEFAULT_SCHEMA;
	info.internal = true;
	catalog.CreateSchema(*con.context, &info);

	if (config.initialize_default_database) {
		// initialize default functions
		BuiltinFunctions builtin(*con.context, catalog);
		builtin.Initialize();
	}

	// commit transactions
	con.Commit();

	if (!in_memory) {
		// create or load the database from disk, if not in-memory mode
		LoadDatabase();
	} else {
		block_manager = make_unique<InMemoryBlockManager>();
		buffer_manager = make_unique<BufferManager>(db, config.temporary_directory, config.maximum_memory);
	}
}

void StorageManager::LoadDatabase() {
	string wal_path = path + ".wal";
	auto &fs = db.GetFileSystem();
	auto &config = db.config;
	bool truncate_wal = false;
	// first check if the database exists
	if (!fs.FileExists(path)) {
		if (read_only) {
			throw CatalogException("Cannot open database \"%s\" in read-only mode: database does not exist", path);
		}
		// check if the WAL exists
		if (fs.FileExists(wal_path)) {
			// WAL file exists but database file does not
			// remove the WAL
			fs.RemoveFile(wal_path);
		}
		// initialize the block manager while creating a new db file
		block_manager = make_unique<SingleFileBlockManager>(db, path, read_only, true, config.use_direct_io);
		buffer_manager = make_unique<BufferManager>(db, config.temporary_directory, config.maximum_memory);
	} else {
		// initialize the block manager while loading the current db file
		auto sf_bm = make_unique<SingleFileBlockManager>(db, path, read_only, false, config.use_direct_io);
		auto sf = sf_bm.get();
		block_manager = move(sf_bm);
		buffer_manager = make_unique<BufferManager>(db, config.temporary_directory, config.maximum_memory);
		sf->LoadFreeList();

		//! Load from storage
		CheckpointManager checkpointer(db);
		checkpointer.LoadFromStorage();
		// check if the WAL file exists
		if (fs.FileExists(wal_path)) {
			// replay the WAL
			truncate_wal = WriteAheadLog::Replay(db, wal_path);
		}
	}
	// initialize the WAL file
	if (!read_only) {
		wal.Initialize(wal_path);
		if (truncate_wal) {
			wal.Truncate(0);
		}
	}
}

void StorageManager::CreateCheckpoint(bool delete_wal, bool force_checkpoint) {
	if (InMemory() || read_only || !wal.initialized) {
		return;
	}
	if (wal.GetWALSize() > 0 || db.config.force_checkpoint || force_checkpoint) {
		// we only need to checkpoint if there is anything in the WAL
		CheckpointManager checkpointer(db);
		checkpointer.CreateCheckpoint();
	}
	if (delete_wal) {
		wal.Delete();
	}
}

} // namespace duckdb




namespace duckdb {

struct TransactionVersionOperator {
	static bool UseInsertedVersion(transaction_t start_time, transaction_t transaction_id, transaction_t id) {
		return id < start_time || id == transaction_id;
	}

	static bool UseDeletedVersion(transaction_t start_time, transaction_t transaction_id, transaction_t id) {
		return !UseInsertedVersion(start_time, transaction_id, id);
	}
};

struct CommittedVersionOperator {
	static bool UseInsertedVersion(transaction_t start_time, transaction_t transaction_id, transaction_t id) {
		return true;
	}

	static bool UseDeletedVersion(transaction_t min_start_time, transaction_t min_transaction_id, transaction_t id) {
		return (id >= min_start_time && id < TRANSACTION_ID_START) || (id >= min_transaction_id);
	}
};

static bool UseVersion(Transaction &transaction, transaction_t id) {
	return TransactionVersionOperator::UseInsertedVersion(transaction.start_time, transaction.transaction_id, id);
}

unique_ptr<ChunkInfo> ChunkInfo::Deserialize(Deserializer &source) {
	auto type = source.Read<ChunkInfoType>();
	switch (type) {
	case ChunkInfoType::EMPTY_INFO:
		return nullptr;
	case ChunkInfoType::CONSTANT_INFO:
		return ChunkConstantInfo::Deserialize(source);
	case ChunkInfoType::VECTOR_INFO:
		return ChunkVectorInfo::Deserialize(source);
	default:
		throw SerializationException("Could not deserialize Chunk Info Type: unrecognized type");
	}
}

//===--------------------------------------------------------------------===//
// Constant info
//===--------------------------------------------------------------------===//
ChunkConstantInfo::ChunkConstantInfo(idx_t start)
    : ChunkInfo(start, ChunkInfoType::CONSTANT_INFO), insert_id(0), delete_id(NOT_DELETED_ID) {
}

template <class OP>
idx_t ChunkConstantInfo::TemplatedGetSelVector(transaction_t start_time, transaction_t transaction_id,
                                               SelectionVector &sel_vector, idx_t max_count) {
	if (OP::UseInsertedVersion(start_time, transaction_id, insert_id) &&
	    OP::UseDeletedVersion(start_time, transaction_id, delete_id)) {
		return max_count;
	}
	return 0;
}

idx_t ChunkConstantInfo::GetSelVector(Transaction &transaction, SelectionVector &sel_vector, idx_t max_count) {
	return TemplatedGetSelVector<TransactionVersionOperator>(transaction.start_time, transaction.transaction_id,
	                                                         sel_vector, max_count);
}

idx_t ChunkConstantInfo::GetCommittedSelVector(transaction_t min_start_id, transaction_t min_transaction_id,
                                               SelectionVector &sel_vector, idx_t max_count) {
	return TemplatedGetSelVector<CommittedVersionOperator>(min_start_id, min_transaction_id, sel_vector, max_count);
}

bool ChunkConstantInfo::Fetch(Transaction &transaction, row_t row) {
	return UseVersion(transaction, insert_id) && !UseVersion(transaction, delete_id);
}

void ChunkConstantInfo::CommitAppend(transaction_t commit_id, idx_t start, idx_t end) {
	D_ASSERT(start == 0 && end == STANDARD_VECTOR_SIZE);
	insert_id = commit_id;
}

void ChunkConstantInfo::Serialize(Serializer &serializer) {
	// we only need to write this node if any tuple deletions have been committed
	bool is_deleted = insert_id >= TRANSACTION_ID_START || delete_id < TRANSACTION_ID_START;
	if (!is_deleted) {
		serializer.Write<ChunkInfoType>(ChunkInfoType::EMPTY_INFO);
		return;
	}
	serializer.Write<ChunkInfoType>(type);
	serializer.Write<idx_t>(start);
}

unique_ptr<ChunkInfo> ChunkConstantInfo::Deserialize(Deserializer &source) {
	auto start = source.Read<idx_t>();

	auto info = make_unique<ChunkConstantInfo>(start);
	info->insert_id = 0;
	info->delete_id = 0;
	return move(info);
}

//===--------------------------------------------------------------------===//
// Vector info
//===--------------------------------------------------------------------===//
ChunkVectorInfo::ChunkVectorInfo(idx_t start)
    : ChunkInfo(start, ChunkInfoType::VECTOR_INFO), insert_id(0), same_inserted_id(true), any_deleted(false) {
	for (idx_t i = 0; i < STANDARD_VECTOR_SIZE; i++) {
		inserted[i] = 0;
		deleted[i] = NOT_DELETED_ID;
	}
}

template <class OP>
idx_t ChunkVectorInfo::TemplatedGetSelVector(transaction_t start_time, transaction_t transaction_id,
                                             SelectionVector &sel_vector, idx_t max_count) {
	idx_t count = 0;
	if (same_inserted_id && !any_deleted) {
		// all tuples have the same inserted id: and no tuples were deleted
		if (OP::UseInsertedVersion(start_time, transaction_id, insert_id)) {
			return max_count;
		} else {
			return 0;
		}
	} else if (same_inserted_id) {
		if (!OP::UseInsertedVersion(start_time, transaction_id, insert_id)) {
			return 0;
		}
		// have to check deleted flag
		for (idx_t i = 0; i < max_count; i++) {
			if (OP::UseDeletedVersion(start_time, transaction_id, deleted[i])) {
				sel_vector.set_index(count++, i);
			}
		}
	} else if (!any_deleted) {
		// have to check inserted flag
		for (idx_t i = 0; i < max_count; i++) {
			if (OP::UseInsertedVersion(start_time, transaction_id, inserted[i])) {
				sel_vector.set_index(count++, i);
			}
		}
	} else {
		// have to check both flags
		for (idx_t i = 0; i < max_count; i++) {
			if (OP::UseInsertedVersion(start_time, transaction_id, inserted[i]) &&
			    OP::UseDeletedVersion(start_time, transaction_id, deleted[i])) {
				sel_vector.set_index(count++, i);
			}
		}
	}
	return count;
}

idx_t ChunkVectorInfo::GetSelVector(transaction_t start_time, transaction_t transaction_id, SelectionVector &sel_vector,
                                    idx_t max_count) {
	return TemplatedGetSelVector<TransactionVersionOperator>(start_time, transaction_id, sel_vector, max_count);
}

idx_t ChunkVectorInfo::GetCommittedSelVector(transaction_t min_start_id, transaction_t min_transaction_id,
                                             SelectionVector &sel_vector, idx_t max_count) {
	return TemplatedGetSelVector<CommittedVersionOperator>(min_start_id, min_transaction_id, sel_vector, max_count);
}

idx_t ChunkVectorInfo::GetSelVector(Transaction &transaction, SelectionVector &sel_vector, idx_t max_count) {
	return GetSelVector(transaction.start_time, transaction.transaction_id, sel_vector, max_count);
}

bool ChunkVectorInfo::Fetch(Transaction &transaction, row_t row) {
	return UseVersion(transaction, inserted[row]) && !UseVersion(transaction, deleted[row]);
}

idx_t ChunkVectorInfo::Delete(Transaction &transaction, row_t rows[], idx_t count) {
	any_deleted = true;

	idx_t deleted_tuples = 0;
	for (idx_t i = 0; i < count; i++) {
		if (deleted[rows[i]] == transaction.transaction_id) {
			continue;
		}
		// first check the chunk for conflicts
		if (deleted[rows[i]] != NOT_DELETED_ID) {
			// tuple was already deleted by another transaction
			throw TransactionException("Conflict on tuple deletion!");
		}
		if (inserted[rows[i]] >= TRANSACTION_ID_START) {
			throw TransactionException("Deleting non-committed tuples is not supported (for now...)");
		}
		// after verifying that there are no conflicts we mark the tuple as deleted
		deleted[rows[i]] = transaction.transaction_id;
		deleted_tuples++;
	}
	return deleted_tuples;
}

void ChunkVectorInfo::CommitDelete(transaction_t commit_id, row_t rows[], idx_t count) {
	for (idx_t i = 0; i < count; i++) {
		deleted[rows[i]] = commit_id;
	}
}

void ChunkVectorInfo::Append(idx_t start, idx_t end, transaction_t commit_id) {
	if (start == 0) {
		insert_id = commit_id;
	} else if (insert_id != commit_id) {
		same_inserted_id = false;
		insert_id = NOT_DELETED_ID;
	}
	for (idx_t i = start; i < end; i++) {
		inserted[i] = commit_id;
	}
}

void ChunkVectorInfo::CommitAppend(transaction_t commit_id, idx_t start, idx_t end) {
	if (same_inserted_id) {
		insert_id = commit_id;
	}
	for (idx_t i = start; i < end; i++) {
		inserted[i] = commit_id;
	}
}

void ChunkVectorInfo::Serialize(Serializer &serializer) {
	SelectionVector sel(STANDARD_VECTOR_SIZE);
	transaction_t start_time = TRANSACTION_ID_START - 1;
	transaction_t transaction_id = DConstants::INVALID_INDEX;
	idx_t count = GetSelVector(start_time, transaction_id, sel, STANDARD_VECTOR_SIZE);
	if (count == STANDARD_VECTOR_SIZE) {
		// nothing is deleted: skip writing anything
		serializer.Write<ChunkInfoType>(ChunkInfoType::EMPTY_INFO);
		return;
	}
	if (count == 0) {
		// everything is deleted: write a constant vector
		serializer.Write<ChunkInfoType>(ChunkInfoType::CONSTANT_INFO);
		serializer.Write<idx_t>(start);
		return;
	}
	// write a boolean vector
	serializer.Write<ChunkInfoType>(ChunkInfoType::VECTOR_INFO);
	serializer.Write<idx_t>(start);
	bool deleted_tuples[STANDARD_VECTOR_SIZE];
	for (idx_t i = 0; i < STANDARD_VECTOR_SIZE; i++) {
		deleted_tuples[i] = true;
	}
	for (idx_t i = 0; i < count; i++) {
		deleted_tuples[sel.get_index(i)] = false;
	}
	serializer.WriteData((data_ptr_t)deleted_tuples, sizeof(bool) * STANDARD_VECTOR_SIZE);
}

unique_ptr<ChunkInfo> ChunkVectorInfo::Deserialize(Deserializer &source) {
	auto start = source.Read<idx_t>();

	auto result = make_unique<ChunkVectorInfo>(start);
	result->any_deleted = true;
	bool deleted_tuples[STANDARD_VECTOR_SIZE];
	source.ReadData((data_ptr_t)deleted_tuples, sizeof(bool) * STANDARD_VECTOR_SIZE);
	for (idx_t i = 0; i < STANDARD_VECTOR_SIZE; i++) {
		if (deleted_tuples[i]) {
			result->deleted[i] = 0;
		}
	}
	return move(result);
}

} // namespace duckdb














namespace duckdb {

ColumnCheckpointState::ColumnCheckpointState(RowGroup &row_group, ColumnData &column_data, TableDataWriter &writer)
    : row_group(row_group), column_data(column_data), writer(writer) {
}

ColumnCheckpointState::~ColumnCheckpointState() {
}

void ColumnCheckpointState::FlushSegment(unique_ptr<ColumnSegment> segment, idx_t segment_size) {
	D_ASSERT(segment_size <= Storage::BLOCK_SIZE);
	auto tuple_count = segment->count.load();
	if (tuple_count == 0) { // LCOV_EXCL_START
		return;
	} // LCOV_EXCL_STOP

	// merge the segment stats into the global stats
	global_stats->Merge(*segment->stats.statistics);

	// get the buffer of the segment and pin it
	auto &db = column_data.GetDatabase();
	auto &buffer_manager = BufferManager::GetBufferManager(db);
	auto &block_manager = BlockManager::GetBlockManager(db);
	auto &checkpoint_manager = writer.GetCheckpointManager();

	bool block_is_constant = segment->stats.statistics->IsConstant();

	block_id_t block_id = INVALID_BLOCK;
	uint32_t offset_in_block = 0;
	bool need_to_write = true;
	PartialBlock *partial_block = nullptr;
	unique_ptr<PartialBlock> owned_partial_block;
	if (!block_is_constant) {
		// non-constant block
		// if the block is less than 80% full, we consider it a "partial block"
		// which means we will try to fit it with other blocks
		if (segment_size <= CheckpointManager::PARTIAL_BLOCK_THRESHOLD) {
			// the block is a partial block
			// check if there is a partial block available we can write to
			if (checkpoint_manager.GetPartialBlock(segment.get(), segment_size, block_id, offset_in_block,
			                                       partial_block, owned_partial_block)) {
				//! there is! increase the reference count of this block
				block_manager.IncreaseBlockReferenceCount(block_id);
			} else {
				// there isn't: generate a new block for this segment
				block_id = block_manager.GetFreeBlockId();
				offset_in_block = 0;
				need_to_write = false;
				// now register this block as a partial block
				checkpoint_manager.RegisterPartialBlock(segment.get(), segment_size, block_id);
			}
		} else {
			// full block: get a free block to write to
			block_id = block_manager.GetFreeBlockId();
			offset_in_block = 0;
		}
	} else {
		// constant block: no need to write anything to disk besides the stats
		// set up the compression function to constant
		auto &config = DBConfig::GetConfig(db);
		segment->function =
		    config.GetCompressionFunction(CompressionType::COMPRESSION_CONSTANT, segment->type.InternalType());
	}

	// construct the data pointer
	DataPointer data_pointer;
	data_pointer.block_pointer.block_id = block_id;
	data_pointer.block_pointer.offset = offset_in_block;
	data_pointer.row_start = row_group.start;
	if (!data_pointers.empty()) {
		auto &last_pointer = data_pointers.back();
		data_pointer.row_start = last_pointer.row_start + last_pointer.tuple_count;
	}
	data_pointer.tuple_count = tuple_count;
	data_pointer.compression_type = segment->function->type;
	data_pointer.statistics = segment->stats.statistics->Copy();

	if (need_to_write) {
		if (partial_block) {
			// pin the current block
			auto old_handle = buffer_manager.Pin(segment->block);
			// pin the new block
			auto new_handle = buffer_manager.Pin(partial_block->block);
			// memcpy the contents of the old block to the new block
			memcpy(new_handle->Ptr() + offset_in_block, old_handle->Ptr(), segment_size);
		} else {
			// convert the segment into a persistent segment that points to this block
			segment->ConvertToPersistent(block_id);
		}
	}
	if (owned_partial_block) {
		// the partial block has become full: write it to disk
		owned_partial_block->FlushToDisk(db);
	}

	// append the segment to the new segment tree
	new_tree.AppendSegment(move(segment));
	data_pointers.push_back(move(data_pointer));
}

void ColumnCheckpointState::FlushToDisk() {
	auto &meta_writer = writer.GetMetaWriter();

	meta_writer.Write<idx_t>(data_pointers.size());
	// then write the data pointers themselves
	for (idx_t k = 0; k < data_pointers.size(); k++) {
		auto &data_pointer = data_pointers[k];
		meta_writer.Write<idx_t>(data_pointer.row_start);
		meta_writer.Write<idx_t>(data_pointer.tuple_count);
		meta_writer.Write<block_id_t>(data_pointer.block_pointer.block_id);
		meta_writer.Write<uint32_t>(data_pointer.block_pointer.offset);
		meta_writer.Write<CompressionType>(data_pointer.compression_type);
		data_pointer.statistics->Serialize(meta_writer);
	}
}

} // namespace duckdb















namespace duckdb {

ColumnData::ColumnData(DataTableInfo &info, idx_t column_index, idx_t start_row, LogicalType type, ColumnData *parent)
    : info(info), column_index(column_index), start(start_row), type(move(type)), parent(parent) {
}

ColumnData::~ColumnData() {
}

DatabaseInstance &ColumnData::GetDatabase() const {
	return info.db;
}

DataTableInfo &ColumnData::GetTableInfo() const {
	return info;
}

const LogicalType &ColumnData::RootType() const {
	if (parent) {
		return parent->RootType();
	}
	return type;
}

idx_t ColumnData::GetMaxEntry() {
	auto last_segment = data.GetLastSegment();
	return last_segment ? last_segment->start + last_segment->count : start;
}

void ColumnData::InitializeScan(ColumnScanState &state) {
	state.current = (ColumnSegment *)data.GetRootSegment();
	state.row_index = state.current ? state.current->start : 0;
	state.internal_index = state.row_index;
	state.initialized = false;
}

void ColumnData::InitializeScanWithOffset(ColumnScanState &state, idx_t row_idx) {
	state.current = (ColumnSegment *)data.GetSegment(row_idx);
	state.row_index = row_idx;
	state.internal_index = state.current->start;
	state.initialized = false;
}

idx_t ColumnData::ScanVector(ColumnScanState &state, Vector &result, idx_t remaining) {
	if (!state.initialized) {
		D_ASSERT(state.current);
		state.current->InitializeScan(state);
		state.internal_index = state.current->start;
		state.initialized = true;
	}
	D_ASSERT(state.internal_index <= state.row_index);
	if (state.internal_index < state.row_index) {
		state.current->Skip(state);
	}
	D_ASSERT(state.current->type == type);
	idx_t initial_remaining = remaining;
	while (remaining > 0) {
		D_ASSERT(state.row_index >= state.current->start &&
		         state.row_index <= state.current->start + state.current->count);
		idx_t scan_count = MinValue<idx_t>(remaining, state.current->start + state.current->count - state.row_index);
		idx_t result_offset = initial_remaining - remaining;
		state.current->Scan(state, scan_count, result, result_offset, scan_count == initial_remaining);

		state.row_index += scan_count;
		remaining -= scan_count;
		if (remaining > 0) {
			if (!state.current->next) {
				break;
			}
			state.current = (ColumnSegment *)state.current->next.get();
			state.current->InitializeScan(state);
			state.segment_checked = false;
			D_ASSERT(state.row_index >= state.current->start &&
			         state.row_index <= state.current->start + state.current->count);
		}
	}
	state.internal_index = state.row_index;
	return initial_remaining - remaining;
}

template <bool SCAN_COMMITTED, bool ALLOW_UPDATES>
idx_t ColumnData::ScanVector(Transaction *transaction, idx_t vector_index, ColumnScanState &state, Vector &result) {
	auto scan_count = ScanVector(state, result, STANDARD_VECTOR_SIZE);

	lock_guard<mutex> update_guard(update_lock);
	if (updates) {
		if (!ALLOW_UPDATES && updates->HasUncommittedUpdates(vector_index)) {
			throw TransactionException("Cannot create index with outstanding updates");
		}
		result.Normalify(scan_count);
		if (SCAN_COMMITTED) {
			updates->FetchCommitted(vector_index, result);
		} else {
			D_ASSERT(transaction);
			updates->FetchUpdates(*transaction, vector_index, result);
		}
	}
	return scan_count;
}

template idx_t ColumnData::ScanVector<false, false>(Transaction *transaction, idx_t vector_index,
                                                    ColumnScanState &state, Vector &result);
template idx_t ColumnData::ScanVector<true, false>(Transaction *transaction, idx_t vector_index, ColumnScanState &state,
                                                   Vector &result);
template idx_t ColumnData::ScanVector<false, true>(Transaction *transaction, idx_t vector_index, ColumnScanState &state,
                                                   Vector &result);
template idx_t ColumnData::ScanVector<true, true>(Transaction *transaction, idx_t vector_index, ColumnScanState &state,
                                                  Vector &result);

idx_t ColumnData::Scan(Transaction &transaction, idx_t vector_index, ColumnScanState &state, Vector &result) {
	return ScanVector<false, true>(&transaction, vector_index, state, result);
}

idx_t ColumnData::ScanCommitted(idx_t vector_index, ColumnScanState &state, Vector &result, bool allow_updates) {
	if (allow_updates) {
		return ScanVector<true, true>(nullptr, vector_index, state, result);
	} else {
		return ScanVector<true, false>(nullptr, vector_index, state, result);
	}
}

void ColumnData::ScanCommittedRange(idx_t row_group_start, idx_t offset_in_row_group, idx_t count, Vector &result) {
	ColumnScanState child_state;
	InitializeScanWithOffset(child_state, row_group_start + offset_in_row_group);
	auto scan_count = ScanVector(child_state, result, count);
	if (updates) {
		result.Normalify(scan_count);
		updates->FetchCommittedRange(offset_in_row_group, count, result);
	}
}

idx_t ColumnData::ScanCount(ColumnScanState &state, Vector &result, idx_t count) {
	if (count == 0) {
		return 0;
	}
	// ScanCount can only be used if there are no updates
	D_ASSERT(!updates);
	return ScanVector(state, result, count);
}

void ColumnData::Select(Transaction &transaction, idx_t vector_index, ColumnScanState &state, Vector &result,
                        SelectionVector &sel, idx_t &count, const TableFilter &filter) {
	idx_t scan_count = Scan(transaction, vector_index, state, result);
	result.Normalify(scan_count);
	ColumnSegment::FilterSelection(sel, result, filter, count, FlatVector::Validity(result));
}

void ColumnData::FilterScan(Transaction &transaction, idx_t vector_index, ColumnScanState &state, Vector &result,
                            SelectionVector &sel, idx_t count) {
	Scan(transaction, vector_index, state, result);
	result.Slice(sel, count);
}

void ColumnData::FilterScanCommitted(idx_t vector_index, ColumnScanState &state, Vector &result, SelectionVector &sel,
                                     idx_t count, bool allow_updates) {
	ScanCommitted(vector_index, state, result, allow_updates);
	result.Slice(sel, count);
}

void ColumnData::Skip(ColumnScanState &state, idx_t count) {
	state.Next(count);
}

void ColumnScanState::NextInternal(idx_t count) {
	if (!current) {
		//! There is no column segment
		return;
	}
	row_index += count;
	while (row_index >= current->start + current->count) {
		current = (ColumnSegment *)current->next.get();
		initialized = false;
		segment_checked = false;
		if (!current) {
			break;
		}
	}
	D_ASSERT(!current || (row_index >= current->start && row_index < current->start + current->count));
}

void ColumnScanState::Next(idx_t count) {
	NextInternal(count);
	for (auto &child_state : child_states) {
		child_state.Next(count);
	}
}

void ColumnScanState::NextVector() {
	Next(STANDARD_VECTOR_SIZE);
}

void ColumnData::Append(BaseStatistics &stats, ColumnAppendState &state, Vector &vector, idx_t count) {
	VectorData vdata;
	vector.Orrify(count, vdata);
	AppendData(stats, state, vdata, count);
}

void ColumnData::InitializeAppend(ColumnAppendState &state) {
	lock_guard<mutex> tree_lock(data.node_lock);
	if (data.nodes.empty()) {
		// no segments yet, append an empty segment
		AppendTransientSegment(start);
	}
	auto segment = (ColumnSegment *)data.GetLastSegment();
	if (segment->segment_type == ColumnSegmentType::PERSISTENT) {
		// no transient segments yet
		auto total_rows = segment->start + segment->count;
		AppendTransientSegment(total_rows);
		state.current = (ColumnSegment *)data.GetLastSegment();
	} else {
		state.current = (ColumnSegment *)segment;
	}

	D_ASSERT(state.current->segment_type == ColumnSegmentType::TRANSIENT);
	state.current->InitializeAppend(state);
}

void ColumnData::AppendData(BaseStatistics &stats, ColumnAppendState &state, VectorData &vdata, idx_t count) {
	idx_t offset = 0;
	while (true) {
		// append the data from the vector
		idx_t copied_elements = state.current->Append(state, vdata, offset, count);
		stats.Merge(*state.current->stats.statistics);
		if (copied_elements == count) {
			// finished copying everything
			break;
		}

		// we couldn't fit everything we wanted in the current column segment, create a new one
		{
			lock_guard<mutex> tree_lock(data.node_lock);
			AppendTransientSegment(state.current->start + state.current->count);
			state.current = (ColumnSegment *)data.GetLastSegment();
			state.current->InitializeAppend(state);
		}
		offset += copied_elements;
		count -= copied_elements;
	}
}

void ColumnData::RevertAppend(row_t start_row) {
	lock_guard<mutex> tree_lock(data.node_lock);
	// check if this row is in the segment tree at all
	if (idx_t(start_row) >= data.nodes.back().row_start + data.nodes.back().node->count) {
		// the start row is equal to the final portion of the column data: nothing was ever appended here
		D_ASSERT(idx_t(start_row) == data.nodes.back().row_start + data.nodes.back().node->count);
		return;
	}
	// find the segment index that the current row belongs to
	idx_t segment_index = data.GetSegmentIndex(start_row);
	auto segment = data.nodes[segment_index].node;
	auto &transient = (ColumnSegment &)*segment;
	D_ASSERT(transient.segment_type == ColumnSegmentType::TRANSIENT);

	// remove any segments AFTER this segment: they should be deleted entirely
	if (segment_index < data.nodes.size() - 1) {
		data.nodes.erase(data.nodes.begin() + segment_index + 1, data.nodes.end());
	}
	segment->next = nullptr;
	transient.RevertAppend(start_row);
}

idx_t ColumnData::Fetch(ColumnScanState &state, row_t row_id, Vector &result) {
	D_ASSERT(row_id >= 0);
	D_ASSERT(idx_t(row_id) >= start);
	// perform the fetch within the segment
	state.row_index = start + ((row_id - start) / STANDARD_VECTOR_SIZE * STANDARD_VECTOR_SIZE);
	state.current = (ColumnSegment *)data.GetSegment(state.row_index);
	state.internal_index = state.current->start;
	return ScanVector(state, result, STANDARD_VECTOR_SIZE);
}

void ColumnData::FetchRow(Transaction &transaction, ColumnFetchState &state, row_t row_id, Vector &result,
                          idx_t result_idx) {
	auto segment = (ColumnSegment *)data.GetSegment(row_id);

	// now perform the fetch within the segment
	segment->FetchRow(state, row_id, result, result_idx);
	// merge any updates made to this row
	lock_guard<mutex> update_guard(update_lock);
	if (updates) {
		updates->FetchRow(transaction, row_id, result, result_idx);
	}
}

void ColumnData::Update(Transaction &transaction, idx_t column_index, Vector &update_vector, row_t *row_ids,
                        idx_t update_count) {
	lock_guard<mutex> update_guard(update_lock);
	if (!updates) {
		updates = make_unique<UpdateSegment>(*this);
	}
	Vector base_vector(type);
	ColumnScanState state;
	auto fetch_count = Fetch(state, row_ids[0], base_vector);

	base_vector.Normalify(fetch_count);
	updates->Update(transaction, column_index, update_vector, row_ids, update_count, base_vector);
}

void ColumnData::UpdateColumn(Transaction &transaction, const vector<column_t> &column_path, Vector &update_vector,
                              row_t *row_ids, idx_t update_count, idx_t depth) {
	// this method should only be called at the end of the path in the base column case
	D_ASSERT(depth >= column_path.size());
	ColumnData::Update(transaction, column_path[0], update_vector, row_ids, update_count);
}

unique_ptr<BaseStatistics> ColumnData::GetUpdateStatistics() {
	lock_guard<mutex> update_guard(update_lock);
	return updates ? updates->GetStatistics() : nullptr;
}

void ColumnData::AppendTransientSegment(idx_t start_row) {
	auto new_segment = ColumnSegment::CreateTransientSegment(GetDatabase(), type, start_row);
	data.AppendSegment(move(new_segment));
}

void ColumnData::CommitDropColumn() {
	auto &block_manager = BlockManager::GetBlockManager(GetDatabase());
	auto segment = (ColumnSegment *)data.GetRootSegment();
	while (segment) {
		if (segment->segment_type == ColumnSegmentType::PERSISTENT) {
			auto block_id = segment->GetBlockId();
			if (block_id != INVALID_BLOCK) {
				block_manager.MarkBlockAsModified(block_id);
			}
		}
		segment = (ColumnSegment *)segment->next.get();
	}
}

unique_ptr<ColumnCheckpointState> ColumnData::CreateCheckpointState(RowGroup &row_group, TableDataWriter &writer) {
	return make_unique<ColumnCheckpointState>(row_group, *this, writer);
}

void ColumnData::CheckpointScan(ColumnSegment *segment, ColumnScanState &state, idx_t row_group_start, idx_t count,
                                Vector &scan_vector) {
	segment->Scan(state, count, scan_vector, 0, true);
	if (updates) {
		scan_vector.Normalify(count);
		updates->FetchCommittedRange(state.row_index - row_group_start, count, scan_vector);
	}
}

unique_ptr<ColumnCheckpointState> ColumnData::Checkpoint(RowGroup &row_group, TableDataWriter &writer,
                                                         ColumnCheckpointInfo &checkpoint_info) {
	// scan the segments of the column data
	// set up the checkpoint state
	auto checkpoint_state = CreateCheckpointState(row_group, writer);
	checkpoint_state->global_stats = BaseStatistics::CreateEmpty(type, StatisticsType::LOCAL_STATS);

	if (!data.root_node) {
		// empty table: flush the empty list
		return checkpoint_state;
	}
	lock_guard<mutex> update_guard(update_lock);

	ColumnDataCheckpointer checkpointer(*this, row_group, *checkpoint_state, checkpoint_info);
	checkpointer.Checkpoint(move(data.root_node));

	// replace the old tree with the new one
	data.Replace(checkpoint_state->new_tree);

	return checkpoint_state;
}

void ColumnData::DeserializeColumn(Deserializer &source) {
	// load the data pointers for the column
	idx_t data_pointer_count = source.Read<idx_t>();
	for (idx_t data_ptr = 0; data_ptr < data_pointer_count; data_ptr++) {
		// read the data pointer
		DataPointer data_pointer;
		data_pointer.row_start = source.Read<idx_t>();
		data_pointer.tuple_count = source.Read<idx_t>();
		data_pointer.block_pointer.block_id = source.Read<block_id_t>();
		data_pointer.block_pointer.offset = source.Read<uint32_t>();
		data_pointer.compression_type = source.Read<CompressionType>();
		data_pointer.statistics = BaseStatistics::Deserialize(source, type);

		// create a persistent segment
		auto segment = ColumnSegment::CreatePersistentSegment(
		    GetDatabase(), data_pointer.block_pointer.block_id, data_pointer.block_pointer.offset, type,
		    data_pointer.row_start, data_pointer.tuple_count, data_pointer.compression_type,
		    move(data_pointer.statistics));
		data.AppendSegment(move(segment));
	}
}

shared_ptr<ColumnData> ColumnData::Deserialize(DataTableInfo &info, idx_t column_index, idx_t start_row,
                                               Deserializer &source, const LogicalType &type, ColumnData *parent) {
	auto entry = ColumnData::CreateColumn(info, column_index, start_row, type, parent);
	entry->DeserializeColumn(source);
	return entry;
}

void ColumnData::GetStorageInfo(idx_t row_group_index, vector<idx_t> col_path, vector<vector<Value>> &result) {
	D_ASSERT(!col_path.empty());

	// convert the column path to a string
	string col_path_str = "[";
	for (idx_t i = 0; i < col_path.size(); i++) {
		if (i > 0) {
			col_path_str += ", ";
		}
		col_path_str += to_string(col_path[i]);
	}
	col_path_str += "]";

	// iterate over the segments
	idx_t segment_idx = 0;
	auto segment = (ColumnSegment *)data.GetRootSegment();
	while (segment) {
		vector<Value> column_info;
		// row_group_id
		column_info.push_back(Value::BIGINT(row_group_index));
		// column_id
		column_info.push_back(Value::BIGINT(col_path[0]));
		// column_path
		column_info.emplace_back(col_path_str);
		// segment_id
		column_info.push_back(Value::BIGINT(segment_idx));
		// segment_type
		column_info.emplace_back(type.ToString());
		// start
		column_info.push_back(Value::BIGINT(segment->start));
		// count
		column_info.push_back(Value::BIGINT(segment->count));
		// compression
		column_info.emplace_back(CompressionTypeToString(segment->function->type));
		// stats
		column_info.emplace_back(segment->stats.statistics ? segment->stats.statistics->ToString()
		                                                   : string("No Stats"));
		// has_updates
		column_info.push_back(Value::BOOLEAN(updates ? true : false));
		// persistent
		// block_id
		// block_offset
		if (segment->segment_type == ColumnSegmentType::PERSISTENT) {
			column_info.push_back(Value::BOOLEAN(true));
			column_info.push_back(Value::BIGINT(segment->GetBlockId()));
			column_info.push_back(Value::BIGINT(segment->GetBlockOffset()));
		} else {
			column_info.push_back(Value::BOOLEAN(false));
			column_info.emplace_back();
			column_info.emplace_back();
		}

		result.push_back(move(column_info));

		segment_idx++;
		segment = (ColumnSegment *)segment->next.get();
	}
}

void ColumnData::Verify(RowGroup &parent) {
#ifdef DEBUG
	D_ASSERT(this->start == parent.start);
	auto root = data.GetRootSegment();
	if (root) {
		D_ASSERT(root != nullptr);
		D_ASSERT(root->start == this->start);
		idx_t prev_end = root->start;
		while (root) {
			D_ASSERT(prev_end == root->start);
			prev_end = root->start + root->count;
			if (!root->next) {
				D_ASSERT(prev_end == parent.start + parent.count);
			}
			root = root->next.get();
		}
	} else {
		if (type.InternalType() != PhysicalType::STRUCT) {
			D_ASSERT(parent.count == 0);
		}
	}
#endif
}

template <class RET, class OP>
static RET CreateColumnInternal(DataTableInfo &info, idx_t column_index, idx_t start_row, const LogicalType &type,
                                ColumnData *parent) {
	if (type.InternalType() == PhysicalType::STRUCT) {
		return OP::template Create<StructColumnData>(info, column_index, start_row, type, parent);
	} else if (type.InternalType() == PhysicalType::LIST) {
		return OP::template Create<ListColumnData>(info, column_index, start_row, type, parent);
	} else if (type.id() == LogicalTypeId::VALIDITY) {
		return OP::template Create<ValidityColumnData>(info, column_index, start_row, parent);
	}
	return OP::template Create<StandardColumnData>(info, column_index, start_row, type, parent);
}

shared_ptr<ColumnData> ColumnData::CreateColumn(DataTableInfo &info, idx_t column_index, idx_t start_row,
                                                const LogicalType &type, ColumnData *parent) {
	return CreateColumnInternal<shared_ptr<ColumnData>, SharedConstructor>(info, column_index, start_row, type, parent);
}

unique_ptr<ColumnData> ColumnData::CreateColumnUnique(DataTableInfo &info, idx_t column_index, idx_t start_row,
                                                      const LogicalType &type, ColumnData *parent) {
	return CreateColumnInternal<unique_ptr<ColumnData>, UniqueConstructor>(info, column_index, start_row, type, parent);
}

} // namespace duckdb





namespace duckdb {

ColumnDataCheckpointer::ColumnDataCheckpointer(ColumnData &col_data_p, RowGroup &row_group_p,
                                               ColumnCheckpointState &state_p, ColumnCheckpointInfo &checkpoint_info_p)
    : col_data(col_data_p), row_group(row_group_p), state(state_p),
      is_validity(GetType().id() == LogicalTypeId::VALIDITY),
      intermediate(is_validity ? LogicalType::BOOLEAN : GetType(), true, is_validity),
      checkpoint_info(checkpoint_info_p) {
	auto &config = DBConfig::GetConfig(GetDatabase());
	compression_functions = config.GetCompressionFunctions(GetType().InternalType());
}

DatabaseInstance &ColumnDataCheckpointer::GetDatabase() {
	return col_data.GetDatabase();
}

const LogicalType &ColumnDataCheckpointer::GetType() const {
	return col_data.type;
}

ColumnData &ColumnDataCheckpointer::GetColumnData() {
	return col_data;
}

RowGroup &ColumnDataCheckpointer::GetRowGroup() {
	return row_group;
}

ColumnCheckpointState &ColumnDataCheckpointer::GetCheckpointState() {
	return state;
}

void ColumnDataCheckpointer::ScanSegments(const std::function<void(Vector &, idx_t)> &callback) {
	Vector scan_vector(intermediate.GetType(), nullptr);
	for (auto segment = (ColumnSegment *)owned_segment.get(); segment; segment = (ColumnSegment *)segment->next.get()) {
		ColumnScanState scan_state;
		scan_state.current = segment;
		segment->InitializeScan(scan_state);

		for (idx_t base_row_index = 0; base_row_index < segment->count; base_row_index += STANDARD_VECTOR_SIZE) {
			scan_vector.Reference(intermediate);

			idx_t count = MinValue<idx_t>(segment->count - base_row_index, STANDARD_VECTOR_SIZE);
			scan_state.row_index = segment->start + base_row_index;

			col_data.CheckpointScan(segment, scan_state, row_group.start, count, scan_vector);

			callback(scan_vector, count);
		}
	}
}

void ForceCompression(vector<CompressionFunction *> &compression_functions, CompressionType compression_type) {
	// On of the force_compression flags has been set
	// check if this compression method is available
	bool found = false;
	for (idx_t i = 0; i < compression_functions.size(); i++) {
		if (compression_functions[i]->type == compression_type) {
			found = true;
			break;
		}
	}
	if (found) {
		// the force_compression method is available
		// clear all other compression methods
		for (idx_t i = 0; i < compression_functions.size(); i++) {
			if (compression_functions[i]->type != compression_type) {
				compression_functions[i] = nullptr;
			}
		}
	}
}

unique_ptr<AnalyzeState> ColumnDataCheckpointer::DetectBestCompressionMethod(idx_t &compression_idx) {
	D_ASSERT(!compression_functions.empty());
	auto &config = DBConfig::GetConfig(GetDatabase());

	auto compression_type = checkpoint_info.compression_type;
	if (compression_type != CompressionType::COMPRESSION_AUTO) {
		ForceCompression(compression_functions, compression_type);
	}
	if (compression_type == CompressionType::COMPRESSION_AUTO &&
	    config.force_compression != CompressionType::COMPRESSION_AUTO) {
		ForceCompression(compression_functions, config.force_compression);
	}
	// set up the analyze states for each compression method
	vector<unique_ptr<AnalyzeState>> analyze_states;
	analyze_states.reserve(compression_functions.size());
	for (idx_t i = 0; i < compression_functions.size(); i++) {
		if (!compression_functions[i]) {
			analyze_states.push_back(nullptr);
			continue;
		}
		analyze_states.push_back(compression_functions[i]->init_analyze(col_data, col_data.type.InternalType()));
	}

	// scan over all the segments and run the analyze step
	ScanSegments([&](Vector &scan_vector, idx_t count) {
		for (idx_t i = 0; i < compression_functions.size(); i++) {
			if (!compression_functions[i]) {
				continue;
			}
			auto success = compression_functions[i]->analyze(*analyze_states[i], scan_vector, count);
			if (!success) {
				// could not use this compression function on this data set
				// erase it
				compression_functions[i] = nullptr;
				analyze_states[i].reset();
			}
		}
	});

	// now that we have passed over all the data, we need to figure out the best method
	// we do this using the final_analyze method
	unique_ptr<AnalyzeState> state;
	compression_idx = DConstants::INVALID_INDEX;
	idx_t best_score = NumericLimits<idx_t>::Maximum();
	for (idx_t i = 0; i < compression_functions.size(); i++) {
		if (!compression_functions[i]) {
			continue;
		}
		auto score = compression_functions[i]->final_analyze(*analyze_states[i]);
		if (score < best_score) {
			compression_idx = i;
			best_score = score;
			state = move(analyze_states[i]);
		}
	}
	return state;
}

void ColumnDataCheckpointer::WriteToDisk() {
	// there were changes or transient segments
	// we need to rewrite the column segments to disk

	// first we check the current segments
	// if there are any persistent segments, we will mark their old block ids as modified
	// since the segments will be rewritten their old on disk data is no longer required
	auto &block_manager = BlockManager::GetBlockManager(GetDatabase());
	for (auto segment = (ColumnSegment *)owned_segment.get(); segment; segment = (ColumnSegment *)segment->next.get()) {
		if (segment->segment_type == ColumnSegmentType::PERSISTENT) {
			// persistent segment has updates: mark it as modified and rewrite the block with the merged updates
			auto block_id = segment->GetBlockId();
			if (block_id != INVALID_BLOCK) {
				block_manager.MarkBlockAsModified(block_id);
			}
		}
	}

	// now we need to write our segment
	// we will first run an analyze step that determines which compression function to use
	idx_t compression_idx;
	auto analyze_state = DetectBestCompressionMethod(compression_idx);

	if (!analyze_state) {
		throw InternalException("No suitable compression/storage method found to store column");
	}

	// now that we have analyzed the compression functions we can start writing to disk
	auto best_function = compression_functions[compression_idx];
	auto compress_state = best_function->init_compression(*this, move(analyze_state));
	ScanSegments(
	    [&](Vector &scan_vector, idx_t count) { best_function->compress(*compress_state, scan_vector, count); });
	best_function->compress_finalize(*compress_state);

	// now we actually write the data to disk
	owned_segment.reset();
}

bool ColumnDataCheckpointer::HasChanges() {
	for (auto segment = (ColumnSegment *)owned_segment.get(); segment; segment = (ColumnSegment *)segment->next.get()) {
		if (segment->segment_type == ColumnSegmentType::TRANSIENT) {
			// transient segment: always need to write to disk
			return true;
		} else {
			// persistent segment; check if there were any updates or deletions in this segment
			idx_t start_row_idx = segment->start - row_group.start;
			idx_t end_row_idx = start_row_idx + segment->count;
			if (col_data.updates && col_data.updates->HasUpdates(start_row_idx, end_row_idx)) {
				return true;
			}
		}
	}
	return false;
}

void ColumnDataCheckpointer::WritePersistentSegments() {
	// all segments are persistent and there are no updates
	// we only need to write the metadata
	auto segment = (ColumnSegment *)owned_segment.get();
	while (segment) {
		auto next_segment = move(segment->next);

		D_ASSERT(segment->segment_type == ColumnSegmentType::PERSISTENT);

		// set up the data pointer directly using the data from the persistent segment
		DataPointer pointer;
		pointer.block_pointer.block_id = segment->GetBlockId();
		pointer.block_pointer.offset = segment->GetBlockOffset();
		pointer.row_start = segment->start;
		pointer.tuple_count = segment->count;
		pointer.compression_type = segment->function->type;
		pointer.statistics = segment->stats.statistics->Copy();

		// merge the persistent stats into the global column stats
		state.global_stats->Merge(*segment->stats.statistics);

		// directly append the current segment to the new tree
		state.new_tree.AppendSegment(move(owned_segment));

		state.data_pointers.push_back(move(pointer));

		// move to the next segment in the list
		owned_segment = move(next_segment);
		segment = (ColumnSegment *)owned_segment.get();
	}
}

void ColumnDataCheckpointer::Checkpoint(unique_ptr<SegmentBase> segment) {
	D_ASSERT(!owned_segment);
	this->owned_segment = move(segment);
	// first check if any of the segments have changes
	if (!HasChanges()) {
		// no changes: only need to write the metadata for this column
		WritePersistentSegments();
	} else {
		// there are changes: rewrite the set of columns
		WriteToDisk();
	}
}

} // namespace duckdb














#include <cstring>

namespace duckdb {

unique_ptr<ColumnSegment> ColumnSegment::CreatePersistentSegment(DatabaseInstance &db, block_id_t block_id,
                                                                 idx_t offset, const LogicalType &type, idx_t start,
                                                                 idx_t count, CompressionType compression_type,
                                                                 unique_ptr<BaseStatistics> statistics) {
	auto &config = DBConfig::GetConfig(db);
	CompressionFunction *function;
	if (block_id == INVALID_BLOCK) {
		function = config.GetCompressionFunction(CompressionType::COMPRESSION_CONSTANT, type.InternalType());
	} else {
		function = config.GetCompressionFunction(compression_type, type.InternalType());
	}
	return make_unique<ColumnSegment>(db, type, ColumnSegmentType::PERSISTENT, start, count, function, move(statistics),
	                                  block_id, offset);
}

unique_ptr<ColumnSegment> ColumnSegment::CreateTransientSegment(DatabaseInstance &db, const LogicalType &type,
                                                                idx_t start) {
	auto &config = DBConfig::GetConfig(db);
	auto function = config.GetCompressionFunction(CompressionType::COMPRESSION_UNCOMPRESSED, type.InternalType());
	return make_unique<ColumnSegment>(db, type, ColumnSegmentType::TRANSIENT, start, 0, function, nullptr,
	                                  INVALID_BLOCK, 0);
}

ColumnSegment::ColumnSegment(DatabaseInstance &db, LogicalType type_p, ColumnSegmentType segment_type, idx_t start,
                             idx_t count, CompressionFunction *function_p, unique_ptr<BaseStatistics> statistics,
                             block_id_t block_id_p, idx_t offset_p)
    : SegmentBase(start, count), db(db), type(move(type_p)), type_size(GetTypeIdSize(type.InternalType())),
      segment_type(segment_type), function(function_p), stats(type, move(statistics)), block_id(block_id_p),
      offset(offset_p) {
	D_ASSERT(function);
	auto &buffer_manager = BufferManager::GetBufferManager(db);
	if (block_id == INVALID_BLOCK) {
		// no block id specified
		// there are two cases here:
		// transient: allocate a buffer for the uncompressed segment
		// persistent: constant segment, no need to allocate anything
		if (segment_type == ColumnSegmentType::TRANSIENT) {
			this->block = buffer_manager.RegisterMemory(Storage::BLOCK_SIZE, false);
		}
	} else {
		D_ASSERT(segment_type == ColumnSegmentType::PERSISTENT);
		this->block = buffer_manager.RegisterBlock(block_id);
	}
	if (function->init_segment) {
		segment_state = function->init_segment(*this, block_id);
	}
}

ColumnSegment::~ColumnSegment() {
}

//===--------------------------------------------------------------------===//
// Scan
//===--------------------------------------------------------------------===//
void ColumnSegment::InitializeScan(ColumnScanState &state) {
	state.scan_state = function->init_scan(*this);
}

void ColumnSegment::Scan(ColumnScanState &state, idx_t scan_count, Vector &result, idx_t result_offset,
                         bool entire_vector) {
	if (entire_vector) {
		D_ASSERT(result_offset == 0);
		Scan(state, scan_count, result);
	} else {
		D_ASSERT(result.GetVectorType() == VectorType::FLAT_VECTOR);
		ScanPartial(state, scan_count, result, result_offset);
		D_ASSERT(result.GetVectorType() == VectorType::FLAT_VECTOR);
	}
}

void ColumnSegment::Skip(ColumnScanState &state) {
	function->skip(*this, state, state.row_index - state.internal_index);
	state.internal_index = state.row_index;
}

void ColumnSegment::Scan(ColumnScanState &state, idx_t scan_count, Vector &result) {
	function->scan_vector(*this, state, scan_count, result);
}

void ColumnSegment::ScanPartial(ColumnScanState &state, idx_t scan_count, Vector &result, idx_t result_offset) {
	function->scan_partial(*this, state, scan_count, result, result_offset);
}

//===--------------------------------------------------------------------===//
// Fetch
//===--------------------------------------------------------------------===//
void ColumnSegment::FetchRow(ColumnFetchState &state, row_t row_id, Vector &result, idx_t result_idx) {
	function->fetch_row(*this, state, row_id - this->start, result, result_idx);
}

void ColumnSegment::InitializeAppend(ColumnAppendState &state) {
	D_ASSERT(segment_type == ColumnSegmentType::TRANSIENT);
}

//===--------------------------------------------------------------------===//
// Append
//===--------------------------------------------------------------------===//
idx_t ColumnSegment::Append(ColumnAppendState &state, VectorData &append_data, idx_t offset, idx_t count) {
	D_ASSERT(segment_type == ColumnSegmentType::TRANSIENT);
	if (!function->append) {
		throw InternalException("Attempting to append to a segment without append method");
	}
	return function->append(*this, stats, append_data, offset, count);
}

idx_t ColumnSegment::FinalizeAppend() {
	D_ASSERT(segment_type == ColumnSegmentType::TRANSIENT);
	if (!function->finalize_append) {
		throw InternalException("Attempting to call FinalizeAppend on a segment without a finalize_append method");
	}
	return function->finalize_append(*this, stats);
}

void ColumnSegment::RevertAppend(idx_t start_row) {
	D_ASSERT(segment_type == ColumnSegmentType::TRANSIENT);
	if (function->revert_append) {
		function->revert_append(*this, start_row);
	}
	this->count = start_row - this->start;
}

//===--------------------------------------------------------------------===//
// Convert To Persistent
//===--------------------------------------------------------------------===//
void ColumnSegment::ConvertToPersistent(block_id_t block_id_p) {
	D_ASSERT(segment_type == ColumnSegmentType::TRANSIENT);
	segment_type = ColumnSegmentType::PERSISTENT;
	block_id = block_id_p;
	offset = 0;

	if (block_id == INVALID_BLOCK) {
		// constant block: reset the block buffer
		block.reset();
	} else {
		// non-constant block: write the block to disk
		auto &buffer_manager = BufferManager::GetBufferManager(db);
		auto &block_manager = BlockManager::GetBlockManager(db);

		// the data for the block already exists in-memory of our block
		// instead of copying the data we alter some metadata so the buffer points to an on-disk block
		block = buffer_manager.ConvertToPersistent(block_manager, block_id, move(block));
	}

	segment_state.reset();
	if (function->init_segment) {
		segment_state = function->init_segment(*this, block_id);
	}
}

void ColumnSegment::ConvertToPersistent(shared_ptr<BlockHandle> block_p, block_id_t block_id_p, uint32_t offset_p) {
	D_ASSERT(segment_type == ColumnSegmentType::TRANSIENT);
	segment_type = ColumnSegmentType::PERSISTENT;
	block_id = block_id_p;
	offset = offset_p;
	block = move(block_p);

	segment_state.reset();
	if (function->init_segment) {
		segment_state = function->init_segment(*this, block_id);
	}
}

//===--------------------------------------------------------------------===//
// Filter Selection
//===--------------------------------------------------------------------===//
template <class T, class OP, bool HAS_NULL>
static idx_t TemplatedFilterSelection(T *vec, T *predicate, SelectionVector &sel, idx_t approved_tuple_count,
                                      ValidityMask &mask, SelectionVector &result_sel) {
	idx_t result_count = 0;
	for (idx_t i = 0; i < approved_tuple_count; i++) {
		auto idx = sel.get_index(i);
		if ((!HAS_NULL || mask.RowIsValid(idx)) && OP::Operation(vec[idx], *predicate)) {
			result_sel.set_index(result_count++, idx);
		}
	}
	return result_count;
}

template <class T>
static void FilterSelectionSwitch(T *vec, T *predicate, SelectionVector &sel, idx_t &approved_tuple_count,
                                  ExpressionType comparison_type, ValidityMask &mask) {
	SelectionVector new_sel(approved_tuple_count);
	// the inplace loops take the result as the last parameter
	switch (comparison_type) {
	case ExpressionType::COMPARE_EQUAL: {
		if (mask.AllValid()) {
			approved_tuple_count =
			    TemplatedFilterSelection<T, Equals, false>(vec, predicate, sel, approved_tuple_count, mask, new_sel);
		} else {
			approved_tuple_count =
			    TemplatedFilterSelection<T, Equals, true>(vec, predicate, sel, approved_tuple_count, mask, new_sel);
		}
		break;
	}
	case ExpressionType::COMPARE_NOTEQUAL: {
		if (mask.AllValid()) {
			approved_tuple_count =
			    TemplatedFilterSelection<T, NotEquals, false>(vec, predicate, sel, approved_tuple_count, mask, new_sel);
		} else {
			approved_tuple_count =
			    TemplatedFilterSelection<T, NotEquals, true>(vec, predicate, sel, approved_tuple_count, mask, new_sel);
		}
		break;
	}
	case ExpressionType::COMPARE_LESSTHAN: {
		if (mask.AllValid()) {
			approved_tuple_count =
			    TemplatedFilterSelection<T, LessThan, false>(vec, predicate, sel, approved_tuple_count, mask, new_sel);
		} else {
			approved_tuple_count =
			    TemplatedFilterSelection<T, LessThan, true>(vec, predicate, sel, approved_tuple_count, mask, new_sel);
		}
		break;
	}
	case ExpressionType::COMPARE_GREATERTHAN: {
		if (mask.AllValid()) {
			approved_tuple_count = TemplatedFilterSelection<T, GreaterThan, false>(vec, predicate, sel,
			                                                                       approved_tuple_count, mask, new_sel);
		} else {
			approved_tuple_count = TemplatedFilterSelection<T, GreaterThan, true>(vec, predicate, sel,
			                                                                      approved_tuple_count, mask, new_sel);
		}
		break;
	}
	case ExpressionType::COMPARE_LESSTHANOREQUALTO: {
		if (mask.AllValid()) {
			approved_tuple_count = TemplatedFilterSelection<T, LessThanEquals, false>(
			    vec, predicate, sel, approved_tuple_count, mask, new_sel);
		} else {
			approved_tuple_count = TemplatedFilterSelection<T, LessThanEquals, true>(
			    vec, predicate, sel, approved_tuple_count, mask, new_sel);
		}
		break;
	}
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO: {
		if (mask.AllValid()) {
			approved_tuple_count = TemplatedFilterSelection<T, GreaterThanEquals, false>(
			    vec, predicate, sel, approved_tuple_count, mask, new_sel);
		} else {
			approved_tuple_count = TemplatedFilterSelection<T, GreaterThanEquals, true>(
			    vec, predicate, sel, approved_tuple_count, mask, new_sel);
		}
		break;
	}
	default:
		throw NotImplementedException("Unknown comparison type for filter pushed down to table!");
	}
	sel.Initialize(new_sel);
}

template <bool IS_NULL>
static idx_t TemplatedNullSelection(SelectionVector &sel, idx_t approved_tuple_count, ValidityMask &mask) {
	if (mask.AllValid()) {
		// no NULL values
		if (IS_NULL) {
			return 0;
		} else {
			return approved_tuple_count;
		}
	} else {
		SelectionVector result_sel(approved_tuple_count);
		idx_t result_count = 0;
		for (idx_t i = 0; i < approved_tuple_count; i++) {
			auto idx = sel.get_index(i);
			if (mask.RowIsValid(idx) != IS_NULL) {
				result_sel.set_index(result_count++, idx);
			}
		}
		sel.Initialize(result_sel);
		return result_count;
	}
}

idx_t ColumnSegment::FilterSelection(SelectionVector &sel, Vector &result, const TableFilter &filter,
                                     idx_t &approved_tuple_count, ValidityMask &mask) {
	switch (filter.filter_type) {
	case TableFilterType::CONJUNCTION_OR: {
		// similar to the CONJUNCTION_AND, but we need to take care of the SelectionVectors (OR all of them)
		idx_t count_total = 0;
		SelectionVector result_sel(approved_tuple_count);
		auto &conjunction_or = (ConjunctionOrFilter &)filter;
		for (auto &child_filter : conjunction_or.child_filters) {
			SelectionVector temp_sel;
			temp_sel.Initialize(sel);
			idx_t temp_tuple_count = approved_tuple_count;
			idx_t temp_count = FilterSelection(temp_sel, result, *child_filter, temp_tuple_count, mask);
			// tuples passed, move them into the actual result vector
			for (idx_t i = 0; i < temp_count; i++) {
				auto new_idx = temp_sel.get_index(i);
				bool is_new_idx = true;
				for (idx_t res_idx = 0; res_idx < count_total; res_idx++) {
					if (result_sel.get_index(res_idx) == new_idx) {
						is_new_idx = false;
						break;
					}
				}
				if (is_new_idx) {
					result_sel.set_index(count_total++, new_idx);
				}
			}
		}
		sel.Initialize(result_sel);
		approved_tuple_count = count_total;
		return approved_tuple_count;
	}
	case TableFilterType::CONJUNCTION_AND: {
		auto &conjunction_and = (ConjunctionAndFilter &)filter;
		for (auto &child_filter : conjunction_and.child_filters) {
			FilterSelection(sel, result, *child_filter, approved_tuple_count, mask);
		}
		return approved_tuple_count;
	}
	case TableFilterType::CONSTANT_COMPARISON: {
		auto &constant_filter = (ConstantFilter &)filter;
		// the inplace loops take the result as the last parameter
		switch (result.GetType().InternalType()) {
		case PhysicalType::UINT8: {
			auto result_flat = FlatVector::GetData<uint8_t>(result);
			Vector predicate_vector(constant_filter.constant);
			auto predicate = FlatVector::GetData<uint8_t>(predicate_vector);
			FilterSelectionSwitch<uint8_t>(result_flat, predicate, sel, approved_tuple_count,
			                               constant_filter.comparison_type, mask);
			break;
		}
		case PhysicalType::UINT16: {
			auto result_flat = FlatVector::GetData<uint16_t>(result);
			Vector predicate_vector(constant_filter.constant);
			auto predicate = FlatVector::GetData<uint16_t>(predicate_vector);
			FilterSelectionSwitch<uint16_t>(result_flat, predicate, sel, approved_tuple_count,
			                                constant_filter.comparison_type, mask);
			break;
		}
		case PhysicalType::UINT32: {
			auto result_flat = FlatVector::GetData<uint32_t>(result);
			Vector predicate_vector(constant_filter.constant);
			auto predicate = FlatVector::GetData<uint32_t>(predicate_vector);
			FilterSelectionSwitch<uint32_t>(result_flat, predicate, sel, approved_tuple_count,
			                                constant_filter.comparison_type, mask);
			break;
		}
		case PhysicalType::UINT64: {
			auto result_flat = FlatVector::GetData<uint64_t>(result);
			Vector predicate_vector(constant_filter.constant);
			auto predicate = FlatVector::GetData<uint64_t>(predicate_vector);
			FilterSelectionSwitch<uint64_t>(result_flat, predicate, sel, approved_tuple_count,
			                                constant_filter.comparison_type, mask);
			break;
		}
		case PhysicalType::INT8: {
			auto result_flat = FlatVector::GetData<int8_t>(result);
			Vector predicate_vector(constant_filter.constant);
			auto predicate = FlatVector::GetData<int8_t>(predicate_vector);
			FilterSelectionSwitch<int8_t>(result_flat, predicate, sel, approved_tuple_count,
			                              constant_filter.comparison_type, mask);
			break;
		}
		case PhysicalType::INT16: {
			auto result_flat = FlatVector::GetData<int16_t>(result);
			Vector predicate_vector(constant_filter.constant);
			auto predicate = FlatVector::GetData<int16_t>(predicate_vector);
			FilterSelectionSwitch<int16_t>(result_flat, predicate, sel, approved_tuple_count,
			                               constant_filter.comparison_type, mask);
			break;
		}
		case PhysicalType::INT32: {
			auto result_flat = FlatVector::GetData<int32_t>(result);
			Vector predicate_vector(constant_filter.constant);
			auto predicate = FlatVector::GetData<int32_t>(predicate_vector);
			FilterSelectionSwitch<int32_t>(result_flat, predicate, sel, approved_tuple_count,
			                               constant_filter.comparison_type, mask);
			break;
		}
		case PhysicalType::INT64: {
			auto result_flat = FlatVector::GetData<int64_t>(result);
			Vector predicate_vector(constant_filter.constant);
			auto predicate = FlatVector::GetData<int64_t>(predicate_vector);
			FilterSelectionSwitch<int64_t>(result_flat, predicate, sel, approved_tuple_count,
			                               constant_filter.comparison_type, mask);
			break;
		}
		case PhysicalType::INT128: {
			auto result_flat = FlatVector::GetData<hugeint_t>(result);
			Vector predicate_vector(constant_filter.constant);
			auto predicate = FlatVector::GetData<hugeint_t>(predicate_vector);
			FilterSelectionSwitch<hugeint_t>(result_flat, predicate, sel, approved_tuple_count,
			                                 constant_filter.comparison_type, mask);
			break;
		}
		case PhysicalType::FLOAT: {
			auto result_flat = FlatVector::GetData<float>(result);
			Vector predicate_vector(constant_filter.constant);
			auto predicate = FlatVector::GetData<float>(predicate_vector);
			FilterSelectionSwitch<float>(result_flat, predicate, sel, approved_tuple_count,
			                             constant_filter.comparison_type, mask);
			break;
		}
		case PhysicalType::DOUBLE: {
			auto result_flat = FlatVector::GetData<double>(result);
			Vector predicate_vector(constant_filter.constant);
			auto predicate = FlatVector::GetData<double>(predicate_vector);
			FilterSelectionSwitch<double>(result_flat, predicate, sel, approved_tuple_count,
			                              constant_filter.comparison_type, mask);
			break;
		}
		case PhysicalType::VARCHAR: {
			auto result_flat = FlatVector::GetData<string_t>(result);
			Vector predicate_vector(constant_filter.constant);
			auto predicate = FlatVector::GetData<string_t>(predicate_vector);
			FilterSelectionSwitch<string_t>(result_flat, predicate, sel, approved_tuple_count,
			                                constant_filter.comparison_type, mask);
			break;
		}
		case PhysicalType::BOOL: {
			auto result_flat = FlatVector::GetData<bool>(result);
			Vector predicate_vector(constant_filter.constant);
			auto predicate = FlatVector::GetData<bool>(predicate_vector);
			FilterSelectionSwitch<bool>(result_flat, predicate, sel, approved_tuple_count,
			                            constant_filter.comparison_type, mask);
			break;
		}
		default:
			throw InvalidTypeException(result.GetType(), "Invalid type for filter pushed down to table comparison");
		}
		return approved_tuple_count;
	}
	case TableFilterType::IS_NULL:
		return TemplatedNullSelection<true>(sel, approved_tuple_count, mask);
	case TableFilterType::IS_NOT_NULL:
		return TemplatedNullSelection<false>(sel, approved_tuple_count, mask);
	default:
		throw InternalException("FIXME: unsupported type for filter selection");
	}
}

} // namespace duckdb



namespace duckdb {

ListColumnData::ListColumnData(DataTableInfo &info, idx_t column_index, idx_t start_row, LogicalType type_p,
                               ColumnData *parent)
    : ColumnData(info, column_index, start_row, move(type_p), parent), validity(info, 0, start_row, this) {
	D_ASSERT(type.InternalType() == PhysicalType::LIST);
	auto &child_type = ListType::GetChildType(type);
	// the child column, with column index 1 (0 is the validity mask)
	child_column = ColumnData::CreateColumnUnique(info, 1, start_row, child_type, this);
}

bool ListColumnData::CheckZonemap(ColumnScanState &state, TableFilter &filter) {
	// table filters are not supported yet for list columns
	return false;
}

void ListColumnData::InitializeScan(ColumnScanState &state) {
	ColumnData::InitializeScan(state);

	// initialize the validity segment
	ColumnScanState validity_state;
	validity.InitializeScan(validity_state);
	state.child_states.push_back(move(validity_state));

	// initialize the child scan
	ColumnScanState child_state;
	child_column->InitializeScan(child_state);
	state.child_states.push_back(move(child_state));
}

list_entry_t ListColumnData::FetchListEntry(idx_t row_idx) {
	auto segment = (ColumnSegment *)data.GetSegment(row_idx);
	ColumnFetchState fetch_state;
	Vector result(type, 1);
	segment->FetchRow(fetch_state, row_idx, result, 0);

	// initialize the child scan with the required offset
	auto list_data = FlatVector::GetData<list_entry_t>(result);
	return list_data[0];
}

void ListColumnData::InitializeScanWithOffset(ColumnScanState &state, idx_t row_idx) {
	if (row_idx == 0) {
		InitializeScan(state);
		return;
	}
	ColumnData::InitializeScanWithOffset(state, row_idx);

	// initialize the validity segment
	ColumnScanState validity_state;
	validity.InitializeScanWithOffset(validity_state, row_idx);
	state.child_states.push_back(move(validity_state));

	// we need to read the list at position row_idx to get the correct row offset of the child
	auto list_entry = FetchListEntry(row_idx);
	auto child_offset = list_entry.offset;

	D_ASSERT(child_offset <= child_column->GetMaxEntry());
	ColumnScanState child_state;
	if (child_offset < child_column->GetMaxEntry()) {
		child_column->InitializeScanWithOffset(child_state, child_offset);
	}
	state.child_states.push_back(move(child_state));
}

idx_t ListColumnData::Scan(Transaction &transaction, idx_t vector_index, ColumnScanState &state, Vector &result) {
	return ScanCount(state, result, STANDARD_VECTOR_SIZE);
}

idx_t ListColumnData::ScanCommitted(idx_t vector_index, ColumnScanState &state, Vector &result, bool allow_updates) {
	return ScanCount(state, result, STANDARD_VECTOR_SIZE);
}

idx_t ListColumnData::ScanCount(ColumnScanState &state, Vector &result, idx_t count) {
	if (count == 0) {
		return 0;
	}
	// updates not supported for lists
	D_ASSERT(!updates);

	idx_t scan_count = ScanVector(state, result, count);
	D_ASSERT(scan_count > 0);
	validity.ScanCount(state.child_states[0], result, count);

	auto data = FlatVector::GetData<list_entry_t>(result);
	auto first_entry = data[0];
	auto last_entry = data[scan_count - 1];

#ifdef DEBUG
	for (idx_t i = 1; i < scan_count; i++) {
		D_ASSERT(data[i].offset == data[i - 1].offset + data[i - 1].length);
	}
#endif
	// shift all offsets so they are 0 at the first entry
	for (idx_t i = 0; i < scan_count; i++) {
		data[i].offset -= first_entry.offset;
	}

	D_ASSERT(last_entry.offset >= first_entry.offset);
	idx_t child_scan_count = last_entry.offset + last_entry.length - first_entry.offset;
	ListVector::Reserve(result, child_scan_count);

	if (child_scan_count > 0) {
		auto &child_entry = ListVector::GetEntry(result);
		D_ASSERT(child_entry.GetType().InternalType() == PhysicalType::STRUCT ||
		         state.child_states[1].row_index + child_scan_count <= child_column->GetMaxEntry());
		child_column->ScanCount(state.child_states[1], child_entry, child_scan_count);
	}

	ListVector::SetListSize(result, child_scan_count);
	return scan_count;
}

void ListColumnData::Skip(ColumnScanState &state, idx_t count) {
	// skip inside the validity segment
	validity.Skip(state.child_states[0], count);

	// we need to read the list entries/offsets to figure out how much to skip
	// note that we only need to read the first and last entry
	// however, let's just read all "count" entries for now
	auto data = unique_ptr<list_entry_t[]>(new list_entry_t[count]);
	Vector result(type, (data_ptr_t)data.get());
	idx_t scan_count = ScanVector(state, result, count);
	if (scan_count == 0) {
		return;
	}

	auto &first_entry = data[0];
	auto &last_entry = data[scan_count - 1];
	idx_t child_scan_count = last_entry.offset + last_entry.length - first_entry.offset;

	// skip the child state forward by the child_scan_count
	child_column->Skip(state.child_states[1], child_scan_count);
}

void ListColumnData::InitializeAppend(ColumnAppendState &state) {
	// initialize the list offset append
	ColumnData::InitializeAppend(state);

	// initialize the validity append
	ColumnAppendState validity_append_state;
	validity.InitializeAppend(validity_append_state);
	state.child_appends.push_back(move(validity_append_state));

	// initialize the child column append
	ColumnAppendState child_append_state;
	child_column->InitializeAppend(child_append_state);
	state.child_appends.push_back(move(child_append_state));
}

void ListColumnData::Append(BaseStatistics &stats_p, ColumnAppendState &state, Vector &vector, idx_t count) {
	D_ASSERT(count > 0);
	auto &stats = (ListStatistics &)stats_p;

	vector.Normalify(count);
	auto &list_validity = FlatVector::Validity(vector);

	// construct the list_entry_t entries to append to the column data
	auto input_offsets = FlatVector::GetData<list_entry_t>(vector);
	auto start_offset = child_column->GetMaxEntry();
	idx_t child_count = 0;

	auto append_offsets = unique_ptr<list_entry_t[]>(new list_entry_t[count]);
	for (idx_t i = 0; i < count; i++) {
		if (list_validity.RowIsValid(i)) {
			append_offsets[i].offset = start_offset + input_offsets[i].offset;
			append_offsets[i].length = input_offsets[i].length;
			child_count += input_offsets[i].length;
		} else {
			if (i > 0) {
				append_offsets[i].offset = append_offsets[i - 1].offset + append_offsets[i - 1].length;
			} else {
				append_offsets[i].offset = start_offset;
			}
			append_offsets[i].length = 0;
		}
	}
#ifdef DEBUG
	D_ASSERT(append_offsets[0].offset == start_offset);
	for (idx_t i = 1; i < count; i++) {
		D_ASSERT(append_offsets[i].offset == append_offsets[i - 1].offset + append_offsets[i - 1].length);
	}
	D_ASSERT(append_offsets[count - 1].offset + append_offsets[count - 1].length - append_offsets[0].offset ==
	         child_count);
#endif

	VectorData vdata;
	vdata.validity = list_validity;
	vdata.sel = FlatVector::IncrementalSelectionVector();
	vdata.data = (data_ptr_t)append_offsets.get();

	// append the list offsets
	ColumnData::AppendData(stats, state, vdata, count);
	// append the validity data
	validity.AppendData(*stats.validity_stats, state.child_appends[0], vdata, count);
	// append the child vector
	if (child_count > 0) {
		auto &child_vector = ListVector::GetEntry(vector);
		child_column->Append(*stats.child_stats, state.child_appends[1], child_vector, child_count);
	}
}

void ListColumnData::RevertAppend(row_t start_row) {
	ColumnData::RevertAppend(start_row);
	validity.RevertAppend(start_row);
	auto column_count = GetMaxEntry();
	if (column_count > start) {
		// revert append in the child column
		auto list_entry = FetchListEntry(column_count - 1);
		child_column->RevertAppend(list_entry.offset + list_entry.length);
	}
}

idx_t ListColumnData::Fetch(ColumnScanState &state, row_t row_id, Vector &result) {
	throw NotImplementedException("List Fetch");
}

void ListColumnData::Update(Transaction &transaction, idx_t column_index, Vector &update_vector, row_t *row_ids,
                            idx_t update_count) {
	throw NotImplementedException("List Update is not supported.");
}

void ListColumnData::UpdateColumn(Transaction &transaction, const vector<column_t> &column_path, Vector &update_vector,
                                  row_t *row_ids, idx_t update_count, idx_t depth) {
	throw NotImplementedException("List Update Column is not supported");
}

unique_ptr<BaseStatistics> ListColumnData::GetUpdateStatistics() {
	return nullptr;
}

void ListColumnData::FetchRow(Transaction &transaction, ColumnFetchState &state, row_t row_id, Vector &result,
                              idx_t result_idx) {
	// insert any child states that are required
	// we need two (validity & list child)
	// note that we need a scan state for the child vector
	// this is because we will (potentially) fetch more than one tuple from the list child
	if (state.child_states.empty()) {
		auto child_state = make_unique<ColumnFetchState>();
		state.child_states.push_back(move(child_state));
	}
	// fetch the list_entry_t and the validity mask for that list
	auto segment = (ColumnSegment *)data.GetSegment(row_id);

	// now perform the fetch within the segment
	segment->FetchRow(state, row_id, result, result_idx);
	validity.FetchRow(transaction, *state.child_states[0], row_id, result, result_idx);

	auto &validity = FlatVector::Validity(result);
	auto list_data = FlatVector::GetData<list_entry_t>(result);
	auto &list_entry = list_data[result_idx];
	auto original_offset = list_entry.offset;
	// set the list entry offset to the size of the current list
	list_entry.offset = ListVector::GetListSize(result);
	if (!validity.RowIsValid(result_idx)) {
		// the list is NULL! no need to fetch the child
		D_ASSERT(list_entry.length == 0);
		return;
	}

	// now we need to read from the child all the elements between [offset...length]
	auto child_scan_count = list_entry.length;
	if (child_scan_count > 0) {
		auto child_state = make_unique<ColumnScanState>();
		auto &child_type = ListType::GetChildType(result.GetType());
		Vector child_scan(child_type, child_scan_count);
		// seek the scan towards the specified position and read [length] entries
		child_column->InitializeScanWithOffset(*child_state, original_offset);
		D_ASSERT(child_type.InternalType() == PhysicalType::STRUCT ||
		         child_state->row_index + child_scan_count <= child_column->GetMaxEntry());
		child_column->ScanCount(*child_state, child_scan, child_scan_count);

		ListVector::Append(result, child_scan, child_scan_count);
	}
}

void ListColumnData::CommitDropColumn() {
	validity.CommitDropColumn();
	child_column->CommitDropColumn();
}

struct ListColumnCheckpointState : public ColumnCheckpointState {
	ListColumnCheckpointState(RowGroup &row_group, ColumnData &column_data, TableDataWriter &writer)
	    : ColumnCheckpointState(row_group, column_data, writer) {
		global_stats = make_unique<ListStatistics>(column_data.type);
	}

	unique_ptr<ColumnCheckpointState> validity_state;
	unique_ptr<ColumnCheckpointState> child_state;

public:
	unique_ptr<BaseStatistics> GetStatistics() override {
		auto stats = global_stats->Copy();
		auto &list_stats = (ListStatistics &)*stats;
		stats->validity_stats = validity_state->GetStatistics();
		list_stats.child_stats = child_state->GetStatistics();
		return stats;
	}

	void FlushToDisk() override {
		ColumnCheckpointState::FlushToDisk();
		validity_state->FlushToDisk();
		child_state->FlushToDisk();
	}
};

unique_ptr<ColumnCheckpointState> ListColumnData::CreateCheckpointState(RowGroup &row_group, TableDataWriter &writer) {
	return make_unique<ListColumnCheckpointState>(row_group, *this, writer);
}

unique_ptr<ColumnCheckpointState> ListColumnData::Checkpoint(RowGroup &row_group, TableDataWriter &writer,
                                                             ColumnCheckpointInfo &checkpoint_info) {
	auto validity_state = validity.Checkpoint(row_group, writer, checkpoint_info);
	auto base_state = ColumnData::Checkpoint(row_group, writer, checkpoint_info);
	auto child_state = child_column->Checkpoint(row_group, writer, checkpoint_info);

	auto &checkpoint_state = (ListColumnCheckpointState &)*base_state;
	checkpoint_state.validity_state = move(validity_state);
	checkpoint_state.child_state = move(child_state);
	return base_state;
}

void ListColumnData::DeserializeColumn(Deserializer &source) {
	ColumnData::DeserializeColumn(source);
	validity.DeserializeColumn(source);
	child_column->DeserializeColumn(source);
}

void ListColumnData::GetStorageInfo(idx_t row_group_index, vector<idx_t> col_path, vector<vector<Value>> &result) {
	col_path.push_back(0);
	validity.GetStorageInfo(row_group_index, col_path, result);
	col_path.back() = 1;
	child_column->GetStorageInfo(row_group_index, col_path, result);
}

} // namespace duckdb



namespace duckdb {

PersistentTableData::PersistentTableData(idx_t column_count) {
}

PersistentTableData::~PersistentTableData() {
}

} // namespace duckdb
















namespace duckdb {

constexpr const idx_t RowGroup::ROW_GROUP_VECTOR_COUNT;
constexpr const idx_t RowGroup::ROW_GROUP_SIZE;

RowGroup::RowGroup(DatabaseInstance &db, DataTableInfo &table_info, idx_t start, idx_t count)
    : SegmentBase(start, count), db(db), table_info(table_info) {

	Verify();
}

RowGroup::RowGroup(DatabaseInstance &db, DataTableInfo &table_info, const vector<LogicalType> &types,
                   RowGroupPointer &pointer)
    : SegmentBase(pointer.row_start, pointer.tuple_count), db(db), table_info(table_info) {
	// deserialize the columns
	if (pointer.data_pointers.size() != types.size()) {
		throw IOException("Row group column count is unaligned with table column count. Corrupt file?");
	}
	for (idx_t i = 0; i < pointer.data_pointers.size(); i++) {
		auto &block_pointer = pointer.data_pointers[i];
		MetaBlockReader column_data_reader(db, block_pointer.block_id);
		column_data_reader.offset = block_pointer.offset;
		this->columns.push_back(ColumnData::Deserialize(table_info, i, start, column_data_reader, types[i], nullptr));
	}

	// set up the statistics
	for (auto &stats : pointer.statistics) {
		auto stats_type = stats->type;
		this->stats.push_back(make_shared<SegmentStatistics>(stats_type, move(stats)));
	}
	this->version_info = move(pointer.versions);

	Verify();
}

RowGroup::~RowGroup() {
}

void RowGroup::InitializeEmpty(const vector<LogicalType> &types) {
	// set up the segment trees for the column segments
	for (idx_t i = 0; i < types.size(); i++) {
		auto column_data = ColumnData::CreateColumn(GetTableInfo(), i, start, types[i]);
		stats.push_back(make_shared<SegmentStatistics>(types[i]));
		columns.push_back(move(column_data));
	}
}

bool RowGroup::InitializeScanWithOffset(RowGroupScanState &state, idx_t vector_offset) {
	auto &column_ids = state.parent.column_ids;
	if (state.parent.table_filters) {
		if (!CheckZonemap(*state.parent.table_filters, column_ids)) {
			return false;
		}
	}

	state.row_group = this;
	state.vector_index = vector_offset;
	state.max_row =
	    this->start > state.parent.max_row ? 0 : MinValue<idx_t>(this->count, state.parent.max_row - this->start);
	state.column_scans = unique_ptr<ColumnScanState[]>(new ColumnScanState[column_ids.size()]);
	for (idx_t i = 0; i < column_ids.size(); i++) {
		auto column = column_ids[i];
		if (column != COLUMN_IDENTIFIER_ROW_ID) {
			columns[column]->InitializeScanWithOffset(state.column_scans[i],
			                                          start + vector_offset * STANDARD_VECTOR_SIZE);
		} else {
			state.column_scans[i].current = nullptr;
		}
	}
	return true;
}

bool RowGroup::InitializeScan(RowGroupScanState &state) {
	auto &column_ids = state.parent.column_ids;
	if (state.parent.table_filters) {
		if (!CheckZonemap(*state.parent.table_filters, column_ids)) {
			return false;
		}
	}
	state.row_group = this;
	state.vector_index = 0;
	state.max_row =
	    this->start > state.parent.max_row ? 0 : MinValue<idx_t>(this->count, state.parent.max_row - this->start);
	state.column_scans = unique_ptr<ColumnScanState[]>(new ColumnScanState[column_ids.size()]);
	for (idx_t i = 0; i < column_ids.size(); i++) {
		auto column = column_ids[i];
		if (column != COLUMN_IDENTIFIER_ROW_ID) {
			columns[column]->InitializeScan(state.column_scans[i]);
		} else {
			state.column_scans[i].current = nullptr;
		}
	}
	return true;
}

unique_ptr<RowGroup> RowGroup::AlterType(ClientContext &context, const LogicalType &target_type, idx_t changed_idx,
                                         ExpressionExecutor &executor, TableScanState &scan_state,
                                         DataChunk &scan_chunk) {
	Verify();

	// construct a new column data for this type
	auto column_data = ColumnData::CreateColumn(GetTableInfo(), changed_idx, start, target_type);

	ColumnAppendState append_state;
	column_data->InitializeAppend(append_state);

	// scan the original table, and fill the new column with the transformed value
	InitializeScan(scan_state.row_group_scan_state);

	Vector append_vector(target_type);
	auto altered_col_stats = make_shared<SegmentStatistics>(target_type);
	while (true) {
		// scan the table
		scan_chunk.Reset();
		ScanCommitted(scan_state.row_group_scan_state, scan_chunk, TableScanType::TABLE_SCAN_COMMITTED_ROWS);
		if (scan_chunk.size() == 0) {
			break;
		}
		// execute the expression
		executor.ExecuteExpression(scan_chunk, append_vector);
		column_data->Append(*altered_col_stats->statistics, append_state, append_vector, scan_chunk.size());
	}

	// set up the row_group based on this row_group
	auto row_group = make_unique<RowGroup>(db, table_info, this->start, this->count);
	row_group->version_info = version_info;
	for (idx_t i = 0; i < columns.size(); i++) {
		if (i == changed_idx) {
			// this is the altered column: use the new column
			row_group->columns.push_back(move(column_data));
			row_group->stats.push_back(move(altered_col_stats));
		} else {
			// this column was not altered: use the data directly
			row_group->columns.push_back(columns[i]);
			row_group->stats.push_back(stats[i]);
		}
	}
	row_group->Verify();
	return row_group;
}

unique_ptr<RowGroup> RowGroup::AddColumn(ClientContext &context, ColumnDefinition &new_column,
                                         ExpressionExecutor &executor, Expression *default_value, Vector &result) {
	Verify();

	// construct a new column data for the new column
	auto added_column = ColumnData::CreateColumn(GetTableInfo(), columns.size(), start, new_column.Type());
	auto added_col_stats = make_shared<SegmentStatistics>(
	    new_column.Type(), BaseStatistics::CreateEmpty(new_column.Type(), StatisticsType::LOCAL_STATS));

	idx_t rows_to_write = this->count;
	if (rows_to_write > 0) {
		DataChunk dummy_chunk;

		ColumnAppendState state;
		added_column->InitializeAppend(state);
		for (idx_t i = 0; i < rows_to_write; i += STANDARD_VECTOR_SIZE) {
			idx_t rows_in_this_vector = MinValue<idx_t>(rows_to_write - i, STANDARD_VECTOR_SIZE);
			if (default_value) {
				dummy_chunk.SetCardinality(rows_in_this_vector);
				executor.ExecuteExpression(dummy_chunk, result);
			}
			added_column->Append(*added_col_stats->statistics, state, result, rows_in_this_vector);
		}
	}

	// set up the row_group based on this row_group
	auto row_group = make_unique<RowGroup>(db, table_info, this->start, this->count);
	row_group->version_info = version_info;
	row_group->columns = columns;
	row_group->stats = stats;
	// now add the new column
	row_group->columns.push_back(move(added_column));
	row_group->stats.push_back(move(added_col_stats));

	row_group->Verify();
	return row_group;
}

unique_ptr<RowGroup> RowGroup::RemoveColumn(idx_t removed_column) {
	Verify();

	D_ASSERT(removed_column < columns.size());

	auto row_group = make_unique<RowGroup>(db, table_info, this->start, this->count);
	row_group->version_info = version_info;
	row_group->columns = columns;
	row_group->stats = stats;
	// now remove the column
	row_group->columns.erase(row_group->columns.begin() + removed_column);
	row_group->stats.erase(row_group->stats.begin() + removed_column);

	row_group->Verify();
	return row_group;
}

void RowGroup::CommitDrop() {
	for (idx_t column_idx = 0; column_idx < columns.size(); column_idx++) {
		CommitDropColumn(column_idx);
	}
}

void RowGroup::CommitDropColumn(idx_t column_idx) {
	D_ASSERT(column_idx < columns.size());
	columns[column_idx]->CommitDropColumn();
}

void RowGroup::NextVector(RowGroupScanState &state) {
	state.vector_index++;
	for (idx_t i = 0; i < state.parent.column_ids.size(); i++) {
		auto column = state.parent.column_ids[i];
		if (column == COLUMN_IDENTIFIER_ROW_ID) {
			continue;
		}
		D_ASSERT(column < columns.size());
		columns[column]->Skip(state.column_scans[i]);
	}
}

bool RowGroup::CheckZonemap(TableFilterSet &filters, const vector<column_t> &column_ids) {
	for (auto &entry : filters.filters) {
		auto column_index = entry.first;
		auto &filter = entry.second;
		auto base_column_index = column_ids[column_index];

		auto propagate_result = filter->CheckStatistics(*stats[base_column_index]->statistics);
		if (propagate_result == FilterPropagateResult::FILTER_ALWAYS_FALSE ||
		    propagate_result == FilterPropagateResult::FILTER_FALSE_OR_NULL) {
			return false;
		}
	}
	return true;
}

bool RowGroup::CheckZonemapSegments(RowGroupScanState &state) {
	if (!state.parent.table_filters) {
		return true;
	}
	auto &column_ids = state.parent.column_ids;
	for (auto &entry : state.parent.table_filters->filters) {
		D_ASSERT(entry.first < column_ids.size());
		auto column_idx = entry.first;
		auto base_column_idx = column_ids[column_idx];
		bool read_segment = columns[base_column_idx]->CheckZonemap(state.column_scans[column_idx], *entry.second);
		if (!read_segment) {
			idx_t target_row =
			    state.column_scans[column_idx].current->start + state.column_scans[column_idx].current->count;
			D_ASSERT(target_row >= this->start);
			D_ASSERT(target_row <= this->start + this->count);
			idx_t target_vector_index = (target_row - this->start) / STANDARD_VECTOR_SIZE;
			if (state.vector_index == target_vector_index) {
				// we can't skip any full vectors because this segment contains less than a full vector
				// for now we just bail-out
				// FIXME: we could check if we can ALSO skip the next segments, in which case skipping a full vector
				// might be possible
				// we don't care that much though, since a single segment that fits less than a full vector is
				// exceedingly rare
				return true;
			}
			while (state.vector_index < target_vector_index) {
				NextVector(state);
			}
			return false;
		}
	}

	return true;
}

template <TableScanType TYPE>
void RowGroup::TemplatedScan(Transaction *transaction, RowGroupScanState &state, DataChunk &result) {
	const bool ALLOW_UPDATES = TYPE != TableScanType::TABLE_SCAN_COMMITTED_ROWS_DISALLOW_UPDATES &&
	                           TYPE != TableScanType::TABLE_SCAN_COMMITTED_ROWS_OMIT_PERMANENTLY_DELETED;
	auto &table_filters = state.parent.table_filters;
	auto &column_ids = state.parent.column_ids;
	auto &adaptive_filter = state.parent.adaptive_filter;
	while (true) {
		if (state.vector_index * STANDARD_VECTOR_SIZE >= state.max_row) {
			// exceeded the amount of rows to scan
			return;
		}
		idx_t current_row = state.vector_index * STANDARD_VECTOR_SIZE;
		auto max_count = MinValue<idx_t>(STANDARD_VECTOR_SIZE, state.max_row - current_row);

		//! first check the zonemap if we have to scan this partition
		if (!CheckZonemapSegments(state)) {
			continue;
		}
		// second, scan the version chunk manager to figure out which tuples to load for this transaction
		idx_t count;
		SelectionVector valid_sel(STANDARD_VECTOR_SIZE);
		if (TYPE == TableScanType::TABLE_SCAN_REGULAR) {
			D_ASSERT(transaction);
			count = state.row_group->GetSelVector(*transaction, state.vector_index, valid_sel, max_count);
			if (count == 0) {
				// nothing to scan for this vector, skip the entire vector
				NextVector(state);
				continue;
			}
		} else if (TYPE == TableScanType::TABLE_SCAN_COMMITTED_ROWS_OMIT_PERMANENTLY_DELETED) {
			auto &transaction_manager = TransactionManager::Get(db);
			auto lowest_active_start = transaction_manager.LowestActiveStart();
			auto lowest_active_id = transaction_manager.LowestActiveId();

			count = state.row_group->GetCommittedSelVector(lowest_active_start, lowest_active_id, state.vector_index,
			                                               valid_sel, max_count);
			if (count == 0) {
				// nothing to scan for this vector, skip the entire vector
				NextVector(state);
				continue;
			}
		} else {
			count = max_count;
		}
		if (count == max_count && !table_filters) {
			// scan all vectors completely: full scan without deletions or table filters
			for (idx_t i = 0; i < column_ids.size(); i++) {
				auto column = column_ids[i];
				if (column == COLUMN_IDENTIFIER_ROW_ID) {
					// scan row id
					D_ASSERT(result.data[i].GetType().InternalType() == ROW_TYPE);
					result.data[i].Sequence(this->start + current_row, 1);
				} else {
					if (TYPE != TableScanType::TABLE_SCAN_REGULAR) {
						columns[column]->ScanCommitted(state.vector_index, state.column_scans[i], result.data[i],
						                               ALLOW_UPDATES);
					} else {
						D_ASSERT(transaction);
						columns[column]->Scan(*transaction, state.vector_index, state.column_scans[i], result.data[i]);
					}
				}
			}
		} else {
			// partial scan: we have deletions or table filters
			idx_t approved_tuple_count = count;
			SelectionVector sel;
			if (count != max_count) {
				sel.Initialize(valid_sel);
			} else {
				sel.Initialize(nullptr);
			}
			//! first, we scan the columns with filters, fetch their data and generate a selection vector.
			//! get runtime statistics
			auto start_time = high_resolution_clock::now();
			if (table_filters) {
				D_ASSERT(ALLOW_UPDATES);
				for (idx_t i = 0; i < table_filters->filters.size(); i++) {
					auto tf_idx = adaptive_filter->permutation[i];
					auto col_idx = column_ids[tf_idx];
					columns[col_idx]->Select(*transaction, state.vector_index, state.column_scans[tf_idx],
					                         result.data[tf_idx], sel, approved_tuple_count,
					                         *table_filters->filters[tf_idx]);
				}
				for (auto &table_filter : table_filters->filters) {
					result.data[table_filter.first].Slice(sel, approved_tuple_count);
				}
			}
			if (approved_tuple_count == 0) {
				// all rows were filtered out by the table filters
				// skip this vector in all the scans that were not scanned yet
				D_ASSERT(table_filters);
				result.Reset();
				for (idx_t i = 0; i < column_ids.size(); i++) {
					auto col_idx = column_ids[i];
					if (col_idx == COLUMN_IDENTIFIER_ROW_ID) {
						continue;
					}
					if (table_filters->filters.find(i) == table_filters->filters.end()) {
						columns[col_idx]->Skip(state.column_scans[i]);
					}
				}
				state.vector_index++;
				continue;
			}
			//! Now we use the selection vector to fetch data for the other columns.
			for (idx_t i = 0; i < column_ids.size(); i++) {
				if (!table_filters || table_filters->filters.find(i) == table_filters->filters.end()) {
					auto column = column_ids[i];
					if (column == COLUMN_IDENTIFIER_ROW_ID) {
						D_ASSERT(result.data[i].GetType().InternalType() == PhysicalType::INT64);
						result.data[i].SetVectorType(VectorType::FLAT_VECTOR);
						auto result_data = (int64_t *)FlatVector::GetData(result.data[i]);
						for (size_t sel_idx = 0; sel_idx < approved_tuple_count; sel_idx++) {
							result_data[sel_idx] = this->start + current_row + sel.get_index(sel_idx);
						}
					} else {
						if (TYPE == TableScanType::TABLE_SCAN_REGULAR) {
							D_ASSERT(transaction);
							columns[column]->FilterScan(*transaction, state.vector_index, state.column_scans[i],
							                            result.data[i], sel, approved_tuple_count);
						} else {
							D_ASSERT(!transaction);
							columns[column]->FilterScanCommitted(state.vector_index, state.column_scans[i],
							                                     result.data[i], sel, approved_tuple_count,
							                                     ALLOW_UPDATES);
						}
					}
				}
			}
			auto end_time = high_resolution_clock::now();
			if (adaptive_filter && table_filters->filters.size() > 1) {
				adaptive_filter->AdaptRuntimeStatistics(duration_cast<duration<double>>(end_time - start_time).count());
			}
			D_ASSERT(approved_tuple_count > 0);
			count = approved_tuple_count;
		}
		result.SetCardinality(count);
		state.vector_index++;
		break;
	}
}

void RowGroup::Scan(Transaction &transaction, RowGroupScanState &state, DataChunk &result) {
	TemplatedScan<TableScanType::TABLE_SCAN_REGULAR>(&transaction, state, result);
}

void RowGroup::ScanCommitted(RowGroupScanState &state, DataChunk &result, TableScanType type) {
	switch (type) {
	case TableScanType::TABLE_SCAN_COMMITTED_ROWS:
		TemplatedScan<TableScanType::TABLE_SCAN_COMMITTED_ROWS>(nullptr, state, result);
		break;
	case TableScanType::TABLE_SCAN_COMMITTED_ROWS_DISALLOW_UPDATES:
		TemplatedScan<TableScanType::TABLE_SCAN_COMMITTED_ROWS_DISALLOW_UPDATES>(nullptr, state, result);
		break;
	case TableScanType::TABLE_SCAN_COMMITTED_ROWS_OMIT_PERMANENTLY_DELETED:
		TemplatedScan<TableScanType::TABLE_SCAN_COMMITTED_ROWS_OMIT_PERMANENTLY_DELETED>(nullptr, state, result);
		break;
	default:
		throw InternalException("Unrecognized table scan type");
	}
}

ChunkInfo *RowGroup::GetChunkInfo(idx_t vector_idx) {
	if (!version_info) {
		return nullptr;
	}
	return version_info->info[vector_idx].get();
}

idx_t RowGroup::GetSelVector(Transaction &transaction, idx_t vector_idx, SelectionVector &sel_vector, idx_t max_count) {
	lock_guard<mutex> lock(row_group_lock);

	auto info = GetChunkInfo(vector_idx);
	if (!info) {
		return max_count;
	}
	return info->GetSelVector(transaction, sel_vector, max_count);
}

idx_t RowGroup::GetCommittedSelVector(transaction_t start_time, transaction_t transaction_id, idx_t vector_idx,
                                      SelectionVector &sel_vector, idx_t max_count) {
	lock_guard<mutex> lock(row_group_lock);

	auto info = GetChunkInfo(vector_idx);
	if (!info) {
		return max_count;
	}
	return info->GetCommittedSelVector(start_time, transaction_id, sel_vector, max_count);
}

bool RowGroup::Fetch(Transaction &transaction, idx_t row) {
	D_ASSERT(row < this->count);
	lock_guard<mutex> lock(row_group_lock);

	idx_t vector_index = row / STANDARD_VECTOR_SIZE;
	auto info = GetChunkInfo(vector_index);
	if (!info) {
		return true;
	}
	return info->Fetch(transaction, row - vector_index * STANDARD_VECTOR_SIZE);
}

void RowGroup::FetchRow(Transaction &transaction, ColumnFetchState &state, const vector<column_t> &column_ids,
                        row_t row_id, DataChunk &result, idx_t result_idx) {
	for (idx_t col_idx = 0; col_idx < column_ids.size(); col_idx++) {
		auto column = column_ids[col_idx];
		if (column == COLUMN_IDENTIFIER_ROW_ID) {
			// row id column: fill in the row ids
			D_ASSERT(result.data[col_idx].GetType().InternalType() == PhysicalType::INT64);
			result.data[col_idx].SetVectorType(VectorType::FLAT_VECTOR);
			auto data = FlatVector::GetData<row_t>(result.data[col_idx]);
			data[result_idx] = row_id;
		} else {
			// regular column: fetch data from the base column
			columns[column]->FetchRow(transaction, state, row_id, result.data[col_idx], result_idx);
		}
	}
}

void RowGroup::AppendVersionInfo(Transaction &transaction, idx_t row_group_start, idx_t count,
                                 transaction_t commit_id) {
	idx_t row_group_end = row_group_start + count;
	lock_guard<mutex> lock(row_group_lock);

	this->count += count;
	D_ASSERT(this->count <= RowGroup::ROW_GROUP_SIZE);

	// create the version_info if it doesn't exist yet
	if (!version_info) {
		version_info = make_unique<VersionNode>();
	}
	idx_t start_vector_idx = row_group_start / STANDARD_VECTOR_SIZE;
	idx_t end_vector_idx = (row_group_end - 1) / STANDARD_VECTOR_SIZE;
	for (idx_t vector_idx = start_vector_idx; vector_idx <= end_vector_idx; vector_idx++) {
		idx_t start = vector_idx == start_vector_idx ? row_group_start - start_vector_idx * STANDARD_VECTOR_SIZE : 0;
		idx_t end =
		    vector_idx == end_vector_idx ? row_group_end - end_vector_idx * STANDARD_VECTOR_SIZE : STANDARD_VECTOR_SIZE;
		if (start == 0 && end == STANDARD_VECTOR_SIZE) {
			// entire vector is encapsulated by append: append a single constant
			auto constant_info = make_unique<ChunkConstantInfo>(this->start + vector_idx * STANDARD_VECTOR_SIZE);
			constant_info->insert_id = commit_id;
			constant_info->delete_id = NOT_DELETED_ID;
			version_info->info[vector_idx] = move(constant_info);
		} else {
			// part of a vector is encapsulated: append to that part
			ChunkVectorInfo *info;
			if (!version_info->info[vector_idx]) {
				// first time appending to this vector: create new info
				auto insert_info = make_unique<ChunkVectorInfo>(this->start + vector_idx * STANDARD_VECTOR_SIZE);
				info = insert_info.get();
				version_info->info[vector_idx] = move(insert_info);
			} else {
				D_ASSERT(version_info->info[vector_idx]->type == ChunkInfoType::VECTOR_INFO);
				// use existing vector
				info = (ChunkVectorInfo *)version_info->info[vector_idx].get();
			}
			info->Append(start, end, commit_id);
		}
	}
}

void RowGroup::CommitAppend(transaction_t commit_id, idx_t row_group_start, idx_t count) {
	D_ASSERT(version_info.get());
	idx_t row_group_end = row_group_start + count;
	lock_guard<mutex> lock(row_group_lock);

	idx_t start_vector_idx = row_group_start / STANDARD_VECTOR_SIZE;
	idx_t end_vector_idx = (row_group_end - 1) / STANDARD_VECTOR_SIZE;
	for (idx_t vector_idx = start_vector_idx; vector_idx <= end_vector_idx; vector_idx++) {
		idx_t start = vector_idx == start_vector_idx ? row_group_start - start_vector_idx * STANDARD_VECTOR_SIZE : 0;
		idx_t end =
		    vector_idx == end_vector_idx ? row_group_end - end_vector_idx * STANDARD_VECTOR_SIZE : STANDARD_VECTOR_SIZE;

		auto info = version_info->info[vector_idx].get();
		info->CommitAppend(commit_id, start, end);
	}
}

void RowGroup::RevertAppend(idx_t row_group_start) {
	if (!version_info) {
		return;
	}
	idx_t start_row = row_group_start - this->start;
	idx_t start_vector_idx = (start_row + (STANDARD_VECTOR_SIZE - 1)) / STANDARD_VECTOR_SIZE;
	for (idx_t vector_idx = start_vector_idx; vector_idx < RowGroup::ROW_GROUP_VECTOR_COUNT; vector_idx++) {
		version_info->info[vector_idx].reset();
	}
	for (auto &column : columns) {
		column->RevertAppend(row_group_start);
	}
	this->count = MinValue<idx_t>(row_group_start - this->start, this->count);
	Verify();
}

void RowGroup::InitializeAppend(Transaction &transaction, RowGroupAppendState &append_state,
                                idx_t remaining_append_count) {
	append_state.row_group = this;
	append_state.offset_in_row_group = this->count;
	// for each column, initialize the append state
	append_state.states = unique_ptr<ColumnAppendState[]>(new ColumnAppendState[columns.size()]);
	for (idx_t i = 0; i < columns.size(); i++) {
		columns[i]->InitializeAppend(append_state.states[i]);
	}
	// append the version info for this row_group
	idx_t append_count = MinValue<idx_t>(remaining_append_count, RowGroup::ROW_GROUP_SIZE - this->count);
	AppendVersionInfo(transaction, this->count, append_count, transaction.transaction_id);
}

void RowGroup::Append(RowGroupAppendState &state, DataChunk &chunk, idx_t append_count) {
	// append to the current row_group
	for (idx_t i = 0; i < columns.size(); i++) {
		columns[i]->Append(*stats[i]->statistics, state.states[i], chunk.data[i], append_count);
	}
	state.offset_in_row_group += append_count;
}

void RowGroup::Update(Transaction &transaction, DataChunk &update_chunk, row_t *ids, idx_t offset, idx_t count,
                      const vector<column_t> &column_ids) {
#ifdef DEBUG
	for (size_t i = offset; i < offset + count; i++) {
		D_ASSERT(ids[i] >= row_t(this->start) && ids[i] < row_t(this->start + this->count));
	}
#endif
	for (idx_t i = 0; i < column_ids.size(); i++) {
		auto column = column_ids[i];
		D_ASSERT(column != COLUMN_IDENTIFIER_ROW_ID);
		D_ASSERT(columns[column]->type.id() == update_chunk.data[i].GetType().id());
		if (offset > 0) {
			Vector sliced_vector(update_chunk.data[i], offset);
			sliced_vector.Normalify(count);
			columns[column]->Update(transaction, column, sliced_vector, ids + offset, count);
		} else {
			columns[column]->Update(transaction, column, update_chunk.data[i], ids, count);
		}
		MergeStatistics(column, *columns[column]->GetUpdateStatistics());
	}
}

void RowGroup::UpdateColumn(Transaction &transaction, DataChunk &updates, Vector &row_ids,
                            const vector<column_t> &column_path) {
	D_ASSERT(updates.ColumnCount() == 1);
	auto ids = FlatVector::GetData<row_t>(row_ids);

	auto primary_column_idx = column_path[0];
	D_ASSERT(primary_column_idx != COLUMN_IDENTIFIER_ROW_ID);
	D_ASSERT(primary_column_idx < columns.size());
	columns[primary_column_idx]->UpdateColumn(transaction, column_path, updates.data[0], ids, updates.size(), 1);
	MergeStatistics(primary_column_idx, *columns[primary_column_idx]->GetUpdateStatistics());
}

unique_ptr<BaseStatistics> RowGroup::GetStatistics(idx_t column_idx) {
	D_ASSERT(column_idx < stats.size());

	lock_guard<mutex> slock(stats_lock);
	return stats[column_idx]->statistics->Copy();
}

void RowGroup::MergeStatistics(idx_t column_idx, BaseStatistics &other) {
	D_ASSERT(column_idx < stats.size());

	lock_guard<mutex> slock(stats_lock);
	stats[column_idx]->statistics->Merge(other);
}

RowGroupPointer RowGroup::Checkpoint(TableDataWriter &writer, vector<unique_ptr<BaseStatistics>> &global_stats) {
	vector<unique_ptr<ColumnCheckpointState>> states;
	states.reserve(columns.size());

	// checkpoint the individual columns of the row group
	for (idx_t column_idx = 0; column_idx < columns.size(); column_idx++) {
		auto &column = columns[column_idx];
		ColumnCheckpointInfo checkpoint_info {writer.GetColumnCompressionType(column_idx)};
		auto checkpoint_state = column->Checkpoint(*this, writer, checkpoint_info);
		D_ASSERT(checkpoint_state);

		auto stats = checkpoint_state->GetStatistics();
		D_ASSERT(stats);

		global_stats[column_idx]->Merge(*stats);
		states.push_back(move(checkpoint_state));
	}

	// construct the row group pointer and write the column meta data to disk
	D_ASSERT(states.size() == columns.size());
	RowGroupPointer row_group_pointer;
	row_group_pointer.row_start = start;
	row_group_pointer.tuple_count = count;
	for (auto &state : states) {
		// get the current position of the meta data writer
		auto &meta_writer = writer.GetMetaWriter();
		auto pointer = meta_writer.GetBlockPointer();

		// store the stats and the data pointers in the row group pointers
		row_group_pointer.data_pointers.push_back(pointer);
		row_group_pointer.statistics.push_back(state->GetStatistics());

		// now flush the actual column data to disk
		state->FlushToDisk();
	}
	row_group_pointer.versions = version_info;
	Verify();
	return row_group_pointer;
}

void RowGroup::CheckpointDeletes(VersionNode *versions, Serializer &serializer) {
	if (!versions) {
		// no version information: write nothing
		serializer.Write<idx_t>(0);
		return;
	}
	// first count how many ChunkInfo's we need to deserialize
	idx_t chunk_info_count = 0;
	for (idx_t vector_idx = 0; vector_idx < RowGroup::ROW_GROUP_VECTOR_COUNT; vector_idx++) {
		auto chunk_info = versions->info[vector_idx].get();
		if (!chunk_info) {
			continue;
		}
		chunk_info_count++;
	}
	// now serialize the actual version information
	serializer.Write<idx_t>(chunk_info_count);
	for (idx_t vector_idx = 0; vector_idx < RowGroup::ROW_GROUP_VECTOR_COUNT; vector_idx++) {
		auto chunk_info = versions->info[vector_idx].get();
		if (!chunk_info) {
			continue;
		}
		serializer.Write<idx_t>(vector_idx);
		chunk_info->Serialize(serializer);
	}
}

shared_ptr<VersionNode> RowGroup::DeserializeDeletes(Deserializer &source) {
	auto chunk_count = source.Read<idx_t>();
	if (chunk_count == 0) {
		// no deletes
		return nullptr;
	}
	auto version_info = make_shared<VersionNode>();
	for (idx_t i = 0; i < chunk_count; i++) {
		idx_t vector_index = source.Read<idx_t>();
		if (vector_index >= RowGroup::ROW_GROUP_VECTOR_COUNT) {
			throw Exception("In DeserializeDeletes, vector_index is out of range for the row group. Corrupted file?");
		}
		version_info->info[vector_index] = ChunkInfo::Deserialize(source);
	}
	return version_info;
}

void RowGroup::Serialize(RowGroupPointer &pointer, Serializer &main_serializer) {
	FieldWriter writer(main_serializer);
	writer.WriteField<uint64_t>(pointer.row_start);
	writer.WriteField<uint64_t>(pointer.tuple_count);
	auto &serializer = writer.GetSerializer();
	for (auto &stats : pointer.statistics) {
		stats->Serialize(serializer);
	}
	for (auto &data_pointer : pointer.data_pointers) {
		serializer.Write<block_id_t>(data_pointer.block_id);
		serializer.Write<uint64_t>(data_pointer.offset);
	}
	CheckpointDeletes(pointer.versions.get(), serializer);
	writer.Finalize();
}

RowGroupPointer RowGroup::Deserialize(Deserializer &main_source, const vector<ColumnDefinition> &columns) {
	RowGroupPointer result;

	FieldReader reader(main_source);
	result.row_start = reader.ReadRequired<uint64_t>();
	result.tuple_count = reader.ReadRequired<uint64_t>();

	result.data_pointers.reserve(columns.size());
	result.statistics.reserve(columns.size());

	auto &source = reader.GetSource();
	for (idx_t i = 0; i < columns.size(); i++) {
		auto &col = columns[i];
		if (col.Generated()) {
			continue;
		}
		auto stats = BaseStatistics::Deserialize(source, columns[i].Type());
		result.statistics.push_back(move(stats));
	}
	for (idx_t i = 0; i < columns.size(); i++) {
		auto &col = columns[i];
		if (col.Generated()) {
			continue;
		}
		BlockPointer pointer;
		pointer.block_id = source.Read<block_id_t>();
		pointer.offset = source.Read<uint64_t>();
		result.data_pointers.push_back(pointer);
	}
	result.versions = DeserializeDeletes(source);

	reader.Finalize();
	return result;
}

//===--------------------------------------------------------------------===//
// GetStorageInfo
//===--------------------------------------------------------------------===//
void RowGroup::GetStorageInfo(idx_t row_group_index, vector<vector<Value>> &result) {
	for (idx_t col_idx = 0; col_idx < columns.size(); col_idx++) {
		columns[col_idx]->GetStorageInfo(row_group_index, {col_idx}, result);
	}
}

//===--------------------------------------------------------------------===//
// Version Delete Information
//===--------------------------------------------------------------------===//
class VersionDeleteState {
public:
	VersionDeleteState(RowGroup &info, Transaction &transaction, DataTable *table, idx_t base_row)
	    : info(info), transaction(transaction), table(table), current_info(nullptr),
	      current_chunk(DConstants::INVALID_INDEX), count(0), base_row(base_row), delete_count(0) {
	}

	RowGroup &info;
	Transaction &transaction;
	DataTable *table;
	ChunkVectorInfo *current_info;
	idx_t current_chunk;
	row_t rows[STANDARD_VECTOR_SIZE];
	idx_t count;
	idx_t base_row;
	idx_t chunk_row;
	idx_t delete_count;

public:
	void Delete(row_t row_id);
	void Flush();
};

idx_t RowGroup::Delete(Transaction &transaction, DataTable *table, row_t *ids, idx_t count) {
	lock_guard<mutex> lock(row_group_lock);
	VersionDeleteState del_state(*this, transaction, table, this->start);

	// obtain a write lock
	for (idx_t i = 0; i < count; i++) {
		D_ASSERT(ids[i] >= 0);
		D_ASSERT(idx_t(ids[i]) >= this->start && idx_t(ids[i]) < this->start + this->count);
		del_state.Delete(ids[i] - this->start);
	}
	del_state.Flush();
	return del_state.delete_count;
}

void RowGroup::Verify() {
#ifdef DEBUG
	for (auto &column : columns) {
		column->Verify(*this);
	}
#endif
}

void VersionDeleteState::Delete(row_t row_id) {
	D_ASSERT(row_id >= 0);
	idx_t vector_idx = row_id / STANDARD_VECTOR_SIZE;
	idx_t idx_in_vector = row_id - vector_idx * STANDARD_VECTOR_SIZE;
	if (current_chunk != vector_idx) {
		Flush();

		if (!info.version_info) {
			info.version_info = make_unique<VersionNode>();
		}

		if (!info.version_info->info[vector_idx]) {
			// no info yet: create it
			info.version_info->info[vector_idx] =
			    make_unique<ChunkVectorInfo>(info.start + vector_idx * STANDARD_VECTOR_SIZE);
		} else if (info.version_info->info[vector_idx]->type == ChunkInfoType::CONSTANT_INFO) {
			auto &constant = (ChunkConstantInfo &)*info.version_info->info[vector_idx];
			// info exists but it's a constant info: convert to a vector info
			auto new_info = make_unique<ChunkVectorInfo>(info.start + vector_idx * STANDARD_VECTOR_SIZE);
			new_info->insert_id = constant.insert_id.load();
			for (idx_t i = 0; i < STANDARD_VECTOR_SIZE; i++) {
				new_info->inserted[i] = constant.insert_id.load();
			}
			info.version_info->info[vector_idx] = move(new_info);
		}
		D_ASSERT(info.version_info->info[vector_idx]->type == ChunkInfoType::VECTOR_INFO);
		current_info = (ChunkVectorInfo *)info.version_info->info[vector_idx].get();
		current_chunk = vector_idx;
		chunk_row = vector_idx * STANDARD_VECTOR_SIZE;
	}
	rows[count++] = idx_in_vector;
}

void VersionDeleteState::Flush() {
	if (count == 0) {
		return;
	}
	// delete in the current info
	delete_count += current_info->Delete(transaction, rows, count);
	// now push the delete into the undo buffer
	transaction.PushDelete(table, current_info, rows, count, base_row + chunk_row);
	count = 0;
}

} // namespace duckdb



namespace duckdb {

SegmentBase *SegmentTree::GetRootSegment() {
	return root_node.get();
}

SegmentBase *SegmentTree::GetLastSegment() {
	return nodes.empty() ? nullptr : nodes.back().node;
}

SegmentBase *SegmentTree::GetSegment(idx_t row_number) {
	lock_guard<mutex> tree_lock(node_lock);
	return nodes[GetSegmentIndex(row_number)].node;
}

idx_t SegmentTree::GetSegmentIndex(idx_t row_number) {
	D_ASSERT(!nodes.empty());
	D_ASSERT(row_number >= nodes[0].row_start);
	D_ASSERT(row_number < nodes.back().row_start + nodes.back().node->count);
	idx_t lower = 0;
	idx_t upper = nodes.size() - 1;
	// binary search to find the node
	while (lower <= upper) {
		idx_t index = (lower + upper) / 2;
		D_ASSERT(index < nodes.size());
		auto &entry = nodes[index];
		D_ASSERT(entry.row_start == entry.node->start);
		if (row_number < entry.row_start) {
			upper = index - 1;
		} else if (row_number >= entry.row_start + entry.node->count) {
			lower = index + 1;
		} else {
			return index;
		}
	}
	throw InternalException("Could not find node in column segment tree!");
}

void SegmentTree::AppendSegment(unique_ptr<SegmentBase> segment) {
	D_ASSERT(segment);
	// add the node to the list of nodes
	SegmentNode node;
	node.row_start = segment->start;
	node.node = segment.get();
	nodes.push_back(node);

	if (nodes.size() > 1) {
		// add the node as the next pointer of the last node
		D_ASSERT(!nodes[nodes.size() - 2].node->next);
		nodes[nodes.size() - 2].node->next = move(segment);
	} else {
		root_node = move(segment);
	}
}

void SegmentTree::Replace(SegmentTree &other) {
	root_node = move(other.root_node);
	nodes = move(other.nodes);
}

} // namespace duckdb







namespace duckdb {

StandardColumnData::StandardColumnData(DataTableInfo &info, idx_t column_index, idx_t start_row, LogicalType type,
                                       ColumnData *parent)
    : ColumnData(info, column_index, start_row, move(type), parent), validity(info, 0, start_row, this) {
}

bool StandardColumnData::CheckZonemap(ColumnScanState &state, TableFilter &filter) {
	if (!state.segment_checked) {
		if (!state.current) {
			return true;
		}
		state.segment_checked = true;
		auto prune_result = filter.CheckStatistics(*state.current->stats.statistics);
		if (prune_result != FilterPropagateResult::FILTER_ALWAYS_FALSE) {
			return true;
		}
		if (updates) {
			auto update_stats = updates->GetStatistics();
			prune_result = filter.CheckStatistics(*update_stats);
			return prune_result != FilterPropagateResult::FILTER_ALWAYS_FALSE;
		} else {
			return false;
		}
	} else {
		return true;
	}
}

void StandardColumnData::InitializeScan(ColumnScanState &state) {
	ColumnData::InitializeScan(state);

	// initialize the validity segment
	ColumnScanState child_state;
	validity.InitializeScan(child_state);
	state.child_states.push_back(move(child_state));
}

void StandardColumnData::InitializeScanWithOffset(ColumnScanState &state, idx_t row_idx) {
	ColumnData::InitializeScanWithOffset(state, row_idx);

	// initialize the validity segment
	ColumnScanState child_state;
	validity.InitializeScanWithOffset(child_state, row_idx);
	state.child_states.push_back(move(child_state));
}

idx_t StandardColumnData::Scan(Transaction &transaction, idx_t vector_index, ColumnScanState &state, Vector &result) {
	D_ASSERT(state.row_index == state.child_states[0].row_index);
	auto scan_count = ColumnData::Scan(transaction, vector_index, state, result);
	validity.Scan(transaction, vector_index, state.child_states[0], result);
	return scan_count;
}

idx_t StandardColumnData::ScanCommitted(idx_t vector_index, ColumnScanState &state, Vector &result,
                                        bool allow_updates) {
	D_ASSERT(state.row_index == state.child_states[0].row_index);
	auto scan_count = ColumnData::ScanCommitted(vector_index, state, result, allow_updates);
	validity.ScanCommitted(vector_index, state.child_states[0], result, allow_updates);
	return scan_count;
}

idx_t StandardColumnData::ScanCount(ColumnScanState &state, Vector &result, idx_t count) {
	auto scan_count = ColumnData::ScanCount(state, result, count);
	validity.ScanCount(state.child_states[0], result, count);
	return scan_count;
}

void StandardColumnData::InitializeAppend(ColumnAppendState &state) {
	ColumnData::InitializeAppend(state);

	ColumnAppendState child_append;
	validity.InitializeAppend(child_append);
	state.child_appends.push_back(move(child_append));
}

void StandardColumnData::AppendData(BaseStatistics &stats, ColumnAppendState &state, VectorData &vdata, idx_t count) {
	ColumnData::AppendData(stats, state, vdata, count);

	validity.AppendData(*stats.validity_stats, state.child_appends[0], vdata, count);
}

void StandardColumnData::RevertAppend(row_t start_row) {
	ColumnData::RevertAppend(start_row);

	validity.RevertAppend(start_row);
}

idx_t StandardColumnData::Fetch(ColumnScanState &state, row_t row_id, Vector &result) {
	// fetch validity mask
	if (state.child_states.empty()) {
		ColumnScanState child_state;
		state.child_states.push_back(move(child_state));
	}
	auto scan_count = ColumnData::Fetch(state, row_id, result);
	validity.Fetch(state.child_states[0], row_id, result);
	return scan_count;
}

void StandardColumnData::Update(Transaction &transaction, idx_t column_index, Vector &update_vector, row_t *row_ids,
                                idx_t update_count) {
	ColumnData::Update(transaction, column_index, update_vector, row_ids, update_count);
	validity.Update(transaction, column_index, update_vector, row_ids, update_count);
}

void StandardColumnData::UpdateColumn(Transaction &transaction, const vector<column_t> &column_path,
                                      Vector &update_vector, row_t *row_ids, idx_t update_count, idx_t depth) {
	if (depth >= column_path.size()) {
		// update this column
		ColumnData::Update(transaction, column_path[0], update_vector, row_ids, update_count);
	} else {
		// update the child column (i.e. the validity column)
		validity.UpdateColumn(transaction, column_path, update_vector, row_ids, update_count, depth + 1);
	}
}

unique_ptr<BaseStatistics> StandardColumnData::GetUpdateStatistics() {
	auto stats = updates ? updates->GetStatistics() : nullptr;
	auto validity_stats = validity.GetUpdateStatistics();
	if (!stats && !validity_stats) {
		return nullptr;
	}
	if (!stats) {
		stats = BaseStatistics::CreateEmpty(type, StatisticsType::GLOBAL_STATS);
	}
	stats->validity_stats = move(validity_stats);
	return stats;
}

void StandardColumnData::FetchRow(Transaction &transaction, ColumnFetchState &state, row_t row_id, Vector &result,
                                  idx_t result_idx) {
	// find the segment the row belongs to
	if (state.child_states.empty()) {
		auto child_state = make_unique<ColumnFetchState>();
		state.child_states.push_back(move(child_state));
	}
	validity.FetchRow(transaction, *state.child_states[0], row_id, result, result_idx);
	ColumnData::FetchRow(transaction, state, row_id, result, result_idx);
}

void StandardColumnData::CommitDropColumn() {
	ColumnData::CommitDropColumn();
	validity.CommitDropColumn();
}

struct StandardColumnCheckpointState : public ColumnCheckpointState {
	StandardColumnCheckpointState(RowGroup &row_group, ColumnData &column_data, TableDataWriter &writer)
	    : ColumnCheckpointState(row_group, column_data, writer) {
	}

	unique_ptr<ColumnCheckpointState> validity_state;

public:
	unique_ptr<BaseStatistics> GetStatistics() override {
		auto stats = global_stats->Copy();
		stats->validity_stats = validity_state->GetStatistics();
		return stats;
	}

	void FlushToDisk() override {
		ColumnCheckpointState::FlushToDisk();
		validity_state->FlushToDisk();
	}
};

unique_ptr<ColumnCheckpointState> StandardColumnData::CreateCheckpointState(RowGroup &row_group,
                                                                            TableDataWriter &writer) {
	return make_unique<StandardColumnCheckpointState>(row_group, *this, writer);
}

unique_ptr<ColumnCheckpointState> StandardColumnData::Checkpoint(RowGroup &row_group, TableDataWriter &writer,
                                                                 ColumnCheckpointInfo &checkpoint_info) {
	auto validity_state = validity.Checkpoint(row_group, writer, checkpoint_info);
	auto base_state = ColumnData::Checkpoint(row_group, writer, checkpoint_info);
	auto &checkpoint_state = (StandardColumnCheckpointState &)*base_state;
	checkpoint_state.validity_state = move(validity_state);
	return base_state;
}

void StandardColumnData::CheckpointScan(ColumnSegment *segment, ColumnScanState &state, idx_t row_group_start,
                                        idx_t count, Vector &scan_vector) {
	ColumnData::CheckpointScan(segment, state, row_group_start, count, scan_vector);

	idx_t offset_in_row_group = state.row_index - row_group_start;
	validity.ScanCommittedRange(row_group_start, offset_in_row_group, count, scan_vector);
}

void StandardColumnData::DeserializeColumn(Deserializer &source) {
	ColumnData::DeserializeColumn(source);
	validity.DeserializeColumn(source);
}

void StandardColumnData::GetStorageInfo(idx_t row_group_index, vector<idx_t> col_path, vector<vector<Value>> &result) {
	ColumnData::GetStorageInfo(row_group_index, col_path, result);
	col_path.push_back(0);
	validity.GetStorageInfo(row_group_index, move(col_path), result);
}

void StandardColumnData::Verify(RowGroup &parent) {
#ifdef DEBUG
	ColumnData::Verify(parent);
	validity.Verify(parent);
#endif
}

} // namespace duckdb



namespace duckdb {

StructColumnData::StructColumnData(DataTableInfo &info, idx_t column_index, idx_t start_row, LogicalType type_p,
                                   ColumnData *parent)
    : ColumnData(info, column_index, start_row, move(type_p), parent), validity(info, 0, start_row, this) {
	D_ASSERT(type.InternalType() == PhysicalType::STRUCT);
	auto &child_types = StructType::GetChildTypes(type);
	D_ASSERT(child_types.size() > 0);
	// the sub column index, starting at 1 (0 is the validity mask)
	idx_t sub_column_index = 1;
	for (auto &child_type : child_types) {
		sub_columns.push_back(
		    ColumnData::CreateColumnUnique(info, sub_column_index, start_row, child_type.second, this));
		sub_column_index++;
	}
}

bool StructColumnData::CheckZonemap(ColumnScanState &state, TableFilter &filter) {
	// table filters are not supported yet for struct columns
	return false;
}

idx_t StructColumnData::GetMaxEntry() {
	return sub_columns[0]->GetMaxEntry();
}

void StructColumnData::InitializeScan(ColumnScanState &state) {
	D_ASSERT(state.child_states.empty());

	state.row_index = 0;
	state.current = nullptr;

	// initialize the validity segment
	ColumnScanState validity_state;
	validity.InitializeScan(validity_state);
	state.child_states.push_back(move(validity_state));

	// initialize the sub-columns
	for (auto &sub_column : sub_columns) {
		ColumnScanState child_state;
		sub_column->InitializeScan(child_state);
		state.child_states.push_back(move(child_state));
	}
}

void StructColumnData::InitializeScanWithOffset(ColumnScanState &state, idx_t row_idx) {
	D_ASSERT(state.child_states.empty());

	state.row_index = row_idx;
	state.current = nullptr;

	// initialize the validity segment
	ColumnScanState validity_state;
	validity.InitializeScanWithOffset(validity_state, row_idx);
	state.child_states.push_back(move(validity_state));

	// initialize the sub-columns
	for (auto &sub_column : sub_columns) {
		ColumnScanState child_state;
		sub_column->InitializeScanWithOffset(child_state, row_idx);
		state.child_states.push_back(move(child_state));
	}
}

idx_t StructColumnData::Scan(Transaction &transaction, idx_t vector_index, ColumnScanState &state, Vector &result) {
	auto scan_count = validity.Scan(transaction, vector_index, state.child_states[0], result);
	auto &child_entries = StructVector::GetEntries(result);
	for (idx_t i = 0; i < sub_columns.size(); i++) {
		sub_columns[i]->Scan(transaction, vector_index, state.child_states[i + 1], *child_entries[i]);
	}
	return scan_count;
}

idx_t StructColumnData::ScanCommitted(idx_t vector_index, ColumnScanState &state, Vector &result, bool allow_updates) {
	auto scan_count = validity.ScanCommitted(vector_index, state.child_states[0], result, allow_updates);
	auto &child_entries = StructVector::GetEntries(result);
	for (idx_t i = 0; i < sub_columns.size(); i++) {
		sub_columns[i]->ScanCommitted(vector_index, state.child_states[i + 1], *child_entries[i], allow_updates);
	}
	return scan_count;
}

idx_t StructColumnData::ScanCount(ColumnScanState &state, Vector &result, idx_t count) {
	auto scan_count = validity.ScanCount(state.child_states[0], result, count);
	auto &child_entries = StructVector::GetEntries(result);
	for (idx_t i = 0; i < sub_columns.size(); i++) {
		sub_columns[i]->ScanCount(state.child_states[i + 1], *child_entries[i], count);
	}
	return scan_count;
}

void StructColumnData::InitializeAppend(ColumnAppendState &state) {
	ColumnAppendState validity_append;
	validity.InitializeAppend(validity_append);
	state.child_appends.push_back(move(validity_append));

	for (auto &sub_column : sub_columns) {
		ColumnAppendState child_append;
		sub_column->InitializeAppend(child_append);
		state.child_appends.push_back(move(child_append));
	}
}

void StructColumnData::Append(BaseStatistics &stats, ColumnAppendState &state, Vector &vector, idx_t count) {
	vector.Normalify(count);

	// append the null values
	validity.Append(*stats.validity_stats, state.child_appends[0], vector, count);

	auto &struct_validity = FlatVector::Validity(vector);

	auto &struct_stats = (StructStatistics &)stats;
	auto &child_entries = StructVector::GetEntries(vector);
	for (idx_t i = 0; i < child_entries.size(); i++) {
		if (!struct_validity.AllValid()) {
			// we set the child entries of the struct to NULL
			// for any values in which the struct itself is NULL
			child_entries[i]->Normalify(count);

			auto &child_validity = FlatVector::Validity(*child_entries[i]);
			child_validity.Combine(struct_validity, count);
		}
		sub_columns[i]->Append(*struct_stats.child_stats[i], state.child_appends[i + 1], *child_entries[i], count);
	}
}

void StructColumnData::RevertAppend(row_t start_row) {
	validity.RevertAppend(start_row);
	for (auto &sub_column : sub_columns) {
		sub_column->RevertAppend(start_row);
	}
}

idx_t StructColumnData::Fetch(ColumnScanState &state, row_t row_id, Vector &result) {
	// fetch validity mask
	auto &child_entries = StructVector::GetEntries(result);
	// insert any child states that are required
	for (idx_t i = state.child_states.size(); i < child_entries.size() + 1; i++) {
		ColumnScanState child_state;
		state.child_states.push_back(move(child_state));
	}
	// fetch the validity state
	idx_t scan_count = validity.Fetch(state.child_states[0], row_id, result);
	// fetch the sub-column states
	for (idx_t i = 0; i < child_entries.size(); i++) {
		sub_columns[i]->Fetch(state.child_states[i + 1], row_id, *child_entries[i]);
	}
	return scan_count;
}

void StructColumnData::Update(Transaction &transaction, idx_t column_index, Vector &update_vector, row_t *row_ids,
                              idx_t update_count) {
	validity.Update(transaction, column_index, update_vector, row_ids, update_count);
	auto &child_entries = StructVector::GetEntries(update_vector);
	for (idx_t i = 0; i < child_entries.size(); i++) {
		sub_columns[i]->Update(transaction, column_index, *child_entries[i], row_ids, update_count);
	}
}

void StructColumnData::UpdateColumn(Transaction &transaction, const vector<column_t> &column_path,
                                    Vector &update_vector, row_t *row_ids, idx_t update_count, idx_t depth) {
	// we can never DIRECTLY update a struct column
	if (depth >= column_path.size()) {
		throw InternalException("Attempting to directly update a struct column - this should not be possible");
	}
	auto update_column = column_path[depth];
	if (update_column == 0) {
		// update the validity column
		validity.UpdateColumn(transaction, column_path, update_vector, row_ids, update_count, depth + 1);
	} else {
		if (update_column > sub_columns.size()) {
			throw InternalException("Update column_path out of range");
		}
		sub_columns[update_column - 1]->UpdateColumn(transaction, column_path, update_vector, row_ids, update_count,
		                                             depth + 1);
	}
}

unique_ptr<BaseStatistics> StructColumnData::GetUpdateStatistics() {
	// check if any child column has updates
	auto stats = BaseStatistics::CreateEmpty(type, StatisticsType::GLOBAL_STATS);
	auto &struct_stats = (StructStatistics &)*stats;
	stats->validity_stats = validity.GetUpdateStatistics();
	for (idx_t i = 0; i < sub_columns.size(); i++) {
		auto child_stats = sub_columns[i]->GetUpdateStatistics();
		if (child_stats) {
			struct_stats.child_stats[i] = move(child_stats);
		}
	}
	return stats;
}

void StructColumnData::FetchRow(Transaction &transaction, ColumnFetchState &state, row_t row_id, Vector &result,
                                idx_t result_idx) {
	// fetch validity mask
	auto &child_entries = StructVector::GetEntries(result);
	// insert any child states that are required
	for (idx_t i = state.child_states.size(); i < child_entries.size() + 1; i++) {
		auto child_state = make_unique<ColumnFetchState>();
		state.child_states.push_back(move(child_state));
	}
	// fetch the validity state
	validity.FetchRow(transaction, *state.child_states[0], row_id, result, result_idx);
	// fetch the sub-column states
	for (idx_t i = 0; i < child_entries.size(); i++) {
		sub_columns[i]->FetchRow(transaction, *state.child_states[i + 1], row_id, *child_entries[i], result_idx);
	}
}

void StructColumnData::CommitDropColumn() {
	validity.CommitDropColumn();
	for (auto &sub_column : sub_columns) {
		sub_column->CommitDropColumn();
	}
}

struct StructColumnCheckpointState : public ColumnCheckpointState {
	StructColumnCheckpointState(RowGroup &row_group, ColumnData &column_data, TableDataWriter &writer)
	    : ColumnCheckpointState(row_group, column_data, writer) {
		global_stats = make_unique<StructStatistics>(column_data.type);
	}

	unique_ptr<ColumnCheckpointState> validity_state;
	vector<unique_ptr<ColumnCheckpointState>> child_states;

public:
	unique_ptr<BaseStatistics> GetStatistics() override {
		auto stats = make_unique<StructStatistics>(column_data.type);
		D_ASSERT(stats->child_stats.size() == child_states.size());
		stats->validity_stats = validity_state->GetStatistics();
		for (idx_t i = 0; i < child_states.size(); i++) {
			stats->child_stats[i] = child_states[i]->GetStatistics();
			D_ASSERT(stats->child_stats[i]);
		}
		return move(stats);
	}

	void FlushToDisk() override {
		validity_state->FlushToDisk();
		for (auto &state : child_states) {
			state->FlushToDisk();
		}
	}
};

unique_ptr<ColumnCheckpointState> StructColumnData::CreateCheckpointState(RowGroup &row_group,
                                                                          TableDataWriter &writer) {
	return make_unique<StructColumnCheckpointState>(row_group, *this, writer);
}

unique_ptr<ColumnCheckpointState> StructColumnData::Checkpoint(RowGroup &row_group, TableDataWriter &writer,
                                                               ColumnCheckpointInfo &checkpoint_info) {
	auto checkpoint_state = make_unique<StructColumnCheckpointState>(row_group, *this, writer);
	checkpoint_state->validity_state = validity.Checkpoint(row_group, writer, checkpoint_info);
	for (auto &sub_column : sub_columns) {
		checkpoint_state->child_states.push_back(sub_column->Checkpoint(row_group, writer, checkpoint_info));
	}
	return move(checkpoint_state);
}

void StructColumnData::DeserializeColumn(Deserializer &source) {
	validity.DeserializeColumn(source);
	for (auto &sub_column : sub_columns) {
		sub_column->DeserializeColumn(source);
	}
}

void StructColumnData::GetStorageInfo(idx_t row_group_index, vector<idx_t> col_path, vector<vector<Value>> &result) {
	col_path.push_back(0);
	validity.GetStorageInfo(row_group_index, col_path, result);
	for (idx_t i = 0; i < sub_columns.size(); i++) {
		col_path.back() = i + 1;
		sub_columns[i]->GetStorageInfo(row_group_index, col_path, result);
	}
}

void StructColumnData::Verify(RowGroup &parent) {
#ifdef DEBUG
	ColumnData::Verify(parent);
	validity.Verify(parent);
	for (auto &sub_column : sub_columns) {
		sub_column->Verify(parent);
	}
#endif
}

} // namespace duckdb










namespace duckdb {

static UpdateSegment::initialize_update_function_t GetInitializeUpdateFunction(PhysicalType type);
static UpdateSegment::fetch_update_function_t GetFetchUpdateFunction(PhysicalType type);
static UpdateSegment::fetch_committed_function_t GetFetchCommittedFunction(PhysicalType type);
static UpdateSegment::fetch_committed_range_function_t GetFetchCommittedRangeFunction(PhysicalType type);

static UpdateSegment::merge_update_function_t GetMergeUpdateFunction(PhysicalType type);
static UpdateSegment::rollback_update_function_t GetRollbackUpdateFunction(PhysicalType type);
static UpdateSegment::statistics_update_function_t GetStatisticsUpdateFunction(PhysicalType type);
static UpdateSegment::fetch_row_function_t GetFetchRowFunction(PhysicalType type);

UpdateSegment::UpdateSegment(ColumnData &column_data) : column_data(column_data), stats(column_data.type) {
	auto physical_type = column_data.type.InternalType();

	this->type_size = GetTypeIdSize(physical_type);

	this->initialize_update_function = GetInitializeUpdateFunction(physical_type);
	this->fetch_update_function = GetFetchUpdateFunction(physical_type);
	this->fetch_committed_function = GetFetchCommittedFunction(physical_type);
	this->fetch_committed_range = GetFetchCommittedRangeFunction(physical_type);
	this->fetch_row_function = GetFetchRowFunction(physical_type);
	this->merge_update_function = GetMergeUpdateFunction(physical_type);
	this->rollback_update_function = GetRollbackUpdateFunction(physical_type);
	this->statistics_update_function = GetStatisticsUpdateFunction(physical_type);
}

UpdateSegment::~UpdateSegment() {
}

void UpdateSegment::ClearUpdates() {
	stats.Reset();
	root.reset();
	heap.Destroy();
}

//===--------------------------------------------------------------------===//
// Update Info Helpers
//===--------------------------------------------------------------------===//
Value UpdateInfo::GetValue(idx_t index) {
	auto &type = segment->column_data.type;

	switch (type.id()) {
	case LogicalTypeId::VALIDITY:
		return Value::BOOLEAN(((bool *)tuple_data)[index]);
	case LogicalTypeId::INTEGER:
		return Value::INTEGER(((int32_t *)tuple_data)[index]);
	default:
		throw NotImplementedException("Unimplemented type for UpdateInfo::GetValue");
	}
}

void UpdateInfo::Print() {
	Printer::Print(ToString());
}

string UpdateInfo::ToString() {
	auto &type = segment->column_data.type;
	string result = "Update Info [" + type.ToString() + ", Count: " + to_string(N) +
	                ", Transaction Id: " + to_string(version_number) + "]\n";
	for (idx_t i = 0; i < N; i++) {
		result += to_string(tuples[i]) + ": " + GetValue(i).ToString() + "\n";
	}
	if (next) {
		result += "\nChild Segment: " + next->ToString();
	}
	return result;
}

void UpdateInfo::Verify() {
#ifdef DEBUG
	for (idx_t i = 1; i < N; i++) {
		D_ASSERT(tuples[i] > tuples[i - 1] && tuples[i] < STANDARD_VECTOR_SIZE);
	}
#endif
}

//===--------------------------------------------------------------------===//
// Update Fetch
//===--------------------------------------------------------------------===//
static void MergeValidityInfo(UpdateInfo *current, ValidityMask &result_mask) {
	auto info_data = (bool *)current->tuple_data;
	for (idx_t i = 0; i < current->N; i++) {
		result_mask.Set(current->tuples[i], info_data[i]);
	}
}

static void UpdateMergeValidity(transaction_t start_time, transaction_t transaction_id, UpdateInfo *info,
                                Vector &result) {
	auto &result_mask = FlatVector::Validity(result);
	UpdateInfo::UpdatesForTransaction(info, start_time, transaction_id,
	                                  [&](UpdateInfo *current) { MergeValidityInfo(current, result_mask); });
}

template <class T>
static void MergeUpdateInfo(UpdateInfo *current, T *result_data) {
	auto info_data = (T *)current->tuple_data;
	if (current->N == STANDARD_VECTOR_SIZE) {
		// special case: update touches ALL tuples of this vector
		// in this case we can just memcpy the data
		// since the layout of the update info is guaranteed to be [0, 1, 2, 3, ...]
		memcpy(result_data, info_data, sizeof(T) * current->N);
	} else {
		for (idx_t i = 0; i < current->N; i++) {
			result_data[current->tuples[i]] = info_data[i];
		}
	}
}

template <class T>
static void UpdateMergeFetch(transaction_t start_time, transaction_t transaction_id, UpdateInfo *info, Vector &result) {
	auto result_data = FlatVector::GetData<T>(result);
	UpdateInfo::UpdatesForTransaction(info, start_time, transaction_id,
	                                  [&](UpdateInfo *current) { MergeUpdateInfo<T>(current, result_data); });
}

static UpdateSegment::fetch_update_function_t GetFetchUpdateFunction(PhysicalType type) {
	switch (type) {
	case PhysicalType::BIT:
		return UpdateMergeValidity;
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		return UpdateMergeFetch<int8_t>;
	case PhysicalType::INT16:
		return UpdateMergeFetch<int16_t>;
	case PhysicalType::INT32:
		return UpdateMergeFetch<int32_t>;
	case PhysicalType::INT64:
		return UpdateMergeFetch<int64_t>;
	case PhysicalType::UINT8:
		return UpdateMergeFetch<uint8_t>;
	case PhysicalType::UINT16:
		return UpdateMergeFetch<uint16_t>;
	case PhysicalType::UINT32:
		return UpdateMergeFetch<uint32_t>;
	case PhysicalType::UINT64:
		return UpdateMergeFetch<uint64_t>;
	case PhysicalType::INT128:
		return UpdateMergeFetch<hugeint_t>;
	case PhysicalType::FLOAT:
		return UpdateMergeFetch<float>;
	case PhysicalType::DOUBLE:
		return UpdateMergeFetch<double>;
	case PhysicalType::INTERVAL:
		return UpdateMergeFetch<interval_t>;
	case PhysicalType::VARCHAR:
		return UpdateMergeFetch<string_t>;
	default:
		throw NotImplementedException("Unimplemented type for update segment");
	}
}

void UpdateSegment::FetchUpdates(Transaction &transaction, idx_t vector_index, Vector &result) {
	auto lock_handle = lock.GetSharedLock();
	if (!root) {
		return;
	}
	if (!root->info[vector_index]) {
		return;
	}
	// FIXME: normalify if this is not the case... need to pass in count?
	D_ASSERT(result.GetVectorType() == VectorType::FLAT_VECTOR);

	fetch_update_function(transaction.start_time, transaction.transaction_id, root->info[vector_index]->info.get(),
	                      result);
}

//===--------------------------------------------------------------------===//
// Fetch Committed
//===--------------------------------------------------------------------===//
static void FetchCommittedValidity(UpdateInfo *info, Vector &result) {
	auto &result_mask = FlatVector::Validity(result);
	MergeValidityInfo(info, result_mask);
}

template <class T>
static void TemplatedFetchCommitted(UpdateInfo *info, Vector &result) {
	auto result_data = FlatVector::GetData<T>(result);
	MergeUpdateInfo<T>(info, result_data);
}

static UpdateSegment::fetch_committed_function_t GetFetchCommittedFunction(PhysicalType type) {
	switch (type) {
	case PhysicalType::BIT:
		return FetchCommittedValidity;
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		return TemplatedFetchCommitted<int8_t>;
	case PhysicalType::INT16:
		return TemplatedFetchCommitted<int16_t>;
	case PhysicalType::INT32:
		return TemplatedFetchCommitted<int32_t>;
	case PhysicalType::INT64:
		return TemplatedFetchCommitted<int64_t>;
	case PhysicalType::UINT8:
		return TemplatedFetchCommitted<uint8_t>;
	case PhysicalType::UINT16:
		return TemplatedFetchCommitted<uint16_t>;
	case PhysicalType::UINT32:
		return TemplatedFetchCommitted<uint32_t>;
	case PhysicalType::UINT64:
		return TemplatedFetchCommitted<uint64_t>;
	case PhysicalType::INT128:
		return TemplatedFetchCommitted<hugeint_t>;
	case PhysicalType::FLOAT:
		return TemplatedFetchCommitted<float>;
	case PhysicalType::DOUBLE:
		return TemplatedFetchCommitted<double>;
	case PhysicalType::INTERVAL:
		return TemplatedFetchCommitted<interval_t>;
	case PhysicalType::VARCHAR:
		return TemplatedFetchCommitted<string_t>;
	default:
		throw NotImplementedException("Unimplemented type for update segment");
	}
}

void UpdateSegment::FetchCommitted(idx_t vector_index, Vector &result) {
	auto lock_handle = lock.GetSharedLock();

	if (!root) {
		return;
	}
	if (!root->info[vector_index]) {
		return;
	}
	// FIXME: normalify if this is not the case... need to pass in count?
	D_ASSERT(result.GetVectorType() == VectorType::FLAT_VECTOR);

	fetch_committed_function(root->info[vector_index]->info.get(), result);
}

//===--------------------------------------------------------------------===//
// Fetch Range
//===--------------------------------------------------------------------===//
static void MergeUpdateInfoRangeValidity(UpdateInfo *current, idx_t start, idx_t end, idx_t result_offset,
                                         ValidityMask &result_mask) {
	auto info_data = (bool *)current->tuple_data;
	for (idx_t i = 0; i < current->N; i++) {
		auto tuple_idx = current->tuples[i];
		if (tuple_idx < start) {
			continue;
		} else if (tuple_idx >= end) {
			break;
		}
		auto result_idx = result_offset + tuple_idx - start;
		result_mask.Set(result_idx, info_data[i]);
	}
}

static void FetchCommittedRangeValidity(UpdateInfo *info, idx_t start, idx_t end, idx_t result_offset, Vector &result) {
	auto &result_mask = FlatVector::Validity(result);
	MergeUpdateInfoRangeValidity(info, start, end, result_offset, result_mask);
}

template <class T>
static void MergeUpdateInfoRange(UpdateInfo *current, idx_t start, idx_t end, idx_t result_offset, T *result_data) {
	auto info_data = (T *)current->tuple_data;
	for (idx_t i = 0; i < current->N; i++) {
		auto tuple_idx = current->tuples[i];
		if (tuple_idx < start) {
			continue;
		} else if (tuple_idx >= end) {
			break;
		}
		auto result_idx = result_offset + tuple_idx - start;
		result_data[result_idx] = info_data[i];
	}
}

template <class T>
static void TemplatedFetchCommittedRange(UpdateInfo *info, idx_t start, idx_t end, idx_t result_offset,
                                         Vector &result) {
	auto result_data = FlatVector::GetData<T>(result);
	MergeUpdateInfoRange<T>(info, start, end, result_offset, result_data);
}

static UpdateSegment::fetch_committed_range_function_t GetFetchCommittedRangeFunction(PhysicalType type) {
	switch (type) {
	case PhysicalType::BIT:
		return FetchCommittedRangeValidity;
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		return TemplatedFetchCommittedRange<int8_t>;
	case PhysicalType::INT16:
		return TemplatedFetchCommittedRange<int16_t>;
	case PhysicalType::INT32:
		return TemplatedFetchCommittedRange<int32_t>;
	case PhysicalType::INT64:
		return TemplatedFetchCommittedRange<int64_t>;
	case PhysicalType::UINT8:
		return TemplatedFetchCommittedRange<uint8_t>;
	case PhysicalType::UINT16:
		return TemplatedFetchCommittedRange<uint16_t>;
	case PhysicalType::UINT32:
		return TemplatedFetchCommittedRange<uint32_t>;
	case PhysicalType::UINT64:
		return TemplatedFetchCommittedRange<uint64_t>;
	case PhysicalType::INT128:
		return TemplatedFetchCommittedRange<hugeint_t>;
	case PhysicalType::FLOAT:
		return TemplatedFetchCommittedRange<float>;
	case PhysicalType::DOUBLE:
		return TemplatedFetchCommittedRange<double>;
	case PhysicalType::INTERVAL:
		return TemplatedFetchCommittedRange<interval_t>;
	case PhysicalType::VARCHAR:
		return TemplatedFetchCommittedRange<string_t>;
	default:
		throw NotImplementedException("Unimplemented type for update segment");
	}
}

void UpdateSegment::FetchCommittedRange(idx_t start_row, idx_t count, Vector &result) {
	D_ASSERT(count > 0);
	if (!root) {
		return;
	}
	D_ASSERT(result.GetVectorType() == VectorType::FLAT_VECTOR);

	idx_t end_row = start_row + count;
	idx_t start_vector = start_row / STANDARD_VECTOR_SIZE;
	idx_t end_vector = (end_row - 1) / STANDARD_VECTOR_SIZE;
	D_ASSERT(start_vector <= end_vector);
	D_ASSERT(end_vector < RowGroup::ROW_GROUP_VECTOR_COUNT);

	for (idx_t vector_idx = start_vector; vector_idx <= end_vector; vector_idx++) {
		if (!root->info[vector_idx]) {
			continue;
		}
		idx_t start_in_vector = vector_idx == start_vector ? start_row - start_vector * STANDARD_VECTOR_SIZE : 0;
		idx_t end_in_vector =
		    vector_idx == end_vector ? end_row - end_vector * STANDARD_VECTOR_SIZE : STANDARD_VECTOR_SIZE;
		D_ASSERT(start_in_vector < end_in_vector);
		D_ASSERT(end_in_vector > 0 && end_in_vector <= STANDARD_VECTOR_SIZE);
		idx_t result_offset = ((vector_idx * STANDARD_VECTOR_SIZE) + start_in_vector) - start_row;
		fetch_committed_range(root->info[vector_idx]->info.get(), start_in_vector, end_in_vector, result_offset,
		                      result);
	}
}

//===--------------------------------------------------------------------===//
// Fetch Row
//===--------------------------------------------------------------------===//
static void FetchRowValidity(transaction_t start_time, transaction_t transaction_id, UpdateInfo *info, idx_t row_idx,
                             Vector &result, idx_t result_idx) {
	auto &result_mask = FlatVector::Validity(result);
	UpdateInfo::UpdatesForTransaction(info, start_time, transaction_id, [&](UpdateInfo *current) {
		auto info_data = (bool *)current->tuple_data;
		// FIXME: we could do a binary search in here
		for (idx_t i = 0; i < current->N; i++) {
			if (current->tuples[i] == row_idx) {
				result_mask.Set(result_idx, info_data[i]);
				break;
			} else if (current->tuples[i] > row_idx) {
				break;
			}
		}
	});
}

template <class T>
static void TemplatedFetchRow(transaction_t start_time, transaction_t transaction_id, UpdateInfo *info, idx_t row_idx,
                              Vector &result, idx_t result_idx) {
	auto result_data = FlatVector::GetData<T>(result);
	UpdateInfo::UpdatesForTransaction(info, start_time, transaction_id, [&](UpdateInfo *current) {
		auto info_data = (T *)current->tuple_data;
		// FIXME: we could do a binary search in here
		for (idx_t i = 0; i < current->N; i++) {
			if (current->tuples[i] == row_idx) {
				result_data[result_idx] = info_data[i];
				break;
			} else if (current->tuples[i] > row_idx) {
				break;
			}
		}
	});
}

static UpdateSegment::fetch_row_function_t GetFetchRowFunction(PhysicalType type) {
	switch (type) {
	case PhysicalType::BIT:
		return FetchRowValidity;
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		return TemplatedFetchRow<int8_t>;
	case PhysicalType::INT16:
		return TemplatedFetchRow<int16_t>;
	case PhysicalType::INT32:
		return TemplatedFetchRow<int32_t>;
	case PhysicalType::INT64:
		return TemplatedFetchRow<int64_t>;
	case PhysicalType::UINT8:
		return TemplatedFetchRow<uint8_t>;
	case PhysicalType::UINT16:
		return TemplatedFetchRow<uint16_t>;
	case PhysicalType::UINT32:
		return TemplatedFetchRow<uint32_t>;
	case PhysicalType::UINT64:
		return TemplatedFetchRow<uint64_t>;
	case PhysicalType::INT128:
		return TemplatedFetchRow<hugeint_t>;
	case PhysicalType::FLOAT:
		return TemplatedFetchRow<float>;
	case PhysicalType::DOUBLE:
		return TemplatedFetchRow<double>;
	case PhysicalType::INTERVAL:
		return TemplatedFetchRow<interval_t>;
	case PhysicalType::VARCHAR:
		return TemplatedFetchRow<string_t>;
	default:
		throw NotImplementedException("Unimplemented type for update segment fetch row");
	}
}

void UpdateSegment::FetchRow(Transaction &transaction, idx_t row_id, Vector &result, idx_t result_idx) {
	if (!root) {
		return;
	}
	idx_t vector_index = (row_id - column_data.start) / STANDARD_VECTOR_SIZE;
	if (!root->info[vector_index]) {
		return;
	}
	idx_t row_in_vector = row_id - vector_index * STANDARD_VECTOR_SIZE;
	fetch_row_function(transaction.start_time, transaction.transaction_id, root->info[vector_index]->info.get(),
	                   row_in_vector, result, result_idx);
}

//===--------------------------------------------------------------------===//
// Rollback update
//===--------------------------------------------------------------------===//
template <class T>
static void RollbackUpdate(UpdateInfo *base_info, UpdateInfo *rollback_info) {
	auto base_data = (T *)base_info->tuple_data;
	auto rollback_data = (T *)rollback_info->tuple_data;
	idx_t base_offset = 0;
	for (idx_t i = 0; i < rollback_info->N; i++) {
		auto id = rollback_info->tuples[i];
		while (base_info->tuples[base_offset] < id) {
			base_offset++;
			D_ASSERT(base_offset < base_info->N);
		}
		base_data[base_offset] = rollback_data[i];
	}
}

static UpdateSegment::rollback_update_function_t GetRollbackUpdateFunction(PhysicalType type) {
	switch (type) {
	case PhysicalType::BIT:
		return RollbackUpdate<bool>;
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		return RollbackUpdate<int8_t>;
	case PhysicalType::INT16:
		return RollbackUpdate<int16_t>;
	case PhysicalType::INT32:
		return RollbackUpdate<int32_t>;
	case PhysicalType::INT64:
		return RollbackUpdate<int64_t>;
	case PhysicalType::UINT8:
		return RollbackUpdate<uint8_t>;
	case PhysicalType::UINT16:
		return RollbackUpdate<uint16_t>;
	case PhysicalType::UINT32:
		return RollbackUpdate<uint32_t>;
	case PhysicalType::UINT64:
		return RollbackUpdate<uint64_t>;
	case PhysicalType::INT128:
		return RollbackUpdate<hugeint_t>;
	case PhysicalType::FLOAT:
		return RollbackUpdate<float>;
	case PhysicalType::DOUBLE:
		return RollbackUpdate<double>;
	case PhysicalType::INTERVAL:
		return RollbackUpdate<interval_t>;
	case PhysicalType::VARCHAR:
		return RollbackUpdate<string_t>;
	default:
		throw NotImplementedException("Unimplemented type for uncompressed segment");
	}
}

void UpdateSegment::RollbackUpdate(UpdateInfo *info) {
	// obtain an exclusive lock
	auto lock_handle = lock.GetExclusiveLock();

	// move the data from the UpdateInfo back into the base info
	D_ASSERT(root->info[info->vector_index]);
	rollback_update_function(root->info[info->vector_index]->info.get(), info);

	// clean up the update chain
	CleanupUpdateInternal(*lock_handle, info);
}

//===--------------------------------------------------------------------===//
// Cleanup Update
//===--------------------------------------------------------------------===//
void UpdateSegment::CleanupUpdateInternal(const StorageLockKey &lock, UpdateInfo *info) {
	D_ASSERT(info->prev);
	auto prev = info->prev;
	prev->next = info->next;
	if (prev->next) {
		prev->next->prev = prev;
	}
}

void UpdateSegment::CleanupUpdate(UpdateInfo *info) {
	// obtain an exclusive lock
	auto lock_handle = lock.GetExclusiveLock();
	CleanupUpdateInternal(*lock_handle, info);
}

//===--------------------------------------------------------------------===//
// Check for conflicts in update
//===--------------------------------------------------------------------===//
static void CheckForConflicts(UpdateInfo *info, Transaction &transaction, row_t *ids, const SelectionVector &sel,
                              idx_t count, row_t offset, UpdateInfo *&node) {
	if (!info) {
		return;
	}
	if (info->version_number == transaction.transaction_id) {
		// this UpdateInfo belongs to the current transaction, set it in the node
		node = info;
	} else if (info->version_number > transaction.start_time) {
		// potential conflict, check that tuple ids do not conflict
		// as both ids and info->tuples are sorted, this is similar to a merge join
		idx_t i = 0, j = 0;
		while (true) {
			auto id = ids[sel.get_index(i)] - offset;
			if (id == info->tuples[j]) {
				throw TransactionException("Conflict on update!");
			} else if (id < info->tuples[j]) {
				// id < the current tuple in info, move to next id
				i++;
				if (i == count) {
					break;
				}
			} else {
				// id > the current tuple, move to next tuple in info
				j++;
				if (j == info->N) {
					break;
				}
			}
		}
	}
	CheckForConflicts(info->next, transaction, ids, sel, count, offset, node);
}

//===--------------------------------------------------------------------===//
// Initialize update info
//===--------------------------------------------------------------------===//
void UpdateSegment::InitializeUpdateInfo(UpdateInfo &info, row_t *ids, const SelectionVector &sel, idx_t count,
                                         idx_t vector_index, idx_t vector_offset) {
	info.segment = this;
	info.vector_index = vector_index;
	info.prev = nullptr;
	info.next = nullptr;

	// set up the tuple ids
	info.N = count;
	for (idx_t i = 0; i < count; i++) {
		auto idx = sel.get_index(i);
		auto id = ids[idx];
		D_ASSERT(idx_t(id) >= vector_offset && idx_t(id) < vector_offset + STANDARD_VECTOR_SIZE);
		info.tuples[i] = id - vector_offset;
	};
}

static void InitializeUpdateValidity(UpdateInfo *base_info, Vector &base_data, UpdateInfo *update_info, Vector &update,
                                     const SelectionVector &sel) {
	auto &update_mask = FlatVector::Validity(update);
	auto tuple_data = (bool *)update_info->tuple_data;

	if (!update_mask.AllValid()) {
		for (idx_t i = 0; i < update_info->N; i++) {
			auto idx = sel.get_index(i);
			tuple_data[i] = update_mask.RowIsValidUnsafe(idx);
		}
	} else {
		for (idx_t i = 0; i < update_info->N; i++) {
			tuple_data[i] = true;
		}
	}

	auto &base_mask = FlatVector::Validity(base_data);
	auto base_tuple_data = (bool *)base_info->tuple_data;
	if (!base_mask.AllValid()) {
		for (idx_t i = 0; i < base_info->N; i++) {
			base_tuple_data[i] = base_mask.RowIsValidUnsafe(base_info->tuples[i]);
		}
	} else {
		for (idx_t i = 0; i < base_info->N; i++) {
			base_tuple_data[i] = true;
		}
	}
}

struct UpdateSelectElement {
	template <class T>
	static T Operation(UpdateSegment *segment, T element) {
		return element;
	}
};

template <>
string_t UpdateSelectElement::Operation(UpdateSegment *segment, string_t element) {
	return element.IsInlined() ? element : segment->GetStringHeap().AddString(element);
}

template <class T>
static void InitializeUpdateData(UpdateInfo *base_info, Vector &base_data, UpdateInfo *update_info, Vector &update,
                                 const SelectionVector &sel) {
	auto update_data = FlatVector::GetData<T>(update);
	auto tuple_data = (T *)update_info->tuple_data;

	for (idx_t i = 0; i < update_info->N; i++) {
		auto idx = sel.get_index(i);
		tuple_data[i] = update_data[idx];
	}

	auto base_array_data = FlatVector::GetData<T>(base_data);
	auto base_tuple_data = (T *)base_info->tuple_data;
	for (idx_t i = 0; i < base_info->N; i++) {
		base_tuple_data[i] =
		    UpdateSelectElement::Operation<T>(base_info->segment, base_array_data[base_info->tuples[i]]);
	}
}

static UpdateSegment::initialize_update_function_t GetInitializeUpdateFunction(PhysicalType type) {
	switch (type) {
	case PhysicalType::BIT:
		return InitializeUpdateValidity;
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		return InitializeUpdateData<int8_t>;
	case PhysicalType::INT16:
		return InitializeUpdateData<int16_t>;
	case PhysicalType::INT32:
		return InitializeUpdateData<int32_t>;
	case PhysicalType::INT64:
		return InitializeUpdateData<int64_t>;
	case PhysicalType::UINT8:
		return InitializeUpdateData<uint8_t>;
	case PhysicalType::UINT16:
		return InitializeUpdateData<uint16_t>;
	case PhysicalType::UINT32:
		return InitializeUpdateData<uint32_t>;
	case PhysicalType::UINT64:
		return InitializeUpdateData<uint64_t>;
	case PhysicalType::INT128:
		return InitializeUpdateData<hugeint_t>;
	case PhysicalType::FLOAT:
		return InitializeUpdateData<float>;
	case PhysicalType::DOUBLE:
		return InitializeUpdateData<double>;
	case PhysicalType::INTERVAL:
		return InitializeUpdateData<interval_t>;
	case PhysicalType::VARCHAR:
		return InitializeUpdateData<string_t>;
	default:
		throw NotImplementedException("Unimplemented type for update segment");
	}
}

//===--------------------------------------------------------------------===//
// Merge update info
//===--------------------------------------------------------------------===//
template <class F1, class F2, class F3>
static idx_t MergeLoop(row_t a[], sel_t b[], idx_t acount, idx_t bcount, idx_t aoffset, F1 merge, F2 pick_a, F3 pick_b,
                       const SelectionVector &asel) {
	idx_t aidx = 0, bidx = 0;
	idx_t count = 0;
	while (aidx < acount && bidx < bcount) {
		auto a_index = asel.get_index(aidx);
		auto a_id = a[a_index] - aoffset;
		auto b_id = b[bidx];
		if (a_id == b_id) {
			merge(a_id, a_index, bidx, count);
			aidx++;
			bidx++;
			count++;
		} else if (a_id < b_id) {
			pick_a(a_id, a_index, count);
			aidx++;
			count++;
		} else {
			pick_b(b_id, bidx, count);
			bidx++;
			count++;
		}
	}
	for (; aidx < acount; aidx++) {
		auto a_index = asel.get_index(aidx);
		pick_a(a[a_index] - aoffset, a_index, count);
		count++;
	}
	for (; bidx < bcount; bidx++) {
		pick_b(b[bidx], bidx, count);
		count++;
	}
	return count;
}

struct ExtractStandardEntry {
	template <class T, class V>
	static T Extract(V *data, idx_t entry) {
		return data[entry];
	}
};

struct ExtractValidityEntry {
	template <class T, class V>
	static T Extract(V *data, idx_t entry) {
		return data->RowIsValid(entry);
	}
};

template <class T, class V, class OP = ExtractStandardEntry>
static void MergeUpdateLoopInternal(UpdateInfo *base_info, V *base_table_data, UpdateInfo *update_info,
                                    V *update_vector_data, row_t *ids, idx_t count, const SelectionVector &sel) {
	auto base_id = base_info->segment->column_data.start + base_info->vector_index * STANDARD_VECTOR_SIZE;
#ifdef DEBUG
	// all of these should be sorted, otherwise the below algorithm does not work
	for (idx_t i = 1; i < count; i++) {
		auto prev_idx = sel.get_index(i - 1);
		auto idx = sel.get_index(i);
		D_ASSERT(ids[idx] > ids[prev_idx] && ids[idx] >= row_t(base_id) &&
		         ids[idx] < row_t(base_id + STANDARD_VECTOR_SIZE));
	}
#endif

	// we have a new batch of updates (update, ids, count)
	// we already have existing updates (base_info)
	// and potentially, this transaction already has updates present (update_info)
	// we need to merge these all together so that the latest updates get merged into base_info
	// and the "old" values (fetched from EITHER base_info OR from base_data) get placed into update_info
	auto base_info_data = (T *)base_info->tuple_data;
	auto update_info_data = (T *)update_info->tuple_data;

	// we first do the merging of the old values
	// what we are trying to do here is update the "update_info" of this transaction with all the old data we require
	// this means we need to merge (1) any previously updated values (stored in update_info->tuples)
	// together with (2)
	// to simplify this, we create new arrays here
	// we memcpy these over afterwards
	T result_values[STANDARD_VECTOR_SIZE];
	sel_t result_ids[STANDARD_VECTOR_SIZE];

	idx_t base_info_offset = 0;
	idx_t update_info_offset = 0;
	idx_t result_offset = 0;
	for (idx_t i = 0; i < count; i++) {
		auto idx = sel.get_index(i);
		// we have to merge the info for "ids[i]"
		auto update_id = ids[idx] - base_id;

		while (update_info_offset < update_info->N && update_info->tuples[update_info_offset] < update_id) {
			// old id comes before the current id: write it
			result_values[result_offset] = update_info_data[update_info_offset];
			result_ids[result_offset++] = update_info->tuples[update_info_offset];
			update_info_offset++;
		}
		// write the new id
		if (update_info_offset < update_info->N && update_info->tuples[update_info_offset] == update_id) {
			// we have an id that is equivalent in the current update info: write the update info
			result_values[result_offset] = update_info_data[update_info_offset];
			result_ids[result_offset++] = update_info->tuples[update_info_offset];
			update_info_offset++;
			continue;
		}

		/// now check if we have the current update_id in the base_info, or if we should fetch it from the base data
		while (base_info_offset < base_info->N && base_info->tuples[base_info_offset] < update_id) {
			base_info_offset++;
		}
		if (base_info_offset < base_info->N && base_info->tuples[base_info_offset] == update_id) {
			// it is! we have to move the tuple from base_info->ids[base_info_offset] to update_info
			result_values[result_offset] = base_info_data[base_info_offset];
		} else {
			// it is not! we have to move base_table_data[update_id] to update_info
			result_values[result_offset] = UpdateSelectElement::Operation<T>(
			    base_info->segment, OP::template Extract<T, V>(base_table_data, update_id));
		}
		result_ids[result_offset++] = update_id;
	}
	// write any remaining entries from the old updates
	while (update_info_offset < update_info->N) {
		result_values[result_offset] = update_info_data[update_info_offset];
		result_ids[result_offset++] = update_info->tuples[update_info_offset];
		update_info_offset++;
	}
	// now copy them back
	update_info->N = result_offset;
	memcpy(update_info_data, result_values, result_offset * sizeof(T));
	memcpy(update_info->tuples, result_ids, result_offset * sizeof(sel_t));

	// now we merge the new values into the base_info
	result_offset = 0;
	auto pick_new = [&](idx_t id, idx_t aidx, idx_t count) {
		result_values[result_offset] = OP::template Extract<T, V>(update_vector_data, aidx);
		result_ids[result_offset] = id;
		result_offset++;
	};
	auto pick_old = [&](idx_t id, idx_t bidx, idx_t count) {
		result_values[result_offset] = base_info_data[bidx];
		result_ids[result_offset] = id;
		result_offset++;
	};
	// now we perform a merge of the new ids with the old ids
	auto merge = [&](idx_t id, idx_t aidx, idx_t bidx, idx_t count) {
		pick_new(id, aidx, count);
	};
	MergeLoop(ids, base_info->tuples, count, base_info->N, base_id, merge, pick_new, pick_old, sel);

	base_info->N = result_offset;
	memcpy(base_info_data, result_values, result_offset * sizeof(T));
	memcpy(base_info->tuples, result_ids, result_offset * sizeof(sel_t));
}

static void MergeValidityLoop(UpdateInfo *base_info, Vector &base_data, UpdateInfo *update_info, Vector &update,
                              row_t *ids, idx_t count, const SelectionVector &sel) {
	auto &base_validity = FlatVector::Validity(base_data);
	auto &update_validity = FlatVector::Validity(update);
	MergeUpdateLoopInternal<bool, ValidityMask, ExtractValidityEntry>(base_info, &base_validity, update_info,
	                                                                  &update_validity, ids, count, sel);
}

template <class T>
static void MergeUpdateLoop(UpdateInfo *base_info, Vector &base_data, UpdateInfo *update_info, Vector &update,
                            row_t *ids, idx_t count, const SelectionVector &sel) {
	auto base_table_data = FlatVector::GetData<T>(base_data);
	auto update_vector_data = FlatVector::GetData<T>(update);
	MergeUpdateLoopInternal<T, T>(base_info, base_table_data, update_info, update_vector_data, ids, count, sel);
}

static UpdateSegment::merge_update_function_t GetMergeUpdateFunction(PhysicalType type) {
	switch (type) {
	case PhysicalType::BIT:
		return MergeValidityLoop;
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		return MergeUpdateLoop<int8_t>;
	case PhysicalType::INT16:
		return MergeUpdateLoop<int16_t>;
	case PhysicalType::INT32:
		return MergeUpdateLoop<int32_t>;
	case PhysicalType::INT64:
		return MergeUpdateLoop<int64_t>;
	case PhysicalType::UINT8:
		return MergeUpdateLoop<uint8_t>;
	case PhysicalType::UINT16:
		return MergeUpdateLoop<uint16_t>;
	case PhysicalType::UINT32:
		return MergeUpdateLoop<uint32_t>;
	case PhysicalType::UINT64:
		return MergeUpdateLoop<uint64_t>;
	case PhysicalType::INT128:
		return MergeUpdateLoop<hugeint_t>;
	case PhysicalType::FLOAT:
		return MergeUpdateLoop<float>;
	case PhysicalType::DOUBLE:
		return MergeUpdateLoop<double>;
	case PhysicalType::INTERVAL:
		return MergeUpdateLoop<interval_t>;
	case PhysicalType::VARCHAR:
		return MergeUpdateLoop<string_t>;
	default:
		throw NotImplementedException("Unimplemented type for uncompressed segment");
	}
}

//===--------------------------------------------------------------------===//
// Update statistics
//===--------------------------------------------------------------------===//
unique_ptr<BaseStatistics> UpdateSegment::GetStatistics() {
	lock_guard<mutex> stats_guard(stats_lock);
	return stats.statistics->Copy();
}

idx_t UpdateValidityStatistics(UpdateSegment *segment, SegmentStatistics &stats, Vector &update, idx_t count,
                               SelectionVector &sel) {
	auto &mask = FlatVector::Validity(update);
	auto &validity = (ValidityStatistics &)*stats.statistics;
	if (!mask.AllValid() && !validity.has_null) {
		for (idx_t i = 0; i < count; i++) {
			if (!mask.RowIsValid(i)) {
				validity.has_null = true;
				break;
			}
		}
	}
	sel.Initialize(nullptr);
	return count;
}

template <class T>
idx_t TemplatedUpdateNumericStatistics(UpdateSegment *segment, SegmentStatistics &stats, Vector &update, idx_t count,
                                       SelectionVector &sel) {
	auto update_data = FlatVector::GetData<T>(update);
	auto &mask = FlatVector::Validity(update);

	if (mask.AllValid()) {
		for (idx_t i = 0; i < count; i++) {
			NumericStatistics::Update<T>(stats, update_data[i]);
		}
		sel.Initialize(nullptr);
		return count;
	} else {
		idx_t not_null_count = 0;
		sel.Initialize(STANDARD_VECTOR_SIZE);
		for (idx_t i = 0; i < count; i++) {
			if (mask.RowIsValid(i)) {
				sel.set_index(not_null_count++, i);
				NumericStatistics::Update<T>(stats, update_data[i]);
			}
		}
		return not_null_count;
	}
}

idx_t UpdateStringStatistics(UpdateSegment *segment, SegmentStatistics &stats, Vector &update, idx_t count,
                             SelectionVector &sel) {
	auto update_data = FlatVector::GetData<string_t>(update);
	auto &mask = FlatVector::Validity(update);
	if (mask.AllValid()) {
		for (idx_t i = 0; i < count; i++) {
			((StringStatistics &)*stats.statistics).Update(update_data[i]);
			if (!update_data[i].IsInlined()) {
				update_data[i] = segment->GetStringHeap().AddString(update_data[i]);
			}
		}
		sel.Initialize(nullptr);
		return count;
	} else {
		idx_t not_null_count = 0;
		sel.Initialize(STANDARD_VECTOR_SIZE);
		for (idx_t i = 0; i < count; i++) {
			if (mask.RowIsValid(i)) {
				sel.set_index(not_null_count++, i);
				((StringStatistics &)*stats.statistics).Update(update_data[i]);
				if (!update_data[i].IsInlined()) {
					update_data[i] = segment->GetStringHeap().AddString(update_data[i]);
				}
			}
		}
		return not_null_count;
	}
}

UpdateSegment::statistics_update_function_t GetStatisticsUpdateFunction(PhysicalType type) {
	switch (type) {
	case PhysicalType::BIT:
		return UpdateValidityStatistics;
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		return TemplatedUpdateNumericStatistics<int8_t>;
	case PhysicalType::INT16:
		return TemplatedUpdateNumericStatistics<int16_t>;
	case PhysicalType::INT32:
		return TemplatedUpdateNumericStatistics<int32_t>;
	case PhysicalType::INT64:
		return TemplatedUpdateNumericStatistics<int64_t>;
	case PhysicalType::UINT8:
		return TemplatedUpdateNumericStatistics<uint8_t>;
	case PhysicalType::UINT16:
		return TemplatedUpdateNumericStatistics<uint16_t>;
	case PhysicalType::UINT32:
		return TemplatedUpdateNumericStatistics<uint32_t>;
	case PhysicalType::UINT64:
		return TemplatedUpdateNumericStatistics<uint64_t>;
	case PhysicalType::INT128:
		return TemplatedUpdateNumericStatistics<hugeint_t>;
	case PhysicalType::FLOAT:
		return TemplatedUpdateNumericStatistics<float>;
	case PhysicalType::DOUBLE:
		return TemplatedUpdateNumericStatistics<double>;
	case PhysicalType::INTERVAL:
		return TemplatedUpdateNumericStatistics<interval_t>;
	case PhysicalType::VARCHAR:
		return UpdateStringStatistics;
	default:
		throw NotImplementedException("Unimplemented type for uncompressed segment");
	}
}

//===--------------------------------------------------------------------===//
// Update
//===--------------------------------------------------------------------===//
static idx_t SortSelectionVector(SelectionVector &sel, idx_t count, row_t *ids) {
	D_ASSERT(count > 0);

	bool is_sorted = true;
	for (idx_t i = 1; i < count; i++) {
		auto prev_idx = sel.get_index(i - 1);
		auto idx = sel.get_index(i);
		if (ids[idx] <= ids[prev_idx]) {
			is_sorted = false;
			break;
		}
	}
	if (is_sorted) {
		// already sorted: bailout
		return count;
	}
	// not sorted: need to sort the selection vector
	SelectionVector sorted_sel(count);
	for (idx_t i = 0; i < count; i++) {
		sorted_sel.set_index(i, sel.get_index(i));
	}
	std::sort(sorted_sel.data(), sorted_sel.data() + count, [&](sel_t l, sel_t r) { return ids[l] < ids[r]; });
	// eliminate any duplicates
	idx_t pos = 1;
	for (idx_t i = 1; i < count; i++) {
		auto prev_idx = sorted_sel.get_index(i - 1);
		auto idx = sorted_sel.get_index(i);
		D_ASSERT(ids[idx] >= ids[prev_idx]);
		if (ids[prev_idx] != ids[idx]) {
			sorted_sel.set_index(pos++, idx);
		}
	}
#ifdef DEBUG
	for (idx_t i = 1; i < pos; i++) {
		auto prev_idx = sorted_sel.get_index(i - 1);
		auto idx = sorted_sel.get_index(i);
		D_ASSERT(ids[idx] > ids[prev_idx]);
	}
#endif

	sel.Initialize(sorted_sel);
	D_ASSERT(pos > 0);
	return pos;
}

void UpdateSegment::Update(Transaction &transaction, idx_t column_index, Vector &update, row_t *ids, idx_t count,
                           Vector &base_data) {
	// obtain an exclusive lock
	auto write_lock = lock.GetExclusiveLock();

	update.Normalify(count);

	// update statistics
	SelectionVector sel;
	{
		lock_guard<mutex> stats_guard(stats_lock);
		count = statistics_update_function(this, stats, update, count, sel);
	}
	if (count == 0) {
		return;
	}

	// subsequent algorithms used by the update require row ids to be (1) sorted, and (2) unique
	// this is usually the case for "standard" queries (e.g. UPDATE tbl SET x=bla WHERE cond)
	// however, for more exotic queries involving e.g. cross products/joins this might not be the case
	// hence we explicitly check here if the ids are sorted and, if not, sort + duplicate eliminate them
	count = SortSelectionVector(sel, count, ids);
	D_ASSERT(count > 0);

	// create the versions for this segment, if there are none yet
	if (!root) {
		root = make_unique<UpdateNode>();
	}

	// get the vector index based on the first id
	// we assert that all updates must be part of the same vector
	auto first_id = ids[sel.get_index(0)];
	idx_t vector_index = (first_id - column_data.start) / STANDARD_VECTOR_SIZE;
	idx_t vector_offset = column_data.start + vector_index * STANDARD_VECTOR_SIZE;

	D_ASSERT(idx_t(first_id) >= column_data.start);
	D_ASSERT(vector_index < RowGroup::ROW_GROUP_VECTOR_COUNT);

	// first check the version chain
	UpdateInfo *node = nullptr;

	if (root->info[vector_index]) {
		// there is already a version here, check if there are any conflicts and search for the node that belongs to
		// this transaction in the version chain
		auto base_info = root->info[vector_index]->info.get();
		CheckForConflicts(base_info->next, transaction, ids, sel, count, vector_offset, node);

		// there are no conflicts
		// first, check if this thread has already done any updates
		auto node = base_info->next;
		while (node) {
			if (node->version_number == transaction.transaction_id) {
				// it has! use this node
				break;
			}
			node = node->next;
		}
		if (!node) {
			// no updates made yet by this transaction: initially the update info to empty
			node = transaction.CreateUpdateInfo(type_size, count);
			node->segment = this;
			node->vector_index = vector_index;
			node->N = 0;
			node->column_index = column_index;

			// insert the new node into the chain
			node->next = base_info->next;
			if (node->next) {
				node->next->prev = node;
			}
			node->prev = base_info;
			base_info->next = node;
		}
		base_info->Verify();
		node->Verify();

		// now we are going to perform the merge
		merge_update_function(base_info, base_data, node, update, ids, count, sel);

		base_info->Verify();
		node->Verify();
	} else {
		// there is no version info yet: create the top level update info and fill it with the updates
		auto result = make_unique<UpdateNodeData>();

		result->info = make_unique<UpdateInfo>();
		result->tuples = unique_ptr<sel_t[]>(new sel_t[STANDARD_VECTOR_SIZE]);
		result->tuple_data = unique_ptr<data_t[]>(new data_t[STANDARD_VECTOR_SIZE * type_size]);
		result->info->tuples = result->tuples.get();
		result->info->tuple_data = result->tuple_data.get();
		result->info->version_number = TRANSACTION_ID_START - 1;
		result->info->column_index = column_index;
		InitializeUpdateInfo(*result->info, ids, sel, count, vector_index, vector_offset);

		// now create the transaction level update info in the undo log
		auto transaction_node = transaction.CreateUpdateInfo(type_size, count);
		InitializeUpdateInfo(*transaction_node, ids, sel, count, vector_index, vector_offset);

		// we write the updates in the
		initialize_update_function(transaction_node, base_data, result->info.get(), update, sel);

		result->info->next = transaction_node;
		result->info->prev = nullptr;
		transaction_node->next = nullptr;
		transaction_node->prev = result->info.get();
		transaction_node->column_index = column_index;

		transaction_node->Verify();
		result->info->Verify();

		root->info[vector_index] = move(result);
	}
}

bool UpdateSegment::HasUpdates() const {
	return root.get() != nullptr;
}

bool UpdateSegment::HasUpdates(idx_t vector_index) const {
	if (!HasUpdates()) {
		return false;
	}
	return root->info[vector_index].get();
}

bool UpdateSegment::HasUncommittedUpdates(idx_t vector_index) {
	if (!HasUpdates(vector_index)) {
		return false;
	}
	auto read_lock = lock.GetSharedLock();
	auto entry = root->info[vector_index].get();
	if (entry->info->next) {
		return true;
	}
	return false;
}

bool UpdateSegment::HasUpdates(idx_t start_row_index, idx_t end_row_index) {
	if (!HasUpdates()) {
		return false;
	}
	auto read_lock = lock.GetSharedLock();
	idx_t base_vector_index = start_row_index / STANDARD_VECTOR_SIZE;
	idx_t end_vector_index = end_row_index / STANDARD_VECTOR_SIZE;
	for (idx_t i = base_vector_index; i <= end_vector_index; i++) {
		if (root->info[i]) {
			return true;
		}
	}
	return false;
}

} // namespace duckdb




namespace duckdb {

ValidityColumnData::ValidityColumnData(DataTableInfo &info, idx_t column_index, idx_t start_row, ColumnData *parent)
    : ColumnData(info, column_index, start_row, LogicalType(LogicalTypeId::VALIDITY), parent) {
}

bool ValidityColumnData::CheckZonemap(ColumnScanState &state, TableFilter &filter) {
	return true;
}

} // namespace duckdb



















namespace duckdb {

class ReplayState {
public:
	ReplayState(DatabaseInstance &db, ClientContext &context, Deserializer &source)
	    : db(db), context(context), source(source), current_table(nullptr), deserialize_only(false),
	      checkpoint_id(INVALID_BLOCK) {
	}

	DatabaseInstance &db;
	ClientContext &context;
	Deserializer &source;
	TableCatalogEntry *current_table;
	bool deserialize_only;
	block_id_t checkpoint_id;

public:
	void ReplayEntry(WALType entry_type);

private:
	void ReplayCreateTable();
	void ReplayDropTable();
	void ReplayAlter();

	void ReplayCreateView();
	void ReplayDropView();

	void ReplayCreateSchema();
	void ReplayDropSchema();

	void ReplayCreateType();
	void ReplayDropType();

	void ReplayCreateSequence();
	void ReplayDropSequence();
	void ReplaySequenceValue();

	void ReplayCreateMacro();
	void ReplayDropMacro();

	void ReplayCreateTableMacro();
	void ReplayDropTableMacro();

	void ReplayUseTable();
	void ReplayInsert();
	void ReplayDelete();
	void ReplayUpdate();
	void ReplayCheckpoint();
};

bool WriteAheadLog::Replay(DatabaseInstance &database, string &path) {
	auto initial_reader = make_unique<BufferedFileReader>(database.GetFileSystem(), path.c_str());
	if (initial_reader->Finished()) {
		// WAL is empty
		return false;
	}
	Connection con(database);
	con.BeginTransaction();

	// first deserialize the WAL to look for a checkpoint flag
	// if there is a checkpoint flag, we might have already flushed the contents of the WAL to disk
	ReplayState checkpoint_state(database, *con.context, *initial_reader);
	checkpoint_state.deserialize_only = true;
	try {
		while (true) {
			// read the current entry
			WALType entry_type = initial_reader->Read<WALType>();
			if (entry_type == WALType::WAL_FLUSH) {
				// check if the file is exhausted
				if (initial_reader->Finished()) {
					// we finished reading the file: break
					break;
				}
			} else {
				// replay the entry
				checkpoint_state.ReplayEntry(entry_type);
			}
		}
	} catch (std::exception &ex) { // LCOV_EXCL_START
		Printer::Print(StringUtil::Format("Exception in WAL playback during initial read: %s\n", ex.what()));
		return false;
	} catch (...) {
		Printer::Print("Unknown Exception in WAL playback during initial read");
		return false;
	} // LCOV_EXCL_STOP
	initial_reader.reset();
	if (checkpoint_state.checkpoint_id != INVALID_BLOCK) {
		// there is a checkpoint flag: check if we need to deserialize the WAL
		auto &manager = BlockManager::GetBlockManager(database);
		if (manager.IsRootBlock(checkpoint_state.checkpoint_id)) {
			// the contents of the WAL have already been checkpointed
			// we can safely truncate the WAL and ignore its contents
			return true;
		}
	}

	// we need to recover from the WAL: actually set up the replay state
	BufferedFileReader reader(database.GetFileSystem(), path.c_str());
	ReplayState state(database, *con.context, reader);

	// replay the WAL
	// note that everything is wrapped inside a try/catch block here
	// there can be errors in WAL replay because of a corrupt WAL file
	// in this case we should throw a warning but startup anyway
	try {
		while (true) {
			// read the current entry
			WALType entry_type = reader.Read<WALType>();
			if (entry_type == WALType::WAL_FLUSH) {
				// flush: commit the current transaction
				con.Commit();
				// check if the file is exhausted
				if (reader.Finished()) {
					// we finished reading the file: break
					break;
				}
				// otherwise we keep on reading
				con.BeginTransaction();
			} else {
				// replay the entry
				state.ReplayEntry(entry_type);
			}
		}
	} catch (std::exception &ex) { // LCOV_EXCL_START
		// FIXME: this should report a proper warning in the connection
		Printer::Print(StringUtil::Format("Exception in WAL playback: %s\n", ex.what()));
		// exception thrown in WAL replay: rollback
		con.Rollback();
	} catch (...) {
		Printer::Print("Unknown Exception in WAL playback: %s\n");
		// exception thrown in WAL replay: rollback
		con.Rollback();
	} // LCOV_EXCL_STOP
	return false;
}

//===--------------------------------------------------------------------===//
// Replay Entries
//===--------------------------------------------------------------------===//
void ReplayState::ReplayEntry(WALType entry_type) {
	switch (entry_type) {
	case WALType::CREATE_TABLE:
		ReplayCreateTable();
		break;
	case WALType::DROP_TABLE:
		ReplayDropTable();
		break;
	case WALType::ALTER_INFO:
		ReplayAlter();
		break;
	case WALType::CREATE_VIEW:
		ReplayCreateView();
		break;
	case WALType::DROP_VIEW:
		ReplayDropView();
		break;
	case WALType::CREATE_SCHEMA:
		ReplayCreateSchema();
		break;
	case WALType::DROP_SCHEMA:
		ReplayDropSchema();
		break;
	case WALType::CREATE_SEQUENCE:
		ReplayCreateSequence();
		break;
	case WALType::DROP_SEQUENCE:
		ReplayDropSequence();
		break;
	case WALType::SEQUENCE_VALUE:
		ReplaySequenceValue();
		break;
	case WALType::CREATE_MACRO:
		ReplayCreateMacro();
		break;
	case WALType::DROP_MACRO:
		ReplayDropMacro();
		break;
	case WALType::CREATE_TABLE_MACRO:
		ReplayCreateTableMacro();
		break;
	case WALType::DROP_TABLE_MACRO:
		ReplayDropTableMacro();
		break;
	case WALType::USE_TABLE:
		ReplayUseTable();
		break;
	case WALType::INSERT_TUPLE:
		ReplayInsert();
		break;
	case WALType::DELETE_TUPLE:
		ReplayDelete();
		break;
	case WALType::UPDATE_TUPLE:
		ReplayUpdate();
		break;
	case WALType::CHECKPOINT:
		ReplayCheckpoint();
		break;
	case WALType::CREATE_TYPE:
		ReplayCreateType();
		break;
	case WALType::DROP_TYPE:
		ReplayDropType();
		break;

	default:
		throw InternalException("Invalid WAL entry type!");
	}
}

//===--------------------------------------------------------------------===//
// Replay Table
//===--------------------------------------------------------------------===//
void ReplayState::ReplayCreateTable() {
	auto info = TableCatalogEntry::Deserialize(source);
	if (deserialize_only) {
		return;
	}

	// bind the constraints to the table again
	auto binder = Binder::CreateBinder(context);
	auto bound_info = binder->BindCreateTableInfo(move(info));

	auto &catalog = Catalog::GetCatalog(context);
	catalog.CreateTable(context, bound_info.get());
}

void ReplayState::ReplayDropTable() {
	DropInfo info;

	info.type = CatalogType::TABLE_ENTRY;
	info.schema = source.Read<string>();
	info.name = source.Read<string>();
	if (deserialize_only) {
		return;
	}

	auto &catalog = Catalog::GetCatalog(context);
	catalog.DropEntry(context, &info);
}

void ReplayState::ReplayAlter() {
	auto info = AlterInfo::Deserialize(source);
	if (deserialize_only) {
		return;
	}
	auto &catalog = Catalog::GetCatalog(context);
	catalog.Alter(context, info.get());
}

//===--------------------------------------------------------------------===//
// Replay View
//===--------------------------------------------------------------------===//
void ReplayState::ReplayCreateView() {
	auto entry = ViewCatalogEntry::Deserialize(source);
	if (deserialize_only) {
		return;
	}

	auto &catalog = Catalog::GetCatalog(context);
	catalog.CreateView(context, entry.get());
}

void ReplayState::ReplayDropView() {
	DropInfo info;
	info.type = CatalogType::VIEW_ENTRY;
	info.schema = source.Read<string>();
	info.name = source.Read<string>();
	if (deserialize_only) {
		return;
	}
	auto &catalog = Catalog::GetCatalog(context);
	catalog.DropEntry(context, &info);
}

//===--------------------------------------------------------------------===//
// Replay Schema
//===--------------------------------------------------------------------===//
void ReplayState::ReplayCreateSchema() {
	CreateSchemaInfo info;
	info.schema = source.Read<string>();
	if (deserialize_only) {
		return;
	}

	auto &catalog = Catalog::GetCatalog(context);
	catalog.CreateSchema(context, &info);
}

void ReplayState::ReplayDropSchema() {
	DropInfo info;

	info.type = CatalogType::SCHEMA_ENTRY;
	info.name = source.Read<string>();
	if (deserialize_only) {
		return;
	}

	auto &catalog = Catalog::GetCatalog(context);
	catalog.DropEntry(context, &info);
}

//===--------------------------------------------------------------------===//
// Replay Custom Type
//===--------------------------------------------------------------------===//
void ReplayState::ReplayCreateType() {
	auto info = TypeCatalogEntry::Deserialize(source);
	if (deserialize_only) {
		return;
	}

	auto &catalog = Catalog::GetCatalog(context);
	catalog.CreateType(context, info.get());
}

void ReplayState::ReplayDropType() {
	DropInfo info;

	info.type = CatalogType::TYPE_ENTRY;
	info.schema = source.Read<string>();
	info.name = source.Read<string>();
	if (deserialize_only) {
		return;
	}

	auto &catalog = Catalog::GetCatalog(context);
	catalog.DropEntry(context, &info);
}

//===--------------------------------------------------------------------===//
// Replay Sequence
//===--------------------------------------------------------------------===//
void ReplayState::ReplayCreateSequence() {
	auto entry = SequenceCatalogEntry::Deserialize(source);
	if (deserialize_only) {
		return;
	}

	auto &catalog = Catalog::GetCatalog(context);
	catalog.CreateSequence(context, entry.get());
}

void ReplayState::ReplayDropSequence() {
	DropInfo info;
	info.type = CatalogType::SEQUENCE_ENTRY;
	info.schema = source.Read<string>();
	info.name = source.Read<string>();
	if (deserialize_only) {
		return;
	}

	auto &catalog = Catalog::GetCatalog(context);
	catalog.DropEntry(context, &info);
}

void ReplayState::ReplaySequenceValue() {
	auto schema = source.Read<string>();
	auto name = source.Read<string>();
	auto usage_count = source.Read<uint64_t>();
	auto counter = source.Read<int64_t>();
	if (deserialize_only) {
		return;
	}

	// fetch the sequence from the catalog
	auto &catalog = Catalog::GetCatalog(context);
	auto seq = catalog.GetEntry<SequenceCatalogEntry>(context, schema, name);
	if (usage_count > seq->usage_count) {
		seq->usage_count = usage_count;
		seq->counter = counter;
	}
}

//===--------------------------------------------------------------------===//
// Replay Macro
//===--------------------------------------------------------------------===//
void ReplayState::ReplayCreateMacro() {
	auto entry = ScalarMacroCatalogEntry::Deserialize(source);
	if (deserialize_only) {
		return;
	}

	auto &catalog = Catalog::GetCatalog(context);
	catalog.CreateFunction(context, entry.get());
}

void ReplayState::ReplayDropMacro() {
	DropInfo info;
	info.type = CatalogType::MACRO_ENTRY;
	info.schema = source.Read<string>();
	info.name = source.Read<string>();
	if (deserialize_only) {
		return;
	}

	auto &catalog = Catalog::GetCatalog(context);
	catalog.DropEntry(context, &info);
}

//===--------------------------------------------------------------------===//
// Replay Table Macro
//===--------------------------------------------------------------------===//
void ReplayState::ReplayCreateTableMacro() {
	auto entry = TableMacroCatalogEntry::Deserialize(source);
	if (deserialize_only) {
		return;
	}

	auto &catalog = Catalog::GetCatalog(context);
	catalog.CreateFunction(context, entry.get());
}

void ReplayState::ReplayDropTableMacro() {
	DropInfo info;
	info.type = CatalogType::TABLE_MACRO_ENTRY;
	info.schema = source.Read<string>();
	info.name = source.Read<string>();
	if (deserialize_only) {
		return;
	}

	auto &catalog = Catalog::GetCatalog(context);
	catalog.DropEntry(context, &info);
}

//===--------------------------------------------------------------------===//
// Replay Data
//===--------------------------------------------------------------------===//
void ReplayState::ReplayUseTable() {
	auto schema_name = source.Read<string>();
	auto table_name = source.Read<string>();
	if (deserialize_only) {
		return;
	}
	auto &catalog = Catalog::GetCatalog(context);
	current_table = catalog.GetEntry<TableCatalogEntry>(context, schema_name, table_name);
}

void ReplayState::ReplayInsert() {
	DataChunk chunk;
	chunk.Deserialize(source);
	if (deserialize_only) {
		return;
	}
	if (!current_table) {
		throw Exception("Corrupt WAL: insert without table");
	}

	// append to the current table
	current_table->storage->Append(*current_table, context, chunk);
}

void ReplayState::ReplayDelete() {
	DataChunk chunk;
	chunk.Deserialize(source);
	if (deserialize_only) {
		return;
	}
	if (!current_table) {
		throw InternalException("Corrupt WAL: delete without table");
	}

	D_ASSERT(chunk.ColumnCount() == 1 && chunk.data[0].GetType() == LogicalType::ROW_TYPE);
	row_t row_ids[1];
	Vector row_identifiers(LogicalType::ROW_TYPE, (data_ptr_t)row_ids);

	auto source_ids = FlatVector::GetData<row_t>(chunk.data[0]);
	// delete the tuples from the current table
	for (idx_t i = 0; i < chunk.size(); i++) {
		row_ids[0] = source_ids[i];
		current_table->storage->Delete(*current_table, context, row_identifiers, 1);
	}
}

void ReplayState::ReplayUpdate() {
	vector<column_t> column_path;
	auto column_index_count = source.Read<idx_t>();
	column_path.reserve(column_index_count);
	for (idx_t i = 0; i < column_index_count; i++) {
		column_path.push_back(source.Read<column_t>());
	}
	DataChunk chunk;
	chunk.Deserialize(source);
	if (deserialize_only) {
		return;
	}
	if (!current_table) {
		throw InternalException("Corrupt WAL: update without table");
	}

	if (column_path[0] >= current_table->columns.size()) {
		throw InternalException("Corrupt WAL: column index for update out of bounds");
	}

	// remove the row id vector from the chunk
	auto row_ids = move(chunk.data.back());
	chunk.data.pop_back();

	// now perform the update
	current_table->storage->UpdateColumn(*current_table, context, row_ids, column_path, chunk);
}

void ReplayState::ReplayCheckpoint() {
	checkpoint_id = source.Read<block_id_t>();
}

} // namespace duckdb









#include <cstring>

namespace duckdb {

WriteAheadLog::WriteAheadLog(DatabaseInstance &database) : initialized(false), skip_writing(false), database(database) {
}

void WriteAheadLog::Initialize(string &path) {
	wal_path = path;
	writer = make_unique<BufferedFileWriter>(database.GetFileSystem(), path.c_str(),
	                                         FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE |
	                                             FileFlags::FILE_FLAGS_APPEND);
	initialized = true;
}

int64_t WriteAheadLog::GetWALSize() {
	D_ASSERT(writer);
	return writer->GetFileSize();
}

idx_t WriteAheadLog::GetTotalWritten() {
	D_ASSERT(writer);
	return writer->GetTotalWritten();
}

void WriteAheadLog::Truncate(int64_t size) {
	writer->Truncate(size);
}

void WriteAheadLog::Delete() {
	if (!initialized) {
		return;
	}
	initialized = false;
	writer.reset();

	auto &fs = FileSystem::GetFileSystem(database);
	fs.RemoveFile(wal_path);
}

//===--------------------------------------------------------------------===//
// Write Entries
//===--------------------------------------------------------------------===//
void WriteAheadLog::WriteCheckpoint(block_id_t meta_block) {
	writer->Write<WALType>(WALType::CHECKPOINT);
	writer->Write<block_id_t>(meta_block);
}

//===--------------------------------------------------------------------===//
// CREATE TABLE
//===--------------------------------------------------------------------===//
void WriteAheadLog::WriteCreateTable(TableCatalogEntry *entry) {
	if (skip_writing) {
		return;
	}
	writer->Write<WALType>(WALType::CREATE_TABLE);
	entry->Serialize(*writer);
}

//===--------------------------------------------------------------------===//
// DROP TABLE
//===--------------------------------------------------------------------===//
void WriteAheadLog::WriteDropTable(TableCatalogEntry *entry) {
	if (skip_writing) {
		return;
	}
	writer->Write<WALType>(WALType::DROP_TABLE);
	writer->WriteString(entry->schema->name);
	writer->WriteString(entry->name);
}

//===--------------------------------------------------------------------===//
// CREATE SCHEMA
//===--------------------------------------------------------------------===//
void WriteAheadLog::WriteCreateSchema(SchemaCatalogEntry *entry) {
	if (skip_writing) {
		return;
	}
	writer->Write<WALType>(WALType::CREATE_SCHEMA);
	writer->WriteString(entry->name);
}

//===--------------------------------------------------------------------===//
// SEQUENCES
//===--------------------------------------------------------------------===//
void WriteAheadLog::WriteCreateSequence(SequenceCatalogEntry *entry) {
	if (skip_writing) {
		return;
	}
	writer->Write<WALType>(WALType::CREATE_SEQUENCE);
	entry->Serialize(*writer);
}

void WriteAheadLog::WriteDropSequence(SequenceCatalogEntry *entry) {
	if (skip_writing) {
		return;
	}
	writer->Write<WALType>(WALType::DROP_SEQUENCE);
	writer->WriteString(entry->schema->name);
	writer->WriteString(entry->name);
}

void WriteAheadLog::WriteSequenceValue(SequenceCatalogEntry *entry, SequenceValue val) {
	if (skip_writing) {
		return;
	}
	writer->Write<WALType>(WALType::SEQUENCE_VALUE);
	writer->WriteString(entry->schema->name);
	writer->WriteString(entry->name);
	writer->Write<uint64_t>(val.usage_count);
	writer->Write<int64_t>(val.counter);
}

//===--------------------------------------------------------------------===//
// MACRO'S
//===--------------------------------------------------------------------===//
void WriteAheadLog::WriteCreateMacro(ScalarMacroCatalogEntry *entry) {
	if (skip_writing) {
		return;
	}
	writer->Write<WALType>(WALType::CREATE_MACRO);
	entry->Serialize(*writer);
}

void WriteAheadLog::WriteDropMacro(ScalarMacroCatalogEntry *entry) {
	if (skip_writing) {
		return;
	}
	writer->Write<WALType>(WALType::DROP_MACRO);
	writer->WriteString(entry->schema->name);
	writer->WriteString(entry->name);
}

void WriteAheadLog::WriteCreateTableMacro(TableMacroCatalogEntry *entry) {
	if (skip_writing) {
		return;
	}
	writer->Write<WALType>(WALType::CREATE_TABLE_MACRO);
	entry->Serialize(*writer);
}

void WriteAheadLog::WriteDropTableMacro(TableMacroCatalogEntry *entry) {
	if (skip_writing) {
		return;
	}
	writer->Write<WALType>(WALType::DROP_TABLE_MACRO);
	writer->WriteString(entry->schema->name);
	writer->WriteString(entry->name);
}

//===--------------------------------------------------------------------===//
// Custom Types
//===--------------------------------------------------------------------===//
void WriteAheadLog::WriteCreateType(TypeCatalogEntry *entry) {
	if (skip_writing) {
		return;
	}
	writer->Write<WALType>(WALType::CREATE_TYPE);
	entry->Serialize(*writer);
}

void WriteAheadLog::WriteDropType(TypeCatalogEntry *entry) {
	if (skip_writing) {
		return;
	}
	writer->Write<WALType>(WALType::DROP_TYPE);
	writer->WriteString(entry->schema->name);
	writer->WriteString(entry->name);
}

//===--------------------------------------------------------------------===//
// VIEWS
//===--------------------------------------------------------------------===//
void WriteAheadLog::WriteCreateView(ViewCatalogEntry *entry) {
	if (skip_writing) {
		return;
	}
	writer->Write<WALType>(WALType::CREATE_VIEW);
	entry->Serialize(*writer);
}

void WriteAheadLog::WriteDropView(ViewCatalogEntry *entry) {
	if (skip_writing) {
		return;
	}
	writer->Write<WALType>(WALType::DROP_VIEW);
	writer->WriteString(entry->schema->name);
	writer->WriteString(entry->name);
}

//===--------------------------------------------------------------------===//
// DROP SCHEMA
//===--------------------------------------------------------------------===//
void WriteAheadLog::WriteDropSchema(SchemaCatalogEntry *entry) {
	if (skip_writing) {
		return;
	}
	writer->Write<WALType>(WALType::DROP_SCHEMA);
	writer->WriteString(entry->name);
}

//===--------------------------------------------------------------------===//
// DATA
//===--------------------------------------------------------------------===//
void WriteAheadLog::WriteSetTable(string &schema, string &table) {
	if (skip_writing) {
		return;
	}
	writer->Write<WALType>(WALType::USE_TABLE);
	writer->WriteString(schema);
	writer->WriteString(table);
}

void WriteAheadLog::WriteInsert(DataChunk &chunk) {
	if (skip_writing) {
		return;
	}
	D_ASSERT(chunk.size() > 0);
	chunk.Verify();

	writer->Write<WALType>(WALType::INSERT_TUPLE);
	chunk.Serialize(*writer);
}

void WriteAheadLog::WriteDelete(DataChunk &chunk) {
	if (skip_writing) {
		return;
	}
	D_ASSERT(chunk.size() > 0);
	D_ASSERT(chunk.ColumnCount() == 1 && chunk.data[0].GetType() == LogicalType::ROW_TYPE);
	chunk.Verify();

	writer->Write<WALType>(WALType::DELETE_TUPLE);
	chunk.Serialize(*writer);
}

void WriteAheadLog::WriteUpdate(DataChunk &chunk, const vector<column_t> &column_indexes) {
	if (skip_writing) {
		return;
	}
	D_ASSERT(chunk.size() > 0);
	D_ASSERT(chunk.ColumnCount() == 2);
	D_ASSERT(chunk.data[1].GetType().id() == LogicalType::ROW_TYPE);
	chunk.Verify();

	writer->Write<WALType>(WALType::UPDATE_TUPLE);
	writer->Write<idx_t>(column_indexes.size());
	for (auto &col_idx : column_indexes) {
		writer->Write<column_t>(col_idx);
	}
	chunk.Serialize(*writer);
}

//===--------------------------------------------------------------------===//
// Write ALTER Statement
//===--------------------------------------------------------------------===//
void WriteAheadLog::WriteAlter(AlterInfo &info) {
	if (skip_writing) {
		return;
	}
	writer->Write<WALType>(WALType::ALTER_INFO);
	info.Serialize(*writer);
}

//===--------------------------------------------------------------------===//
// FLUSH
//===--------------------------------------------------------------------===//
void WriteAheadLog::Flush() {
	if (skip_writing) {
		return;
	}
	// write an empty entry
	writer->Write<WALType>(WALType::WAL_FLUSH);
	// flushes all changes made to the WAL to disk
	writer->Sync();
}

} // namespace duckdb











namespace duckdb {

CleanupState::CleanupState() : current_table(nullptr), count(0) {
}

CleanupState::~CleanupState() {
	Flush();
}

void CleanupState::CleanupEntry(UndoFlags type, data_ptr_t data) {
	switch (type) {
	case UndoFlags::CATALOG_ENTRY: {
		auto catalog_entry = Load<CatalogEntry *>(data);
		D_ASSERT(catalog_entry);
		D_ASSERT(catalog_entry->set);
		catalog_entry->set->CleanupEntry(catalog_entry);
		break;
	}
	case UndoFlags::DELETE_TUPLE: {
		auto info = (DeleteInfo *)data;
		CleanupDelete(info);
		break;
	}
	case UndoFlags::UPDATE_TUPLE: {
		auto info = (UpdateInfo *)data;
		CleanupUpdate(info);
		break;
	}
	default:
		break;
	}
}

void CleanupState::CleanupUpdate(UpdateInfo *info) {
	// remove the update info from the update chain
	// first obtain an exclusive lock on the segment
	info->segment->CleanupUpdate(info);
}

void CleanupState::CleanupDelete(DeleteInfo *info) {
	auto version_table = info->table;
	version_table->info->cardinality -= info->count;
	if (version_table->info->indexes.Empty()) {
		// this table has no indexes: no cleanup to be done
		return;
	}
	if (current_table != version_table) {
		// table for this entry differs from previous table: flush and switch to the new table
		Flush();
		current_table = version_table;
	}
	count = 0;
	for (idx_t i = 0; i < info->count; i++) {
		row_numbers[count++] = info->vinfo->start + info->rows[i];
	}
	Flush();
}

void CleanupState::Flush() {
	if (count == 0) {
		return;
	}

	// set up the row identifiers vector
	Vector row_identifiers(LogicalType::ROW_TYPE, (data_ptr_t)row_numbers);

	// delete the tuples from all the indexes
	current_table->RemoveFromIndexes(row_identifiers, count);

	count = 0;
}

} // namespace duckdb

















namespace duckdb {

CommitState::CommitState(transaction_t commit_id, WriteAheadLog *log)
    : log(log), commit_id(commit_id), current_table_info(nullptr) {
}

void CommitState::SwitchTable(DataTableInfo *table_info, UndoFlags new_op) {
	if (current_table_info != table_info) {
		// write the current table to the log
		log->WriteSetTable(table_info->schema, table_info->table);
		current_table_info = table_info;
	}
}

void CommitState::WriteCatalogEntry(CatalogEntry *entry, data_ptr_t dataptr) {
	if (entry->temporary || entry->parent->temporary) {
		return;
	}
	D_ASSERT(log);
	// look at the type of the parent entry
	auto parent = entry->parent;
	switch (parent->type) {
	case CatalogType::TABLE_ENTRY:
		if (entry->type == CatalogType::TABLE_ENTRY) {
			auto table_entry = (TableCatalogEntry *)entry;
			// ALTER TABLE statement, read the extra data after the entry
			auto extra_data_size = Load<idx_t>(dataptr);
			auto extra_data = (data_ptr_t)(dataptr + sizeof(idx_t));
			// deserialize it
			BufferedDeserializer source(extra_data, extra_data_size);
			auto info = AlterInfo::Deserialize(source);
			// write the alter table in the log
			table_entry->CommitAlter(*info);
			log->WriteAlter(*info);
		} else {
			// CREATE TABLE statement
			log->WriteCreateTable((TableCatalogEntry *)parent);
		}
		break;
	case CatalogType::SCHEMA_ENTRY:
		if (entry->type == CatalogType::SCHEMA_ENTRY) {
			// ALTER TABLE statement, skip it
			return;
		}
		log->WriteCreateSchema((SchemaCatalogEntry *)parent);
		break;
	case CatalogType::VIEW_ENTRY:
		if (entry->type == CatalogType::VIEW_ENTRY) {
			// ALTER TABLE statement, read the extra data after the entry
			auto extra_data_size = Load<idx_t>(dataptr);
			auto extra_data = (data_ptr_t)(dataptr + sizeof(idx_t));
			// deserialize it
			BufferedDeserializer source(extra_data, extra_data_size);
			auto info = AlterInfo::Deserialize(source);
			// write the alter table in the log
			log->WriteAlter(*info);
		} else {
			log->WriteCreateView((ViewCatalogEntry *)parent);
		}
		break;
	case CatalogType::SEQUENCE_ENTRY:
		log->WriteCreateSequence((SequenceCatalogEntry *)parent);
		break;
	case CatalogType::MACRO_ENTRY:
		log->WriteCreateMacro((ScalarMacroCatalogEntry *)parent);
		break;
	case CatalogType::TABLE_MACRO_ENTRY:
		log->WriteCreateTableMacro((TableMacroCatalogEntry *)parent);
		break;

	case CatalogType::TYPE_ENTRY:
		log->WriteCreateType((TypeCatalogEntry *)parent);
		break;
	case CatalogType::DELETED_ENTRY:
		switch (entry->type) {
		case CatalogType::TABLE_ENTRY: {
			auto table_entry = (TableCatalogEntry *)entry;
			table_entry->CommitDrop();
			log->WriteDropTable(table_entry);
			break;
		}
		case CatalogType::SCHEMA_ENTRY:
			log->WriteDropSchema((SchemaCatalogEntry *)entry);
			break;
		case CatalogType::VIEW_ENTRY:
			log->WriteDropView((ViewCatalogEntry *)entry);
			break;
		case CatalogType::SEQUENCE_ENTRY:
			log->WriteDropSequence((SequenceCatalogEntry *)entry);
			break;
		case CatalogType::MACRO_ENTRY:
			log->WriteDropMacro((ScalarMacroCatalogEntry *)entry);
			break;
		case CatalogType::TABLE_MACRO_ENTRY:
			log->WriteDropTableMacro((TableMacroCatalogEntry *)entry);
			break;
		case CatalogType::TYPE_ENTRY:
			log->WriteDropType((TypeCatalogEntry *)entry);
			break;
		case CatalogType::INDEX_ENTRY:
		case CatalogType::PREPARED_STATEMENT:
		case CatalogType::SCALAR_FUNCTION_ENTRY:
			// do nothing, indexes/prepared statements/functions aren't persisted to disk
			break;
		default:
			throw InternalException("Don't know how to drop this type!");
		}
		break;
	case CatalogType::INDEX_ENTRY:
	case CatalogType::PREPARED_STATEMENT:
	case CatalogType::AGGREGATE_FUNCTION_ENTRY:
	case CatalogType::SCALAR_FUNCTION_ENTRY:
	case CatalogType::TABLE_FUNCTION_ENTRY:
	case CatalogType::COPY_FUNCTION_ENTRY:
	case CatalogType::PRAGMA_FUNCTION_ENTRY:
	case CatalogType::COLLATION_ENTRY:
		// do nothing, these entries are not persisted to disk
		break;
	default:
		throw InternalException("UndoBuffer - don't know how to write this entry to the WAL");
	}
}

void CommitState::WriteDelete(DeleteInfo *info) {
	D_ASSERT(log);
	// switch to the current table, if necessary
	SwitchTable(info->table->info.get(), UndoFlags::DELETE_TUPLE);

	if (!delete_chunk) {
		delete_chunk = make_unique<DataChunk>();
		vector<LogicalType> delete_types = {LogicalType::ROW_TYPE};
		delete_chunk->Initialize(delete_types);
	}
	auto rows = FlatVector::GetData<row_t>(delete_chunk->data[0]);
	for (idx_t i = 0; i < info->count; i++) {
		rows[i] = info->base_row + info->rows[i];
	}
	delete_chunk->SetCardinality(info->count);
	log->WriteDelete(*delete_chunk);
}

void CommitState::WriteUpdate(UpdateInfo *info) {
	D_ASSERT(log);
	// switch to the current table, if necessary
	auto &column_data = info->segment->column_data;
	auto &table_info = column_data.GetTableInfo();

	SwitchTable(&table_info, UndoFlags::UPDATE_TUPLE);

	// initialize the update chunk
	vector<LogicalType> update_types;
	if (column_data.type.id() == LogicalTypeId::VALIDITY) {
		update_types.emplace_back(LogicalType::BOOLEAN);
	} else {
		update_types.push_back(column_data.type);
	}
	update_types.emplace_back(LogicalType::ROW_TYPE);

	update_chunk = make_unique<DataChunk>();
	update_chunk->Initialize(update_types);

	// fetch the updated values from the base segment
	info->segment->FetchCommitted(info->vector_index, update_chunk->data[0]);

	// write the row ids into the chunk
	auto row_ids = FlatVector::GetData<row_t>(update_chunk->data[1]);
	idx_t start = column_data.start + info->vector_index * STANDARD_VECTOR_SIZE;
	for (idx_t i = 0; i < info->N; i++) {
		row_ids[info->tuples[i]] = start + info->tuples[i];
	}
	if (column_data.type.id() == LogicalTypeId::VALIDITY) {
		// zero-initialize the booleans
		// FIXME: this is only required because of NullValue<T> in Vector::Serialize...
		auto booleans = FlatVector::GetData<bool>(update_chunk->data[0]);
		for (idx_t i = 0; i < info->N; i++) {
			auto idx = info->tuples[i];
			booleans[idx] = false;
		}
	}
	SelectionVector sel(info->tuples);
	update_chunk->Slice(sel, info->N);

	// construct the column index path
	vector<column_t> column_indexes;
	auto column_data_ptr = &column_data;
	while (column_data_ptr->parent) {
		column_indexes.push_back(column_data_ptr->column_index);
		column_data_ptr = column_data_ptr->parent;
	}
	column_indexes.push_back(info->column_index);
	std::reverse(column_indexes.begin(), column_indexes.end());

	log->WriteUpdate(*update_chunk, column_indexes);
}

template <bool HAS_LOG>
void CommitState::CommitEntry(UndoFlags type, data_ptr_t data) {
	switch (type) {
	case UndoFlags::CATALOG_ENTRY: {
		// set the commit timestamp of the catalog entry to the given id
		auto catalog_entry = Load<CatalogEntry *>(data);
		D_ASSERT(catalog_entry->parent);
		catalog_entry->set->UpdateTimestamp(catalog_entry->parent, commit_id);
		if (catalog_entry->name != catalog_entry->parent->name) {
			catalog_entry->set->UpdateTimestamp(catalog_entry, commit_id);
		}
		if (HAS_LOG) {
			// push the catalog update to the WAL
			WriteCatalogEntry(catalog_entry, data + sizeof(CatalogEntry *));
		}
		break;
	}
	case UndoFlags::INSERT_TUPLE: {
		// append:
		auto info = (AppendInfo *)data;
		if (HAS_LOG && !info->table->info->IsTemporary()) {
			info->table->WriteToLog(*log, info->start_row, info->count);
		}
		// mark the tuples as committed
		info->table->CommitAppend(commit_id, info->start_row, info->count);
		break;
	}
	case UndoFlags::DELETE_TUPLE: {
		// deletion:
		auto info = (DeleteInfo *)data;
		if (HAS_LOG && !info->table->info->IsTemporary()) {
			WriteDelete(info);
		}
		// mark the tuples as committed
		info->vinfo->CommitDelete(commit_id, info->rows, info->count);
		break;
	}
	case UndoFlags::UPDATE_TUPLE: {
		// update:
		auto info = (UpdateInfo *)data;
		if (HAS_LOG && !info->segment->column_data.GetTableInfo().IsTemporary()) {
			WriteUpdate(info);
		}
		info->version_number = commit_id;
		break;
	}
	default:
		throw InternalException("UndoBuffer - don't know how to commit this type!");
	}
}

void CommitState::RevertCommit(UndoFlags type, data_ptr_t data) {
	transaction_t transaction_id = commit_id;
	switch (type) {
	case UndoFlags::CATALOG_ENTRY: {
		// set the commit timestamp of the catalog entry to the given id
		auto catalog_entry = Load<CatalogEntry *>(data);
		D_ASSERT(catalog_entry->parent);
		catalog_entry->set->UpdateTimestamp(catalog_entry->parent, transaction_id);
		if (catalog_entry->name != catalog_entry->parent->name) {
			catalog_entry->set->UpdateTimestamp(catalog_entry, transaction_id);
		}
		break;
	}
	case UndoFlags::INSERT_TUPLE: {
		auto info = (AppendInfo *)data;
		// revert this append
		info->table->RevertAppend(info->start_row, info->count);
		break;
	}
	case UndoFlags::DELETE_TUPLE: {
		// deletion:
		auto info = (DeleteInfo *)data;
		info->table->info->cardinality += info->count;
		// revert the commit by writing the (uncommitted) transaction_id back into the version info
		info->vinfo->CommitDelete(transaction_id, info->rows, info->count);
		break;
	}
	case UndoFlags::UPDATE_TUPLE: {
		// update:
		auto info = (UpdateInfo *)data;
		info->version_number = transaction_id;
		break;
	}
	default:
		throw InternalException("UndoBuffer - don't know how to revert commit of this type!");
	}
}

template void CommitState::CommitEntry<true>(UndoFlags type, data_ptr_t data);
template void CommitState::CommitEntry<false>(UndoFlags type, data_ptr_t data);

} // namespace duckdb












namespace duckdb {

void RollbackState::RollbackEntry(UndoFlags type, data_ptr_t data) {
	switch (type) {
	case UndoFlags::CATALOG_ENTRY: {
		// undo this catalog entry
		auto catalog_entry = Load<CatalogEntry *>(data);
		D_ASSERT(catalog_entry->set);
		catalog_entry->set->Undo(catalog_entry);
		break;
	}
	case UndoFlags::INSERT_TUPLE: {
		auto info = (AppendInfo *)data;
		// revert the append in the base table
		info->table->RevertAppend(info->start_row, info->count);
		break;
	}
	case UndoFlags::DELETE_TUPLE: {
		auto info = (DeleteInfo *)data;
		// reset the deleted flag on rollback
		info->vinfo->CommitDelete(NOT_DELETED_ID, info->rows, info->count);
		break;
	}
	case UndoFlags::UPDATE_TUPLE: {
		auto info = (UpdateInfo *)data;
		info->segment->RollbackUpdate(info);
		break;
	}
	default: // LCOV_EXCL_START
		D_ASSERT(type == UndoFlags::EMPTY_ENTRY);
		break;
	} // LCOV_EXCL_STOP
}

} // namespace duckdb

















#include <cstring>

namespace duckdb {

Transaction &Transaction::GetTransaction(ClientContext &context) {
	return context.ActiveTransaction();
}

void Transaction::PushCatalogEntry(CatalogEntry *entry, data_ptr_t extra_data, idx_t extra_data_size) {
	idx_t alloc_size = sizeof(CatalogEntry *);
	if (extra_data_size > 0) {
		alloc_size += extra_data_size + sizeof(idx_t);
	}
	auto baseptr = undo_buffer.CreateEntry(UndoFlags::CATALOG_ENTRY, alloc_size);
	// store the pointer to the catalog entry
	Store<CatalogEntry *>(entry, baseptr);
	if (extra_data_size > 0) {
		// copy the extra data behind the catalog entry pointer (if any)
		baseptr += sizeof(CatalogEntry *);
		// first store the extra data size
		Store<idx_t>(extra_data_size, baseptr);
		baseptr += sizeof(idx_t);
		// then copy over the actual data
		memcpy(baseptr, extra_data, extra_data_size);
	}
}

void Transaction::PushDelete(DataTable *table, ChunkVectorInfo *vinfo, row_t rows[], idx_t count, idx_t base_row) {
	auto delete_info =
	    (DeleteInfo *)undo_buffer.CreateEntry(UndoFlags::DELETE_TUPLE, sizeof(DeleteInfo) + sizeof(row_t) * count);
	delete_info->vinfo = vinfo;
	delete_info->table = table;
	delete_info->count = count;
	delete_info->base_row = base_row;
	memcpy(delete_info->rows, rows, sizeof(row_t) * count);
}

void Transaction::PushAppend(DataTable *table, idx_t start_row, idx_t row_count) {
	auto append_info = (AppendInfo *)undo_buffer.CreateEntry(UndoFlags::INSERT_TUPLE, sizeof(AppendInfo));
	append_info->table = table;
	append_info->start_row = start_row;
	append_info->count = row_count;
}

UpdateInfo *Transaction::CreateUpdateInfo(idx_t type_size, idx_t entries) {
	auto update_info = (UpdateInfo *)undo_buffer.CreateEntry(
	    UndoFlags::UPDATE_TUPLE, sizeof(UpdateInfo) + (sizeof(sel_t) + type_size) * STANDARD_VECTOR_SIZE);
	update_info->max = STANDARD_VECTOR_SIZE;
	update_info->tuples = (sel_t *)(((data_ptr_t)update_info) + sizeof(UpdateInfo));
	update_info->tuple_data = ((data_ptr_t)update_info) + sizeof(UpdateInfo) + sizeof(sel_t) * update_info->max;
	update_info->version_number = transaction_id;
	return update_info;
}

bool Transaction::ChangesMade() {
	return undo_buffer.ChangesMade() || storage.ChangesMade();
}

bool Transaction::AutomaticCheckpoint(DatabaseInstance &db) {
	auto &config = DBConfig::GetConfig(db);
	auto &storage_manager = StorageManager::GetStorageManager(db);
	auto log = storage_manager.GetWriteAheadLog();
	if (!log) {
		return false;
	}

	auto initial_size = log->GetWALSize();
	idx_t expected_wal_size = initial_size + storage.EstimatedSize() + undo_buffer.EstimatedSize();
	return expected_wal_size > config.checkpoint_wal_size;
}

string Transaction::Commit(DatabaseInstance &db, transaction_t commit_id, bool checkpoint) noexcept {
	this->commit_id = commit_id;
	auto &storage_manager = StorageManager::GetStorageManager(db);
	auto log = storage_manager.GetWriteAheadLog();

	UndoBuffer::IteratorState iterator_state;
	LocalStorage::CommitState commit_state;
	idx_t initial_wal_size = 0;
	idx_t initial_written = 0;
	if (log) {
		auto initial_size = log->GetWALSize();
		initial_written = log->GetTotalWritten();
		initial_wal_size = initial_size < 0 ? 0 : idx_t(initial_size);
	} else {
		D_ASSERT(!checkpoint);
	}
	try {
		if (checkpoint) {
			// check if we are checkpointing after this commit
			// if we are checkpointing, we don't need to write anything to the WAL
			// this saves us a lot of unnecessary writes to disk in the case of large commits
			log->skip_writing = true;
		}
		storage.Commit(commit_state, *this, log, commit_id);
		undo_buffer.Commit(iterator_state, log, commit_id);
		if (log) {
			// commit any sequences that were used to the WAL
			for (auto &entry : sequence_usage) {
				log->WriteSequenceValue(entry.first, entry.second);
			}
			// flush the WAL if any changes were made
			if (log->GetTotalWritten() > initial_written) {
				D_ASSERT(!checkpoint);
				D_ASSERT(!log->skip_writing);
				log->Flush();
			}
			log->skip_writing = false;
		}
		return string();
	} catch (std::exception &ex) {
		undo_buffer.RevertCommit(iterator_state, transaction_id);
		if (log) {
			log->skip_writing = false;
			if (log->GetTotalWritten() > initial_written) {
				// remove any entries written into the WAL by truncating it
				log->Truncate(initial_wal_size);
			}
		}
		D_ASSERT(!log || !log->skip_writing);
		return ex.what();
	}
}

} // namespace duckdb






namespace duckdb {

TransactionContext::~TransactionContext() {
	if (current_transaction) {
		try {
			Rollback();
		} catch (...) {
		}
	}
}

void TransactionContext::BeginTransaction() {
	if (current_transaction) {
		throw TransactionException("cannot start a transaction within a transaction");
	}
	current_transaction = transaction_manager.StartTransaction(context);
}

void TransactionContext::Commit() {
	if (!current_transaction) {
		throw TransactionException("failed to commit: no transaction active");
	}
	auto transaction = current_transaction;
	SetAutoCommit(true);
	current_transaction = nullptr;
	string error = transaction_manager.CommitTransaction(context, transaction);
	if (!error.empty()) {
		throw TransactionException("Failed to commit: %s", error);
	}
}

void TransactionContext::SetAutoCommit(bool value) {
	auto_commit = value;
	if (!auto_commit && !current_transaction) {
		BeginTransaction();
	}
}

void TransactionContext::Rollback() {
	if (!current_transaction) {
		throw TransactionException("failed to rollback: no transaction active");
	}
	auto transaction = current_transaction;
	ClearTransaction();
	transaction_manager.RollbackTransaction(transaction);
}

void TransactionContext::ClearTransaction() {
	SetAutoCommit(true);
	current_transaction = nullptr;
}

} // namespace duckdb













namespace duckdb {

struct CheckpointLock {
	explicit CheckpointLock(TransactionManager &manager) : manager(manager), is_locked(false) {
	}
	~CheckpointLock() {
		Unlock();
	}

	TransactionManager &manager;
	bool is_locked;

	void Lock() {
		D_ASSERT(!manager.thread_is_checkpointing);
		manager.thread_is_checkpointing = true;
		is_locked = true;
	}
	void Unlock() {
		if (!is_locked) {
			return;
		}
		D_ASSERT(manager.thread_is_checkpointing);
		manager.thread_is_checkpointing = false;
		is_locked = false;
	}
};

TransactionManager::TransactionManager(DatabaseInstance &db) : db(db), thread_is_checkpointing(false) {
	// start timestamp starts at zero
	current_start_timestamp = 0;
	// transaction ID starts very high:
	// it should be much higher than the current start timestamp
	// if transaction_id < start_timestamp for any set of active transactions
	// uncommited data could be read by
	current_transaction_id = TRANSACTION_ID_START;
	// the current active query id
	current_query_number = 1;
	lowest_active_id = TRANSACTION_ID_START;
	lowest_active_start = MAX_TRANSACTION_ID;
}

TransactionManager::~TransactionManager() {
}

Transaction *TransactionManager::StartTransaction(ClientContext &context) {
	// obtain the transaction lock during this function
	lock_guard<mutex> lock(transaction_lock);
	if (current_start_timestamp >= TRANSACTION_ID_START) { // LCOV_EXCL_START
		throw InternalException("Cannot start more transactions, ran out of "
		                        "transaction identifiers!");
	} // LCOV_EXCL_STOP

	// obtain the start time and transaction ID of this transaction
	transaction_t start_time = current_start_timestamp++;
	transaction_t transaction_id = current_transaction_id++;
	timestamp_t start_timestamp = Timestamp::GetCurrentTimestamp();
	if (active_transactions.empty()) {
		lowest_active_start = start_time;
		lowest_active_id = transaction_id;
	}

	// create the actual transaction
	auto &catalog = Catalog::GetCatalog(db);
	auto transaction = make_unique<Transaction>(weak_ptr<ClientContext>(context.shared_from_this()), start_time,
	                                            transaction_id, start_timestamp, catalog.GetCatalogVersion());
	auto transaction_ptr = transaction.get();

	// store it in the set of active transactions
	active_transactions.push_back(move(transaction));
	return transaction_ptr;
}

struct ClientLockWrapper {
	ClientLockWrapper(mutex &client_lock, shared_ptr<ClientContext> connection)
	    : connection(move(connection)), connection_lock(make_unique<lock_guard<mutex>>(client_lock)) {
	}

	shared_ptr<ClientContext> connection;
	unique_ptr<lock_guard<mutex>> connection_lock;
};

void TransactionManager::LockClients(vector<ClientLockWrapper> &client_locks, ClientContext &context) {
	auto &connection_manager = ConnectionManager::Get(context);
	client_locks.emplace_back(connection_manager.connections_lock, nullptr);
	auto connection_list = connection_manager.GetConnectionList();
	for (auto &con : connection_list) {
		if (con.get() == &context) {
			continue;
		}
		auto &context_lock = con->context_lock;
		client_locks.emplace_back(context_lock, move(con));
	}
}

void TransactionManager::Checkpoint(ClientContext &context, bool force) {
	auto &storage_manager = StorageManager::GetStorageManager(db);
	if (storage_manager.InMemory()) {
		return;
	}

	// first check if no other thread is checkpointing right now
	auto lock = make_unique<lock_guard<mutex>>(transaction_lock);
	if (thread_is_checkpointing) {
		throw TransactionException("Cannot CHECKPOINT: another thread is checkpointing right now");
	}
	CheckpointLock checkpoint_lock(*this);
	checkpoint_lock.Lock();
	lock.reset();

	// lock all the clients AND the connection manager now
	// this ensures no new queries can be started, and no new connections to the database can be made
	// to avoid deadlock we release the transaction lock while locking the clients
	vector<ClientLockWrapper> client_locks;
	LockClients(client_locks, context);

	lock = make_unique<lock_guard<mutex>>(transaction_lock);
	auto current = &Transaction::GetTransaction(context);
	if (current->ChangesMade()) {
		throw TransactionException("Cannot CHECKPOINT: the current transaction has transaction local changes");
	}
	if (!force) {
		if (!CanCheckpoint(current)) {
			throw TransactionException("Cannot CHECKPOINT: there are other transactions. Use FORCE CHECKPOINT to abort "
			                           "the other transactions and force a checkpoint");
		}
	} else {
		if (!CanCheckpoint(current)) {
			for (size_t i = 0; i < active_transactions.size(); i++) {
				auto &transaction = active_transactions[i];
				// rollback the transaction
				transaction->Rollback();
				auto transaction_context = transaction->context.lock();

				// remove the transaction id from the list of active transactions
				// potentially resulting in garbage collection
				RemoveTransaction(transaction.get());
				if (transaction_context) {
					transaction_context->transaction.ClearTransaction();
				}
				i--;
			}
			D_ASSERT(CanCheckpoint(nullptr));
		}
	}
	auto &storage = StorageManager::GetStorageManager(context);
	storage.CreateCheckpoint();
}

bool TransactionManager::CanCheckpoint(Transaction *current) {
	auto &storage_manager = StorageManager::GetStorageManager(db);
	if (storage_manager.InMemory()) {
		return false;
	}
	if (!recently_committed_transactions.empty() || !old_transactions.empty()) {
		return false;
	}
	for (auto &transaction : active_transactions) {
		if (transaction.get() != current) {
			return false;
		}
	}
	return true;
}

string TransactionManager::CommitTransaction(ClientContext &context, Transaction *transaction) {
	vector<ClientLockWrapper> client_locks;
	auto lock = make_unique<lock_guard<mutex>>(transaction_lock);
	CheckpointLock checkpoint_lock(*this);
	// check if we can checkpoint
	bool checkpoint = thread_is_checkpointing ? false : CanCheckpoint(transaction);
	if (checkpoint) {
		if (transaction->AutomaticCheckpoint(db)) {
			checkpoint_lock.Lock();
			// we might be able to checkpoint: lock all clients
			// to avoid deadlock we release the transaction lock while locking the clients
			lock.reset();

			LockClients(client_locks, context);

			lock = make_unique<lock_guard<mutex>>(transaction_lock);
			checkpoint = CanCheckpoint(transaction);
			if (!checkpoint) {
				checkpoint_lock.Unlock();
				client_locks.clear();
			}
		} else {
			checkpoint = false;
		}
	}
	// obtain a commit id for the transaction
	transaction_t commit_id = current_start_timestamp++;
	// commit the UndoBuffer of the transaction
	string error = transaction->Commit(db, commit_id, checkpoint);
	if (!error.empty()) {
		// commit unsuccessful: rollback the transaction instead
		checkpoint = false;
		transaction->commit_id = 0;
		transaction->Rollback();
	}
	if (!checkpoint) {
		// we won't checkpoint after all: unlock the clients again
		checkpoint_lock.Unlock();
		client_locks.clear();
	}

	// commit successful: remove the transaction id from the list of active transactions
	// potentially resulting in garbage collection
	RemoveTransaction(transaction);
	// now perform a checkpoint if (1) we are able to checkpoint, and (2) the WAL has reached sufficient size to
	// checkpoint
	if (checkpoint) {
		// checkpoint the database to disk
		auto &storage_manager = StorageManager::GetStorageManager(db);
		storage_manager.CreateCheckpoint(false, true);
	}
	return error;
}

void TransactionManager::RollbackTransaction(Transaction *transaction) {
	// obtain the transaction lock during this function
	lock_guard<mutex> lock(transaction_lock);

	// rollback the transaction
	transaction->Rollback();

	// remove the transaction id from the list of active transactions
	// potentially resulting in garbage collection
	RemoveTransaction(transaction);
}

void TransactionManager::RemoveTransaction(Transaction *transaction) noexcept {
	// remove the transaction from the list of active transactions
	idx_t t_index = active_transactions.size();
	// check for the lowest and highest start time in the list of transactions
	transaction_t lowest_start_time = TRANSACTION_ID_START;
	transaction_t lowest_transaction_id = MAX_TRANSACTION_ID;
	transaction_t lowest_active_query = MAXIMUM_QUERY_ID;
	for (idx_t i = 0; i < active_transactions.size(); i++) {
		if (active_transactions[i].get() == transaction) {
			t_index = i;
		} else {
			transaction_t active_query = active_transactions[i]->active_query;
			lowest_start_time = MinValue(lowest_start_time, active_transactions[i]->start_time);
			lowest_active_query = MinValue(lowest_active_query, active_query);
			lowest_transaction_id = MinValue(lowest_transaction_id, active_transactions[i]->transaction_id);
		}
	}
	lowest_active_start = lowest_start_time;
	lowest_active_id = lowest_transaction_id;

	transaction_t lowest_stored_query = lowest_start_time;
	D_ASSERT(t_index != active_transactions.size());
	auto current_transaction = move(active_transactions[t_index]);
	if (transaction->commit_id != 0) {
		// the transaction was committed, add it to the list of recently
		// committed transactions
		recently_committed_transactions.push_back(move(current_transaction));
	} else {
		// the transaction was aborted, but we might still need its information
		// add it to the set of transactions awaiting GC
		current_transaction->highest_active_query = current_query_number;
		old_transactions.push_back(move(current_transaction));
	}
	// remove the transaction from the set of currently active transactions
	active_transactions.erase(active_transactions.begin() + t_index);
	// traverse the recently_committed transactions to see if we can remove any
	idx_t i = 0;
	for (; i < recently_committed_transactions.size(); i++) {
		D_ASSERT(recently_committed_transactions[i]);
		lowest_stored_query = MinValue(recently_committed_transactions[i]->start_time, lowest_stored_query);
		if (recently_committed_transactions[i]->commit_id < lowest_start_time) {
			// changes made BEFORE this transaction are no longer relevant
			// we can cleanup the undo buffer

			// HOWEVER: any currently running QUERY can still be using
			// the version information after the cleanup!

			// if we remove the UndoBuffer immediately, we have a race
			// condition

			// we can only safely do the actual memory cleanup when all the
			// currently active queries have finished running! (actually,
			// when all the currently active scans have finished running...)
			recently_committed_transactions[i]->Cleanup();
			// store the current highest active query
			recently_committed_transactions[i]->highest_active_query = current_query_number;
			// move it to the list of transactions awaiting GC
			old_transactions.push_back(move(recently_committed_transactions[i]));
		} else {
			// recently_committed_transactions is ordered on commit_id
			// implicitly thus if the current one is bigger than
			// lowest_start_time any subsequent ones are also bigger
			break;
		}
	}
	if (i > 0) {
		// we garbage collected transactions: remove them from the list
		recently_committed_transactions.erase(recently_committed_transactions.begin(),
		                                      recently_committed_transactions.begin() + i);
	}
	// check if we can free the memory of any old transactions
	i = active_transactions.empty() ? old_transactions.size() : 0;
	for (; i < old_transactions.size(); i++) {
		D_ASSERT(old_transactions[i]);
		D_ASSERT(old_transactions[i]->highest_active_query > 0);
		if (old_transactions[i]->highest_active_query >= lowest_active_query) {
			// there is still a query running that could be using
			// this transactions' data
			break;
		}
	}
	if (i > 0) {
		// we garbage collected transactions: remove them from the list
		old_transactions.erase(old_transactions.begin(), old_transactions.begin() + i);
	}
	// check if we can free the memory of any old catalog sets
	for (i = 0; i < old_catalog_sets.size(); i++) {
		D_ASSERT(old_catalog_sets[i].highest_active_query > 0);
		if (old_catalog_sets[i].highest_active_query >= lowest_stored_query) {
			// there is still a query running that could be using
			// this catalog sets' data
			break;
		}
	}
	if (i > 0) {
		// we garbage collected catalog sets: remove them from the list
		old_catalog_sets.erase(old_catalog_sets.begin(), old_catalog_sets.begin() + i);
	}
}

} // namespace duckdb













#include <unordered_map>

namespace duckdb {
constexpr uint32_t DEFAULT_UNDO_CHUNK_SIZE = 4096 * 3;
constexpr uint32_t UNDO_ENTRY_HEADER_SIZE = sizeof(UndoFlags) + sizeof(uint32_t);

static idx_t AlignLength(idx_t len) {
	return (len + 7) / 8 * 8;
}

UndoBuffer::UndoBuffer() {
	head = make_unique<UndoChunk>(0);
	tail = head.get();
}

UndoChunk::UndoChunk(idx_t size) : current_position(0), maximum_size(size), prev(nullptr) {
	if (size > 0) {
		data = unique_ptr<data_t[]>(new data_t[maximum_size]);
	}
}
UndoChunk::~UndoChunk() {
	if (next) {
		auto current_next = move(next);
		while (current_next) {
			current_next = move(current_next->next);
		}
	}
}

data_ptr_t UndoChunk::WriteEntry(UndoFlags type, uint32_t len) {
	len = AlignLength(len);
	D_ASSERT(sizeof(UndoFlags) + sizeof(len) == 8);
	Store<UndoFlags>(type, data.get() + current_position);
	current_position += sizeof(UndoFlags);
	Store<uint32_t>(len, data.get() + current_position);
	current_position += sizeof(uint32_t);

	data_ptr_t result = data.get() + current_position;
	current_position += len;
	return result;
}

data_ptr_t UndoBuffer::CreateEntry(UndoFlags type, idx_t len) {
	D_ASSERT(len <= NumericLimits<uint32_t>::Maximum());
	idx_t needed_space = AlignLength(len + UNDO_ENTRY_HEADER_SIZE);
	if (head->current_position + needed_space >= head->maximum_size) {
		auto new_chunk =
		    make_unique<UndoChunk>(needed_space > DEFAULT_UNDO_CHUNK_SIZE ? needed_space : DEFAULT_UNDO_CHUNK_SIZE);
		head->prev = new_chunk.get();
		new_chunk->next = move(head);
		head = move(new_chunk);
	}
	return head->WriteEntry(type, len);
}

template <class T>
void UndoBuffer::IterateEntries(UndoBuffer::IteratorState &state, T &&callback) {
	// iterate in insertion order: start with the tail
	state.current = tail;
	while (state.current) {
		state.start = state.current->data.get();
		state.end = state.start + state.current->current_position;
		while (state.start < state.end) {
			UndoFlags type = Load<UndoFlags>(state.start);
			state.start += sizeof(UndoFlags);

			uint32_t len = Load<uint32_t>(state.start);
			state.start += sizeof(uint32_t);
			callback(type, state.start);
			state.start += len;
		}
		state.current = state.current->prev;
	}
}

template <class T>
void UndoBuffer::IterateEntries(UndoBuffer::IteratorState &state, UndoBuffer::IteratorState &end_state, T &&callback) {
	// iterate in insertion order: start with the tail
	state.current = tail;
	while (state.current) {
		state.start = state.current->data.get();
		state.end =
		    state.current == end_state.current ? end_state.start : state.start + state.current->current_position;
		while (state.start < state.end) {
			auto type = Load<UndoFlags>(state.start);
			state.start += sizeof(UndoFlags);
			auto len = Load<uint32_t>(state.start);
			state.start += sizeof(uint32_t);
			callback(type, state.start);
			state.start += len;
		}
		if (state.current == end_state.current) {
			// finished executing until the current end state
			return;
		}
		state.current = state.current->prev;
	}
}

template <class T>
void UndoBuffer::ReverseIterateEntries(T &&callback) {
	// iterate in reverse insertion order: start with the head
	auto current = head.get();
	while (current) {
		data_ptr_t start = current->data.get();
		data_ptr_t end = start + current->current_position;
		// create a vector with all nodes in this chunk
		vector<pair<UndoFlags, data_ptr_t>> nodes;
		while (start < end) {
			auto type = Load<UndoFlags>(start);
			start += sizeof(UndoFlags);
			auto len = Load<uint32_t>(start);
			start += sizeof(uint32_t);
			nodes.emplace_back(type, start);
			start += len;
		}
		// iterate over it in reverse order
		for (idx_t i = nodes.size(); i > 0; i--) {
			callback(nodes[i - 1].first, nodes[i - 1].second);
		}
		current = current->next.get();
	}
}

bool UndoBuffer::ChangesMade() {
	return head->maximum_size > 0;
}

idx_t UndoBuffer::EstimatedSize() {
	idx_t estimated_size = 0;
	auto node = head.get();
	while (node) {
		estimated_size += node->current_position;
		node = node->next.get();
	}
	return estimated_size;
}

void UndoBuffer::Cleanup() {
	// garbage collect everything in the Undo Chunk
	// this should only happen if
	//  (1) the transaction this UndoBuffer belongs to has successfully
	//  committed
	//      (on Rollback the Rollback() function should be called, that clears
	//      the chunks)
	//  (2) there is no active transaction with start_id < commit_id of this
	//  transaction
	CleanupState state;
	UndoBuffer::IteratorState iterator_state;
	IterateEntries(iterator_state, [&](UndoFlags type, data_ptr_t data) { state.CleanupEntry(type, data); });
}

void UndoBuffer::Commit(UndoBuffer::IteratorState &iterator_state, WriteAheadLog *log, transaction_t commit_id) {
	CommitState state(commit_id, log);
	if (log) {
		// commit WITH write ahead log
		IterateEntries(iterator_state, [&](UndoFlags type, data_ptr_t data) { state.CommitEntry<true>(type, data); });
	} else {
		// commit WITHOUT write ahead log
		IterateEntries(iterator_state, [&](UndoFlags type, data_ptr_t data) { state.CommitEntry<false>(type, data); });
	}
}

void UndoBuffer::RevertCommit(UndoBuffer::IteratorState &end_state, transaction_t transaction_id) {
	CommitState state(transaction_id, nullptr);
	UndoBuffer::IteratorState start_state;
	IterateEntries(start_state, end_state, [&](UndoFlags type, data_ptr_t data) { state.RevertCommit(type, data); });
}

void UndoBuffer::Rollback() noexcept {
	// rollback needs to be performed in reverse
	RollbackState state;
	ReverseIterateEntries([&](UndoFlags type, data_ptr_t data) { state.RollbackEntry(type, data); });
}
} // namespace duckdb
