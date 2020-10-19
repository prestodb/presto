/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.sql.analyzer;

import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.PrestoWarning;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.sql.planner.ParameterRewriter;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.facebook.presto.sql.tree.ArithmeticBinaryExpression;
import com.facebook.presto.sql.tree.ArithmeticUnaryExpression;
import com.facebook.presto.sql.tree.ArrayConstructor;
import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.AtTimeZone;
import com.facebook.presto.sql.tree.BetweenPredicate;
import com.facebook.presto.sql.tree.BindExpression;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.CoalesceExpression;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.CurrentTime;
import com.facebook.presto.sql.tree.DereferenceExpression;
import com.facebook.presto.sql.tree.ExistsPredicate;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.ExpressionTreeRewriter;
import com.facebook.presto.sql.tree.Extract;
import com.facebook.presto.sql.tree.FieldReference;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.GroupingOperation;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.IfExpression;
import com.facebook.presto.sql.tree.InListExpression;
import com.facebook.presto.sql.tree.InPredicate;
import com.facebook.presto.sql.tree.IsNotNullPredicate;
import com.facebook.presto.sql.tree.IsNullPredicate;
import com.facebook.presto.sql.tree.LambdaExpression;
import com.facebook.presto.sql.tree.LikePredicate;
import com.facebook.presto.sql.tree.Literal;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.NodeRef;
import com.facebook.presto.sql.tree.NotExpression;
import com.facebook.presto.sql.tree.NullIfExpression;
import com.facebook.presto.sql.tree.Parameter;
import com.facebook.presto.sql.tree.Row;
import com.facebook.presto.sql.tree.SearchedCaseExpression;
import com.facebook.presto.sql.tree.SimpleCaseExpression;
import com.facebook.presto.sql.tree.SortItem;
import com.facebook.presto.sql.tree.SubqueryExpression;
import com.facebook.presto.sql.tree.SubscriptExpression;
import com.facebook.presto.sql.tree.TryExpression;
import com.facebook.presto.sql.tree.WhenClause;
import com.facebook.presto.sql.tree.Window;
import com.facebook.presto.sql.tree.WindowFrame;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.spi.StandardWarningCode.PERFORMANCE_WARNING;
import static com.facebook.presto.spi.function.FunctionKind.AGGREGATE;
import static com.facebook.presto.sql.NodeUtils.getSortItemsFromOrderBy;
import static com.facebook.presto.sql.analyzer.ExpressionTreeUtils.checkAndGetColumnReferenceField;
import static com.facebook.presto.sql.analyzer.ExpressionTreeUtils.extractAggregateFunctions;
import static com.facebook.presto.sql.analyzer.ExpressionTreeUtils.extractWindowFunctions;
import static com.facebook.presto.sql.analyzer.FreeLambdaReferenceExtractor.hasFreeReferencesToLambdaArgument;
import static com.facebook.presto.sql.analyzer.ScopeReferenceExtractor.getReferencesToScope;
import static com.facebook.presto.sql.analyzer.ScopeReferenceExtractor.hasReferencesToScope;
import static com.facebook.presto.sql.analyzer.ScopeReferenceExtractor.isFieldFromScope;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.INVALID_PROCEDURE_ARGUMENTS;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MUST_BE_AGGREGATE_OR_GROUP_BY;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MUST_BE_AGGREGATION_FUNCTION;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.NESTED_AGGREGATION;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.NESTED_WINDOW;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.ORDER_BY_MUST_BE_IN_AGGREGATE;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.REFERENCE_TO_OUTPUT_ATTRIBUTE_WITHIN_ORDER_BY_AGGREGATION;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.REFERENCE_TO_OUTPUT_ATTRIBUTE_WITHIN_ORDER_BY_GROUPING;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;

/**
 * Checks whether an expression is constant with respect to the group
 */
class AggregationAnalyzer
{
    // fields and expressions in the group by clause
    private final Set<FieldId> groupingFields;
    private final List<Expression> expressions;
    private final Multimap<NodeRef<Expression>, FieldId> columnReferences;

    private final Metadata metadata;
    private final Analysis analysis;

    private final Scope sourceScope;
    private final Optional<Scope> orderByScope;
    private final WarningCollector warningCollector;
    private final FunctionResolution functionResolution;

    public static void verifySourceAggregations(
            List<Expression> groupByExpressions,
            Scope sourceScope,
            Expression expression,
            Metadata metadata,
            Analysis analysis,
            WarningCollector warningCollector)
    {
        AggregationAnalyzer analyzer = new AggregationAnalyzer(groupByExpressions, sourceScope, Optional.empty(), metadata, analysis, warningCollector);
        analyzer.analyze(expression);
    }

    public static void verifyOrderByAggregations(
            List<Expression> groupByExpressions,
            Scope sourceScope,
            Scope orderByScope,
            Expression expression,
            Metadata metadata,
            Analysis analysis,
            WarningCollector warningCollector)
    {
        AggregationAnalyzer analyzer = new AggregationAnalyzer(groupByExpressions, sourceScope, Optional.of(orderByScope), metadata, analysis, warningCollector);
        analyzer.analyze(expression);
    }

    private AggregationAnalyzer(List<Expression> groupByExpressions, Scope sourceScope, Optional<Scope> orderByScope, Metadata metadata, Analysis analysis, WarningCollector warningCollector)
    {
        requireNonNull(groupByExpressions, "groupByExpressions is null");
        requireNonNull(sourceScope, "sourceScope is null");
        requireNonNull(orderByScope, "orderByScope is null");
        requireNonNull(metadata, "metadata is null");
        requireNonNull(analysis, "analysis is null");
        requireNonNull(warningCollector, "warningCollector is null");

        this.sourceScope = sourceScope;
        this.warningCollector = warningCollector;
        this.orderByScope = orderByScope;
        this.metadata = metadata;
        this.analysis = analysis;
        this.functionResolution = new FunctionResolution(metadata.getFunctionAndTypeManager());
        this.expressions = groupByExpressions.stream()
                .map(e -> ExpressionTreeRewriter.rewriteWith(new ParameterRewriter(analysis.getParameters()), e))
                .collect(toImmutableList());

        this.columnReferences = analysis.getColumnReferenceFields();

        this.groupingFields = groupByExpressions.stream()
                .map(NodeRef::of)
                .filter(columnReferences::containsKey)
                .map(columnReferences::get)
                .flatMap(Collection::stream)
                .collect(toImmutableSet());

        this.groupingFields.forEach(fieldId -> {
            checkState(isFieldFromScope(fieldId, sourceScope),
                    "Grouping field %s should originate from %s", fieldId, sourceScope.getRelationType());
        });
    }

    private void analyze(Expression expression)
    {
        Visitor visitor = new Visitor();
        if (!visitor.process(expression, null)) {
            throw new SemanticException(MUST_BE_AGGREGATE_OR_GROUP_BY, expression, "'%s' must be an aggregate expression or appear in GROUP BY clause", expression);
        }
    }

    /**
     * visitor returns true if all expressions are constant with respect to the group.
     */
    private class Visitor
            extends AstVisitor<Boolean, Void>
    {
        @Override
        protected Boolean visitExpression(Expression node, Void context)
        {
            throw new UnsupportedOperationException("aggregation analysis not yet implemented for: " + node.getClass().getName());
        }

        @Override
        protected Boolean visitAtTimeZone(AtTimeZone node, Void context)
        {
            return process(node.getValue(), context);
        }

        @Override
        protected Boolean visitSubqueryExpression(SubqueryExpression node, Void context)
        {
            /*
             * Column reference can resolve to (a) some subquery's scope, (b) a projection (ORDER BY scope),
             * (c) source scope or (d) outer query scope (effectively a constant).
             * From AggregationAnalyzer's perspective, only case (c) needs verification.
             */
            getReferencesToScope(node, analysis, sourceScope)
                    .filter(expression -> !isGroupingKey(expression))
                    .findFirst()
                    .ifPresent(expression -> {
                        throw new SemanticException(MUST_BE_AGGREGATE_OR_GROUP_BY, expression,
                                "Subquery uses '%s' which must appear in GROUP BY clause", expression);
                    });

            return true;
        }

        @Override
        protected Boolean visitExists(ExistsPredicate node, Void context)
        {
            checkState(node.getSubquery() instanceof SubqueryExpression);
            return process(node.getSubquery(), context);
        }

        @Override
        protected Boolean visitSubscriptExpression(SubscriptExpression node, Void context)
        {
            return process(node.getBase(), context) &&
                    process(node.getIndex(), context);
        }

        @Override
        protected Boolean visitArrayConstructor(ArrayConstructor node, Void context)
        {
            return node.getValues().stream().allMatch(expression -> process(expression, context));
        }

        @Override
        protected Boolean visitCast(Cast node, Void context)
        {
            return process(node.getExpression(), context);
        }

        @Override
        protected Boolean visitCoalesceExpression(CoalesceExpression node, Void context)
        {
            return node.getOperands().stream().allMatch(expression -> process(expression, context));
        }

        @Override
        protected Boolean visitNullIfExpression(NullIfExpression node, Void context)
        {
            return process(node.getFirst(), context) && process(node.getSecond(), context);
        }

        @Override
        protected Boolean visitExtract(Extract node, Void context)
        {
            return process(node.getExpression(), context);
        }

        @Override
        protected Boolean visitBetweenPredicate(BetweenPredicate node, Void context)
        {
            return process(node.getMin(), context) &&
                    process(node.getValue(), context) &&
                    process(node.getMax(), context);
        }

        @Override
        protected Boolean visitCurrentTime(CurrentTime node, Void context)
        {
            return true;
        }

        @Override
        protected Boolean visitArithmeticBinary(ArithmeticBinaryExpression node, Void context)
        {
            return process(node.getLeft(), context) && process(node.getRight(), context);
        }

        @Override
        protected Boolean visitComparisonExpression(ComparisonExpression node, Void context)
        {
            return process(node.getLeft(), context) && process(node.getRight(), context);
        }

        @Override
        protected Boolean visitLiteral(Literal node, Void context)
        {
            return true;
        }

        @Override
        protected Boolean visitIsNotNullPredicate(IsNotNullPredicate node, Void context)
        {
            return process(node.getValue(), context);
        }

        @Override
        protected Boolean visitIsNullPredicate(IsNullPredicate node, Void context)
        {
            return process(node.getValue(), context);
        }

        @Override
        protected Boolean visitLikePredicate(LikePredicate node, Void context)
        {
            return process(node.getValue(), context) && process(node.getPattern(), context);
        }

        @Override
        protected Boolean visitInListExpression(InListExpression node, Void context)
        {
            return node.getValues().stream().allMatch(expression -> process(expression, context));
        }

        @Override
        protected Boolean visitInPredicate(InPredicate node, Void context)
        {
            return process(node.getValue(), context) && process(node.getValueList(), context);
        }

        @Override
        protected Boolean visitFunctionCall(FunctionCall node, Void context)
        {
            if (metadata.getFunctionAndTypeManager().getFunctionMetadata(analysis.getFunctionHandle(node)).getFunctionKind() == AGGREGATE) {
                if (functionResolution.isCountFunction(analysis.getFunctionHandle(node)) && node.isDistinct()) {
                    warningCollector.add(new PrestoWarning(
                            PERFORMANCE_WARNING,
                            "COUNT(DISTINCT xxx) can be a very expensive operation when the cardinality is high for xxx. In most scenarios, using approx_distinct instead would be enough"));
                }
                if (!node.getWindow().isPresent()) {
                    List<FunctionCall> aggregateFunctions = extractAggregateFunctions(analysis.getFunctionHandles(), node.getArguments(), metadata.getFunctionAndTypeManager());
                    List<FunctionCall> windowFunctions = extractWindowFunctions(node.getArguments());

                    if (!aggregateFunctions.isEmpty()) {
                        throw new SemanticException(NESTED_AGGREGATION,
                                node,
                                "Cannot nest aggregations inside aggregation '%s': %s",
                                node.getName(),
                                aggregateFunctions);
                    }

                    if (!windowFunctions.isEmpty()) {
                        throw new SemanticException(NESTED_WINDOW,
                                node,
                                "Cannot nest window functions inside aggregation '%s': %s",
                                node.getName(),
                                windowFunctions);
                    }

                    if (node.getOrderBy().isPresent()) {
                        List<Expression> sortKeys = node.getOrderBy().get().getSortItems().stream()
                                .map(SortItem::getSortKey)
                                .collect(toImmutableList());
                        if (node.isDistinct()) {
                            List<FieldId> fieldIds = node.getArguments().stream()
                                    .map(NodeRef::of)
                                    .map(columnReferences::get)
                                    .filter(Objects::nonNull)
                                    .flatMap(Collection::stream)
                                    .collect(toImmutableList());
                            for (Expression sortKey : sortKeys) {
                                if (!node.getArguments().contains(sortKey)
                                        && !(columnReferences.containsKey(NodeRef.of(sortKey)) && fieldIds.containsAll(columnReferences.get(NodeRef.of(sortKey))))) {
                                    throw new SemanticException(
                                            ORDER_BY_MUST_BE_IN_AGGREGATE,
                                            sortKey,
                                            "For aggregate function with DISTINCT, ORDER BY expressions must appear in arguments");
                                }
                            }
                        }
                        // ensure that no output fields are referenced from ORDER BY clause
                        if (orderByScope.isPresent()) {
                            for (Expression sortKey : sortKeys) {
                                verifyNoOrderByReferencesToOutputColumns(
                                        sortKey,
                                        REFERENCE_TO_OUTPUT_ATTRIBUTE_WITHIN_ORDER_BY_AGGREGATION,
                                        "ORDER BY clause in aggregation function must not reference query output columns");
                            }
                        }
                    }

                    // ensure that no output fields are referenced from ORDER BY clause
                    if (orderByScope.isPresent()) {
                        node.getArguments().stream()
                                .forEach(argument -> verifyNoOrderByReferencesToOutputColumns(
                                        argument,
                                        REFERENCE_TO_OUTPUT_ATTRIBUTE_WITHIN_ORDER_BY_AGGREGATION,
                                        "Invalid reference to output projection attribute from ORDER BY aggregation"));
                    }

                    return true;
                }
            }
            else {
                if (node.getFilter().isPresent()) {
                    throw new SemanticException(MUST_BE_AGGREGATION_FUNCTION,
                            node,
                            "Filter is only valid for aggregation functions",
                            node);
                }
                if (node.getOrderBy().isPresent()) {
                    throw new SemanticException(MUST_BE_AGGREGATION_FUNCTION, node, "ORDER BY is only valid for aggregation functions");
                }
            }

            if (node.getWindow().isPresent() && !process(node.getWindow().get(), context)) {
                return false;
            }

            return node.getArguments().stream().allMatch(expression -> process(expression, context));
        }

        @Override
        protected Boolean visitLambdaExpression(LambdaExpression node, Void context)
        {
            return process(node.getBody(), context);
        }

        @Override
        protected Boolean visitBindExpression(BindExpression node, Void context)
        {
            for (Expression value : node.getValues()) {
                if (!process(value, context)) {
                    return false;
                }
            }
            return process(node.getFunction(), context);
        }

        @Override
        public Boolean visitWindow(Window node, Void context)
        {
            for (Expression expression : node.getPartitionBy()) {
                if (!process(expression, context)) {
                    throw new SemanticException(MUST_BE_AGGREGATE_OR_GROUP_BY,
                            expression,
                            "PARTITION BY expression '%s' must be an aggregate expression or appear in GROUP BY clause",
                            expression);
                }
            }

            for (SortItem sortItem : getSortItemsFromOrderBy(node.getOrderBy())) {
                Expression expression = sortItem.getSortKey();
                if (!process(expression, context)) {
                    throw new SemanticException(MUST_BE_AGGREGATE_OR_GROUP_BY,
                            expression,
                            "ORDER BY expression '%s' must be an aggregate expression or appear in GROUP BY clause",
                            expression);
                }
            }

            if (node.getFrame().isPresent()) {
                process(node.getFrame().get(), context);
            }

            return true;
        }

        @Override
        public Boolean visitWindowFrame(WindowFrame node, Void context)
        {
            Optional<Expression> start = node.getStart().getValue();
            if (start.isPresent()) {
                if (!process(start.get(), context)) {
                    throw new SemanticException(MUST_BE_AGGREGATE_OR_GROUP_BY, start.get(), "Window frame start must be an aggregate expression or appear in GROUP BY clause");
                }
            }
            if (node.getEnd().isPresent() && node.getEnd().get().getValue().isPresent()) {
                Expression endValue = node.getEnd().get().getValue().get();
                if (!process(endValue, context)) {
                    throw new SemanticException(MUST_BE_AGGREGATE_OR_GROUP_BY, endValue, "Window frame end must be an aggregate expression or appear in GROUP BY clause");
                }
            }

            return true;
        }

        @Override
        protected Boolean visitIdentifier(Identifier node, Void context)
        {
            if (analysis.getLambdaArgumentReferences().containsKey(NodeRef.of(node))) {
                return true;
            }
            return isGroupingKey(node);
        }

        @Override
        protected Boolean visitDereferenceExpression(DereferenceExpression node, Void context)
        {
            if (columnReferences.containsKey(NodeRef.<Expression>of(node))) {
                return isGroupingKey(node);
            }

            // Allow SELECT col1.f1 FROM table1 GROUP BY col1
            return process(node.getBase(), context);
        }

        private boolean isGroupingKey(Expression node)
        {
            FieldId fieldId = checkAndGetColumnReferenceField(node, columnReferences);

            if (orderByScope.isPresent() && isFieldFromScope(fieldId, orderByScope.get())) {
                return true;
            }

            return groupingFields.contains(fieldId);
        }

        @Override
        protected Boolean visitFieldReference(FieldReference node, Void context)
        {
            if (orderByScope.isPresent()) {
                return true;
            }

            FieldId fieldId = checkAndGetColumnReferenceField(node, columnReferences);
            boolean inGroup = groupingFields.contains(fieldId);
            if (!inGroup) {
                Field field = sourceScope.getRelationType().getFieldByIndex(node.getFieldIndex());

                String column;
                if (!field.getName().isPresent()) {
                    column = Integer.toString(node.getFieldIndex() + 1);
                }
                else if (field.getRelationAlias().isPresent()) {
                    column = String.format("'%s.%s'", field.getRelationAlias().get(), field.getName().get());
                }
                else {
                    column = "'" + field.getName().get() + "'";
                }

                throw new SemanticException(MUST_BE_AGGREGATE_OR_GROUP_BY, node, "Column %s not in GROUP BY clause", column);
            }
            return inGroup;
        }

        @Override
        protected Boolean visitArithmeticUnary(ArithmeticUnaryExpression node, Void context)
        {
            return process(node.getValue(), context);
        }

        @Override
        protected Boolean visitNotExpression(NotExpression node, Void context)
        {
            return process(node.getValue(), context);
        }

        @Override
        protected Boolean visitLogicalBinaryExpression(LogicalBinaryExpression node, Void context)
        {
            return process(node.getLeft(), context) && process(node.getRight(), context);
        }

        @Override
        protected Boolean visitIfExpression(IfExpression node, Void context)
        {
            ImmutableList.Builder<Expression> expressions = ImmutableList.<Expression>builder()
                    .add(node.getCondition())
                    .add(node.getTrueValue());

            if (node.getFalseValue().isPresent()) {
                expressions.add(node.getFalseValue().get());
            }

            return expressions.build().stream().allMatch(expression -> process(expression, context));
        }

        @Override
        protected Boolean visitSimpleCaseExpression(SimpleCaseExpression node, Void context)
        {
            if (!process(node.getOperand(), context)) {
                return false;
            }

            for (WhenClause whenClause : node.getWhenClauses()) {
                if (!process(whenClause.getOperand(), context) || !process(whenClause.getResult(), context)) {
                    return false;
                }
            }

            if (node.getDefaultValue().isPresent() && !process(node.getDefaultValue().get(), context)) {
                return false;
            }

            return true;
        }

        @Override
        protected Boolean visitSearchedCaseExpression(SearchedCaseExpression node, Void context)
        {
            for (WhenClause whenClause : node.getWhenClauses()) {
                if (!process(whenClause.getOperand(), context) || !process(whenClause.getResult(), context)) {
                    return false;
                }
            }

            return !node.getDefaultValue().isPresent() || process(node.getDefaultValue().get(), context);
        }

        @Override
        protected Boolean visitTryExpression(TryExpression node, Void context)
        {
            return process(node.getInnerExpression(), context);
        }

        @Override
        public Boolean visitRow(Row node, final Void context)
        {
            return node.getItems().stream()
                    .allMatch(item -> process(item, context));
        }

        @Override
        public Boolean visitParameter(Parameter node, Void context)
        {
            if (analysis.isDescribe()) {
                return true;
            }
            List<Expression> parameters = analysis.getParameters();
            checkArgument(node.getPosition() < parameters.size(), "Invalid parameter number %s, max values is %s", node.getPosition(), parameters.size() - 1);
            return process(parameters.get(node.getPosition()), context);
        }

        public Boolean visitGroupingOperation(GroupingOperation node, Void context)
        {
            // ensure that no output fields are referenced from ORDER BY clause
            if (orderByScope.isPresent()) {
                node.getGroupingColumns().forEach(groupingColumn -> verifyNoOrderByReferencesToOutputColumns(
                        groupingColumn,
                        REFERENCE_TO_OUTPUT_ATTRIBUTE_WITHIN_ORDER_BY_GROUPING,
                        "Invalid reference to output of SELECT clause from grouping() expression in ORDER BY"));
            }

            Optional<Expression> argumentNotInGroupBy = node.getGroupingColumns().stream()
                    .filter(argument -> !columnReferences.containsKey(NodeRef.of(argument)) || !isGroupingKey(argument))
                    .findAny();
            if (argumentNotInGroupBy.isPresent()) {
                throw new SemanticException(
                        INVALID_PROCEDURE_ARGUMENTS,
                        node,
                        "The arguments to GROUPING() must be expressions referenced by the GROUP BY at the associated query level. Mismatch due to %s.",
                        argumentNotInGroupBy.get());
            }
            return true;
        }

        @Override
        public Boolean process(Node node, @Nullable Void context)
        {
            if (expressions.stream().anyMatch(node::equals)
                    && (!orderByScope.isPresent() || !hasOrderByReferencesToOutputColumns(node))
                    && !hasFreeReferencesToLambdaArgument(node, analysis)) {
                return true;
            }

            return super.process(node, context);
        }
    }

    private boolean hasOrderByReferencesToOutputColumns(Node node)
    {
        return hasReferencesToScope(node, analysis, orderByScope.get());
    }

    private void verifyNoOrderByReferencesToOutputColumns(Node node, SemanticErrorCode errorCode, String errorString)
    {
        getReferencesToScope(node, analysis, orderByScope.get())
                .findFirst()
                .ifPresent(expression -> {
                    throw new SemanticException(errorCode, expression, errorString);
                });
    }
}
