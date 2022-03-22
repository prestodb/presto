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
package com.facebook.presto.cost;

import com.facebook.presto.Session;
import com.facebook.presto.common.function.OperatorType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.function.FunctionMetadata;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.InputReferenceExpression;
import com.facebook.presto.spi.relation.LambdaDefinitionExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.RowExpressionVisitor;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.analyzer.ExpressionAnalyzer;
import com.facebook.presto.sql.analyzer.Scope;
import com.facebook.presto.sql.planner.ExpressionInterpreter;
import com.facebook.presto.sql.planner.LiteralEncoder;
import com.facebook.presto.sql.planner.LiteralInterpreter;
import com.facebook.presto.sql.planner.NoOpVariableResolver;
import com.facebook.presto.sql.planner.RowExpressionInterpreter;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.BetweenPredicate;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.InListExpression;
import com.facebook.presto.sql.tree.InPredicate;
import com.facebook.presto.sql.tree.IsNotNullPredicate;
import com.facebook.presto.sql.tree.IsNullPredicate;
import com.facebook.presto.sql.tree.Literal;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.NodeRef;
import com.facebook.presto.sql.tree.NotExpression;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nullable;
import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;

import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.cost.ComparisonStatsCalculator.estimateExpressionToExpressionComparison;
import static com.facebook.presto.cost.ComparisonStatsCalculator.estimateExpressionToLiteralComparison;
import static com.facebook.presto.cost.PlanNodeStatsEstimateMath.addStatsAndSumDistinctValues;
import static com.facebook.presto.cost.PlanNodeStatsEstimateMath.capStats;
import static com.facebook.presto.cost.PlanNodeStatsEstimateMath.subtractSubsetStats;
import static com.facebook.presto.cost.StatsUtil.toStatsRepresentation;
import static com.facebook.presto.expressions.DynamicFilters.isDynamicFilter;
import static com.facebook.presto.expressions.LogicalRowExpressions.TRUE_CONSTANT;
import static com.facebook.presto.expressions.LogicalRowExpressions.and;
import static com.facebook.presto.spi.relation.ExpressionOptimizer.Level.OPTIMIZED;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.IS_NULL;
import static com.facebook.presto.sql.ExpressionUtils.and;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.sql.relational.Expressions.call;
import static com.facebook.presto.sql.relational.Expressions.constantNull;
import static com.facebook.presto.sql.tree.BooleanLiteral.FALSE_LITERAL;
import static com.facebook.presto.sql.tree.ComparisonExpression.Operator.EQUAL;
import static com.facebook.presto.sql.tree.ComparisonExpression.Operator.GREATER_THAN;
import static com.facebook.presto.sql.tree.ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL;
import static com.facebook.presto.sql.tree.ComparisonExpression.Operator.IS_DISTINCT_FROM;
import static com.facebook.presto.sql.tree.ComparisonExpression.Operator.LESS_THAN;
import static com.facebook.presto.sql.tree.ComparisonExpression.Operator.LESS_THAN_OR_EQUAL;
import static com.facebook.presto.sql.tree.ComparisonExpression.Operator.NOT_EQUAL;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.Double.NaN;
import static java.lang.Double.isInfinite;
import static java.lang.Double.isNaN;
import static java.lang.Double.min;
import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;

public class FilterStatsCalculator
{
    static final double UNKNOWN_FILTER_COEFFICIENT = 0.9;

    private final Metadata metadata;
    private final ScalarStatsCalculator scalarStatsCalculator;
    private final StatsNormalizer normalizer;
    private final LiteralEncoder literalEncoder;
    private final FunctionResolution functionResolution;

    @Inject
    public FilterStatsCalculator(Metadata metadata, ScalarStatsCalculator scalarStatsCalculator, StatsNormalizer normalizer)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.scalarStatsCalculator = requireNonNull(scalarStatsCalculator, "scalarStatsCalculator is null");
        this.normalizer = requireNonNull(normalizer, "normalizer is null");
        this.literalEncoder = new LiteralEncoder(metadata.getBlockEncodingSerde());
        this.functionResolution = new FunctionResolution(metadata.getFunctionAndTypeManager());
    }

    @Deprecated
    public PlanNodeStatsEstimate filterStats(
            PlanNodeStatsEstimate statsEstimate,
            Expression predicate,
            Session session,
            TypeProvider types)
    {
        Expression simplifiedExpression = simplifyExpression(session, predicate, types);
        return new FilterExpressionStatsCalculatingVisitor(statsEstimate, session, types)
                .process(simplifiedExpression);
    }

    public PlanNodeStatsEstimate filterStats(
            PlanNodeStatsEstimate statsEstimate,
            RowExpression predicate,
            ConnectorSession session)
    {
        RowExpression simplifiedExpression = simplifyExpression(session, predicate);
        return new FilterRowExpressionStatsCalculatingVisitor(statsEstimate, session, metadata.getFunctionAndTypeManager()).process(simplifiedExpression);
    }

    public PlanNodeStatsEstimate filterStats(
            PlanNodeStatsEstimate statsEstimate,
            RowExpression predicate,
            Session session)
    {
        return filterStats(statsEstimate, predicate, session.toConnectorSession());
    }

    private Expression simplifyExpression(Session session, Expression predicate, TypeProvider types)
    {
        // TODO reuse com.facebook.presto.sql.planner.iterative.rule.SimplifyExpressions.rewrite

        Map<NodeRef<Expression>, Type> expressionTypes = getExpressionTypes(session, predicate, types);
        ExpressionInterpreter interpreter = ExpressionInterpreter.expressionOptimizer(predicate, metadata, session, expressionTypes);
        Object value = interpreter.optimize(NoOpVariableResolver.INSTANCE);

        if (value == null) {
            // Expression evaluates to SQL null, which in Filter is equivalent to false. This assumes the expression is a top-level expression (eg. not in NOT).
            value = false;
        }
        return literalEncoder.toExpression(value, BOOLEAN);
    }

    private RowExpression simplifyExpression(ConnectorSession session, RowExpression predicate)
    {
        RowExpressionInterpreter interpreter = new RowExpressionInterpreter(predicate, metadata, session, OPTIMIZED);
        Object value = interpreter.optimize();

        if (value == null) {
            // Expression evaluates to SQL null, which in Filter is equivalent to false. This assumes the expression is a top-level expression (eg. not in NOT).
            value = false;
        }
        return LiteralEncoder.toRowExpression(value, BOOLEAN);
    }

    private Map<NodeRef<Expression>, Type> getExpressionTypes(Session session, Expression expression, TypeProvider types)
    {
        ExpressionAnalyzer expressionAnalyzer = ExpressionAnalyzer.createWithoutSubqueries(
                metadata.getFunctionAndTypeManager(),
                session,
                types,
                emptyList(),
                node -> new IllegalStateException("Unexpected node: %s" + node),
                WarningCollector.NOOP,
                false);
        expressionAnalyzer.analyze(expression, Scope.create());
        return expressionAnalyzer.getExpressionTypes();
    }

    private class FilterExpressionStatsCalculatingVisitor
            extends AstVisitor<PlanNodeStatsEstimate, Void>
    {
        private final PlanNodeStatsEstimate input;
        private final Session session;
        private final TypeProvider types;

        FilterExpressionStatsCalculatingVisitor(PlanNodeStatsEstimate input, Session session, TypeProvider types)
        {
            this.input = input;
            this.session = session;
            this.types = types;
        }

        @Override
        public PlanNodeStatsEstimate process(Node node, @Nullable Void context)
        {
            return normalizer.normalize(super.process(node, context));
        }

        @Override
        protected PlanNodeStatsEstimate visitExpression(Expression node, Void context)
        {
            return PlanNodeStatsEstimate.unknown();
        }

        @Override
        protected PlanNodeStatsEstimate visitNotExpression(NotExpression node, Void context)
        {
            if (node.getValue() instanceof IsNullPredicate) {
                return process(new IsNotNullPredicate(((IsNullPredicate) node.getValue()).getValue()));
            }
            return subtractSubsetStats(input, process(node.getValue()));
        }

        @Override
        protected PlanNodeStatsEstimate visitLogicalBinaryExpression(LogicalBinaryExpression node, Void context)
        {
            switch (node.getOperator()) {
                case AND:
                    return estimateLogicalAnd(node.getLeft(), node.getRight());
                case OR:
                    return estimateLogicalOr(node.getLeft(), node.getRight());
                default:
                    throw new IllegalArgumentException("Unexpected binary operator: " + node.getOperator());
            }
        }

        private PlanNodeStatsEstimate estimateLogicalAnd(Expression left, Expression right)
        {
            // first try to estimate in the fair way
            PlanNodeStatsEstimate leftEstimate = process(left);
            if (!leftEstimate.isOutputRowCountUnknown()) {
                PlanNodeStatsEstimate logicalAndEstimate = new FilterExpressionStatsCalculatingVisitor(leftEstimate, session, types).process(right);
                if (!logicalAndEstimate.isOutputRowCountUnknown()) {
                    return logicalAndEstimate;
                }
            }

            // If some of the filters cannot be estimated, take the smallest estimate.
            // Apply 0.9 filter factor as "unknown filter" factor.
            PlanNodeStatsEstimate rightEstimate = process(right);
            PlanNodeStatsEstimate smallestKnownEstimate;
            if (leftEstimate.isOutputRowCountUnknown()) {
                smallestKnownEstimate = rightEstimate;
            }
            else if (rightEstimate.isOutputRowCountUnknown()) {
                smallestKnownEstimate = leftEstimate;
            }
            else {
                smallestKnownEstimate = leftEstimate.getOutputRowCount() <= rightEstimate.getOutputRowCount() ? leftEstimate : rightEstimate;
            }
            if (smallestKnownEstimate.isOutputRowCountUnknown()) {
                return PlanNodeStatsEstimate.unknown();
            }
            return smallestKnownEstimate.mapOutputRowCount(rowCount -> rowCount * UNKNOWN_FILTER_COEFFICIENT);
        }

        private PlanNodeStatsEstimate estimateLogicalOr(Expression left, Expression right)
        {
            PlanNodeStatsEstimate leftEstimate = process(left);
            if (leftEstimate.isOutputRowCountUnknown()) {
                return PlanNodeStatsEstimate.unknown();
            }

            PlanNodeStatsEstimate rightEstimate = process(right);
            if (rightEstimate.isOutputRowCountUnknown()) {
                return PlanNodeStatsEstimate.unknown();
            }

            PlanNodeStatsEstimate andEstimate = new FilterExpressionStatsCalculatingVisitor(leftEstimate, session, types).process(right);
            if (andEstimate.isOutputRowCountUnknown()) {
                return PlanNodeStatsEstimate.unknown();
            }

            return capStats(
                    subtractSubsetStats(
                            addStatsAndSumDistinctValues(leftEstimate, rightEstimate),
                            andEstimate),
                    input);
        }

        @Override
        protected PlanNodeStatsEstimate visitBooleanLiteral(BooleanLiteral node, Void context)
        {
            if (node.getValue()) {
                return input;
            }

            PlanNodeStatsEstimate.Builder result = PlanNodeStatsEstimate.builder();
            result.setOutputRowCount(0.0);
            input.getVariablesWithKnownStatistics().forEach(variable -> result.addVariableStatistics(variable, VariableStatsEstimate.zero()));
            return result.build();
        }

        @Override
        protected PlanNodeStatsEstimate visitIsNotNullPredicate(IsNotNullPredicate node, Void context)
        {
            if (node.getValue() instanceof SymbolReference) {
                VariableReferenceExpression variable = toVariable(node.getValue());
                VariableStatsEstimate variableStats = input.getVariableStatistics(variable);
                PlanNodeStatsEstimate.Builder result = PlanNodeStatsEstimate.buildFrom(input);
                result.setOutputRowCount(input.getOutputRowCount() * (1 - variableStats.getNullsFraction()));
                result.addVariableStatistics(variable, variableStats.mapNullsFraction(x -> 0.0));
                return result.build();
            }
            return PlanNodeStatsEstimate.unknown();
        }

        @Override
        protected PlanNodeStatsEstimate visitIsNullPredicate(IsNullPredicate node, Void context)
        {
            if (node.getValue() instanceof SymbolReference) {
                VariableReferenceExpression variable = toVariable(node.getValue());
                VariableStatsEstimate variableStats = input.getVariableStatistics(variable);
                PlanNodeStatsEstimate.Builder result = PlanNodeStatsEstimate.buildFrom(input);
                result.setOutputRowCount(input.getOutputRowCount() * variableStats.getNullsFraction());
                result.addVariableStatistics(variable, VariableStatsEstimate.builder()
                        .setNullsFraction(1.0)
                        .setLowValue(NaN)
                        .setHighValue(NaN)
                        .setDistinctValuesCount(0.0)
                        .build());
                return result.build();
            }
            return PlanNodeStatsEstimate.unknown();
        }

        @Override
        protected PlanNodeStatsEstimate visitBetweenPredicate(BetweenPredicate node, Void context)
        {
            if (!(node.getValue() instanceof SymbolReference)) {
                return PlanNodeStatsEstimate.unknown();
            }
            if (!getExpressionStats(node.getMin()).isSingleValue()) {
                return PlanNodeStatsEstimate.unknown();
            }
            if (!getExpressionStats(node.getMax()).isSingleValue()) {
                return PlanNodeStatsEstimate.unknown();
            }

            VariableStatsEstimate valueStats = input.getVariableStatistics(toVariable(node.getValue()));
            Expression lowerBound = new ComparisonExpression(GREATER_THAN_OR_EQUAL, node.getValue(), node.getMin());
            Expression upperBound = new ComparisonExpression(LESS_THAN_OR_EQUAL, node.getValue(), node.getMax());

            Expression transformed;
            if (isInfinite(valueStats.getLowValue())) {
                // We want to do heuristic cut (infinite range to finite range) ASAP and then do filtering on finite range.
                // We rely on 'and()' being processed left to right
                transformed = and(lowerBound, upperBound);
            }
            else {
                transformed = and(upperBound, lowerBound);
            }
            return process(transformed);
        }

        @Override
        protected PlanNodeStatsEstimate visitInPredicate(InPredicate node, Void context)
        {
            if (!(node.getValueList() instanceof InListExpression)) {
                return PlanNodeStatsEstimate.unknown();
            }

            InListExpression inList = (InListExpression) node.getValueList();
            ImmutableList<PlanNodeStatsEstimate> equalityEstimates = inList.getValues().stream()
                    .map(inValue -> process(new ComparisonExpression(EQUAL, node.getValue(), inValue)))
                    .collect(toImmutableList());

            if (equalityEstimates.stream().anyMatch(PlanNodeStatsEstimate::isOutputRowCountUnknown)) {
                return PlanNodeStatsEstimate.unknown();
            }

            PlanNodeStatsEstimate inEstimate = equalityEstimates.stream()
                    .reduce(PlanNodeStatsEstimateMath::addStatsAndSumDistinctValues)
                    .orElse(PlanNodeStatsEstimate.unknown());

            if (inEstimate.isOutputRowCountUnknown()) {
                return PlanNodeStatsEstimate.unknown();
            }

            VariableStatsEstimate valueStats = getExpressionStats(node.getValue());
            if (valueStats.isUnknown()) {
                return PlanNodeStatsEstimate.unknown();
            }

            double notNullValuesBeforeIn = input.getOutputRowCount() * (1 - valueStats.getNullsFraction());

            PlanNodeStatsEstimate.Builder result = PlanNodeStatsEstimate.buildFrom(input);
            result.setOutputRowCount(min(inEstimate.getOutputRowCount(), notNullValuesBeforeIn));

            if (node.getValue() instanceof SymbolReference) {
                VariableReferenceExpression valueVariable = toVariable(node.getValue());
                VariableStatsEstimate newvariableStats = inEstimate.getVariableStatistics(valueVariable)
                        .mapDistinctValuesCount(newDistinctValuesCount -> min(newDistinctValuesCount, valueStats.getDistinctValuesCount()));
                result.addVariableStatistics(valueVariable, newvariableStats);
            }
            return result.build();
        }

        @Override
        protected PlanNodeStatsEstimate visitComparisonExpression(ComparisonExpression node, Void context)
        {
            ComparisonExpression.Operator operator = node.getOperator();
            Expression left = node.getLeft();
            Expression right = node.getRight();

            checkArgument(!(left instanceof Literal && right instanceof Literal), "Literal-to-literal not supported here, should be eliminated earlier");

            if (!(left instanceof SymbolReference) && right instanceof SymbolReference) {
                // normalize so that symbol is on the left
                return process(new ComparisonExpression(operator.flip(), right, left));
            }

            if (left instanceof Literal && !(right instanceof Literal)) {
                // normalize so that literal is on the right
                return process(new ComparisonExpression(operator.flip(), right, left));
            }

            if (left instanceof SymbolReference && left.equals(right)) {
                return process(new IsNotNullPredicate(left));
            }

            VariableStatsEstimate leftStats = getExpressionStats(left);
            Optional<VariableReferenceExpression> leftVariable = left instanceof SymbolReference ? Optional.of(toVariable(left)) : Optional.empty();
            if (right instanceof Literal) {
                Object literalValue = LiteralInterpreter.evaluate(metadata, session.toConnectorSession(), right);
                if (literalValue == null) {
                    return visitBooleanLiteral(FALSE_LITERAL, null);
                }
                OptionalDouble literal = toStatsRepresentation(metadata, session, getType(left), literalValue);
                return estimateExpressionToLiteralComparison(input, leftStats, leftVariable, literal, operator);
            }

            VariableStatsEstimate rightStats = getExpressionStats(right);
            if (rightStats.isSingleValue()) {
                OptionalDouble value = isNaN(rightStats.getLowValue()) ? OptionalDouble.empty() : OptionalDouble.of(rightStats.getLowValue());
                return estimateExpressionToLiteralComparison(input, leftStats, leftVariable, value, operator);
            }

            Optional<VariableReferenceExpression> rightVariable = right instanceof SymbolReference ? Optional.of(toVariable(right)) : Optional.empty();
            return estimateExpressionToExpressionComparison(input, leftStats, leftVariable, rightStats, rightVariable, operator);
        }

        private Type getType(Expression expression)
        {
            if (expression instanceof SymbolReference) {
                return requireNonNull(types.get(expression), () -> format("No type for expression %s", expression));
            }

            ExpressionAnalyzer expressionAnalyzer = ExpressionAnalyzer.createWithoutSubqueries(
                    metadata.getFunctionAndTypeManager(),
                    session,
                    types,
                    ImmutableList.of(),
                    // At this stage, there should be no subqueries in the plan.
                    node -> new VerifyException("Unexpected subquery"),
                    WarningCollector.NOOP,
                    false);
            return expressionAnalyzer.analyze(expression, Scope.create());
        }

        private VariableStatsEstimate getExpressionStats(Expression expression)
        {
            if (expression instanceof SymbolReference) {
                VariableReferenceExpression variable = toVariable(expression);
                return requireNonNull(input.getVariableStatistics(variable), () -> format("No statistics for variable %s", variable));
            }
            return scalarStatsCalculator.calculate(expression, input, session, types);
        }

        private VariableReferenceExpression toVariable(Expression expression)
        {
            checkArgument(expression instanceof SymbolReference, "Unexpected expression: %s", expression);
            return new VariableReferenceExpression(((SymbolReference) expression).getName(), types.get(expression));
        }
    }

    private class FilterRowExpressionStatsCalculatingVisitor
            implements RowExpressionVisitor<PlanNodeStatsEstimate, Void>
    {
        private final PlanNodeStatsEstimate input;
        private final ConnectorSession session;
        private final FunctionAndTypeManager functionAndTypeManager;

        FilterRowExpressionStatsCalculatingVisitor(PlanNodeStatsEstimate input, ConnectorSession session, FunctionAndTypeManager functionAndTypeManager)
        {
            this.input = requireNonNull(input, "input is null");
            this.session = requireNonNull(session, "session is null");
            this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionManager is null");
        }

        @Override
        public PlanNodeStatsEstimate visitSpecialForm(SpecialFormExpression node, Void context)
        {
            switch (node.getForm()) {
                case AND: {
                    return estimateLogicalAnd(node.getArguments().get(0), node.getArguments().get(1));
                }
                case OR: {
                    return estimateLogicalOr(node.getArguments().get(0), node.getArguments().get(1));
                }
                case IN: {
                    return estimateIn(node.getArguments().get(0), node.getArguments().subList(1, node.getArguments().size()));
                }
                case IS_NULL: {
                    return estimateIsNull(node.getArguments().get(0));
                }
                default:
                    return PlanNodeStatsEstimate.unknown();
            }
        }

        @Override
        public PlanNodeStatsEstimate visitConstant(ConstantExpression node, Void context)
        {
            if (node.getType().equals(BOOLEAN)) {
                if (node.getValue() != null && (boolean) node.getValue()) {
                    return input;
                }
                PlanNodeStatsEstimate.Builder result = PlanNodeStatsEstimate.builder();
                result.setOutputRowCount(0.0);
                input.getVariablesWithKnownStatistics().forEach(variable -> result.addVariableStatistics(variable, VariableStatsEstimate.zero()));
                return result.build();
            }
            return PlanNodeStatsEstimate.unknown();
        }

        @Override
        public PlanNodeStatsEstimate visitLambda(LambdaDefinitionExpression node, Void context)
        {
            return PlanNodeStatsEstimate.unknown();
        }

        @Override
        public PlanNodeStatsEstimate visitVariableReference(VariableReferenceExpression node, Void context)
        {
            return PlanNodeStatsEstimate.unknown();
        }

        @Override
        public PlanNodeStatsEstimate visitCall(CallExpression node, Void context)
        {
            // comparison case
            FunctionMetadata functionMetadata = metadata.getFunctionAndTypeManager().getFunctionMetadata(node.getFunctionHandle());
            if (functionMetadata.getOperatorType().map(OperatorType::isComparisonOperator).orElse(false)) {
                OperatorType operatorType = functionMetadata.getOperatorType().get();
                RowExpression left = node.getArguments().get(0);
                RowExpression right = node.getArguments().get(1);

                checkArgument(!(left instanceof ConstantExpression && right instanceof ConstantExpression), "Literal-to-literal not supported here, should be eliminated earlier");

                if (!(left instanceof VariableReferenceExpression) && right instanceof VariableReferenceExpression) {
                    // normalize so that variable is on the left
                    OperatorType flippedOperator = flip(operatorType);
                    return process(call(flippedOperator.name(), metadata.getFunctionAndTypeManager().resolveOperator(flippedOperator, fromTypes(right.getType(), left.getType())), BOOLEAN, right, left));
                }

                if (left instanceof ConstantExpression) {
                    // normalize so that literal is on the right
                    OperatorType flippedOperator = flip(operatorType);
                    return process(call(flippedOperator.name(), metadata.getFunctionAndTypeManager().resolveOperator(flippedOperator, fromTypes(right.getType(), left.getType())), BOOLEAN, right, left));
                }

                if (left instanceof VariableReferenceExpression && left.equals(right)) {
                    return process(not(isNull(left)));
                }

                VariableStatsEstimate leftStats = getRowExpressionStats(left);
                Optional<VariableReferenceExpression> leftVariable = left instanceof VariableReferenceExpression ? Optional.of((VariableReferenceExpression) left) : Optional.empty();
                if (right instanceof ConstantExpression) {
                    Object rightValue = ((ConstantExpression) right).getValue();
                    if (rightValue == null) {
                        return visitConstant(constantNull(BOOLEAN), null);
                    }
                    OptionalDouble literal = toStatsRepresentation(metadata.getFunctionAndTypeManager(), session, right.getType(), rightValue);
                    return estimateExpressionToLiteralComparison(input, leftStats, leftVariable, literal, getComparisonOperator(operatorType));
                }

                VariableStatsEstimate rightStats = getRowExpressionStats(right);
                if (rightStats.isSingleValue()) {
                    OptionalDouble value = isNaN(rightStats.getLowValue()) ? OptionalDouble.empty() : OptionalDouble.of(rightStats.getLowValue());
                    return estimateExpressionToLiteralComparison(input, leftStats, leftVariable, value, getComparisonOperator(operatorType));
                }

                Optional<VariableReferenceExpression> rightVariable = right instanceof VariableReferenceExpression ? Optional.of((VariableReferenceExpression) right) : Optional.empty();
                return estimateExpressionToExpressionComparison(input, leftStats, leftVariable, rightStats, rightVariable, getComparisonOperator(operatorType));
            }

            // NOT case
            if (node.getFunctionHandle().equals(functionResolution.notFunction())) {
                RowExpression arguemnt = node.getArguments().get(0);
                if (arguemnt instanceof SpecialFormExpression && ((SpecialFormExpression) arguemnt).getForm().equals(IS_NULL)) {
                    // IS NOT NULL case
                    RowExpression innerArugment = ((SpecialFormExpression) arguemnt).getArguments().get(0);
                    if (innerArugment instanceof VariableReferenceExpression) {
                        VariableReferenceExpression variable = (VariableReferenceExpression) innerArugment;
                        VariableStatsEstimate variableStats = input.getVariableStatistics(variable);
                        PlanNodeStatsEstimate.Builder result = PlanNodeStatsEstimate.buildFrom(input);
                        result.setOutputRowCount(input.getOutputRowCount() * (1 - variableStats.getNullsFraction()));
                        result.addVariableStatistics(variable, variableStats.mapNullsFraction(x -> 0.0));
                        return result.build();
                    }
                    return PlanNodeStatsEstimate.unknown();
                }
                return subtractSubsetStats(input, process(arguemnt));
            }

            // BETWEEN case
            if (functionResolution.isBetweenFunction(node.getFunctionHandle())) {
                RowExpression value = node.getArguments().get(0);
                RowExpression min = node.getArguments().get(1);
                RowExpression max = node.getArguments().get(2);
                if (!(value instanceof VariableReferenceExpression)) {
                    return PlanNodeStatsEstimate.unknown();
                }
                if (!getRowExpressionStats(min).isSingleValue()) {
                    return PlanNodeStatsEstimate.unknown();
                }
                if (!getRowExpressionStats(max).isSingleValue()) {
                    return PlanNodeStatsEstimate.unknown();
                }

                VariableStatsEstimate valueStats = input.getVariableStatistics((VariableReferenceExpression) value);
                RowExpression lowerBound = call(
                        OperatorType.GREATER_THAN_OR_EQUAL.name(),
                        metadata.getFunctionAndTypeManager().resolveOperator(OperatorType.GREATER_THAN_OR_EQUAL, fromTypes(value.getType(), min.getType())),
                        BOOLEAN,
                        value,
                        min);
                RowExpression upperBound = call(
                        OperatorType.LESS_THAN_OR_EQUAL.name(),
                        metadata.getFunctionAndTypeManager().resolveOperator(OperatorType.LESS_THAN_OR_EQUAL, fromTypes(value.getType(), max.getType())),
                        BOOLEAN,
                        value,
                        max);

                RowExpression transformed;
                if (isInfinite(valueStats.getLowValue())) {
                    // We want to do heuristic cut (infinite range to finite range) ASAP and then do filtering on finite range.
                    // We rely on 'and()' being processed left to right
                    transformed = and(lowerBound, upperBound);
                }
                else {
                    transformed = and(upperBound, lowerBound);
                }
                return process(transformed);
            }

            if (isDynamicFilter(node)) {
                return process(TRUE_CONSTANT);
            }

            return PlanNodeStatsEstimate.unknown();
        }

        @Override
        public PlanNodeStatsEstimate visitInputReference(InputReferenceExpression node, Void context)
        {
            throw new UnsupportedOperationException("plan node stats estimation should not reach channel mapping");
        }

        private FilterRowExpressionStatsCalculatingVisitor newEstimate(PlanNodeStatsEstimate input)
        {
            return new FilterRowExpressionStatsCalculatingVisitor(input, session, functionAndTypeManager);
        }

        private PlanNodeStatsEstimate process(RowExpression rowExpression)
        {
            return normalizer.normalize(rowExpression.accept(this, null));
        }

        private PlanNodeStatsEstimate estimateLogicalAnd(RowExpression left, RowExpression right)
        {
            // first try to estimate in the fair way
            PlanNodeStatsEstimate leftEstimate = process(left);
            if (!leftEstimate.isOutputRowCountUnknown()) {
                PlanNodeStatsEstimate logicalAndEstimate = newEstimate(leftEstimate).process(right);
                if (!logicalAndEstimate.isOutputRowCountUnknown()) {
                    return logicalAndEstimate;
                }
            }

            // If some of the filters cannot be estimated, take the smallest estimate.
            // Apply 0.9 filter factor as "unknown filter" factor.
            PlanNodeStatsEstimate rightEstimate = process(right);
            PlanNodeStatsEstimate smallestKnownEstimate;
            if (leftEstimate.isOutputRowCountUnknown()) {
                smallestKnownEstimate = rightEstimate;
            }
            else if (rightEstimate.isOutputRowCountUnknown()) {
                smallestKnownEstimate = leftEstimate;
            }
            else {
                smallestKnownEstimate = leftEstimate.getOutputRowCount() <= rightEstimate.getOutputRowCount() ? leftEstimate : rightEstimate;
            }
            if (smallestKnownEstimate.isOutputRowCountUnknown()) {
                return PlanNodeStatsEstimate.unknown();
            }
            return smallestKnownEstimate.mapOutputRowCount(rowCount -> rowCount * UNKNOWN_FILTER_COEFFICIENT);
        }

        private PlanNodeStatsEstimate estimateLogicalOr(RowExpression left, RowExpression right)
        {
            PlanNodeStatsEstimate leftEstimate = process(left);
            if (leftEstimate.isOutputRowCountUnknown()) {
                return PlanNodeStatsEstimate.unknown();
            }

            PlanNodeStatsEstimate rightEstimate = process(right);
            if (rightEstimate.isOutputRowCountUnknown()) {
                return PlanNodeStatsEstimate.unknown();
            }

            PlanNodeStatsEstimate andEstimate = newEstimate(leftEstimate).process(right);
            if (andEstimate.isOutputRowCountUnknown()) {
                return PlanNodeStatsEstimate.unknown();
            }

            return capStats(
                    subtractSubsetStats(
                            addStatsAndSumDistinctValues(leftEstimate, rightEstimate),
                            andEstimate),
                    input);
        }

        private PlanNodeStatsEstimate estimateIn(RowExpression value, List<RowExpression> candidates)
        {
            ImmutableList<PlanNodeStatsEstimate> equalityEstimates = candidates.stream()
                    .map(inValue -> process(call(OperatorType.EQUAL.name(), metadata.getFunctionAndTypeManager().resolveOperator(OperatorType.EQUAL, fromTypes(value.getType(), inValue.getType())), BOOLEAN, value, inValue)))
                    .collect(toImmutableList());

            if (equalityEstimates.stream().anyMatch(PlanNodeStatsEstimate::isOutputRowCountUnknown)) {
                return PlanNodeStatsEstimate.unknown();
            }

            PlanNodeStatsEstimate inEstimate = equalityEstimates.stream()
                    .reduce(PlanNodeStatsEstimateMath::addStatsAndSumDistinctValues)
                    .orElse(PlanNodeStatsEstimate.unknown());

            if (inEstimate.isOutputRowCountUnknown()) {
                return PlanNodeStatsEstimate.unknown();
            }

            VariableStatsEstimate valueStats = getRowExpressionStats(value);
            if (valueStats.isUnknown()) {
                return PlanNodeStatsEstimate.unknown();
            }

            double notNullValuesBeforeIn = input.getOutputRowCount() * (1 - valueStats.getNullsFraction());

            PlanNodeStatsEstimate.Builder result = PlanNodeStatsEstimate.buildFrom(input);
            result.setOutputRowCount(min(inEstimate.getOutputRowCount(), notNullValuesBeforeIn));

            if (value instanceof VariableReferenceExpression) {
                VariableReferenceExpression valueVariable = (VariableReferenceExpression) value;
                VariableStatsEstimate newVariableStats = inEstimate.getVariableStatistics(valueVariable)
                        .mapDistinctValuesCount(newDistinctValuesCount -> min(newDistinctValuesCount, valueStats.getDistinctValuesCount()));
                result.addVariableStatistics(valueVariable, newVariableStats);
            }
            return result.build();
        }

        private PlanNodeStatsEstimate estimateIsNull(RowExpression expression)
        {
            if (expression instanceof VariableReferenceExpression) {
                VariableReferenceExpression variable = (VariableReferenceExpression) expression;
                VariableStatsEstimate variableStats = input.getVariableStatistics(variable);
                PlanNodeStatsEstimate.Builder result = PlanNodeStatsEstimate.buildFrom(input);
                result.setOutputRowCount(input.getOutputRowCount() * variableStats.getNullsFraction());
                result.addVariableStatistics(variable, VariableStatsEstimate.builder()
                        .setNullsFraction(1.0)
                        .setLowValue(NaN)
                        .setHighValue(NaN)
                        .setDistinctValuesCount(0.0)
                        .build());
                return result.build();
            }
            return PlanNodeStatsEstimate.unknown();
        }

        private RowExpression isNull(RowExpression expression)
        {
            return new SpecialFormExpression(IS_NULL, BOOLEAN, expression);
        }

        private RowExpression not(RowExpression expression)
        {
            return call("not", functionResolution.notFunction(), expression.getType(), expression);
        }

        private ComparisonExpression.Operator getComparisonOperator(OperatorType operator)
        {
            switch (operator) {
                case EQUAL:
                    return EQUAL;
                case NOT_EQUAL:
                    return NOT_EQUAL;
                case LESS_THAN:
                    return LESS_THAN;
                case LESS_THAN_OR_EQUAL:
                    return LESS_THAN_OR_EQUAL;
                case GREATER_THAN:
                    return GREATER_THAN;
                case GREATER_THAN_OR_EQUAL:
                    return GREATER_THAN_OR_EQUAL;
                case IS_DISTINCT_FROM:
                    return IS_DISTINCT_FROM;
                default:
                    throw new IllegalStateException("Unsupported comparison operator type: " + operator);
            }
        }

        private OperatorType flip(OperatorType operatorType)
        {
            switch (operatorType) {
                case EQUAL:
                    return OperatorType.EQUAL;
                case NOT_EQUAL:
                    return OperatorType.NOT_EQUAL;
                case LESS_THAN:
                    return OperatorType.GREATER_THAN;
                case LESS_THAN_OR_EQUAL:
                    return OperatorType.GREATER_THAN_OR_EQUAL;
                case GREATER_THAN:
                    return OperatorType.LESS_THAN;
                case GREATER_THAN_OR_EQUAL:
                    return OperatorType.LESS_THAN_OR_EQUAL;
                case IS_DISTINCT_FROM:
                    return OperatorType.IS_DISTINCT_FROM;
                default:
                    throw new IllegalArgumentException("Unsupported comparison: " + operatorType);
            }
        }

        private VariableStatsEstimate getRowExpressionStats(RowExpression expression)
        {
            if (expression instanceof VariableReferenceExpression) {
                VariableReferenceExpression variable = (VariableReferenceExpression) expression;
                return requireNonNull(input.getVariableStatistics(variable), () -> format("No statistics for variable %s", variable));
            }
            return scalarStatsCalculator.calculate(expression, input, session);
        }
    }
}
