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
package io.prestosql.cost;

import com.google.common.collect.ImmutableList;
import io.prestosql.Session;
import io.prestosql.execution.warnings.WarningCollector;
import io.prestosql.metadata.Metadata;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeSignature;
import io.prestosql.sql.analyzer.ExpressionAnalyzer;
import io.prestosql.sql.analyzer.Scope;
import io.prestosql.sql.planner.ExpressionInterpreter;
import io.prestosql.sql.planner.NoOpSymbolResolver;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.TypeProvider;
import io.prestosql.sql.tree.ArithmeticBinaryExpression;
import io.prestosql.sql.tree.ArithmeticUnaryExpression;
import io.prestosql.sql.tree.AstVisitor;
import io.prestosql.sql.tree.Cast;
import io.prestosql.sql.tree.CoalesceExpression;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.FunctionCall;
import io.prestosql.sql.tree.Literal;
import io.prestosql.sql.tree.Node;
import io.prestosql.sql.tree.NodeRef;
import io.prestosql.sql.tree.NullLiteral;
import io.prestosql.sql.tree.SymbolReference;

import javax.inject.Inject;

import java.util.Map;
import java.util.OptionalDouble;

import static io.prestosql.cost.StatsUtil.toStatsRepresentation;
import static io.prestosql.sql.planner.LiteralInterpreter.evaluate;
import static io.prestosql.util.MoreMath.max;
import static io.prestosql.util.MoreMath.min;
import static java.lang.Double.NaN;
import static java.lang.Double.isFinite;
import static java.lang.Double.isNaN;
import static java.lang.Math.abs;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;

public class ScalarStatsCalculator
{
    private final Metadata metadata;

    @Inject
    public ScalarStatsCalculator(Metadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata can not be null");
    }

    public SymbolStatsEstimate calculate(Expression scalarExpression, PlanNodeStatsEstimate inputStatistics, Session session, TypeProvider types)
    {
        return new Visitor(inputStatistics, session, types).process(scalarExpression);
    }

    private class Visitor
            extends AstVisitor<SymbolStatsEstimate, Void>
    {
        private final PlanNodeStatsEstimate input;
        private final Session session;
        private final TypeProvider types;

        Visitor(PlanNodeStatsEstimate input, Session session, TypeProvider types)
        {
            this.input = input;
            this.session = session;
            this.types = types;
        }

        @Override
        protected SymbolStatsEstimate visitNode(Node node, Void context)
        {
            return SymbolStatsEstimate.unknown();
        }

        @Override
        protected SymbolStatsEstimate visitSymbolReference(SymbolReference node, Void context)
        {
            return input.getSymbolStatistics(Symbol.from(node));
        }

        @Override
        protected SymbolStatsEstimate visitNullLiteral(NullLiteral node, Void context)
        {
            return nullStatsEstimate();
        }

        @Override
        protected SymbolStatsEstimate visitLiteral(Literal node, Void context)
        {
            Object value = evaluate(metadata, session.toConnectorSession(), node);
            Type type = ExpressionAnalyzer.createConstantAnalyzer(metadata, session, ImmutableList.of(), WarningCollector.NOOP).analyze(node, Scope.create());
            OptionalDouble doubleValue = toStatsRepresentation(metadata, session, type, value);
            SymbolStatsEstimate.Builder estimate = SymbolStatsEstimate.builder()
                    .setNullsFraction(0)
                    .setDistinctValuesCount(1);

            if (doubleValue.isPresent()) {
                estimate.setLowValue(doubleValue.getAsDouble());
                estimate.setHighValue(doubleValue.getAsDouble());
            }
            return estimate.build();
        }

        @Override
        protected SymbolStatsEstimate visitFunctionCall(FunctionCall node, Void context)
        {
            Map<NodeRef<Expression>, Type> expressionTypes = getExpressionTypes(session, node, types);
            ExpressionInterpreter interpreter = ExpressionInterpreter.expressionOptimizer(node, metadata, session, expressionTypes);
            Object value = interpreter.optimize(NoOpSymbolResolver.INSTANCE);

            if (value == null || value instanceof NullLiteral) {
                return nullStatsEstimate();
            }

            if (value instanceof Expression && !(value instanceof Literal)) {
                // value is not a constant
                return SymbolStatsEstimate.unknown();
            }

            // value is a constant
            return SymbolStatsEstimate.builder()
                    .setNullsFraction(0)
                    .setDistinctValuesCount(1)
                    .build();
        }

        private Map<NodeRef<Expression>, Type> getExpressionTypes(Session session, Expression expression, TypeProvider types)
        {
            ExpressionAnalyzer expressionAnalyzer = ExpressionAnalyzer.createWithoutSubqueries(
                    metadata.getFunctionRegistry(),
                    metadata.getTypeManager(),
                    session,
                    types,
                    emptyList(),
                    node -> new IllegalStateException("Unexpected node: %s" + node),
                    WarningCollector.NOOP,
                    false);
            expressionAnalyzer.analyze(expression, Scope.create());
            return expressionAnalyzer.getExpressionTypes();
        }

        @Override
        protected SymbolStatsEstimate visitCast(Cast node, Void context)
        {
            SymbolStatsEstimate sourceStats = process(node.getExpression());
            TypeSignature targetType = TypeSignature.parseTypeSignature(node.getType());

            // todo - make this general postprocessing rule.
            double distinctValuesCount = sourceStats.getDistinctValuesCount();
            double lowValue = sourceStats.getLowValue();
            double highValue = sourceStats.getHighValue();

            if (isIntegralType(targetType)) {
                // todo handle low/high value changes if range gets narrower due to cast (e.g. BIGINT -> SMALLINT)
                if (isFinite(lowValue)) {
                    lowValue = Math.round(lowValue);
                }
                if (isFinite(highValue)) {
                    highValue = Math.round(highValue);
                }
                if (isFinite(lowValue) && isFinite(highValue)) {
                    double integersInRange = highValue - lowValue + 1;
                    if (!isNaN(distinctValuesCount) && distinctValuesCount > integersInRange) {
                        distinctValuesCount = integersInRange;
                    }
                }
            }

            return SymbolStatsEstimate.builder()
                    .setNullsFraction(sourceStats.getNullsFraction())
                    .setLowValue(lowValue)
                    .setHighValue(highValue)
                    .setDistinctValuesCount(distinctValuesCount)
                    .build();
        }

        private boolean isIntegralType(TypeSignature targetType)
        {
            switch (targetType.getBase()) {
                case StandardTypes.BIGINT:
                case StandardTypes.INTEGER:
                case StandardTypes.SMALLINT:
                case StandardTypes.TINYINT:
                    return true;
                case StandardTypes.DECIMAL:
                    DecimalType decimalType = (DecimalType) metadata.getType(targetType);
                    return decimalType.getScale() == 0;
                default:
                    return false;
            }
        }

        @Override
        protected SymbolStatsEstimate visitArithmeticUnary(ArithmeticUnaryExpression node, Void context)
        {
            SymbolStatsEstimate stats = process(node.getValue());
            switch (node.getSign()) {
                case PLUS:
                    return stats;
                case MINUS:
                    return SymbolStatsEstimate.buildFrom(stats)
                            .setLowValue(-stats.getHighValue())
                            .setHighValue(-stats.getLowValue())
                            .build();
                default:
                    throw new IllegalStateException("Unexpected sign: " + node.getSign());
            }
        }

        @Override
        protected SymbolStatsEstimate visitArithmeticBinary(ArithmeticBinaryExpression node, Void context)
        {
            requireNonNull(node, "node is null");
            SymbolStatsEstimate left = process(node.getLeft());
            SymbolStatsEstimate right = process(node.getRight());

            SymbolStatsEstimate.Builder result = SymbolStatsEstimate.builder()
                    .setAverageRowSize(Math.max(left.getAverageRowSize(), right.getAverageRowSize()))
                    .setNullsFraction(left.getNullsFraction() + right.getNullsFraction() - left.getNullsFraction() * right.getNullsFraction())
                    .setDistinctValuesCount(min(left.getDistinctValuesCount() * right.getDistinctValuesCount(), input.getOutputRowCount()));

            double leftLow = left.getLowValue();
            double leftHigh = left.getHighValue();
            double rightLow = right.getLowValue();
            double rightHigh = right.getHighValue();
            if (isNaN(leftLow) || isNaN(leftHigh) || isNaN(rightLow) || isNaN(rightHigh)) {
                result.setLowValue(NaN)
                        .setHighValue(NaN);
            }
            else if (node.getOperator() == ArithmeticBinaryExpression.Operator.DIVIDE && rightLow < 0 && rightHigh > 0) {
                result.setLowValue(Double.NEGATIVE_INFINITY)
                        .setHighValue(Double.POSITIVE_INFINITY);
            }
            else if (node.getOperator() == ArithmeticBinaryExpression.Operator.MODULUS) {
                double maxDivisor = max(abs(rightLow), abs(rightHigh));
                if (leftHigh <= 0) {
                    result.setLowValue(max(-maxDivisor, leftLow))
                            .setHighValue(0);
                }
                else if (leftLow >= 0) {
                    result.setLowValue(0)
                            .setHighValue(min(maxDivisor, leftHigh));
                }
                else {
                    result.setLowValue(max(-maxDivisor, leftLow))
                            .setHighValue(min(maxDivisor, leftHigh));
                }
            }
            else {
                double v1 = operate(node.getOperator(), leftLow, rightLow);
                double v2 = operate(node.getOperator(), leftLow, rightHigh);
                double v3 = operate(node.getOperator(), leftHigh, rightLow);
                double v4 = operate(node.getOperator(), leftHigh, rightHigh);
                double lowValue = min(v1, v2, v3, v4);
                double highValue = max(v1, v2, v3, v4);

                result.setLowValue(lowValue)
                        .setHighValue(highValue);
            }

            return result.build();
        }

        private double operate(ArithmeticBinaryExpression.Operator operator, double left, double right)
        {
            switch (operator) {
                case ADD:
                    return left + right;
                case SUBTRACT:
                    return left - right;
                case MULTIPLY:
                    return left * right;
                case DIVIDE:
                    return left / right;
                case MODULUS:
                    return left % right;
                default:
                    throw new IllegalStateException("Unsupported ArithmeticBinaryExpression.Operator: " + operator);
            }
        }

        @Override
        protected SymbolStatsEstimate visitCoalesceExpression(CoalesceExpression node, Void context)
        {
            requireNonNull(node, "node is null");
            SymbolStatsEstimate result = null;
            for (Expression operand : node.getOperands()) {
                SymbolStatsEstimate operandEstimates = process(operand);
                if (result != null) {
                    result = estimateCoalesce(result, operandEstimates);
                }
                else {
                    result = operandEstimates;
                }
            }
            return requireNonNull(result, "result is null");
        }

        private SymbolStatsEstimate estimateCoalesce(SymbolStatsEstimate left, SymbolStatsEstimate right)
        {
            // Question to reviewer: do you have a method to check if fraction is empty or saturated?
            if (left.getNullsFraction() == 0) {
                return left;
            }
            else if (left.getNullsFraction() == 1.0) {
                return right;
            }
            else {
                return SymbolStatsEstimate.builder()
                        .setLowValue(min(left.getLowValue(), right.getLowValue()))
                        .setHighValue(max(left.getHighValue(), right.getHighValue()))
                        .setDistinctValuesCount(left.getDistinctValuesCount() +
                                min(right.getDistinctValuesCount(), input.getOutputRowCount() * left.getNullsFraction()))
                        .setNullsFraction(left.getNullsFraction() * right.getNullsFraction())
                        // TODO check if dataSize estimation method is correct
                        .setAverageRowSize(max(left.getAverageRowSize(), right.getAverageRowSize()))
                        .build();
            }
        }
    }

    private static SymbolStatsEstimate nullStatsEstimate()
    {
        return SymbolStatsEstimate.builder()
                .setDistinctValuesCount(0)
                .setNullsFraction(1)
                .build();
    }
}
