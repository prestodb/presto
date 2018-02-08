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
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.sql.analyzer.ExpressionAnalyzer;
import com.facebook.presto.sql.analyzer.Scope;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.tree.ArithmeticBinaryExpression;
import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Literal;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.NullLiteral;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;

import java.util.OptionalDouble;

import static com.facebook.presto.cost.StatsUtil.toStatsRepresentation;
import static com.facebook.presto.sql.planner.LiteralInterpreter.evaluate;
import static com.facebook.presto.util.MoreMath.max;
import static com.facebook.presto.util.MoreMath.min;
import static java.lang.Double.isFinite;
import static java.lang.Double.isNaN;
import static java.lang.Math.abs;
import static java.util.Objects.requireNonNull;

public class ScalarStatsCalculator
{
    private final Metadata metadata;

    @Inject
    public ScalarStatsCalculator(Metadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata can not be null");
    }

    public SymbolStatsEstimate calculate(Expression scalarExpression, PlanNodeStatsEstimate inputStatistics, Session session)
    {
        return new Visitor(inputStatistics, session).process(scalarExpression);
    }

    private class Visitor
            extends AstVisitor<SymbolStatsEstimate, Void>
    {
        private final PlanNodeStatsEstimate input;
        private final Session session;

        Visitor(PlanNodeStatsEstimate input, Session session)
        {
            this.input = input;
            this.session = session;
        }

        @Override
        protected SymbolStatsEstimate visitNode(Node node, Void context)
        {
            return SymbolStatsEstimate.UNKNOWN_STATS;
        }

        @Override
        protected SymbolStatsEstimate visitSymbolReference(SymbolReference node, Void context)
        {
            return input.getSymbolStatistics(Symbol.from(node));
        }

        @Override
        protected SymbolStatsEstimate visitNullLiteral(NullLiteral node, Void context)
        {
            return SymbolStatsEstimate.builder()
                    .setDistinctValuesCount(0)
                    .setNullsFraction(1)
                    .build();
        }

        @Override
        protected SymbolStatsEstimate visitLiteral(Literal node, Void context)
        {
            Object value = evaluate(metadata, session.toConnectorSession(), node);
            Type type = ExpressionAnalyzer.createConstantAnalyzer(metadata, session, ImmutableList.of()).analyze(node, Scope.create());
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
            if (node.getType() == ArithmeticBinaryExpression.Type.DIVIDE && rightLow < 0 && rightHigh > 0) {
                result.setLowValue(Double.NEGATIVE_INFINITY)
                        .setHighValue(Double.POSITIVE_INFINITY);
            }
            else if (node.getType() == ArithmeticBinaryExpression.Type.MODULUS) {
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
                double v1 = operate(node.getType(), leftLow, rightLow);
                double v2 = operate(node.getType(), leftLow, rightHigh);
                double v3 = operate(node.getType(), leftHigh, rightLow);
                double v4 = operate(node.getType(), leftHigh, rightHigh);
                double lowValue = min(v1, v2, v3, v4);
                double highValue = max(v1, v2, v3, v4);

                result.setLowValue(lowValue)
                        .setHighValue(highValue);
            }

            return result.build();
        }

        private double operate(ArithmeticBinaryExpression.Type type, double left, double right)
        {
            switch (type) {
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
                    throw new IllegalStateException("Unsupported ArithmeticBinaryExpression.Type: " + type);
            }
        }
    }
}
