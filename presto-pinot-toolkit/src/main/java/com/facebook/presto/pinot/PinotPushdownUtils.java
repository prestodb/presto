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
package com.facebook.presto.pinot;

import com.facebook.presto.spi.block.SortOrder;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.TopNNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.CharType;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.IntegerType;
import com.facebook.presto.spi.type.RealType;
import com.facebook.presto.spi.type.SmallintType;
import com.facebook.presto.spi.type.TinyintType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.pinot.PinotErrorCode.PINOT_UNSUPPORTED_EXPRESSION;
import static com.facebook.presto.spi.type.Decimals.decodeUnscaledValue;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.Float.intBitsToFloat;
import static java.lang.String.format;

public class PinotPushdownUtils
{
    private PinotPushdownUtils() {}

    public enum ExpressionType
    {
        GROUP_BY,
        AGGREGATE,
    }

    /**
     * Group by field description
     */
    public static class GroupByColumnNode
            extends AggregationColumnNode
    {
        private final VariableReferenceExpression inputColumn;

        public GroupByColumnNode(VariableReferenceExpression inputColumn, VariableReferenceExpression output)
        {
            super(ExpressionType.GROUP_BY, output);
            this.inputColumn = inputColumn;
        }

        public VariableReferenceExpression getInputColumn()
        {
            return inputColumn;
        }

        @Override
        public String toString()
        {
            return inputColumn.toString();
        }
    }

    /**
     * Agg function description.
     */
    public static class AggregationFunctionColumnNode
            extends AggregationColumnNode
    {
        private final CallExpression callExpression;

        public AggregationFunctionColumnNode(VariableReferenceExpression output, CallExpression callExpression)
        {
            super(ExpressionType.AGGREGATE, output);
            this.callExpression = callExpression;
        }

        public CallExpression getCallExpression()
        {
            return callExpression;
        }

        @Override
        public String toString()
        {
            return callExpression.toString();
        }
    }

    public static void checkSupported(boolean condition, String errorMessage, Object... errorMessageArgs)
    {
        if (!condition) {
            throw new PinotException(PinotErrorCode.PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), String.format(errorMessage, errorMessageArgs));
        }
    }

    public abstract static class AggregationColumnNode
    {
        private final ExpressionType expressionType;
        private final VariableReferenceExpression outputColumn;

        public AggregationColumnNode(ExpressionType expressionType, VariableReferenceExpression outputColumn)
        {
            this.expressionType = expressionType;
            this.outputColumn = outputColumn;
        }

        public VariableReferenceExpression getOutputColumn()
        {
            return outputColumn;
        }

        public ExpressionType getExpressionType()
        {
            return expressionType;
        }
    }

    public static List<AggregationColumnNode> computeAggregationNodes(AggregationNode aggregationNode)
    {
        int groupByKeyIndex = 0;
        ImmutableList.Builder<AggregationColumnNode> nodeBuilder = ImmutableList.builder();
        for (VariableReferenceExpression outputColumn : aggregationNode.getOutputVariables()) {
            AggregationNode.Aggregation agg = aggregationNode.getAggregations().get(outputColumn);

            if (agg != null) {
                if (agg.getFilter().isPresent()
                        || agg.isDistinct()
                        || agg.getOrderBy().isPresent()
                        || agg.getMask().isPresent()) {
                    throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), "Unsupported aggregation node " + aggregationNode);
                }
                nodeBuilder.add(new AggregationFunctionColumnNode(outputColumn, agg.getCall()));
            }
            else {
                // group by output
                VariableReferenceExpression inputColumn = aggregationNode.getGroupingKeys().get(groupByKeyIndex);
                nodeBuilder.add(new GroupByColumnNode(inputColumn, outputColumn));
                groupByKeyIndex++;
            }
        }
        return nodeBuilder.build();
    }

    public static LinkedHashMap<VariableReferenceExpression, SortOrder> getOrderingScheme(TopNNode topNNode)
    {
        LinkedHashMap<VariableReferenceExpression, SortOrder> orderingScheme = new LinkedHashMap<>();
        topNNode.getOrderingScheme().getOrderByVariables().forEach(value -> orderingScheme.put(value, topNNode.getOrderingScheme().getOrdering(value)));
        return orderingScheme;
    }

    private static Number decodeDecimal(BigInteger unscaledValue, DecimalType type)
    {
        return new BigDecimal(unscaledValue, type.getScale(), new MathContext(type.getPrecision()));
    }

    // Copied from com.facebook.presto.sql.planner.LiteralInterpreter.evaluate
    public static String getLiteralAsString(ConstantExpression node)
    {
        Type type = node.getType();

        if (node.getValue() == null) {
            throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), String.format("Null constant expression %s with value of type %s", node, type));
        }
        if (type instanceof BooleanType) {
            return String.valueOf(((Boolean) node.getValue()).booleanValue());
        }
        if (type instanceof BigintType || type instanceof TinyintType || type instanceof SmallintType || type instanceof IntegerType) {
            Number number = (Number) node.getValue();
            return format("%d", number.longValue());
        }
        if (type instanceof DoubleType) {
            return node.getValue().toString();
        }
        if (type instanceof RealType) {
            Long number = (Long) node.getValue();
            return format("%f", intBitsToFloat(number.intValue()));
        }
        if (type instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) type;
            if (decimalType.isShort()) {
                checkState(node.getValue() instanceof Long);
                return decodeDecimal(BigInteger.valueOf((long) node.getValue()), decimalType).toString();
            }
            checkState(node.getValue() instanceof Slice);
            Slice value = (Slice) node.getValue();
            return decodeDecimal(decodeUnscaledValue(value), decimalType).toString();
        }
        if (type instanceof VarcharType || type instanceof CharType) {
            return "'" + ((Slice) node.getValue()).toStringUtf8() + "'";
        }
        throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), String.format("Cannot handle the constant expression %s with value of type %s", node, type));
    }
}
