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
package com.facebook.presto.plugin.clickhouse.optimization;

import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.BooleanType;
import com.facebook.presto.common.type.CharType;
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.RealType;
import com.facebook.presto.common.type.SmallintType;
import com.facebook.presto.common.type.SqlTimestamp;
import com.facebook.presto.common.type.SqlTimestampWithTimeZone;
import com.facebook.presto.common.type.TimestampType;
import com.facebook.presto.common.type.TimestampWithTimeZoneType;
import com.facebook.presto.common.type.TinyintType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.plugin.clickhouse.optimization.ClickHouseAggregationColumnNode.AggregationFunctionColumnNode;
import com.facebook.presto.plugin.clickhouse.optimization.ClickHouseAggregationColumnNode.GroupByColumnNode;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.facebook.presto.common.type.Decimals.decodeUnscaledValue;
import static com.facebook.presto.plugin.clickhouse.ClickHouseErrorCode.CLICKHOUSE_PUSHDOWN_UNSUPPORTED_EXPRESSION;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.Float.intBitsToFloat;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class ClickHousePushdownUtils
{
    private ClickHousePushdownUtils() {}

    public static List<ClickHouseAggregationColumnNode> computeAggregationNodes(AggregationNode aggregationNode)
    {
        int groupByKeyIndex = 0;
        ImmutableList.Builder<ClickHouseAggregationColumnNode> nodeBuilder = ImmutableList.builder();
        for (VariableReferenceExpression outputColumn : aggregationNode.getOutputVariables()) {
            AggregationNode.Aggregation aggregation = aggregationNode.getAggregations().get(outputColumn);

            if (aggregation != null) {
                if (aggregation.getFilter().isPresent()
                        || aggregation.isDistinct()
                        || aggregation.getOrderBy().isPresent()) {
                    throw new PrestoException(CLICKHOUSE_PUSHDOWN_UNSUPPORTED_EXPRESSION, "Unsupported aggregation node " + aggregationNode);
                }
                nodeBuilder.add(new AggregationFunctionColumnNode(outputColumn, aggregation.getCall()));
            }
            else {
                VariableReferenceExpression inputColumn = aggregationNode.getGroupingKeys().get(groupByKeyIndex);
                nodeBuilder.add(new GroupByColumnNode(inputColumn, outputColumn));
                groupByKeyIndex++;
            }
        }
        return nodeBuilder.build();
    }

    private static Set<String> getGroupKeys(List<VariableReferenceExpression> groupingKeys)
    {
        Set<String> groupKeySet = new HashSet<>();
        groupingKeys.forEach(groupingKey -> groupKeySet.add(groupingKey.getName()));
        return groupKeySet;
    }

    private static Number decodeDecimal(BigInteger unscaledValue, DecimalType type)
    {
        return new BigDecimal(unscaledValue, type.getScale(), new MathContext(type.getPrecision()));
    }

    public static String getLiteralAsString(ConnectorSession session, ConstantExpression node)
    {
        Type type = node.getType();

        if (node.getValue() == null) {
            throw new PrestoException(CLICKHOUSE_PUSHDOWN_UNSUPPORTED_EXPRESSION, "Null constant expression: " + node + " with value of type: " + type);
        }
        if (type instanceof BooleanType) {
            return String.valueOf(((Boolean) node.getValue()).booleanValue());
        }
        if (type instanceof BigintType || type instanceof TinyintType || type instanceof SmallintType
                || type instanceof IntegerType) {
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
        if (type instanceof TimestampType) {
            return getTimestampLiteralAsString(session, (long) node.getValue());
        }
        if (type instanceof TimestampWithTimeZoneType) {
            return getTimestampLiteralAsString(session, new SqlTimestampWithTimeZone((long) node.getValue()).getMillisUtc());
        }
        throw new PrestoException(CLICKHOUSE_PUSHDOWN_UNSUPPORTED_EXPRESSION, "Cannot handle the constant expression: " + node + " with value of type: " + type);
    }

    private static String getTimestampLiteralAsString(ConnectorSession session, long millisUtc)
    {
        SqlTimestamp sqlTimestamp = new SqlTimestamp(millisUtc, MILLISECONDS);
        return "TIMESTAMP '" + sqlTimestamp.toString() + "'";
    }
}
