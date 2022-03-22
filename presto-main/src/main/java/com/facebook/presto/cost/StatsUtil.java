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
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.BooleanType;
import com.facebook.presto.common.type.DateType;
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.RealType;
import com.facebook.presto.common.type.SmallintType;
import com.facebook.presto.common.type.TinyintType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.statistics.ColumnStatistics;
import com.facebook.presto.spi.statistics.TableStatistics;
import com.facebook.presto.sql.InterpretedFunctionInvoker;

import java.util.OptionalDouble;

import static com.facebook.presto.metadata.CastType.CAST;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;

final class StatsUtil
{
    private StatsUtil() {}

    static OptionalDouble toStatsRepresentation(Metadata metadata, Session session, Type type, Object value)
    {
        return toStatsRepresentation(metadata.getFunctionAndTypeManager(), session.toConnectorSession(), type, value);
    }

    static OptionalDouble toStatsRepresentation(FunctionAndTypeManager functionAndTypeManager, ConnectorSession session, Type type, Object value)
    {
        requireNonNull(value, "value is null");

        if (convertibleToDoubleWithCast(type)) {
            InterpretedFunctionInvoker functionInvoker = new InterpretedFunctionInvoker(functionAndTypeManager);
            FunctionHandle cast = functionAndTypeManager.lookupCast(CAST, type.getTypeSignature(), DoubleType.DOUBLE.getTypeSignature());

            return OptionalDouble.of((double) functionInvoker.invoke(cast, session.getSqlFunctionProperties(), singletonList(value)));
        }

        if (DateType.DATE.equals(type)) {
            return OptionalDouble.of(((Long) value).doubleValue());
        }

        return OptionalDouble.empty();
    }

    private static boolean convertibleToDoubleWithCast(Type type)
    {
        return type instanceof DecimalType
                || DoubleType.DOUBLE.equals(type)
                || RealType.REAL.equals(type)
                || BigintType.BIGINT.equals(type)
                || IntegerType.INTEGER.equals(type)
                || SmallintType.SMALLINT.equals(type)
                || TinyintType.TINYINT.equals(type)
                || BooleanType.BOOLEAN.equals(type);
    }

    public static VariableStatsEstimate toVariableStatsEstimate(TableStatistics tableStatistics, ColumnStatistics columnStatistics)
    {
        double nullsFraction = columnStatistics.getNullsFraction().getValue();
        double nonNullRowsCount = tableStatistics.getRowCount().getValue() * (1.0 - nullsFraction);
        double averageRowSize = nonNullRowsCount == 0 ? 0 : columnStatistics.getDataSize().getValue() / nonNullRowsCount;
        VariableStatsEstimate.Builder result = VariableStatsEstimate.builder();
        result.setNullsFraction(nullsFraction);
        result.setDistinctValuesCount(columnStatistics.getDistinctValuesCount().getValue());
        result.setAverageRowSize(averageRowSize);
        columnStatistics.getRange().ifPresent(range -> {
            result.setLowValue(range.getMin());
            result.setHighValue(range.getMax());
        });
        return result.build();
    }
}
