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

import io.prestosql.Session;
import io.prestosql.metadata.FunctionRegistry;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.Signature;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.type.BigintType;
import io.prestosql.spi.type.BooleanType;
import io.prestosql.spi.type.DateType;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.DoubleType;
import io.prestosql.spi.type.IntegerType;
import io.prestosql.spi.type.RealType;
import io.prestosql.spi.type.SmallintType;
import io.prestosql.spi.type.TinyintType;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.InterpretedFunctionInvoker;

import java.util.OptionalDouble;

import static java.util.Collections.singletonList;

final class StatsUtil
{
    private StatsUtil() {}

    static OptionalDouble toStatsRepresentation(Metadata metadata, Session session, Type type, Object value)
    {
        return toStatsRepresentation(metadata.getFunctionRegistry(), session.toConnectorSession(), type, value);
    }

    static OptionalDouble toStatsRepresentation(FunctionRegistry functionRegistry, ConnectorSession session, Type type, Object value)
    {
        if (convertibleToDoubleWithCast(type)) {
            InterpretedFunctionInvoker functionInvoker = new InterpretedFunctionInvoker(functionRegistry);
            Signature castSignature = functionRegistry.getCoercion(type, DoubleType.DOUBLE);
            return OptionalDouble.of((double) functionInvoker.invoke(castSignature, session, singletonList(value)));
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
}
