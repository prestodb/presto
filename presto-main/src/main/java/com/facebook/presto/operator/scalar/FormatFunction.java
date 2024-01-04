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
package com.facebook.presto.operator.scalar;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.prestosql.annotation.UsedByGeneratedCode;
import io.prestosql.metadata.BoundVariables;
import io.prestosql.metadata.FunctionRegistry;
import io.prestosql.metadata.OperatorNotFoundException;
import io.prestosql.metadata.Signature;
import io.prestosql.metadata.SqlScalarFunction;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;

import java.lang.invoke.MethodHandle;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.IllegalFormatException;
import java.util.List;
import java.util.function.BiFunction;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Streams.mapWithIndex;
import static io.airlift.slice.Slices.utf8Slice;
import static io.prestosql.metadata.FunctionKind.SCALAR;
import static io.prestosql.metadata.Signature.internalScalarFunction;
import static io.prestosql.metadata.Signature.withVariadicBound;
import static io.prestosql.operator.scalar.ScalarFunctionImplementation.ArgumentProperty.valueTypeArgumentProperty;
import static io.prestosql.operator.scalar.ScalarFunctionImplementation.NullConvention.RETURN_NULL_ON_NULL;
import static io.prestosql.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.Chars.isCharType;
import static io.prestosql.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.prestosql.spi.type.DateTimeEncoding.unpackZoneKey;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.Decimals.decodeUnscaledValue;
import static io.prestosql.spi.type.Decimals.isLongDecimal;
import static io.prestosql.spi.type.Decimals.isShortDecimal;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.RealType.REAL;
import static io.prestosql.spi.type.SmallintType.SMALLINT;
import static io.prestosql.spi.type.TimeType.TIME;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static io.prestosql.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static io.prestosql.spi.type.TinyintType.TINYINT;
import static io.prestosql.spi.type.TypeSignature.parseTypeSignature;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.spi.type.Varchars.isVarcharType;
import static io.prestosql.type.JsonType.JSON;
import static io.prestosql.type.UnknownType.UNKNOWN;
import static io.prestosql.util.Failures.internalError;
import static io.prestosql.util.Reflection.methodHandle;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public final class FormatFunction
        extends SqlScalarFunction
{
    public static final String NAME = "$format";

    public static final FormatFunction FORMAT_FUNCTION = new FormatFunction();
    private static final MethodHandle METHOD_HANDLE = methodHandle(FormatFunction.class, "formatFloat", List.class, ConnectorSession.class, Slice.class, Block.class);

    private FormatFunction()
    {
        super(Signature.builder()
                .kind(SCALAR)
                .name(NAME)
                .typeVariableConstraints(withVariadicBound("T", "row"))
                .argumentTypes(VARCHAR.getTypeSignature(), parseTypeSignature("T"))
                .returnType(VARCHAR.getTypeSignature())
                .build());
    }

    @Override
    public ScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, TypeManager typeManager, FunctionRegistry functionRegistry) {
        return new ScalarFunctionImplementation(
                false,
                ImmutableList.of(
                        valueTypeArgumentProperty(RETURN_NULL_ON_NULL),
                        valueTypeArgumentProperty(RETURN_NULL_ON_NULL)),
                METHOD_HANDLE,
                true);
    }

    @UsedByGeneratedCode
    public static Slice formatFloat(Double value, Integer decimalPlaces) {
        String formattedValue = String.format("%." + decimalPlaces + "f", value);
        return utf8Slice(formattedValue);
    }
}