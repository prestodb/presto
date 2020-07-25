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
package com.facebook.presto.tests.datatype;

import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.BooleanType;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.RealType;
import com.facebook.presto.common.type.SmallintType;
import com.facebook.presto.common.type.TinyintType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarbinaryType;
import com.facebook.presto.common.type.VarcharType;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Optional;
import java.util.function.Function;

import static com.facebook.presto.common.type.CharType.createCharType;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.DecimalType.createDecimalType;
import static com.facebook.presto.common.type.VarcharType.createUnboundedVarcharType;
import static com.google.common.base.Strings.padEnd;
import static com.google.common.io.BaseEncoding.base16;
import static java.lang.String.format;
import static java.math.RoundingMode.UNNECESSARY;
import static java.util.function.Function.identity;

public class DataType<T>
{
    private final String insertType;
    private final Type prestoResultType;
    private final Function<T, String> toLiteral;
    private final Function<T, ?> toPrestoQueryResult;

    public static DataType<Boolean> booleanDataType()
    {
        return dataType("boolean", BooleanType.BOOLEAN);
    }

    public static DataType<Long> bigintDataType()
    {
        return dataType("bigint", BigintType.BIGINT);
    }

    public static DataType<Integer> integerDataType()
    {
        return dataType("integer", IntegerType.INTEGER);
    }

    public static DataType<Short> smallintDataType()
    {
        return dataType("smallint", SmallintType.SMALLINT);
    }

    public static DataType<Byte> tinyintDataType()
    {
        return dataType("tinyint", TinyintType.TINYINT);
    }

    public static DataType<Double> doubleDataType()
    {
        return dataType("double", DoubleType.DOUBLE);
    }

    public static DataType<Float> realDataType()
    {
        return dataType("real", RealType.REAL);
    }

    public static DataType<String> varcharDataType(int size)
    {
        return varcharDataType(size, "");
    }

    public static DataType<String> varcharDataType(int size, String properties)
    {
        return varcharDataType(Optional.of(size), properties);
    }

    public static DataType<String> varcharDataType()
    {
        return varcharDataType(Optional.empty(), "");
    }

    private static DataType<String> varcharDataType(Optional<Integer> length, String properties)
    {
        String prefix = length.map(size -> "varchar(" + size + ")").orElse("varchar");
        String suffix = properties.isEmpty() ? "" : " " + properties;
        VarcharType varcharType = length.map(VarcharType::createVarcharType).orElse(createUnboundedVarcharType());
        return stringDataType(prefix + suffix, varcharType);
    }

    public static DataType<String> stringDataType(String insertType, Type prestoResultType)
    {
        return dataType(insertType, prestoResultType, DataType::quote, Function.identity());
    }

    public static DataType<String> charDataType(int length)
    {
        return charDataType(length, "");
    }

    public static DataType<String> charDataType(int length, String properties)
    {
        String suffix = properties.isEmpty() ? "" : " " + properties;
        return charDataType("char(" + length + ")" + suffix, length);
    }

    public static DataType<String> charDataType(String insertType, int length)
    {
        return dataType(insertType, createCharType(length), DataType::quote, input -> padEnd(input, length, ' '));
    }

    public static DataType<byte[]> varbinaryDataType()
    {
        return dataType("varbinary", VarbinaryType.VARBINARY, DataType::binaryLiteral, Function.identity());
    }

    public static DataType<BigDecimal> decimalDataType(int precision, int scale)
    {
        String databaseType = format("decimal(%s, %s)", precision, scale);
        return dataType(
                databaseType,
                createDecimalType(precision, scale),
                bigDecimal -> format("CAST('%s' AS %s)", bigDecimal, databaseType),
                bigDecimal -> bigDecimal.setScale(scale, UNNECESSARY));
    }

    public static DataType<LocalDate> dateDataType()
    {
        return dataType(
                "DATE",
                DATE,
                DateTimeFormatter.ofPattern("'DATE '''yyyy-MM-dd''")::format,
                identity());
    }

    private static String quote(String value)
    {
        return "'" + value + "'";
    }

    /**
     * Formats bytes using SQL standard format for binary string literal
     */
    public static String binaryLiteral(byte[] value)
    {
        return "X'" + base16().encode(value) + "'";
    }

    private static <T> DataType<T> dataType(String insertType, Type prestoResultType)
    {
        return new DataType<>(insertType, prestoResultType, Object::toString, Function.identity());
    }

    public static <T> DataType<T> dataType(String insertType, Type prestoResultType, Function<T, String> toLiteral, Function<T, ?> toPrestoQueryResult)
    {
        return new DataType<>(insertType, prestoResultType, toLiteral, toPrestoQueryResult);
    }

    private DataType(String insertType, Type prestoResultType, Function<T, String> toLiteral, Function<T, ?> toPrestoQueryResult)
    {
        this.insertType = insertType;
        this.prestoResultType = prestoResultType;
        this.toLiteral = toLiteral;
        this.toPrestoQueryResult = toPrestoQueryResult;
    }

    public String toLiteral(T inputValue)
    {
        if (inputValue == null) {
            return "NULL";
        }
        return toLiteral.apply(inputValue);
    }

    public Object toPrestoQueryResult(T inputValue)
    {
        return toPrestoQueryResult.apply(inputValue);
    }

    public String getInsertType()
    {
        return insertType;
    }

    public Type getPrestoResultType()
    {
        return prestoResultType;
    }
}
