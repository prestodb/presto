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
package io.prestosql.operator.scalar;

import io.airlift.slice.Slice;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.function.Description;
import io.prestosql.spi.function.LiteralParameters;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlType;

import java.math.BigDecimal;
import java.math.BigInteger;

import static io.prestosql.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.prestosql.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static io.prestosql.spi.type.Decimals.encodeUnscaledValue;
import static java.lang.Character.isDigit;
import static java.lang.String.format;

public final class DataSizeFunctions
{
    private DataSizeFunctions() {}

    @Description("converts data size string to bytes")
    @ScalarFunction("parse_presto_data_size")
    @LiteralParameters("x")
    @SqlType("decimal(38,0)")
    public static Slice parsePrestoDataSize(@SqlType("varchar(x)") Slice input)
    {
        String dataSize = input.toStringUtf8();

        int valueLength = 0;
        for (int i = 0; i < dataSize.length(); i++) {
            char c = dataSize.charAt(i);
            if (isDigit(c) || c == '.') {
                valueLength++;
            }
            else {
                break;
            }
        }

        if (valueLength == 0) {
            throw invalidDataSize(dataSize);
        }

        BigDecimal value = parseValue(dataSize.substring(0, valueLength), dataSize);
        Unit unit = Unit.parse(dataSize.substring(valueLength), dataSize);
        BigInteger bytes = value.multiply(unit.getFactor()).toBigInteger();
        try {
            return encodeUnscaledValue(bytes);
        }
        catch (ArithmeticException e) {
            throw new PrestoException(NUMERIC_VALUE_OUT_OF_RANGE, format("Value out of range: '%s' ('%sB')", dataSize, bytes));
        }
    }

    private static BigDecimal parseValue(String value, String dataSize)
    {
        try {
            return new BigDecimal(value);
        }
        catch (NumberFormatException e) {
            throw invalidDataSize(dataSize);
        }
    }

    private static PrestoException invalidDataSize(String dataSize)
    {
        return new PrestoException(INVALID_FUNCTION_ARGUMENT, format("Invalid data size: '%s'", dataSize));
    }

    private enum Unit
    {
        BYTE(BigDecimal.ONE),
        KILOBYTE(new BigDecimal(1L << 10)),
        MEGABYTE(new BigDecimal(1L << 20)),
        GIGABYTE(new BigDecimal(1L << 30)),
        TERABYTE(new BigDecimal(1L << 40)),
        PETABYTE(new BigDecimal(1L << 50)),
        EXABYTE(new BigDecimal(1L << 60)),
        ZETTABYTE(new BigDecimal(1L << 60).multiply(new BigDecimal(1L << 10))),
        YOTTABYTE(new BigDecimal(1L << 60).multiply(new BigDecimal(1L << 20)));

        private final BigDecimal factor;

        Unit(BigDecimal factor)
        {
            this.factor = factor;
        }

        public BigDecimal getFactor()
        {
            return factor;
        }

        public static Unit parse(String unitString, String dataSize)
        {
            switch (unitString) {
                case "B":
                    return BYTE;
                case "kB":
                    return KILOBYTE;
                case "MB":
                    return MEGABYTE;
                case "GB":
                    return GIGABYTE;
                case "TB":
                    return TERABYTE;
                case "PB":
                    return PETABYTE;
                case "EB":
                    return EXABYTE;
                case "ZB":
                    return ZETTABYTE;
                case "YB":
                    return YOTTABYTE;
                default:
                    throw invalidDataSize(dataSize);
            }
        }
    }
}
