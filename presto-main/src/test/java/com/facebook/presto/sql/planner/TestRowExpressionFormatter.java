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
package com.facebook.presto.sql.planner;

import com.facebook.presto.spi.block.LongArrayBlockBuilder;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.type.ArrayType;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.Decimals;
import com.facebook.presto.sql.planner.planPrinter.RowExpressionFormatter;
import com.google.common.io.BaseEncoding;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.testng.annotations.Test;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.CharType.createCharType;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.sql.relational.Expressions.constant;
import static com.facebook.presto.sql.relational.Expressions.constantNull;
import static com.facebook.presto.type.ColorType.COLOR;
import static com.facebook.presto.type.UnknownType.UNKNOWN;
import static java.lang.Float.floatToIntBits;
import static org.testng.Assert.assertEquals;

public class TestRowExpressionFormatter
{
    private static final RowExpressionFormatter FORMATTER = new RowExpressionFormatter(TEST_SESSION.toConnectorSession());
    @Test
    public void testConstants()
    {
        // null
        RowExpression constantExpression = constantNull(UNKNOWN);
        assertEquals(format(constantExpression), "null");

        // boolean
        constantExpression = constant(true, BOOLEAN);
        assertEquals(format(constantExpression), "BOOLEAN true");

        // double
        constantExpression = constant(1.1, DOUBLE);
        assertEquals(format(constantExpression), "DOUBLE 1.1");
        constantExpression = constant(Double.NaN, DOUBLE);
        assertEquals(format(constantExpression), "DOUBLE NaN");
        constantExpression = constant(Double.POSITIVE_INFINITY, DOUBLE);
        assertEquals(format(constantExpression), "DOUBLE Infinity");

        // real
        constantExpression = constant((long) floatToIntBits(1.1f), REAL);
        assertEquals(format(constantExpression), "REAL 1.1");
        constantExpression = constant((long) floatToIntBits(Float.NaN), REAL);
        assertEquals(format(constantExpression), "REAL NaN");
        constantExpression = constant((long) floatToIntBits(Float.POSITIVE_INFINITY), REAL);
        assertEquals(format(constantExpression), "REAL Infinity");

        // string
        constantExpression = constant(Slices.utf8Slice("abcde"), VARCHAR);
        assertEquals(format(constantExpression), "VARCHAR abcde");
        constantExpression = constant(Slices.utf8Slice("fgh"), createCharType(3));
        assertEquals(format(constantExpression), "CHAR(3) fgh");

        // integer
        constantExpression = constant(1L, TINYINT);
        assertEquals(format(constantExpression), "TINYINT 1");
        constantExpression = constant(1L, SMALLINT);
        assertEquals(format(constantExpression), "SMALLINT 1");
        constantExpression = constant(1L, INTEGER);
        assertEquals(format(constantExpression), "INTEGER 1");
        constantExpression = constant(1L, BIGINT);
        assertEquals(format(constantExpression), "BIGINT 1");

        // varbinary
        Slice value = Slices.wrappedBuffer(BaseEncoding.base16().decode("123456"));
        constantExpression = constant(value, VARBINARY);
        assertEquals(format(constantExpression), "VARBINARY 12 34 56");

        // color
        constantExpression = constant(256, COLOR);
        assertEquals(format(constantExpression), "COLOR 256");

        // long and short decimals
        constantExpression = constant(decimal("1.2345678910"), DecimalType.createDecimalType(11, 10));
        assertEquals(format(constantExpression), "DECIMAL(11,10) 1.2345678910");
        constantExpression = constant(decimal("1.281734081274028174012432412423134"), DecimalType.createDecimalType(34, 33));
        assertEquals(format(constantExpression), "DECIMAL(34,33) 1.281734081274028174012432412423134");

        // time
        constantExpression = constant(662727600000L, TIMESTAMP);
        assertEquals(format(constantExpression), "TIMESTAMP 1991-01-01 00:00:00.000");
        constantExpression = constant(7670L, DATE);
        assertEquals(format(constantExpression), "DATE 1991-01-01");

        // block
        constantExpression = constant(new LongArrayBlockBuilder(null, 4).writeLong(1L).writeLong(2).build(), new ArrayType(BIGINT));
        assertEquals(format(constantExpression), "[Block: position count: 2; size: 96 bytes]");
    }

    protected static Object decimal(String decimalString)
    {
        return Decimals.parseIncludeLeadingZerosInPrecision(decimalString).getObject();
    }

    private static String format(RowExpression expression)
    {
        return FORMATTER.formatRowExpression(expression);
    }
}
