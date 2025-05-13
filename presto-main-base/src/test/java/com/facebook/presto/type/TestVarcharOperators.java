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
package com.facebook.presto.type;

import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.SmallintType;
import com.facebook.presto.common.type.TinyintType;
import com.facebook.presto.operator.scalar.AbstractTestFunctions;
import org.testng.annotations.Test;

import static com.facebook.presto.common.function.OperatorType.INDETERMINATE;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.common.type.VarcharType.createVarcharType;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;

public class TestVarcharOperators
        extends AbstractTestFunctions
{
    @Test
    public void testLiteral()
    {
        assertFunction("'foo'", createVarcharType(3), "foo");
        assertFunction("'bar'", createVarcharType(3), "bar");
        assertFunction("''", createVarcharType(0), "");
    }

    @Test
    public void testTypeConstructor()
    {
        assertFunction("VARCHAR 'foo'", VARCHAR, "foo");
        assertFunction("VARCHAR 'bar'", VARCHAR, "bar");
        assertFunction("VARCHAR ''", VARCHAR, "");
    }

    @Test
    public void testAdd()
    {
        // TODO change expected return type to createVarcharType(6) when function resolving is fixed
        assertFunction("'foo' || 'foo'", VARCHAR, "foo" + "foo");
        assertFunction("'foo' || 'bar'", VARCHAR, "foo" + "bar");
        assertFunction("'bar' || 'foo'", VARCHAR, "bar" + "foo");
        assertFunction("'bar' || 'bar'", VARCHAR, "bar" + "bar");
        assertFunction("'bar' || 'barbaz'", VARCHAR, "bar" + "barbaz");
    }

    @Test
    public void testEqual()
    {
        assertFunction("'foo' = 'foo'", BOOLEAN, true);
        assertFunction("'foo' = 'bar'", BOOLEAN, false);
        assertFunction("'bar' = 'foo'", BOOLEAN, false);
        assertFunction("'bar' = 'bar'", BOOLEAN, true);
    }

    @Test
    public void testNotEqual()
    {
        assertFunction("'foo' <> 'foo'", BOOLEAN, false);
        assertFunction("'foo' <> 'bar'", BOOLEAN, true);
        assertFunction("'bar' <> 'foo'", BOOLEAN, true);
        assertFunction("'bar' <> 'bar'", BOOLEAN, false);
    }

    @Test
    public void testLessThan()
    {
        assertFunction("'foo' < 'foo'", BOOLEAN, false);
        assertFunction("'foo' < 'bar'", BOOLEAN, false);
        assertFunction("'bar' < 'foo'", BOOLEAN, true);
        assertFunction("'bar' < 'bar'", BOOLEAN, false);
    }

    @Test
    public void testLessThanOrEqual()
    {
        assertFunction("'foo' <= 'foo'", BOOLEAN, true);
        assertFunction("'foo' <= 'bar'", BOOLEAN, false);
        assertFunction("'bar' <= 'foo'", BOOLEAN, true);
        assertFunction("'bar' <= 'bar'", BOOLEAN, true);
    }

    @Test
    public void testGreaterThan()
    {
        assertFunction("'foo' > 'foo'", BOOLEAN, false);
        assertFunction("'foo' > 'bar'", BOOLEAN, true);
        assertFunction("'bar' > 'foo'", BOOLEAN, false);
        assertFunction("'bar' > 'bar'", BOOLEAN, false);
    }

    @Test
    public void testGreaterThanOrEqual()
    {
        assertFunction("'foo' >= 'foo'", BOOLEAN, true);
        assertFunction("'foo' >= 'bar'", BOOLEAN, true);
        assertFunction("'bar' >= 'foo'", BOOLEAN, false);
        assertFunction("'bar' >= 'bar'", BOOLEAN, true);
    }

    @Test
    public void testBetween()
    {
        assertFunction("'foo' BETWEEN 'foo' AND 'foo'", BOOLEAN, true);
        assertFunction("'foo' BETWEEN 'foo' AND 'bar'", BOOLEAN, false);

        assertFunction("'foo' BETWEEN 'bar' AND 'foo'", BOOLEAN, true);
        assertFunction("'foo' BETWEEN 'bar' AND 'bar'", BOOLEAN, false);

        assertFunction("'bar' BETWEEN 'foo' AND 'foo'", BOOLEAN, false);
        assertFunction("'bar' BETWEEN 'foo' AND 'bar'", BOOLEAN, false);

        assertFunction("'bar' BETWEEN 'bar' AND 'foo'", BOOLEAN, true);
        assertFunction("'bar' BETWEEN 'bar' AND 'bar'", BOOLEAN, true);
    }

    @Test
    public void testIsDistinctFrom()
    {
        assertFunction("CAST(NULL AS VARCHAR) IS DISTINCT FROM CAST(NULL AS VARCHAR)", BOOLEAN, false);
        assertFunction("'foo' IS DISTINCT FROM 'foo'", BOOLEAN, false);
        assertFunction("'foo' IS DISTINCT FROM 'fo0'", BOOLEAN, true);
        assertFunction("NULL IS DISTINCT FROM 'foo'", BOOLEAN, true);
        assertFunction("'foo' IS DISTINCT FROM NULL", BOOLEAN, true);
    }

    @Test
    public void testIndeterminate()
    {
        assertOperator(INDETERMINATE, "cast(null as varchar)", BOOLEAN, true);
        assertOperator(INDETERMINATE, "'foo'", BOOLEAN, false);
        assertOperator(INDETERMINATE, "cast(123456 as varchar)", BOOLEAN, false);
        assertOperator(INDETERMINATE, "cast(12345.0123 as varchar)", BOOLEAN, false);
        assertOperator(INDETERMINATE, "cast(true as varchar)", BOOLEAN, false);
    }

    @Test
    public void testCastVarcharAsInteger()
    {
        assertFunction("CAST('6' AS BIGINT)", BigintType.BIGINT, 6L);
        assertFunction("CAST('7' AS INTEGER)", IntegerType.INTEGER, 7);
        assertFunction("CAST('8' AS SMALLINT)", SmallintType.SMALLINT, (short) 8);
        assertFunction("CAST('9' AS TINYINT)", TinyintType.TINYINT, (byte) 9);

        assertFunction("CAST(' 6    ' AS BIGINT)", BigintType.BIGINT, 6L);
        assertFunction("CAST('  7   ' AS INTEGER)", IntegerType.INTEGER, 7);
        assertFunction("CAST('   8  ' AS SMALLINT)", SmallintType.SMALLINT, (short) 8);
        assertFunction("CAST('    9 ' AS TINYINT)", TinyintType.TINYINT, (byte) 9);

        assertInvalidFunction("CAST('6 7' AS BIGINT)", INVALID_CAST_ARGUMENT, "Cannot cast '6 7' to BIGINT");
        assertInvalidFunction("CAST('7 8' AS INTEGER)", INVALID_CAST_ARGUMENT, "Cannot cast '7 8' to INT");
        assertInvalidFunction("CAST('8 9' AS SMALLINT)", INVALID_CAST_ARGUMENT, "Cannot cast '8 9' to SMALLINT");
        assertInvalidFunction("CAST('9 6' AS TINYINT)", INVALID_CAST_ARGUMENT, "Cannot cast '9 6' to TINYINT");
    }
}
