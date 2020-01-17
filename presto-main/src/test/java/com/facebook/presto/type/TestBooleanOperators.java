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

import com.facebook.presto.operator.scalar.AbstractTestFunctions;
import org.testng.annotations.Test;

import static com.facebook.presto.spi.function.OperatorType.INDETERMINATE;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;

public class TestBooleanOperators
        extends AbstractTestFunctions
{
    @Test
    public void testLiteral()
    {
        assertFunction("true", BOOLEAN, true);
        assertFunction("false", BOOLEAN, false);
    }

    @Test
    public void testTypeConstructor()
    {
        assertFunction("BOOLEAN 'true'", BOOLEAN, true);
        assertFunction("BOOLEAN 'false'", BOOLEAN, false);
    }

    @Test
    public void testEqual()
    {
        assertFunction("true = true", BOOLEAN, true);
        assertFunction("true = false", BOOLEAN, false);
        assertFunction("false = true", BOOLEAN, false);
        assertFunction("false = false", BOOLEAN, true);
    }

    @Test
    public void testNotEqual()
    {
        assertFunction("true <> true", BOOLEAN, false);
        assertFunction("true <> false", BOOLEAN, true);
        assertFunction("false <> true", BOOLEAN, true);
        assertFunction("false <> false", BOOLEAN, false);
    }

    @Test
    public void testLessThan()
    {
        assertFunction("true < true", BOOLEAN, false);
        assertFunction("true < false", BOOLEAN, false);
        assertFunction("false < true", BOOLEAN, true);
        assertFunction("false < false", BOOLEAN, false);
    }

    @Test
    public void testLessThanOrEqual()
    {
        assertFunction("true <= true", BOOLEAN, true);
        assertFunction("true <= false", BOOLEAN, false);
        assertFunction("false <= true", BOOLEAN, true);
        assertFunction("false <= false", BOOLEAN, true);
    }

    @Test
    public void testGreaterThan()
    {
        assertFunction("true > true", BOOLEAN, false);
        assertFunction("true > false", BOOLEAN, true);
        assertFunction("false > true", BOOLEAN, false);
        assertFunction("false > false", BOOLEAN, false);
    }

    @Test
    public void testGreaterThanOrEqual()
    {
        assertFunction("true >= true", BOOLEAN, true);
        assertFunction("true >= false", BOOLEAN, true);
        assertFunction("false >= true", BOOLEAN, false);
        assertFunction("false >= false", BOOLEAN, true);
    }

    @Test
    public void testBetween()
    {
        assertFunction("true BETWEEN true AND true", BOOLEAN, true);
        assertFunction("true BETWEEN true AND false", BOOLEAN, false);
        assertFunction("true BETWEEN false AND true", BOOLEAN, true);
        assertFunction("true BETWEEN false AND false", BOOLEAN, false);
        assertFunction("false BETWEEN true AND true", BOOLEAN, false);
        assertFunction("false BETWEEN true AND false", BOOLEAN, false);
        assertFunction("false BETWEEN false AND true", BOOLEAN, true);
        assertFunction("false BETWEEN false AND false", BOOLEAN, true);
    }

    @Test
    public void testCastToReal()
    {
        assertFunction("cast(true as real)", REAL, 1.0f);
        assertFunction("cast(false as real)", REAL, 0.0f);
    }

    @Test
    public void testCastToVarchar()
    {
        assertFunction("cast(true as varchar)", VARCHAR, "true");
        assertFunction("cast(false as varchar)", VARCHAR, "false");
    }

    @Test
    public void testCastFromVarchar()
    {
        assertFunction("cast('true' as boolean)", BOOLEAN, true);
        assertFunction("cast('false' as boolean)", BOOLEAN, false);
    }

    @Test
    public void testIsDistinctFrom()
    {
        assertFunction("CAST(NULL AS BOOLEAN) IS DISTINCT FROM CAST(NULL AS BOOLEAN)", BOOLEAN, false);
        assertFunction("FALSE IS DISTINCT FROM FALSE", BOOLEAN, false);
        assertFunction("TRUE IS DISTINCT FROM TRUE", BOOLEAN, false);
        assertFunction("FALSE IS DISTINCT FROM TRUE", BOOLEAN, true);
        assertFunction("TRUE IS DISTINCT FROM FALSE", BOOLEAN, true);
        assertFunction("FALSE IS DISTINCT FROM NULL", BOOLEAN, true);
        assertFunction("TRUE IS DISTINCT FROM NULL", BOOLEAN, true);
    }

    @Test
    public void testIndeterminate()
    {
        assertOperator(INDETERMINATE, "cast(null AS BOOLEAN)", BOOLEAN, true);
        assertOperator(INDETERMINATE, "true", BOOLEAN, false);
        assertOperator(INDETERMINATE, "false", BOOLEAN, false);
        assertOperator(INDETERMINATE, "true AND false", BOOLEAN, false);
        assertOperator(INDETERMINATE, "true OR false", BOOLEAN, false);
    }
}
