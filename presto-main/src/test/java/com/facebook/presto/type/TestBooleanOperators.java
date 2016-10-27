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

import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;

public class TestBooleanOperators
        extends AbstractTestFunctions
{
    @Test
    public void testLiteral()
            throws Exception
    {
        assertFunction("true", BOOLEAN, true);
        assertFunction("false", BOOLEAN, false);
    }

    @Test
    public void testTypeConstructor()
            throws Exception
    {
        assertFunction("BOOLEAN 'true'", BOOLEAN, true);
        assertFunction("BOOLEAN 'false'", BOOLEAN, false);
    }

    @Test
    public void testEqual()
            throws Exception
    {
        assertFunction("true = true", BOOLEAN, true);
        assertFunction("true = false", BOOLEAN, false);
        assertFunction("false = true", BOOLEAN, false);
        assertFunction("false = false", BOOLEAN, true);
    }

    @Test
    public void testNotEqual()
            throws Exception
    {
        assertFunction("true <> true", BOOLEAN, false);
        assertFunction("true <> false", BOOLEAN, true);
        assertFunction("false <> true", BOOLEAN, true);
        assertFunction("false <> false", BOOLEAN, false);
    }

    @Test
    public void testLessThan()
            throws Exception
    {
        assertFunction("true < true", BOOLEAN, false);
        assertFunction("true < false", BOOLEAN, false);
        assertFunction("false < true", BOOLEAN, true);
        assertFunction("false < false", BOOLEAN, false);
    }

    @Test
    public void testLessThanOrEqual()
            throws Exception
    {
        assertFunction("true <= true", BOOLEAN, true);
        assertFunction("true <= false", BOOLEAN, false);
        assertFunction("false <= true", BOOLEAN, true);
        assertFunction("false <= false", BOOLEAN, true);
    }

    @Test
    public void testGreaterThan()
            throws Exception
    {
        assertFunction("true > true", BOOLEAN, false);
        assertFunction("true > false", BOOLEAN, true);
        assertFunction("false > true", BOOLEAN, false);
        assertFunction("false > false", BOOLEAN, false);
    }

    @Test
    public void testGreaterThanOrEqual()
            throws Exception
    {
        assertFunction("true >= true", BOOLEAN, true);
        assertFunction("true >= false", BOOLEAN, true);
        assertFunction("false >= true", BOOLEAN, false);
        assertFunction("false >= false", BOOLEAN, true);
    }

    @Test
    public void testBetween()
            throws Exception
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
    public void testCastToVarchar()
            throws Exception
    {
        assertFunction("cast(true as varchar)", VARCHAR, "true");
        assertFunction("cast(false as varchar)", VARCHAR, "false");
    }

    @Test
    public void testCastFromVarchar()
            throws Exception
    {
        assertFunction("cast('true' as boolean)", BOOLEAN, true);
        assertFunction("cast('false' as boolean)", BOOLEAN, false);
    }
    @Test
    public void testIsDistinctFrom()
            throws Exception
    {
        assertFunction("CAST(NULL AS BOOLEAN) IS DISTINCT FROM CAST(NULL AS BOOLEAN)", BOOLEAN, false);
        assertFunction("FALSE IS DISTINCT FROM FALSE", BOOLEAN, false);
        assertFunction("TRUE IS DISTINCT FROM TRUE", BOOLEAN, false);
        assertFunction("FALSE IS DISTINCT FROM TRUE", BOOLEAN, true);
        assertFunction("TRUE IS DISTINCT FROM FALSE", BOOLEAN, true);
        assertFunction("FALSE IS DISTINCT FROM NULL", BOOLEAN, true);
        assertFunction("TRUE IS DISTINCT FROM NULL", BOOLEAN, true);
    }
}
