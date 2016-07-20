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
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import org.testng.annotations.Test;

import javax.annotation.Nullable;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.type.UnknownType.UNKNOWN;

public class TestUnknownOperators
        extends AbstractTestFunctions
{
    private TestUnknownOperators()
    {
        registerScalar(getClass());
    }

    @ScalarFunction(value = "null_function", deterministic = false)
    @Nullable
    @SqlType("unknown")
    public static Void nullFunction()
    {
        return null;
    }

    @Test
    public void testLiteral()
            throws Exception
    {
        assertFunction("NULL", UNKNOWN, null);
    }

    @Test
    public void testEqual()
            throws Exception
    {
        assertFunction("NULL = NULL", BOOLEAN, null);
    }

    @Test
    public void testNotEqual()
            throws Exception
    {
        assertFunction("NULL <> NULL", BOOLEAN, null);
    }

    @Test
    public void testLessThan()
            throws Exception
    {
        assertFunction("NULL < NULL", BOOLEAN, null);
    }

    @Test
    public void testLessThanOrEqual()
            throws Exception
    {
        assertFunction("NULL <= NULL", BOOLEAN, null);
    }

    @Test
    public void testGreaterThan()
            throws Exception
    {
        assertFunction("NULL > NULL", BOOLEAN, null);
    }

    @Test
    public void testGreaterThanOrEqual()
            throws Exception
    {
        assertFunction("NULL >= NULL", BOOLEAN, null);
    }

    @Test
    public void testBetween()
            throws Exception
    {
        assertFunction("NULL BETWEEN NULL AND NULL", BOOLEAN, null);
    }

    @Test
    public void testCastToBigint()
            throws Exception
    {
        assertFunction("cast(NULL as bigint)", BIGINT, null);
        assertFunction("cast(null_function() as bigint)", BIGINT, null);
    }

    @Test
    public void testCastToVarchar()
            throws Exception
    {
        assertFunction("cast(NULL as varchar)", VARCHAR, null);
        assertFunction("cast(null_function() as varchar)", VARCHAR, null);
    }

    @Test
    public void testCastToDouble()
            throws Exception
    {
        assertFunction("cast(NULL as double)", DOUBLE, null);
        assertFunction("cast(null_function() as double)", DOUBLE, null);
    }

    @Test
    public void testCastToBoolean()
            throws Exception
    {
        assertFunction("cast(NULL as boolean)", BOOLEAN, null);
        assertFunction("cast(null_function() as boolean)", BOOLEAN, null);
    }
}
