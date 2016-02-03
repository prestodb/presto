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

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;

public class TestDoubleOperators
        extends AbstractTestFunctions
{
    @Test
    public void testLiteral()
            throws Exception
    {
        assertFunction("cast(37.7 as double)", DOUBLE, 37.7);
        assertFunction("cast(17.1 as double)", DOUBLE, 17.1);
    }

    @Test
    public void testAdd()
            throws Exception
    {
        assertFunction("cast(37.7 as double) + cast(37.7 as double)", DOUBLE, 37.7 + 37.7);
        assertFunction("cast(37.7 as double) + cast(17.1 as double)", DOUBLE, 37.7 + 17.1);
        assertFunction("cast(17.1 as double) + cast(37.7 as double)", DOUBLE, 17.1 + 37.7);
        assertFunction("cast(17.1 as double) + cast(17.1 as double)", DOUBLE, 17.1 + 17.1);
    }

    @Test
    public void testSubtract()
            throws Exception
    {
        assertFunction("cast(37.7 as double) - cast(37.7 as double)", DOUBLE, 37.7 - 37.7);
        assertFunction("cast(37.7 as double) - cast(17.1 as double)", DOUBLE, 37.7 - 17.1);
        assertFunction("cast(17.1 as double) - cast(37.7 as double)", DOUBLE, 17.1 - 37.7);
        assertFunction("cast(17.1 as double) - cast(17.1 as double)", DOUBLE, 17.1 - 17.1);
    }

    @Test
    public void testMultiply()
            throws Exception
    {
        assertFunction("cast(37.7 as double) * cast(37.7 as double)", DOUBLE, 37.7 * 37.7);
        assertFunction("cast(37.7 as double) * cast(17.1 as double)", DOUBLE, 37.7 * 17.1);
        assertFunction("cast(17.1 as double) * cast(37.7 as double)", DOUBLE, 17.1 * 37.7);
        assertFunction("cast(17.1 as double) * cast(17.1 as double)", DOUBLE, 17.1 * 17.1);
    }

    @Test
    public void testDivide()
            throws Exception
    {
        assertFunction("cast(37.7 as double) / cast(37.7 as double)", DOUBLE, 37.7 / 37.7);
        assertFunction("cast(37.7 as double) / cast(17.1 as double)", DOUBLE, 37.7 / 17.1);
        assertFunction("cast(17.1 as double) / cast(37.7 as double)", DOUBLE, 17.1 / 37.7);
        assertFunction("cast(17.1 as double) / cast(17.1 as double)", DOUBLE, 17.1 / 17.1);
    }

    @Test
    public void testModulus()
            throws Exception
    {
        assertFunction("cast(37.7 as double) % cast(37.7 as double)", DOUBLE, 37.7 % 37.7);
        assertFunction("cast(37.7 as double) % cast(17.1 as double)", DOUBLE, 37.7 % 17.1);
        assertFunction("cast(17.1 as double) % cast(37.7 as double)", DOUBLE, 17.1 % 37.7);
        assertFunction("cast(17.1 as double) % cast(17.1 as double)", DOUBLE, 17.1 % 17.1);
    }

    @Test
    public void testNegation()
            throws Exception
    {
        assertFunction("-(cast(37.7 as double))", DOUBLE, -37.7);
        assertFunction("-(cast(17.1 as double))", DOUBLE, -17.1);
    }

    @Test
    public void testEqual()
            throws Exception
    {
        assertFunction("cast(37.7 as double) = cast(37.7 as double)", BOOLEAN, true);
        assertFunction("cast(37.7 as double) = cast(17.1 as double)", BOOLEAN, false);
        assertFunction("cast(17.1 as double) = cast(37.7 as double)", BOOLEAN, false);
        assertFunction("cast(17.1 as double) = cast(17.1 as double)", BOOLEAN, true);
    }

    @Test
    public void testNotEqual()
            throws Exception
    {
        assertFunction("cast(37.7 as double) <> cast(37.7 as double)", BOOLEAN, false);
        assertFunction("cast(37.7 as double) <> cast(17.1 as double)", BOOLEAN, true);
        assertFunction("cast(17.1 as double) <> cast(37.7 as double)", BOOLEAN, true);
        assertFunction("cast(17.1 as double) <> cast(17.1 as double)", BOOLEAN, false);
    }

    @Test
    public void testLessThan()
            throws Exception
    {
        assertFunction("cast(37.7 as double) < cast(37.7 as double)", BOOLEAN, false);
        assertFunction("cast(37.7 as double) < cast(17.1 as double)", BOOLEAN, false);
        assertFunction("cast(17.1 as double) < cast(37.7 as double)", BOOLEAN, true);
        assertFunction("cast(17.1 as double) < cast(17.1 as double)", BOOLEAN, false);
    }

    @Test
    public void testLessThanOrEqual()
            throws Exception
    {
        assertFunction("cast(37.7 as double) <= cast(37.7 as double)", BOOLEAN, true);
        assertFunction("cast(37.7 as double) <= cast(17.1 as double)", BOOLEAN, false);
        assertFunction("cast(17.1 as double) <= cast(37.7 as double)", BOOLEAN, true);
        assertFunction("cast(17.1 as double) <= cast(17.1 as double)", BOOLEAN, true);
    }

    @Test
    public void testGreaterThan()
            throws Exception
    {
        assertFunction("cast(37.7 as double) > cast(37.7 as double)", BOOLEAN, false);
        assertFunction("cast(37.7 as double) > cast(17.1 as double)", BOOLEAN, true);
        assertFunction("cast(17.1 as double) > cast(37.7 as double)", BOOLEAN, false);
        assertFunction("cast(17.1 as double) > cast(17.1 as double)", BOOLEAN, false);
    }

    @Test
    public void testGreaterThanOrEqual()
            throws Exception
    {
        assertFunction("cast(37.7 as double) >= cast(37.7 as double)", BOOLEAN, true);
        assertFunction("cast(37.7 as double) >= cast(17.1 as double)", BOOLEAN, true);
        assertFunction("cast(17.1 as double) >= cast(37.7 as double)", BOOLEAN, false);
        assertFunction("cast(17.1 as double) >= cast(17.1 as double)", BOOLEAN, true);
    }

    @Test
    public void testBetween()
            throws Exception
    {
        assertFunction("cast(37.7 as double) BETWEEN cast(37.7 as double) AND cast(37.7 as double)", BOOLEAN, true);
        assertFunction("cast(37.7 as double) BETWEEN cast(37.7 as double) AND cast(17.1 as double)", BOOLEAN, false);

        assertFunction("cast(37.7 as double) BETWEEN cast(17.1 as double) AND cast(37.7 as double)", BOOLEAN, true);
        assertFunction("cast(37.7 as double) BETWEEN cast(17.1 as double) AND cast(17.1 as double)", BOOLEAN, false);

        assertFunction("cast(17.1 as double) BETWEEN cast(37.7 as double) AND cast(37.7 as double)", BOOLEAN, false);
        assertFunction("cast(17.1 as double) BETWEEN cast(37.7 as double) AND cast(17.1 as double)", BOOLEAN, false);

        assertFunction("cast(17.1 as double) BETWEEN cast(17.1 as double) AND cast(37.7 as double)", BOOLEAN, true);
        assertFunction("cast(17.1 as double) BETWEEN cast(17.1 as double) AND cast(17.1 as double)", BOOLEAN, true);
    }

    @Test
    public void testCastToVarchar()
            throws Exception
    {
        assertFunction("cast(cast(37.7 as double) as varchar)", VARCHAR, "37.7");
        assertFunction("cast(cast(17.1 as double) as varchar)", VARCHAR, "17.1");
    }

    @Test
    public void testCastToBigint()
            throws Exception
    {
        assertFunction("cast(cast(37.7 as double) as bigint)", BIGINT, 38L);
        assertFunction("cast(cast(17.1 as double) as bigint)", BIGINT, 17L);
    }

    @Test
    public void testCastToBoolean()
            throws Exception
    {
        assertFunction("cast(cast(37.7 as double) as boolean)", BOOLEAN, true);
        assertFunction("cast(cast(17.1 as double) as boolean)", BOOLEAN, true);
        assertFunction("cast(cast(0.0 as double) as boolean)", BOOLEAN, false);
    }

    @Test
    public void testCastFromVarchar()
            throws Exception
    {
        assertFunction("cast('37.7' as double)", DOUBLE, 37.7);
        assertFunction("cast('17.1' as double)", DOUBLE, 17.1);
    }
}
