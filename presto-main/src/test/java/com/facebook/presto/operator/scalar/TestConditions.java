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

import org.testng.annotations.Test;

import static com.facebook.presto.spi.StandardErrorCode.DIVISION_BY_ZERO;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.spi.type.VarcharType.createVarcharType;

public class TestConditions
        extends AbstractTestFunctions
{
    @Test
    public void testLike()
    {
        assertFunction("'_monkey_' like 'X_monkeyX_' escape 'X'", BOOLEAN, true);

        assertFunction("'monkey' like 'monkey'", BOOLEAN, true);
        assertFunction("'monkey' like 'mon%'", BOOLEAN, true);
        assertFunction("'monkey' like 'mon_ey'", BOOLEAN, true);
        assertFunction("'monkey' like 'm____y'", BOOLEAN, true);

        assertFunction("'monkey' like 'dain'", BOOLEAN, false);
        assertFunction("'monkey' like 'key'", BOOLEAN, false);

        assertFunction("'_monkey_' like '\\_monkey\\_'", BOOLEAN, false);
        assertFunction("'_monkey_' like 'X_monkeyX_' escape 'X'", BOOLEAN, true);
        assertFunction("'_monkey_' like '_monkey_' escape ''", BOOLEAN, true);

        assertFunction("'*?.(){}+|^$,\\' like '*?.(){}+|^$,\\' escape ''", BOOLEAN, true);

        assertFunction("null like 'monkey'", BOOLEAN, null);
        assertFunction("'monkey' like null", BOOLEAN, null);
        assertFunction("'monkey' like 'monkey' escape null", BOOLEAN, null);

        assertFunction("'_monkey_' not like 'X_monkeyX_' escape 'X'", BOOLEAN, false);

        assertFunction("'monkey' not like 'monkey'", BOOLEAN, false);
        assertFunction("'monkey' not like 'mon%'", BOOLEAN, false);
        assertFunction("'monkey' not like 'mon_ey'", BOOLEAN, false);
        assertFunction("'monkey' not like 'm____y'", BOOLEAN, false);

        assertFunction("'monkey' not like 'dain'", BOOLEAN, true);
        assertFunction("'monkey' not like 'key'", BOOLEAN, true);

        assertFunction("'_monkey_' not like '\\_monkey\\_'", BOOLEAN, true);
        assertFunction("'_monkey_' not like 'X_monkeyX_' escape 'X'", BOOLEAN, false);
        assertFunction("'_monkey_' not like '_monkey_' escape ''", BOOLEAN, false);

        assertFunction("'*?.(){}+|^$,\\' not like '*?.(){}+|^$,\\' escape ''", BOOLEAN, false);

        assertFunction("null not like 'monkey'", BOOLEAN, null);
        assertFunction("'monkey' not like null", BOOLEAN, null);
        assertFunction("'monkey' not like 'monkey' escape null", BOOLEAN, null);

        assertInvalidFunction("'monkey' like 'monkey' escape 'foo'", "Escape must be empty or a single character");
    }

    @Test
    public void testDistinctFrom()
            throws Exception
    {
        assertFunction("NULL IS DISTINCT FROM NULL", BOOLEAN, false);
        assertFunction("NULL IS DISTINCT FROM 1", BOOLEAN, true);
        assertFunction("1 IS DISTINCT FROM NULL", BOOLEAN, true);
        assertFunction("1 IS DISTINCT FROM 1", BOOLEAN, false);
        assertFunction("1 IS DISTINCT FROM 2", BOOLEAN, true);

        assertFunction("NULL IS NOT DISTINCT FROM NULL", BOOLEAN, true);
        assertFunction("NULL IS NOT DISTINCT FROM 1", BOOLEAN, false);
        assertFunction("1 IS NOT DISTINCT FROM NULL", BOOLEAN, false);
        assertFunction("1 IS NOT DISTINCT FROM 1", BOOLEAN, true);
        assertFunction("1 IS NOT DISTINCT FROM 2", BOOLEAN, false);
    }

    @Test
    public void testBetween()
    {
        assertFunction("3 between 2 and 4", BOOLEAN, true);
        assertFunction("3 between 3 and 3", BOOLEAN, true);
        assertFunction("3 between 2 and 3", BOOLEAN, true);
        assertFunction("3 between 3 and 4", BOOLEAN, true);
        assertFunction("3 between 4 and 2", BOOLEAN, false);
        assertFunction("2 between 3 and 4", BOOLEAN, false);
        assertFunction("5 between 3 and 4", BOOLEAN, false);
        assertFunction("null between 2 and 4", BOOLEAN, null);
        assertFunction("3 between null and 4", BOOLEAN, null);
        assertFunction("3 between 2 and null", BOOLEAN, null);

        assertFunction("3 between 3 and 4000000000", BOOLEAN, true);
        assertFunction("5 between 3 and 4000000000", BOOLEAN, true);
        assertFunction("3 between BIGINT '3' and 4", BOOLEAN, true);
        assertFunction("BIGINT '3' between 3 and 4", BOOLEAN, true);

        assertFunction("'c' between 'b' and 'd'", BOOLEAN, true);
        assertFunction("'c' between 'c' and 'c'", BOOLEAN, true);
        assertFunction("'c' between 'b' and 'c'", BOOLEAN, true);
        assertFunction("'c' between 'c' and 'd'", BOOLEAN, true);
        assertFunction("'c' between 'd' and 'b'", BOOLEAN, false);
        assertFunction("'b' between 'c' and 'd'", BOOLEAN, false);
        assertFunction("'e' between 'c' and 'd'", BOOLEAN, false);
        assertFunction("null between 'b' and 'd'", BOOLEAN, null);
        assertFunction("'c' between null and 'd'", BOOLEAN, null);
        assertFunction("'c' between 'b' and null", BOOLEAN, null);

        assertFunction("3 not between 2 and 4", BOOLEAN, false);
        assertFunction("3 not between 3 and 3", BOOLEAN, false);
        assertFunction("3 not between 2 and 3", BOOLEAN, false);
        assertFunction("3 not between 3 and 4", BOOLEAN, false);
        assertFunction("3 not between 4 and 2", BOOLEAN, true);
        assertFunction("2 not between 3 and 4", BOOLEAN, true);
        assertFunction("5 not between 3 and 4", BOOLEAN, true);
        assertFunction("null not between 2 and 4", BOOLEAN, null);
        assertFunction("3 not between null and 4", BOOLEAN, null);
        assertFunction("3 not between 2 and null", BOOLEAN, null);

        assertFunction("'c' not between 'b' and 'd'", BOOLEAN, false);
        assertFunction("'c' not between 'c' and 'c'", BOOLEAN, false);
        assertFunction("'c' not between 'b' and 'c'", BOOLEAN, false);
        assertFunction("'c' not between 'c' and 'd'", BOOLEAN, false);
        assertFunction("'c' not between 'd' and 'b'", BOOLEAN, true);
        assertFunction("'b' not between 'c' and 'd'", BOOLEAN, true);
        assertFunction("'e' not between 'c' and 'd'", BOOLEAN, true);
        assertFunction("null not between 'b' and 'd'", BOOLEAN, null);
        assertFunction("'c' not between null and 'd'", BOOLEAN, null);
        assertFunction("'c' not between 'b' and null", BOOLEAN, null);
    }

    @Test
    public void testIn()
    {
        assertFunction("3 in (2, 4, 3, 5)", BOOLEAN, true);
        assertFunction("3 not in (2, 4, 3, 5)", BOOLEAN, false);
        assertFunction("3 in (2, 4, 9, 5)", BOOLEAN, false);
        assertFunction("3 in (2, null, 3, 5)", BOOLEAN, true);

        assertFunction("'foo' in ('bar', 'baz', 'foo', 'blah')", BOOLEAN, true);
        assertFunction("'foo' in ('bar', 'baz', 'buz', 'blah')", BOOLEAN, false);
        assertFunction("'foo' in ('bar', null, 'foo', 'blah')", BOOLEAN, true);

        assertFunction("(null in (2, null, 3, 5)) is null", BOOLEAN, true);
        assertFunction("(3 in (2, null)) is null", BOOLEAN, true);
        assertFunction("(null not in (2, null, 3, 5)) is null", BOOLEAN, true);
        assertFunction("(3 not in (2, null)) is null", BOOLEAN, true);
    }

    @Test
    public void testInDoesNotShortCircuit()
    {
        assertInvalidFunction("3 in (2, 4, 3, 5 / 0)", DIVISION_BY_ZERO);
    }

    @Test
    public void testSearchCase()
    {
        assertFunction("case " +
                        "when true then 33 " +
                        "end",
                INTEGER,
                33);

        assertFunction("case " +
                        "when true then BIGINT '33' " +
                        "end",
                BIGINT,
                33L);

        assertFunction("case " +
                        "when false then 1 " +
                        "else 33 " +
                        "end",
                INTEGER,
                33);

        assertFunction("case " +
                        "when false then 10000000000 " +
                        "else 33 " +
                        "end",
                BIGINT,
                33L);

        assertFunction("case " +
                        "when false then 1 " +
                        "when false then 1 " +
                        "when true then 33 " +
                        "else 1 " +
                        "end",
                INTEGER,
                33);

        assertFunction("case " +
                        "when false then BIGINT '1' " +
                        "when false then 1 " +
                        "when true then 33 " +
                        "else 1 " +
                        "end",
                BIGINT,
                33L);

        assertFunction("case " +
                        "when false then 10000000000 " +
                        "when false then 1 " +
                        "when true then 33 " +
                        "else 1 " +
                        "end",
                BIGINT,
                33L);

        assertFunction("case " +
                        "when false then 1 " +
                        "end",
                INTEGER,
                null);

        assertFunction("case " +
                        "when true then null " +
                        "else 'foo' " +
                        "end",
                createVarcharType(3),
                null);

        assertFunction("case " +
                        "when null then 1 " +
                        "when true then 33 " +
                        "end",
                INTEGER,
                33);

        assertFunction("case " +
                        "when null then 10000000000 " +
                        "when true then 33 " +
                        "end",
                BIGINT,
                33L);

        assertFunction("case " +
                        "when false then 1.0 " +
                        "when true then 33 " +
                        "end",
                DOUBLE,
                33.0);
    }

    @Test
    public void testSimpleCase()
    {
        assertFunction("case true " +
                        "when true then cast(null as varchar) " +
                        "else 'foo' " +
                        "end",
                VARCHAR,
                null);

        assertFunction("case true " +
                        "when true then 33 " +
                        "end",
                INTEGER,
                33);

        assertFunction("case true " +
                        "when true then BIGINT '33' " +
                        "end",
                BIGINT,
                33L);

        assertFunction("case true " +
                        "when false then 1 " +
                        "else 33 " +
                        "end",
                INTEGER,
                33);

        assertFunction("case true " +
                        "when false then 10000000000 " +
                        "else 33 " +
                        "end",
                BIGINT,
                33L);

        assertFunction("case true " +
                        "when false then 1 " +
                        "when false then 1 " +
                        "when true then 33 " +
                        "else 1 " +
                        "end",
                INTEGER,
                33);

        assertFunction("case true " +
                        "when false then 1 " +
                        "end",
                INTEGER,
                null);

        assertFunction("case true " +
                        "when true then null " +
                        "else 'foo' " +
                        "end",
                createVarcharType(3),
                null);

        assertFunction("case true " +
                        "when null then 10000000000 " +
                        "when true then 33 " +
                        "end",
                BIGINT,
                33L);

        assertFunction("case true " +
                        "when null then 1 " +
                        "when true then 33 " +
                        "end",
                INTEGER,
                33);

        assertFunction("case null " +
                        "when true then 1 " +
                        "else 33 " +
                        "end",
                INTEGER,
                33);

        assertFunction("case true " +
                        "when false then 1.0 " +
                        "when true then 33 " +
                        "end",
                DOUBLE,
                33.0);
    }
}
