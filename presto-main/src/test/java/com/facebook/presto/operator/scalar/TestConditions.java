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

import com.facebook.presto.spi.PrestoException;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestConditions
{
    private FunctionAssertions functionAssertions;

    @BeforeClass
    public void setUp()
    {
        functionAssertions = new FunctionAssertions();
    }

    @Test
    public void testLike()
    {
        assertFunction("'_monkey_' like 'X_monkeyX_' escape 'X'", true);

        assertFunction("'monkey' like 'monkey'", true);
        assertFunction("'monkey' like 'mon%'", true);
        assertFunction("'monkey' like 'mon_ey'", true);
        assertFunction("'monkey' like 'm____y'", true);

        assertFunction("'monkey' like 'dain'", false);
        assertFunction("'monkey' like 'key'", false);

        assertFunction("'_monkey_' like '\\_monkey\\_'", false);
        assertFunction("'_monkey_' like 'X_monkeyX_' escape 'X'", true);
        assertFunction("'_monkey_' like '_monkey_' escape ''", true);

        assertFunction("'*?.(){}+|^$,\\' like '*?.(){}+|^$,\\' escape ''", true);

        assertFunction("null like 'monkey'", null);
        assertFunction("'monkey' like null", null);
        assertFunction("'monkey' like 'monkey' escape null", null);

        assertFunction("'_monkey_' not like 'X_monkeyX_' escape 'X'", false);

        assertFunction("'monkey' not like 'monkey'", false);
        assertFunction("'monkey' not like 'mon%'", false);
        assertFunction("'monkey' not like 'mon_ey'", false);
        assertFunction("'monkey' not like 'm____y'", false);

        assertFunction("'monkey' not like 'dain'", true);
        assertFunction("'monkey' not like 'key'", true);

        assertFunction("'_monkey_' not like '\\_monkey\\_'", true);
        assertFunction("'_monkey_' not like 'X_monkeyX_' escape 'X'", false);
        assertFunction("'_monkey_' not like '_monkey_' escape ''", false);

        assertFunction("'*?.(){}+|^$,\\' not like '*?.(){}+|^$,\\' escape ''", false);

        assertFunction("null not like 'monkey'", null);
        assertFunction("'monkey' not like null", null);
        assertFunction("'monkey' not like 'monkey' escape null", null);
    }

    @Test
    public void testDistinctFrom()
            throws Exception
    {
        assertFunction("NULL IS DISTINCT FROM NULL", false);
        assertFunction("NULL IS DISTINCT FROM 1", true);
        assertFunction("1 IS DISTINCT FROM NULL", true);
        assertFunction("1 IS DISTINCT FROM 1", false);
        assertFunction("1 IS DISTINCT FROM 2", true);

        assertFunction("NULL IS NOT DISTINCT FROM NULL", true);
        assertFunction("NULL IS NOT DISTINCT FROM 1", false);
        assertFunction("1 IS NOT DISTINCT FROM NULL", false);
        assertFunction("1 IS NOT DISTINCT FROM 1", true);
        assertFunction("1 IS NOT DISTINCT FROM 2", false);
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = ".*escape must be empty or a single character.*")
    public void testLikeInvalidEscape()
    {
        evaluate("'monkey' like 'monkey' escape 'foo'");
    }

    @Test
    public void testBetween()
    {
        assertFunction("3 between 2 and 4", true);
        assertFunction("3 between 3 and 3", true);
        assertFunction("3 between 2 and 3", true);
        assertFunction("3 between 3 and 4", true);
        assertFunction("3 between 4 and 2", false);
        assertFunction("2 between 3 and 4", false);
        assertFunction("5 between 3 and 4", false);
        assertFunction("null between 2 and 4", null);
        assertFunction("3 between null and 4", null);
        assertFunction("3 between 2 and null", null);

        assertFunction("'c' between 'b' and 'd'", true);
        assertFunction("'c' between 'c' and 'c'", true);
        assertFunction("'c' between 'b' and 'c'", true);
        assertFunction("'c' between 'c' and 'd'", true);
        assertFunction("'c' between 'd' and 'b'", false);
        assertFunction("'b' between 'c' and 'd'", false);
        assertFunction("'e' between 'c' and 'd'", false);
        assertFunction("null between 'b' and 'd'", null);
        assertFunction("'c' between null and 'd'", null);
        assertFunction("'c' between 'b' and null", null);

        assertFunction("3 not between 2 and 4", false);
        assertFunction("3 not between 3 and 3", false);
        assertFunction("3 not between 2 and 3", false);
        assertFunction("3 not between 3 and 4", false);
        assertFunction("3 not between 4 and 2", true);
        assertFunction("2 not between 3 and 4", true);
        assertFunction("5 not between 3 and 4", true);
        assertFunction("null not between 2 and 4", null);
        assertFunction("3 not between null and 4", null);
        assertFunction("3 not between 2 and null", null);

        assertFunction("'c' not between 'b' and 'd'", false);
        assertFunction("'c' not between 'c' and 'c'", false);
        assertFunction("'c' not between 'b' and 'c'", false);
        assertFunction("'c' not between 'c' and 'd'", false);
        assertFunction("'c' not between 'd' and 'b'", true);
        assertFunction("'b' not between 'c' and 'd'", true);
        assertFunction("'e' not between 'c' and 'd'", true);
        assertFunction("null not between 'b' and 'd'", null);
        assertFunction("'c' not between null and 'd'", null);
        assertFunction("'c' not between 'b' and null", null);
    }

    @Test
    public void testIn()
    {
        assertFunction("3 in (2, 4, 3, 5)", true);
        assertFunction("3 not in (2, 4, 3, 5)", false);
        assertFunction("3 in (2, 4, 9, 5)", false);
        assertFunction("3 in (2, null, 3, 5)", true);

        assertFunction("'foo' in ('bar', 'baz', 'foo', 'blah')", true);
        assertFunction("'foo' in ('bar', 'baz', 'buz', 'blah')", false);
        assertFunction("'foo' in ('bar', null, 'foo', 'blah')", true);

        assertFunction("(null in (2, null, 3, 5)) is null", true);
        assertFunction("(3 in (2, null)) is null", true);
        assertFunction("(null not in (2, null, 3, 5)) is null", true);
        assertFunction("(3 not in (2, null)) is null", true);
    }

    @Test(expectedExceptions = PrestoException.class)
    public void testInDoesNotShortCircuit()
    {
        evaluate("3 in (2, 4, 3, 5 / 0)");
    }

    @Test
    public void testSearchCase()
    {
        assertFunction("case " +
                "when true then 33 " +
                "end",
                33L);

        assertFunction("case " +
                "when false then 1 " +
                "else 33 " +
                "end",
                33L);

        assertFunction("case " +
                "when false then 1 " +
                "when false then 1 " +
                "when true then 33 " +
                "else 1 " +
                "end",
                33L);

        assertFunction("case " +
                "when false then 1 " +
                "end",
                null);

        assertFunction("case " +
                "when true then null " +
                "else 'foo' " +
                "end",
                null);

        assertFunction("case " +
                "when null then 1 " +
                "when true then 33 " +
                "end",
                33L);

        assertFunction("case " +
                "when false then 1.0 " +
                "when true then 33 " +
                "end",
                33.0);
    }

    @Test
    public void testSimpleCase()
    {
        assertFunction("case true " +
                "when true then cast(null as varchar) " +
                "else 'foo' " +
                "end",
                null);

        assertFunction("case true " +
                "when true then 33 " +
                "end",
                33L);

        assertFunction("case true " +
                "when false then 1 " +
                "else 33 " +
                "end",
                33L);

        assertFunction("case true " +
                "when false then 1 " +
                "when false then 1 " +
                "when true then 33 " +
                "else 1 " +
                "end",
                33L);

        assertFunction("case true " +
                "when false then 1 " +
                "end",
                null);

        assertFunction("case true " +
                "when true then null " +
                "else 'foo' " +
                "end",
                null);

        assertFunction("case true " +
                "when null then 1 " +
                "when true then 33 " +
                "end",
                33L);

        assertFunction("case null " +
                "when true then 1 " +
                "else 33 " +
                "end",
                33);

        assertFunction("case true " +
                "when false then 1.0 " +
                "when true then 33 " +
                "end",
                33.0);
    }

    private void assertFunction(String projection, Object expected)
    {
        functionAssertions.assertFunction(projection, expected);
    }

    private void evaluate(String projection)
    {
        functionAssertions.tryEvaluate(projection);
    }
}
