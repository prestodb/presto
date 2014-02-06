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

import com.facebook.presto.sql.analyzer.SemanticException;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.fail;

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
        functionAssertions.assertFunction("'_monkey_' like 'X_monkeyX_' escape 'X'", true);

        functionAssertions.assertFunction("'monkey' like 'monkey'", true);
        functionAssertions.assertFunction("'monkey' like 'mon%'", true);
        functionAssertions.assertFunction("'monkey' like 'mon_ey'", true);
        functionAssertions.assertFunction("'monkey' like 'm____y'", true);

        functionAssertions.assertFunction("'monkey' like 'dain'", false);
        functionAssertions.assertFunction("'monkey' like 'key'", false);

        functionAssertions.assertFunction("'_monkey_' like '\\_monkey\\_'", false);
        functionAssertions.assertFunction("'_monkey_' like 'X_monkeyX_' escape 'X'", true);
        functionAssertions.assertFunction("'_monkey_' like '_monkey_' escape ''", true);

        functionAssertions.assertFunction("'*?.(){}+|^$,\\' like '*?.(){}+|^$,\\' escape ''", true);

        functionAssertions.assertFunction("null like 'monkey'", null);
        functionAssertions.assertFunction("'monkey' like null", null);
        functionAssertions.assertFunction("'monkey' like 'monkey' escape null", null);
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = ".*escape must be empty or a single character.*")
    public void testLikeInvalidEscape()
    {
        functionAssertions.selectSingleValue("'monkey' like 'monkey' escape 'foo'");
    }

    @Test
    public void testBetween()
    {
        functionAssertions.assertFunction("3 between 2 and 4", true);
        functionAssertions.assertFunction("3 between 3 and 3", true);
        functionAssertions.assertFunction("3 between 2 and 3", true);
        functionAssertions.assertFunction("3 between 3 and 4", true);
        functionAssertions.assertFunction("3 between 4 and 2", false);
        functionAssertions.assertFunction("2 between 3 and 4", false);
        functionAssertions.assertFunction("5 between 3 and 4", false);
        functionAssertions.assertFunction("null between 2 and 4", null);
        functionAssertions.assertFunction("3 between null and 4", null);
        functionAssertions.assertFunction("3 between 2 and null", null);

        functionAssertions.assertFunction("'c' between 'b' and 'd'", true);
        functionAssertions.assertFunction("'c' between 'c' and 'c'", true);
        functionAssertions.assertFunction("'c' between 'b' and 'c'", true);
        functionAssertions.assertFunction("'c' between 'c' and 'd'", true);
        functionAssertions.assertFunction("'c' between 'd' and 'b'", false);
        functionAssertions.assertFunction("'b' between 'c' and 'd'", false);
        functionAssertions.assertFunction("'e' between 'c' and 'd'", false);
        functionAssertions.assertFunction("null between 'b' and 'd'", null);
        functionAssertions.assertFunction("'c' between null and 'd'", null);
        functionAssertions.assertFunction("'c' between 'b' and null", null);
    }

    @Test
    public void testIn()
    {
        functionAssertions.assertFunction("3 in (2, 4, 3, 5)", true);
        functionAssertions.assertFunction("3 not in (2, 4, 3, 5)", false);
        functionAssertions.assertFunction("3 in (2, 4, 9, 5)", false);
        functionAssertions.assertFunction("3 in (2, null, 3, 5)", true);

        functionAssertions.assertFunction("'foo' in ('bar', 'baz', 'foo', 'blah')", true);
        functionAssertions.assertFunction("'foo' in ('bar', 'baz', 'buz', 'blah')", false);
        functionAssertions.assertFunction("'foo' in ('bar', null, 'foo', 'blah')", true);

        functionAssertions.assertFunction("null in (2, null, 3, 5) is null", true);
        functionAssertions.assertFunction("3 in (2, null) is null", true);
        functionAssertions.assertFunction("null not in (2, null, 3, 5) is null", true);
        functionAssertions.assertFunction("3 not in (2, null) is null", true);
    }

    @Test(expectedExceptions = ArithmeticException.class)
    public void testInDoesNotShortCircuit()
    {
        functionAssertions.selectSingleValue("3 in (2, 4, 3, 5 / 0)");
    }

    @Test
    public void testSearchCase()
    {
        functionAssertions.assertFunction("case " +
                "when true then 33 " +
                "end",
                33L);

        functionAssertions.assertFunction("case " +
                "when false then 1 " +
                "else 33 " +
                "end",
                33L);

        functionAssertions.assertFunction("case " +
                "when false then 1 " +
                "when false then 1 " +
                "when true then 33 " +
                "else 1 " +
                "end",
                33L);

        functionAssertions.assertFunction("case " +
                "when false then 1 " +
                "end",
                null);

        functionAssertions.assertFunction("case " +
                "when true then null " +
                "else 'foo' " +
                "end",
                null);

        functionAssertions.assertFunction("case " +
                "when null then 1 " +
                "when true then 33 " +
                "end",
                33L);

        // todo coercion to double
        try {
            functionAssertions.selectSingleValue("case " +
                    "when false then 1.0 " +
                    "when true then 33 " +
                    "end");
            fail("Expected SemanticException");
        }
        catch (ClassCastException | SemanticException expected) {
        }
    }

    @Test
    public void testSimpleCase()
    {
        functionAssertions.assertFunction("case true " +
                "when true then 33 " +
                "end",
                33L);

        functionAssertions.assertFunction("case true " +
                "when false then 1 " +
                "else 33 " +
                "end",
                33L);

        functionAssertions.assertFunction("case true " +
                "when false then 1 " +
                "when false then 1 " +
                "when true then 33 " +
                "else 1 " +
                "end",
                33L);

        functionAssertions.assertFunction("case true " +
                "when false then 1 " +
                "end",
                null);

        functionAssertions.assertFunction("case true " +
                "when true then null " +
                "else 'foo' " +
                "end",
                null);

        functionAssertions.assertFunction("case true " +
                "when null then 1 " +
                "when true then 33 " +
                "end",
                33L);

        functionAssertions.assertFunction("case null " +
                "when true then 1 " +
                "else 33 " +
                "end",
                33);

        // todo coercion to double
        try {
            functionAssertions.selectSingleValue("case true " +
                    "when false then 1.0 " +
                    "when true then 33 " +
                    "end");
            fail("Expected SemanticException");
        }
        catch (ClassCastException | SemanticException expected) {
        }
    }
}
