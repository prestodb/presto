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
import org.testng.annotations.Test;

import static com.facebook.presto.operator.scalar.FunctionAssertions.assertFunction;
import static com.facebook.presto.operator.scalar.FunctionAssertions.selectSingleValue;
import static org.testng.Assert.fail;

public class TestConditions
{
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
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = ".*escape must be empty or a single character.*")
    public void testLikeInvalidEscape()
    {
        selectSingleValue("'monkey' like 'monkey' escape 'foo'");
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

        assertFunction("null in (2, null, 3, 5) is null", true);
        assertFunction("3 in (2, null) is null", true);
        assertFunction("null not in (2, null, 3, 5) is null", true);
        assertFunction("3 not in (2, null) is null", true);
    }

    @Test(expectedExceptions = ArithmeticException.class)
    public void testInDoesNotShortCircuit()
    {
        selectSingleValue("3 in (2, 4, 3, 5 / 0)");
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

        // todo coercion to double
        try {
            selectSingleValue("case " +
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

        // todo coercion to double
        try {
            selectSingleValue("case true " +
                    "when false then 1.0 " +
                    "when true then 33 " +
                    "end");
            fail("Expected SemanticException");
        }
        catch (ClassCastException | SemanticException expected) {
        }
    }
}
