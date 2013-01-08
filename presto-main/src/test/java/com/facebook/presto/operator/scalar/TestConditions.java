/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.operator.scalar;

import org.testng.annotations.Test;

import static com.facebook.presto.operator.scalar.FunctionAssertions.assertFunction;
import static com.facebook.presto.operator.scalar.FunctionAssertions.selectBooleanValue;

public class TestConditions
{
    @Test
    public void testLike()
    {
        assertFunction("'monkey' like 'monkey'", true);
        assertFunction("'monkey' like 'mon%'", true);
        assertFunction("'monkey' like 'mon_ey'", true);
        assertFunction("'monkey' like 'm____y'", true);

        assertFunction("'monkey' like 'dain'", false);
        assertFunction("'monkey' like 'key'", false);

        assertFunction("'_monkey_' like '\\_monkey\\_'", true);
        assertFunction("'_monkey_' like 'X_monkeyX_' escape 'X'", true);
        assertFunction("'_monkey_' like '_monkey_' escape ''", true);

        assertFunction("'*?.(){}+|^$,\\' like '*?.(){}+|^$,\\' escape ''", true);

        assertFunction("null like 'monkey'", false);
        assertFunction("'monkey' like null", false);
        assertFunction("'monkey' like 'monkey' escape null", false);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "escape must be empty or a single character.*")
    public void testLikeInvalidEscape()
    {
        selectBooleanValue("'monkey' like 'monkey' escape 'foo'");
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
        assertFunction("null between 2 and 4", false);
        assertFunction("3 between null and 4", false);
        assertFunction("3 between 2 and null", false);

        assertFunction("'c' between 'b' and 'd'", true);
        assertFunction("'c' between 'c' and 'c'", true);
        assertFunction("'c' between 'b' and 'c'", true);
        assertFunction("'c' between 'c' and 'd'", true);
        assertFunction("'c' between 'd' and 'b'", false);
        assertFunction("'b' between 'c' and 'd'", false);
        assertFunction("'e' between 'c' and 'd'", false);
        assertFunction("null between 'b' and 'd'", false);
        assertFunction("'c' between null and 'd'", false);
        assertFunction("'c' between 'b' and null", false);
    }

    @Test
    public void testIn()
    {
        assertFunction("3 in (2, 4, 3, 5)", true);
        assertFunction("3 in (2, 4, 9, 5)", false);
        assertFunction("3 in (2, null, 3, 5)", true);

        assertFunction("'foo' in ('bar', 'baz', 'foo', 'blah')", true);
        assertFunction("'foo' in ('bar', 'baz', 'buz', 'blah')", false);
        assertFunction("'foo' in ('bar', null, 'foo', 'blah')", true);

        // todo test that these are null when we support boolean types
        assertFunction("null in (2, null, 3, 5)", false);
        assertFunction("3 in (2, null)", false);
    }

    @Test(expectedExceptions = ArithmeticException.class)
    public void testInDoesNotShortCircuit()
    {
        selectBooleanValue("3 in (2, 4, 3, 5 / 0)");
    }

}
