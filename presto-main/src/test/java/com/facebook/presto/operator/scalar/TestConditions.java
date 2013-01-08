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
}
