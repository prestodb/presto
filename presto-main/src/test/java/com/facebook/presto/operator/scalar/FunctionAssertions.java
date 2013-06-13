/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.operator.scalar;

import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.util.LocalQueryRunner;
import com.facebook.presto.util.MaterializedResult;
import com.google.common.collect.Iterables;

import static com.facebook.presto.util.LocalQueryRunner.createDualLocalQueryRunner;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.testng.Assert.assertEquals;

public final class FunctionAssertions
{
    private FunctionAssertions() {}

    public static void assertFunction(String projection, long expected)
    {
        assertEquals(selectSingleValue(projection), expected);
    }

    public static void assertFunction(String projection, double expected)
    {
        assertEquals(selectSingleValue(projection), expected);
    }

    public static void assertFunction(String projection, String expected)
    {
        assertEquals(selectSingleValue(projection), expected);
    }

    public static void assertFunction(String projection, boolean expected)
    {
        assertEquals(selectSingleValue(projection), expected);
    }

    public static Object selectSingleValue(String projection)
    {
        return selectSingleValue(projection, createDualLocalQueryRunner());
    }

    public static Object selectSingleValue(String projection, Session session)
    {
        return selectSingleValue(projection, createDualLocalQueryRunner(session));
    }

    private static Object selectSingleValue(String projection, LocalQueryRunner runner)
    {
        checkNotNull(projection, "projection is null");

        MaterializedResult result = runner.execute("SELECT " + projection + " FROM dual");

        assertEquals(result.getTupleInfo().getFieldCount(), 1);
        assertEquals(result.getMaterializedTuples().size(), 1);

        return Iterables.getOnlyElement(result.getMaterializedTuples()).getField(0);
    }
}
