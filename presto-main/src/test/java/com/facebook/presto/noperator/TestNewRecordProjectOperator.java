/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.noperator;

import com.facebook.presto.ingest.InfiniteRecordSet;
import com.facebook.presto.operator.Page;
import com.facebook.presto.spi.InMemoryRecordSet;
import com.facebook.presto.util.MaterializedResult;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.spi.ColumnType.LONG;
import static com.facebook.presto.spi.ColumnType.STRING;
import static com.facebook.presto.tuple.TupleInfo.Type.FIXED_INT_64;
import static com.facebook.presto.tuple.TupleInfo.Type.VARIABLE_BINARY;
import static com.facebook.presto.util.MaterializedResult.resultBuilder;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

public class TestNewRecordProjectOperator
{
    @Test
    public void testSingleColumn()
            throws Exception
    {
        InMemoryRecordSet records = new InMemoryRecordSet(ImmutableList.of(STRING), ImmutableList.copyOf(new List<?>[]{ImmutableList.of("abc"), ImmutableList.of("def"), ImmutableList.of("g")}));

        NewOperator operator = new NewRecordProjectOperator(records);

        MaterializedResult expected = resultBuilder(VARIABLE_BINARY)
                .row("abc")
                .row("def")
                .row("g")
                .build();

        NewOperatorAssertion.assertOperatorEquals(operator, expected);
    }

    @Test
    public void testMultiColumn()
            throws Exception
    {
        InMemoryRecordSet records = new InMemoryRecordSet(ImmutableList.of(STRING, LONG), ImmutableList.copyOf(new List<?>[]{ImmutableList.of("abc", 1L), ImmutableList.of("def", 2L), ImmutableList.of("g", 0L)}));

        NewOperator operator = new NewRecordProjectOperator(records);

        MaterializedResult expected = resultBuilder(VARIABLE_BINARY, FIXED_INT_64)
                .row("abc", 1)
                .row("def", 2)
                .row("g", 0)
                .build();

        NewOperatorAssertion.assertOperatorEquals(operator, expected);
    }

    @Test
    public void testFinish()
            throws Exception
    {
        InfiniteRecordSet records = new InfiniteRecordSet(ImmutableList.of(STRING, LONG), ImmutableList.of("abc", 1L));

        NewOperator operator = new NewRecordProjectOperator(records);

        // verify initial state
        assertEquals(operator.isFinished(), false);
        assertEquals(operator.needsInput(), false);

        // first read will be null due to buffering
        assertNull(operator.getOutput());

        // read first page
        Page page = null;
        for (int i = 0; i < 100; i++) {
            page = operator.getOutput();
            if (page != null) {
                break;
            }
        }
        assertNotNull(page);

        // verify state
        assertEquals(operator.isFinished(), false);
        assertEquals(operator.needsInput(), false);

        // start second page... will be null due to buffering
        assertNull(operator.getOutput());

        // verify state
        assertEquals(operator.isFinished(), false);
        assertEquals(operator.needsInput(), false);

        // finish
        operator.finish();

        // verify state
        assertEquals(operator.isFinished(), false);
        assertEquals(operator.needsInput(), false);

        // read the buffered page
        assertNotNull(operator.getOutput());

        // verify state
        assertEquals(operator.isFinished(), true);
        assertEquals(operator.needsInput(), false);
        assertEquals(operator.getOutput(), null);
    }
}
