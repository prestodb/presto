/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.ingest;

import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.TaskOutput;
import com.facebook.presto.operator.AlignmentOperator;
import com.facebook.presto.operator.OperatorStats;
import com.facebook.presto.operator.PageIterator;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.List;

import static com.facebook.presto.block.BlockAssertions.createLongsBlockIterable;
import static com.facebook.presto.block.BlockAssertions.createStringsBlockIterable;
import static com.facebook.presto.operator.OperatorAssertions.assertOperatorEquals;
import static com.facebook.presto.tuple.TupleInfo.Type.FIXED_INT_64;
import static com.facebook.presto.tuple.TupleInfo.Type.VARIABLE_BINARY;
import static org.testng.Assert.assertEquals;

public class TestRecordProjectOperator
{
    @Test
    public void testSingleColumn()
            throws Exception
    {
        InMemoryRecordSet records = new InMemoryRecordSet(ImmutableList.of(VARIABLE_BINARY), ImmutableList.copyOf(new List<?>[]{ImmutableList.of("abc"), ImmutableList.of("def"), ImmutableList.of("g")}));

        RecordProjectOperator recordProjectOperator = new RecordProjectOperator(records, records.getColumns());
        assertOperatorEquals(recordProjectOperator, new AlignmentOperator(createStringsBlockIterable("abc", "def", "g")));
    }

    @Test
    public void testMultiColumn()
            throws Exception
    {
        InMemoryRecordSet records = new InMemoryRecordSet(ImmutableList.of(VARIABLE_BINARY, FIXED_INT_64), ImmutableList.copyOf(new List<?>[]{ImmutableList.of("abc", 1L), ImmutableList.of("def", 2L), ImmutableList.of("g", 0L)}));

        RecordProjectOperator recordProjectOperator = new RecordProjectOperator(records, records.getColumns());
        assertOperatorEquals(recordProjectOperator, new AlignmentOperator(
                createStringsBlockIterable("abc", "def", "g"),
                createLongsBlockIterable(1, 2, 0)
        ));
    }

    @Test
    public void testFinish()
            throws Exception
    {
        InfiniteRecordSet records = new InfiniteRecordSet(ImmutableList.of(VARIABLE_BINARY, FIXED_INT_64), ImmutableList.of("abc", 1L));

        RecordProjectOperator operator = new RecordProjectOperator(records, records.getColumns());

        TaskOutput taskOutput = new TaskOutput(new TaskId("0", "0", "0"), URI.create("unknown://unknown"), 1000);
        taskOutput.addResultQueue("unknown");
        taskOutput.noMoreResultQueues();

        int pageCount = 0;
        PageIterator iterator = operator.iterator(new OperatorStats(taskOutput));
        while (iterator.hasNext()) {
            iterator.next();
            pageCount++;

            // stop when page count is 2
            if (pageCount == 2) {
                taskOutput.finish();
            }
            if (pageCount > 2) {
                break;
            }
        }
        assertEquals(pageCount, 2);
    }
}
