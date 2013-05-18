/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.ingest;

import com.facebook.presto.execution.SqlTaskManagerStats;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.TaskOutput;
import com.facebook.presto.spi.InMemoryRecordSet;
import com.facebook.presto.operator.AlignmentOperator;
import com.facebook.presto.operator.OperatorStats;
import com.facebook.presto.operator.PageIterator;
import com.facebook.presto.util.Threads;
import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import static com.facebook.presto.block.BlockAssertions.createLongsBlockIterable;
import static com.facebook.presto.block.BlockAssertions.createStringsBlockIterable;
import static com.facebook.presto.operator.OperatorAssertions.assertOperatorEquals;
import static com.facebook.presto.spi.ColumnType.LONG;
import static com.facebook.presto.spi.ColumnType.STRING;
import static org.testng.Assert.assertEquals;

public class TestRecordProjectOperator
{
    private final Executor executor = Executors.newCachedThreadPool(Threads.daemonThreadsNamed("test-%d"));

    @Test
    public void testSingleColumn()
            throws Exception
    {
        InMemoryRecordSet records = new InMemoryRecordSet(ImmutableList.of(STRING), ImmutableList.copyOf(new List<?>[]{ImmutableList.of("abc"), ImmutableList.of("def"), ImmutableList.of("g")}));

        RecordProjectOperator recordProjectOperator = new RecordProjectOperator(records);
        assertOperatorEquals(recordProjectOperator, new AlignmentOperator(createStringsBlockIterable("abc", "def", "g")));
    }

    @Test
    public void testMultiColumn()
            throws Exception
    {
        InMemoryRecordSet records = new InMemoryRecordSet(ImmutableList.of(STRING, LONG), ImmutableList.copyOf(new List<?>[]{ImmutableList.of("abc", 1L), ImmutableList.of("def", 2L), ImmutableList.of("g", 0L)}));

        RecordProjectOperator recordProjectOperator = new RecordProjectOperator(records);
        assertOperatorEquals(recordProjectOperator, new AlignmentOperator(
                createStringsBlockIterable("abc", "def", "g"),
                createLongsBlockIterable(1, 2, 0)
        ));
    }

    @Test
    public void testFinish()
            throws Exception
    {
        InfiniteRecordSet records = new InfiniteRecordSet(ImmutableList.of(STRING, LONG), ImmutableList.of("abc", 1L));

        RecordProjectOperator operator = new RecordProjectOperator(records);

        TaskOutput taskOutput = new TaskOutput(new TaskId("0", "0", "0"), URI.create("unknown://unknown"), new DataSize(100, Unit.MEGABYTE), executor, new SqlTaskManagerStats());
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
