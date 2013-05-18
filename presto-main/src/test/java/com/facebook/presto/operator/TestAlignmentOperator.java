package com.facebook.presto.operator;

import com.facebook.presto.block.BlockAssertions;
import com.facebook.presto.block.BlockIterable;
import com.facebook.presto.execution.SqlTaskManagerStats;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.TaskOutput;
import com.facebook.presto.util.Threads;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import static com.facebook.presto.block.BlockAssertions.createLongsBlock;
import static com.facebook.presto.block.BlockAssertions.createStringsBlock;
import static com.facebook.presto.operator.OperatorAssertions.assertOperatorEquals;
import static com.facebook.presto.operator.OperatorAssertions.createOperator;
import static com.facebook.presto.tuple.TupleInfo.Type.FIXED_INT_64;
import static com.facebook.presto.tuple.TupleInfo.Type.VARIABLE_BINARY;
import static org.testng.Assert.assertEquals;

public class TestAlignmentOperator
{
    private final Executor executor = Executors.newCachedThreadPool(Threads.daemonThreadsNamed("test-%d"));

    @Test
    public void testAlignment()
            throws Exception
    {
        AlignmentOperator operator = createAlignmentOperator();
        assertOperatorEquals(operator, createOperator(
                new Page(createStringsBlock("alice", "bob", "charlie", "dave"),
                        createLongsBlock(0, 1, 2, 3)),
                new Page(createStringsBlock("alice", "bob", "charlie", "dave"),
                        createLongsBlock(4, 5, 6, 7)),
                new Page(createStringsBlock("alice", "bob", "charlie", "dave"),
                        createLongsBlock(8, 9, 10, 11))
        ));
    }

    @Test
    public void testFinish()
            throws Exception
    {
        AlignmentOperator operator = createAlignmentOperator();

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

    private AlignmentOperator createAlignmentOperator()
    {
        BlockIterable channel0 = BlockAssertions.blockIterableBuilder(VARIABLE_BINARY)
                .append("alice")
                .append("bob")
                .append("charlie")
                .append("dave")
                .newBlock()
                .append("alice")
                .append("bob")
                .append("charlie")
                .append("dave")
                .newBlock()
                .append("alice")
                .append("bob")
                .append("charlie")
                .append("dave")
                .build();

        BlockIterable channel1 = BlockAssertions.blockIterableBuilder(FIXED_INT_64)
                .append(0)
                .append(1)
                .append(2)
                .append(3)
                .append(4)
                .append(5)
                .append(6)
                .append(7)
                .append(8)
                .append(9)
                .append(10)
                .append(11)
                .build();

        return new AlignmentOperator(channel0, channel1);
    }
}
