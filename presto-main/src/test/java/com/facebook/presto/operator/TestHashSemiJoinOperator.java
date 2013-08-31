package com.facebook.presto.operator;

import com.facebook.presto.execution.TaskMemoryManager;
import com.facebook.presto.tuple.TupleInfo;
import io.airlift.units.DataSize;
import org.testng.annotations.Test;

import static com.facebook.presto.block.BlockAssertions.createBooleansBlock;
import static com.facebook.presto.block.BlockAssertions.createLongSequenceBlock;
import static com.facebook.presto.block.BlockAssertions.createLongsBlock;
import static com.facebook.presto.block.BlockAssertions.createStringSequenceBlock;
import static com.facebook.presto.operator.CancelTester.assertCancel;
import static com.facebook.presto.operator.CancelTester.createCancelableDataSource;
import static com.facebook.presto.operator.OperatorAssertions.assertOperatorEquals;
import static com.facebook.presto.operator.OperatorAssertions.createOperator;
import static com.facebook.presto.tuple.TupleInfo.Type.VARIABLE_BINARY;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;

public class TestHashSemiJoinOperator
{
    @Test
    public void testSemiJoin()
            throws Exception
    {
        Operator buildSource = createOperator(new Page(createLongsBlock(10, 30, 30, 35, 36, 37, 50)));

        Operator probeSource = createOperator(new Page(
                createLongSequenceBlock(30, 40),
                createLongSequenceBlock(0, 10)));

        SourceSetSupplier setProvider = new SourceSetSupplier(buildSource, 0, 10, new TaskMemoryManager(new DataSize(1, MEGABYTE)), new OperatorStats());
        HashSemiJoinOperator semiJoinOperator = new HashSemiJoinOperator(probeSource, 0, setProvider);

        Operator expected = createOperator(new Page(
                createLongSequenceBlock(30, 40),
                createLongSequenceBlock(0, 10),
                createBooleansBlock(true, false, false, false, false, true, true, true, false, false)));

        assertOperatorEquals(semiJoinOperator, expected);
    }

    @Test
    public void testBuildSideNulls()
            throws Exception
    {
        Operator buildSource = createOperator(new Page(createLongsBlock(0L, 1L, 2L, 2L, 3L, null)));

        Operator probeSource = createOperator(new Page(createLongSequenceBlock(1, 5)));

        SourceSetSupplier setProvider = new SourceSetSupplier(buildSource, 0, 10, new TaskMemoryManager(new DataSize(1, MEGABYTE)), new OperatorStats());
        HashSemiJoinOperator semiJoinOperator = new HashSemiJoinOperator(probeSource, 0, setProvider);

        Operator expected = createOperator(new Page(
                createLongSequenceBlock(1, 5),
                createBooleansBlock(true, true, true, null)));

        assertOperatorEquals(semiJoinOperator, expected);
    }

    @Test
    public void testProbeSideNulls()
            throws Exception
    {
        Operator buildSource = createOperator(new Page(createLongsBlock(0L, 1L, 3L)));

        Operator probeSource = createOperator(new Page(createLongsBlock(0L, null, 1L, 2L)));

        SourceSetSupplier setProvider = new SourceSetSupplier(buildSource, 0, 10, new TaskMemoryManager(new DataSize(1, MEGABYTE)), new OperatorStats());
        HashSemiJoinOperator semiJoinOperator = new HashSemiJoinOperator(probeSource, 0, setProvider);

        Operator expected = createOperator(new Page(
                createLongsBlock(0L, null, 1L, 2L),
                createBooleansBlock(true, null, true, false)));

        assertOperatorEquals(semiJoinOperator, expected);
    }

    @Test
    public void testProbeAndBuildNulls()
            throws Exception
    {
        Operator buildSource = createOperator(new Page(createLongsBlock(0L, 1L, null, 3L)));

        Operator probeSource = createOperator(new Page(createLongsBlock(0L, null, 1L, 2L)));

        SourceSetSupplier setProvider = new SourceSetSupplier(buildSource, 0, 10, new TaskMemoryManager(new DataSize(1, MEGABYTE)), new OperatorStats());
        HashSemiJoinOperator semiJoinOperator = new HashSemiJoinOperator(probeSource, 0, setProvider);

        Operator expected = createOperator(new Page(
                createLongsBlock(0L, null, 1L, 2L),
                createBooleansBlock(true, null, true, null)));

        assertOperatorEquals(semiJoinOperator, expected);
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = "Task exceeded max memory size.*")
    public void testMemoryLimit()
            throws Exception
    {
        Operator buildSource = createOperator(new Page(
                createStringSequenceBlock(20, 30)));

        Operator probeSource = createOperator(new Page(
                createStringSequenceBlock(0, 1000),
                createLongSequenceBlock(1000, 2000),
                createLongSequenceBlock(2000, 3000)));

        SourceSetSupplier setProvider = new SourceSetSupplier(buildSource, 0, 10, new TaskMemoryManager(new DataSize(1, BYTE)), new OperatorStats());
        HashSemiJoinOperator semiJoinOperator = new HashSemiJoinOperator(probeSource, 0, setProvider);
        semiJoinOperator.iterator(new OperatorStats()).next();
    }

    @Test
    public void testCancelProbe()
            throws Exception
    {
        Operator build = createOperator(new Page(createStringSequenceBlock(20, 30)));
        BlockingOperator probe = createCancelableDataSource(new TupleInfo(VARIABLE_BINARY), new TupleInfo(VARIABLE_BINARY));

        SourceSetSupplier setProvider = new SourceSetSupplier(build, 0, 10, new TaskMemoryManager(new DataSize(1, MEGABYTE)), new OperatorStats());
        HashSemiJoinOperator semiJoinOperator = new HashSemiJoinOperator(probe, 0, setProvider);
        assertCancel(semiJoinOperator, probe);
    }

    @Test
    public void testCancelBuild()
            throws Exception
    {
        // the build side stops lazily so we need to allow a single page from the operator before the exception is thrown
        BlockingOperator build = createCancelableDataSource(new Page(createStringSequenceBlock(20, 30)), 1, new TupleInfo(VARIABLE_BINARY));
        Operator probe = createOperator(new Page(createStringSequenceBlock(20, 30)));

        SourceSetSupplier setProvider = new SourceSetSupplier(build, 0, 10, new TaskMemoryManager(new DataSize(1, MEGABYTE)), new OperatorStats());
        HashSemiJoinOperator semiJoinOperator = new HashSemiJoinOperator(probe, 0, setProvider);
        assertCancel(semiJoinOperator, build);
    }
}
