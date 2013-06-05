package com.facebook.presto.operator;

import com.facebook.presto.tuple.TupleInfo;
import io.airlift.units.DataSize;
import org.testng.annotations.Test;

import static com.facebook.presto.block.BlockAssertions.createLongNullSequence;
import static com.facebook.presto.block.BlockAssertions.createLongSequence;
import static com.facebook.presto.block.BlockAssertions.createLongSequenceBlock;
import static com.facebook.presto.block.BlockAssertions.createLongsBlock;
import static com.facebook.presto.block.BlockAssertions.createStringNullSequence;
import static com.facebook.presto.block.BlockAssertions.createStringSequence;
import static com.facebook.presto.block.BlockAssertions.createStringSequenceBlock;
import static com.facebook.presto.block.BlockAssertions.createStringsBlock;
import static com.facebook.presto.operator.CancelTester.assertCancel;
import static com.facebook.presto.operator.CancelTester.createCancelableDataSource;
import static com.facebook.presto.operator.OperatorAssertions.assertOperatorEquals;
import static com.facebook.presto.operator.OperatorAssertions.createOperator;
import static com.facebook.presto.tuple.TupleInfo.Type.VARIABLE_BINARY;
import static com.google.common.collect.Iterables.concat;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;

public class TestHashJoinOperator
{
    @Test
    public void testInnerJoin()
            throws Exception
    {
        Operator buildSource = createOperator(new Page(
                createStringSequenceBlock(20, 30),
                createLongSequenceBlock(30, 40),
                createLongSequenceBlock(40, 50)));

        Operator probeSource = createOperator(new Page(
                createStringSequenceBlock(0, 1000),
                createLongSequenceBlock(1000, 2000),
                createLongSequenceBlock(2000, 3000)));

        HashJoinOperator joinOperator = HashJoinOperator.innerJoin(new SourceHashProvider(buildSource, 0, 10, new DataSize(1, MEGABYTE), new OperatorStats()), probeSource, 0);

        Operator expected = createOperator(new Page(
                createStringSequenceBlock(20, 30),
                createLongSequenceBlock(1020, 1030),
                createLongSequenceBlock(2020, 2030),
                createStringSequenceBlock(20, 30),
                createLongSequenceBlock(30, 40),
                createLongSequenceBlock(40, 50)));

        assertOperatorEquals(joinOperator, expected);
    }

    @Test
    public void testProbeOuterJoin()
            throws Exception
    {
        Operator buildSource = createOperator(new Page(
                createStringSequenceBlock(20, 30),
                createLongSequenceBlock(30, 40),
                createLongSequenceBlock(40, 50)));

        Operator probeSource = createOperator(new Page(
                createStringSequenceBlock(0, 1000),
                createLongSequenceBlock(1000, 2000),
                createLongSequenceBlock(2000, 3000)));

        HashJoinOperator joinOperator = HashJoinOperator.probeOuterjoin(new SourceHashProvider(buildSource, 0, 10, new DataSize(1, MEGABYTE), new OperatorStats()), probeSource, 0);

        Operator expected = createOperator(new Page(
                createStringSequenceBlock(0, 1000),
                createLongSequenceBlock(1000, 2000),
                createLongSequenceBlock(2000, 3000),
                createStringsBlock(concat(createStringNullSequence(20), createStringSequence(20, 30), createStringNullSequence(970))),
                createLongsBlock(concat(createLongNullSequence(20), createLongSequence(30, 40), createLongNullSequence(970))),
                createLongsBlock(concat(createLongNullSequence(20), createLongSequence(40, 50), createLongNullSequence(970)))));

        assertOperatorEquals(joinOperator, expected);
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = "Query exceeded max operator memory size.*")
    public void testMemoryLimit()
            throws Exception
    {
        Operator buildSource = createOperator(new Page(
                createStringSequenceBlock(20, 30),
                createLongSequenceBlock(30, 40),
                createLongSequenceBlock(40, 50)));

        Operator probeSource = createOperator(new Page(
                createStringSequenceBlock(0, 1000),
                createLongSequenceBlock(1000, 2000),
                createLongSequenceBlock(2000, 3000)));

        HashJoinOperator operator = HashJoinOperator.innerJoin(new SourceHashProvider(buildSource, 0, 10, new DataSize(1, BYTE), new OperatorStats()), probeSource, 0);
        operator.iterator(new OperatorStats()).next();
    }

    @Test
    public void testCancelProbe()
            throws Exception
    {
        Operator build = createOperator(new Page(createStringSequenceBlock(20, 30)));
        BlockingOperator probe = createCancelableDataSource(new TupleInfo(VARIABLE_BINARY), new TupleInfo(VARIABLE_BINARY));

        Operator operator = HashJoinOperator.innerJoin(new SourceHashProvider(build, 0, 10, new DataSize(1, MEGABYTE), new OperatorStats()), probe, 0);
        assertCancel(operator, probe);
    }

    @Test
    public void testCancelBuild()
            throws Exception
    {
        // the build side stops lazily so we need to allow a single page from the operator before the exception is thrown
        BlockingOperator build = createCancelableDataSource(new Page(createStringSequenceBlock(20, 30)), 1, new TupleInfo(VARIABLE_BINARY));
        Operator probe = createOperator(new Page(createStringSequenceBlock(20, 30)));

        Operator operator = HashJoinOperator.innerJoin(new SourceHashProvider(build, 0, 10, new DataSize(1, MEGABYTE), new OperatorStats()), probe, 0);
        assertCancel(operator, build);
    }
}
