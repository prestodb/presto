package com.facebook.presto.operator;

import org.testng.annotations.Test;

import static com.facebook.presto.block.BlockAssertions.createLongSequenceBlock;
import static com.facebook.presto.block.BlockAssertions.createStringSequenceBlock;
import static com.facebook.presto.operator.OperatorAssertions.assertOperatorEquals;
import static com.facebook.presto.operator.OperatorAssertions.createOperator;

public class TestHashJoinOperator
{
    @Test
    public void testJoin()
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

        HashJoinOperator joinOperator = new HashJoinOperator(new SourceHashProvider(buildSource, 0, 10, new OperatorStats()), probeSource, 0);

        Operator expected = createOperator(new Page(
                createStringSequenceBlock(20, 30),
                createLongSequenceBlock(1020, 1030),
                createLongSequenceBlock(2020, 2030),
                createStringSequenceBlock(20, 30),
                createLongSequenceBlock(30, 40),
                createLongSequenceBlock(40, 50)));

        assertOperatorEquals(joinOperator, expected);
    }
}
