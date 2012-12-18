package com.facebook.presto.operator;

import com.facebook.presto.block.BlockAssertions;
import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.sql.planner.plan.AggregationNode.Step;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import static com.facebook.presto.operator.AggregationFunctionDefinition.aggregation;
import static com.facebook.presto.operator.OperatorAssertions.createOperator;
import static com.facebook.presto.operator.aggregation.CountFixedWidthAggregation.COUNT;
import static com.facebook.presto.operator.aggregation.LongAverageFixedWidthAggregation.LONG_AVERAGE;
import static com.facebook.presto.operator.aggregation.LongSumFixedWidthAggregation.LONG_SUM;
import static com.facebook.presto.operator.aggregation.VarBinaryVariableWidthMaxAggregation.VAR_BINARY_MAX;
import static com.facebook.presto.tuple.TupleInfo.SINGLE_DOUBLE;
import static com.facebook.presto.tuple.TupleInfo.SINGLE_LONG;
import static com.facebook.presto.tuple.TupleInfo.SINGLE_VARBINARY;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

public class TestNewAggregationOperator
{
    @Test
    public void testAggregation()
            throws Exception
    {
        Operator source = createOperator(new Page(
                BlockAssertions.createStringSequenceBlock(0, 100),
                BlockAssertions.createLongSequenceBlock(0, 100),
                BlockAssertions.createStringSequenceBlock(300, 400)));


        NewAggregationOperator actual = new NewAggregationOperator(source,
                Step.SINGLE,
                ImmutableList.of(aggregation(COUNT, 0),
                        aggregation(LONG_SUM, 1),
                        aggregation(LONG_AVERAGE, 1),
                        aggregation(VAR_BINARY_MAX, 2)));

        Page expectedPage = new Page(
                new BlockBuilder(SINGLE_LONG)
                        .append(100L)
                        .build(),
                new BlockBuilder(SINGLE_LONG)
                        .append(4950L)
                        .build(),
                new BlockBuilder(SINGLE_DOUBLE)
                        .append(49.5)
                        .build(),
                new BlockBuilder(SINGLE_VARBINARY)
                        .append("399")
                        .build()
        );

        PageIterator pages = actual.iterator(new OperatorStats());

        Page actualPage = pages.next();
        assertEquals(actualPage.getChannelCount(), 4);
        PageAssertions.assertPageEquals(actualPage, expectedPage);

        assertFalse(pages.hasNext());
    }
}
