package com.facebook.presto.operator;

import com.facebook.presto.block.BlockAssertions;
import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.sql.planner.plan.AggregationNode.Step;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import static com.facebook.presto.operator.NewHashAggregationOperator.AggregationFunctionDefinition.aggregation;
import static com.facebook.presto.operator.OperatorAssertions.createOperator;
import static com.facebook.presto.operator.aggregation.CountFixedWidthAggregation.COUNT;
import static com.facebook.presto.operator.aggregation.LongAverageFixedWidthAggregation.LONG_AVERAGE;
import static com.facebook.presto.operator.aggregation.LongSumFixedWidthAggregation.LONG_SUM;
import static com.facebook.presto.operator.aggregation.VarBinaryVariableWidthMaxAggregation.VAR_BINARY_MAX;
import static com.facebook.presto.tuple.TupleInfo.Type.DOUBLE;
import static com.facebook.presto.tuple.TupleInfo.Type.FIXED_INT_64;
import static com.facebook.presto.tuple.TupleInfo.Type.VARIABLE_BINARY;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

public class TestNewHashAggregationOperator
{
    @Test
    public void testHashAggregation()
            throws Exception
    {
        Page expectedPage = new Page(
                new BlockBuilder(new TupleInfo(VARIABLE_BINARY))
                        .append("0")
                        .append("1")
                        .append("2")
                        .append("3")
                        .append("4")
                        .append("5")
                        .append("6")
                        .append("7")
                        .append("8")
                        .append("9")
                        .build(),
                new BlockBuilder(new TupleInfo(FIXED_INT_64))
                        .append(3)
                        .append(3)
                        .append(3)
                        .append(3)
                        .append(3)
                        .append(3)
                        .append(3)
                        .append(3)
                        .append(3)
                        .append(3)
                        .build(),
                new BlockBuilder(new TupleInfo(FIXED_INT_64))
                        .append(0)
                        .append(3)
                        .append(6)
                        .append(9)
                        .append(12)
                        .append(15)
                        .append(18)
                        .append(21)
                        .append(24)
                        .append(27)
                        .build(),
                new BlockBuilder(new TupleInfo(DOUBLE))
                        .append(0.0)
                        .append(1.0)
                        .append(2.0)
                        .append(3.0)
                        .append(4.0)
                        .append(5.0)
                        .append(6.0)
                        .append(7.0)
                        .append(8.0)
                        .append(9.0)
                        .build(),
                new BlockBuilder(new TupleInfo(VARIABLE_BINARY))
                        .append("300")
                        .append("301")
                        .append("302")
                        .append("303")
                        .append("304")
                        .append("305")
                        .append("306")
                        .append("307")
                        .append("308")
                        .append("309")
                        .build()
        );

        Operator source = createOperator(
                new Page(
                        BlockAssertions.createStringSequenceBlock(100, 110),
                        BlockAssertions.createStringSequenceBlock(0, 10),
                        BlockAssertions.createStringSequenceBlock(100, 110),
                        BlockAssertions.createLongSequenceBlock(0, 10)),
                new Page(
                        BlockAssertions.createStringSequenceBlock(100, 110),
                        BlockAssertions.createStringSequenceBlock(0, 10),
                        BlockAssertions.createStringSequenceBlock(200, 210),
                        BlockAssertions.createLongSequenceBlock(0, 10)),
                new Page(
                        BlockAssertions.createStringSequenceBlock(100, 110),
                        BlockAssertions.createStringSequenceBlock(0, 10),
                        BlockAssertions.createStringSequenceBlock(300, 310),
                        BlockAssertions.createLongSequenceBlock(0, 10))
        );

        NewHashAggregationOperator actual = new NewHashAggregationOperator(source,
                1,
                Step.SINGLE,
                ImmutableList.of(aggregation(COUNT, 0),
                        aggregation(LONG_SUM, 3),
                        aggregation(LONG_AVERAGE, 3),
                        aggregation(VAR_BINARY_MAX, 2)), 100_000);

        PageIterator pages = actual.iterator(new OperatorStats());

        Page actualPage = pages.next();
        assertEquals(actualPage.getChannelCount(), 5);
        PageAssertions.assertPageEquals(actualPage, expectedPage);

        assertFalse(pages.hasNext());

    }
}
