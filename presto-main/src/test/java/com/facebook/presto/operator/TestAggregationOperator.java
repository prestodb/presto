package com.facebook.presto.operator;

import com.facebook.presto.block.BlockAssertions;
import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.sql.planner.plan.AggregationNode.Step;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import static com.facebook.presto.operator.AggregationFunctionDefinition.aggregation;
import static com.facebook.presto.operator.CancelTester.assertCancel;
import static com.facebook.presto.operator.CancelTester.createCancelableDataSource;
import static com.facebook.presto.operator.OperatorAssertions.createOperator;
import static com.facebook.presto.operator.aggregation.CountAggregation.COUNT;
import static com.facebook.presto.operator.aggregation.CountColumnAggregation.COUNT_COLUMN;
import static com.facebook.presto.operator.aggregation.DoubleSumAggregation.DOUBLE_SUM;
import static com.facebook.presto.operator.aggregation.LongAverageAggregation.LONG_AVERAGE;
import static com.facebook.presto.operator.aggregation.LongSumAggregation.LONG_SUM;
import static com.facebook.presto.operator.aggregation.VarBinaryMaxAggregation.VAR_BINARY_MAX;
import static com.facebook.presto.tuple.TupleInfo.SINGLE_DOUBLE;
import static com.facebook.presto.tuple.TupleInfo.SINGLE_LONG;
import static com.facebook.presto.tuple.TupleInfo.SINGLE_VARBINARY;
import static com.facebook.presto.tuple.TupleInfo.Type.VARIABLE_BINARY;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

public class TestAggregationOperator
{
    @Test
    public void testAggregation()
            throws Exception
    {
        Operator source = createOperator(new Page(
                BlockAssertions.createStringSequenceBlock(0, 100),
                BlockAssertions.createLongSequenceBlock(0, 100),
                BlockAssertions.createStringSequenceBlock(300, 400),
                BlockAssertions.createCompositeTupleSequenceBlock(500, 600)));


        AggregationOperator actual = new AggregationOperator(source,
                Step.SINGLE,
                ImmutableList.of(aggregation(COUNT, new Input(0, 0)),
                        aggregation(LONG_SUM, new Input(1, 0)),
                        aggregation(LONG_AVERAGE, new Input(1, 0)),
                        aggregation(VAR_BINARY_MAX, new Input(2, 0)),
                        aggregation(COUNT_COLUMN, new Input(0, 0)),
                        aggregation(LONG_SUM, new Input(3, 0)),
                        aggregation(DOUBLE_SUM, new Input(3, 1)),
                        aggregation(VAR_BINARY_MAX, new Input(3, 2))));

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
                        .build(),
                new BlockBuilder(SINGLE_LONG)
                        .append(100L)
                        .build(),
                new BlockBuilder(SINGLE_LONG)
                        .append(54950L)
                        .build(),
                new BlockBuilder(SINGLE_DOUBLE)
                        .append(54950.0)
                        .build(),
                new BlockBuilder(SINGLE_VARBINARY)
                        .append("599")
                        .build()
        );

        PageIterator pages = actual.iterator(new OperatorStats());

        Page actualPage = pages.next();
        assertEquals(actualPage.getChannelCount(), 8);
        PageAssertions.assertPageEquals(actualPage, expectedPage);

        assertFalse(pages.hasNext());
    }

    @Test
    public void testCancel()
            throws Exception
    {
        BlockingOperator blockingOperator = createCancelableDataSource(new TupleInfo(VARIABLE_BINARY), new TupleInfo(VARIABLE_BINARY));
        Operator operator = new AggregationOperator(blockingOperator, Step.SINGLE, ImmutableList.of(aggregation(COUNT, new Input(0, 0))));
        assertCancel(operator, blockingOperator);
    }
}
