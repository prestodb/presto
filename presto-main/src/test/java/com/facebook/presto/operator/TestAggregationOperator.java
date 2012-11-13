package com.facebook.presto.operator;

import com.facebook.presto.block.BlockAssertions;
import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import static com.facebook.presto.operator.OperatorAssertions.assertOperatorEquals;
import static com.facebook.presto.operator.OperatorAssertions.createOperator;
import static com.facebook.presto.operator.ProjectionFunctions.concat;
import static com.facebook.presto.operator.ProjectionFunctions.singleColumn;
import static com.facebook.presto.operator.aggregation.AggregationFunctions.singleNodeAggregation;
import static com.facebook.presto.operator.aggregation.CountAggregation.countAggregation;
import static com.facebook.presto.operator.aggregation.LongAverageAggregation.longAverageAggregation;
import static com.facebook.presto.operator.aggregation.LongSumAggregation.longSumAggregation;
import static com.facebook.presto.tuple.TupleInfo.Type.DOUBLE;
import static com.facebook.presto.tuple.TupleInfo.Type.FIXED_INT_64;

public class TestAggregationOperator
{
    @Test
    public void testAlignment()
            throws Exception
    {
        Operator source = createOperator(new Page(
                BlockAssertions.createStringSequenceBlock(0, 100),
                BlockAssertions.createLongSequenceBlock(0, 100)));

        AggregationOperator actual = new AggregationOperator(source,
                ImmutableList.of(singleNodeAggregation(countAggregation(0, 0)),
                        singleNodeAggregation(longSumAggregation(1, 0)),
                        singleNodeAggregation(longAverageAggregation(1, 0))),
                ImmutableList.of(concat(singleColumn(FIXED_INT_64, 0, 0),
                        singleColumn(FIXED_INT_64, 1, 0),
                        singleColumn(DOUBLE, 2, 0))));

        Operator expected = createOperator(new Page(new BlockBuilder(new TupleInfo(FIXED_INT_64, FIXED_INT_64, DOUBLE))
                .append(100L).append(4950L).append(49.5)
                .build()));

        assertOperatorEquals(actual, expected);
    }
}
