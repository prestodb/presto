package com.facebook.presto.noperator;

import com.facebook.presto.TupleInfo;
import com.facebook.presto.nblock.BlockAssertions;
import com.facebook.presto.nblock.BlockBuilder;
import com.facebook.presto.noperator.aggregation.CountAggregation;
import com.facebook.presto.noperator.aggregation.LongAverageAggregation;
import com.facebook.presto.noperator.aggregation.LongSumAggregation;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import static com.facebook.presto.TupleInfo.Type.DOUBLE;
import static com.facebook.presto.TupleInfo.Type.FIXED_INT_64;
import static com.facebook.presto.noperator.OperatorAssertions.assertOperatorEquals;
import static com.facebook.presto.noperator.OperatorAssertions.createOperator;
import static com.facebook.presto.noperator.ProjectionFunctions.concat;
import static com.facebook.presto.noperator.ProjectionFunctions.singleColumn;

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
                ImmutableList.of(CountAggregation.PROVIDER, LongSumAggregation.provider(1, 0), LongAverageAggregation.provider(1, 0)),
                ImmutableList.of(concat(singleColumn(FIXED_INT_64, 0, 0), singleColumn(FIXED_INT_64, 1, 0), singleColumn(DOUBLE, 2, 0))));

        Operator expected = createOperator(new Page(new BlockBuilder(0, new TupleInfo(FIXED_INT_64, FIXED_INT_64, DOUBLE))
                .append(100L).append(4950L).append(49.5)
                .build()));

        assertOperatorEquals(actual, expected);
    }
}
