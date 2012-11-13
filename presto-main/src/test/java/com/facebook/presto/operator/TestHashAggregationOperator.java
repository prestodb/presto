package com.facebook.presto.operator;

import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockAssertions;
import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.operator.aggregation.CountAggregation;
import com.facebook.presto.operator.aggregation.LongAverageAggregation;
import com.facebook.presto.operator.aggregation.LongSumAggregation;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.Iterator;

import static com.facebook.presto.tuple.TupleInfo.Type.DOUBLE;
import static com.facebook.presto.tuple.TupleInfo.Type.FIXED_INT_64;
import static com.facebook.presto.tuple.TupleInfo.Type.VARIABLE_BINARY;
import static com.facebook.presto.operator.OperatorAssertions.createOperator;
import static com.facebook.presto.operator.ProjectionFunctions.concat;
import static com.facebook.presto.operator.ProjectionFunctions.singleColumn;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

public class TestHashAggregationOperator
{
    @Test
    public void testAlignment()
            throws Exception
    {
        Block expectedBlock = new BlockBuilder(new TupleInfo(VARIABLE_BINARY, FIXED_INT_64, FIXED_INT_64, DOUBLE))
                .append("0").append(3).append(0).append(0.0)
                .append("1").append(3).append(3).append(1.0)
                .append("2").append(3).append(6).append(2.0)
                .append("3").append(3).append(9).append(3.0)
                .append("4").append(3).append(12).append(4.0)
                .append("5").append(3).append(15).append(5.0)
                .append("6").append(3).append(18).append(6.0)
                .append("7").append(3).append(21).append(7.0)
                .append("8").append(3).append(24).append(8.0)
                .append("9").append(3).append(27).append(9.0)
                .build();

        Operator source = createOperator(new Page(
                BlockAssertions.createStringSequenceBlock(0, 10),
                BlockAssertions.createLongSequenceBlock(0, 10),
                BlockAssertions.createStringSequenceBlock(0, 10)),
                new Page(
                        BlockAssertions.createStringSequenceBlock(0, 10),
                        BlockAssertions.createLongSequenceBlock(0, 10),
                        BlockAssertions.createStringSequenceBlock(0, 10)),
                new Page(
                        BlockAssertions.createStringSequenceBlock(0, 10),
                        BlockAssertions.createLongSequenceBlock(0, 10),
                        BlockAssertions.createStringSequenceBlock(0, 10))
        );

        HashAggregationOperator actual = new HashAggregationOperator(source,
                2,
                ImmutableList.of(CountAggregation.PROVIDER, LongSumAggregation.provider(1, 0), LongAverageAggregation.provider(1, 0)),
                ImmutableList.of(concat(singleColumn(VARIABLE_BINARY, 0, 0), singleColumn(FIXED_INT_64, 1, 0), singleColumn(FIXED_INT_64, 2, 0), singleColumn(DOUBLE, 3, 0))));
        Iterator<Page> pages = actual.iterator();

        Page page = pages.next();
        assertEquals(page.getChannelCount(), 1);
        Block actualBlock = page.getBlock(0);
        BlockAssertions.assertBlockEqualsIgnoreOrder(actualBlock, expectedBlock);

        assertFalse(pages.hasNext());

    }
}
