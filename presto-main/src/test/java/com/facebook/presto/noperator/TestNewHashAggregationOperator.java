package com.facebook.presto.noperator;

import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.execution.TaskMemoryManager;
import com.facebook.presto.operator.Page;
import com.facebook.presto.sql.planner.plan.AggregationNode.Step;
import com.facebook.presto.sql.tree.Input;
import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.util.MaterializedResult;
import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.block.BlockAssertions.COMPOSITE_SEQUENCE_TUPLE_INFO;
import static com.facebook.presto.noperator.NewOperatorAssertion.assertOperatorEquals;
import static com.facebook.presto.noperator.NewOperatorAssertion.toPages;
import static com.facebook.presto.noperator.RowPagesBuilder.rowPagesBuilder;
import static com.facebook.presto.operator.AggregationFunctionDefinition.aggregation;
import static com.facebook.presto.operator.aggregation.CountAggregation.COUNT;
import static com.facebook.presto.operator.aggregation.CountColumnAggregation.COUNT_COLUMN;
import static com.facebook.presto.operator.aggregation.DoubleSumAggregation.DOUBLE_SUM;
import static com.facebook.presto.operator.aggregation.LongAverageAggregation.LONG_AVERAGE;
import static com.facebook.presto.operator.aggregation.LongSumAggregation.LONG_SUM;
import static com.facebook.presto.operator.aggregation.VarBinaryMaxAggregation.VAR_BINARY_MAX;
import static com.facebook.presto.tuple.TupleInfo.SINGLE_LONG;
import static com.facebook.presto.tuple.TupleInfo.SINGLE_VARBINARY;
import static com.facebook.presto.tuple.TupleInfo.Type.DOUBLE;
import static com.facebook.presto.tuple.TupleInfo.Type.FIXED_INT_64;
import static com.facebook.presto.tuple.TupleInfo.Type.VARIABLE_BINARY;
import static com.facebook.presto.util.MaterializedResult.resultBuilder;
import static org.testng.Assert.assertEquals;

public class TestNewHashAggregationOperator
{
    @Test
    public void testHashAggregation()
            throws Exception
    {
        List<Page> input = rowPagesBuilder(SINGLE_VARBINARY, SINGLE_VARBINARY, SINGLE_VARBINARY, SINGLE_LONG, COMPOSITE_SEQUENCE_TUPLE_INFO)
                .addSequencePage(10, 100, 0, 100, 0, 500)
                .addSequencePage(10, 100, 0, 200, 0, 500)
                .addSequencePage(10, 100, 0, 300, 0, 500)
                .build();

        NewOperator operator = new NewHashAggregationOperator(
                SINGLE_VARBINARY,
                1,
                Step.SINGLE,
                ImmutableList.of(aggregation(COUNT, new Input(0, 0)),
                        aggregation(LONG_SUM, new Input(3, 0)),
                        aggregation(LONG_AVERAGE, new Input(3, 0)),
                        aggregation(VAR_BINARY_MAX, new Input(2, 0)),
                        aggregation(COUNT_COLUMN, new Input(0, 0)),
                        aggregation(LONG_SUM, new Input(4, 1)),
                        aggregation(DOUBLE_SUM, new Input(4, 2)),
                        aggregation(VAR_BINARY_MAX, new Input(4, 3))),
                100_000,
                new TaskMemoryManager(new DataSize(100, Unit.MEGABYTE)));

        MaterializedResult expected = resultBuilder(VARIABLE_BINARY, FIXED_INT_64, FIXED_INT_64, DOUBLE, VARIABLE_BINARY, FIXED_INT_64, FIXED_INT_64, DOUBLE, VARIABLE_BINARY)
                .row("0", 3, 0, 0.0, "300", 3, 500 * 3, 500.0 * 3, "500")
                .row("1", 3, 3, 1.0, "301", 3, 501 * 3, 501.0 * 3, "501")
                .row("2", 3, 6, 2.0, "302", 3, 502 * 3, 502.0 * 3, "502")
                .row("3", 3, 9, 3.0, "303", 3, 503 * 3, 503.0 * 3, "503")
                .row("4", 3, 12, 4.0, "304", 3, 504 * 3, 504.0 * 3, "504")
                .row("5", 3, 15, 5.0, "305", 3, 505 * 3, 505.0 * 3, "505")
                .row("6", 3, 18, 6.0, "306", 3, 506 * 3, 506.0 * 3, "506")
                .row("7", 3, 21, 7.0, "307", 3, 507 * 3, 507.0 * 3, "507")
                .row("8", 3, 24, 8.0, "308", 3, 508 * 3, 508.0 * 3, "508")
                .row("9", 3, 27, 9.0, "309", 3, 509 * 3, 509.0 * 3, "509")
                .build();

        assertOperatorEquals(operator, input, expected);
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = "Task exceeded max memory size of 10B")
    public void testMemoryLimit()
    {
        List<Page> input = rowPagesBuilder(SINGLE_VARBINARY, SINGLE_VARBINARY, SINGLE_VARBINARY, SINGLE_LONG)
                .addSequencePage(10, 100, 0, 100, 0)
                .addSequencePage(10, 100, 0, 200, 0)
                .addSequencePage(10, 100, 0, 300, 0)
                .build();

        NewOperator operator = new NewHashAggregationOperator(
                SINGLE_VARBINARY,
                1,
                Step.SINGLE,
                ImmutableList.of(aggregation(COUNT, new Input(0, 0)),
                        aggregation(LONG_SUM, new Input(3, 0)),
                        aggregation(LONG_AVERAGE, new Input(3, 0)),
                        aggregation(VAR_BINARY_MAX, new Input(2, 0))),
                100_000,
                new TaskMemoryManager(new DataSize(10, Unit.BYTE), new DataSize(10, Unit.BYTE)));

        toPages(operator, input);
    }

    @Test
    public void testMultiSliceAggregationOutput()
    {
        long fixedWidthSize = new TupleInfo(TupleInfo.Type.FIXED_INT_64, TupleInfo.Type.DOUBLE).getFixedSize();
        int multiSlicePositionCount = (int) (BlockBuilder.DEFAULT_MAX_BLOCK_SIZE.toBytes() / fixedWidthSize) * 2;

        List<Page> input = rowPagesBuilder(SINGLE_VARBINARY, SINGLE_LONG)
                .addSequencePage(multiSlicePositionCount, 0, 0)
                .build();

        NewOperator operator = new NewHashAggregationOperator(
                SINGLE_LONG,
                1,
                Step.SINGLE,
                ImmutableList.of(aggregation(COUNT, new Input(0, 0)),
                        aggregation(LONG_AVERAGE, new Input(1, 0))),
                100_000,
                new TaskMemoryManager(new DataSize(100, Unit.MEGABYTE)));

        assertEquals(toPages(operator, input).size(), 2);
    }
}
