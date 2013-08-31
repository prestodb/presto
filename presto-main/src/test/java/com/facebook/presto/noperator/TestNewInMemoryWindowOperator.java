package com.facebook.presto.noperator;

import com.facebook.presto.execution.TaskMemoryManager;
import com.facebook.presto.operator.Page;
import com.facebook.presto.operator.window.RowNumberFunction;
import com.facebook.presto.operator.window.WindowFunction;
import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.util.MaterializedResult;
import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.noperator.NewOperatorAssertion.assertOperatorEquals;
import static com.facebook.presto.noperator.NewOperatorAssertion.toPages;
import static com.facebook.presto.noperator.RowPagesBuilder.rowPagesBuilder;
import static com.facebook.presto.tuple.TupleInfo.SINGLE_DOUBLE;
import static com.facebook.presto.tuple.TupleInfo.SINGLE_LONG;
import static com.facebook.presto.tuple.TupleInfo.Type.BOOLEAN;
import static com.facebook.presto.tuple.TupleInfo.Type.DOUBLE;
import static com.facebook.presto.tuple.TupleInfo.Type.FIXED_INT_64;
import static com.facebook.presto.tuple.TupleInfo.Type.VARIABLE_BINARY;
import static com.facebook.presto.util.MaterializedResult.resultBuilder;

public class TestNewInMemoryWindowOperator
{
    private static final DataSize MAX_SIZE = new DataSize(1, Unit.MEGABYTE);
    private static final List<WindowFunction> ROW_NUMBER = ImmutableList.<WindowFunction>of(new RowNumberFunction());

    @Test
    public void testRowNumber()
            throws Exception
    {
        List<Page> input = rowPagesBuilder(SINGLE_LONG, SINGLE_DOUBLE)
                .row(2, 0.3)
                .row(4, 0.2)
                .row(6, 0.1)
                .pageBreak()
                .row(-1, -0.1)
                .row(5, 0.4)
                .build();

        NewOperator operator = new NewInMemoryWindowOperator(
                ImmutableList.of(SINGLE_LONG, SINGLE_DOUBLE),
                0,
                ints(1, 0),
                ROW_NUMBER,
                ints(),
                ints(0),
                bools(true),
                10,
                new TaskMemoryManager(MAX_SIZE));

        MaterializedResult expected = resultBuilder(DOUBLE, FIXED_INT_64, FIXED_INT_64)
                .row(-0.1, -1, 1)
                .row(0.3, 2, 2)
                .row(0.2, 4, 3)
                .row(0.4, 5, 4)
                .row(0.1, 6, 5)
                .build();

        assertOperatorEquals(operator, input, expected);
    }

    @Test
    public void testRowNumberPartition()
            throws Exception
    {
        TupleInfo sourceTupleInfo = new TupleInfo(VARIABLE_BINARY, FIXED_INT_64, DOUBLE, BOOLEAN);

        List<Page> input = rowPagesBuilder(sourceTupleInfo)
                .row("b", -1, -0.1, true)
                .row("a", 2, 0.3, false)
                .row("a", 4, 0.2, true)
                .pageBreak()
                .row("b", 5, 0.4, false)
                .row("a", 6, 0.1, true)
                .build();

        NewOperator operator = new NewInMemoryWindowOperator(
                ImmutableList.of(sourceTupleInfo),
                0,
                ints(0),
                ROW_NUMBER,
                ints(0),
                ints(1),
                bools(true),
                10,
                new TaskMemoryManager(MAX_SIZE));

        MaterializedResult expected = resultBuilder(VARIABLE_BINARY, FIXED_INT_64, DOUBLE, BOOLEAN, FIXED_INT_64)
                .row("a", 2, 0.3, false, 1)
                .row("a", 4, 0.2, true, 2)
                .row("a", 6, 0.1, true, 3)
                .row("b", -1, -0.1, true, 1)
                .row("b", 5, 0.4, false, 2)
                .build();

        assertOperatorEquals(operator, input, expected);
    }

    @Test
    public void testRowNumberArbitrary()
            throws Exception
    {
        List<Page> input = rowPagesBuilder(SINGLE_LONG)
                .row(1)
                .row(3)
                .row(5)
                .row(7)
                .pageBreak()
                .row(2)
                .row(4)
                .row(6)
                .row(8)
                .build();

        NewOperator operator = new NewInMemoryWindowOperator(
                ImmutableList.of(SINGLE_LONG),
                0,
                ints(0),
                ROW_NUMBER,
                ints(),
                ints(),
                bools(),
                10,
                new TaskMemoryManager(MAX_SIZE));

        MaterializedResult expected = resultBuilder(FIXED_INT_64, FIXED_INT_64)
                .row(1, 1)
                .row(3, 2)
                .row(5, 3)
                .row(7, 4)
                .row(2, 5)
                .row(4, 6)
                .row(6, 7)
                .row(8, 8)
                .build();

        assertOperatorEquals(operator, input, expected);
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = "Task exceeded max memory size of 10B")
    public void testMemoryLimit()
            throws Exception
    {
        List<Page> input = rowPagesBuilder(SINGLE_LONG, SINGLE_DOUBLE)
                .row(1, 0.1)
                .row(2, 0.2)
                .pageBreak()
                .row(-1, -0.1)
                .row(4, 0.4)
                .build();

        NewOperator operator = new NewInMemoryWindowOperator(
                ImmutableList.of(SINGLE_LONG, SINGLE_DOUBLE),
                0,
                ints(1),
                ROW_NUMBER,
                ints(),
                ints(0),
                bools(true),
                10,
                new TaskMemoryManager(new DataSize(10, Unit.BYTE)));

        toPages(operator, input);
    }

    private static int[] ints(int... array)
    {
        return array;
    }

    private static boolean[] bools(boolean... array)
    {
        return array;
    }
}
