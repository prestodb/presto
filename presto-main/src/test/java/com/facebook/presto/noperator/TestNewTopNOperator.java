package com.facebook.presto.noperator;

import com.facebook.presto.execution.TaskMemoryManager;
import com.facebook.presto.operator.Page;
import com.facebook.presto.operator.ProjectionFunctions;
import com.facebook.presto.sql.tree.SortItem;
import com.facebook.presto.tuple.FieldOrderedTupleComparator;
import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.util.MaterializedResult;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;
import io.airlift.units.DataSize;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.noperator.NewOperatorAssertion.assertOperatorEquals;
import static com.facebook.presto.noperator.RowPagesBuilder.rowPagesBuilder;
import static com.facebook.presto.operator.ProjectionFunctions.singleColumn;
import static com.facebook.presto.tuple.TupleInfo.SINGLE_DOUBLE;
import static com.facebook.presto.tuple.TupleInfo.SINGLE_LONG;
import static com.facebook.presto.tuple.TupleInfo.Type.DOUBLE;
import static com.facebook.presto.tuple.TupleInfo.Type.FIXED_INT_64;
import static com.facebook.presto.tuple.TupleInfo.Type.VARIABLE_BINARY;
import static com.facebook.presto.util.MaterializedResult.resultBuilder;

public class TestNewTopNOperator
{
    @Test
    public void testSingleFieldKey()
            throws Exception
    {
        List<Page> input = rowPagesBuilder(SINGLE_LONG, SINGLE_DOUBLE)
                .row(1, 0.1)
                .row(2, 0.2)
                .pageBreak()
                .row(-1, -0.1)
                .row(4, 0.4)
                .pageBreak()
                .row(5, 0.5)
                .row(4, 0.41)
                .row(6, 0.6)
                .pageBreak()
                .build();

        NewOperator operator = new NewTopNOperator(
                2,
                0,
                ImmutableList.of(singleColumn(FIXED_INT_64, 0, 0), singleColumn(DOUBLE, 1, 0)),
                Ordering.from(new FieldOrderedTupleComparator(ImmutableList.of(0), ImmutableList.of(SortItem.Ordering.DESCENDING))),
                false,
                new TaskMemoryManager(new DataSize(1, DataSize.Unit.MEGABYTE)));

        MaterializedResult expected = resultBuilder(FIXED_INT_64, DOUBLE)
                .row(6, 0.6)
                .row(5, 0.5)
                .build();

        assertOperatorEquals(operator, input, expected);
    }

    @Test
    public void testMultiFieldKey()
            throws Exception
    {
        TupleInfo tupleInfo = new TupleInfo(VARIABLE_BINARY, FIXED_INT_64);
        List<Page> input = rowPagesBuilder(tupleInfo)
                .row("a", 1)
                .row("b", 2)
                .pageBreak()
                .row("f", 3)
                .row("a", 4)
                .pageBreak()
                .row("d", 5)
                .row("d", 7)
                .row("e", 6)
                .build();

        NewOperator operator = new NewTopNOperator(
                3,
                0,
                ImmutableList.of(ProjectionFunctions.concat(singleColumn(VARIABLE_BINARY, 0, 0), singleColumn(FIXED_INT_64, 0, 1))),
                Ordering.from(new FieldOrderedTupleComparator(ImmutableList.of(0, 1), ImmutableList.of(SortItem.Ordering.DESCENDING, SortItem.Ordering.DESCENDING))),
                false,
                new TaskMemoryManager(new DataSize(1, DataSize.Unit.MEGABYTE)));


        MaterializedResult expected = resultBuilder(tupleInfo)
                .row("f", 3)
                .row("e", 6)
                .row("d", 7)
                .build();

        assertOperatorEquals(operator, input, expected);
    }

    @Test
    public void testReverseOrder()
            throws Exception
    {
        List<Page> input = rowPagesBuilder(SINGLE_LONG, SINGLE_DOUBLE)
                .row(1, 0.1)
                .row(2, 0.2)
                .pageBreak()
                .row(-1, -0.1)
                .row(4, 0.4)
                .pageBreak()
                .row(5, 0.5)
                .row(4, 0.41)
                .row(6, 0.6)
                .pageBreak()
                .build();

        NewOperator operator = new NewTopNOperator(
                2,
                0,
                ImmutableList.of(singleColumn(FIXED_INT_64, 0, 0), singleColumn(DOUBLE, 1, 0)),
                Ordering.from(new FieldOrderedTupleComparator(ImmutableList.of(0), ImmutableList.of(SortItem.Ordering.ASCENDING))),
                false,
                new TaskMemoryManager(new DataSize(1, DataSize.Unit.MEGABYTE)));

        MaterializedResult expected = resultBuilder(FIXED_INT_64, DOUBLE)
                .row(-1, -0.1)
                .row(1, 0.1)
                .build();

        assertOperatorEquals(operator, input, expected);
    }
}
