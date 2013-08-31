package com.facebook.presto.noperator;

import com.facebook.presto.operator.Page;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.noperator.RowPagesBuilder.rowPagesBuilder;
import static com.facebook.presto.tuple.TupleInfo.SINGLE_LONG;

public class TestNewLimitOperator
{
    @Test
    public void testLimitWithPageAlignment()
            throws Exception
    {
        List<Page> input = rowPagesBuilder(SINGLE_LONG)
                .addSequencePage(3, 1)
                .addSequencePage(2, 4)
                .addSequencePage(2, 6)
                .build();

        NewLimitOperator operator = new NewLimitOperator(ImmutableList.of(SINGLE_LONG), 5);

        List<Page> expected = rowPagesBuilder(SINGLE_LONG)
                .addSequencePage(3, 1)
                .addSequencePage(2, 4)
                .build();

        NewOperatorAssertion.assertOperatorEquals(operator, input, expected);
    }

    @Test
    public void testLimitWithBlockView()
            throws Exception
    {
        List<Page> input = rowPagesBuilder(SINGLE_LONG)
                .addSequencePage(3, 1)
                .addSequencePage(2, 4)
                .addSequencePage(2, 6)
                .build();

        NewLimitOperator operator = new NewLimitOperator(ImmutableList.of(SINGLE_LONG), 6);

        List<Page> expected = rowPagesBuilder(SINGLE_LONG)
                .addSequencePage(3, 1)
                .addSequencePage(2, 4)
                .addSequencePage(1, 6)
                .build();

        NewOperatorAssertion.assertOperatorEquals(operator, input, expected);
    }
}
