package com.facebook.presto.noperator;

import com.facebook.presto.operator.Page;
import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.util.MaterializedResult;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.facebook.presto.operator.PageAssertions.assertPageEquals;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

public final class NewOperatorAssertion
{
    private NewOperatorAssertion()
    {
    }

    public static List<Page> toPages(NewOperator operator, List<Page> input)
    {
        ImmutableList.Builder<Page> outputPages = ImmutableList.builder();

        // verify initial state
        assertEquals(operator.isFinished(), false);
        assertEquals(operator.needsInput(), true);
        assertEquals(operator.getOutput(), null);

        // process input pages
        for (Page inputPage : input) {
            // read output until input is needed or operator is finished
            while (!operator.needsInput() && !operator.isFinished()) {
                Page outputPage = operator.getOutput();
                assertNotNull(outputPage);
                outputPages.add(outputPage);
            }

            if (operator.isFinished()) {
                break;
            }

            assertEquals(operator.needsInput(), true);
            operator.addInput(inputPage);

            Page outputPage = operator.getOutput();
            if (outputPage != null) {
                outputPages.add(outputPage);
            }
        }

        // finish
        operator.finish();
        assertEquals(operator.needsInput(), false);

        // add remaining output pages
        addRemainingOutputPages(operator, outputPages);
        return outputPages.build();
    }

    public static List<Page> toPages(NewOperator operator)
    {
        // operator does not have input so should never require input
        assertEquals(operator.needsInput(), false);

        ImmutableList.Builder<Page> outputPages = ImmutableList.builder();
        addRemainingOutputPages(operator, outputPages);
        return outputPages.build();
    }

    private static void addRemainingOutputPages(NewOperator operator, ImmutableList.Builder<Page> outputPages)
    {
        // pull remaining output pages
        while (true) {
            // at this point the operator should not need more input
            assertEquals(operator.needsInput(), false);

            Page outputPage = operator.getOutput();
            if (outputPage == null) {
                break;
            }
            outputPages.add(outputPage);
        }

        // verify final state
        assertEquals(operator.isFinished(), true);
        assertEquals(operator.needsInput(), false);
        assertEquals(operator.getOutput(), null);
    }

    public static MaterializedResult toMaterializedResult(List<TupleInfo> tupleInfos, List<Page> pages)
    {
        // materialize pages
        MaterializingOperator materializingOperator = new MaterializingOperator(tupleInfos);
        for (Page outputPage : pages) {
            assertEquals(materializingOperator.needsInput(), true);
            materializingOperator.addInput(outputPage);
        }
        materializingOperator.finish();
        return materializingOperator.getMaterializedResult();
    }

    public static void assertOperatorEquals(NewOperator operator, List<Page> expected)
    {
        List<Page> actual = toPages(operator);
        assertEquals(actual.size(), expected.size());
        for (int i = 0; i < actual.size(); i++) {
            assertPageEquals(actual.get(i), expected.get(i));
        }
    }

    public static void assertOperatorEquals(NewOperator operator, List<Page> input, List<Page> expected)
    {
        List<Page> actual = toPages(operator, input);
        assertEquals(actual.size(), expected.size());
        for (int i = 0; i < actual.size(); i++) {
            assertPageEquals(actual.get(i), expected.get(i));
        }
    }

    public static void assertOperatorEquals(NewOperator operator, MaterializedResult expected)
    {
        List<Page> pages = toPages(operator);
        MaterializedResult actual = toMaterializedResult(operator.getTupleInfos(), pages);
        assertEquals(actual, expected);
    }

    public static void assertOperatorEquals(NewOperator operator, List<Page> input, MaterializedResult expected)
    {
        List<Page> pages = toPages(operator, input);
        MaterializedResult actual = toMaterializedResult(operator.getTupleInfos(), pages);
        assertEquals(actual, expected);
    }
}
