package com.facebook.presto.operator.window;

import com.facebook.presto.util.MaterializedResult;
import org.testng.annotations.Test;

import static com.facebook.presto.operator.window.WindowAssertions.assertWindowQuery;
import static com.facebook.presto.tuple.TupleInfo.Type.FIXED_INT_64;
import static com.facebook.presto.tuple.TupleInfo.Type.VARIABLE_BINARY;
import static com.facebook.presto.util.MaterializedResult.resultBuilder;

public class TestWindowFunctions
{
    @Test
    public void testRowNumber()
    {
        MaterializedResult expected = resultBuilder(FIXED_INT_64, VARIABLE_BINARY, FIXED_INT_64)
                .row(1, "O", 1)
                .row(2, "O", 2)
                .row(3, "F", 3)
                .row(4, "O", 4)
                .row(5, "F", 5)
                .row(6, "F", 6)
                .row(7, "O", 7)
                .row(32, "O", 8)
                .row(33, "F", 9)
                .row(34, "O", 10)
                .build();

        assertWindowQuery("row_number() OVER ()", expected);
        assertWindowQuery("row_number() OVER (ORDER BY orderkey)", expected);
    }

    @Test
    public void testRowNumberPartitioning()
    {
        assertWindowQuery("row_number() OVER (PARTITION BY orderstatus ORDER BY orderkey)",
                resultBuilder(FIXED_INT_64, VARIABLE_BINARY, FIXED_INT_64)
                        .row(3, "F", 1)
                        .row(5, "F", 2)
                        .row(6, "F", 3)
                        .row(33, "F", 4)
                        .row(1, "O", 1)
                        .row(2, "O", 2)
                        .row(4, "O", 3)
                        .row(7, "O", 4)
                        .row(32, "O", 5)
                        .row(34, "O", 6)
                        .build());

        // TODO: add better test for non-deterministic sorting behavior
        assertWindowQuery("row_number() OVER (PARTITION BY orderstatus)",
                resultBuilder(FIXED_INT_64, VARIABLE_BINARY, FIXED_INT_64)
                        .row(3, "F", 1)
                        .row(5, "F", 2)
                        .row(33, "F", 3)
                        .row(6, "F", 4)
                        .row(32, "O", 1)
                        .row(34, "O", 2)
                        .row(1, "O", 3)
                        .row(2, "O", 4)
                        .row(4, "O", 5)
                        .row(7, "O", 6)
                        .build());
    }
}
