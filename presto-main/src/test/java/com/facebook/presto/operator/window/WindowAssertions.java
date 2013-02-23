package com.facebook.presto.operator.window;

import com.facebook.presto.util.MaterializedResult;
import org.intellij.lang.annotations.Language;

import static com.facebook.presto.AbstractTestQueries.assertEqualsIgnoreOrder;
import static com.facebook.presto.util.LocalQueryRunner.createTpchLocalQueryRunner;
import static java.lang.String.format;

public final class WindowAssertions
{
    private WindowAssertions() {}

    public static MaterializedResult computeActual(@Language("SQL") String sql)
    {
        return createTpchLocalQueryRunner().execute(sql);
    }

    public static void assertWindowQuery(@Language("SQL") String sql, MaterializedResult expected)
    {
        @Language("SQL") String query = format("" +
                "SELECT orderkey, orderstatus,\n%s\n" +
                "FROM (SELECT * FROM orders ORDER BY orderkey LIMIT 10) x\n" +
                "ORDER BY orderkey", sql);

        MaterializedResult actual = computeActual(query);
        assertEqualsIgnoreOrder(actual.getMaterializedTuples(), expected.getMaterializedTuples());
    }
}
