/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.sql.query;

import com.facebook.presto.Session;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.assertions.PlanAssert;
import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.testing.QueryRunner;
import org.intellij.lang.annotations.Language;

import java.io.Closeable;
import java.util.List;
import java.util.function.Consumer;

import static com.facebook.airlift.testing.Assertions.assertEqualsIgnoreOrder;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.google.common.base.Strings.nullToEmpty;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

class QueryAssertions
        implements Closeable
{
    private final QueryRunner runner;

    public QueryAssertions()
    {
        this(testSessionBuilder()
                .setCatalog("local")
                .setSchema("default")
                .build());
    }

    public QueryAssertions(Session session)
    {
        runner = new LocalQueryRunner(session);
    }

    public QueryRunner getQueryRunner()
    {
        return runner;
    }

    public void assertFails(@Language("SQL") String sql, @Language("RegExp") String expectedMessageRegExp)
    {
        try {
            runner.execute(runner.getDefaultSession(), sql).toTestTypes();
            fail(format("Expected query to fail: %s", sql));
        }
        catch (RuntimeException exception) {
            if (!nullToEmpty(exception.getMessage()).matches(expectedMessageRegExp)) {
                fail(format("Expected exception message '%s' to match '%s' for query: %s", exception.getMessage(), expectedMessageRegExp, sql), exception);
            }
        }
    }

    public void assertQueryAndPlan(
            @Language("SQL") String actual,
            @Language("SQL") String expected,
            PlanMatchPattern pattern,
            Consumer<Plan> planValidator)
    {
        assertQuery(actual, expected);
        Plan plan = runner.createPlan(runner.getDefaultSession(), actual, WarningCollector.NOOP);
        PlanAssert.assertPlan(runner.getDefaultSession(), runner.getMetadata(), runner.getStatsCalculator(), plan, pattern);
        planValidator.accept(plan);
    }

    public void assertQuery(@Language("SQL") String actual, @Language("SQL") String expected)
    {
        assertQuery(actual, expected, false);
    }

    public void assertQuery(@Language("SQL") String actual, @Language("SQL") String expected, boolean ensureOrdering)
    {
        MaterializedResult actualResults = null;
        try {
            actualResults = runner.execute(runner.getDefaultSession(), actual).toTestTypes();
        }
        catch (RuntimeException ex) {
            fail("Execution of 'actual' query failed: " + actual, ex);
        }

        MaterializedResult expectedResults = null;
        try {
            expectedResults = runner.execute(runner.getDefaultSession(), expected).toTestTypes();
        }
        catch (RuntimeException ex) {
            fail("Execution of 'expected' query failed: " + expected, ex);
        }

        assertEquals(actualResults.getTypes(), expectedResults.getTypes(), "Types mismatch for query: \n " + actual + "\n:");

        List<MaterializedRow> actualRows = actualResults.getMaterializedRows();
        List<MaterializedRow> expectedRows = expectedResults.getMaterializedRows();

        if (ensureOrdering) {
            if (!actualRows.equals(expectedRows)) {
                assertEquals(actualRows, expectedRows, "For query: \n " + actual + "\n:");
            }
        }
        else {
            assertEqualsIgnoreOrder(actualRows, expectedRows, "For query: \n " + actual);
        }
    }

    @Override
    public void close()
    {
        runner.close();
    }
}
