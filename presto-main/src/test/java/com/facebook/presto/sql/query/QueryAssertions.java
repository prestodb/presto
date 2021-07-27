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
import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.AssertProvider;
import org.assertj.core.api.ListAssert;
import org.assertj.core.presentation.Representation;
import org.assertj.core.presentation.StandardRepresentation;
import org.intellij.lang.annotations.Language;

import java.io.Closeable;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static com.facebook.airlift.testing.Assertions.assertEqualsIgnoreOrder;
import static com.facebook.presto.sql.query.QueryAssertions.QueryAssert.newQueryAssert;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.google.common.base.Strings.nullToEmpty;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class QueryAssertions
        implements Closeable
{
    protected QueryRunner runner;

    public QueryAssertions()
    {
        this(testSessionBuilder()
                .setCatalog("local")
                .setSchema("default")
                .build());
    }

    public QueryAssertions(Map<String, String> systemProperties)
    {
        Session.SessionBuilder builder = testSessionBuilder()
                .setCatalog("local")
                .setSchema("default");
        systemProperties.forEach(builder::setSystemProperty);
        runner = new LocalQueryRunner(builder.build());
    }

    public QueryAssertions(Session session)
    {
        runner = new LocalQueryRunner(session);
    }

    public QueryAssertions(QueryRunner runner)
    {
        this.runner = requireNonNull(runner, "runner is null");
    }

    public QueryRunner getQueryRunner()
    {
        return runner;
    }

    public Session.SessionBuilder sessionBuilder()
    {
        return Session.builder(runner.getDefaultSession());
    }

    public Session getDefaultSession()
    {
        return runner.getDefaultSession();
    }

    public AssertProvider<QueryAssert> query(@Language("SQL") String query)
    {
        return query(query, runner.getDefaultSession());
    }

    public AssertProvider<QueryAssert> query(@Language("SQL") String query, Session session)
    {
        return newQueryAssert(query, runner, session);
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

    private void assertQuery(@Language("SQL") String actual, @Language("SQL") String expected, boolean ensureOrdering)
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

    public static class QueryAssert
            extends AbstractAssert<QueryAssert, MaterializedResult>
    {
        private static final Representation ROWS_REPRESENTATION = new StandardRepresentation()
        {
            @Override
            public String toStringOf(Object object)
            {
                if (object instanceof List) {
                    List<?> list = (List<?>) object;
                    return list.stream()
                            .map(this::toStringOf)
                            .collect(Collectors.joining(", "));
                }
                if (object instanceof MaterializedRow) {
                    MaterializedRow row = (MaterializedRow) object;

                    return row.getFields().stream()
                            .map(Object::toString)
                            .collect(Collectors.joining(", ", "(", ")"));
                }
                else {
                    return super.toStringOf(object);
                }
            }
        };

        private final QueryRunner runner;
        private final Session session;
        private boolean ordered;

        static AssertProvider<QueryAssert> newQueryAssert(String query, QueryRunner runner, Session session)
        {
            MaterializedResult result = runner.execute(session, query);
            return () -> new QueryAssert(runner, session, result);
        }

        public QueryAssert(QueryRunner runner, Session session, MaterializedResult actual)
        {
            super(actual, Object.class);
            this.runner = runner;
            this.session = session;
        }

        public QueryAssert matches(BiFunction<Session, QueryRunner, MaterializedResult> evaluator)
        {
            MaterializedResult expected = evaluator.apply(session, runner);
            return isEqualTo(expected);
        }

        public QueryAssert ordered()
        {
            ordered = true;
            return this;
        }

        public QueryAssert matches(@Language("SQL") String query)
        {
            MaterializedResult expected = runner.execute(session, query);

            return satisfies(actual -> {
                assertThat(actual.getTypes())
                        .as("Output types")
                        .isEqualTo(expected.getTypes());

                ListAssert<MaterializedRow> assertion = assertThat(actual.getMaterializedRows())
                        .as("Rows")
                        .withRepresentation(ROWS_REPRESENTATION);

                if (ordered) {
                    assertion.containsExactlyElementsOf(expected.getMaterializedRows());
                }
                else {
                    assertion.containsExactlyInAnyOrder(expected.getMaterializedRows().toArray(new MaterializedRow[0]));
                }
            });
        }
    }
}
