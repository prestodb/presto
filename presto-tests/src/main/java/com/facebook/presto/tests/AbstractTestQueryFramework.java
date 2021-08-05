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
package com.facebook.presto.tests;

import com.facebook.presto.Session;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.cost.CostCalculator;
import com.facebook.presto.cost.CostCalculatorUsingExchanges;
import com.facebook.presto.cost.CostCalculatorWithEstimatedExchanges;
import com.facebook.presto.cost.CostComparator;
import com.facebook.presto.cost.TaskCountEstimator;
import com.facebook.presto.execution.QueryManagerConfig;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.security.AccessDeniedException;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.analyzer.QueryExplainer;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.PartitioningProviderManager;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.PlanFragmenter;
import com.facebook.presto.sql.planner.PlanOptimizers;
import com.facebook.presto.sql.planner.SubPlan;
import com.facebook.presto.sql.planner.assertions.PlanAssert;
import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.facebook.presto.sql.planner.optimizations.PlanOptimizer;
import com.facebook.presto.sql.planner.sanity.PlanChecker;
import com.facebook.presto.sql.tree.ExplainType;
import com.facebook.presto.testing.ExpectedQueryRunner;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.testing.TestingAccessControlManager.TestingPrivilege;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import org.intellij.lang.annotations.Language;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.weakref.jmx.MBeanExporter;
import org.weakref.jmx.testing.TestingMBeanServer;

import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.Consumer;

import static com.facebook.airlift.testing.Closeables.closeAllRuntimeException;
import static com.facebook.presto.sql.ParsingUtil.createParsingOptions;
import static com.facebook.presto.sql.SqlFormatter.formatSql;
import static com.facebook.presto.transaction.TransactionBuilder.transaction;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public abstract class AbstractTestQueryFramework
{
    private QueryRunner queryRunner;
    private ExpectedQueryRunner expectedQueryRunner;
    private SqlParser sqlParser;

    @BeforeClass
    public void init()
            throws Exception
    {
        queryRunner = createQueryRunner();
        expectedQueryRunner = createExpectedQueryRunner();
        sqlParser = new SqlParser();
    }

    protected abstract QueryRunner createQueryRunner()
            throws Exception;

    protected ExpectedQueryRunner createExpectedQueryRunner()
            throws Exception
    {
        return new H2QueryRunner();
    }

    @AfterClass(alwaysRun = true)
    public void close()
            throws Exception
    {
        closeAllRuntimeException(queryRunner, expectedQueryRunner);
        queryRunner = null;
        expectedQueryRunner = null;
        sqlParser = null;
    }

    protected Session getSession()
    {
        return queryRunner.getDefaultSession();
    }

    public final int getNodeCount()
    {
        return queryRunner.getNodeCount();
    }

    protected MaterializedResult computeActual(@Language("SQL") String sql)
    {
        return computeActual(getSession(), sql);
    }

    protected MaterializedResult computeActual(Session session, @Language("SQL") String sql)
    {
        return queryRunner.execute(session, sql).toTestTypes();
    }

    protected Object computeScalar(@Language("SQL") String sql)
    {
        return computeActual(sql).getOnlyValue();
    }

    protected void assertQuery(@Language("SQL") String sql)
    {
        assertQuery(getSession(), sql);
    }

    protected void assertQuery(Session session, @Language("SQL") String sql)
    {
        QueryAssertions.assertQuery(queryRunner, session, sql, expectedQueryRunner, sql, false, false);
    }

    protected void assertQuery(@Language("SQL") String actual, @Language("SQL") String expected)
    {
        QueryAssertions.assertQuery(queryRunner, getSession(), actual, expectedQueryRunner, expected, false, false);
    }

    protected void assertQuery(Session session, @Language("SQL") String actual, @Language("SQL") String expected)
    {
        QueryAssertions.assertQuery(queryRunner, session, actual, expectedQueryRunner, expected, false, false);
    }

    protected void assertQuery(Session session, @Language("SQL") String sql, Consumer<Plan> planAssertion)
    {
        checkArgument(queryRunner instanceof DistributedQueryRunner, "pattern assertion is only supported for DistributedQueryRunner");
        QueryAssertions.assertQuery(queryRunner, session, sql, expectedQueryRunner, sql, false, false, planAssertion);
    }

    protected void assertQuery(Session session, @Language("SQL") String actual, @Language("SQL") String expected, Consumer<Plan> planAssertion)
    {
        checkArgument(queryRunner instanceof DistributedQueryRunner, "pattern assertion is only supported for DistributedQueryRunner");
        QueryAssertions.assertQuery(queryRunner, session, actual, expectedQueryRunner, expected, false, false, planAssertion);
    }

    public void assertQueryOrdered(@Language("SQL") String sql)
    {
        assertQueryOrdered(getSession(), sql);
    }

    public void assertQueryOrdered(Session session, @Language("SQL") String sql)
    {
        assertQueryOrdered(session, sql, sql);
    }

    protected void assertQueryOrdered(@Language("SQL") String actual, @Language("SQL") String expected)
    {
        assertQueryOrdered(getSession(), actual, expected);
    }

    protected void assertQueryOrdered(Session session, @Language("SQL") String actual, @Language("SQL") String expected)
    {
        QueryAssertions.assertQuery(queryRunner, session, actual, expectedQueryRunner, expected, true, false);
    }

    protected void assertUpdate(@Language("SQL") String actual, @Language("SQL") String expected)
    {
        assertUpdate(getSession(), actual, expected);
    }

    protected void assertUpdate(Session session, @Language("SQL") String actual, @Language("SQL") String expected)
    {
        QueryAssertions.assertQuery(queryRunner, session, actual, expectedQueryRunner, expected, false, true);
    }

    protected void assertUpdate(@Language("SQL") String sql)
    {
        assertUpdate(getSession(), sql);
    }

    protected void assertUpdate(Session session, @Language("SQL") String sql)
    {
        QueryAssertions.assertUpdate(queryRunner, session, sql, OptionalLong.empty(), Optional.empty());
    }

    protected void assertUpdate(@Language("SQL") String sql, long count)
    {
        assertUpdate(getSession(), sql, count);
    }

    protected void assertUpdate(Session session, @Language("SQL") String sql, long count)
    {
        QueryAssertions.assertUpdate(queryRunner, session, sql, OptionalLong.of(count), Optional.empty());
    }

    protected void assertUpdate(Session session, @Language("SQL") String sql, long count, Consumer<Plan> planAssertion)
    {
        QueryAssertions.assertUpdate(queryRunner, session, sql, OptionalLong.of(count), Optional.of(planAssertion));
    }

    protected void assertQuerySucceeds(Session session, @Language("SQL") String sql)
    {
        QueryAssertions.assertQuerySucceeds(queryRunner, session, sql);
    }

    protected void assertQuerySucceeds(@Language("SQL") String sql)
    {
        QueryAssertions.assertQuerySucceeds(queryRunner, getSession(), sql);
    }

    protected void assertQueryFailsEventually(@Language("SQL") String sql, @Language("RegExp") String expectedMessageRegExp, Duration timeout)
    {
        QueryAssertions.assertQueryFailsEventually(queryRunner, getSession(), sql, expectedMessageRegExp, timeout);
    }

    protected void assertQueryFails(@Language("SQL") String sql, @Language("RegExp") String expectedMessageRegExp)
    {
        QueryAssertions.assertQueryFails(queryRunner, getSession(), sql, expectedMessageRegExp);
    }

    protected void assertQueryFails(Session session, @Language("SQL") String sql, @Language("RegExp") String expectedMessageRegExp)
    {
        QueryAssertions.assertQueryFails(queryRunner, session, sql, expectedMessageRegExp);
    }

    protected void assertQueryReturnsEmptyResult(@Language("SQL") String sql)
    {
        QueryAssertions.assertQueryReturnsEmptyResult(queryRunner, getSession(), sql);
    }

    protected void assertQueryReturnsEmptyResult(Session session, @Language("SQL") String sql)
    {
        QueryAssertions.assertQueryReturnsEmptyResult(queryRunner, session, sql);
    }

    protected void assertAccessAllowed(@Language("SQL") String sql, TestingPrivilege... deniedPrivileges)
    {
        assertAccessAllowed(getSession(), sql, deniedPrivileges);
    }

    protected void assertAccessAllowed(Session session, @Language("SQL") String sql, TestingPrivilege... deniedPrivileges)
    {
        executeExclusively(() -> {
            try {
                queryRunner.getAccessControl().deny(deniedPrivileges);
                queryRunner.execute(session, sql);
            }
            finally {
                queryRunner.getAccessControl().reset();
            }
        });
    }

    protected void assertAccessDenied(@Language("SQL") String sql, @Language("RegExp") String exceptionsMessageRegExp, TestingPrivilege... deniedPrivileges)
    {
        assertAccessDenied(getSession(), sql, exceptionsMessageRegExp, deniedPrivileges);
    }

    protected void assertAccessDenied(
            Session session,
            @Language("SQL") String sql,
            @Language("RegExp") String exceptionsMessageRegExp,
            TestingPrivilege... deniedPrivileges)
    {
        executeExclusively(() -> {
            try {
                queryRunner.getAccessControl().deny(deniedPrivileges);
                queryRunner.execute(session, sql);
                fail("Expected " + AccessDeniedException.class.getSimpleName());
            }
            catch (RuntimeException e) {
                assertExceptionMessage(sql, e, ".*Access Denied: " + exceptionsMessageRegExp);
            }
            finally {
                queryRunner.getAccessControl().reset();
            }
        });
    }

    protected void assertTableColumnNames(String tableName, String... columnNames)
    {
        MaterializedResult result = computeActual("DESCRIBE " + tableName);
        List<String> expected = ImmutableList.copyOf(columnNames);
        List<String> actual = result.getMaterializedRows().stream()
                .map(row -> (String) row.getField(0))
                .collect(toImmutableList());
        assertEquals(actual, expected);
    }

    private static void assertExceptionMessage(String sql, Exception exception, @Language("RegExp") String regex)
    {
        if (!nullToEmpty(exception.getMessage()).matches(regex)) {
            fail(format("Expected exception message '%s' to match '%s' for query: %s", exception.getMessage(), regex, sql), exception);
        }
    }

    protected MaterializedResult computeExpected(@Language("SQL") String sql, List<? extends Type> resultTypes)
    {
        return expectedQueryRunner.execute(getSession(), sql, resultTypes);
    }

    protected void executeExclusively(Runnable executionBlock)
    {
        queryRunner.getExclusiveLock().lock();
        try {
            executionBlock.run();
        }
        finally {
            queryRunner.getExclusiveLock().unlock();
        }
    }

    protected String formatSqlText(String sql)
    {
        return formatSql(sqlParser.createStatement(sql, createParsingOptions(queryRunner.getDefaultSession())), Optional.empty());
    }

    //TODO: should WarningCollector be added?
    public String getExplainPlan(String query, ExplainType.Type planType)
    {
        QueryExplainer explainer = getQueryExplainer();
        return transaction(queryRunner.getTransactionManager(), queryRunner.getAccessControl())
                .singleStatement()
                .execute(queryRunner.getDefaultSession(), session -> {
                    return explainer.getPlan(session, sqlParser.createStatement(query, createParsingOptions(session)), planType, emptyList(), false, WarningCollector.NOOP);
                });
    }

    public String getGraphvizExplainPlan(String query, ExplainType.Type planType)
    {
        QueryExplainer explainer = getQueryExplainer();
        return transaction(queryRunner.getTransactionManager(), queryRunner.getAccessControl())
                .singleStatement()
                .execute(queryRunner.getDefaultSession(), session -> {
                    return explainer.getGraphvizPlan(session, sqlParser.createStatement(query, createParsingOptions(session)), planType, emptyList(), WarningCollector.NOOP);
                });
    }

    public String getJsonExplainPlan(String query, ExplainType.Type planType)
    {
        QueryExplainer explainer = getQueryExplainer();
        return transaction(queryRunner.getTransactionManager(), queryRunner.getAccessControl())
                .singleStatement()
                .execute(queryRunner.getDefaultSession(), session -> {
                    return explainer.getJsonPlan(session, sqlParser.createStatement(query, createParsingOptions(session)), planType, emptyList(), WarningCollector.NOOP);
                });
    }

    protected void assertPlan(@Language("SQL") String query, PlanMatchPattern pattern)
    {
        assertPlan(queryRunner.getDefaultSession(), query, pattern);
    }

    protected void assertPlan(Session session, @Language("SQL") String query, PlanMatchPattern pattern)
    {
        assertPlan(session, query, pattern, plan -> {});
    }

    protected void assertPlan(Session session, @Language("SQL") String query, PlanMatchPattern pattern, Consumer<Plan> planValidator)
    {
        QueryExplainer explainer = getQueryExplainer();
        transaction(queryRunner.getTransactionManager(), queryRunner.getAccessControl())
                .singleStatement()
                .execute(session, transactionSession -> {
                    Plan actualPlan = explainer.getLogicalPlan(transactionSession, sqlParser.createStatement(query, createParsingOptions(transactionSession)), emptyList(), WarningCollector.NOOP);
                    PlanAssert.assertPlan(transactionSession, queryRunner.getMetadata(), queryRunner.getStatsCalculator(), actualPlan, pattern);
                    planValidator.accept(actualPlan);
                    return null;
                });
    }

    protected SubPlan subplan(String sql)
    {
        return subplan(sql, queryRunner.getDefaultSession());
    }

    protected SubPlan subplan(String sql, Session session)
    {
        QueryExplainer explainer = getQueryExplainer();
        try {
            return transaction(queryRunner.getTransactionManager(), queryRunner.getAccessControl())
                    .singleStatement()
                    .execute(session, transactionSession -> {
                        return explainer.getDistributedPlan(transactionSession, sqlParser.createStatement(sql, createParsingOptions(transactionSession)), emptyList(), WarningCollector.NOOP);
                    });
        }
        catch (RuntimeException e) {
            throw new AssertionError("Planning failed for SQL: " + sql, e);
        }
    }

    private QueryExplainer getQueryExplainer()
    {
        Metadata metadata = queryRunner.getMetadata();
        FeaturesConfig featuresConfig = new FeaturesConfig().setOptimizeHashGeneration(true);
        boolean forceSingleNode = queryRunner.getNodeCount() == 1;
        TaskCountEstimator taskCountEstimator = new TaskCountEstimator(queryRunner::getNodeCount);
        CostCalculator costCalculator = new CostCalculatorUsingExchanges(taskCountEstimator);
        List<PlanOptimizer> optimizers = new PlanOptimizers(
                metadata,
                sqlParser,
                forceSingleNode,
                new MBeanExporter(new TestingMBeanServer()),
                queryRunner.getSplitManager(),
                queryRunner.getPlanOptimizerManager(),
                queryRunner.getPageSourceManager(),
                queryRunner.getStatsCalculator(),
                costCalculator,
                new CostCalculatorWithEstimatedExchanges(costCalculator, taskCountEstimator),
                new CostComparator(featuresConfig),
                taskCountEstimator,
                new PartitioningProviderManager()).getPlanningTimeOptimizers();
        return new QueryExplainer(
                optimizers,
                new PlanFragmenter(metadata, queryRunner.getNodePartitioningManager(), new QueryManagerConfig(), sqlParser, new FeaturesConfig()),
                metadata,
                queryRunner.getAccessControl(),
                sqlParser,
                queryRunner.getStatsCalculator(),
                costCalculator,
                ImmutableMap.of(),
                new PlanChecker(new FeaturesConfig(), false));
    }

    protected static void skipTestUnless(boolean requirement)
    {
        if (!requirement) {
            throw new SkipException("requirement not met");
        }
    }

    protected QueryRunner getQueryRunner()
    {
        checkState(queryRunner != null, "queryRunner not set");
        return queryRunner;
    }

    public interface QueryRunnerSupplier
    {
        QueryRunner get()
                throws Exception;
    }
    public interface ExpectedQueryRunnerSupplier
    {
        ExpectedQueryRunner get()
                throws Exception;
    }
}
