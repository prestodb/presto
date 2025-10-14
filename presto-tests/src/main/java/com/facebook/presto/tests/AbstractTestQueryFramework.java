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

import com.facebook.airlift.node.NodeInfo;
import com.facebook.airlift.units.Duration;
import com.facebook.presto.Session;
import com.facebook.presto.common.transaction.TransactionId;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.cost.CostCalculator;
import com.facebook.presto.cost.CostCalculatorUsingExchanges;
import com.facebook.presto.cost.CostCalculatorWithEstimatedExchanges;
import com.facebook.presto.cost.CostComparator;
import com.facebook.presto.cost.TaskCountEstimator;
import com.facebook.presto.execution.QueryManagerConfig;
import com.facebook.presto.execution.TaskManagerConfig;
import com.facebook.presto.metadata.InMemoryNodeManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.nodeManager.PluginNodeManager;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.security.AccessDeniedException;
import com.facebook.presto.spi.security.AllowAllAccessControl;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.analyzer.QueryExplainer;
import com.facebook.presto.sql.expressions.ExpressionOptimizerManager;
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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
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
import static com.facebook.presto.sql.SqlFormatter.formatSql;
import static com.facebook.presto.transaction.TransactionBuilder.transaction;
import static com.facebook.presto.util.AnalyzerUtil.createParsingOptions;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public abstract class AbstractTestQueryFramework
{
    private static final NodeInfo NODE_INFO = new NodeInfo("test");
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
        createTables();
    }

    protected void createTables() {}

    protected abstract QueryRunner createQueryRunner()
            throws Exception;

    protected ExpectedQueryRunner createExpectedQueryRunner()
            throws Exception
    {
        return new H2QueryRunner();
    }

    protected ObjectMapper createObjectMapper()
    {
        return new ObjectMapper();
    }

    protected FeaturesConfig createFeaturesConfig()
    {
        return new FeaturesConfig().setOptimizeHashGeneration(true);
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

    protected Object computeScalar(Session session, @Language("SQL") String sql)
    {
        return computeActual(session, sql).getOnlyValue();
    }

    protected MaterializedResult computeActual(QueryRunner queryRunner, Session session, @Language("SQL") String sql)
    {
        return queryRunner.execute(session, sql).toTestTypes();
    }

    protected Object computeScalarExpected(Session session, @Language("SQL") String sql)
    {
        return computeActual((QueryRunner) expectedQueryRunner, session, sql).getOnlyValue();
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

    protected void assertQueryWithSameQueryRunner(@Language("SQL") String actual, @Language("SQL") String expected)
    {
        checkArgument(!actual.equals(expected));
        QueryAssertions.assertQuery(queryRunner, getSession(), actual, queryRunner, expected, false, false);
    }

    protected void assertQueryWithSameQueryRunner(Session session, @Language("SQL") String actual, @Language("SQL") String expected)
    {
        checkArgument(!actual.equals(expected));
        QueryAssertions.assertQuery(queryRunner, session, actual, queryRunner, expected, false, false);
    }

    protected void assertQueryOrderedWithSameQueryRunner(@Language("SQL") String actual, @Language("SQL") String expected)
    {
        checkArgument(!actual.equals(expected));
        QueryAssertions.assertQuery(queryRunner, getSession(), actual, queryRunner, expected, true, false);
    }

    protected void assertQueryWithSameQueryRunner(Session actualSession, @Language("SQL") String query, Session expectedSession)
    {
        QueryAssertions.assertQuery(queryRunner, actualSession, query, queryRunner, expectedSession, query, false, false);
    }

    protected void assertQueryWithSameQueryRunner(Session actualSession, @Language("SQL") String actual, Session expectedSession, @Language("SQL") String expected)
    {
        checkArgument(!actual.equals(expected));
        QueryAssertions.assertQuery(queryRunner, actualSession, actual, queryRunner, expectedSession, expected, false, false);
    }

    protected void assertQuery(Session session, @Language("SQL") String actual, @Language("SQL") String expected)
    {
        QueryAssertions.assertQuery(queryRunner, session, actual, expectedQueryRunner, expected, false, false);
    }

    protected void assertQuery(Session actualSession, @Language("SQL") String actual, Session expectedSession, @Language("SQL") String expected)
    {
        QueryAssertions.assertQuery(queryRunner, actualSession, actual, expectedQueryRunner, expectedSession, expected, false, false);
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

    protected Session assertStartTransaction(Session session, @Language("SQL") String sql)
    {
        return QueryAssertions.assertStartTransaction(queryRunner, session, sql);
    }

    protected Session assertEndTransaction(Session session, @Language("SQL") String sql)
    {
        return QueryAssertions.assertEndTransaction(queryRunner, session, sql);
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

    protected void assertUpdateExpected(Session session, @Language("SQL") String sql, long count)
    {
        QueryAssertions.assertUpdate((QueryRunner) expectedQueryRunner, session, sql, OptionalLong.of(count), Optional.empty());
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

    protected void assertQueryFailsExact(@Language("SQL") String sql, @Language("RegExp") String expectedMessageRegExp)
    {
        QueryAssertions.assertQueryFails(queryRunner, getSession(), sql, expectedMessageRegExp, false, true);
    }

    protected void assertQueryFails(QueryRunner queryRunner, @Language("SQL") String sql, @Language("RegExp") String expectedMessageRegExp)
    {
        QueryAssertions.assertQueryFails(queryRunner, getSession(), sql, expectedMessageRegExp);
    }

    protected void assertQueryFails(@Language("SQL") String sql, @Language("RegExp") String expectedMessageRegExp, boolean usePatternMatcher)
    {
        QueryAssertions.assertQueryFails(queryRunner, getSession(), sql, expectedMessageRegExp, usePatternMatcher, false);
    }

    protected void assertQueryFails(Session session, @Language("SQL") String sql, @Language("RegExp") String expectedMessageRegExp)
    {
        QueryAssertions.assertQueryFails(queryRunner, session, sql, expectedMessageRegExp);
    }

    protected void assertQueryError(QueryRunner queryRunner, Session session, @Language("SQL") String sql, @Language("RegExp") String expectedMessageRegExp)
    {
        try {
            queryRunner.execute(session, sql);
        }
        catch (AssertionError e) {
            assertErrorMessage(sql, e, expectedMessageRegExp);
        }
    }

    protected void assertQueryError(@Language("SQL") String sql, @Language("RegExp") String expectedMessageRegExp)
    {
        assertQueryError(queryRunner, getSession(), sql, expectedMessageRegExp);
    }

    protected void assertQueryFails(Session session, @Language("SQL") String sql, @Language("RegExp") String expectedMessageRegExp, boolean usePatternMatcher)
    {
        QueryAssertions.assertQueryFails(queryRunner, session, sql, expectedMessageRegExp, usePatternMatcher, false);
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

    protected void assertExplainAnalyze(@Language("SQL") String query)
    {
        String value = (String) computeActual(query).getOnlyValue();

        assertTrue(value.matches("(?s:.*)CPU:.*, Input:.*, Output(?s:.*)"), format("Expected output to contain \"CPU:.*, Input:.*, Output\", but it is %s", value));

        // TODO: check that rendered plan is as expected, once stats are collected in a consistent way
        // assertTrue(value.contains("Cost: "), format("Expected output to contain \"Cost: \", but it is %s", value));
    }

    protected void assertCreateTableAsSelect(String table, @Language("SQL") String query, @Language("SQL") String rowCountQuery)
    {
        assertCreateTableAsSelect(getSession(), table, query, query, rowCountQuery);
    }

    protected void assertCreateTableAsSelect(String table, @Language("SQL") String query, @Language("SQL") String expectedQuery, @Language("SQL") String rowCountQuery)
    {
        assertCreateTableAsSelect(getSession(), table, query, expectedQuery, rowCountQuery);
    }

    protected void assertCreateTableAsSelect(Session session, String table, @Language("SQL") String query, @Language("SQL") String expectedQuery, @Language("SQL") String rowCountQuery)
    {
        assertUpdate(session, "CREATE TABLE " + table + " AS " + query, rowCountQuery);
        assertQuery(session, "SELECT * FROM " + table, expectedQuery);
        assertUpdate(session, "DROP TABLE " + table);

        assertFalse(getQueryRunner().tableExists(session, table));
    }

    private static void assertErrorMessage(String sql, AssertionError error, @Language("RegExp") String regex)
    {
        if (!nullToEmpty(error.getMessage()).matches(regex)) {
            fail(format("Expected error message '%s' to match '%s' for query: %s", error.getMessage(), regex, sql), error);
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
    public String getExplainPlan(String explainCommandText, String query, ExplainType.Type planType)
    {
        QueryExplainer explainer = getQueryExplainer();
        return transaction(queryRunner.getTransactionManager(), queryRunner.getAccessControl())
                .singleStatement()
                .execute(queryRunner.getDefaultSession(), session -> {
                    return explainer.getPlan(session, sqlParser.createStatement(explainCommandText.replaceAll(".", " ") + query, createParsingOptions(session)), planType, emptyList(), false, WarningCollector.NOOP, query);
                });
    }

    public String getGraphvizExplainPlan(String explainCommandText, String query, ExplainType.Type planType)
    {
        QueryExplainer explainer = getQueryExplainer();
        return transaction(queryRunner.getTransactionManager(), queryRunner.getAccessControl())
                .singleStatement()
                .execute(queryRunner.getDefaultSession(), session -> {
                    return explainer.getGraphvizPlan(session, sqlParser.createStatement(explainCommandText.replaceAll(".", " ") + query, createParsingOptions(session)), planType, emptyList(), WarningCollector.NOOP, query);
                });
    }

    public String getJsonExplainPlan(String explainCommandText, String query, ExplainType.Type planType)
    {
        QueryExplainer explainer = getQueryExplainer();
        return transaction(queryRunner.getTransactionManager(), queryRunner.getAccessControl())
                .singleStatement()
                .execute(queryRunner.getDefaultSession(), session -> {
                    return explainer.getJsonPlan(session, sqlParser.createStatement(explainCommandText.replaceAll(".", " ") + query, createParsingOptions(session)), planType, emptyList(), WarningCollector.NOOP, query);
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
                    Plan actualPlan = explainer.getLogicalPlan(transactionSession, sqlParser.createStatement(query, createParsingOptions(transactionSession)), emptyList(), WarningCollector.NOOP, query);
                    PlanAssert.assertPlan(transactionSession, queryRunner.getMetadata(), queryRunner.getStatsCalculator(), actualPlan, pattern);
                    planValidator.accept(actualPlan);
                    return null;
                });
    }

    protected Plan plan(@Language("SQL") String sql, Session session)
    {
        QueryExplainer explainer = getQueryExplainer();
        try {
            return transaction(queryRunner.getTransactionManager(), queryRunner.getAccessControl())
                    .singleStatement()
                    .execute(session, transactionSession -> {
                        return explainer.getLogicalPlan(transactionSession, sqlParser.createStatement(sql, createParsingOptions(transactionSession)), emptyList(), WarningCollector.NOOP, sql);
                    });
        }
        catch (RuntimeException e) {
            throw new AssertionError("Planning failed for SQL: " + sql, e);
        }
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
                        return explainer.getDistributedPlan(transactionSession, sqlParser.createStatement(sql, createParsingOptions(transactionSession)), emptyList(), WarningCollector.NOOP, sql);
                    });
        }
        catch (RuntimeException e) {
            throw new AssertionError("Planning failed for SQL: " + sql, e);
        }
    }

    protected QueryExplainer getQueryExplainer()
    {
        Metadata metadata = queryRunner.getMetadata();
        FeaturesConfig featuresConfig = createFeaturesConfig();
        boolean noExchange = queryRunner.getNodeCount() == 1;
        TaskCountEstimator taskCountEstimator = new TaskCountEstimator(queryRunner::getNodeCount);
        CostCalculator costCalculator = new CostCalculatorUsingExchanges(taskCountEstimator);
        List<PlanOptimizer> optimizers = new PlanOptimizers(
                metadata,
                sqlParser,
                noExchange,
                new MBeanExporter(new TestingMBeanServer()),
                queryRunner.getSplitManager(),
                queryRunner.getPlanOptimizerManager(),
                queryRunner.getPageSourceManager(),
                queryRunner.getStatsCalculator(),
                costCalculator,
                new CostCalculatorWithEstimatedExchanges(costCalculator, taskCountEstimator),
                new CostComparator(featuresConfig),
                taskCountEstimator,
                new PartitioningProviderManager(),
                featuresConfig,
                new ExpressionOptimizerManager(
                        new PluginNodeManager(new InMemoryNodeManager()),
                        queryRunner.getMetadata().getFunctionAndTypeManager()),
                new TaskManagerConfig(),
                queryRunner.getAccessControl())
                .getPlanningTimeOptimizers();
        return new QueryExplainer(
                optimizers,
                new PlanFragmenter(metadata, queryRunner.getNodePartitioningManager(), new QueryManagerConfig(), featuresConfig, queryRunner.getPlanCheckerProviderManager()),
                metadata,
                queryRunner.getAccessControl(),
                sqlParser,
                queryRunner.getStatsCalculator(),
                costCalculator,
                ImmutableMap.of(),
                new PlanChecker(featuresConfig, false, queryRunner.getPlanCheckerProviderManager()));
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

    protected DistributedQueryRunner getDistributedQueryRunner()
    {
        checkState(queryRunner != null, "queryRunner not set");
        checkState(queryRunner instanceof DistributedQueryRunner, "queryRunner is not an instance of DistributedQueryRunner");
        return (DistributedQueryRunner) queryRunner;
    }

    protected ExpectedQueryRunner getExpectedQueryRunner()
    {
        checkState(expectedQueryRunner != null, "expectedQueryRunner not set");
        return expectedQueryRunner;
    }

    protected SqlParser getSqlParser()
    {
        checkState(sqlParser != null, "sqlParser not set");
        return sqlParser;
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

    public static void dropTableIfExists(QueryRunner queryRunner, String catalogName, String schemaName, String tableName)
    {
        queryRunner.execute(format("DROP TABLE IF EXISTS %s.%s.%s", catalogName, schemaName, tableName));
    }

    protected String normalizeIdentifier(String name, String catalogName)
    {
        Metadata metadata = getQueryRunner().getMetadata();
        TransactionId txid = getQueryRunner().getTransactionManager().beginTransaction(false);
        Session session = getSession().beginTransactionId(txid, getQueryRunner().getTransactionManager(), new AllowAllAccessControl());
        try {
            return metadata.normalizeIdentifier(session, catalogName, name);
        }
        finally {
            getQueryRunner().getTransactionManager().asyncAbort(txid);
        }
    }
}
