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
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.security.AccessDeniedException;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.analyzer.QueryExplainer;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.PlanOptimizers;
import com.facebook.presto.sql.planner.optimizations.PlanOptimizer;
import com.facebook.presto.sql.tree.ExplainType;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.testing.TestingAccessControlManager.TestingPrivilege;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.AfterClass;

import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import static com.facebook.presto.sql.SqlFormatter.formatSql;
import static com.facebook.presto.transaction.TransactionBuilder.transaction;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public abstract class AbstractTestQueryFramework
{
    protected final H2QueryRunner h2QueryRunner;
    protected final QueryRunner queryRunner;
    private final SqlParser sqlParser;

    protected AbstractTestQueryFramework(QueryRunner queryRunner)
    {
        this.queryRunner = queryRunner;
        h2QueryRunner = new H2QueryRunner();
        sqlParser = new SqlParser();
    }

    @AfterClass(alwaysRun = true)
    public void close()
            throws Exception
    {
        try {
            h2QueryRunner.close();
        }
        finally {
            queryRunner.close();
        }
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
        return queryRunner.execute(session, sql).toJdbcTypes();
    }

    protected void assertQuery(@Language("SQL") String sql)
            throws Exception
    {
        assertQuery(getSession(), sql);
    }

    protected void assertQuery(Session session, @Language("SQL") String sql)
            throws Exception
    {
        QueryAssertions.assertQuery(queryRunner, session, sql, h2QueryRunner, sql, false, false);
    }

    public void assertQueryOrdered(@Language("SQL") String sql)
            throws Exception
    {
        QueryAssertions.assertQuery(queryRunner, getSession(), sql, h2QueryRunner, sql, true, false);
    }

    protected void assertQuery(@Language("SQL") String actual, @Language("SQL") String expected)
            throws Exception
    {
        QueryAssertions.assertQuery(queryRunner, getSession(), actual, h2QueryRunner, expected, false, false);
    }

    protected void assertQuery(Session session, @Language("SQL") String actual, @Language("SQL") String expected)
            throws Exception
    {
        QueryAssertions.assertQuery(queryRunner, session, actual, h2QueryRunner, expected, false, false);
    }

    protected void assertQueryOrdered(@Language("SQL") String actual, @Language("SQL") String expected)
            throws Exception
    {
        assertQueryOrdered(getSession(), actual, expected);
    }

    protected void assertQueryOrdered(Session session, @Language("SQL") String actual, @Language("SQL") String expected)
            throws Exception
    {
        QueryAssertions.assertQuery(queryRunner, session, actual, h2QueryRunner, expected, true, false);
    }

    protected void assertUpdate(@Language("SQL") String actual, @Language("SQL") String expected)
            throws Exception
    {
        assertUpdate(getSession(), actual, expected);
    }

    protected void assertUpdate(Session session, @Language("SQL") String actual, @Language("SQL") String expected)
            throws Exception
    {
        QueryAssertions.assertQuery(queryRunner, session, actual, h2QueryRunner, expected, false, true);
    }

    protected void assertUpdate(@Language("SQL") String sql)
    {
        assertUpdate(getSession(), sql);
    }

    protected void assertUpdate(Session session, @Language("SQL") String sql)
    {
        QueryAssertions.assertUpdate(queryRunner, session, sql, OptionalLong.empty());
    }

    protected void assertUpdate(@Language("SQL") String sql, long count)
    {
        assertUpdate(getSession(), sql, count);
    }

    protected void assertUpdate(Session session, @Language("SQL") String sql, long count)
    {
        QueryAssertions.assertUpdate(queryRunner, session, sql, OptionalLong.of(count));
    }

    protected void assertQueryFails(@Language("SQL") String sql, @Language("RegExp") String expectedMessageRegExp)
    {
        assertQueryFails(getSession(), sql, expectedMessageRegExp);
    }

    protected void assertQueryFails(Session session, @Language("SQL") String sql, @Language("RegExp") String expectedMessageRegExp)
    {
        queryRunner.getExclusiveLock().lock();
        try {
            queryRunner.execute(session, sql);
            fail(format("Expected query to fail: %s", sql));
        }
        catch (RuntimeException ex) {
            assertExceptionMessage(sql, ex, expectedMessageRegExp);
        }
        finally {
            queryRunner.getExclusiveLock().unlock();
        }
    }

    public void assertApproximateQuery(Session session, @Language("SQL") String actual, @Language("SQL") String expected)
            throws Exception
    {
        QueryAssertions.assertApproximateQuery(queryRunner,
                session,
                actual,
                h2QueryRunner,
                expected);
    }

    protected void assertAccessAllowed(@Language("SQL") String sql, TestingPrivilege... deniedPrivileges)
            throws Exception
    {
        assertAccessAllowed(getSession(), sql, deniedPrivileges);
    }

    protected void assertAccessAllowed(Session session, @Language("SQL") String sql, TestingPrivilege... deniedPrivileges)
            throws Exception
    {
        queryRunner.getExclusiveLock().lock();
        try {
            queryRunner.getAccessControl().deny(deniedPrivileges);
            queryRunner.execute(session, sql);
        }
        finally {
            queryRunner.getAccessControl().reset();
            queryRunner.getExclusiveLock().unlock();
        }
    }

    protected void assertAccessDenied(@Language("SQL") String sql, @Language("RegExp") String exceptionsMessageRegExp, TestingPrivilege... deniedPrivileges)
            throws Exception
    {
        assertAccessDenied(getSession(), sql, exceptionsMessageRegExp, deniedPrivileges);
    }

    protected void assertAccessDenied(
            Session session,
            @Language("SQL") String sql,
            @Language("RegExp") String exceptionsMessageRegExp,
            TestingPrivilege... deniedPrivileges)
            throws Exception
    {
        queryRunner.getExclusiveLock().lock();
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
            queryRunner.getExclusiveLock().unlock();
        }
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
        if (!exception.getMessage().matches(regex)) {
            fail(format("Expected exception message '%s' to match '%s' for query: %s", exception.getMessage(), regex, sql));
        }
    }

    protected MaterializedResult computeExpected(@Language("SQL") String sql, List<? extends Type> resultTypes)
    {
        return h2QueryRunner.execute(getSession(), sql, resultTypes);
    }

    protected String formatSqlText(String sql)
    {
        return formatSql(sqlParser.createStatement(sql), Optional.empty());
    }

    public String getExplainPlan(String query, ExplainType.Type planType)
    {
        QueryExplainer explainer = getQueryExplainer();
        return transaction(queryRunner.getTransactionManager())
                .singleStatement()
                .execute(queryRunner.getDefaultSession(), session -> {
                    return explainer.getPlan(session, sqlParser.createStatement(query), planType, emptyList());
                });
    }

    public String getGraphvizExplainPlan(String query, ExplainType.Type planType)
    {
        QueryExplainer explainer = getQueryExplainer();
        return transaction(queryRunner.getTransactionManager())
                .singleStatement()
                .execute(queryRunner.getDefaultSession(), session -> {
                    return explainer.getGraphvizPlan(session, sqlParser.createStatement(query), planType, emptyList());
                });
    }

    private QueryExplainer getQueryExplainer()
    {
        Metadata metadata = queryRunner.getMetadata();
        FeaturesConfig featuresConfig = new FeaturesConfig().setExperimentalSyntaxEnabled(true).setOptimizeHashGeneration(true);
        boolean forceSingleNode = queryRunner.getNodeCount() == 1;
        List<PlanOptimizer> optimizers = new PlanOptimizers(metadata, sqlParser, featuresConfig, forceSingleNode).get();
        return new QueryExplainer(
                optimizers,
                metadata,
                queryRunner.getAccessControl(),
                sqlParser,
                ImmutableMap.of(),
                featuresConfig.isExperimentalSyntaxEnabled());
    }
}
