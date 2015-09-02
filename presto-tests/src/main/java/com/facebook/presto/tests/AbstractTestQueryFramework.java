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
import com.facebook.presto.index.IndexManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.security.AccessDeniedException;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.analyzer.QueryExplainer;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.PlanOptimizersFactory;
import com.facebook.presto.sql.planner.optimizations.PlanOptimizer;
import com.facebook.presto.sql.tree.ExplainType;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.testing.TestingAccessControlManager.TestingPrivilege;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.AfterClass;

import java.util.List;

import static java.lang.String.format;
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
    private void close()
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
        QueryAssertions.assertQuery(queryRunner, session, sql, h2QueryRunner, sql, false);
    }

    public void assertQueryOrdered(@Language("SQL") String sql)
            throws Exception
    {
        QueryAssertions.assertQuery(queryRunner, getSession(), sql, h2QueryRunner, sql, true);
    }

    protected void assertQuery(@Language("SQL") String actual, @Language("SQL") String expected)
            throws Exception
    {
        QueryAssertions.assertQuery(queryRunner, getSession(), actual, h2QueryRunner, expected, false);
    }

    protected void assertQuery(Session session, @Language("SQL") String actual, @Language("SQL") String expected)
            throws Exception
    {
        QueryAssertions.assertQuery(queryRunner, session, actual, h2QueryRunner, expected, false);
    }

    protected void assertQueryOrdered(@Language("SQL") String actual, @Language("SQL") String expected)
            throws Exception
    {
        assertQueryOrdered(getSession(), actual, expected);
    }

    protected void assertQueryOrdered(Session session, @Language("SQL") String actual, @Language("SQL") String expected)
            throws Exception
    {
        QueryAssertions.assertQuery(queryRunner, session, actual, h2QueryRunner, expected, true);
    }

    protected void assertQueryTrue(@Language("SQL") String sql)
            throws Exception
    {
        assertQuery(sql, "SELECT true");
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
            assertExceptionMessage(e, exceptionsMessageRegExp);
        }
        finally {
            queryRunner.getAccessControl().reset();
            queryRunner.getExclusiveLock().unlock();
        }
    }

    private static void assertExceptionMessage(Exception exception, @Language("RegExp") String exceptionMessagePattern)
    {
        String regex = ".*Access Denied: " + exceptionMessagePattern;
        if (!exception.getMessage().matches(regex)) {
            fail(format("Expected exception message '%s' to match '%s'", exception.getMessage(), regex));
        }
    }

    protected MaterializedResult computeExpected(@Language("SQL") String sql, List<? extends Type> resultTypes)
    {
        return h2QueryRunner.execute(getSession(), sql, resultTypes);
    }

    public Function<MaterializedRow, String> onlyColumnGetter()
    {
        return new Function<MaterializedRow, String>()
        {
            @Override
            public String apply(MaterializedRow input)
            {
                assertEquals(input.getFieldCount(), 1);
                return (String) input.getField(0);
            }
        };
    }

    public String getExplainPlan(String query, ExplainType.Type planType)
    {
        QueryExplainer explainer = getQueryExplainer();
        return explainer.getPlan(queryRunner.getDefaultSession(), sqlParser.createStatement(query), planType);
    }

    public String getGraphvizExplainPlan(String query, ExplainType.Type planType)
    {
        QueryExplainer explainer = getQueryExplainer();
        return explainer.getGraphvizPlan(queryRunner.getDefaultSession(), sqlParser.createStatement(query), planType);
    }

    private QueryExplainer getQueryExplainer()
    {
        Metadata metadata = queryRunner.getMetadata();
        FeaturesConfig featuresConfig = new FeaturesConfig().setExperimentalSyntaxEnabled(true).setOptimizeHashGeneration(true);
        boolean forceSingleNode = queryRunner.getNodeCount() == 1;
        List<PlanOptimizer> optimizers = new PlanOptimizersFactory(metadata, sqlParser, new IndexManager(), featuresConfig, forceSingleNode).get();
        return new QueryExplainer(
                optimizers,
                metadata,
                queryRunner.getAccessControl(),
                sqlParser,
                ImmutableMap.of(),
                featuresConfig.isExperimentalSyntaxEnabled());
    }
}
