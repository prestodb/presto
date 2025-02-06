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
import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.plugin.rewriter.RewriteToFixedQueryRewriterPlugin;
import com.facebook.presto.resourceGroups.FileResourceGroupConfigurationManagerFactory;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.security.AccessDeniedException;
import com.facebook.presto.tests.tpch.TpchQueryRunnerBuilder;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import static com.facebook.presto.SystemSessionProperties.IS_QUERY_REWRITER_PLUGIN_ENABLED;
import static com.facebook.presto.SystemSessionProperties.IS_QUERY_REWRITER_PLUGIN_SUCCEEDED;
import static com.facebook.presto.execution.QueryState.FAILED;
import static com.facebook.presto.execution.QueryState.FINISHED;
import static com.facebook.presto.execution.QueryState.RUNNING;
import static com.facebook.presto.execution.TestQueryRunnerUtil.createQuery;
import static com.facebook.presto.execution.TestQueryRunnerUtil.waitForQueryState;
import static com.facebook.presto.testing.TestingAccessControlManager.TestingPrivilegeType.SELECT_COLUMN;
import static com.facebook.presto.testing.TestingAccessControlManager.privilege;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.tests.QueryAssertions.assertQuerySucceeds;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static com.facebook.presto.utils.ResourceUtils.getResourceFilePath;
import static com.google.common.base.Strings.nullToEmpty;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

@Test(singleThreaded = true)
public class TestExternalQueryRewriterAccess
{
    private static final String TPCH = "tpch";
    private DistributedQueryRunner queryRunner;

    private static final Session TEST_SESSION = testSessionBuilder()
            .setCatalog(TPCH)
            .setSchema(TINY_SCHEMA_NAME)
            .setSystemProperty("is_query_rewriter_plugin_succeeded", "true")
            .build();

    // Setup a new query runner for each test
    private DistributedQueryRunner createQueryRunner() throws Exception
    {
        DistributedQueryRunner queryRunner = TpchQueryRunnerBuilder.builder().build();
        queryRunner.getCoordinator().getResourceGroupManager().get().addConfigurationManagerFactory(new FileResourceGroupConfigurationManagerFactory());
        queryRunner.getCoordinator().getResourceGroupManager().get()
                .forceSetConfigurationManager("file", ImmutableMap.of("resource-groups.config-file", getResourceFilePath("resource_groups_config_simple.json")));
        return queryRunner;
    }

    private Session createSession()
    {
        return testSessionBuilder()
                .setCatalog(TPCH)
                .setSchema(TINY_SCHEMA_NAME)
                .setSystemProperty(IS_QUERY_REWRITER_PLUGIN_ENABLED, "true")
                .build();
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown()
    {
        if (queryRunner != null) {
            queryRunner.close();
            queryRunner = null;
        }
    }

    private void installQueryRewriter(String rewrittenQuery) throws InterruptedException
    {
        // Create and install a new plugin
        queryRunner.installPlugin(new RewriteToFixedQueryRewriterPlugin(rewrittenQuery));
        queryRunner.getCoordinator().getQueryRewriterManager().loadQueryRewriterProvider(ImmutableMap.of("query-rewriter.name", "test"));
        queryRunner.waitForClusterToGetReady();
    }

    @Test
    public void testQueryRewriteWithAccess()
            throws Exception
    {
        // Create a fresh query runner for this test
        queryRunner = createQueryRunner();
        Session session = createSession();

        // User inputs a query against customer table but it gets rewritten to use region table
        String originalQuery = "SELECT COUNT(*) FROM customer";
        String rewrittenQuery = "SELECT COUNT(*) FROM region";

        // Install the query rewriter plugin that will rewrite all queries to the fixed query
        installQueryRewriter(rewrittenQuery);

        // User has access to both tables
        QueryId queryId = createQuery(queryRunner, session, originalQuery);
        waitForQueryState(queryRunner, 0, queryId, ImmutableSet.of(RUNNING, FINISHED));

        // Verify the query was rewritten
        QueryInfo queryInfo = queryRunner.getQueryInfo(queryId);
        assertEquals(queryInfo.getQuery(), rewrittenQuery);
        assertEquals(queryInfo.getSession().getSystemProperties().getOrDefault(IS_QUERY_REWRITER_PLUGIN_SUCCEEDED, "false"), "true");
    }

    @Test
    public void testPositiveScenario()
            throws Exception
    {
        // Create a fresh query runner for this test
        queryRunner = createQueryRunner();
        Session session = createSession();

        // Original query on customer table that user has access to
        String originalQuery = "SELECT COUNT(*) FROM customer";
        // Query gets rewritten to use region table that user DOESN'T have access to
        String rewrittenQuery = "SELECT COUNT(*) FROM region";

        // Install the query rewriter plugin that will rewrite all queries to the fixed query
        installQueryRewriter(rewrittenQuery);

        // Deny access to region table
        queryRunner.getAccessControl().deny(privilege("region", SELECT_COLUMN));

        // Running this should succeed because access control checks
        // should be performed on the original query, not the rewritten one
        QueryId queryId = createQuery(queryRunner, session, originalQuery);
        waitForQueryState(queryRunner, 0, queryId, ImmutableSet.of(RUNNING, FINISHED));

        // Verify the query was rewritten
        QueryInfo queryInfo = queryRunner.getQueryInfo(queryId);
        assertEquals(queryInfo.getQuery(), rewrittenQuery);
        assertEquals(queryInfo.getSession().getSystemProperties().getOrDefault(IS_QUERY_REWRITER_PLUGIN_SUCCEEDED, "false"), "true");
    }

    @Test
    public void testNegativeScenario()
            throws Exception
    {
        // Create a fresh query runner for this test
        queryRunner = createQueryRunner();
        Session session = createSession();
        // Deny access to customer table (the original query table)
        // Original query on customer table that user DOESN'T have access to
        String originalQuery = "SELECT COUNT(*) FROM customer";
        // Query gets rewritten to use region table that user has access to
        String rewrittenQuery = "SELECT COUNT(*) FROM region";

        // Install the query rewriter plugin that will rewrite all queries to the fixed query
        installQueryRewriter(rewrittenQuery);

        queryRunner.getAccessControl().deny(privilege("customer", SELECT_COLUMN));

        try {
            // Running this should fail because access control checks
            // should be performed on the original query, which the user doesn't have access to
            QueryId queryId = createQuery(queryRunner, session, originalQuery);
            waitForQueryState(queryRunner, 0, queryId, ImmutableSet.of(FAILED));
        }
        catch (RuntimeException e) {
            assertExceptionMessage(originalQuery, e, ".*Access Denied: Cannot select from columns .* in table .*customer.*");
        }
    }

    @Test
    public void testMqtQueryAccessDeniedOnOriginalQueryAfterRewriteAttempt()
            throws Exception
    {
        queryRunner = createQueryRunner();
        String actualQuery = "SELECT COUNT(*) FROM customer";
        String rewrittenQuery = "SELECT COUNT(*) FROM orders"; // This might not be directly relevant for the assertion
        String tableName = "customer"; // Deny access to the original table
        try {
            queryRunner.getAccessControl().deny(privilege(tableName, SELECT_COLUMN));
            queryRunner.execute(TEST_SESSION, actualQuery);
            fail("Expected " + AccessDeniedException.class.getSimpleName());
        }
        catch (RuntimeException e) {
            assertExceptionMessage(actualQuery, e, ".*Access Denied: Cannot select from columns \\[.*\\] in table .*customer.*");
        }
        finally {
            queryRunner.getAccessControl().reset();
        }
    }

    @Test
    public void testMqtQueryRewritePluginFails()
            throws Exception
    {
        queryRunner = createQueryRunner();
        Session sessionWithPluginFailure = testSessionBuilder()
                .setCatalog(TPCH)
                .setSchema(TINY_SCHEMA_NAME)
                .setSystemProperty("is_query_rewriter_plugin_succeeded", "false")
                .build();
        String actualQuery = "SELECT COUNT(*) FROM part";
        // Expect the original query to be executed without rewriting
        assertQuerySucceeds(queryRunner, sessionWithPluginFailure, actualQuery);
    }

    private static void assertExceptionMessage(String sql, Exception exception, @Language("RegExp") String regex)
    {
        if (!nullToEmpty(exception.getMessage()).matches(regex)) {
            fail(format("Expected exception message '%s' to match '%s' for query: %s", exception.getMessage(), regex, sql), exception);
        }
    }
}
