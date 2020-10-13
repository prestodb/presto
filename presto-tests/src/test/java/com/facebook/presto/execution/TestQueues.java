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
package com.facebook.presto.execution;

import com.facebook.airlift.http.client.HttpClient;
import com.facebook.airlift.http.client.Request;
import com.facebook.airlift.http.client.jetty.JettyHttpClient;
import com.facebook.airlift.json.ObjectMapperProvider;
import com.facebook.presto.Session;
import com.facebook.presto.dispatcher.DispatchManager;
import com.facebook.presto.resourceGroups.ResourceGroupManagerPlugin;
import com.facebook.presto.server.BasicQueryInfo;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.spi.resourceGroups.ResourceGroupId;
import com.facebook.presto.spi.session.ResourceEstimates;
import com.facebook.presto.sql.Serialization;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.facebook.presto.tests.tpch.TpchQueryRunnerBuilder;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.ws.rs.core.Response.Status;

import java.io.IOException;
import java.util.Optional;
import java.util.Set;

import static com.facebook.airlift.http.client.Request.Builder.prepareGet;
import static com.facebook.airlift.http.client.Request.Builder.preparePut;
import static com.facebook.airlift.http.client.StringResponseHandler.createStringResponseHandler;
import static com.facebook.airlift.testing.Closeables.closeQuietly;
import static com.facebook.presto.SystemSessionProperties.HASH_PARTITION_COUNT;
import static com.facebook.presto.execution.QueryState.FAILED;
import static com.facebook.presto.execution.QueryState.FINISHED;
import static com.facebook.presto.execution.QueryState.QUEUED;
import static com.facebook.presto.execution.QueryState.RUNNING;
import static com.facebook.presto.execution.TestQueryRunnerUtil.cancelQuery;
import static com.facebook.presto.execution.TestQueryRunnerUtil.createQuery;
import static com.facebook.presto.execution.TestQueryRunnerUtil.createQueryRunner;
import static com.facebook.presto.execution.TestQueryRunnerUtil.waitForQueryState;
import static com.facebook.presto.metadata.FunctionAndTypeManager.createTestFunctionAndTypeManager;
import static com.facebook.presto.spi.StandardErrorCode.ADMINISTRATIVELY_KILLED;
import static com.facebook.presto.spi.StandardErrorCode.ADMINISTRATIVELY_PREEMPTED;
import static com.facebook.presto.spi.StandardErrorCode.QUERY_REJECTED;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;
import static javax.ws.rs.core.Response.Status.fromStatusCode;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

// run single threaded to avoid creating multiple query runners at once
@Test(singleThreaded = true)
public class TestQueues
{
    public static final String LONG_LASTING_QUERY = "SELECT COUNT(*) FROM lineitem";

    private DistributedQueryRunner queryRunner;
    private ObjectMapper objectMapper;
    private HttpClient client;

    @BeforeMethod
    public void setup()
            throws Exception
    {
        queryRunner = createQueryRunner();
        client = new JettyHttpClient();
        objectMapper = new ObjectMapperProvider().get();
        objectMapper.registerModule(new SimpleModule() {
            {
                addKeyDeserializer(VariableReferenceExpression.class, new Serialization.VariableReferenceExpressionDeserializer(createTestFunctionAndTypeManager()));
            }
        });
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown()
    {
        closeQuietly(client);
        closeQuietly(queryRunner);
        queryRunner = null;
    }

    @Test(timeOut = 240_000)
    public void testResourceGroupManager()
            throws Exception
    {
        queryRunner.installPlugin(new ResourceGroupManagerPlugin());
        queryRunner.getCoordinator().getResourceGroupManager().get().setConfigurationManager("file", ImmutableMap.of("resource-groups.config-file", getResourceFilePath("resource_groups_config_dashboard.json")));

        // submit first "dashboard" query
        QueryId firstDashboardQuery = createDashboardQuery(queryRunner);

        // wait for the first "dashboard" query to start
        waitForQueryState(queryRunner, firstDashboardQuery, RUNNING);

        // submit second "dashboard" query
        QueryId secondDashboardQuery = createDashboardQuery(queryRunner);

        // wait for the second "dashboard" query to be queued ("dashboard.${USER}" queue strategy only allows one "dashboard" query to be accepted for execution)
        waitForQueryState(queryRunner, secondDashboardQuery, QUEUED);

        // submit first non "dashboard" query
        QueryId firstNonDashboardQuery = createAdHocQuery(queryRunner);

        // wait for the first non "dashboard" query to start
        waitForQueryState(queryRunner, firstNonDashboardQuery, RUNNING);

        // submit second non "dashboard" query
        QueryId secondNonDashboardQuery = createAdHocQuery(queryRunner);

        // wait for the second non "dashboard" query to start
        waitForQueryState(queryRunner, secondNonDashboardQuery, RUNNING);

        // cancel first "dashboard" query, second "dashboard" query and second non "dashboard" query should start running
        cancelQuery(queryRunner, firstDashboardQuery);
        waitForQueryState(queryRunner, firstDashboardQuery, FAILED);
        waitForQueryState(queryRunner, secondDashboardQuery, RUNNING);
    }

    @Test(timeOut = 240_000)
    public void testExceedSoftLimits()
            throws Exception
    {
        queryRunner.installPlugin(new ResourceGroupManagerPlugin());
        queryRunner.getCoordinator().getResourceGroupManager().get().setConfigurationManager("file", ImmutableMap.of("resource-groups.config-file", getResourceFilePath("resource_groups_config_soft_limits.json")));

        QueryId scheduled1 = createScheduledQuery(queryRunner);
        waitForQueryState(queryRunner, scheduled1, RUNNING);

        QueryId scheduled2 = createScheduledQuery(queryRunner);
        waitForQueryState(queryRunner, scheduled2, RUNNING);

        QueryId scheduled3 = createScheduledQuery(queryRunner);
        waitForQueryState(queryRunner, scheduled3, RUNNING);

        // cluster is now 'at capacity' - scheduled is running 3 (i.e. over soft limit)

        QueryId backfill1 = createBackfill(queryRunner);
        QueryId scheduled4 = createScheduledQuery(queryRunner);

        cancelQuery(queryRunner, scheduled1);

        // backfill should be chosen to run next
        waitForQueryState(queryRunner, backfill1, RUNNING);

        cancelQuery(queryRunner, scheduled2);
        cancelQuery(queryRunner, scheduled3);
        cancelQuery(queryRunner, scheduled4);

        QueryId backfill2 = createBackfill(queryRunner);
        waitForQueryState(queryRunner, backfill2, RUNNING);

        QueryId backfill3 = createBackfill(queryRunner);
        waitForQueryState(queryRunner, backfill3, RUNNING);

        // cluster is now 'at capacity' - backfills is running 3 (i.e. over soft limit)

        QueryId backfill4 = createBackfill(queryRunner);
        QueryId scheduled5 = createScheduledQuery(queryRunner);
        cancelQuery(queryRunner, backfill1);

        // scheduled should be chosen to run next
        waitForQueryState(queryRunner, scheduled5, RUNNING);
        cancelQuery(queryRunner, backfill2);
        cancelQuery(queryRunner, backfill3);
        cancelQuery(queryRunner, backfill4);
        cancelQuery(queryRunner, scheduled5);

        waitForQueryState(queryRunner, scheduled5, FAILED);
    }

    private QueryId createBackfill(DistributedQueryRunner queryRunner)
    {
        return createQuery(queryRunner, newSession("backfill", ImmutableSet.of(), null), LONG_LASTING_QUERY);
    }

    private QueryId createScheduledQuery(DistributedQueryRunner queryRunner)
    {
        return createQuery(queryRunner, newSession("scheduled", ImmutableSet.of(), null), LONG_LASTING_QUERY);
    }

    @Test(timeOut = 240_000)
    public void testResourceGroupManagerWithTwoDashboardQueriesRequestedAtTheSameTime()
            throws Exception
    {
        queryRunner.installPlugin(new ResourceGroupManagerPlugin());
        queryRunner.getCoordinator().getResourceGroupManager().get().setConfigurationManager("file", ImmutableMap.of("resource-groups.config-file", getResourceFilePath("resource_groups_config_dashboard.json")));

        QueryId firstDashboardQuery = createDashboardQuery(queryRunner);
        QueryId secondDashboardQuery = createDashboardQuery(queryRunner);

        ImmutableSet<QueryState> queuedOrRunning = ImmutableSet.of(QUEUED, RUNNING);
        waitForQueryState(queryRunner, firstDashboardQuery, queuedOrRunning);
        waitForQueryState(queryRunner, secondDashboardQuery, queuedOrRunning);
    }

    @Test(timeOut = 240_000)
    public void testResourceGroupManagerWithTooManyQueriesScheduled()
            throws Exception
    {
        queryRunner.installPlugin(new ResourceGroupManagerPlugin());
        queryRunner.getCoordinator().getResourceGroupManager().get().setConfigurationManager("file", ImmutableMap.of("resource-groups.config-file", getResourceFilePath("resource_groups_config_dashboard.json")));

        QueryId firstDashboardQuery = createDashboardQuery(queryRunner);
        waitForQueryState(queryRunner, firstDashboardQuery, RUNNING);

        QueryId secondDashboardQuery = createDashboardQuery(queryRunner);
        waitForQueryState(queryRunner, secondDashboardQuery, QUEUED);

        QueryId thirdDashboardQuery = createDashboardQuery(queryRunner);
        waitForQueryState(queryRunner, thirdDashboardQuery, FAILED);
    }

    @Test(timeOut = 240_000)
    public void testResourceGroupManagerRejection()
            throws Exception
    {
        testRejection();
    }

    @Test(timeOut = 240_000)
    public void testClientTagsBasedSelection()
            throws Exception
    {
        queryRunner.installPlugin(new ResourceGroupManagerPlugin());
        queryRunner.getCoordinator().getResourceGroupManager().get()
                .setConfigurationManager("file", ImmutableMap.of("resource-groups.config-file", getResourceFilePath("resource_groups_client_tags_based_config.json")));
        assertResourceGroup(queryRunner, newSessionWithTags(ImmutableSet.of("a")), LONG_LASTING_QUERY, createResourceGroupId("global", "a", "default"));
        assertResourceGroup(queryRunner, newSessionWithTags(ImmutableSet.of("b")), LONG_LASTING_QUERY, createResourceGroupId("global", "b"));
        assertResourceGroup(queryRunner, newSessionWithTags(ImmutableSet.of("a", "c")), LONG_LASTING_QUERY, createResourceGroupId("global", "a", "c"));
    }

    @Test(timeOut = 240_000)
    public void testSelectorResourceEstimateBasedSelection()
            throws Exception
    {
        queryRunner.installPlugin(new ResourceGroupManagerPlugin());
        queryRunner.getCoordinator().getResourceGroupManager().get()
                .setConfigurationManager("file", ImmutableMap.of("resource-groups.config-file", getResourceFilePath("resource_groups_resource_estimate_based_config.json")));

        assertResourceGroup(
                queryRunner,
                newSessionWithResourceEstimates(new ResourceEstimates(
                        Optional.of(Duration.valueOf("4m")),
                        Optional.empty(),
                        Optional.of(DataSize.valueOf("400MB")),
                        Optional.empty())),
                LONG_LASTING_QUERY,
                createResourceGroupId("global", "small"));

        assertResourceGroup(
                queryRunner,
                newSessionWithResourceEstimates(new ResourceEstimates(
                        Optional.of(Duration.valueOf("4m")),
                        Optional.empty(),
                        Optional.of(DataSize.valueOf("600MB")),
                        Optional.empty())),
                LONG_LASTING_QUERY,
                createResourceGroupId("global", "other"));

        assertResourceGroup(
                queryRunner,
                newSessionWithResourceEstimates(new ResourceEstimates(
                        Optional.of(Duration.valueOf("4m")),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty())),
                LONG_LASTING_QUERY,
                createResourceGroupId("global", "other"));

        assertResourceGroup(
                queryRunner,
                newSessionWithResourceEstimates(new ResourceEstimates(
                        Optional.of(Duration.valueOf("1s")),
                        Optional.of(Duration.valueOf("1s")),
                        Optional.of(DataSize.valueOf("6TB")),
                        Optional.empty())),
                LONG_LASTING_QUERY,
                createResourceGroupId("global", "huge_memory"));

        assertResourceGroup(
                queryRunner,
                newSessionWithResourceEstimates(new ResourceEstimates(
                        Optional.of(Duration.valueOf("100h")),
                        Optional.empty(),
                        Optional.of(DataSize.valueOf("4TB")),
                        Optional.empty())),
                LONG_LASTING_QUERY,
                createResourceGroupId("global", "other"));
    }

    @Test(timeOut = 240_000)
    public void testQueryTypeBasedSelection()
            throws Exception
    {
        try (DistributedQueryRunner queryRunner = TpchQueryRunnerBuilder.builder().build()) {
            queryRunner.installPlugin(new ResourceGroupManagerPlugin());
            queryRunner.getCoordinator().getResourceGroupManager().get()
                    .setConfigurationManager("file", ImmutableMap.of("resource-groups.config-file", getResourceFilePath("resource_groups_query_type_based_config.json")));
            assertResourceGroup(queryRunner, newAdhocSession(), LONG_LASTING_QUERY, createResourceGroupId("global", "select"));
            assertResourceGroup(queryRunner, newAdhocSession(), "SHOW TABLES", createResourceGroupId("global", "describe"));
            assertResourceGroup(queryRunner, newAdhocSession(), "EXPLAIN " + LONG_LASTING_QUERY, createResourceGroupId("global", "explain"));
            assertResourceGroup(queryRunner, newAdhocSession(), "DESCRIBE lineitem", createResourceGroupId("global", "describe"));
            assertResourceGroup(queryRunner, newAdhocSession(), "RESET SESSION " + HASH_PARTITION_COUNT, createResourceGroupId("global", "data_definition"));
        }
    }

    @Test(timeOut = 240_000)
    public void testQueuedQueryInteraction()
            throws Exception
    {
        queryRunner.installPlugin(new ResourceGroupManagerPlugin());
        queryRunner.getCoordinator().getResourceGroupManager().get().setConfigurationManager("file", ImmutableMap.of("resource-groups.config-file", getResourceFilePath("resource_groups_config_dashboard.json")));

        // submit first "dashboard" query
        QueryId firstDashboardQuery = createDashboardQuery(queryRunner);

        // wait for the first "dashboard" query to start
        waitForQueryState(queryRunner, firstDashboardQuery, RUNNING);

        // submit second "dashboard" query
        QueryId secondDashboardQuery = createDashboardQuery(queryRunner);

        // wait for the second "dashboard" query to be queued ("dashboard.${USER}" queue strategy only allows one "dashboard" query to be accepted for execution)
        waitForQueryState(queryRunner, secondDashboardQuery, QUEUED);

        // Retrieve information for the queued query
        BasicQueryInfo queryInfo = getBasicQueryInfo("/v1/query/" + secondDashboardQuery.getId());
        assertNotNull(queryInfo);
        assertEquals(queryInfo.getState(), QUEUED);
        assertEquals(queryInfo.getQuery(), LONG_LASTING_QUERY);
        assertNotNull(queryInfo.getQueryStats());

        killQuery(format("/v1/query/%s/killed", secondDashboardQuery.getId()));
        queryInfo = getBasicQueryInfo("/v1/query/" + secondDashboardQuery.getId());
        assertNotNull(queryInfo);
        assertEquals(queryInfo.getErrorCode(), ADMINISTRATIVELY_KILLED.toErrorCode());

        // submit third "dashboard" query
        QueryId thirdDashboardQuery = createDashboardQuery(queryRunner);
        waitForQueryState(queryRunner, thirdDashboardQuery, QUEUED);

        killQuery(format("/v1/query/%s/preempted", thirdDashboardQuery.getId()));
        queryInfo = getBasicQueryInfo("/v1/query/" + thirdDashboardQuery.getId());
        assertNotNull(queryInfo);
        assertEquals(queryInfo.getErrorCode(), ADMINISTRATIVELY_PREEMPTED.toErrorCode());
    }

    private void assertResourceGroup(DistributedQueryRunner queryRunner, Session session, String query, ResourceGroupId expectedResourceGroup)
            throws InterruptedException
    {
        QueryId queryId = createQuery(queryRunner, session, query);
        waitForQueryState(queryRunner, queryId, ImmutableSet.of(RUNNING, FINISHED));
        Optional<ResourceGroupId> resourceGroupId = queryRunner.getCoordinator().getQueryManager().getFullQueryInfo(queryId).getResourceGroupId();
        assertTrue(resourceGroupId.isPresent(), "Query should have a resource group");
        assertEquals(resourceGroupId.get(), expectedResourceGroup, format("Expected: '%s' resource group, found: %s", expectedResourceGroup, resourceGroupId.get()));
    }

    private void testRejection()
            throws Exception
    {
        try (DistributedQueryRunner queryRunner = createQueryRunner()) {
            queryRunner.installPlugin(new ResourceGroupManagerPlugin());
            queryRunner.getCoordinator().getResourceGroupManager().get().setConfigurationManager("file", ImmutableMap.of("resource-groups.config-file", getResourceFilePath("resource_groups_config_dashboard.json")));

            QueryId queryId = createQuery(queryRunner, newRejectionSession(), LONG_LASTING_QUERY);
            waitForQueryState(queryRunner, queryId, FAILED);
            DispatchManager dispatchManager = queryRunner.getCoordinator().getDispatchManager();
            assertEquals(dispatchManager.getQueryInfo(queryId).getErrorCode(), QUERY_REJECTED.toErrorCode());
        }
    }

    private String getResourceFilePath(String fileName)
    {
        return this.getClass().getClassLoader().getResource(fileName).getPath();
    }

    private QueryId createDashboardQuery(DistributedQueryRunner queryRunner)
    {
        return createQuery(queryRunner, newSession("dashboard", ImmutableSet.of(), null), LONG_LASTING_QUERY);
    }

    private QueryId createAdHocQuery(DistributedQueryRunner queryRunner)
    {
        return createQuery(queryRunner, newAdhocSession(), LONG_LASTING_QUERY);
    }

    private static Session newAdhocSession()
    {
        return newSession("adhoc", ImmutableSet.of(), null);
    }

    private static Session newRejectionSession()
    {
        return newSession("reject", ImmutableSet.of(), null);
    }

    private static Session newSessionWithTags(Set<String> clientTags)
    {
        return newSession("sessionWithTags", clientTags, null);
    }

    private static Session newSessionWithResourceEstimates(ResourceEstimates resourceEstimates)
    {
        return newSession("sessionWithTags", ImmutableSet.of(), resourceEstimates);
    }

    public static Session newSession(String source, Set<String> clientTags, ResourceEstimates resourceEstimates)
    {
        return testSessionBuilder()
                .setCatalog("tpch")
                .setSchema("sf100000")
                .setSource(source)
                .setClientTags(clientTags)
                .setResourceEstimates(resourceEstimates)
                .build();
    }

    public static ResourceGroupId createResourceGroupId(String root, String... subGroups)
    {
        return new ResourceGroupId(ImmutableList.<String>builder()
                .add(requireNonNull(root, "root is null"))
                .addAll(asList(subGroups))
                .build());
    }

    private BasicQueryInfo getBasicQueryInfo(String path)
            throws IOException
    {
        requireNonNull(path, "path is null");
        Request request = prepareGet().setUri(queryRunner.getCoordinator().resolve(path)).build();
        String body = client.execute(request, createStringResponseHandler()).getBody();
        return objectMapper.readValue(body, BasicQueryInfo.class);
    }

    private void killQuery(String path)
    {
        requireNonNull(path, "path is null");
        Request request = preparePut().setUri(queryRunner.getCoordinator().resolve(path)).build();
        Status status = fromStatusCode(client.execute(request, createStringResponseHandler()).getStatusCode());
        assertEquals(Status.OK, status);
    }
}
