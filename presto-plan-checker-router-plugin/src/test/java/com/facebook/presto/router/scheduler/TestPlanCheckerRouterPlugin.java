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
package com.facebook.presto.router.scheduler;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.log.Logging;
import com.facebook.presto.client.QueryError;
import com.facebook.presto.client.QueryResults;
import com.facebook.presto.common.ErrorType;
import com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils;
import com.facebook.presto.server.MockHttpServletRequest;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.router.RouterRequestInfo;
import com.facebook.presto.spi.router.Scheduler;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ListMultimap;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.Optional;

import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_CATALOG;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_SCHEMA;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_TIME_ZONE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_USER;
import static com.facebook.presto.common.ErrorType.USER_ERROR;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createLineitem;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createRegion;
import static com.facebook.presto.sidecar.NativeSidecarPluginQueryRunnerUtils.setupNativeSidecarPlugin;
import static java.util.Collections.emptyList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

public class TestPlanCheckerRouterPlugin
        extends AbstractTestQueryFramework
{
    private static final JsonCodec<QueryResults> QUERY_RESULTS_CODEC = jsonCodec(QueryResults.class);
    private PlanCheckerRouterPluginConfig planCheckerRouterConfig;

    @BeforeClass
    public void setup()
            throws Exception
    {
        Logging.initialize();

        URI javaRouterUri = new URI("192.168.0.1");
        URI nativeRouterUri = new URI("192.168.0.2");

        URI planCheckerClusters = ((DistributedQueryRunner) getQueryRunner()).getCoordinator().getBaseUrl();
        planCheckerRouterConfig =
                new PlanCheckerRouterPluginConfig()
                        .setJavaRouterURI(javaRouterUri)
                        .setNativeRouterURI(nativeRouterUri)
                        .setPlanCheckClustersURIs(planCheckerClusters.toString());
    }

    @Override
    protected void createTables()
    {
        // TODO : We create a Java query runner and use it create tables, this fails in CI with the native query runner. Need to fix this
        QueryRunner queryRunner = (QueryRunner) getExpectedQueryRunner();
        createLineitem(queryRunner);
        createRegion(queryRunner);
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        DistributedQueryRunner queryRunner = (DistributedQueryRunner) PrestoNativeQueryRunnerUtils.nativeHiveQueryRunnerBuilder()
                .setAddStorageFormatToPath(true)
                .setFailOnNestedLoopJoin(true)
                .setCoordinatorSidecarEnabled(true)
                .build();
        setupNativeSidecarPlugin(queryRunner);
        return queryRunner;
    }

    @Override
    protected QueryRunner createExpectedQueryRunner()
            throws Exception
    {
        return PrestoNativeQueryRunnerUtils.javaHiveQueryRunnerBuilder()
                .setAddStorageFormatToPath(true)
                .build();
    }

    @Test
    public void testPlanCheckerPluginWithNativeCompatibleQueries()
    {
        Scheduler scheduler = new PlanCheckerRouterPluginScheduler(planCheckerRouterConfig);
        scheduler.setCandidates(planCheckerRouterConfig.getPlanCheckClustersURIs());

        // native compatible query
        Optional<URI> target = scheduler.getDestination(
                getMockRouterRequestInfo(
                        ImmutableListMultimap.of(
                                PRESTO_USER, "test",
                                PRESTO_TIME_ZONE, "America/Bahia_Banderas",
                                PRESTO_CATALOG, "tpch",
                                PRESTO_SCHEMA, "tiny"),
                        "SELECT lower(comment) from region"));
        assertTrue(target.isPresent());
        assertEquals(target.get(), planCheckerRouterConfig.getNativeRouterURI());
    }

    @Test
    public void testPlanCheckerPluginWithNativeIncompatibleQueries()
    {
        Scheduler scheduler = new PlanCheckerRouterPluginScheduler(planCheckerRouterConfig);
        scheduler.setCandidates(planCheckerRouterConfig.getPlanCheckClustersURIs());

        // native incompatible query
        Optional<URI> target = scheduler.getDestination(
                getMockRouterRequestInfo(
                        ImmutableListMultimap.of(
                                PRESTO_USER, "test",
                                PRESTO_TIME_ZONE, "America/Bahia_Banderas",
                                PRESTO_CATALOG, "tpch",
                                PRESTO_SCHEMA, "tiny"),
                        "SELECT EXISTS(SELECT 1 WHERE l.orderkey > 0 OR l.orderkey != 3) FROM lineitem l LIMIT 1"));
        assertTrue(target.isPresent());
        assertEquals(target.get(), planCheckerRouterConfig.getJavaRouterURI());
    }

    @Test
    public void testPlanCheckerPluginWithUserErrors()
    {
        Scheduler scheduler = new PlanCheckerRouterPluginScheduler(planCheckerRouterConfig);
        scheduler.setCandidates(planCheckerRouterConfig.getPlanCheckClustersURIs());

        // queries with user error, the below query does not have a defined catalog and schema
        try {
            scheduler.getDestination(
                    getMockRouterRequestInfo(
                            ImmutableListMultimap.of(
                                    PRESTO_USER, "test",
                                    PRESTO_TIME_ZONE, "America/Bahia_Banderas"),
                            "SELECT lower(comment) from region"));
        }
        catch (PrestoException e) {
            verifyQueryError(e, "line 1:55: Schema must be specified when session schema is not set");
        }
    }

    private static RouterRequestInfo getMockRouterRequestInfo(ListMultimap<String, String> headers, String query)
    {
        return new RouterRequestInfo("test", Optional.empty(), emptyList(), query, new MockHttpServletRequest(headers));
    }

    private static void verifyQueryError(PrestoException e, String message)
    {
        QueryError error = QUERY_RESULTS_CODEC.fromJson(e.getMessage()).getError();
        assertNotNull(error);
        assertTrue(error.getMessage().equalsIgnoreCase(message));
        assertSame(ErrorType.valueOf(error.getErrorType()), USER_ERROR);
    }
}
