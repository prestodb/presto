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
package com.facebook.presto.nativetests;

import com.facebook.airlift.bootstrap.Bootstrap;
import com.facebook.airlift.http.server.HttpServerInfo;
import com.facebook.airlift.http.server.testing.TestingHttpServerModule;
import com.facebook.airlift.jaxrs.JaxrsModule;
import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.json.JsonModule;
import com.facebook.airlift.log.Logging;
import com.facebook.airlift.node.testing.TestingNodeModule;
import com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils;
import com.facebook.presto.router.RouterModule;
import com.facebook.presto.router.scheduler.CustomSchedulerManager;
import com.facebook.presto.router.scheduler.PlanCheckerRouterPluginConfig;
import com.facebook.presto.router.scheduler.PlanCheckerRouterPluginPrestoClient;
import com.facebook.presto.router.scheduler.PlanCheckerRouterPluginSchedulerFactory;
import com.facebook.presto.router.security.RouterSecurityModule;
import com.facebook.presto.router.spec.GroupSpec;
import com.facebook.presto.router.spec.RouterSpec;
import com.facebook.presto.router.spec.SelectorRuleSpec;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableList;
import com.google.inject.Injector;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Optional;

import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static com.facebook.airlift.testing.Closeables.closeAllRuntimeException;
import static com.facebook.presto.router.scheduler.SchedulerType.CUSTOM_PLUGIN_SCHEDULER;
import static java.lang.Boolean.parseBoolean;
import static java.lang.String.format;
import static java.sql.DriverManager.getConnection;
import static java.util.Collections.singletonList;
import static org.testng.Assert.assertEquals;

public class TestPlanCheckerRouterPlugin
        extends AbstractTestQueryFramework
{
    private URI httpServerUri;
    private String storageFormat;
    private boolean sidecarEnabled;
    // mock object only to check the redirect requests counters.
    private PlanCheckerRouterPluginPrestoClient planCheckerRouterPluginPrestoClient;
    private QueryRunner nativeQueryRunner;

    @BeforeClass
    public void init()
            throws Exception
    {
        storageFormat = System.getProperty("storageFormat", "PARQUET");
        sidecarEnabled = parseBoolean(System.getProperty("sidecarEnabled", "true"));
        super.init();
        Logging.initialize();

        nativeQueryRunner = getQueryRunner();

        // for testing purposes, we can skip the router chaining part and specify the native/java clusters directly here
        URI nativeClusterURI = ((DistributedQueryRunner) getQueryRunner()).getCoordinator().getBaseUrl();
        URI javaClusterURI = ((DistributedQueryRunner) getExpectedQueryRunner()).getCoordinator().getBaseUrl();
        PlanCheckerRouterPluginConfig planCheckerRouterConfig = new PlanCheckerRouterPluginConfig()
                .setPlanCheckClustersURIs(nativeClusterURI.toString())
                .setJavaRouterURI(javaClusterURI)
                .setNativeRouterURI(nativeClusterURI)
                .setJavaClusterFallbackEnabled(true);

        planCheckerRouterPluginPrestoClient =
                new PlanCheckerRouterPluginPrestoClient(planCheckerRouterConfig);

        Path tempFile = Files.createTempFile("temp-config", ".json");
        File configFile = getConfigFile(singletonList(planCheckerRouterConfig.getNativeRouterURI()), tempFile.toFile());

        Bootstrap app = new Bootstrap(
                new TestingNodeModule("test"),
                new TestingHttpServerModule(),
                new JsonModule(),
                new JaxrsModule(true),
                new RouterSecurityModule(),
                new RouterModule(Optional.of(
                        new CustomSchedulerManager(
                                ImmutableList.of(new PlanCheckerRouterPluginSchedulerFactory()),
                                getPluginSchedulerConfigFile(planCheckerRouterConfig)))));

        Injector injector = app.doNotInitializeLogging()
                .setRequiredConfigurationProperty("router.config-file", configFile.getAbsolutePath())
                .setRequiredConfigurationProperty("presto.version", "test")
                .quiet().initialize();

        httpServerUri = injector.getInstance(HttpServerInfo.class).getHttpUri();
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return NativeTestsUtils.createNativeQueryRunner(storageFormat, sidecarEnabled);
    }

    @Override
    protected void createTables()
    {
        NativeTestsUtils.createTables(storageFormat);
    }

    @Override
    protected QueryRunner createExpectedQueryRunner()
            throws Exception
    {
        return PrestoNativeQueryRunnerUtils.javaHiveQueryRunnerBuilder()
                .setAddStorageFormatToPath(true)
                .setStorageFormat(storageFormat)
                .build();
    }

    @Test
    public void testNativeCompatibleQueries()
            throws Exception
    {
        if (sidecarEnabled) {
            List<String> queries = getNativeCompatibleQueries();
            for (String query : queries) {
                runQuery(query, httpServerUri);
            }
            assertEquals(planCheckerRouterPluginPrestoClient.getNativeClusterRedirectRequests().getTotalCount(), queries.size());
        }
    }

    @Test
    public void testNativeIncompatibleQueries()
            throws Exception
    {
        if (sidecarEnabled) {
            List<String> queries = getNativeIncompatibleQueries();
            for (String query : queries) {
                runQuery(query, httpServerUri);
            }
            // testFailingQueriesOnBothClusters() test case will run before this.
            // Since all the queries are failing on a native plan checker cluster, we redirect them to a java cluster and will count as a Java cluster redirect.
            assertEquals(planCheckerRouterPluginPrestoClient.getJavaClusterRedirectRequests().getTotalCount(), queries.size() + getFailingQueriesOnBothClustersProvider().length);
        }
    }

    @Test(dataProvider = "failingQueriesOnBothClustersProvider")
    public void testFailingQueriesOnBothClusters(String query, String exceptionMessage)
            throws SQLException
    {
        if (sidecarEnabled) {
            runQuery(query, httpServerUri, Optional.of(exceptionMessage));
        }
    }

    @Test(dependsOnMethods =
            {"testFailingQueriesOnBothClusters",
                    "testNativeCompatibleQueries",
                    "testNativeIncompatibleQueries"})
    public void testPlanCheckerClusterNotAvailable()
            throws SQLException
    {
        if (sidecarEnabled) {
            closeAllRuntimeException(nativeQueryRunner);
            nativeQueryRunner = null;
            for (String query : getNativeIncompatibleQueries()) {
                runQuery(query, httpServerUri);
            }
        }
    }

    @DataProvider(name = "failingQueriesOnBothClustersProvider")
    public Object[][] getFailingQueriesOnBothClustersProvider()
    {
        return new Object[][] {
                {"select * from nation", "line 1:15: Schema must be specified when session schema is not set"}};
    }

    private static List<String> getNativeCompatibleQueries()
    {
        String catalog = "hive";
        String schema = "tpch";
        return ImmutableList.of(
                format("SELECT lower(comment) from %s.%s.region", catalog, schema),
                "SHOW FUNCTIONS",
                format("SELECT approx_distinct(CAST(custkey AS DECIMAL(18, 0))) FROM %s.%s.orders", catalog, schema));
    }

    private static List<String> getNativeIncompatibleQueries()
    {
        return ImmutableList.of(
                "SELECT array_agg(a) OVER(ORDER BY a ASC NULLS FIRST GROUPS BETWEEN 1 PRECEDING AND 2 FOLLOWING) FROM (VALUES 3, 3, 3, 2, 2, 1, null, null) T(a)",
                "SELECT x AS y FROM (values (1,2), (2,3)) t(x, y) GROUP BY x ORDER BY apply(x, x -> -x) + 2*x");
    }

    private static File getConfigFile(List<URI> serverURIs, File tempFile)
            throws IOException
    {
        RouterSpec spec = new RouterSpec(ImmutableList.of(new GroupSpec("plan-checkers", serverURIs, Optional.empty(), Optional.empty())),
                ImmutableList.of(new SelectorRuleSpec(Optional.empty(), Optional.empty(), Optional.empty(), "plan-checkers")),
                Optional.of(CUSTOM_PLUGIN_SCHEDULER),
                Optional.empty(),
                Optional.empty());
        JsonCodec<RouterSpec> codec = jsonCodec(RouterSpec.class);
        Files.write(tempFile.toPath(), codec.toBytes(spec));
        return tempFile;
    }

    private static File getPluginSchedulerConfigFile(PlanCheckerRouterPluginConfig planCheckerRouterConfig)
            throws IOException
    {
        Path tempPluginSchedulerConfigFile = Files.createTempFile("router-scheduler-plan-checker", ".properties");
        String schedulerName = "router-scheduler.name=plan-checker";
        String planCheckerClusterURIs = format("plan-check-clusters-uris=%s", planCheckerRouterConfig.getPlanCheckClustersURIs().get(0));
        String javaClusterURI = format("router-java-url=%s", planCheckerRouterConfig.getJavaRouterURI());
        String nativeClusterURI = format("router-native-url=%s", planCheckerRouterConfig.getNativeRouterURI());
        String javaClusterFallbackEnabled = format("enable-java-cluster-fallback=%s", planCheckerRouterConfig.isJavaClusterFallbackEnabled());
        Files.write(tempPluginSchedulerConfigFile, ImmutableList.of(schedulerName, planCheckerClusterURIs, javaClusterURI, nativeClusterURI, javaClusterFallbackEnabled));
        return tempPluginSchedulerConfigFile.toFile();
    }

    private static void runQuery(String sql, URI uri)
            throws SQLException
    {
        runQuery(sql, uri, Optional.empty());
    }

    private static void runQuery(String sql, URI uri, Optional<String> exceptionMessage)
            throws SQLException
    {
        try {
            Connection connection = createConnection(uri);
            Statement statement = connection.createStatement();
            statement.executeQuery(sql);
        }
        catch (SQLException e) {
            if (exceptionMessage.isPresent()) {
                assertEquals(e.getCause().getMessage(), exceptionMessage.get());
                return;
            }
            throw e;
        }
    }

    private static Connection createConnection(URI uri)
            throws SQLException
    {
        String url = format("jdbc:presto://%s:%s", uri.getHost(), uri.getPort());
        return getConnection(url, "user", null);
    }
}
