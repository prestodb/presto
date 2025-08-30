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
import com.facebook.airlift.units.Duration;
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
import static com.facebook.presto.router.scheduler.SchedulerType.CUSTOM_PLUGIN_SCHEDULER;
import static java.lang.String.format;
import static java.sql.DriverManager.getConnection;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.testng.Assert.assertEquals;

public abstract class BasePlanCheckerTest
        extends AbstractTestQueryFramework
{
    protected URI httpServerUri;
    protected String storageFormat;
    protected boolean sidecarEnabled;
    protected PlanCheckerRouterPluginPrestoClient planCheckerRouterPluginPrestoClient;

    @BeforeClass
    public void init() throws Exception
    {
        storageFormat = System.getProperty("storageFormat", "PARQUET");
        sidecarEnabled = Boolean.parseBoolean(System.getProperty("sidecarEnabled", "true"));
        super.init();
        Logging.initialize();

        URI nativeClusterURI = ((DistributedQueryRunner) getQueryRunner()).getCoordinator().getBaseUrl();
        URI javaClusterURI = ((DistributedQueryRunner) getExpectedQueryRunner()).getCoordinator().getBaseUrl();

        PlanCheckerRouterPluginConfig planCheckerRouterConfig = new PlanCheckerRouterPluginConfig()
                .setPlanCheckClustersURIs(nativeClusterURI.toString())
                .setJavaRouterURI(javaClusterURI)
                .setNativeRouterURI(nativeClusterURI)
                .setJavaClusterFallbackEnabled(true)
                .setJavaClusterQueryRetryEnabled(true)
                .setClientRequestTimeout(new Duration(5, MINUTES));

        planCheckerRouterPluginPrestoClient = new PlanCheckerRouterPluginPrestoClient(planCheckerRouterConfig);

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

    protected abstract QueryRunner createQueryRunner() throws Exception;

    protected abstract QueryRunner createExpectedQueryRunner() throws Exception;

    @Override
    protected void createTables()
    {
        NativeTestsUtils.createTables(storageFormat);
    }

    protected static File getConfigFile(List<URI> serverURIs, File tempFile)
            throws IOException
    {
        RouterSpec spec = new RouterSpec(
                ImmutableList.of(new GroupSpec("plan-checkers", serverURIs, Optional.empty(), Optional.empty())),
                ImmutableList.of(new SelectorRuleSpec(Optional.empty(), Optional.empty(), Optional.empty(), "plan-checkers")),
                Optional.of(CUSTOM_PLUGIN_SCHEDULER),
                Optional.empty(),
                Optional.empty());
        JsonCodec<RouterSpec> codec = jsonCodec(RouterSpec.class);
        Files.write(tempFile.toPath(), codec.toBytes(spec));
        return tempFile;
    }

    protected static File getPluginSchedulerConfigFile(PlanCheckerRouterPluginConfig planCheckerRouterConfig)
            throws IOException
    {
        Path tempPluginSchedulerConfigFile = Files.createTempFile("router-scheduler-plan-checker", ".properties");
        Files.write(tempPluginSchedulerConfigFile, ImmutableList.of(
                "router-scheduler.name=plan-checker",
                "plan-check-clusters-uris=" + planCheckerRouterConfig.getPlanCheckClustersURIs().get(0),
                "router-java-url=" + planCheckerRouterConfig.getJavaRouterURI(),
                "router-native-url=" + planCheckerRouterConfig.getNativeRouterURI(),
                "enable-java-cluster-fallback=" + planCheckerRouterConfig.isJavaClusterFallbackEnabled(),
                "enable-java-cluster-query-retry=" + planCheckerRouterConfig.isJavaClusterQueryRetryEnabled(),
                "client-request-timeout=" + planCheckerRouterConfig.getClientRequestTimeout()));
        return tempPluginSchedulerConfigFile.toFile();
    }

    protected static Connection createConnection(URI uri)
            throws SQLException
    {
        String url = format("jdbc:presto://%s:%s", uri.getHost(), uri.getPort());
        return getConnection(url, "user", null);
    }

    protected static void runQuery(String sql, URI uri)
            throws SQLException
    {
        runQuery(sql, uri, Optional.empty());
    }

    protected static void runQuery(String sql, URI uri, Optional<String> exceptionMessage)
            throws SQLException
    {
        try (Connection connection = createConnection(uri);
                Statement statement = connection.createStatement()) {
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
}
