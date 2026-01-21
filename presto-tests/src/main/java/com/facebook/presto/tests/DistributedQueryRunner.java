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

import com.facebook.airlift.discovery.server.testing.TestingDiscoveryServer;
import com.facebook.airlift.http.client.HttpClient;
import com.facebook.airlift.http.client.Request;
import com.facebook.airlift.http.client.jetty.JettyHttpClient;
import com.facebook.airlift.log.Logger;
import com.facebook.airlift.testing.Assertions;
import com.facebook.airlift.units.Duration;
import com.facebook.presto.Session;
import com.facebook.presto.Session.SessionBuilder;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.cost.StatsCalculator;
import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.execution.QueryManager;
import com.facebook.presto.metadata.AllNodes;
import com.facebook.presto.metadata.Catalog;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.security.AllowAllSystemAccessControl;
import com.facebook.presto.server.BasicQueryInfo;
import com.facebook.presto.server.testing.TestingPrestoServer;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.CoordinatorPlugin;
import com.facebook.presto.spi.NodePoolType;
import com.facebook.presto.spi.NodeState;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.eventlistener.EventListener;
import com.facebook.presto.spi.function.SqlFunction;
import com.facebook.presto.split.PageSourceManager;
import com.facebook.presto.split.SplitManager;
import com.facebook.presto.sql.expressions.ExpressionOptimizerManager;
import com.facebook.presto.sql.parser.SqlParserOptions;
import com.facebook.presto.sql.planner.ConnectorPlanOptimizerManager;
import com.facebook.presto.sql.planner.NodePartitioningManager;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.sanity.PlanCheckerProviderManager;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.testing.TestingAccessControlManager;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Closer;
import com.google.inject.Module;
import org.intellij.lang.annotations.Language;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiFunction;
import java.util.function.Function;

import static com.facebook.airlift.http.client.JsonResponseHandler.createJsonResponseHandler;
import static com.facebook.airlift.http.client.Request.Builder.prepareGet;
import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static com.facebook.airlift.units.Duration.nanosSince;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_USER;
import static com.facebook.presto.spi.NodePoolType.INTERMEDIATE;
import static com.facebook.presto.spi.NodePoolType.LEAF;
import static com.facebook.presto.testing.TestingSession.TESTING_CATALOG;
import static com.facebook.presto.testing.TestingSession.createBogusTestingCatalog;
import static com.facebook.presto.tests.AbstractTestQueries.TEST_CATALOG_PROPERTIES;
import static com.facebook.presto.tests.AbstractTestQueries.TEST_SYSTEM_PROPERTIES;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.String.format;
import static java.lang.System.nanoTime;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class DistributedQueryRunner
        implements QueryRunner
{
    private static final Logger log = Logger.get(DistributedQueryRunner.class);
    private static final String ENVIRONMENT = "testing";
    private static final String DEFAULT_USER = "user";
    private static final SqlParserOptions DEFAULT_SQL_PARSER_OPTIONS = new SqlParserOptions();

    private final TestingDiscoveryServer discoveryServer;
    private final List<TestingPrestoServer> coordinators;
    private final int coordinatorCount;
    private final List<TestingPrestoServer> servers;
    private final List<Process> externalWorkers;
    private final List<Module> extraModules;

    private final Closer closer = Closer.create();
    private final HttpClient client = new JettyHttpClient();

    private final List<TestingPrestoClient> prestoClients;

    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private Optional<TestingPrestoServer> catalogServer = Optional.empty();
    private Optional<List<TestingPrestoServer>> resourceManagers;
    private Optional<TestingPrestoServer> coordinatorSidecar = Optional.empty();

    private final int resourceManagerCount;
    private final AtomicReference<Handle> testFunctionNamespacesHandle = new AtomicReference<>();

    private final Map<String, String> accessControlProperties;

    @Deprecated
    public DistributedQueryRunner(Session defaultSession, int nodeCount)
            throws Exception
    {
        this(defaultSession, nodeCount, ImmutableMap.of());
    }

    @Deprecated
    public DistributedQueryRunner(Session defaultSession, int nodeCount, Map<String, String> extraProperties)
            throws Exception
    {
        this(
                false,
                false,
                false,
                false,
                defaultSession,
                nodeCount,
                1,
                1,
                extraProperties,
                ImmutableMap.of(),
                ImmutableMap.of(),
                ImmutableMap.of(),
                ImmutableMap.of(),
                DEFAULT_SQL_PARSER_OPTIONS,
                ENVIRONMENT,
                Optional.empty(),
                Optional.empty(),
                ImmutableList.of(),
                ImmutableMap.of("access-control.name", AllowAllSystemAccessControl.NAME));
    }

    public static Builder builder(Session defaultSession)
    {
        return new Builder(defaultSession);
    }

    private DistributedQueryRunner(
            boolean resourceManagerEnabled,
            boolean catalogServerEnabled,
            boolean coordinatorSidecarEnabled,
            boolean skipLoadingResourceGroupConfigurationManager,
            Session defaultSession,
            int nodeCount,
            int coordinatorCount,
            int resourceManagerCount,
            Map<String, String> extraProperties,
            Map<String, String> coordinatorProperties,
            Map<String, String> resourceManagerProperties,
            Map<String, String> catalogServerProperties,
            Map<String, String> coordinatorSidecarProperties,
            SqlParserOptions parserOptions,
            String environment,
            Optional<Path> dataDirectory,
            Optional<BiFunction<Integer, URI, Process>> externalWorkerLauncher,
            List<Module> extraModules,
            Map<String, String> accessControlProperties)
            throws Exception
    {
        requireNonNull(defaultSession, "defaultSession is null");
        this.extraModules = requireNonNull(extraModules, "extraModules is null");
        this.accessControlProperties = requireNonNull(accessControlProperties, "accessControlProperties is null");

        try {
            long start = nanoTime();
            discoveryServer = new TestingDiscoveryServer(environment);
            this.coordinatorCount = coordinatorCount;
            this.resourceManagerCount = resourceManagerCount;
            closer.register(() -> closeUnchecked(discoveryServer));
            log.info("Created TestingDiscoveryServer in %s", nanosSince(start).convertToMostSuccinctTimeUnit());
            URI discoveryUrl = discoveryServer.getBaseUrl();
            log.info("Discovery URL %s", discoveryUrl);

            ImmutableList.Builder<TestingPrestoServer> servers = ImmutableList.builder();
            ImmutableList.Builder<TestingPrestoServer> coordinators = ImmutableList.builder();
            ImmutableList.Builder<TestingPrestoServer> resourceManagers = ImmutableList.builder();
            Map<String, String> extraCoordinatorProperties = new HashMap<>();

            if (externalWorkerLauncher.isPresent()) {
                ImmutableList.Builder<Process> externalWorkersBuilder = ImmutableList.builder();
                for (int i = 0; i < nodeCount; i++) {
                    externalWorkersBuilder.add(externalWorkerLauncher.get().apply(i, discoveryUrl));
                }
                externalWorkers = externalWorkersBuilder.build();
                closer.register(() -> {
                    for (Process worker : externalWorkers) {
                        worker.destroyForcibly();
                    }
                });

                // Don't use coordinator as worker
                extraCoordinatorProperties.put("node-scheduler.include-coordinator", "false");
            }
            else {
                externalWorkers = ImmutableList.of();

                for (int i = (coordinatorCount + (resourceManagerEnabled ? resourceManagerCount : 0)); i < nodeCount; i++) {
                    // We are simply splitting the nodes into leaf and intermediate for testing purpose
                    NodePoolType workerPool = i % 2 == 0 ? LEAF : INTERMEDIATE;
                    Map<String, String> workerProperties = new HashMap<>(extraProperties);
                    workerProperties.put("pool-type", workerPool.name());
                    TestingPrestoServer worker = closer.register(createTestingPrestoServer(
                            discoveryUrl,
                            false,
                            resourceManagerEnabled,
                            false,
                            catalogServerEnabled,
                            false,
                            coordinatorSidecarEnabled,
                            false,
                            skipLoadingResourceGroupConfigurationManager,
                            false,
                            workerProperties,
                            parserOptions,
                            environment,
                            dataDirectory,
                            extraModules));
                    servers.add(worker);
                }
            }

            extraCoordinatorProperties.put("experimental.iterative-optimizer-enabled", "true");
            extraCoordinatorProperties.putAll(extraProperties);
            extraCoordinatorProperties.putAll(coordinatorProperties);

            if (resourceManagerEnabled) {
                for (int i = 0; i < resourceManagerCount; i++) {
                    Map<String, String> rmProperties = new HashMap<>(resourceManagerProperties);
                    if (resourceManagerProperties.get("raft.isEnabled") != null) {
                        int raftPort = Integer.valueOf(resourceManagerProperties.get("raft.port")) + i;
                        rmProperties.replace("raft.port", String.valueOf(raftPort));
                    }
                    TestingPrestoServer resourceManager = closer.register(createTestingPrestoServer(
                            discoveryUrl,
                            true,
                            true,
                            false,
                            false,
                            false,
                            false,
                            false,
                            skipLoadingResourceGroupConfigurationManager,
                            false,
                            rmProperties,
                            parserOptions,
                            environment,
                            dataDirectory,
                            extraModules));
                    servers.add(resourceManager);
                    resourceManagers.add(resourceManager);
                }
            }

            if (catalogServerEnabled) {
                catalogServer = Optional.of(closer.register(createTestingPrestoServer(
                        discoveryUrl,
                        false,
                        false,
                        true,
                        true,
                        false,
                        false,
                        false,
                        skipLoadingResourceGroupConfigurationManager,
                        false,
                        catalogServerProperties,
                        parserOptions,
                        environment,
                        dataDirectory,
                        extraModules)));
                servers.add(catalogServer.get());
            }

            if (coordinatorSidecarEnabled) {
                coordinatorSidecar = Optional.of(closer.register(createTestingPrestoServer(
                        discoveryUrl,
                        false,
                        false,
                        false,
                        false,
                        true,
                        true,
                        false,
                        skipLoadingResourceGroupConfigurationManager,
                        false,
                        coordinatorSidecarProperties,
                        parserOptions,
                        environment,
                        dataDirectory,
                        extraModules)));
                servers.add(coordinatorSidecar.get());
            }

            final boolean loadDefaultSystemAccessControl = !accessControlProperties.containsKey("access-control.name") ||
                    accessControlProperties.get("access-control.name").equals("allow-all");
            for (int i = 0; i < coordinatorCount; i++) {
                TestingPrestoServer coordinator = closer.register(createTestingPrestoServer(
                        discoveryUrl,
                        false,
                        resourceManagerEnabled,
                        false,
                        catalogServerEnabled,
                        false,
                        false,
                        true,
                        skipLoadingResourceGroupConfigurationManager,
                        loadDefaultSystemAccessControl,
                        extraCoordinatorProperties,
                        parserOptions,
                        environment,
                        dataDirectory,
                        extraModules));
                servers.add(coordinator);
                coordinators.add(coordinator);
                extraCoordinatorProperties.remove("http-server.http.port");
            }

            this.servers = servers.build();
            this.coordinators = coordinators.build();
            this.resourceManagers = Optional.of(resourceManagers.build());
        }
        catch (Exception e) {
            try {
                throw closer.rethrow(e, Exception.class);
            }
            finally {
                closer.close();
            }
        }

        // copy session using property manager in coordinator
        defaultSession = defaultSession.toSessionRepresentation().toSession(coordinators.get(0).getMetadata().getSessionPropertyManager());

        ImmutableList.Builder<TestingPrestoClient> prestoClientsBuilder = ImmutableList.builder();
        for (int i = 0; i < coordinatorCount; i++) {
            prestoClientsBuilder.add(closer.register(new TestingPrestoClient(coordinators.get(i), defaultSession)));
        }
        prestoClients = prestoClientsBuilder.build();

        try {
            waitForAllNodesGloballyVisible();
        }
        catch (TimeoutException e) {
            closer.close();
            throw e;
        }

        long start = nanoTime();
        for (TestingPrestoServer server : servers) {
            server.getMetadata().registerBuiltInFunctions(AbstractTestQueries.CUSTOM_FUNCTIONS);
        }
        log.info("Added functions in %s", nanosSince(start).convertToMostSuccinctTimeUnit());

        for (TestingPrestoServer server : servers) {
            // add bogus catalog for testing procedures and session properties
            Catalog bogusTestingCatalog = createBogusTestingCatalog(TESTING_CATALOG);
            server.getCatalogManager().registerCatalog(bogusTestingCatalog);

            SessionPropertyManager sessionPropertyManager = server.getMetadata().getSessionPropertyManager();
            sessionPropertyManager.addSystemSessionProperties(TEST_SYSTEM_PROPERTIES);
            sessionPropertyManager.addConnectorSessionProperties(bogusTestingCatalog.getConnectorId(), TEST_CATALOG_PROPERTIES);
        }
    }

    public void waitForClusterToGetReady()
            throws InterruptedException
    {
        for (int i = 0; i < coordinators.size(); i++) {
            NodeState state = NodeState.INACTIVE;
            while (state != NodeState.ACTIVE) {
                MILLISECONDS.sleep(10);
                state = getCoordinatorInfoState(i);
            }
        }

        int availableCoordinators = 0;
        if (getResourceManager().isPresent()) {
            while (availableCoordinators != coordinators.size()) {
                MILLISECONDS.sleep(10);
                availableCoordinators = getResourceManager().get().getNodeManager().getCoordinators().size();
            }
        }
    }

    private NodeState getCoordinatorInfoState(int coordinator)
    {
        URI uri = URI.create(getCoordinator(coordinator).getBaseUrl().toString() + "/v1/info/state");
        Request request = prepareGet()
                .setHeader(PRESTO_USER, DEFAULT_USER)
                .setUri(uri)
                .build();

        NodeState state = client.execute(request, createJsonResponseHandler(jsonCodec(NodeState.class)));
        return state;
    }

    public NodeState getWorkerInfoState(int worker)
    {
        URI uri = URI.create(getWorker(worker).getBaseUrl().toString() + "/v1/info/state");
        Request request = prepareGet()
                .setHeader(PRESTO_USER, DEFAULT_USER)
                .setUri(uri)
                .build();

        NodeState state = client.execute(request, createJsonResponseHandler(jsonCodec(NodeState.class)));
        return state;
    }

    public int sendWorkerRequest(int worker, String body)
    {
        try {
            URL url = new URL(getWorker(worker).getBaseUrl().toString() + "/v1/info/state");
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("PUT");
            connection.setDoOutput(true);
            connection.setRequestProperty("Content-Type", "application/json");

            try (OutputStream os = connection.getOutputStream()) {
                byte[] input = body.getBytes(StandardCharsets.UTF_8);
                os.write(input, 0, input.length);
            }
            return connection.getResponseCode();
        }
        catch (Exception e) {
            e.printStackTrace();
            return 500;
        }
    }

    private static TestingPrestoServer createTestingPrestoServer(
            URI discoveryUri,
            boolean resourceManager,
            boolean resourceManagerEnabled,
            boolean catalogServer,
            boolean catalogServerEnabled,
            boolean coordinatorSidecar,
            boolean coordinatorSidecarEnabled,
            boolean coordinator,
            boolean skipLoadingResourceGroupConfigurationManager,
            boolean loadDefaultSystemAccessControl,
            Map<String, String> extraProperties,
            SqlParserOptions parserOptions,
            String environment,
            Optional<Path> dataDirectory,
            List<Module> extraModules)
            throws Exception
    {
        long start = nanoTime();
        ImmutableMap.Builder<String, String> propertiesBuilder = ImmutableMap.<String, String>builder()
                .put("query.client.timeout", "10m")
                .put("exchange.http-client.idle-timeout", "1h")
                .put("task.max-index-memory", "16kB") // causes index joins to fault load
                .put("datasources", "system")
                .put("distributed-index-joins-enabled", "true")
                .put("exchange.checksum-enabled", "true");
        if (coordinator) {
            propertiesBuilder.put("node-scheduler.include-coordinator", extraProperties.getOrDefault("node-scheduler.include-coordinator", "true"));
            propertiesBuilder.put("join-distribution-type", "PARTITIONED");
        }
        HashMap<String, String> properties = new HashMap<>(propertiesBuilder.build());
        properties.putAll(extraProperties);

        TestingPrestoServer server = new TestingPrestoServer(
                resourceManager,
                resourceManagerEnabled,
                catalogServer,
                catalogServerEnabled,
                coordinatorSidecar,
                coordinatorSidecarEnabled,
                coordinator,
                skipLoadingResourceGroupConfigurationManager,
                loadDefaultSystemAccessControl,
                properties,
                environment,
                discoveryUri,
                parserOptions,
                extraModules,
                dataDirectory);

        String nodeRole = "worker";
        if (coordinator) {
            nodeRole = "coordinator";
        }
        else if (resourceManager) {
            nodeRole = "resourceManager";
        }
        else if (catalogServer) {
            nodeRole = "catalogServer";
        }
        else if (coordinatorSidecar) {
            nodeRole = "coordinatorSidecar";
        }
        log.info("Created %s TestingPrestoServer in %s: %s", nodeRole, nanosSince(start).convertToMostSuccinctTimeUnit(), server.getBaseUrl());

        return server;
    }

    private void waitForAllNodesGloballyVisible()
            throws Exception
    {
        long startTimeInMs = nanoTime();
        int expectedActiveNodes = externalWorkers.size() + servers.size();
        Duration timeout = new Duration(100, SECONDS);

        for (int serverIndex = 0; serverIndex < servers.size(); ) {
            TestingPrestoServer server = servers.get(serverIndex);
            AllNodes allNodes = server.refreshNodes();
            int activeNodeCount = allNodes.getActiveNodes().size();

            if (!allNodes.getInactiveNodes().isEmpty()) {
                throwTimeoutIfNotReady(
                        startTimeInMs,
                        timeout,
                        format("Timed out waiting for all nodes to be globally visible. Inactive nodes: %s", allNodes.getInactiveNodes()));
                MILLISECONDS.sleep(10);
                serverIndex = 0;
            }
            else if ((server.isCoordinator() || server.isResourceManager()) && activeNodeCount != expectedActiveNodes) {
                throwTimeoutIfNotReady(
                        startTimeInMs,
                        timeout,
                        format("Timed out waiting for all nodes to be globally visible. Node count: %s, expected: %s", activeNodeCount, expectedActiveNodes));
                MILLISECONDS.sleep(10);
                serverIndex = 0;
            }
            else {
                log.info("Server %s has %s active nodes", server.getBaseUrl(), activeNodeCount);
                serverIndex++;
            }
        }

        log.info("Announced servers in %s", nanosSince(startTimeInMs).convertToMostSuccinctTimeUnit());
    }

    private static void throwTimeoutIfNotReady(long startTimeInMs, Duration timeout, String message)
            throws TimeoutException
    {
        if (nanosSince(startTimeInMs).compareTo(timeout) >= 0) {
            throw new TimeoutException(message);
        }
    }

    public TestingPrestoClient getRandomClient()
    {
        return prestoClients.get(getRandomCoordinatorIndex());
    }

    private int getRandomCoordinatorIndex()
    {
        return ThreadLocalRandom.current().nextInt(prestoClients.size());
    }

    @Override
    public int getNodeCount()
    {
        return servers.size() + externalWorkers.size();
    }

    @Override
    public Session getDefaultSession()
    {
        return getRandomClient().getDefaultSession();
    }

    @Override
    public TransactionManager getTransactionManager()
    {
        checkState(coordinators.size() == 1, "Expected a single coordinator");
        return coordinators.get(0).getTransactionManager();
    }

    @Override
    public Metadata getMetadata()
    {
        checkState(coordinators.size() == 1, "Expected a single coordinator");
        return coordinators.get(0).getMetadata();
    }

    @Override
    public SplitManager getSplitManager()
    {
        checkState(coordinators.size() == 1, "Expected a single coordinator");
        return coordinators.get(0).getSplitManager();
    }

    @Override
    public PageSourceManager getPageSourceManager()
    {
        checkState(coordinators.size() == 1, "Expected a single coordinator");
        return coordinators.get(0).getPageSourceManager();
    }

    @Override
    public NodePartitioningManager getNodePartitioningManager()
    {
        checkState(coordinators.size() == 1, "Expected a single coordinator");
        return coordinators.get(0).getNodePartitioningManager();
    }

    @Override
    public ConnectorPlanOptimizerManager getPlanOptimizerManager()
    {
        checkState(coordinators.size() == 1, "Expected a single coordinator");
        return coordinators.get(0).getPlanOptimizerManager();
    }

    @Override
    public StatsCalculator getStatsCalculator()
    {
        checkState(coordinators.size() == 1, "Expected a single coordinator");
        return coordinators.get(0).getStatsCalculator();
    }

    @Override
    public List<EventListener> getEventListeners()
    {
        checkState(coordinators.size() == 1, "Expected a single coordinator");
        return coordinators.get(0).getEventListeners();
    }

    @Override
    public TestingAccessControlManager getAccessControl()
    {
        checkState(coordinators.size() == 1, "Expected a single coordinator");
        return coordinators.get(0).getAccessControl();
    }

    @Override
    public PlanCheckerProviderManager getPlanCheckerProviderManager()
    {
        checkState(coordinators.size() == 1, "Expected a single coordinator");
        return coordinators.get(0).getPlanCheckerProviderManager();
    }

    @Override
    public ExpressionOptimizerManager getExpressionManager()
    {
        checkState(coordinators.size() == 1, "Expected a single coordinator");
        return coordinators.get(0).getExpressionManager();
    }

    public TestingPrestoServer getCoordinator()
    {
        checkState(coordinators.size() == 1, "Expected a single coordinator");
        return coordinators.get(0);
    }

    public TestingPrestoServer getCoordinator(int coordinator)
    {
        checkState(coordinator < coordinators.size(), format("Expected coordinator index %d < %d", coordinator, coordinatorCount));
        return coordinators.get(coordinator);
    }

    private TestingPrestoServer getWorker(int worker)
    {
        checkState(worker < servers.size(), format("Expected worker index %d < %d", worker, servers.size()));
        return servers.get(worker);
    }

    public List<TestingPrestoServer> getCoordinators()
    {
        return coordinators;
    }

    public Optional<TestingPrestoServer> getResourceManager()
    {
        return resourceManagers.isPresent() && !resourceManagers.get().isEmpty() ? Optional.of(resourceManagers.get().get(0)) : Optional.empty();
    }

    public Optional<TestingPrestoServer> getCatalogServer()
    {
        return catalogServer;
    }

    public Optional<TestingPrestoServer> getCoordinatorSidecar()
    {
        return coordinatorSidecar;
    }

    public TestingPrestoServer getResourceManager(int resourceManager)
    {
        checkState(resourceManager < resourceManagers.get().size(), format("Expected resource manager index %d < %d", resourceManager, resourceManagerCount));
        return resourceManagers.get().get(resourceManager);
    }

    public List<TestingPrestoServer> getResourceManagers()
    {
        return resourceManagers.get();
    }

    public List<TestingPrestoServer> getCoordinatorWorkers()
    {
        return getServers().stream().filter(server -> !server.isResourceManager()).collect(ImmutableList.toImmutableList());
    }

    public List<TestingPrestoServer> getServers()
    {
        return ImmutableList.copyOf(servers);
    }

    @Override
    public void installPlugin(Plugin plugin)
    {
        installPlugin(plugin, false);
    }

    @Override
    public void installCoordinatorPlugin(CoordinatorPlugin plugin)
    {
        installCoordinatorPlugin(plugin, false);
    }

    public void createCatalog(String catalogName, String connectorName)
    {
        createCatalog(catalogName, connectorName, ImmutableMap.of());
    }

    @Override
    public void createCatalog(String catalogName, String connectorName, Map<String, String> properties)
    {
        long start = nanoTime();
        Set<ConnectorId> connectorIds = new HashSet<>();
        for (TestingPrestoServer server : servers) {
            connectorIds.add(server.createCatalog(catalogName, connectorName, properties));
        }
        ConnectorId connectorId = getOnlyElement(connectorIds);
        log.info("Created catalog %s (%s) in %s", catalogName, connectorId, nanosSince(start));

        // wait for all nodes to announce the new catalog
        start = nanoTime();
        while (!isConnectorVisibleToAllNodes(connectorId)) {
            Assertions.assertLessThan(nanosSince(start), new Duration(100, SECONDS), "waiting for connector " + connectorId + " to be initialized in every node");
            try {
                MILLISECONDS.sleep(10);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }
        log.info("Announced catalog %s (%s) in %s", catalogName, connectorId, nanosSince(start));
    }

    @Override
    public void loadFunctionNamespaceManager(String functionNamespaceManagerName, String catalogName, Map<String, String> properties)
    {
        for (TestingPrestoServer server : servers) {
            server.getMetadata().getFunctionAndTypeManager().loadFunctionNamespaceManager(functionNamespaceManagerName, catalogName, properties, server.getPluginNodeManager());
        }
    }

    /**
     * This method exists only because it is currently impossible to create a function namespace from the query engine,
     * and therefore the query runner needs to be aware of the H2 handle in order to create function namespaces when
     * required by the tests.
     * <p>
     * TODO: Remove when there is a generic way of creating function namespaces as if creating schemas.
     */
    public void enableTestFunctionNamespaces(List<String> catalogNames, Map<String, String> additionalProperties)
    {
        enableTestFunctionNamespaces(catalogNames, additionalProperties, false);
    }

    public void enableTestFunctionNamespacesOnCoordinators(List<String> catalogNames, Map<String, String> additionalProperties)
    {
        enableTestFunctionNamespaces(catalogNames, additionalProperties, true);
    }

    public void createTestFunctionNamespace(String catalogName, String schemaName)
    {
        checkState(testFunctionNamespacesHandle.get() != null, "Test function namespaces not enabled");

        testFunctionNamespacesHandle.get().execute("INSERT INTO function_namespaces SELECT ?, ?", catalogName, schemaName);
    }

    public void loadSystemAccessControl()
    {
        for (TestingPrestoServer server : servers) {
            if (server.isCoordinator()) {
                server.getAccessControl().loadSystemAccessControl(accessControlProperties);
            }
        }
    }

    private boolean isConnectorVisibleToAllNodes(ConnectorId connectorId)
    {
        if (!externalWorkers.isEmpty()) {
            return true;
        }

        for (TestingPrestoServer server : servers) {
            server.refreshNodes();
            Set<InternalNode> activeNodesWithConnector = server.getActiveNodesWithConnector(connectorId);
            if (((server.isCoordinator() && server.nodeSchedulerIncludeCoordinator()) || server.isResourceManager()) && activeNodesWithConnector.size() != servers.size()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public List<QualifiedObjectName> listTables(Session session, String catalog, String schema)
    {
        lock.readLock().lock();
        try {
            return getRandomClient().listTables(session, catalog, schema);
        }
        finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public boolean tableExists(Session session, String table)
    {
        lock.readLock().lock();
        try {
            return getRandomClient().tableExists(session, table);
        }
        finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public MaterializedResult execute(@Language("SQL") String sql)
    {
        return execute(getRandomCoordinatorIndex(), sql);
    }

    @Override
    public MaterializedResult execute(Session session, @Language("SQL") String sql)
    {
        return execute(getRandomCoordinatorIndex(), session, sql);
    }

    public ResultWithQueryId<MaterializedResult> executeWithQueryId(Session session, @Language("SQL") String sql)
    {
        return executeWithQueryId(getRandomCoordinatorIndex(), session, sql);
    }

    @Override
    public MaterializedResultWithPlan executeWithPlan(Session session, String sql, WarningCollector warningCollector)
    {
        ResultWithQueryId<MaterializedResult> resultWithQueryId = executeWithQueryId(session, sql);
        return new MaterializedResultWithPlan(resultWithQueryId.getResult().toTestTypes(), getQueryPlan(resultWithQueryId.getQueryId()));
    }

    public MaterializedResult execute(int coordinator, @Language("SQL") String sql)
    {
        checkArgument(coordinator >= 0 && coordinator < coordinators.size());
        lock.readLock().lock();
        try {
            return prestoClients.get(coordinator).execute(sql).getResult();
        }
        finally {
            lock.readLock().unlock();
        }
    }

    public MaterializedResult execute(int coordinator, Session session, @Language("SQL") String sql)
    {
        checkArgument(coordinator >= 0 && coordinator < coordinators.size());
        lock.readLock().lock();
        try {
            return prestoClients.get(coordinator).execute(session, sql).getResult();
        }
        finally {
            lock.readLock().unlock();
        }
    }

    public ResultWithQueryId<MaterializedResult> executeWithQueryId(int coordinator, Session session, @Language("SQL") String sql)
    {
        checkArgument(coordinator >= 0 && coordinator < coordinators.size());
        lock.readLock().lock();
        try {
            return prestoClients.get(coordinator).execute(session, sql);
        }
        finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public Plan createPlan(Session session, String sql, WarningCollector warningCollector)
    {
        QueryId queryId = executeWithQueryId(session, sql).getQueryId();
        Plan queryPlan = getQueryPlan(queryId);
        checkState(coordinators.size() == 1, "Expected a single coordinator");
        coordinators.get(0).getQueryManager().cancelQuery(queryId);
        return queryPlan;
    }

    public List<BasicQueryInfo> getQueries()
    {
        checkState(coordinators.size() == 1, "Expected a single coordinator");
        return coordinators.get(0).getQueryManager().getQueries();
    }

    public QueryInfo getQueryInfo(QueryId queryId)
    {
        checkState(coordinators.size() == 1, "Expected a single coordinator");
        return coordinators.get(0).getQueryManager().getFullQueryInfo(queryId);
    }

    public Plan getQueryPlan(QueryId queryId)
    {
        checkState(coordinators.size() == 1, "Expected a single coordinator");
        return coordinators.get(0).getQueryPlan(queryId);
    }

    @Override
    public Lock getExclusiveLock()
    {
        return lock.writeLock();
    }

    @Override
    public final synchronized void close()
    {
        cancelAllQueries();
        try {
            closer.close();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public void cancelAllQueries()
    {
        for (TestingPrestoServer coordinator : coordinators) {
            QueryManager queryManager = coordinator.getQueryManager();
            for (BasicQueryInfo queryInfo : queryManager.getQueries()) {
                if (!queryInfo.getState().isDone()) {
                    queryManager.cancelQuery(queryInfo.getQueryId());
                }
            }
        }
    }

    private void enableTestFunctionNamespaces(List<String> catalogNames, Map<String, String> additionalProperties, boolean coordinatorOnly)
    {
        checkState(testFunctionNamespacesHandle.get() == null, "Test function namespaces already enabled");

        String databaseName = nanoTime() + "_" + ThreadLocalRandom.current().nextInt();
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("database-name", databaseName)
                .putAll(additionalProperties)
                .build();
        installPlugin(new H2FunctionNamespaceManagerPlugin(), coordinatorOnly);
        for (String catalogName : catalogNames) {
            loadFunctionNamespaceManager("h2", catalogName, properties, coordinatorOnly);
        }

        Handle handle = Jdbi.open(H2ConnectionModule.getJdbcUrl(databaseName));
        testFunctionNamespacesHandle.set(handle);
        closer.register(handle);
    }

    private void loadFunctionNamespaceManager(
            String functionNamespaceManagerName,
            String catalogName,
            Map<String, String> properties,
            boolean coordinatorOnly)
    {
        for (TestingPrestoServer server : servers) {
            if (coordinatorOnly && !server.isCoordinator()) {
                continue;
            }
            server.getMetadata().getFunctionAndTypeManager().loadFunctionNamespaceManager(functionNamespaceManagerName, catalogName, properties, server.getPluginNodeManager());
        }
    }

    private void installPlugin(Plugin plugin, boolean coordinatorOnly)
    {
        long start = nanoTime();
        for (TestingPrestoServer server : servers) {
            if (coordinatorOnly && !server.isCoordinator()) {
                continue;
            }
            server.installPlugin(plugin);
        }
        log.info("Installed plugin %s in %s", plugin.getClass().getSimpleName(), nanosSince(start).convertToMostSuccinctTimeUnit());
    }

    public void registerWorkerAggregateFunctions(List<? extends SqlFunction> aggregateFunctions)
    {
        for (TestingPrestoServer server : servers) {
            if (!server.isCoordinator()) {
                continue;
            }
            server.registerWorkerAggregateFunctions(aggregateFunctions);
        }
    }

    private void installCoordinatorPlugin(CoordinatorPlugin plugin, boolean coordinatorOnly)
    {
        long start = nanoTime();
        for (TestingPrestoServer server : servers) {
            if (coordinatorOnly && !server.isCoordinator()) {
                continue;
            }
            server.installCoordinatorPlugin(plugin);
        }
        log.info("Installed plugin %s in %s", plugin.getClass().getSimpleName(), nanosSince(start).convertToMostSuccinctTimeUnit());
    }

    @Override
    public void loadSessionPropertyProvider(String sessionPropertyProviderName, Map<String, String> properties)
    {
        for (TestingPrestoServer server : servers) {
            server.getMetadata().getSessionPropertyManager().loadSessionPropertyProvider(
                    sessionPropertyProviderName,
                    properties,
                    Optional.ofNullable(server.getMetadata().getFunctionAndTypeManager()),
                    Optional.ofNullable(server.getPluginNodeManager()));
        }
    }

    @Override
    public void loadTypeManager(String typeManagerName)
    {
        for (TestingPrestoServer server : servers) {
            server.getMetadata().getFunctionAndTypeManager().loadTypeManager(typeManagerName);
        }
    }

    @Override
    public void loadPlanCheckerProviderManager(String planCheckerProviderName, Map<String, String> properties)
    {
        for (TestingPrestoServer server : servers) {
            server.getPlanCheckerProviderManager().loadPlanCheckerProvider(planCheckerProviderName, properties, server.getPluginNodeManager());
        }
    }

    @Override
    public void triggerConflictCheckWithBuiltInFunctions()
    {
        for (TestingPrestoServer server : servers) {
            server.triggerConflictCheckWithBuiltInFunctions();
        }
    }

    public void registerNativeFunctions()
    {
        for (TestingPrestoServer server : servers) {
            server.registerWorkerFunctions();
        }
    }

    @Override
    public void loadTVFProvider(String tvfProviderName)
    {
        for (TestingPrestoServer server : servers) {
            server.getMetadata().getFunctionAndTypeManager().loadTVFProvider(
                    tvfProviderName,
                    server.getPluginNodeManager());
        }
    }

    private static void closeUnchecked(AutoCloseable closeable)
    {
        try {
            closeable.close();
        }
        catch (Exception e) {
            throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }

    public static class Builder
    {
        private Session defaultSession;
        private int nodeCount = 4;
        private int coordinatorCount = 1;
        private Map<String, String> extraProperties = ImmutableMap.of();
        private Map<String, String> coordinatorProperties = ImmutableMap.of();
        private Map<String, String> resourceManagerProperties = ImmutableMap.of();
        private Map<String, String> catalogServerProperties = ImmutableMap.of();
        private Map<String, String> coordinatorSidecarProperties = ImmutableMap.of();
        private SqlParserOptions parserOptions = DEFAULT_SQL_PARSER_OPTIONS;
        private String environment = ENVIRONMENT;
        private Optional<Path> dataDirectory = Optional.empty();
        private Optional<BiFunction<Integer, URI, Process>> externalWorkerLauncher = Optional.empty();
        private boolean resourceManagerEnabled;
        private boolean catalogServerEnabled;
        private boolean coordinatorSidecarEnabled;
        private boolean skipLoadingResourceGroupConfigurationManager;
        private List<Module> extraModules = ImmutableList.of();
        private int resourceManagerCount = 1;
        private Map<String, String> accessControlProperties = ImmutableMap.of("access-control.name", AllowAllSystemAccessControl.NAME);

        protected Builder(Session defaultSession)
        {
            this.defaultSession = requireNonNull(defaultSession, "defaultSession is null");
        }

        public Builder amendSession(Function<SessionBuilder, SessionBuilder> amendSession)
        {
            SessionBuilder builder = Session.builder(defaultSession);
            this.defaultSession = amendSession.apply(builder).build();
            return this;
        }

        public Builder setNodeCount(int nodeCount)
        {
            this.nodeCount = nodeCount;
            return this;
        }

        public Builder setCoordinatorCount(int coordinatorCount)
        {
            this.coordinatorCount = coordinatorCount;
            return this;
        }

        public Builder setExtraProperties(Map<String, String> extraProperties)
        {
            this.extraProperties = extraProperties;
            return this;
        }

        /**
         * Sets extra properties being equal to a map containing given key and value.
         * Note, that calling this method OVERWRITES previously set property values.
         * As a result, it should only be used when only one extra property needs to be set.
         */
        public Builder setSingleExtraProperty(String key, String value)
        {
            return setExtraProperties(ImmutableMap.of(key, value));
        }

        public Builder setCoordinatorProperties(Map<String, String> coordinatorProperties)
        {
            this.coordinatorProperties = coordinatorProperties;
            return this;
        }

        public Builder setResourceManagerProperties(Map<String, String> resourceManagerProperties)
        {
            this.resourceManagerProperties = resourceManagerProperties;
            return this;
        }

        public Builder setCatalogServerProperties(Map<String, String> catalogServerProperties)
        {
            this.catalogServerProperties = catalogServerProperties;
            return this;
        }

        public Builder setCoordinatorSidecarProperties(Map<String, String> coordinatorSidecarProperties)
        {
            this.coordinatorSidecarProperties = coordinatorSidecarProperties;
            return this;
        }

        /**
         * Sets coordinator properties being equal to a map containing given key and value.
         * Note, that calling this method OVERWRITES previously set property values.
         * As a result, it should only be used when only one coordinator property needs to be set.
         */
        public Builder setSingleCoordinatorProperty(String key, String value)
        {
            return setCoordinatorProperties(ImmutableMap.of(key, value));
        }

        public Builder setParserOptions(SqlParserOptions parserOptions)
        {
            this.parserOptions = parserOptions;
            return this;
        }

        public Builder setEnvironment(String environment)
        {
            this.environment = environment;
            return this;
        }

        public Builder setDataDirectory(Optional<Path> dataDirectory)
        {
            this.dataDirectory = requireNonNull(dataDirectory, "dataDirectory is null");
            return this;
        }

        public Builder setExternalWorkerLauncher(Optional<BiFunction<Integer, URI, Process>> externalWorkerLauncher)
        {
            this.externalWorkerLauncher = requireNonNull(externalWorkerLauncher, "externalWorkerLauncher is null");
            return this;
        }

        public Builder setResourceManagerEnabled(boolean resourceManagerEnabled)
        {
            this.resourceManagerEnabled = resourceManagerEnabled;
            return this;
        }

        public Builder setCatalogServerEnabled(boolean catalogServerEnabled)
        {
            this.catalogServerEnabled = catalogServerEnabled;
            return this;
        }

        public Builder setCoordinatorSidecarEnabled(boolean coordinatorSidecarEnabled)
        {
            this.coordinatorSidecarEnabled = coordinatorSidecarEnabled;
            return this;
        }

        public Builder setExtraModules(List<Module> extraModules)
        {
            this.extraModules = extraModules;
            return this;
        }

        public Builder setResourceManagerCount(int resourceManagerCount)
        {
            this.resourceManagerCount = resourceManagerCount;
            return this;
        }

        public Builder setSkipLoadingResourceGroupConfigurationManager(boolean skipLoadingResourceGroupConfigurationManager)
        {
            this.skipLoadingResourceGroupConfigurationManager = skipLoadingResourceGroupConfigurationManager;
            return this;
        }

        public Builder setAccessControlProperties(Map<String, String> accessControlProperties)
        {
            this.accessControlProperties = accessControlProperties;
            return this;
        }

        public DistributedQueryRunner build()
                throws Exception
        {
            return new DistributedQueryRunner(
                    resourceManagerEnabled,
                    catalogServerEnabled,
                    coordinatorSidecarEnabled,
                    skipLoadingResourceGroupConfigurationManager,
                    defaultSession,
                    nodeCount,
                    coordinatorCount,
                    resourceManagerCount,
                    extraProperties,
                    coordinatorProperties,
                    resourceManagerProperties,
                    catalogServerProperties,
                    coordinatorSidecarProperties,
                    parserOptions,
                    environment,
                    dataDirectory,
                    externalWorkerLauncher,
                    extraModules,
                    accessControlProperties);
        }
    }
}
