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

package com.facebook.presto.nativeworker;

import com.facebook.airlift.http.client.HttpClient;
import com.facebook.airlift.http.client.HttpClientConfig;
import com.facebook.airlift.http.client.Request;
import com.facebook.airlift.http.client.jetty.JettyHttpClient;
import com.facebook.presto.Session;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.cost.StatsCalculator;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.eventlistener.EventListener;
import com.facebook.presto.split.PageSourceManager;
import com.facebook.presto.split.SplitManager;
import com.facebook.presto.sql.expressions.ExpressionOptimizerManager;
import com.facebook.presto.sql.planner.ConnectorPlanOptimizerManager;
import com.facebook.presto.sql.planner.NodePartitioningManager;
import com.facebook.presto.sql.planner.sanity.PlanCheckerProviderManager;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.testing.TestingAccessControlManager;
import com.facebook.presto.transaction.TransactionManager;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.net.URI;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.logging.Logger;

import static com.facebook.airlift.http.client.Request.Builder.prepareGet;
import static com.facebook.airlift.http.client.StringResponseHandler.StringResponse;
import static com.facebook.airlift.http.client.StringResponseHandler.createStringResponseHandler;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;

public class ContainerQueryRunner
        implements QueryRunner
{
    protected static final Network network = Network.newNetwork();
    protected static final Network networkExpected = Network.newNetwork();
    protected static final String PRESTO_COORDINATOR_IMAGE = System.getProperty("coordinatorImage", "presto-coordinator:latest");
    protected static final String PRESTO_WORKER_IMAGE = System.getProperty("workerImage", "presto-worker:latest");
    protected static final String CONTAINER_TIMEOUT = System.getProperty("containerTimeout", "120");
    protected static final String CLUSTER_SHUTDOWN_TIMEOUT = System.getProperty("clusterShutDownTimeout", "10");
    protected static final String BASE_DIR = System.getProperty("user.dir");
    protected static final int DEFAULT_COORDINATOR_PORT = 8080;
    protected static final int DEFAULT_FUNCTION_SERVER_PORT = 1122;
    protected static final String TPCH_CATALOG = "tpch";
    protected static final String TINY_SCHEMA = "tiny";
    protected static final int DEFAULT_NUMBER_OF_WORKERS = 4;
    protected static final Logger logger = Logger.getLogger(ContainerQueryRunner.class.getName());
    protected final GenericContainer<?> coordinator;
    protected final List<GenericContainer<?>> workers = new ArrayList<>();
    protected final Optional<GenericContainer<?>> sidecar;
    protected final int coordinatorPort;
    protected final String catalog;
    protected final String schema;
    protected final int numberOfWorkers;
    protected GenericContainer<?> functionServer;
    protected int functionServerPort;
    protected boolean enableFunctionServer;
    protected Connection connection;

    public ContainerQueryRunner()
            throws InterruptedException, IOException
    {
        this(DEFAULT_COORDINATOR_PORT, TPCH_CATALOG, TINY_SCHEMA, DEFAULT_NUMBER_OF_WORKERS, DEFAULT_FUNCTION_SERVER_PORT, false);
    }

    public ContainerQueryRunner(int coordinatorPort, String catalog, String schema, int numberOfWorkers, int functionServerPort, boolean enableFunctionServer)
            throws InterruptedException, IOException
    {
        this.coordinatorPort = coordinatorPort;
        this.catalog = catalog;
        this.schema = schema;
        this.functionServerPort = functionServerPort;
        this.enableFunctionServer = enableFunctionServer;
        this.numberOfWorkers = numberOfWorkers;
        this.sidecar = Optional.empty();

        // Start function server first if enabled
        if (enableFunctionServer) {
            this.functionServer = createFunctionServer();
            this.functionServer.start();
            logger.info("Presto function server is deployed at http://" + functionServer.getHost() + ":" + functionServer.getMappedPort(functionServerPort));
        }

        this.coordinator = createCoordinator();
        coordinator.start();
        startWorkers(numberOfWorkers, true, false);

        startCoordinatorAndLogUI();
        initializeConnection();
        cleanupDirectories(numberOfWorkers, true, false, enableFunctionServer);
    }

    public ContainerQueryRunner(int numberOfWorkers, boolean isNativeCluster, boolean isSidecarEnabled, int functionServerPort, boolean enableFunctionServer)
            throws IOException, InterruptedException
    {
        this.coordinatorPort = DEFAULT_COORDINATOR_PORT;
        this.catalog = TPCH_CATALOG;
        this.schema = TINY_SCHEMA;
        this.numberOfWorkers = numberOfWorkers;

        // Start function server first if enabled
        if (enableFunctionServer) {
            this.functionServer = createFunctionServer();
            this.functionServer.start();
            logger.info("Presto function server is deployed at http://" + functionServer.getHost() + ":" + functionServer.getMappedPort(functionServerPort));
        }

        this.coordinator = createCoordinator(isNativeCluster, isSidecarEnabled);
        coordinator.start();
        startWorkers(numberOfWorkers, isNativeCluster, isSidecarEnabled);

        TimeUnit.SECONDS.sleep(5);

        if (isSidecarEnabled) {
            GenericContainer<?> sidecarContainer = createSidecar(7777 + numberOfWorkers, "sidecar", isNativeCluster);
            sidecarContainer.start();
            this.sidecar = Optional.of(sidecarContainer);
            // First, wait for coordinator to become ACTIVE
            waitForCoordinatorActive(coordinator.getHost(), coordinator.getMappedPort(coordinatorPort), 60, 5);
        }
        else {
            this.sidecar = Optional.empty();
        }

        startCoordinatorAndLogUI();
        initializeConnection();
        cleanupDirectories(numberOfWorkers, isNativeCluster, isSidecarEnabled);
    }

    private void startWorkers(int numberOfWorkers, boolean isNativeCluster, boolean isSidecarEnabled)
            throws InterruptedException, IOException
    {
        ContainerQueryRunnerUtils.deleteDirectory(BASE_DIR + "/testcontainers/coordinator");

        if (isNativeCluster) {
            for (int i = 0; i < numberOfWorkers; i++) {
                workers.add(createNativeWorker(7777 + i, "native-worker-" + i, isSidecarEnabled, false));
            }
        }
        else {
            for (int i = 0; i < numberOfWorkers; i++) {
                workers.add(createJavaWorker(7777 + i, "java-worker-" + i));
            }
        }

        workers.forEach(GenericContainer::start);
    }

    private void cleanupDirectories(int numberOfWorkers, boolean isNativeCluster, boolean isSidecarEnabled, boolean isFunctionServerEnabled)
    {
        for (int i = 0; i < numberOfWorkers; i++) {
            String workerType = isNativeCluster ? "native-worker-" : "java-worker-";
            ContainerQueryRunnerUtils.deleteDirectory(BASE_DIR + "/testcontainers/" + workerType + i);
        }

        if (isSidecarEnabled) {
            ContainerQueryRunnerUtils.deleteDirectory(BASE_DIR + "/testcontainers/sidecar");
        }

        if (isFunctionServerEnabled) {
            ContainerQueryRunnerUtils.deleteDirectory(BASE_DIR + "/testcontainers/function-server");
        }
    }

    private void startCoordinatorAndLogUI()
    {
        String dockerHostIp = coordinator.getHost();
        logger.info("Presto UI is accessible at http://" + dockerHostIp + ":" + coordinator.getMappedPort(coordinatorPort));
    }

    private void initializeConnection()
    {
        String dockerHostIp = coordinator.getHost();
        int mappedPort = coordinator.getMappedPort(coordinatorPort);

        String url = String.format("jdbc:presto://%s:%s/%s/%s?%s",
                dockerHostIp,
                mappedPort,
                catalog,
                schema,
                enableFunctionServer ? "timeZoneId=UTC&sessionProperties=remote_functions_enabled:true" : "timeZoneId=UTC");

        try {
            this.connection = DriverManager.getConnection(url, "test", null);
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void waitForCoordinatorActive(String host, int port, int maxRetries, int retryDelaySeconds)
    {
        String endpoint = String.format("http://%s:%d/v1/info/state", host, port);

        HttpClientConfig config = new HttpClientConfig()
                .setConnectTimeout(new com.facebook.airlift.units.Duration(2, TimeUnit.SECONDS))
                .setRequestTimeout(new com.facebook.airlift.units.Duration(2, TimeUnit.SECONDS));

        HttpClient httpClient = new JettyHttpClient(config);

        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                Request request = prepareGet().setUri(URI.create(endpoint)).build();

                StringResponse response =
                        httpClient.execute(request, createStringResponseHandler());

                if (response.getStatusCode() == 200) {
                    String body = response.getBody().trim().replaceAll("^\"|\"$", "");
                    logger.info(String.format("Attempt %d: State = %s", attempt, body));

                    if ("ACTIVE".equalsIgnoreCase(body)) {
                        logger.info("Coordinator is ACTIVE.");
                        return;
                    }
                } else {
                    logger.warning(String.format("Attempt %d: Non-200 response: %d%n", attempt, response.getStatusCode()));
                }
            } catch (Exception e) {
                logger.severe(String.format("Attempt %d: Error contacting coordinator: %s%n", attempt, e.getMessage()));
            }


            try {
                Thread.sleep(retryDelaySeconds * 1000L);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while waiting for coordinator to become ACTIVE");
            }
        }

        throw new RuntimeException("Coordinator did not become ACTIVE in time.");
    }

    protected GenericContainer<?> createCoordinator()
            throws IOException
    {
        return createCoordinator(true, false);
    }
    protected GenericContainer<?> createCoordinator(boolean isNativeCluster, boolean isSidecarEnabled)
            throws IOException
    {
        ContainerQueryRunnerUtils.createCoordinatorTpchProperties();
        ContainerQueryRunnerUtils.createCoordinatorTpcdsProperties();
        ContainerQueryRunnerUtils.createCoordinatorConfigProperties(coordinatorPort, isNativeCluster, isSidecarEnabled);
        if (isSidecarEnabled && isNativeCluster) {
            ContainerQueryRunnerUtils.createCoordinatorSidecarProperties();
        }
        ContainerQueryRunnerUtils.createCoordinatorJvmConfig();
        ContainerQueryRunnerUtils.createCoordinatorLogProperties();
        ContainerQueryRunnerUtils.createCoordinatorNodeProperties();
        ContainerQueryRunnerUtils.createCoordinatorEntryPointScript(); // Never run function server in coordinator
        if (enableFunctionServer) {
            ContainerQueryRunnerUtils.createRestRemoteProperties(functionServerPort);
        }

        return new GenericContainer<>(PRESTO_COORDINATOR_IMAGE)
                .withNetwork(isNativeCluster ? network : networkExpected)
                .withNetworkAliases("presto-coordinator")
                .withCopyFileToContainer(MountableFile.forHostPath(BASE_DIR + "/testcontainers/coordinator/etc"), "/opt/presto-server/etc")
                .withCopyFileToContainer(MountableFile.forHostPath(BASE_DIR + "/testcontainers/coordinator/entrypoint.sh"), "/opt/entrypoint.sh")
                .waitingFor(Wait.forLogMessage(".*======== SERVER STARTED ========.*", 1))
                .withStartupTimeout(Duration.ofSeconds(Long.parseLong(CONTAINER_TIMEOUT)))
                .withExposedPorts(coordinatorPort);
    }
    protected GenericContainer<?> createJavaWorker(int port, String nodeId)
        throws IOException
    {
        ContainerQueryRunnerUtils.createJavaWorkerConfigProperties(port, coordinatorPort, nodeId);
        ContainerQueryRunnerUtils.createNativeWorkerTpchProperties(nodeId);
        ContainerQueryRunnerUtils.createJavaEntryPointScript(nodeId);
        ContainerQueryRunnerUtils.createNativeWorkerNodeProperties(nodeId);
        return new GenericContainer<>(PRESTO_COORDINATOR_IMAGE)
                .withExposedPorts(port)
                .withNetwork(networkExpected)
                .withNetworkAliases(nodeId)
                .withCopyFileToContainer(MountableFile.forHostPath(BASE_DIR + "/testcontainers/" + nodeId + "/etc"), "/opt/presto-server/etc")
                .withCopyFileToContainer(MountableFile.forHostPath(BASE_DIR + "/testcontainers/" + nodeId + "/entrypoint.sh"), "/opt/entrypoint.sh");
    }

    protected GenericContainer<?> createSidecar(int port, String nodeId, boolean isNativeCluster)
        throws IOException
    {
        return createNativeWorker(port, nodeId, isNativeCluster, true, true);
    }

    private GenericContainer<?> createNativeWorker(int port, String nodeId, boolean isSidecarEnabled, boolean isSidecarNode)
            throws IOException
    {
        return createNativeWorker(port, nodeId, true, isSidecarEnabled, isSidecarNode);
    }

    private GenericContainer<?> createNativeWorker(int port, String nodeId, boolean isNativeCluster, boolean isSidecarEnabled, boolean isSidecarNode)
            throws IOException
    {
        ContainerQueryRunnerUtils.createNativeWorkerConfigProperties(coordinatorPort, functionServerPort, nodeId, isSidecarEnabled, isSidecarNode);
        if (!isSidecarEnabled) {
            ContainerQueryRunnerUtils.createNativeWorkerTpchProperties(nodeId);
        }
        ContainerQueryRunnerUtils.createNativeWorkerEntryPointScript(nodeId);
        ContainerQueryRunnerUtils.createNativeWorkerNodeProperties(nodeId);
        return new GenericContainer<>(PRESTO_WORKER_IMAGE)
                .withExposedPorts(port)
                .withNetwork(isNativeCluster ? network : networkExpected)
                .withNetworkAliases(nodeId)
                .withCopyFileToContainer(MountableFile.forHostPath(BASE_DIR + "/testcontainers/" + nodeId + "/etc"), "/opt/presto-server/etc")
                .withCopyFileToContainer(MountableFile.forHostPath(BASE_DIR + "/testcontainers/" + nodeId + "/entrypoint.sh"), "/opt/entrypoint.sh")
                .waitingFor(Wait.forLogMessage(".*Announcement succeeded: HTTP 202.*", 1));
    }

    protected GenericContainer<?> createFunctionServer()
            throws IOException
    {
        ContainerQueryRunnerUtils.createFunctionServerConfigProperties(functionServerPort);
        ContainerQueryRunnerUtils.createFunctionServerEntryPointScript();

        // Reuse the coordinator image since it already contains the function server jar
        return new GenericContainer<>(PRESTO_COORDINATOR_IMAGE)
                .withNetwork(network)
                .withNetworkAliases("presto-remote-function-server")
                .withCopyFileToContainer(MountableFile.forHostPath(BASE_DIR + "/testcontainers/function-server/etc"), "/opt/function-server/etc")
                .withCopyFileToContainer(MountableFile.forHostPath(BASE_DIR + "/testcontainers/function-server/entrypoint.sh"), "/opt/entrypoint.sh")
                .waitingFor(Wait.forLogMessage(".*======== REMOTE FUNCTION SERVER STARTED at: .*", 1))
                .withStartupTimeout(Duration.ofSeconds(Long.parseLong(CONTAINER_TIMEOUT)))
                .withExposedPorts(functionServerPort);
    }

    @Override
    public void close()
    {
        try {
            TimeUnit.SECONDS.sleep(Long.parseLong(CLUSTER_SHUTDOWN_TIMEOUT));
        }
        catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        coordinator.stop();
        workers.forEach(GenericContainer::stop);
        sidecar.ifPresent(GenericContainer::stop);
        if (functionServer != null) {
            functionServer.stop();
        }
    }

    @Override
    public TransactionManager getTransactionManager()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Metadata getMetadata()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public SplitManager getSplitManager()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public PageSourceManager getPageSourceManager()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public NodePartitioningManager getNodePartitioningManager()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ConnectorPlanOptimizerManager getPlanOptimizerManager()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public PlanCheckerProviderManager getPlanCheckerProviderManager()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public StatsCalculator getStatsCalculator()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<EventListener> getEventListeners()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public TestingAccessControlManager getAccessControl()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ExpressionOptimizerManager getExpressionManager()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public MaterializedResult execute(String sql)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public MaterializedResult execute(Session session, String sql, List<? extends Type> resultTypes)
    {
        // Added logic similar to H2QueryRunner.
        try {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(sql);
            MaterializedResult rawResult = ContainerQueryRunnerUtils.toMaterializedResult(resultSet);

            // Coerce the raw result to the requested resultTypes
            List<MaterializedRow> coercedRows = new ArrayList<>();
            for (MaterializedRow row : rawResult.getMaterializedRows()) {
                List<Object> coercedValues = new ArrayList<>();
                for (int i = 0; i < resultTypes.size(); i++) {
                    Object value = row.getField(i);
                    coercedValues.add(value);
                }
                coercedRows.add(new MaterializedRow(MaterializedResult.DEFAULT_PRECISION, coercedValues));
            }
            return new MaterializedResult(coercedRows, resultTypes);
        }
        catch (SQLException e) {
            throw new RuntimeException("Error executing query: " + sql, e);
        }
    }

    @Override
    public List<QualifiedObjectName> listTables(Session session, String catalog, String schema)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean tableExists(Session session, String table)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void installPlugin(Plugin plugin)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createCatalog(String catalogName, String connectorName, Map<String, String> properties)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void loadFunctionNamespaceManager(String functionNamespaceManagerName, String catalogName, Map<String, String> properties)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Lock getExclusiveLock()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getNodeCount()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Session getDefaultSession()
    {
        return testSessionBuilder()
                .setCatalog(catalog)
                .setSchema(schema)
                .build();
    }

    @Override
    public MaterializedResult execute(Session session, String sql)
    {
        try {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(sql);
            return ContainerQueryRunnerUtils.toMaterializedResult(resultSet);
        }
        catch (SQLException e) {
            throw new RuntimeException("Error executing query: " + sql + "\n" + e.getMessage());
        }
    }
}
