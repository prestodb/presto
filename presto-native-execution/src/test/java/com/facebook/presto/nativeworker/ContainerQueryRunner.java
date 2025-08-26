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
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.logging.Logger;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;

public class ContainerQueryRunner
        implements QueryRunner
{
    protected static final Network network = Network.newNetwork();
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

    protected final int coordinatorPort;
    protected final String catalog;
    protected final String schema;
    protected int functionServerPort;
    protected Connection connection;

    public ContainerQueryRunner()
            throws InterruptedException, IOException
    {
        this(DEFAULT_COORDINATOR_PORT, TPCH_CATALOG, TINY_SCHEMA, DEFAULT_NUMBER_OF_WORKERS, DEFAULT_FUNCTION_SERVER_PORT);
    }

    public ContainerQueryRunner(int coordinatorPort, String catalog, String schema, int numberOfWorkers, int functionServerPort)
            throws InterruptedException, IOException
    {
        this.coordinatorPort = coordinatorPort;
        this.catalog = catalog;
        this.schema = schema;
        this.functionServerPort = functionServerPort;

        this.coordinator = createCoordinator();
        for (int i = 0; i < numberOfWorkers; i++) {
            workers.add(createNativeWorker(7777 + i, "native-worker-" + i));
        }

        coordinator.start();
        workers.forEach(GenericContainer::start);

        TimeUnit.SECONDS.sleep(5);

        String dockerHostIp = coordinator.getHost();
        logger.info("Presto UI is accessible at http://" + dockerHostIp + ":" + coordinator.getMappedPort(coordinatorPort));

        String url = String.format("jdbc:presto://%s:%s/%s/%s?%s",
                dockerHostIp,
                coordinator.getMappedPort(coordinatorPort),
                catalog,
                schema,
                "timeZoneId=UTC");

        postStartContainers(url);
    }

    protected void postStartContainers(String url)
    {
        try {
            this.connection = DriverManager.getConnection(url, "test", null);
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }

        // Delete the temporary files once the containers are started.
        ContainerQueryRunnerUtils.deleteDirectory(BASE_DIR + "/testcontainers/coordinator");
        for (GenericContainer<?> worker : workers) {
            String alias = worker.getNetworkAliases().get(1);
            ContainerQueryRunnerUtils.deleteDirectory(BASE_DIR + "/testcontainers/" + alias);
        }
    }

    protected GenericContainer<?> createCoordinator()
            throws IOException
    {
        ContainerQueryRunnerUtils.createCoordinatorTpchProperties();
        ContainerQueryRunnerUtils.createCoordinatorTpcdsProperties();
        ContainerQueryRunnerUtils.createCoordinatorConfigProperties(coordinatorPort);
        ContainerQueryRunnerUtils.createCoordinatorJvmConfig();
        ContainerQueryRunnerUtils.createCoordinatorLogProperties();
        ContainerQueryRunnerUtils.createCoordinatorNodeProperties();
        ContainerQueryRunnerUtils.createCoordinatorEntryPointScript();

        return new GenericContainer<>(PRESTO_COORDINATOR_IMAGE)
                .withExposedPorts(coordinatorPort)
                .withNetwork(network)
                .withNetworkAliases("presto-coordinator")
                .withCopyFileToContainer(MountableFile.forHostPath(BASE_DIR + "/testcontainers/coordinator/etc"), "/opt/presto-server/etc")
                .withCopyFileToContainer(MountableFile.forHostPath(BASE_DIR + "/testcontainers/coordinator/entrypoint.sh"), "/opt/entrypoint.sh")
                .waitingFor(Wait.forLogMessage(".*======== SERVER STARTED ========.*", 1))
                .withStartupTimeout(Duration.ofSeconds(Long.parseLong(CONTAINER_TIMEOUT)));
    }

    protected GenericContainer<?> createNativeWorker(int port, String nodeId)
            throws IOException
    {
        ContainerQueryRunnerUtils.createNativeWorkerConfigProperties(coordinatorPort, nodeId);
        ContainerQueryRunnerUtils.createNativeWorkerTpchProperties(nodeId);
        ContainerQueryRunnerUtils.createNativeWorkerEntryPointScript(nodeId);
        ContainerQueryRunnerUtils.createNativeWorkerNodeProperties(nodeId);
        return new GenericContainer<>(PRESTO_WORKER_IMAGE)
                .withExposedPorts(port)
                .withNetwork(network)
                .withNetworkAliases(nodeId)
                .withCopyFileToContainer(MountableFile.forHostPath(BASE_DIR + "/testcontainers/" + nodeId + "/etc"), "/opt/presto-server/etc")
                .withCopyFileToContainer(MountableFile.forHostPath(BASE_DIR + "/testcontainers/" + nodeId + "/entrypoint.sh"), "/opt/entrypoint.sh")
                .waitingFor(Wait.forLogMessage(".*Announcement succeeded: HTTP 202.*", 1));
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
                    Type targetType = resultTypes.get(i);
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
            throw new RuntimeException("Error executing query: " + sql, e);
        }
    }
}
