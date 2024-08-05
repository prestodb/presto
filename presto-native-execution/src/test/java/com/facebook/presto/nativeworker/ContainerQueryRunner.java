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
import com.facebook.presto.sql.planner.ConnectorPlanOptimizerManager;
import com.facebook.presto.sql.planner.NodePartitioningManager;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.testing.TestingAccessControlManager;
import com.facebook.presto.transaction.TransactionManager;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.logging.Logger;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static org.testng.Assert.fail;

public class ContainerQueryRunner
        implements QueryRunner
{
    private static final Network network = Network.newNetwork();
    private static final String PRESTO_COORDINATOR_IMAGE = System.getProperty("coordinatorImage", "presto-coordinator:latest");
    private static final String PRESTO_WORKER_IMAGE = System.getProperty("workerImage", "presto-worker:latest");
    private static final String CONTAINER_TIMEOUT = System.getProperty("containerTimeout", "120");
    private static final String CLUSTER_SHUTDOWN_TIMEOUT = System.getProperty("clusterShutDownTimeout", "10");
    private static final String BASE_DIR = System.getProperty("user.dir");
    private static final int DEFAULT_COORDINATOR_PORT = 8080;
    private static final String TPCH_CATALOG = "tpch";
    private static final String TINY_SCHEMA = "tiny";
    private static final int DEFAULT_NUMBER_OF_WORKERS = 4;
    private final GenericContainer<?> coordinator;
    private final List<GenericContainer<?>> workers = new ArrayList<>();
    private final int coordinatorPort;
    private final String catalog;
    private final String schema;
    private final int numberOfWorkers;
    private static Logger logger = Logger.getLogger(ContainerQueryRunner.class.getName());

    public ContainerQueryRunner()
            throws InterruptedException, IOException
    {
        this(DEFAULT_COORDINATOR_PORT, TPCH_CATALOG, TINY_SCHEMA, DEFAULT_NUMBER_OF_WORKERS);
    }

    public ContainerQueryRunner(int coordinatorPort, String catalog, String schema, int numberOfWorkers)
            throws InterruptedException, IOException
    {
        this.coordinatorPort = coordinatorPort;
        this.catalog = catalog;
        this.schema = schema;
        this.numberOfWorkers = numberOfWorkers;

        // The container details can be added as properties in VM options for testing in IntelliJ.
        coordinator = createCoordinator();
        for (int i = 0; i < numberOfWorkers; i++) {
            workers.add(createNativeWorker(7777 + i, "native-worker-" + i));
        }

        coordinator.start();
        workers.forEach(GenericContainer::start);

        logger.info("Presto UI is accessible at http://localhost:" + coordinator.getMappedPort(coordinatorPort));
        TimeUnit.SECONDS.sleep(5);
    }

    private GenericContainer<?> createCoordinator()
            throws IOException
    {
        ContainerQueryRunnerUtils.createCoordinatorTpchProperties();
        ContainerQueryRunnerUtils.createCoordinatorConfigProperties(coordinatorPort);
        ContainerQueryRunnerUtils.createCoordinatorJvmConfig();
        ContainerQueryRunnerUtils.createCoordinatorLogProperties();
        ContainerQueryRunnerUtils.createCoordinatorNodeProperties();
        ContainerQueryRunnerUtils.createCoordinatorEntryPointScript();

        return new GenericContainer<>(PRESTO_COORDINATOR_IMAGE)
                .withExposedPorts(coordinatorPort)
                .withNetwork(network).withNetworkAliases("presto-coordinator")
                .withFileSystemBind(BASE_DIR + "/testcontainers/coordinator/etc", "/opt/presto-server/etc", BindMode.READ_WRITE)
                .withFileSystemBind(BASE_DIR + "/testcontainers/coordinator/entrypoint.sh", "/opt/entrypoint.sh", BindMode.READ_ONLY)
                .waitingFor(Wait.forLogMessage(".*======== SERVER STARTED ========.*", 1))
                .withStartupTimeout(Duration.ofSeconds(Long.parseLong(CONTAINER_TIMEOUT)));
    }

    private GenericContainer<?> createNativeWorker(int port, String nodeId)
            throws IOException
    {
        ContainerQueryRunnerUtils.createNativeWorkerConfigProperties(coordinatorPort, nodeId);
        ContainerQueryRunnerUtils.createNativeWorkerTpchProperties(nodeId);
        ContainerQueryRunnerUtils.createNativeWorkerEntryPointScript(nodeId);
        ContainerQueryRunnerUtils.createNativeWorkerNodeProperties(nodeId);
        ContainerQueryRunnerUtils.createNativeWorkerVeloxProperties(nodeId);
        return new GenericContainer<>(PRESTO_WORKER_IMAGE)
                .withExposedPorts(port)
                .withNetwork(network).withNetworkAliases(nodeId)
                .withFileSystemBind(BASE_DIR + "/testcontainers/" + nodeId + "/etc", "/opt/presto-server/etc", BindMode.READ_ONLY)
                .withFileSystemBind(BASE_DIR + "/testcontainers/" + nodeId + "/entrypoint.sh", "/opt/entrypoint.sh", BindMode.READ_ONLY)
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
        ContainerQueryRunnerUtils.deleteDirectory(BASE_DIR + "/testcontainers/coordinator");
        for (int i = 0; i < numberOfWorkers; i++) {
            ContainerQueryRunnerUtils.deleteDirectory(BASE_DIR + "/testcontainers/native-worker-" + i);
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
    public StatsCalculator getStatsCalculator()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<EventListener> getEventListener()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public TestingAccessControlManager getAccessControl()
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
        throw new UnsupportedOperationException();
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
        String[] command = {
                "/opt/presto-cli",
                "--server",
                "presto-coordinator:" + coordinatorPort,
                "--catalog",
                catalog,
                "--schema",
                schema,
                "--execute",
                sql
        };

        Container.ExecResult execResult;
        try {
            execResult = coordinator.execInContainer(command);

            if (execResult.getExitCode() != 0) {
                String errorDetails = String.format("Stdout: %s%nStderr: %s", execResult.getStdout(), execResult.getStderr());
                fail("Presto CLI exited with error code: " + execResult.getExitCode() + "\n" + errorDetails);
            }

            return ContainerQueryRunnerUtils.toMaterializedResult(execResult.getStdout());
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
