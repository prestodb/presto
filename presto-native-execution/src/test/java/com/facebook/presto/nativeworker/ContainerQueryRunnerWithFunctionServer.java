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
import com.facebook.presto.testing.MaterializedResult;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static java.sql.DriverManager.getConnection;

public class ContainerQueryRunnerWithFunctionServer
        extends ContainerQueryRunner
{
    private static final Network network = Network.newNetwork();
    private static final String PRESTO_COORDINATOR_IMAGE = System.getProperty("coordinatorImage", "presto-coordinator:latest");
    private static final String PRESTO_WORKER_IMAGE = System.getProperty("workerImage", "presto-worker:latest");
    private static final String CONTAINER_TIMEOUT = System.getProperty("containerTimeout", "120");
    private static final String CLUSTER_SHUTDOWN_TIMEOUT = System.getProperty("clusterShutDownTimeout", "10");
    private static final String BASE_DIR = System.getProperty("user.dir");
    private static final int DEFAULT_COORDINATOR_PORT = 8080;
    private static final int DEFAULT_FUNCTION_SERVER_PORT = 1122;
    private static final String TPCH_CATALOG = "tpch";
    private static final String TINY_SCHEMA = "tiny";
    private static final int DEFAULT_NUMBER_OF_WORKERS = 4;
    private static final Logger logger = Logger.getLogger(ContainerQueryRunner.class.getName());
    private final GenericContainer<?> coordinator;
    private final List<GenericContainer<?>> workers = new ArrayList<>();
    private final int coordinatorPort;
    private final int functionServerPort;
    private final String catalog;
    private final String schema;
    private final Connection connection;

    public ContainerQueryRunnerWithFunctionServer()
            throws InterruptedException, IOException
    {
        this(DEFAULT_COORDINATOR_PORT, DEFAULT_FUNCTION_SERVER_PORT, TPCH_CATALOG, TINY_SCHEMA, DEFAULT_NUMBER_OF_WORKERS);
    }

    public ContainerQueryRunnerWithFunctionServer(int coordinatorPort, int functionServerPort, String catalog, String schema, int numberOfWorkers)
            throws InterruptedException, IOException
    {
        this.coordinatorPort = coordinatorPort;
        this.functionServerPort = functionServerPort;
        this.catalog = catalog;
        this.schema = schema;

        // The container details can be added as properties in VM options for testing in IntelliJ.
        coordinator = createCoordinator();
        for (int i = 0; i < numberOfWorkers; i++) {
            workers.add(createNativeWorker(7777 + i, "native-worker-" + i));
        }

        coordinator.start();
        workers.forEach(GenericContainer::start);

        logger.info("Presto UI is accessible at http://localhost:" + coordinator.getMappedPort(coordinatorPort));

        TimeUnit.SECONDS.sleep(5);

        String url = String.format("jdbc:presto://localhost:%s/%s/%s?%s",
                coordinator.getMappedPort(coordinatorPort),
                catalog,
                schema,
                "timeZoneId=UTC");

        try {
            connection = getConnection(url, "test", null);
            Statement statement = connection.createStatement();
            statement.execute("set session remote_functions_enabled=true");
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }

        // Delete the temporary files once the containers are started.
        ContainerQueryRunnerUtils.deleteDirectory(BASE_DIR + "/testcontainers/coordinator");
        for (int i = 0; i < numberOfWorkers; i++) {
            ContainerQueryRunnerUtils.deleteDirectory(BASE_DIR + "/testcontainers/native-worker-" + i);
        }
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
        ContainerQueryRunnerUtils.createFunctionNamespaceRemotePropertiesWithFunctionServer(functionServerPort);
        ContainerQueryRunnerUtils.createFunctionServerConfigProperties(functionServerPort);

        return new GenericContainer<>(PRESTO_COORDINATOR_IMAGE)
                .withExposedPorts(coordinatorPort)
                .withNetwork(network).withNetworkAliases("presto-coordinator")
                .withFileSystemBind(BASE_DIR + "/testcontainers/coordinator/etc", "/opt/presto-server/etc", BindMode.READ_WRITE)
                .withFileSystemBind(BASE_DIR + "/testcontainers/coordinator/etc/function-server", "/opt/function-server/etc", BindMode.READ_ONLY)
                .withFileSystemBind(BASE_DIR + "/testcontainers/coordinator/entrypoint.sh", "/opt/entrypoint.sh", BindMode.READ_ONLY)
                .waitingFor(Wait.forLogMessage(".*======== SERVER STARTED ========.*", 1))
                .withStartupTimeout(Duration.ofSeconds(Long.parseLong(CONTAINER_TIMEOUT)));
    }

    private GenericContainer<?> createNativeWorker(int port, String nodeId)
            throws IOException
    {
        ContainerQueryRunnerUtils.createNativeWorkerConfigPropertiesWithFunctionServer(coordinatorPort, functionServerPort, nodeId);
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
