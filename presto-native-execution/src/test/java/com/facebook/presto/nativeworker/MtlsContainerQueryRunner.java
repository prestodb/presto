package com.facebook.presto.nativeworker;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.time.Duration;

/**
 * A {@link ContainerQueryRunner} variant that configures the entire cluster
 * (Function Server, coordinator, native workers) with mTLS + JWT authentication.
 *
 * <p>Cert files are read from the presto-function-server test-jar classpath,
 * extracted to a temporary host directory, and then copied into each container
 * by Testcontainers at startup. No Docker image changes are required.
 *
 * <p>Container-internal cert path:  /opt/presto-server/certs/  (coordinator + workers)
 *                                   /opt/function-server/certs/ (function server)
 */
public class MtlsContainerQueryRunner
        extends ContainerQueryRunner
{
    public static final int DEFAULT_FUNCTION_SERVER_HTTPS_PORT = 8443;
    public static final String JWT_SHARED_SECRET = "supersecret";

    private static final String COORDINATOR_DIR = "coordinator-mtls";
    private static final String FUNCTION_SERVER_DIR = "function-server-mtls";

    public MtlsContainerQueryRunner()
            throws InterruptedException, IOException
    {
        super(DEFAULT_COORDINATOR_PORT, TPCH_CATALOG, TINY_SCHEMA, DEFAULT_NUMBER_OF_WORKERS,
                DEFAULT_FUNCTION_SERVER_HTTPS_PORT, true);
    }

    @Override
    protected GenericContainer<?> createFunctionServer()
            throws IOException
    {
        ContainerQueryRunnerUtils.extractCertsToHostDir();

        ContainerQueryRunnerUtils.createFunctionServerMtlsConfigProperties(
                DEFAULT_FUNCTION_SERVER_HTTPS_PORT, JWT_SHARED_SECRET);
        ContainerQueryRunnerUtils.createFunctionServerEntryPointScript();

        return new GenericContainer<>(PRESTO_COORDINATOR_IMAGE)
                .withNetwork(network)
                .withNetworkAliases("presto-remote-function-server")
                .withCopyFileToContainer(
                        MountableFile.forHostPath(BASE_DIR + "/testcontainers/" + FUNCTION_SERVER_DIR + "/etc"),
                        "/opt/function-server/etc")
                .withCopyFileToContainer(
                        MountableFile.forHostPath(BASE_DIR + "/testcontainers/certs"),
                        "/opt/function-server/certs")
                .withCopyFileToContainer(
                        MountableFile.forHostPath(BASE_DIR + "/testcontainers/" + FUNCTION_SERVER_DIR + "/entrypoint.sh"),
                        "/opt/entrypoint.sh")
                .waitingFor(Wait.forLogMessage(".*======== REMOTE FUNCTION SERVER STARTED at: .*", 1))
                .withStartupTimeout(Duration.ofSeconds(Long.parseLong(CONTAINER_TIMEOUT)))
                .withExposedPorts(DEFAULT_FUNCTION_SERVER_HTTPS_PORT);
    }

    @Override
    protected GenericContainer<?> createCoordinator()
            throws IOException
    {
        ContainerQueryRunnerUtils.createCoordinatorTpchProperties();
        ContainerQueryRunnerUtils.createCoordinatorTpcdsProperties();
        ContainerQueryRunnerUtils.createCoordinatorMtlsConfigProperties(
                coordinatorPort, JWT_SHARED_SECRET);
        ContainerQueryRunnerUtils.createRestRemoteMtlsProperties(DEFAULT_FUNCTION_SERVER_HTTPS_PORT);
        ContainerQueryRunnerUtils.createCoordinatorJvmConfig();
        ContainerQueryRunnerUtils.createCoordinatorLogProperties();
        ContainerQueryRunnerUtils.createCoordinatorNodeProperties();
        ContainerQueryRunnerUtils.createCoordinatorEntryPointScript();

        return new GenericContainer<>(PRESTO_COORDINATOR_IMAGE)
                .withNetwork(network)
                .withNetworkAliases("presto-coordinator")
                .withCopyFileToContainer(
                        MountableFile.forHostPath(BASE_DIR + "/testcontainers/" + COORDINATOR_DIR + "/etc"),
                        "/opt/presto-server/etc")
                .withCopyFileToContainer(
                        MountableFile.forHostPath(BASE_DIR + "/testcontainers/certs"),
                        "/opt/presto-server/certs")
                .withCopyFileToContainer(
                        MountableFile.forHostPath(BASE_DIR + "/testcontainers/" + COORDINATOR_DIR + "/entrypoint.sh"),
                        "/opt/entrypoint.sh")
                .waitingFor(Wait.forLogMessage(".*======== SERVER STARTED ========.*", 1))
                .withStartupTimeout(Duration.ofSeconds(Long.parseLong(CONTAINER_TIMEOUT)))
                .withExposedPorts(coordinatorPort);
    }

    @Override
    protected GenericContainer<?> createNativeWorker(int port, String nodeId)
            throws IOException
    {
        ContainerQueryRunnerUtils.createNativeWorkerMtlsConfigPropertiesWithFnServer(
                coordinatorPort, DEFAULT_FUNCTION_SERVER_HTTPS_PORT, nodeId, JWT_SHARED_SECRET);
        ContainerQueryRunnerUtils.createNativeWorkerTpchProperties(nodeId);
        ContainerQueryRunnerUtils.createNativeWorkerEntryPointScript(nodeId);
        ContainerQueryRunnerUtils.createNativeWorkerNodeProperties(nodeId);

        return new GenericContainer<>(PRESTO_WORKER_IMAGE)
                .withExposedPorts(port)
                .withNetwork(network)
                .withNetworkAliases(nodeId)
                .withCopyFileToContainer(
                        MountableFile.forHostPath(BASE_DIR + "/testcontainers/" + nodeId + "-mtls/etc"),
                        "/opt/presto-server/etc")
                .withCopyFileToContainer(
                        MountableFile.forHostPath(BASE_DIR + "/testcontainers/certs"),
                        "/opt/presto-server/certs")
                .withCopyFileToContainer(
                        MountableFile.forHostPath(BASE_DIR + "/testcontainers/" + nodeId + "-mtls/entrypoint.sh"),
                        "/opt/entrypoint.sh")
                .waitingFor(Wait.forLogMessage(".*Announcement succeeded: HTTP 202.*", 1));
    }
}