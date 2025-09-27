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

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.time.Duration;

public class ContainerQueryRunnerWithFunctionServer
        extends ContainerQueryRunner
{
    public ContainerQueryRunnerWithFunctionServer()
            throws InterruptedException, IOException
    {
        this(DEFAULT_COORDINATOR_PORT, TPCH_CATALOG, TINY_SCHEMA, DEFAULT_NUMBER_OF_WORKERS, DEFAULT_FUNCTION_SERVER_PORT);
    }

    public ContainerQueryRunnerWithFunctionServer(
            int coordinatorPort,
            String catalog,
            String schema,
            int numberOfWorkers,
            int functionServerPort)
            throws InterruptedException, IOException
    {
        super(coordinatorPort, catalog, schema, numberOfWorkers, functionServerPort);

        String url = String.format("jdbc:presto://%s:%s/%s/%s?%s",
                coordinator.getHost(),
                coordinator.getMappedPort(coordinatorPort),
                catalog,
                schema,
                "timeZoneId=UTC&sessionProperties=remote_functions_enabled:true");

        postStartContainers(url);
    }

    @Override
    protected GenericContainer<?> createCoordinator()
            throws IOException
    {
        ContainerQueryRunnerUtils.createCoordinatorTpcdsProperties();
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
                .withNetwork(network)
                .withNetworkAliases("presto-coordinator")
                .withCopyFileToContainer(MountableFile.forHostPath(BASE_DIR + "/testcontainers/coordinator/etc"), "/opt/presto-server/etc")
                .withCopyFileToContainer(MountableFile.forHostPath(BASE_DIR + "/testcontainers/coordinator/etc/function-server/etc"), "/opt/function-server/etc")
                .withCopyFileToContainer(MountableFile.forHostPath(BASE_DIR + "/testcontainers/coordinator/entrypoint.sh"), "/opt/entrypoint.sh")
                .waitingFor(Wait.forLogMessage(".*======== SERVER STARTED ========.*", 1))
                .withStartupTimeout(Duration.ofSeconds(Long.parseLong(CONTAINER_TIMEOUT)));
    }
}
