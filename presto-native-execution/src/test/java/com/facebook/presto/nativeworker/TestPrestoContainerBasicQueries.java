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

import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestPrestoContainerBasicQueries
{
    private static final String PRESTO_COORDINATOR_IMAGE = System.getProperty("coordinatorImage", "presto-coordinator:latest");
    private static final String PRESTO_WORKER_IMAGE = System.getProperty("workerImage", "presto-worker:latest");
    private static final String BASE_DIR = System.getProperty("user.dir");
    private static final String CONTAINER_TIMEOUT = System.getProperty("containerTimeout", "120");
    private static final Network network = Network.newNetwork();
    private GenericContainer<?> coordinator;
    private GenericContainer<?> worker;

    @BeforeClass
    public void setup()
            throws InterruptedException
    {
        // TODO: This framework is tested only in Ubuntu x86_64, as there is no support to run the native docker images in ARM based system,
        // Once this is fixed, the container details can be added as properties in VM options for testing in IntelliJ.
        coordinator = new GenericContainer<>(PRESTO_COORDINATOR_IMAGE)
                .withExposedPorts(8081)
                .withNetwork(network).withNetworkAliases("presto-coordinator")
                .withFileSystemBind(BASE_DIR + "/testcontainers/coordinator/etc", "/opt/presto-server/etc", BindMode.READ_WRITE)
                .withFileSystemBind(BASE_DIR + "/testcontainers/coordinator/entrypoint.sh", "/opt/entrypoint.sh", BindMode.READ_ONLY)
                .waitingFor(Wait.forLogMessage(".*======== SERVER STARTED ========.*", 1))
                .withStartupTimeout(Duration.ofSeconds(Long.parseLong(CONTAINER_TIMEOUT)));

        worker = new GenericContainer<>(PRESTO_WORKER_IMAGE)
                .withExposedPorts(7777)
                .withNetwork(network).withNetworkAliases("presto-worker")
                .withFileSystemBind(BASE_DIR + "/testcontainers/nativeworker/velox-etc", "/opt/presto-server/etc", BindMode.READ_ONLY)
                .withFileSystemBind(BASE_DIR + "/testcontainers/nativeworker/entrypoint.sh", "/opt/entrypoint.sh", BindMode.READ_ONLY)
                .waitingFor(Wait.forLogMessage(".*Announcement succeeded: HTTP 202.*", 1));

        coordinator.start();
        worker.start();

        // Wait for worker to announce itself.
        TimeUnit.SECONDS.sleep(5);
    }

    @AfterClass
    public void tearDown()
    {
        coordinator.stop();
        worker.stop();
    }

    private Container.ExecResult executeQuery(String sql)
            throws IOException, InterruptedException
    {
        // Command to run inside the coordinator container using the presto-cli.
        String[] command = {
                "/opt/presto-cli",
                "--server",
                "presto-coordinator:8081",
                "--catalog",
                "tpch",
                "--schema",
                "tiny",
                "--execute",
                sql
        };

        Container.ExecResult execResult = coordinator.execInContainer(command);
        if (execResult.getExitCode() != 0) {
            String errorDetails = "Stdout: " + execResult.getStdout() + "\nStderr: " + execResult.getStderr();
            fail("Presto CLI exited with error code: " + execResult.getExitCode() + "\n" + errorDetails);
        }
        return execResult;
    }

    @Test
    public void testBasics()
            throws IOException, InterruptedException
    {
        String selectRuntimeNodes = "select * from system.runtime.nodes";
        Container.ExecResult nodes = executeQuery(selectRuntimeNodes);
        assertTrue(nodes.getStdout().contains("native-worker-1"), "Native worker is not present.");

        String showCatalogs = "show catalogs";
        Container.ExecResult catalogs = executeQuery(showCatalogs);
        assertTrue(catalogs.getStdout().contains("tpch"), "tpch is not present.");

        String showSession = "show session";
        Container.ExecResult sessionProperties = executeQuery(showSession);
        assertTrue(sessionProperties.getStdout().contains("native_aggregation_spill_all"), "native_aggregation_spill_all is not present.");
    }

    @Test
    public void testFunctions()
            throws IOException, InterruptedException
    {
        String countValues = "SELECT COUNT(*) FROM (VALUES 1, 0, 0, 2, 3, 3) as t(x)";
        Container.ExecResult countResult = executeQuery(countValues);
        assertTrue(countResult.getStdout().contains("6"), "Count is incorrect.");

        String sqlArrayIntegers = "SELECT array_sort(ARRAY [5, 20, null, 5, 3, 50])";
        Container.ExecResult execResultIntegers = executeQuery(sqlArrayIntegers);
        assertTrue(execResultIntegers.getStdout().contains("[3, 5, 5, 20, 50, null]"), "Integer array not sorted correctly.");

        String countOrders = "SELECT COUNT(*) FROM orders";
        Container.ExecResult count = executeQuery(countOrders);
        assertTrue(count.getStdout().contains("15000"), "Incorrect count.");

        String totalDiscount = "SELECT SUM(l_discount) FROM lineitem WHERE l_discount != 0.04";
        Container.ExecResult total = executeQuery(totalDiscount);
        assertTrue(total.getStdout().contains("2786.779999999882"), "Incorrect total discount.");

        String joinOrders = "SELECT c.c_name, c.c_address, o.o_orderdate FROM customer c JOIN orders o ON c.c_custkey = o.o_custkey ORDER BY o.o_orderdate DESC";
        Container.ExecResult join = executeQuery(joinOrders);
        String[] lines = join.getStdout().split("\n");
        int orders = 0;
        for (String line : lines) {
            if (line.contains("Customer#")) {
                orders++;
            }
            else {
                fail("Invalid order details" + line + "\n");
            }
        }
        assertEquals(orders, 172, "Mismatch in joining orders");
    }
}
