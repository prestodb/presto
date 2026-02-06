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

import com.facebook.presto.Session;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.tpch.TpchQueryRunnerBuilder;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test that verifies the Rust Cascades optimizer works end-to-end
 * with Presto's TPC-H catalog.
 *
 * <p>The test spawns the Rust {@code optx-server} as a subprocess on a dynamic port,
 * creates a Presto query runner with TPC-H tables, and verifies that:
 * <ul>
 *   <li>EXPLAIN queries succeed with the optimizer enabled</li>
 *   <li>Query results are correct (match the default optimizer's results)</li>
 *   <li>The optimizer falls back gracefully when the server is unavailable</li>
 * </ul>
 *
 * <h3>Prerequisites</h3>
 * The optx-server binary must be built before running this test:
 * <pre>
 * cd presto-optimizer-rs &amp;&amp; cargo build -p optx-server
 * </pre>
 *
 * <h3>Running</h3>
 * <pre>
 * ./mvnw test -pl :presto-tests -Dtest=TestRustCascadeOptimizer
 * </pre>
 *
 * Or with an explicit binary path:
 * <pre>
 * ./mvnw test -pl :presto-tests -Dtest=TestRustCascadeOptimizer \
 *   -Doptx.server.binary=presto-optimizer-rs/target/release/optx-server
 * </pre>
 */
@Test(singleThreaded = true)
public class TestRustCascadeOptimizer
        extends AbstractTestQueryFramework
{
    private Process serverProcess;
    private int serverPort;
    private String serverUrl;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return TpchQueryRunnerBuilder.builder().build();
    }

    @BeforeClass
    public void startRustServer()
            throws Exception
    {
        Path binary = findOptxServerBinary();
        if (binary == null) {
            throw new SkipException(
                    "optx-server binary not found. Build with: " +
                            "cd presto-optimizer-rs && cargo build -p optx-server");
        }

        serverPort = findFreePort();
        serverUrl = "http://localhost:" + serverPort;

        ProcessBuilder pb = new ProcessBuilder(binary.toString(), "--port", String.valueOf(serverPort));
        pb.environment().put("RUST_LOG", "warn");
        pb.inheritIO();
        serverProcess = pb.start();

        waitForServerHealthy();
    }

    @AfterClass(alwaysRun = true)
    public void stopRustServer()
    {
        if (serverProcess != null) {
            serverProcess.destroyForcibly();
            try {
                serverProcess.waitFor();
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Test
    public void testExplainMultiTableJoin()
    {
        String query = "SELECT n.name, count(*) " +
                "FROM nation n, customer c, orders o, lineitem l " +
                "WHERE n.nationkey = c.nationkey " +
                "AND c.custkey = o.custkey " +
                "AND o.orderkey = l.orderkey " +
                "GROUP BY n.name";

        MaterializedResult result = computeActual(optimizerSession(), "EXPLAIN " + query);
        String plan = (String) result.getOnlyValue();
        assertThat(plan).contains("InnerJoin");
    }

    @Test
    public void testThreeTableJoinCorrectness()
    {
        String query = "SELECT n.name, count(*) cnt " +
                "FROM nation n, customer c, orders o " +
                "WHERE n.nationkey = c.nationkey " +
                "AND c.custkey = o.custkey " +
                "GROUP BY n.name " +
                "ORDER BY n.name";

        MaterializedResult expected = computeActual(query);
        MaterializedResult actual = computeActual(optimizerSession(), query);

        assertThat(actual.getMaterializedRows())
                .isEqualTo(expected.getMaterializedRows());
    }

    @Test
    public void testFourTableJoinCorrectness()
    {
        String query = "SELECT n.name, sum(l.extendedprice) revenue " +
                "FROM nation n, customer c, orders o, lineitem l " +
                "WHERE n.nationkey = c.nationkey " +
                "AND c.custkey = o.custkey " +
                "AND o.orderkey = l.orderkey " +
                "GROUP BY n.name " +
                "ORDER BY n.name";

        MaterializedResult expected = computeActual(query);
        MaterializedResult actual = computeActual(optimizerSession(), query);

        assertThat(actual.getMaterializedRows())
                .isEqualTo(expected.getMaterializedRows());
    }

    @Test
    public void testTpchQ5Correctness()
    {
        String query = "SELECT n.name, sum(l.extendedprice * (1 - l.discount)) revenue " +
                "FROM customer c, orders o, lineitem l, supplier s, nation n, region r " +
                "WHERE c.custkey = o.custkey " +
                "AND l.orderkey = o.orderkey " +
                "AND l.suppkey = s.suppkey " +
                "AND c.nationkey = s.nationkey " +
                "AND s.nationkey = n.nationkey " +
                "AND n.regionkey = r.regionkey " +
                "AND r.name = 'ASIA' " +
                "AND o.orderdate >= DATE '1994-01-01' " +
                "AND o.orderdate < DATE '1995-01-01' " +
                "GROUP BY n.name " +
                "ORDER BY revenue DESC";

        MaterializedResult expected = computeActual(query);
        MaterializedResult actual = computeActual(optimizerSession(), query);

        assertThat(actual.getMaterializedRows())
                .isEqualTo(expected.getMaterializedRows());
    }

    @Test
    public void testGracefulFallbackWhenServerUnavailable()
    {
        // Point to a non-existent server — the optimizer should fall back silently.
        Session session = Session.builder(getSession())
                .setSystemProperty("use_rust_cascade_optimizer", "true")
                .setSystemProperty("rust_cascade_optimizer_url", "http://localhost:1")
                .build();

        String query = "SELECT n.name, count(*) " +
                "FROM nation n, customer c " +
                "WHERE n.nationkey = c.nationkey " +
                "GROUP BY n.name " +
                "ORDER BY n.name";

        MaterializedResult result = computeActual(session, query);
        assertThat(result.getRowCount()).isGreaterThan(0);
    }

    private Session optimizerSession()
    {
        return Session.builder(getSession())
                .setSystemProperty("use_rust_cascade_optimizer", "true")
                .setSystemProperty("rust_cascade_optimizer_url", serverUrl)
                .build();
    }

    private static Path findOptxServerBinary()
    {
        // Check system property first.
        String binaryPath = System.getProperty("optx.server.binary");
        if (binaryPath != null) {
            Path path = Paths.get(binaryPath);
            if (Files.isExecutable(path)) {
                return path;
            }
        }

        // Try relative paths from the working directory.
        Path workDir = Paths.get(System.getProperty("user.dir"));
        for (String relativePath : new String[] {
                "presto-optimizer-rs/target/release/optx-server",
                "presto-optimizer-rs/target/debug/optx-server",
                "../presto-optimizer-rs/target/release/optx-server",
                "../presto-optimizer-rs/target/debug/optx-server"}) {
            Path path = workDir.resolve(relativePath);
            if (Files.isExecutable(path)) {
                return path;
            }
        }

        return null;
    }

    private static int findFreePort()
            throws IOException
    {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private void waitForServerHealthy()
            throws Exception
    {
        HttpClient client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(2))
                .build();

        for (int i = 0; i < 30; i++) {
            if (!serverProcess.isAlive()) {
                throw new RuntimeException("optx-server process exited with code " + serverProcess.exitValue());
            }
            try {
                HttpResponse<String> response = client.send(
                        HttpRequest.newBuilder()
                                .uri(URI.create(serverUrl + "/health"))
                                .GET()
                                .timeout(Duration.ofSeconds(2))
                                .build(),
                        HttpResponse.BodyHandlers.ofString());
                if (response.statusCode() == 200) {
                    return;
                }
            }
            catch (Exception e) {
                // Server not ready yet — retry.
            }
            Thread.sleep(500);
        }
        throw new RuntimeException("optx-server did not become healthy within 15 seconds");
    }
}
