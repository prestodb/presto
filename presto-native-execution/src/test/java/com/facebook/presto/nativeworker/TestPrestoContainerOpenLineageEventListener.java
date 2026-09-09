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

import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.facebook.presto.nativeworker.ContainerQueryRunner.DEFAULT_COORDINATOR_PORT;
import static com.facebook.presto.nativeworker.ContainerQueryRunner.containerStartupTimeout;
import static com.facebook.presto.nativeworker.ContainerQueryRunner.networkExpected;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

/**
 * Runs the OpenLineage event listener inside the packaged coordinator image and sends its events
 * to a real Marquez server (the OpenLineage reference backend) running on the cluster network.
 * <p>
 * The packaged server loads the plugin through the real plugin class loader with only the
 * server's own jars on the parent side, which unit tests on a flat class path cannot cover
 * (see <a href="https://github.com/prestodb/presto/issues/28460">prestodb/presto#28460</a>).
 * Marquez validates every event against the OpenLineage specification before storing it, so
 * the assertions below go through the Marquez REST API instead of inspecting raw requests.
 * Modeled on Trino's {@code TestOpenLineageEventListenerMarquezIntegration}.
 */
public class TestPrestoContainerOpenLineageEventListener
        extends AbstractTestQueryFramework
{
    private static final String MARQUEZ_VERSION = "0.51.1";
    private static final String MARQUEZ_IMAGE = "marquezproject/marquez:" + MARQUEZ_VERSION;
    private static final String MARQUEZ_ALIAS = "marquez";
    private static final int MARQUEZ_PORT = 5000;
    private static final int MARQUEZ_ADMIN_PORT = 5001;
    private static final String MARQUEZ_CONFIG_PATH = "/opt/marquez/marquez.yaml";
    private static final String LINEAGE_ENDPOINT = "/api/v1/lineage";

    // Marquez's own docker-compose runs against postgres:14
    private static final String POSTGRES_IMAGE = "postgres:14";
    private static final String POSTGRES_ALIAS = "marquez-postgres";
    private static final String POSTGRES_DATABASE = "marquez";
    private static final String POSTGRES_USER = "marquez";
    private static final String POSTGRES_PASSWORD = "marquez";

    private static final String PRESTO_URI = "http://presto-coordinator:" + DEFAULT_COORDINATOR_PORT;
    // The listener derives job and dataset namespaces from presto.uri by swapping the scheme
    private static final String OPENLINEAGE_NAMESPACE = "presto://presto-coordinator:" + DEFAULT_COORDINATOR_PORT;
    private static final String INPUT_TABLE = "tpch.tiny.nation";
    // The TPC-H connector reports the schema of a table by its scale factor, so the dataset name differs from the table name in the query
    private static final String INPUT_DATASET = "tpch.sf0.01.nation";

    private static final Duration EVENT_TIMEOUT = Duration.ofSeconds(90);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final HttpClient HTTP_CLIENT = HttpClient.newHttpClient();

    // Minimal Marquez server configuration (same shape as marquez.dev.yml shipped in the image, without the search backend)
    private static final String MARQUEZ_CONFIG = String.join("\n",
            "server:",
            "  applicationConnectors:",
            "    - type: http",
            "      port: " + MARQUEZ_PORT,
            "      httpCompliance: RFC7230_LEGACY",
            "  adminConnectors:",
            "    - type: http",
            "      port: " + MARQUEZ_ADMIN_PORT,
            "db:",
            "  driverClass: org.postgresql.Driver",
            "  url: jdbc:postgresql://" + POSTGRES_ALIAS + ":5432/" + POSTGRES_DATABASE,
            "  user: " + POSTGRES_USER,
            "  password: " + POSTGRES_PASSWORD,
            "migrateOnStartup: true",
            "logging:",
            "  level: INFO",
            "  appenders:",
            "    - type: console",
            "");

    private GenericContainer<?> postgres;
    private GenericContainer<?> marquez;

    @Override
    protected ContainerQueryRunner createQueryRunner()
            throws Exception
    {
        postgres = new GenericContainer<>(POSTGRES_IMAGE)
                .withNetwork(networkExpected)
                .withNetworkAliases(POSTGRES_ALIAS)
                .withEnv("POSTGRES_DB", POSTGRES_DATABASE)
                .withEnv("POSTGRES_USER", POSTGRES_USER)
                .withEnv("POSTGRES_PASSWORD", POSTGRES_PASSWORD)
                // Logged once by the init step and once by the final server start
                .waitingFor(Wait.forLogMessage(".*database system is ready to accept connections.*", 2))
                .withStartupTimeout(containerStartupTimeout());
        postgres.start();

        marquez = new GenericContainer<>(MARQUEZ_IMAGE)
                .withNetwork(networkExpected)
                .withNetworkAliases(MARQUEZ_ALIAS)
                .withExposedPorts(MARQUEZ_PORT, MARQUEZ_ADMIN_PORT)
                .withEnv("MARQUEZ_CONFIG", MARQUEZ_CONFIG_PATH)
                .withCopyToContainer(Transferable.of(MARQUEZ_CONFIG), MARQUEZ_CONFIG_PATH)
                .waitingFor(Wait.forHttp("/ping").forPort(MARQUEZ_ADMIN_PORT).forStatusCode(200))
                .withStartupTimeout(containerStartupTimeout());
        marquez.start();

        return new ContainerQueryRunner(new ContainerQueryRunner.Config()
                .setNativeCluster(false)
                .setNumberOfWorkers(1)
                .setEventListenerProperties(ImmutableMap.<String, String>builder()
                        .put("event-listener.name", "openlineage-event-listener")
                        .put("openlineage-event-listener.presto.uri", PRESTO_URI)
                        // Completion events are only emitted for these query types; a SELECT is enough to build the statistics facet
                        .put("openlineage-event-listener.presto.include-query-types", "SELECT")
                        .put("openlineage-event-listener.transport.type", "HTTP")
                        .put("openlineage-event-listener.transport.url", format("http://%s:%s", MARQUEZ_ALIAS, MARQUEZ_PORT))
                        .put("openlineage-event-listener.transport.endpoint", LINEAGE_ENDPOINT)
                        .put("openlineage-event-listener.transport.compression", "gzip")
                        .build()));
    }

    @AfterClass(alwaysRun = true)
    public void stopMarquez()
    {
        if (marquez != null) {
            marquez.stop();
        }
        if (postgres != null) {
            postgres.stop();
        }
    }

    @Test
    public void testCompletedQueryRegisteredInMarquez()
            throws Exception
    {
        // The coordinator's tpch catalog uses standard column naming
        String sql = "SELECT n_nationkey, n_regionkey FROM " + INPUT_TABLE;
        assertEquals(computeActual(sql).getRowCount(), 25);
        String queryId = (String) computeActual("SELECT query_id FROM system.runtime.queries WHERE query = '" + sql + "'").getOnlyValue();

        // Marquez only stores events that pass OpenLineage schema validation, so a registered job proves the wire format
        JsonNode job = awaitCompletedJob(queryId);
        assertEquals(job.path("latestRun").path("state").asText(), "COMPLETED", "job: " + job);

        // Run facets are returned by the run endpoint; the job endpoint only carries job facets on its latest run
        HttpResponse<String> runResponse = marquezGet("/api/v1/jobs/runs/" + encode(job.path("latestRun").path("id").asText()));
        assertEquals(runResponse.statusCode(), 200, "run lookup: " + runResponse.body());
        JsonNode facets = MAPPER.readTree(runResponse.body()).path("facets");
        assertEquals(facets.path("presto_metadata").path("query_id").asText(), queryId, "run facets: " + facets);
        JsonNode statistics = facets.path("presto_query_statistics");
        assertEquals(statistics.path("complete").asText(), "true", "query statistics facet: " + statistics);
        assertTrue(statistics.has("cpuTime") && statistics.has("wallTime"), "query statistics facet: " + statistics);

        Set<String> inputs = stream(job.path("inputs"))
                .map(input -> input.path("namespace").asText() + " " + input.path("name").asText())
                .collect(toImmutableSet());
        assertTrue(inputs.contains(OPENLINEAGE_NAMESPACE + " " + INPUT_DATASET), "job inputs: " + inputs);

        // The input dataset itself, including the schema facet, is registered under the presto namespace
        HttpResponse<String> datasetResponse = marquezGet(format("/api/v1/namespaces/%s/datasets/%s", encode(OPENLINEAGE_NAMESPACE), encode(INPUT_DATASET)));
        assertEquals(datasetResponse.statusCode(), 200, "dataset lookup: " + datasetResponse.body());
        Set<String> fields = stream(MAPPER.readTree(datasetResponse.body()).path("fields"))
                .map(field -> field.path("name").asText())
                .collect(toImmutableSet());
        assertTrue(fields.containsAll(ImmutableSet.of("n_nationkey", "n_regionkey")), "dataset fields: " + fields);

        String coordinatorLogs = ((ContainerQueryRunner) getQueryRunner()).getCoordinatorLogs();
        assertFalse(coordinatorLogs.contains("NoClassDefFoundError"), "coordinator log contains NoClassDefFoundError");
        assertFalse(coordinatorLogs.contains("at com.facebook.presto.plugin.openlineage."), "coordinator log contains a stack trace from the OpenLineage plugin");
    }

    private JsonNode awaitCompletedJob(String queryId)
            throws Exception
    {
        String jobPath = format("/api/v1/namespaces/%s/jobs/%s", encode(OPENLINEAGE_NAMESPACE), encode(queryId));
        long deadline = System.nanoTime() + EVENT_TIMEOUT.toNanos();
        HttpResponse<String> response;
        while (true) {
            response = marquezGet(jobPath);
            if (response.statusCode() == 200) {
                JsonNode job = MAPPER.readTree(response.body());
                // The START event registers the run as RUNNING first; wait for the COMPLETE event
                if ("COMPLETED".equals(job.path("latestRun").path("state").asText())) {
                    return job;
                }
            }
            if (System.nanoTime() > deadline) {
                fail(format("Job %s not completed in Marquez within %s. Last response %s: %s%nMarquez log tail:%n%s%nCoordinator log tail:%n%s",
                        queryId,
                        EVENT_TIMEOUT,
                        response.statusCode(),
                        response.body(),
                        tail(marquez.getLogs()),
                        tail(((ContainerQueryRunner) getQueryRunner()).getCoordinatorLogs())));
            }
            MILLISECONDS.sleep(500);
        }
    }

    private HttpResponse<String> marquezGet(String path)
            throws Exception
    {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(format("http://%s:%s%s", marquez.getHost(), marquez.getMappedPort(MARQUEZ_PORT), path)))
                .GET()
                .build();
        return HTTP_CLIENT.send(request, HttpResponse.BodyHandlers.ofString());
    }

    private static String encode(String value)
    {
        return URLEncoder.encode(value, UTF_8);
    }

    private static Stream<JsonNode> stream(JsonNode array)
    {
        return StreamSupport.stream(array.spliterator(), false);
    }

    private static String tail(String logs)
    {
        List<String> lines = logs.lines().collect(toImmutableList());
        return String.join("\n", lines.subList(Math.max(0, lines.size() - 50), lines.size()));
    }
}
