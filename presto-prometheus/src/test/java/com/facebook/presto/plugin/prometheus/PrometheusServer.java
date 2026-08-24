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
package com.facebook.presto.plugin.prometheus;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.io.Closeable;
import java.net.URI;
import java.time.Duration;

import static org.testcontainers.utility.MountableFile.forClasspathResource;
import static org.testng.Assert.fail;

public class PrometheusServer
        implements Closeable
{
    private static final int PROMETHEUS_PORT = 9090;
    private static final String DEFAULT_VERSION = "v2.15.1";
    public static final String BASIC_AUTH_VERSION = "v2.35.0";
    private static final Integer MAX_TRIES = 120;
    private static final Integer TIME_BETWEEN_TRIES_MILLIS = 1000;

    public static final String USER = "admin";
    public static final String PASSWORD = "password";
    private static final String PROMETHEUS_QUERY_API = "/api/v1/query?query=up[1d]";

    private final GenericContainer<?> dockerContainer;

    public PrometheusServer()
    {
        this(DEFAULT_VERSION, false);
    }

    public PrometheusServer(String version, boolean enableBasicAuth)
    {
        this.dockerContainer = new GenericContainer<>("prom/prometheus:" + version)
                .withExposedPorts(PROMETHEUS_PORT)
                .withStartupTimeout(Duration.ofSeconds(120));
        if (enableBasicAuth) {
            // Mount web.yml to configure Prometheus with the Basic Auth user and bcrypt-hashed password.
            this.dockerContainer
                    .withCommand("--config.file=/etc/prometheus/prometheus.yml", "--web.config.file=/etc/prometheus/web.yml")
                    .withCopyFileToContainer(forClasspathResource("web.yml"), "/etc/prometheus/web.yml")
                    .waitingFor(Wait.forHttp(PROMETHEUS_QUERY_API)
                            .forResponsePredicate(response -> response.contains("\"values\""))
                            .withBasicCredentials(USER, PASSWORD));
        }
        else {
            this.dockerContainer.waitingFor(Wait.forHttp("/"));
        }
        this.dockerContainer.start();
    }

    protected static void checkServerReady(PrometheusClient client)
            throws Exception
    {
        // ensure Prometheus available and ready to answer metrics list query
        for (int tries = 0; tries < MAX_TRIES; tries++) {
            if (client.getTableNames("default").contains("up")) {
                return;
            }
            Thread.sleep(TIME_BETWEEN_TRIES_MILLIS);
        }
        fail("Prometheus container not available for metrics query in " + MAX_TRIES * TIME_BETWEEN_TRIES_MILLIS + " milliseconds.");
    }

    public URI getUri()
    {
        return URI.create("http://" + dockerContainer.getContainerIpAddress() + ":" + dockerContainer.getMappedPort(PROMETHEUS_PORT) + "/");
    }

    public void close()
    {
        dockerContainer.close();
    }
}
