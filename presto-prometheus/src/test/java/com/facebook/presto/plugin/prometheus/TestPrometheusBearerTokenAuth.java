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

import com.facebook.presto.spi.PrestoException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.nio.file.Files;

import static com.facebook.presto.plugin.prometheus.MetadataUtil.METRIC_CODEC;
import static com.facebook.presto.plugin.prometheus.PrometheusHttpServer.BEARER_TOKEN;
import static com.facebook.presto.plugin.prometheus.TestPrometheusTable.TYPE_MANAGER;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertFalse;

public class TestPrometheusBearerTokenAuth
{
    private PrometheusHttpServer server;

    @BeforeClass
    public void setUp()
    {
        server = new PrometheusHttpServer(BEARER_TOKEN);
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        if (server != null) {
            server.stop();
        }
    }

    @Test
    public void testValidBearerTokenSucceeds()
            throws Exception
    {
        File tokenFile = File.createTempFile("prometheus-bearer-token", ".txt");
        tokenFile.deleteOnExit();
        Files.write(tokenFile.toPath(), BEARER_TOKEN.getBytes(UTF_8));

        PrometheusConnectorConfig config = new PrometheusConnectorConfig();
        config.setPrometheusURI(server.resolve("/prometheus-data/prometheus-metrics.json"));
        config.setBearerTokenFile(tokenFile);
        PrometheusClient client = new PrometheusClient(config, METRIC_CODEC, TYPE_MANAGER);

        assertFalse(client.getTableNames("default").isEmpty(),
                "Client with correct bearer token should reach the server successfully");
    }

    @Test
    public void testBearerTokenFileWithTrailingNewlineSucceeds()
            throws Exception
    {
        File tokenFile = File.createTempFile("prometheus-bearer-token-newline", ".txt");
        tokenFile.deleteOnExit();
        Files.write(tokenFile.toPath(), (BEARER_TOKEN + "\n").getBytes(UTF_8));

        PrometheusConnectorConfig config = new PrometheusConnectorConfig();
        config.setPrometheusURI(server.resolve("/prometheus-data/prometheus-metrics.json"));
        config.setBearerTokenFile(tokenFile);
        PrometheusClient client = new PrometheusClient(config, METRIC_CODEC, TYPE_MANAGER);

        assertFalse(client.getTableNames("default").isEmpty(),
                "Token file with trailing newline should be trimmed and succeed");
    }

    @Test
    public void testWrongBearerTokenIsRejected()
            throws Exception
    {
        File tokenFile = File.createTempFile("prometheus-bearer-token-wrong", ".txt");
        tokenFile.deleteOnExit();
        Files.write(tokenFile.toPath(), "wrong-token".getBytes(UTF_8));

        PrometheusConnectorConfig config = new PrometheusConnectorConfig();
        config.setPrometheusURI(server.resolve("/prometheus-data/prometheus-metrics.json"));
        config.setBearerTokenFile(tokenFile);
        PrometheusClient client = new PrometheusClient(config, METRIC_CODEC, TYPE_MANAGER);

        assertThatThrownBy(() -> client.getTableNames("default"))
                .isInstanceOf(PrestoException.class)
                .hasMessageContaining("Bad response 401");
    }

    @Test
    public void testMissingBearerTokenIsRejected()
    {
        // No token file — server enforcing bearer auth must reject with 401.
        PrometheusConnectorConfig config = new PrometheusConnectorConfig();
        config.setPrometheusURI(server.resolve("/prometheus-data/prometheus-metrics.json"));
        PrometheusClient client = new PrometheusClient(config, METRIC_CODEC, TYPE_MANAGER);

        assertThatThrownBy(() -> client.getTableNames("default"))
                .isInstanceOf(PrestoException.class)
                .hasMessageContaining("Bad response 401");
    }
}
