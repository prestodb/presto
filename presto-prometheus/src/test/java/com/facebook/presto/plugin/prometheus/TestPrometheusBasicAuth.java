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
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import static com.facebook.presto.plugin.prometheus.MetadataUtil.METRIC_CODEC;
import static com.facebook.presto.plugin.prometheus.PrometheusQueryRunner.createPrometheusQueryRunner;
import static com.facebook.presto.plugin.prometheus.PrometheusServer.BASIC_AUTH_VERSION;
import static com.facebook.presto.plugin.prometheus.PrometheusServer.PASSWORD;
import static com.facebook.presto.plugin.prometheus.PrometheusServer.USER;
import static com.facebook.presto.plugin.prometheus.TestPrometheusTable.TYPE_MANAGER;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestPrometheusBasicAuth
        extends AbstractTestQueryFramework
{
    private PrometheusServer server;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        this.server = new PrometheusServer(BASIC_AUTH_VERSION, true);
        return createPrometheusQueryRunner(ImmutableMap.of(
                "prometheus.uri", server.getUri().toString(),
                "prometheus.auth.user", USER,
                "prometheus.auth.password", PASSWORD));
    }

    @AfterClass(alwaysRun = true)
    public final void destroy()
    {
        if (server != null) {
            server.close();
            server = null;
        }
    }

    @Test
    public void testSelectWithValidCredentials()
    {
        assertQuery("SHOW TABLES IN prometheus.default LIKE 'up'", "VALUES 'up'");
        assertQuery("SELECT labels['job'] FROM prometheus.default.up LIMIT 1", "VALUES 'prometheus'");
    }

    @Test
    public void testInvalidCredentials()
    {
        PrometheusConnectorConfig config = new PrometheusConnectorConfig();
        config.setPrometheusURI(server.getUri());
        config.setBasicAuthUser("invalid-user");
        config.setBasicAuthPassword("invalid-password");
        PrometheusClient client = new PrometheusClient(config, METRIC_CODEC, TYPE_MANAGER);
        assertThatThrownBy(() -> client.getTableNames("default"))
                .isInstanceOf(PrestoException.class)
                .hasMessageContaining("Bad response 401");
    }

    @Test
    public void testMissingCredentials()
    {
        PrometheusConnectorConfig config = new PrometheusConnectorConfig();
        config.setPrometheusURI(server.getUri());
        PrometheusClient client = new PrometheusClient(config, METRIC_CODEC, TYPE_MANAGER);
        assertThatThrownBy(() -> client.getTableNames("default"))
                .isInstanceOf(PrestoException.class)
                .hasMessageContaining("Bad response 401");
    }
}
