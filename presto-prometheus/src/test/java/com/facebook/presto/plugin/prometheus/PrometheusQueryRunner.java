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

import com.facebook.airlift.log.Logger;
import com.facebook.airlift.log.Logging;
import com.facebook.presto.Session;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;

import java.util.Map;

import static com.facebook.airlift.testing.Closeables.closeAllSuppress;
import static com.facebook.presto.plugin.prometheus.MetadataUtil.METRIC_CODEC;
import static com.facebook.presto.plugin.prometheus.TestPrometheusTable.TYPE_MANAGER;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;

public class PrometheusQueryRunner
{
    private PrometheusQueryRunner() {}

    public static DistributedQueryRunner createPrometheusQueryRunner(PrometheusServer server)
            throws Exception
    {
        DistributedQueryRunner queryRunner = null;
        try {
            queryRunner = DistributedQueryRunner.builder(createSession()).build();

            queryRunner.installPlugin(new PrometheusPlugin());
            Map<String, String> properties = ImmutableMap.of(
                    "prometheus.uri", server.getUri().toString());
            queryRunner.createCatalog("prometheus", "prometheus", properties);
            return queryRunner;
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
    }

    private static Session createSession()
    {
        return testSessionBuilder()
                .setCatalog("prometheus")
                .setSchema("default")
                .build();
    }

    public static PrometheusClient createPrometheusClient(PrometheusServer server)
    {
        PrometheusConnectorConfig config = new PrometheusConnectorConfig();
        config.setPrometheusURI(server.getUri());
        config.setQueryChunkSizeDuration(Duration.valueOf("1d"));
        config.setMaxQueryRangeDuration(Duration.valueOf("21d"));
        config.setCacheDuration(Duration.valueOf("30s"));
        return new PrometheusClient(config, METRIC_CODEC, TYPE_MANAGER);
    }

    public static void main(String[] args)
            throws Exception
    {
        Logging.initialize();
        DistributedQueryRunner queryRunner = createPrometheusQueryRunner(new PrometheusServer());
        Thread.sleep(10);
        Logger log = Logger.get(PrometheusQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
