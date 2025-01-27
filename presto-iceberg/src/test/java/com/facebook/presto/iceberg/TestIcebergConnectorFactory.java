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
package com.facebook.presto.iceberg;

import com.facebook.presto.spi.connector.ConnectorFactory;
import com.facebook.presto.testing.TestingConnectorContext;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.lang.management.ManagementFactory;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestIcebergConnectorFactory
{
    @Test
    public void testCachingHiveMetastore()
    {
        Map<String, String> config = ImmutableMap.<String, String>builder()
                .put("hive.metastore.uri", "thrift://localhost:9083")
                .put("hive.metastore-cache-ttl", "10m")
                .buildOrThrow();

        assertThatThrownBy(() -> createConnector(config))
                .hasMessageContaining("In-memory hive metastore caching must not be enabled for Iceberg");
    }

    private static void createConnector(Map<String, String> config)
    {
        ConnectorFactory factory = new IcebergConnectorFactory(ManagementFactory.getPlatformMBeanServer());
        factory.create("iceberg-test", config, new TestingConnectorContext())
                .shutdown();
    }
}
