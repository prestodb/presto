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
package com.facebook.presto.redis.unittests;

import com.facebook.presto.connector.meta.SupportedFeatures;
import com.facebook.presto.connector.unittest.CreatesSchemas;
import com.facebook.presto.connector.unittest.TestMetadata;
import com.facebook.presto.connector.unittest.TestMetadataSchema;
import com.facebook.presto.connector.unittest.TestMetadataTable;
import com.facebook.presto.redis.RedisPlugin;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.facebook.presto.testing.TestingConnectorContext;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

import static com.google.common.collect.Iterables.getOnlyElement;

@SupportedFeatures({})
public class TestRedisMetadata
        implements TestMetadata, TestMetadataSchema, TestMetadataTable, CreatesSchemas
{
    @Override
    public Connector getConnector()
    {
        RedisPlugin plugin = new RedisPlugin();
        Iterable<ConnectorFactory> connectorFactories = plugin.getConnectorFactories();
        ConnectorFactory factory = getOnlyElement(connectorFactories);

        Map<String, String> config = ImmutableMap.of(
                "redis.nodes", "1");

        return factory.create("redis", config, new TestingConnectorContext());
    }

    @Override
    public Map<String, Object> getTableProperties()
    {
        return ImmutableMap.of();
    }
}
