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
package io.prestosql.plugin.thrift;

import com.google.common.collect.ImmutableMap;
import io.prestosql.spi.Plugin;
import io.prestosql.spi.connector.Connector;
import io.prestosql.spi.connector.ConnectorFactory;
import io.prestosql.testing.TestingConnectorContext;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.ServiceLoader;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.testing.Assertions.assertInstanceOf;
import static org.testng.Assert.assertNotNull;

public class TestThriftPlugin
{
    @Test
    public void testPlugin()
    {
        ThriftPlugin plugin = loadPlugin(ThriftPlugin.class);

        ConnectorFactory factory = getOnlyElement(plugin.getConnectorFactories());
        assertInstanceOf(factory, ThriftConnectorFactory.class);

        Map<String, String> config = ImmutableMap.of("presto.thrift.client.addresses", "localhost:7779");

        Connector connector = factory.create("test", config, new TestingConnectorContext());
        assertNotNull(connector);
        assertInstanceOf(connector, ThriftConnector.class);
    }

    @SuppressWarnings("unchecked")
    private static <T extends Plugin> T loadPlugin(Class<T> clazz)
    {
        for (Plugin plugin : ServiceLoader.load(Plugin.class)) {
            if (clazz.isInstance(plugin)) {
                return (T) plugin;
            }
        }
        throw new AssertionError("did not find plugin: " + clazz.getName());
    }
}
