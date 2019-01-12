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
package io.prestosql.plugin.redis;

import com.google.common.collect.ImmutableMap;
import io.prestosql.spi.connector.Connector;
import io.prestosql.spi.connector.ConnectorFactory;
import io.prestosql.testing.TestingConnectorContext;
import org.testng.annotations.Test;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.testing.Assertions.assertInstanceOf;
import static org.testng.Assert.assertNotNull;

@Test
public class TestRedisPlugin
{
    @Test
    public void testStartup()
    {
        RedisPlugin plugin = new RedisPlugin();

        ConnectorFactory factory = getOnlyElement(plugin.getConnectorFactories());
        assertInstanceOf(factory, RedisConnectorFactory.class);

        Connector c = factory.create(
                "test-connector",
                ImmutableMap.<String, String>builder()
                        .put("redis.table-names", "test")
                        .put("redis.nodes", "localhost:6379")
                        .build(),
                new TestingConnectorContext());
        assertNotNull(c);
    }
}
