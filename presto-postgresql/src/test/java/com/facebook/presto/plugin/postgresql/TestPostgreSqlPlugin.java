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
package com.facebook.presto.plugin.postgresql;

import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.connector.ConnectorContext;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.facebook.presto.testing.TestingConnectorFactoryContext;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.google.common.collect.Iterables.getOnlyElement;

public class TestPostgreSqlPlugin
{
    @Test
    public void testCreateConnector()
            throws Exception
    {
        Plugin plugin = new PostgreSqlPlugin();
        ConnectorFactory factory = getOnlyElement(plugin.getConnectorFactories(new TestingConnectorFactoryContext()));
        factory.create("test", ImmutableMap.of("connection-url", "test"), new ConnectorContext() {});
    }
}
