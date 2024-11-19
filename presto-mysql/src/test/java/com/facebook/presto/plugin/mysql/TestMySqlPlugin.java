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
package com.facebook.presto.plugin.mysql;

import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.facebook.presto.testing.TestingConnectorContext;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.stream.StreamSupport;

import static com.google.common.collect.MoreCollectors.onlyElement;

public class TestMySqlPlugin
{
    @Test
    public void testCreateConnector()
    {
        Plugin plugin = new MySqlPlugin();
        ConnectorFactory factory = StreamSupport.stream(plugin.getConnectorFactories().spliterator(), false).collect(onlyElement());
        factory.create("test", ImmutableMap.of("connection-url", "jdbc:mysql://test"), new TestingConnectorContext());
    }
}
