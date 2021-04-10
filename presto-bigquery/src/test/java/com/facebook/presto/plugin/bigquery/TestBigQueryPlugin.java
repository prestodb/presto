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
package com.facebook.presto.plugin.bigquery;

import com.facebook.presto.spi.connector.ConnectorFactory;
import org.testng.annotations.Test;

import static com.facebook.airlift.testing.Assertions.assertInstanceOf;
import static com.google.common.collect.Iterables.getOnlyElement;

@Test
public class TestBigQueryPlugin
{
    @Test
    public void testStartup()
    {
        BigQueryPlugin plugin = new BigQueryPlugin();

        ConnectorFactory factory = getOnlyElement(plugin.getConnectorFactories());
        assertInstanceOf(factory, BigQueryConnectorFactory.class);
    }
}
