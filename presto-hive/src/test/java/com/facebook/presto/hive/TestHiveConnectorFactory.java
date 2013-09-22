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
package com.facebook.presto.hive;

import com.facebook.presto.spi.Connector;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.ConnectorMetadata;
import com.facebook.presto.spi.ConnectorRecordSetProvider;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.facebook.presto.spi.classloader.ClassLoaderSafeConnectorHandleResolver;
import com.facebook.presto.spi.classloader.ClassLoaderSafeConnectorMetadata;
import com.facebook.presto.spi.classloader.ClassLoaderSafeConnectorRecordSetProvider;
import com.facebook.presto.spi.classloader.ClassLoaderSafeConnectorSplitManager;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static io.airlift.testing.Assertions.assertInstanceOf;

public class TestHiveConnectorFactory
{
    @Test
    public void testGetClient()
    {
        assertCreateConnector("thrift://localhost:1234");
        assertCreateConnector("discovery::");
    }

    private static void assertCreateConnector(String metastoreUri)
    {
        HiveConnectorFactory connectorFactory = new HiveConnectorFactory(
                "hive-test",
                ImmutableMap.<String, String>builder()
                        .put("node.environment", "test")
                        .put("hive.metastore.uri", metastoreUri)
                        .build(),
                HiveConnector.class.getClassLoader());

        Connector connector = connectorFactory.create("hive-test", ImmutableMap.<String, String>of());
        assertInstanceOf(connector.getService(ConnectorMetadata.class), ClassLoaderSafeConnectorMetadata.class);
        assertInstanceOf(connector.getService(ConnectorSplitManager.class), ClassLoaderSafeConnectorSplitManager.class);
        assertInstanceOf(connector.getService(ConnectorRecordSetProvider.class), ClassLoaderSafeConnectorRecordSetProvider.class);
        assertInstanceOf(connector.getService(ConnectorHandleResolver.class), ClassLoaderSafeConnectorHandleResolver.class);
    }
}
