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
import com.facebook.presto.spi.ConnectorPageSourceProvider;
import com.facebook.presto.spi.classloader.ClassLoaderSafeConnectorHandleResolver;
import com.facebook.presto.spi.classloader.ClassLoaderSafeConnectorMetadata;
import com.facebook.presto.spi.classloader.ClassLoaderSafeConnectorSplitManager;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static io.airlift.testing.Assertions.assertContains;
import static io.airlift.testing.Assertions.assertInstanceOf;
import static org.testng.Assert.fail;

public class TestHiveConnectorFactory
{
    @Test
    public void testGetClient()
    {
        assertCreateConnector("thrift://localhost:1234");
        assertCreateConnector("thrift://localhost:1234,thrift://192.0.2.3:5678");

        assertCreateConnectorFails("abc", "metastoreUri scheme is missing: abc");
        assertCreateConnectorFails("thrift://:8090", "metastoreUri host is missing: thrift://:8090");
        assertCreateConnectorFails("thrift://localhost", "metastoreUri port is missing: thrift://localhost");
        assertCreateConnectorFails("abc::", "metastoreUri scheme must be thrift: abc::");
        assertCreateConnectorFails("", "metastoreUris must specify at least one URI");
        assertCreateConnectorFails("thrift://localhost:1234,thrift://test-1", "metastoreUri port is missing: thrift://test-1");
    }

    private static void assertCreateConnector(String metastoreUri)
    {
        HiveConnectorFactory connectorFactory = new HiveConnectorFactory(
                "hive-test",
                ImmutableMap.<String, String>builder()
                        .put("node.environment", "test")
                        .put("hive.metastore.uri", metastoreUri)
                        .build(),
                HiveConnector.class.getClassLoader(),
                null,
                new TypeRegistry());

        Connector connector = connectorFactory.create("hive-test", ImmutableMap.<String, String>of());
        assertInstanceOf(connector.getMetadata(), ClassLoaderSafeConnectorMetadata.class);
        assertInstanceOf(connector.getSplitManager(), ClassLoaderSafeConnectorSplitManager.class);
        assertInstanceOf(connector.getPageSourceProvider(), ConnectorPageSourceProvider.class);
        assertInstanceOf(connector.getHandleResolver(), ClassLoaderSafeConnectorHandleResolver.class);
    }

    private static void assertCreateConnectorFails(String metastoreUri, String exceptionString)
    {
        try {
            assertCreateConnector(metastoreUri);
            fail("expected connector creation to fail:" + metastoreUri);
        }
        catch (RuntimeException e) {
            assertContains(e.getMessage(), exceptionString);
        }
    }
}
