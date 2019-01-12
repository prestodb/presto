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
package io.prestosql.plugin.hive;

import com.google.common.collect.ImmutableMap;
import io.prestosql.spi.connector.Connector;
import io.prestosql.spi.connector.ConnectorPageSourceProvider;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.classloader.ClassLoaderSafeConnectorMetadata;
import io.prestosql.spi.connector.classloader.ClassLoaderSafeConnectorSplitManager;
import io.prestosql.testing.TestingConnectorContext;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;

import static io.airlift.testing.Assertions.assertContains;
import static io.airlift.testing.Assertions.assertInstanceOf;
import static io.prestosql.spi.transaction.IsolationLevel.READ_UNCOMMITTED;
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
                HiveConnector.class.getClassLoader(),
                Optional.empty());

        Map<String, String> config = ImmutableMap.<String, String>builder()
                .put("hive.metastore.uri", metastoreUri)
                .build();

        Connector connector = connectorFactory.create("hive-test", config, new TestingConnectorContext());
        ConnectorTransactionHandle transaction = connector.beginTransaction(READ_UNCOMMITTED, true);
        assertInstanceOf(connector.getMetadata(transaction), ClassLoaderSafeConnectorMetadata.class);
        assertInstanceOf(connector.getSplitManager(), ClassLoaderSafeConnectorSplitManager.class);
        assertInstanceOf(connector.getPageSourceProvider(), ConnectorPageSourceProvider.class);
        connector.commit(transaction);
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
