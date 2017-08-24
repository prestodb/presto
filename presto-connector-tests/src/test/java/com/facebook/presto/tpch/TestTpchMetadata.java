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
package com.facebook.presto.tpch;

import com.facebook.presto.connector.meta.SupportedFeatures;
import com.facebook.presto.connector.unittest.TestMetadata;
import com.facebook.presto.connector.unittest.TestMetadataSchema;
import com.facebook.presto.connector.unittest.TestMetadataTable;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.facebook.presto.testing.TestingConnectorContext;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;

/*
 * Don't put leaf classes for other connectors in this module. The TPCH
 * connector is special because presto-main depends on presto-tpch for its own
 * testing.  presto-connector-tests depends on presto-main, and so we can't add
 * presto-connector-tests as a dependency of presto-tpch without creating a
 * circular dependency.
 */
@SupportedFeatures({})
public class TestTpchMetadata
        implements TestMetadata, TestMetadataSchema, TestMetadataTable
{
    @Override
    public Connector getConnector()
    {
        TpchPlugin plugin = new TpchPlugin();
        Iterable<ConnectorFactory> connectorFactories = plugin.getConnectorFactories();

        ConnectorFactory factory = getOnlyElement(connectorFactories);
        return factory.create("tpch", ImmutableMap.of(), new TestingConnectorContext());
    }

    @Override
    public Map<String, Object> getTableProperties()
    {
        return ImmutableMap.of();
    }

    @Override
    public List<String> systemSchemas()
    {
        return ImmutableList.of(
                "tiny",
                "sf1",
                "sf100",
                "sf300",
                "sf1000",
                "sf3000",
                "sf10000",
                "sf30000",
                "sf100000");
    }

    private static final List<String> PER_SCHEMA_TABLES = ImmutableList.of(
            "customer",
            "orders",
            "lineitem",
            "part",
            "partsupp",
            "supplier",
            "nation",
            "region");

    @Override
    public List<SchemaTableName> systemTables()
    {
        return systemSchemas().stream()
                .flatMap(schema -> PER_SCHEMA_TABLES.stream().map(table -> new SchemaTableName(schema, table)))
                .collect(toImmutableList());
    }
}
