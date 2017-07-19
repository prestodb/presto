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

import com.facebook.presto.connector.ConnectorTestHelper;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.facebook.presto.testing.TestingConnectorContext;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tests.tpch.TpchQueryRunner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;

public class TpchTestHelper
        extends ConnectorTestHelper
{
    @Override
    public Connector getConnector()
            throws IOException
    {
        TpchPlugin plugin = new TpchPlugin();
        Iterable<ConnectorFactory> connectorFactories = plugin.getConnectorFactories();

        Map<String, String> properties = ImmutableMap.of();

        ConnectorFactory factory = getOnlyElement(connectorFactories);
        return factory.create("tpch", properties, new TestingConnectorContext());
    }

    @Override
    public AbstractTestQueryFramework.QueryRunnerSupplier getQueryRunnerSupplier()
    {
        return TpchQueryRunner::createQueryRunner;
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
