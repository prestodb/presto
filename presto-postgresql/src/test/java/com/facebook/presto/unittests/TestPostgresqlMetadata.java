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
package com.facebook.presto.unittests;

import com.facebook.presto.connector.meta.SupportedFeatures;
import com.facebook.presto.connector.unittest.TestMetadata;
import com.facebook.presto.connector.unittest.TestMetadataSchema;
import com.facebook.presto.connector.unittest.TestMetadataTable;
import com.facebook.presto.connector.unittest.ValidatesSchema;
import com.facebook.presto.plugin.postgresql.PostgreSqlPlugin;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.facebook.presto.testing.TestingConnectorContext;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.testing.postgresql.TestingPostgreSqlServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

import java.util.List;
import java.util.Map;

import static com.facebook.presto.connector.meta.ConnectorFeature.CREATE_TABLE_AS;
import static com.facebook.presto.connector.meta.ConnectorFeature.DROP_TABLE;
import static com.google.common.collect.Iterables.getOnlyElement;

@SupportedFeatures({
        CREATE_TABLE_AS,
        DROP_TABLE})
public class TestPostgresqlMetadata
        implements TestMetadata, TestMetadataTable, TestMetadataSchema, ValidatesSchema
{
    private static final String PUBLIC = "public";
    private TestingPostgreSqlServer server;
    private Connector connector;

    @BeforeAll
    public void beforeAll()
            throws Exception
    {
        this.server = new TestingPostgreSqlServer("testuser", "unittests");
        this.connector = createPostgresqlConnector(server);
    }

    @AfterAll
    public void cleanUp()
            throws Exception
    {
        connector.shutdown();
        server.close();
    }

    @Override
    public String onlySchemaName()
    {
        return PUBLIC;
    }

    @Override
    public Connector getConnector()
    {
        return connector;
    }

    private static Connector createPostgresqlConnector(TestingPostgreSqlServer server)
            throws Exception
    {
        PostgreSqlPlugin plugin = new PostgreSqlPlugin();
        Iterable<ConnectorFactory> connectorFactories = plugin.getConnectorFactories();

        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("connection-url", server.getJdbcUrl())
                .put("allow-drop-table", "true")
                .build();

        ConnectorFactory factory = getOnlyElement(connectorFactories);
        return factory.create("postgresql", properties, new TestingConnectorContext());
    }

    @Override
    public Map<String, Object> getTableProperties()
    {
        return ImmutableMap.of();
    }

    @Override
    public List<String> systemSchemas()
    {
        return ImmutableList.of("pg_catalog", PUBLIC);
    }
}
