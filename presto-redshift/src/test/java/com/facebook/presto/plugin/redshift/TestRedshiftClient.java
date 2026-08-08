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
package com.facebook.presto.plugin.redshift;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.plugin.jdbc.DefaultTableLocationProvider;
import com.facebook.presto.plugin.jdbc.JdbcConnectorId;
import com.facebook.presto.plugin.jdbc.JdbcMetadataCache;
import com.facebook.presto.plugin.jdbc.JdbcMetadataCacheStats;
import com.facebook.presto.plugin.jdbc.JdbcMetadataConfig;
import com.facebook.presto.spi.ConnectorViewDefinition;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.analyzer.ViewDefinition;
import com.facebook.presto.sql.parser.ParsingOptions;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.DefaultTraversalVisitor;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.sql.tree.Table;
import com.facebook.presto.sql.tree.WithQuery;
import com.facebook.presto.testing.TestingConnectorSession;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class TestRedshiftClient
{
    private static final SqlParser SQL_PARSER = new SqlParser();
    private static final ParsingOptions PARSING_OPTIONS = ParsingOptions.builder().build();
    private static final TestingConnectorSession SESSION = new TestingConnectorSession(ImmutableList.of());
    private static final JsonCodec<ViewDefinition> VIEW_DEFINITION_JSON_CODEC = JsonCodec.jsonCodec(ViewDefinition.class);

    @Test
    public void testViewDefinitionCodecSerializesCorrectly()
    {
        ViewDefinition viewDefinition = new ViewDefinition(
                "SELECT id FROM public.orders",
                Optional.of("redshift"),
                Optional.of("public"),
                ImmutableList.of(new ViewDefinition.ViewColumn("id", BigintType.BIGINT)),
                Optional.of("test_user"),
                false);

        String json = VIEW_DEFINITION_JSON_CODEC.toJson(viewDefinition);

        assertTrue(json.contains("\"originalSql\""), "JSON should contain originalSql field");
        assertTrue(json.contains("\"catalog\""), "JSON should contain catalog field");
        assertTrue(json.contains("\"schema\""), "JSON should contain schema field");
        assertTrue(json.contains("\"columns\""), "JSON should contain columns field");
        assertTrue(json.contains("\"owner\""), "JSON should contain owner field");
        assertTrue(json.contains("\"runAsInvoker\""), "JSON should contain runAsInvoker field");
        assertTrue(json.contains("SELECT id FROM public.orders"), "JSON should contain originalSql value");
        assertTrue(json.contains("\"name\" : \"id\""), "JSON should contain column name");
        assertTrue(json.contains("\"type\" : \"bigint\""), "JSON should contain column type as string");
    }

    @Test
    public void testParsesSimpleSelect()
    {
        String sql = "SELECT * FROM orders";
        Statement statement = SQL_PARSER.createStatement(sql, PARSING_OPTIONS);

        assertNotNull(statement, "Parser should successfully parse simple SELECT");
    }

    @Test
    public void testParsesThreePartTableName()
    {
        String sql = "SELECT * FROM \"catalog\".\"schema\".\"table\"";
        Statement statement = SQL_PARSER.createStatement(sql, PARSING_OPTIONS);

        Set<String> tableNames = extractTableNames(statement);
        assertEquals(tableNames.size(), 1);
        assertTrue(tableNames.contains("\"catalog\".\"schema\".\"table\""));
    }

    @Test
    public void testParsesTwoPartTableName()
    {
        String sql = "SELECT * FROM \"schema\".\"table\"";
        Statement statement = SQL_PARSER.createStatement(sql, PARSING_OPTIONS);

        Set<String> tableNames = extractTableNames(statement);
        assertEquals(tableNames.size(), 1);
        assertTrue(tableNames.contains("\"schema\".\"table\""));
    }

    @Test
    public void testParsesOnePartTableName()
    {
        String sql = "SELECT * FROM orders";
        Statement statement = SQL_PARSER.createStatement(sql, PARSING_OPTIONS);

        Set<String> tableNames = extractTableNames(statement);
        assertEquals(tableNames.size(), 1);
        assertTrue(tableNames.contains("orders"));
    }

    @Test
    public void testCteNamesAreIdentified()
    {
        String sql = "WITH temp AS (SELECT 1) SELECT * FROM temp";
        Statement statement = SQL_PARSER.createStatement(sql, PARSING_OPTIONS);

        Set<String> cteNames = extractCteNames(statement);
        assertEquals(cteNames.size(), 1);
        assertTrue(cteNames.contains("temp"));
    }

    @Test
    public void testNestedCtes()
    {
        String sql = "WITH outer_cte AS (WITH inner_cte AS (SELECT 1) SELECT * FROM inner_cte) SELECT * FROM outer_cte";
        Statement statement = SQL_PARSER.createStatement(sql, PARSING_OPTIONS);

        Set<String> cteNames = extractCteNames(statement);
        assertEquals(cteNames.size(), 2);
        assertTrue(cteNames.contains("outer_cte"));
        assertTrue(cteNames.contains("inner_cte"));
    }

    @Test
    public void testMultipleCtes()
    {
        String sql = "WITH cte1 AS (SELECT 1), cte2 AS (SELECT 2) SELECT * FROM cte1 JOIN cte2 ON true";
        Statement statement = SQL_PARSER.createStatement(sql, PARSING_OPTIONS);

        Set<String> cteNames = extractCteNames(statement);
        assertEquals(cteNames.size(), 2);
        assertTrue(cteNames.contains("cte1"));
        assertTrue(cteNames.contains("cte2"));
    }

    @Test
    public void testCteWithRealTable()
    {
        String sql = "WITH temp AS (SELECT * FROM orders) SELECT * FROM temp";
        Statement statement = SQL_PARSER.createStatement(sql, PARSING_OPTIONS);

        Set<String> cteNames = extractCteNames(statement);
        Set<String> tableNames = extractTableNames(statement);

        assertTrue(cteNames.contains("temp"));
        assertTrue(tableNames.contains("orders"));
        assertTrue(tableNames.contains("temp"));
    }

    @Test
    public void testJoinWithMultipleTables()
    {
        String sql = "SELECT * FROM orders o JOIN customers c ON o.customer_id = c.id";
        Statement statement = SQL_PARSER.createStatement(sql, PARSING_OPTIONS);

        Set<String> tableNames = extractTableNames(statement);
        assertEquals(tableNames.size(), 2);
        assertTrue(tableNames.contains("orders"));
        assertTrue(tableNames.contains("customers"));
    }

    @Test
    public void testSubquery()
    {
        String sql = "SELECT * FROM (SELECT * FROM orders) subq";
        Statement statement = SQL_PARSER.createStatement(sql, PARSING_OPTIONS);

        Set<String> tableNames = extractTableNames(statement);
        assertTrue(tableNames.contains("orders"));
    }

    @Test
    public void testUnion()
    {
        String sql = "SELECT * FROM orders UNION ALL SELECT * FROM archived_orders";
        Statement statement = SQL_PARSER.createStatement(sql, PARSING_OPTIONS);

        Set<String> tableNames = extractTableNames(statement);
        assertEquals(tableNames.size(), 2);
        assertTrue(tableNames.contains("orders"));
        assertTrue(tableNames.contains("archived_orders"));
    }

    @Test
    public void testQuotedIdentifiers()
    {
        String sql = "SELECT * FROM \"Orders\"";
        Statement statement = SQL_PARSER.createStatement(sql, PARSING_OPTIONS);

        Set<String> tableNames = extractTableNames(statement);
        assertEquals(tableNames.size(), 1);
        assertTrue(tableNames.contains("\"Orders\""));
    }

    @Test
    public void testMixedQuotedAndUnquoted()
    {
        String sql = "SELECT * FROM \"Orders\" o JOIN customers c ON o.customer_id = c.customer_id";
        Statement statement = SQL_PARSER.createStatement(sql, PARSING_OPTIONS);

        Set<String> tableNames = extractTableNames(statement);
        assertEquals(tableNames.size(), 2);
        assertTrue(tableNames.contains("\"Orders\""));
        assertTrue(tableNames.contains("customers"));
    }

    @Test
    public void testFederatedQueryDetectionMultipleCatalogs()
    {
        String sql = "SELECT * FROM catalog1.schema.table1 t1 JOIN catalog2.schema.table2 t2 ON t1.col=t2.col";

        assertTrue(isFederatedQuery(sql, "catalog1"),
                "Query with multiple catalogs should be detected as federated");
    }

    @Test
    public void testFederatedQueryDetectionSameCatalog()
    {
        String sql = "SELECT * FROM catalog1.schema.table1 t1 JOIN catalog1.schema.table2 t2 ON t1.col=t2.col";

        assertFalse(isFederatedQuery(sql, "catalog1"),
                "Query with same catalog should not be federated");
    }

    @Test
    public void testFederatedQueryDetectionNoCatalog()
    {
        String sql = "SELECT * FROM schema.table1 t1 JOIN schema.table2 t2 ON t1.col=t2.col";

        assertFalse(isFederatedQuery(sql, "catalog1"),
                "Query without catalog prefix should not be federated");
    }

    @Test
    public void testComplexQueryWithWindowFunction()
    {
        String sql = "SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date) FROM orders";
        Statement statement = SQL_PARSER.createStatement(sql, PARSING_OPTIONS);

        Set<String> tableNames = extractTableNames(statement);
        assertEquals(tableNames.size(), 1);
        assertTrue(tableNames.contains("orders"));
    }

    @Test
    public void testComplexQueryWithGroupBy()
    {
        String sql = "SELECT customer_id, COUNT(*) FROM orders GROUP BY customer_id HAVING COUNT(*) > 5";
        Statement statement = SQL_PARSER.createStatement(sql, PARSING_OPTIONS);

        Set<String> tableNames = extractTableNames(statement);
        assertEquals(tableNames.size(), 1);
        assertTrue(tableNames.contains("orders"));
    }

    @Test
    public void testGetViewsShortCircuitsToEmptyForSpecificViewWhenDatasourceManaged()
    {
        RedshiftMetadata metadata = metadataWithDatasourceManagedViews(true);

        Map<SchemaTableName, ConnectorViewDefinition> views =
                metadata.getViews(SESSION, new SchemaTablePrefix("test_schema", "test_view"));

        assertNotNull(views, "views should not be null");
        assertTrue(views.isEmpty(),
                "getViews must short-circuit to an empty map before touching "
                        + "redshiftClient when datasource-managed views is enabled");
    }

    @Test
    public void testGetViewsShortCircuitsToEmptyForWholeSchemaWhenDatasourceManaged()
    {
        RedshiftMetadata metadata = metadataWithDatasourceManagedViews(true);

        Map<SchemaTableName, ConnectorViewDefinition> views =
                metadata.getViews(SESSION, new SchemaTablePrefix("test_schema"));

        assertNotNull(views, "views should not be null");
        assertTrue(views.isEmpty(),
                "Schema-level getViews must also short-circuit to an empty map");
    }

    @Test
    public void testGetViewsShortCircuitsToEmptyForCatalogWideQueryWhenDatasourceManaged()
    {
        RedshiftMetadata metadata = metadataWithDatasourceManagedViews(true);

        Map<SchemaTableName, ConnectorViewDefinition> views =
                metadata.getViews(SESSION, new SchemaTablePrefix());

        assertNotNull(views, "views should not be null");
        assertTrue(views.isEmpty(),
                "Catalog-wide (empty prefix) getViews must also short-circuit "
                        + "to an empty map");
    }

    private static RedshiftMetadata metadataWithDatasourceManagedViews(boolean enabled)
    {
        RedshiftConfig config = new RedshiftConfig();
        config.setDatasourceManagedViewsEnabled(enabled);
        // A syntactically valid but unreachable URL, never dialed for these tests.
        config.setConnectionUrl("jdbc:redshift://unused.invalid:5439/dev");

        RedshiftClient client = new RedshiftClient(new JdbcConnectorId("test"), config, VIEW_DEFINITION_JSON_CODEC);
        JdbcMetadataCache cache = new JdbcMetadataCache(client, new JdbcMetadataConfig(), new JdbcMetadataCacheStats());

        return new RedshiftMetadata(
                cache,
                client,
                false,
                new DefaultTableLocationProvider(config),
                config);
    }

    private Set<String> extractTableNames(Statement statement)
    {
        Set<String> tableNames = new HashSet<>();
        new DefaultTraversalVisitor<Void, Void>()
        {
            @Override
            protected Void visitTable(Table node, Void context)
            {
                List<Identifier> parts = node.getName().getOriginalParts();
                String tableName = parts.stream()
                        .map(Object::toString)
                        .collect(Collectors.joining("."));
                tableNames.add(tableName);
                return super.visitTable(node, context);
            }
        }.process(statement, null);
        return tableNames;
    }

    private Set<String> extractCteNames(Statement statement)
    {
        Set<String> cteNames = new HashSet<>();
        new DefaultTraversalVisitor<Void, Void>()
        {
            @Override
            protected Void visitWithQuery(WithQuery node, Void context)
            {
                cteNames.add(node.getName().getValue());
                return super.visitWithQuery(node, context);
            }
        }.process(statement, null);
        return cteNames;
    }

    private boolean isFederatedQuery(String sql, String currentCatalog)
    {
        Statement statement = SQL_PARSER.createStatement(sql, PARSING_OPTIONS);
        Set<String> sourceCatalogs = new HashSet<>();

        new DefaultTraversalVisitor<Void, Void>()
        {
            @Override
            protected Void visitTable(Table node, Void context)
            {
                List<String> parts = node.getName().getParts();
                if (parts.size() == 3) {
                    sourceCatalogs.add(parts.get(0));
                }
                return super.visitTable(node, context);
            }
        }.process(statement, null);

        return sourceCatalogs.size() > 1 ||
                (sourceCatalogs.size() == 1 && !sourceCatalogs.contains(currentCatalog));
    }
}
