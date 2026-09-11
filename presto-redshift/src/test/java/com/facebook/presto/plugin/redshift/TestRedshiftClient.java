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
import com.facebook.airlift.json.JsonCodecFactory;
import com.facebook.airlift.json.JsonObjectMapperProvider;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.TestingTypeDeserializer;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.plugin.jdbc.JdbcConnectorId;
import com.facebook.presto.plugin.jdbc.JdbcIdentity;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.analyzer.ViewDefinition;
import com.facebook.presto.testing.TestingConnectorSession;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.h2.Driver;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Locale;
import java.util.Optional;
import java.util.Properties;

import static com.facebook.presto.plugin.redshift.RedshiftClient.ViewRewriteMode.ADD_SCHEMA;
import static com.facebook.presto.plugin.redshift.RedshiftClient.ViewRewriteMode.STRIP_CATALOG;
import static org.testng.Assert.assertEquals;

public class TestRedshiftClient
{
    private static final TestingConnectorSession SESSION = new TestingConnectorSession(ImmutableList.of());
    private static final JdbcIdentity IDENTITY = JdbcIdentity.from(SESSION);

    private static final JsonCodec<ViewDefinition> VIEW_DEFINITION_JSON_CODEC = buildViewDefinitionCodec();
    private static final JsonCodec<ViewDefinition> STUB_VIEW_CODEC = JsonCodec.jsonCodec(ViewDefinition.class);

    private static JsonCodec<ViewDefinition> buildViewDefinitionCodec()
    {
        JsonObjectMapperProvider provider = new JsonObjectMapperProvider();
        provider.setJsonDeserializers(ImmutableMap.of(Type.class, new TestingTypeDeserializer(FunctionAndTypeManager.createTestFunctionAndTypeManager())));
        return new JsonCodecFactory(provider, true).jsonCodec(ViewDefinition.class);
    }

    @DataProvider
    public static Object[][] viewDefinitionCases()
    {
        ImmutableList<ViewDefinition.ViewColumn> columns = ImmutableList.of(new ViewDefinition.ViewColumn("id", BigintType.BIGINT));
        return new Object[][] {
                // definer-security: owner present, runAsInvoker = false
                {new ViewDefinition("SELECT id FROM public.orders", Optional.of("redshift"), Optional.of("public"), columns, Optional.of("test_user"), false)},
                // invoker-security: no owner, runAsInvoker = true
                {new ViewDefinition("SELECT id FROM public.orders", Optional.of("redshift"), Optional.of("public"), columns, Optional.empty(), true)},
        };
    }

    @Test(dataProvider = "viewDefinitionCases")
    public void testViewDefinitionCodecRoundTrip(ViewDefinition expected)
    {
        ViewDefinition actual = VIEW_DEFINITION_JSON_CODEC.fromJson(VIEW_DEFINITION_JSON_CODEC.toJson(expected));

        assertEquals(actual.getOriginalSql(), expected.getOriginalSql());
        assertEquals(actual.getCatalog(), expected.getCatalog());
        assertEquals(actual.getSchema(), expected.getSchema());
        assertEquals(actual.getOwner(), expected.getOwner());
        assertEquals(actual.isRunAsInvoker(), expected.isRunAsInvoker());
        assertEquals(actual.getColumns().size(), expected.getColumns().size());
        for (int i = 0; i < expected.getColumns().size(); i++) {
            assertEquals(actual.getColumns().get(i).getName(), expected.getColumns().get(i).getName());
            assertEquals(actual.getColumns().get(i).getType(), expected.getColumns().get(i).getType());
        }
    }

    @DataProvider
    public static Object[][] stripCatalogCases()
    {
        return new Object[][] {
                // single table
                {
                    "SELECT * FROM \"redshift\".\"public\".\"orders\"",
                    "SELECT * FROM \"public\".\"orders\""
                },
                // multiple tables joined
                {
                    "SELECT * FROM redshift.public.orders o JOIN \"redshift\".\"public\".\"customers\" c ON o.customer_id = c.id",
                    "SELECT * FROM \"public\".\"orders\" o JOIN \"public\".\"customers\" c ON o.customer_id = c.id"
                },
                // CTE - temp is not rewritten; base table inside CTE is
                {
                    "WITH temp AS (SELECT * FROM \"redshift\".public.\"orders\") SELECT * FROM temp",
                    "WITH temp AS (SELECT * FROM \"public\".\"orders\") SELECT * FROM temp"
                },
                // already two-part (no catalog)
                {
                    "SELECT * FROM public.orders",
                    "SELECT * FROM public.orders"
                },
        };
    }

    @Test(dataProvider = "stripCatalogCases")
    public void testRewriteViewSqlStripCatalog(String input, String expected)
            throws SQLException
    {
        try (Connection connection = createH2Connection("public")) {
            assertEquals(createRedshiftClient().rewriteViewSql(SESSION, IDENTITY, connection, input, STRIP_CATALOG), expected);
        }
    }

    @DataProvider
    public static Object[][] crossCatalogCases()
    {
        return new Object[][] {
                // single reference to a foreign catalog
                {"SELECT * FROM \"other_catalog\".\"public\".\"orders\""},
                // mixed: one own-catalog + one foreign
                {"SELECT * FROM \"redshift\".\"public\".\"orders\" o JOIN other_catalog.public.customers c ON o.id = c.id"},
        };
    }

    @Test(dataProvider = "crossCatalogCases",
            expectedExceptions = PrestoException.class,
            expectedExceptionsMessageRegExp = "You can create the view for a table only if that table is in the same catalog\\.")
    public void testRewriteViewSqlStripCatalogRejectsCrossCatalog(String sql)
            throws SQLException
    {
        try (Connection connection = createH2Connection("public")) {
            createRedshiftClient().rewriteViewSql(SESSION, IDENTITY, connection, sql, STRIP_CATALOG);
        }
    }

    @DataProvider
    public static Object[][] addSchemaCases()
    {
        return new Object[][] {
                // unqualified bare name - schema prepended from connection
                {"sales", "SELECT * FROM orders", "SELECT * FROM \"sales\".\"orders\""},
                // unqualified bare name - default H2 schema "public"
                {"public", "SELECT * FROM orders", "SELECT * FROM \"public\".\"orders\""},
                // CTE name must NOT be qualified; only the real table inside it is
                {"public", "WITH temp AS (SELECT * FROM orders) SELECT * FROM temp", "WITH temp AS (SELECT * FROM \"public\".\"orders\") SELECT * FROM temp"},
                // already two-part - no-op (already qualified)
                {"public", "SELECT * FROM public.orders", "SELECT * FROM public.orders"},
                // invalid SQL - returned unchanged
                {"public", "SELECT FROM WHERE", "SELECT FROM WHERE"},
        };
    }

    @Test(dataProvider = "addSchemaCases")
    public void testRewriteViewSqlAddSchema(String schema, String input, String expected)
            throws SQLException
    {
        try (Connection connection = createH2Connection(schema)) {
            assertEquals(createRedshiftClient().rewriteViewSql(SESSION, IDENTITY, connection, input, ADD_SCHEMA), expected);
        }
    }

    private static Connection createH2Connection(String schema)
            throws SQLException
    {
        String h2Url = "jdbc:h2:mem:test_" + System.nanoTime() + ";DATABASE_TO_LOWER=TRUE";
        Connection connection = new Driver().connect(h2Url, new Properties());
        // H2 only accepts SET SCHEMA for schemas that already exist.
        // PUBLIC is always present, any other schema must be created first.
        String normalizedSchema = schema.toLowerCase(Locale.ENGLISH);
        if (!normalizedSchema.equals("public")) {
            try (Statement stmt = connection.createStatement()) {
                stmt.execute("CREATE SCHEMA IF NOT EXISTS " + normalizedSchema);
            }
        }
        connection.setSchema(normalizedSchema);
        return connection;
    }

    private static RedshiftClient createRedshiftClient()
    {
        RedshiftConfig config = new RedshiftConfig();
        config.setConnectionUrl("jdbc:redshift://unused.invalid:5439/dev");
        return new RedshiftClient(new JdbcConnectorId("redshift"), config, STUB_VIEW_CODEC);
    }
}
