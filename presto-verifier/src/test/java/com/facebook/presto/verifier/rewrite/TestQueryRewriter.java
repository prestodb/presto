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
package com.facebook.presto.verifier.rewrite;

import com.facebook.presto.sql.parser.ParsingOptions;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.parser.SqlParserOptions;
import com.facebook.presto.sql.tree.CreateTableAsSelect;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.tests.StandaloneQueryRunner;
import com.facebook.presto.verifier.framework.ClusterType;
import com.facebook.presto.verifier.framework.QueryBundle;
import com.facebook.presto.verifier.framework.QueryConfiguration;
import com.facebook.presto.verifier.framework.QueryObjectBundle;
import com.facebook.presto.verifier.framework.VerificationContext;
import com.facebook.presto.verifier.framework.VerifierConfig;
import com.facebook.presto.verifier.prestoaction.JdbcPrestoAction;
import com.facebook.presto.verifier.prestoaction.JdbcUrlSelector;
import com.facebook.presto.verifier.prestoaction.PrestoAction;
import com.facebook.presto.verifier.prestoaction.PrestoActionConfig;
import com.facebook.presto.verifier.prestoaction.PrestoExceptionClassifier;
import com.facebook.presto.verifier.prestoaction.QueryActionsConfig;
import com.facebook.presto.verifier.retry.RetryConfig;
import com.google.common.collect.ImmutableList;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.sql.SqlFormatter.formatSql;
import static com.facebook.presto.sql.parser.IdentifierSymbol.AT_SIGN;
import static com.facebook.presto.sql.parser.IdentifierSymbol.COLON;
import static com.facebook.presto.sql.parser.ParsingOptions.DecimalLiteralTreatment.AS_DOUBLE;
import static com.facebook.presto.verifier.VerifierTestUtil.CATALOG;
import static com.facebook.presto.verifier.VerifierTestUtil.SCHEMA;
import static com.facebook.presto.verifier.VerifierTestUtil.createTypeManager;
import static com.facebook.presto.verifier.VerifierTestUtil.setupPresto;
import static com.facebook.presto.verifier.framework.ClusterType.CONTROL;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Test
public class TestQueryRewriter
{
    private static final String SUITE = "test-suite";
    private static final String NAME = "test-query";
    private static final QueryConfiguration CONFIGURATION = new QueryConfiguration(CATALOG, SCHEMA, Optional.of("user"), Optional.empty(), Optional.empty());
    private static final ParsingOptions PARSING_OPTIONS = ParsingOptions.builder().setDecimalLiteralTreatment(AS_DOUBLE).build();
    private static final QueryRewriteConfig QUERY_REWRITE_CONFIG = new QueryRewriteConfig()
            .setTablePrefix("local.tmp")
            .setTableProperties("{\"p_int\": 30, \"p_long\": 4294967297, \"p_double\": 1.5, \"p_varchar\": \"test\", \"p_bool\": true}");
    private static final SqlParser sqlParser = new SqlParser(new SqlParserOptions().allowIdentifierSymbol(COLON, AT_SIGN));

    private static StandaloneQueryRunner queryRunner;
    private static PrestoAction prestoAction;

    @BeforeClass
    public void setup()
            throws Exception
    {
        queryRunner = setupPresto();
        queryRunner.execute("CREATE TABLE test_table (a bigint, b varchar)");
        PrestoActionConfig prestoActionConfig = new PrestoActionConfig()
                .setHosts(queryRunner.getServer().getAddress().getHost())
                .setJdbcPort(queryRunner.getServer().getAddress().getPort());
        prestoAction = new JdbcPrestoAction(
                PrestoExceptionClassifier.defaultBuilder().build(),
                CONFIGURATION,
                VerificationContext.create(SUITE, NAME),
                new JdbcUrlSelector(prestoActionConfig.getJdbcUrls()),
                prestoActionConfig,
                new QueryActionsConfig().getMetadataTimeout(),
                new QueryActionsConfig().getChecksumTimeout(),
                new RetryConfig(),
                new RetryConfig(),
                new VerifierConfig().setTestId("test"));
    }

    @AfterClass
    public void teardown()
    {
        queryRunner.close();
    }

    @Test
    public void testRewriteSelect()
    {
        assertShadowed(
                getQueryRewriter(),
                "SELECT " +
                        "1 x, " +
                        "2 y, " +
                        "3 \"X?p$7\", " +
                        "4 \"x\", " +
                        "5 x_p_7, " +
                        "6 a, " +
                        "* " +
                        "FROM test_table",
                "local.tmp",
                ImmutableList.of(),
                "CREATE TABLE %s( \"x\", \"y\", \"x_p_7\", \"x__1\", \"x_p_7__1\", \"a\", \"a__1\", \"b\" )\n" +
                        "WITH (\n" +
                        "   p_int = 30,\n" +
                        "   p_long = 4294967297,\n" +
                        "   p_double = 1.5E0,\n" +
                        "   p_varchar = 'test',\n" +
                        "   p_bool = true\n" +
                        ") AS SELECT\n" +
                        "  1 x\n" +
                        ", 2 y\n" +
                        ", 3 \"X?p$7\"\n" +
                        ", 4 \"x\"\n" +
                        ", 5 x_p_7\n" +
                        ", 6 a\n" +
                        ", *\n" +
                        "FROM\n" +
                        "  test_table",
                ImmutableList.of("DROP TABLE IF EXISTS %s"));

        assertShadowed(
                getQueryRewriter(),
                "SELECT * FROM test_table a CROSS JOIN test_table b",
                "local.tmp",
                ImmutableList.of(),
                "CREATE TABLE %s( \"a\", \"b\", \"a__1\", \"b__1\" )\n" +
                        "WITH (\n" +
                        "   p_int = 30,\n" +
                        "   p_long = 4294967297,\n" +
                        "   p_double = 1.5E0,\n" +
                        "   p_varchar = 'test',\n" +
                        "   p_bool = true\n" +
                        ") AS SELECT *\n" +
                        "FROM\n" +
                        "  (test_table a\n" +
                        "CROSS JOIN test_table b)",
                ImmutableList.of("DROP TABLE IF EXISTS %s"));
    }

    @Test
    public void testRewriteInsert()
    {
        assertShadowed(
                getQueryRewriter(),
                "INSERT INTO dest_table SELECT * FROM test_table",
                "local.tmp",
                ImmutableList.of("CREATE TABLE %s (\n" +
                        "   LIKE dest_table INCLUDING PROPERTIES\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   p_int = 30,\n" +
                        "   p_long = 4294967297,\n" +
                        "   p_double = 1.5E0,\n" +
                        "   p_varchar = 'test',\n" +
                        "   p_bool = true\n" +
                        ")"),
                "INSERT INTO %s SELECT * FROM test_table",
                ImmutableList.of("DROP TABLE IF EXISTS %s"));
    }

    @Test
    public void testRewriteCreateTableAsSelect()
    {
        assertShadowed(
                getQueryRewriter(),
                "CREATE TABLE dest_table WITH (test_property = 90) AS SELECT * FROM test_table",
                "local.tmp",
                ImmutableList.of(),
                "CREATE TABLE %s\n" +
                        "WITH (\n" +
                        "   p_varchar = 'test',\n" +
                        "   p_long = 4294967297,\n" +
                        "   p_int = 30,\n" +
                        "   test_property = 90,\n" +
                        "   p_double = 1.5E0,\n" +
                        "   p_bool = true\n" +
                        ") AS SELECT *\n" +
                        "FROM\n" +
                        "  test_table",
                ImmutableList.of("DROP TABLE IF EXISTS %s"));
    }

    @Test
    public void testTemporaryTableName()
    {
        QueryRewriter tableNameRewriter = getQueryRewriter(new QueryRewriteConfig().setTablePrefix("tmp"));
        QueryRewriter schemaRewriter = getQueryRewriter(new QueryRewriteConfig().setTablePrefix("local.tmp"));
        QueryRewriter catalogRewriter = getQueryRewriter(new QueryRewriteConfig().setTablePrefix("verifier_batch.local.tmp"));

        @Language("SQL") String query = "INSERT INTO dest_table SELECT * FROM test_table";
        assertTableName(tableNameRewriter, query, "tmp_");
        assertTableName(schemaRewriter, query, "local.tmp_");
        assertTableName(catalogRewriter, query, "verifier_batch.local.tmp_");

        query = "INSERT INTO default.dest_table SELECT * FROM test_table";
        assertTableName(tableNameRewriter, query, "default.tmp_");
        assertTableName(schemaRewriter, query, "local.tmp_");
        assertTableName(catalogRewriter, query, "verifier_batch.local.tmp_");

        query = "INSERT INTO verifier.default.dest_table SELECT * FROM test_table";
        assertTableName(tableNameRewriter, query, "verifier.default.tmp_");
        assertTableName(schemaRewriter, query, "verifier.local.tmp_");
        assertTableName(catalogRewriter, query, "verifier_batch.local.tmp_");
    }

    @Test
    public void testRewriteDate()
    {
        QueryBundle queryBundle = getQueryRewriter().rewriteQuery("SELECT date '2020-01-01', date(now()) today", CONTROL);
        assertCreateTableAs(queryBundle.getQuery(), "SELECT\n" +
                "  CAST(date '2020-01-01' AS timestamp)\n" +
                ", CAST(date(now()) AS timestamp) today");
    }

    @Test
    public void testRewriteTime()
    {
        QueryBundle queryBundle = getQueryRewriter().rewriteQuery("SELECT time '12:34:56', time '12:34:56' now", CONTROL);
        assertCreateTableAs(queryBundle.getQuery(), "SELECT\n" +
                "  CAST(time '12:34:56' AS timestamp)\n" +
                ", CAST(time '12:34:56' AS timestamp) now");
    }

    @Test
    public void testRewriteTimestampWithTimeZone()
    {
        QueryBundle queryBundle = getQueryRewriter().rewriteQuery("SELECT now(), now() now", CONTROL);
        assertCreateTableAs(queryBundle.getQuery(), "SELECT\n" +
                "  CAST(now() AS varchar)\n" +
                ", CAST(now() AS varchar) now");
    }

    @Test
    public void testRewriteUnknown()
    {
        QueryBundle queryBundle = getQueryRewriter().rewriteQuery("SELECT null, null unknown", CONTROL);
        assertCreateTableAs(queryBundle.getQuery(), "SELECT\n" +
                "  CAST(null AS bigint)\n" +
                ", CAST(null AS bigint) unknown");
    }

    @Test
    public void testRewriteDecimal()
    {
        QueryBundle queryBundle = getQueryRewriter().rewriteQuery("SELECT decimal '1.2', decimal '1.2' d", CONTROL);
        assertCreateTableAs(queryBundle.getQuery(), "SELECT\n" +
                "  CAST(decimal '1.2' AS double)\n" +
                ", CAST(decimal '1.2' AS double) d");
    }

    @Test
    public void testRewriteNonStorableStructuredTypes()
    {
        QueryBundle queryBundle = getQueryRewriter().rewriteQuery(
                "SELECT\n" +
                        "    ARRAY[DATE '2020-01-01'],\n" +
                        "    ARRAY[NULL],\n" +
                        "    MAP(\n" +
                        "        ARRAY[DATE '2020-01-01'], ARRAY[\n" +
                        "            CAST(ROW(1, 'a', DATE '2020-01-01') AS ROW(x int, y VARCHAR, z date))\n" +
                        "        ]\n" +
                        "    ),\n" +
                        "    ROW(NULL)",
                CONTROL);
        assertCreateTableAs(
                queryBundle.getQuery(),
                "SELECT\n" +
                        "    CAST(ARRAY[DATE '2020-01-01'] AS ARRAY(timestamp)),\n" +
                        "    CAST(ARRAY[NULL] AS ARRAY(BIGINT)),\n" +
                        "    CAST(\n" +
                        "        \"map\"(\n" +
                        "            ARRAY[DATE '2020-01-01'],\n" +
                        "            ARRAY[\n" +
                        "                CAST(ROW (1, 'a', DATE '2020-01-01') AS ROW(x int, y VARCHAR, z date))\n" +
                        "            ]\n" +
                        "        ) AS MAP(timestamp, ROW(x INTEGER, y VARCHAR, z timestamp))\n" +
                        "    ),\n" +
                        "    CAST(ROW (NULL) AS ROW(BIGINT))");
    }

    private void assertShadowed(
            QueryRewriter queryRewriter,
            @Language("SQL") String query,
            String prefix,
            List<String> expectedSetupTemplates,
            @Language("SQL") String expectedTemplates,
            List<String> expectedTeardownTemplates)
    {
        for (ClusterType cluster : ClusterType.values()) {
            QueryObjectBundle bundle = queryRewriter.rewriteQuery(query, cluster);

            String tableName = bundle.getObjectName().toString();
            assertTrue(tableName.startsWith(prefix + "_"));

            assertStatements(bundle.getSetupQueries(), templateToStatements(expectedSetupTemplates, tableName));
            assertStatements(ImmutableList.of(bundle.getQuery()), templateToStatements(ImmutableList.of(expectedTemplates), tableName));
            assertStatements(bundle.getTeardownQueries(), templateToStatements(expectedTeardownTemplates, tableName));
        }
    }

    private static void assertTableName(QueryRewriter queryRewriter, @Language("SQL") String query, String expectedPrefix)
    {
        QueryObjectBundle bundle = queryRewriter.rewriteQuery(query, CONTROL);
        assertTrue(bundle.getObjectName().toString().startsWith(expectedPrefix));
    }

    private static void assertCreateTableAs(Statement statement, String selectQuery)
    {
        assertTrue(statement instanceof CreateTableAsSelect);
        Query query = ((CreateTableAsSelect) statement).getQuery();
        assertEquals(formatSql(query, Optional.empty()), formatSql(sqlParser.createStatement(selectQuery, PARSING_OPTIONS), Optional.empty()));
    }

    private static List<Statement> templateToStatements(List<String> templates, String tableName)
    {
        return templates.stream()
                .map(template -> format(template, tableName))
                .map(query -> sqlParser.createStatement(query, PARSING_OPTIONS))
                .collect(toImmutableList());
    }

    private static void assertStatements(List<Statement> actual, List<Statement> expected)
    {
        List<String> actualQueries = actual.stream()
                .map(statement -> formatSql(statement, Optional.empty()))
                .collect(toImmutableList());
        List<String> expectedQueries = expected.stream()
                .map(statement -> formatSql(statement, Optional.empty()))
                .collect(toImmutableList());
        assertEquals(actualQueries, expectedQueries);
    }

    private QueryRewriter getQueryRewriter()
    {
        return getQueryRewriter(QUERY_REWRITE_CONFIG);
    }

    private QueryRewriter getQueryRewriter(QueryRewriteConfig config)
    {
        return new VerificationQueryRewriterFactory(sqlParser, createTypeManager(), config, config).create(prestoAction);
    }
}
