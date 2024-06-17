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
import com.facebook.presto.verifier.prestoaction.DefaultClientInfoFactory;
import com.facebook.presto.verifier.prestoaction.JdbcPrestoAction;
import com.facebook.presto.verifier.prestoaction.JdbcUrlSelector;
import com.facebook.presto.verifier.prestoaction.PrestoAction;
import com.facebook.presto.verifier.prestoaction.PrestoActionConfig;
import com.facebook.presto.verifier.prestoaction.PrestoExceptionClassifier;
import com.facebook.presto.verifier.prestoaction.QueryActionsConfig;
import com.facebook.presto.verifier.retry.RetryConfig;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
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
import static com.facebook.presto.verifier.framework.ClusterType.TEST;
import static com.facebook.presto.verifier.rewrite.FunctionCallRewriter.FunctionCallSubstitute;
import static com.facebook.presto.verifier.rewrite.FunctionCallRewriter.validateAndConstructFunctionCallSubstituteMap;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertEqualsDeep;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

@Test
public class TestQueryRewriter
{
    private static final String SUITE = "test-suite";
    private static final String NAME = "test-query";
    private static final QueryConfiguration CONFIGURATION = new QueryConfiguration(
            CATALOG,
            SCHEMA,
            Optional.of("user"),
            Optional.empty(),
            Optional.empty(),
            Optional.empty());
    private static final ParsingOptions PARSING_OPTIONS = ParsingOptions.builder().setDecimalLiteralTreatment(AS_DOUBLE).build();
    private static final QueryRewriteConfig QUERY_REWRITE_CONFIG = new QueryRewriteConfig()
            .setTablePrefix("local.tmp")
            .setTableProperties("{\"p_int\": 30, \"p_long\": 4294967297, \"p_double\": 1.5, \"p_varchar\": \"test\", \"p_bool\": true}");
    private static final VerifierConfig VERIFIER_CONFIG = new VerifierConfig();
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
                new DefaultClientInfoFactory(new VerifierConfig().setTestId("test")));
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
        QueryRewriter tableNameRewriter = getQueryRewriter(new QueryRewriteConfig().setTablePrefix("tmp"), VERIFIER_CONFIG);
        QueryRewriter schemaRewriter = getQueryRewriter(new QueryRewriteConfig().setTablePrefix("local.tmp"), VERIFIER_CONFIG);
        QueryRewriter catalogRewriter = getQueryRewriter(new QueryRewriteConfig().setTablePrefix("verifier_batch.local.tmp"), VERIFIER_CONFIG);

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
        QueryBundle queryBundle = getQueryRewriter().rewriteQuery("SELECT date '2020-01-01', date(now()) today", CONFIGURATION, CONTROL);
        assertCreateTableAs(queryBundle.getQuery(), "SELECT\n" +
                "  CAST(date '2020-01-01' AS timestamp)\n" +
                ", CAST(date(now()) AS timestamp) today");
    }

    @Test
    public void testRewriteTime()
    {
        QueryBundle queryBundle = getQueryRewriter().rewriteQuery("SELECT time '12:34:56', time '12:34:56' now", CONFIGURATION, CONTROL);
        assertCreateTableAs(queryBundle.getQuery(), "SELECT\n" +
                "  CAST(time '12:34:56' AS timestamp)\n" +
                ", CAST(time '12:34:56' AS timestamp) now");
    }

    @Test
    public void testRewriteTimestampWithTimeZone()
    {
        QueryBundle queryBundle = getQueryRewriter().rewriteQuery("SELECT now(), now() now", CONFIGURATION, CONTROL);
        assertCreateTableAs(queryBundle.getQuery(), "SELECT\n" +
                "  CAST(now() AS varchar)\n" +
                ", CAST(now() AS varchar) now");
    }

    @Test
    public void testRewriteUnknown()
    {
        QueryBundle queryBundle = getQueryRewriter().rewriteQuery("SELECT null, null unknown", CONFIGURATION, CONTROL);
        assertCreateTableAs(queryBundle.getQuery(), "SELECT\n" +
                "  CAST(null AS bigint)\n" +
                ", CAST(null AS bigint) unknown");
    }

    @Test
    public void testRewriteDecimal()
    {
        QueryBundle queryBundle = getQueryRewriter().rewriteQuery("SELECT decimal '1.2', decimal '1.2' d", CONFIGURATION, CONTROL);
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
                CONFIGURATION, CONTROL);
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

    @Test
    public void testConstructMakeFunctionCallSubstituteMap()
    {
        SqlParser sqlParser = new SqlParser();

        assertEqualsDeep(validateAndConstructFunctionCallSubstituteMap("/max_by(a,_)/max(a)/").asMap(),
                ImmutableMultimap.of("max_by", new FunctionCallSubstitute(sqlParser.createExpression("max_by(a,_)", PARSING_OPTIONS), sqlParser.createExpression("max(a)", PARSING_OPTIONS))).asMap());
        assertEqualsDeep(validateAndConstructFunctionCallSubstituteMap("/approx_percentile(a,0.1)/avg(a)/,/approx_percentile(a,array[0.1,0.1])/array[avg(a),max(a)]/").asMap(),
                ImmutableMultimap.of("approx_percentile", new FunctionCallSubstitute(sqlParser.createExpression("approx_percentile(a,0.1)", PARSING_OPTIONS), sqlParser.createExpression("avg(a)", PARSING_OPTIONS)),
                        "approx_percentile", new FunctionCallSubstitute(sqlParser.createExpression("approx_percentile(a,array[0.1,0.1])", PARSING_OPTIONS), sqlParser.createExpression("array[avg(a),max(a)]", PARSING_OPTIONS))).asMap());
        assertEqualsDeep(validateAndConstructFunctionCallSubstituteMap("").asMap(),
                ImmutableMultimap.of().asMap());

        assertThrows(IllegalArgumentException.class, () -> validateAndConstructFunctionCallSubstituteMap("/func1(a,b)//"));
        assertThrows(IllegalArgumentException.class, () -> validateAndConstructFunctionCallSubstituteMap("/func1(,,c1)/func2(c1)/"));
        assertThrows(IllegalArgumentException.class, () -> validateAndConstructFunctionCallSubstituteMap("/func1(c0,0.1)/row(1,true)/"));
    }

    @Test
    public void testRewriteFunctionCalls()
    {
        VerifierConfig verifierConfig = new VerifierConfig().setFunctionSubstitutes(
                "/approx_distinct(x)/count(x)/," +
                        "/approx_percentile(x,array[0.9])/repeat(avg(x),cast(cardinality(array[0.9]) as integer))/," +
                        "/approx_percentile(x,_)/avg(x)/," +
                        "/arbitrary(x)/min(x)/," +
                        "/array_agg(x)/if(typeof(arbitrary(x))='integer', array_sort(array_agg(x)), array_agg(x))/," +
                        "/current_timestamp/timestamp '2023-01-01 00:00:00 UTC'/," +
                        "/first_value(x)/if(min(x) is not null, min(x), max(x))/," +
                        "/max_by(x,_)/max(x)/," +
                        "/map_agg(x,y)/transform_values(multimap_agg(x,y),(k,v)->array_max(v))/," +
                        "/min_by(x,_)/min(x)/," +
                        "/now()/date_trunc('day',now())/," +
                        "/rand()/1/," +
                        "/row_number() over (partition by x order by y)/row_number() over (partition by y)/");
        QueryRewriter queryRewriter = getQueryRewriter(new QueryRewriteConfig(), verifierConfig);

        // Test rewriting nested function calls.
        assertCreateTableAs(
                queryRewriter.rewriteQuery(
                        "SELECT\n" +
                                "    TRIM(ARBITRARY(b))\n" +
                                "FROM test_table",
                        CONFIGURATION, CONTROL).getQuery(),
                "SELECT\n" +
                        "    TRIM(MIN(b))\n" +
                        "FROM test_table");

        // Test rewriting with nested function calls.
        assertCreateTableAs(
                queryRewriter.rewriteQuery(
                        "SELECT\n" +
                                "    MAP_AGG(a,b)\n" +
                                "FROM test_table",
                        CONFIGURATION, CONTROL).getQuery(),
                "SELECT\n" +
                        "    TRANSFORM_VALUES(MULTIMAP_AGG(a,b),(k,v)->ARRAY_MAX(v))\n" +
                        "FROM test_table");

        // Test rewriting with literal.
        assertCreateTableAs(
                queryRewriter.rewriteQuery(
                        "SELECT RAND()",
                        CONFIGURATION, CONTROL).getQuery(),
                "SELECT 1");

        // Test rewriting with if expression.
        assertCreateTableAs(
                queryRewriter.rewriteQuery(
                        "SELECT\n" +
                                "    ARRAY_AGG(DISTINCT a)\n" +
                                "FROM test_table",
                        CONFIGURATION, CONTROL).getQuery(),
                "SELECT\n" +
                        "    IF(TYPEOF(ARBITRARY(DISTINCT a))='integer', ARRAY_SORT(ARRAY_AGG(DISTINCT a)), ARRAY_AGG(DISTINCT a))\n" +
                        "FROM test_table");

        // Test rewriting CurrentTime function.
        assertCreateTableAs(
                queryRewriter.rewriteQuery(
                        "SELECT\n" +
                                "    TO_UNIXTIME(CURRENT_TIMESTAMP)\n" +
                                "FROM test_table",
                        CONFIGURATION, CONTROL).getQuery(),
                "SELECT\n" +
                        "    TO_UNIXTIME(TIMESTAMP '2023-01-01 00:00:00 UTC')\n" +
                        "FROM test_table");

        // Test rewriting NOW function.
        assertCreateTableAs(
                queryRewriter.rewriteQuery(
                        "SELECT\n" +
                                "    TO_UNIXTIME(NOW())\n" +
                                "FROM test_table",
                        CONFIGURATION, CONTROL).getQuery(),
                "SELECT\n" +
                        "    TO_UNIXTIME(DATE_TRUNC('day',NOW()))\n" +
                        "FROM test_table");

        // Test rewriting columns in Join.
        assertCreateTableAs(
                queryRewriter.rewriteQuery(
                        "SELECT *\n" +
                                "FROM test_table x\n" +
                                "JOIN (\n" +
                                "    SELECT\n" +
                                "        b,\n" +
                                "        APPROX_PERCENTILE(a, 0.5) AS a\n" +
                                "    FROM test_table\n" +
                                "    GROUP BY\n" +
                                "        1\n" +
                                ") y\n" +
                                "    ON (x.b = y.b)",
                        CONFIGURATION, CONTROL).getQuery(),
                "SELECT *\n" +
                        "FROM test_table x\n" +
                        "JOIN (\n" +
                        "    SELECT\n" +
                        "        b,\n" +
                        "        AVG(a) AS a\n" +
                        "    FROM test_table\n" +
                        "    GROUP BY\n" +
                        "        1\n" +
                        ") y\n" +
                        "    ON (x.b = y.b)");

        // Test rewriting columns in SubqueryExpression.
        assertCreateTableAs(
                queryRewriter.rewriteQuery(
                        "SELECT a, b\n" +
                                "FROM test_table\n" +
                                "WHERE a IN (\n" +
                                "    SELECT\n" +
                                "        ARBITRARY(a)\n" +
                                "    FROM test_table\n" +
                                ")",
                        CONFIGURATION, CONTROL).getQuery(),
                "SELECT a, b\n" +
                        "FROM test_table\n" +
                        "WHERE a IN (\n" +
                        "    SELECT\n" +
                        "        MIN(a)\n" +
                        "    FROM test_table\n" +
                        ")");

        // Test rewriting columns in TableSubquery.
        assertCreateTableAs(
                queryRewriter.rewriteQuery(
                        "SELECT num\n" +
                                "FROM (\n" +
                                "    SELECT\n" +
                                "        APPROX_DISTINCT(b) AS num\n" +
                                "    FROM test_table\n" +
                                ") x",
                        CONFIGURATION, CONTROL).getQuery(),
                "SELECT num\n" +
                        "FROM (\n" +
                        "    SELECT\n" +
                        "        COUNT(b) AS num\n" +
                        "    FROM test_table\n" +
                        ") x");

        // Test rewriting columns in With.
        assertCreateTableAs(
                queryRewriter.rewriteQuery(
                        "WITH x AS (\n" +
                                "    SELECT\n" +
                                "        MAX_BY(a, b) AS a\n" +
                                "    FROM test_table\n" +
                                ")\n" +
                                "SELECT\n" +
                                "    a\n" +
                                "FROM x", CONFIGURATION, CONTROL).getQuery(),
                "WITH x AS (\n" +
                        "    SELECT\n" +
                        "        MAX(a) AS a\n" +
                        "    FROM test_table\n" +
                        ")\n" +
                        "SELECT\n" +
                        "    a\n" +
                        "FROM x");

        // Test rewriting columns in Union.
        assertCreateTableAs(
                queryRewriter.rewriteQuery(
                        "SELECT\n" +
                                "    ARBITRARY(a)\n" +
                                "FROM (\n" +
                                "    SELECT \n" +
                                "       MIN_BY(a, b) AS a\n" +
                                "    FROM test_table\n" +
                                "\n" +
                                "    UNION ALL\n" +
                                "\n" +
                                "    SELECT \n" +
                                "       MIN_BY(a, b) AS a\n" +
                                "    FROM test_table\n" +
                                ") x",
                        CONFIGURATION, CONTROL).getQuery(),
                "SELECT\n" +
                        "    MIN(a)\n" +
                        "FROM (\n" +
                        "    SELECT \n" +
                        "       MIN(a) AS a\n" +
                        "    FROM test_table\n" +
                        "\n" +
                        "    UNION ALL\n" +
                        "\n" +
                        "    SELECT \n" +
                        "       MIN(a) AS a\n" +
                        "    FROM test_table\n" +
                        ") x");

        // Test rewriting window functions with partition and order derived from the original.
        assertCreateTableAs(
                queryRewriter.rewriteQuery(
                        "SELECT\n" +
                                "    FIRST_VALUE(a) OVER (\n" +
                                "        PARTITION BY b\n" +
                                "    )\n" +
                                "FROM test_table",
                        CONFIGURATION, CONTROL).getQuery(),
                "SELECT\n" +
                        "    IF(\n" +
                        "        MIN(a) OVER (\n" +
                        "            PARTITION BY\n" +
                        "                b\n" +
                        "        ) IS NOT NULL,\n" +
                        "        MIN(a) OVER (\n" +
                        "            PARTITION BY\n" +
                        "                b\n" +
                        "        ),\n" +
                        "        MAX(a) OVER (\n" +
                        "            PARTITION BY\n" +
                        "                b\n" +
                        "        )\n" +
                        "    )\n" +
                        "FROM test_table");

        // Test rewriting window functions with partition and order resolving.
        assertCreateTableAs(
                queryRewriter.rewriteQuery(
                        "SELECT\n" +
                                "    ROW_NUMBER() OVER (\n" +
                                "        PARTITION BY a\n" +
                                "        ORDER BY b DESC\n" +
                                "    )\n" +
                                "FROM test_table",
                        CONFIGURATION, CONTROL).getQuery(),
                "SELECT\n" +
                        "    ROW_NUMBER() OVER (\n" +
                        "        PARTITION BY b\n" +
                        "        ORDER BY b DESC\n" +
                        "    )\n" +
                        "FROM test_table");

        // Test mapping of multiple substitutions with match precedence and matching of literal arguments.
        assertCreateTableAs(
                queryRewriter.rewriteQuery(
                        "SELECT\n" +
                                "    APPROX_PERCENTILE(a, 0.95),\n" +
                                "    APPROX_PERCENTILE(a, ARRAY[0.5, 0.9])\n" +
                                "FROM test_table",
                        CONFIGURATION, CONTROL).getQuery(),
                "SELECT\n" +
                        "    AVG(a),\n" +
                        "    REPEAT(AVG(a), CAST(CARDINALITY(ARRAY[0.5, 0.9]) AS INTEGER))\n" +
                        "FROM test_table");
    }

    @Test
    public void testReuseTableRewrite()
    {
        String query = "INSERT INTO dest_table SELECT * FROM test_table";
        QueryConfiguration configuration = new QueryConfiguration(
                CATALOG,
                SCHEMA,
                Optional.of("user"),
                Optional.empty(),
                Optional.empty(),
                true);
        assertShadowed(
                getQueryRewriter(new QueryRewriteConfig().setReuseTable(true), VERIFIER_CONFIG),
                query,
                "local.tmp",
                ImmutableMap.of(TEST, configuration, CONTROL, configuration),
                ImmutableList.of(),
                query,
                ImmutableList.of());
    }

    private void assertShadowed(
            QueryRewriter queryRewriter,
            @Language("SQL") String query,
            String prefix,
            List<String> expectedSetupTemplates,
            @Language("SQL") String expectedTemplates,
            List<String> expectedTeardownTemplates)
    {
        assertShadowed(queryRewriter, query, prefix, ImmutableMap.of(CONTROL, CONFIGURATION, TEST, CONFIGURATION), expectedSetupTemplates, expectedTemplates, expectedTeardownTemplates);
    }

    private void assertShadowed(
            QueryRewriter queryRewriter,
            @Language("SQL") String query,
            String prefix,
            Map<ClusterType, QueryConfiguration> queryConfigurations,
            List<String> expectedSetupTemplates,
            @Language("SQL") String expectedTemplates,
            List<String> expectedTeardownTemplates)
    {
        for (ClusterType cluster : ClusterType.values()) {
            QueryObjectBundle bundle = queryRewriter.rewriteQuery(query, queryConfigurations.get(cluster), cluster, true);

            String tableName = bundle.getObjectName().toString();
            if (!bundle.isReuseTable()) {
                assertTrue(tableName.startsWith(prefix + "_"));
            }

            assertStatements(bundle.getSetupQueries(), templateToStatements(expectedSetupTemplates, tableName));
            assertStatements(ImmutableList.of(bundle.getQuery()), templateToStatements(ImmutableList.of(expectedTemplates), tableName));
            assertStatements(bundle.getTeardownQueries(), templateToStatements(expectedTeardownTemplates, tableName));
        }
    }

    private static void assertTableName(QueryRewriter queryRewriter, @Language("SQL") String query, String expectedPrefix)
    {
        QueryObjectBundle bundle = queryRewriter.rewriteQuery(query, CONFIGURATION, CONTROL);
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
        return getQueryRewriter(QUERY_REWRITE_CONFIG, VERIFIER_CONFIG);
    }

    private QueryRewriter getQueryRewriter(QueryRewriteConfig rewriteConfig, VerifierConfig verifierConfig)
    {
        return new VerificationQueryRewriterFactory(sqlParser, createTypeManager(), rewriteConfig, rewriteConfig, verifierConfig).create(prestoAction);
    }
}
