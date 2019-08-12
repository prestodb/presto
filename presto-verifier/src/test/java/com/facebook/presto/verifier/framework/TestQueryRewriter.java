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
package com.facebook.presto.verifier.framework;

import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.parser.SqlParserOptions;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.Property;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.tests.StandaloneQueryRunner;
import com.facebook.presto.verifier.retry.RetryConfig;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.sql.parser.IdentifierSymbol.AT_SIGN;
import static com.facebook.presto.sql.parser.IdentifierSymbol.COLON;
import static com.facebook.presto.verifier.VerifierTestUtil.CATALOG;
import static com.facebook.presto.verifier.VerifierTestUtil.SCHEMA;
import static com.facebook.presto.verifier.VerifierTestUtil.setupPresto;
import static com.facebook.presto.verifier.framework.ClusterType.CONTROL;
import static com.facebook.presto.verifier.framework.VerifierUtil.PARSING_OPTIONS;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Test
public class TestQueryRewriter
{
    private static final String DEFAULT_PREFIX = "local.tmp";
    private static final QueryConfiguration CONFIGURATION = new QueryConfiguration(CATALOG, SCHEMA, Optional.of("user"), Optional.empty(), Optional.empty());
    private static final List<Property> TABLE_PROPERTIES_OVERRIDE = ImmutableList.of(new Property(new Identifier("test_property"), new LongLiteral("21")));
    private static final SqlParser sqlParser = new SqlParser(new SqlParserOptions().allowIdentifierSymbol(COLON, AT_SIGN));

    private static StandaloneQueryRunner queryRunner;

    @BeforeClass
    public void setup()
            throws Exception
    {
        queryRunner = setupPresto();
        queryRunner.execute("CREATE TABLE test_table (a bigint, b varchar)");
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
                getQueryRewriter(DEFAULT_PREFIX),
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
                "CREATE TABLE %s " +
                        "(\"x\", \"y\", \"x_p_7\", \"x__1\", \"x_p_7__1\", \"a\", \"a__1\", \"b\") WITH (test_property = 21) AS " +
                        "SELECT " +
                        "1 x, " +
                        "2 y, " +
                        "3 \"X?p$7\", " +
                        "4 \"x\", " +
                        "5 x_p_7, " +
                        "6 a, " +
                        "* " +
                        "FROM test_table",
                ImmutableList.of("DROP TABLE IF EXISTS %s"));

        assertShadowed(
                getQueryRewriter(DEFAULT_PREFIX),
                "SELECT * FROM test_table a CROSS JOIN test_table b",
                "local.tmp",
                ImmutableList.of(),
                "CREATE TABLE %s " +
                        "(\"a\", \"b\", \"a__1\", \"b__1\") WITH (test_property = 21) AS " +
                        "SELECT * FROM test_table a CROSS JOIN test_table b",
                ImmutableList.of("DROP TABLE IF EXISTS %s"));
    }

    @Test
    public void testRewriteInsert()
    {
        assertShadowed(
                getQueryRewriter(DEFAULT_PREFIX),
                "INSERT INTO dest_table SELECT * FROM test_table",
                "local.tmp",
                ImmutableList.of("CREATE TABLE %s (LIKE dest_table INCLUDING PROPERTIES) WITH (test_property = 21)"),
                "INSERT INTO %s SELECT * FROM test_table",
                ImmutableList.of("DROP TABLE IF EXISTS %s"));
    }

    @Test
    public void testRewriteCreateTableAsSelect()
    {
        assertShadowed(
                getQueryRewriter(DEFAULT_PREFIX),
                "CREATE TABLE dest_table WITH (test_property = 90) AS SELECT * FROM test_table",
                "local.tmp",
                ImmutableList.of(),
                "CREATE TABLE %s WITH (test_property = 21) AS SELECT * FROM test_table",
                ImmutableList.of("DROP TABLE IF EXISTS %s"));
    }

    @Test
    public void testTemporaryTableName()
    {
        QueryRewriter tableNameRewriter = getQueryRewriter("tmp");
        QueryRewriter schemaRewriter = getQueryRewriter("local.tmp");
        QueryRewriter catalogRewriter = getQueryRewriter("verifier_batch.local.tmp");

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

    private void assertShadowed(
            QueryRewriter queryRewriter,
            @Language("SQL") String query,
            String prefix,
            List<String> expectedSetupTemplates,
            @Language("SQL") String expectedTemplates,
            List<String> expectedTeardownTemplates)
    {
        for (ClusterType cluster : ClusterType.values()) {
            QueryBundle bundle = queryRewriter.rewriteQuery(query, cluster);

            String tableName = bundle.getTableName().toString();
            assertTrue(tableName.startsWith(prefix + "_"));

            assertEquals(bundle.getSetupQueries(), templateToStatements(expectedSetupTemplates, tableName));
            assertEquals(ImmutableList.of(bundle.getQuery()), templateToStatements(ImmutableList.of(expectedTemplates), tableName));
            assertEquals(bundle.getTeardownQueries(), templateToStatements(expectedTeardownTemplates, tableName));
        }
    }

    private void assertTableName(QueryRewriter queryRewriter, @Language("SQL") String query, String expectedPrefix)
    {
        QueryBundle bundle = queryRewriter.rewriteQuery(query, CONTROL);
        assertTrue(bundle.getTableName().toString().startsWith(expectedPrefix));
    }

    private List<Statement> templateToStatements(List<String> templates, String tableName)
    {
        return templates.stream()
                .map(template -> format(template, tableName))
                .map(query -> sqlParser.createStatement(query, PARSING_OPTIONS))
                .collect(toImmutableList());
    }

    private QueryRewriter getQueryRewriter(String prefix)
    {
        String gateway = queryRunner.getServer().getBaseUrl().toString().replace("http", "jdbc:presto");
        VerifierConfig config = new VerifierConfig()
                .setControlJdbcUrl(gateway)
                .setTestJdbcUrl(gateway)
                .setControlTablePrefix(prefix)
                .setTestTablePrefix(prefix);
        return new QueryRewriter(
                sqlParser,
                new JdbcPrestoAction(
                        new PrestoExceptionClassifier(ImmutableSet.of(), ImmutableSet.of()),
                        CONFIGURATION,
                        CONFIGURATION,
                        new VerificationContext(),
                        config,
                        new RetryConfig(),
                        new RetryConfig()),
                TABLE_PROPERTIES_OVERRIDE,
                config);
    }
}
