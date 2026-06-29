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
package com.facebook.presto.iceberg.optimizer.derivedColumns;

import com.facebook.airlift.http.server.testing.TestingHttpServer;
import com.facebook.presto.iceberg.IcebergQueryRunner;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.relation.DeterminismEvaluator;
import com.facebook.presto.spi.relation.DomainTranslator;
import com.facebook.presto.spi.relation.ExpressionOptimizer;
import com.facebook.presto.spi.relation.PredicateCompiler;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.RowExpressionService;
import com.facebook.presto.sql.gen.RowExpressionPredicateCompiler;
import com.facebook.presto.sql.planner.optimizations.PlanNodeSearcher;
import com.facebook.presto.sql.planner.planPrinter.RowExpressionFormatter;
import com.facebook.presto.sql.relational.RowExpressionDeterminismEvaluator;
import com.facebook.presto.sql.relational.RowExpressionDomainTranslator;
import com.facebook.presto.sql.relational.RowExpressionOptimizer;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.testing.QueryRunner.MaterializedResultWithPlan;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.assertj.core.util.Files;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.airlift.testing.Assertions.assertEqualsIgnoreOrder;
import static com.facebook.presto.iceberg.CatalogType.REST;
import static com.facebook.presto.iceberg.rest.IcebergRestTestUtil.getRestServer;
import static com.facebook.presto.iceberg.rest.IcebergRestTestUtil.restConnectorProperties;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.filter;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableScan;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestIcebergDerivedColumnOptimizer
        extends AbstractTestQueryFramework
{
    @Language("SQL") private static final String CREATE_TABLE_SQL =
            "CREATE TABLE test_table1 (\n" +
                    " \"c1\" bigint,\n" +
                    " \"c2\" varchar,\n" +
                    " \"c3\" double,\n" +
                    " \"c2_derived\" varchar AS lower(c2)\n" +
                    " )";

    @Language("SQL") private static final String CREATE_TABLE_SQL2 =
            " CREATE TABLE test_table2 (                   \n" +
                    "     \"c1\" bigint,                                                 \n" +
                    "     \"c2\" varchar,                                                \n" +
                    "     \"c3\" double,\n" +
                    "     \"c2_derived\" varchar AS lower(c2) PERSISTENT,\n" +
                    "     \"c2_derived2\" varchar AS concat('A', c2) PERSISTENT\n" +
                    "  )";

    private File warehouseLocation;
    private TestingHttpServer restServer;
    private static final MetadataManager METADATA = MetadataManager.createTestMetadataManager();

    private static final RowExpressionService ROW_EXPRESSION_SERVICE = new RowExpressionService()
    {
        @Override
        public DomainTranslator getDomainTranslator()
        {
            return new RowExpressionDomainTranslator(METADATA);
        }

        @Override
        public ExpressionOptimizer getExpressionOptimizer(ConnectorSession session)
        {
            return new RowExpressionOptimizer(METADATA);
        }

        @Override
        public PredicateCompiler getPredicateCompiler()
        {
            return new RowExpressionPredicateCompiler(METADATA);
        }

        @Override
        public DeterminismEvaluator getDeterminismEvaluator()
        {
            return new RowExpressionDeterminismEvaluator(METADATA);
        }

        @Override
        public String formatRowExpression(ConnectorSession session, RowExpression expression)
        {
            return new RowExpressionFormatter(METADATA.getFunctionAndTypeManager()).formatRowExpression(session, expression);
        }
    };

    @BeforeClass
    @Override
    public void init()
            throws Exception
    {
        warehouseLocation = Files.newTemporaryFolder();
        restServer = getRestServer(warehouseLocation.getAbsolutePath());
        restServer.start();
        super.init();
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
            throws Exception
    {
        if (restServer != null) {
            restServer.stop();
        }
        if (warehouseLocation != null) {
            deleteRecursively(warehouseLocation.toPath(), ALLOW_INSECURE);
        }
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.builder()
                .setCatalogType(REST)
                .setExtraConnectorProperties(ImmutableMap.<String, String>builder()
                        .putAll(restConnectorProperties(restServer.getBaseUrl().toString()))
                        .put("iceberg.derived_columns.enable", "true")
                        .build())
                .setDataDirectory(Optional.of(warehouseLocation.toPath()))
                .setSchemaName("test_schema")
                .setCreateTpchTables(false)
                .setAddJmxPlugin(false)
                .build()
                .getQueryRunner();
    }

    @Test
    public void testBasicFilterPredicateRewrite()
    {
        try {
            assertUpdate(CREATE_TABLE_SQL);
            assertUpdate("INSERT INTO test_table1 VALUES (123, 'B', 12.2, lower('B')), (120, 'C', 12.3, lower('C')), (121, 'A', 12.1, lower('A'))", 3);
            assertQuery("SELECT * FROM test_table1 WHERE lower(c2) = 'a'", "VALUES (121, 'A', 12.1, 'a')");
            assertQuery("SELECT * FROM test_table1 WHERE upper(c2) = 'A'", "VALUES (121, 'A', 12.1, 'a')");
            assertPlan("SELECT * FROM test_table1 WHERE upper(c2) = 'A'",
                    anyTree(filter("(upper(c2)) = (VARCHAR'A')", tableScan("test_table1", ImmutableMap.of("c1", "c1", "c2", "c2")))));
            assertPlanFilterPredicate("(c2_derived) = (VARCHAR'a')", "SELECT * FROM test_table1 WHERE lower(c2) = 'a'");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS test_table1");
        }
    }

    @Test
    public void testBasicProjectionRewrite()
    {
        try {
            assertUpdate(CREATE_TABLE_SQL);
            assertUpdate("INSERT INTO test_table1 VALUES (123, 'B', 12.2, lower('B')), (120, 'C', 12.3, lower('C')), (121, 'A', 12.1, lower('A'))", 3);
            @Language("SQL") String query = "SELECT lower(c2), c1 FROM test_table1 WHERE lower(c2) = 'a'";
            assertQuery(query, "VALUES ('a', 121)");
            assertNoProjectNode(query);
            assertPlanFilterPredicate("(c2_derived) = (VARCHAR'a')", query);
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS test_table1");
        }
    }

    public void testUnionAllQueries()
    {
        try {
            assertUpdate(CREATE_TABLE_SQL);
            assertUpdate(CREATE_TABLE_SQL2);
            assertUpdate("INSERT INTO test_table1 VALUES (123, 'B', 12.2, lower('B')), (120, 'C', 12.3, lower('C')), (121, 'A', 12.1, lower('A'))", 3);
            assertUpdate("INSERT INTO test_table2 VALUES (123, 'B', 12.2, lower('B'), concat('A', lower('B'))), (120, 'C', 12.3, lower('C'), concat('A', lower('C')))," +
                    " (121, 'A', 12.1, lower('A'), concat('A', lower('A')))", 3);
            @Language("SQL") String query = "SELECT c1, c2 from test_table1 WHERE lower(c2) = 'b' UNION ALL SELECT c1, c2 from test_table2 WHERE lower(c2) = 'c'";
            assertQuery(query, "VALUES (123, 'B'), (120, 'C')");
            assertPlanFilterAndProject(List.of("(c2_derived) = (VARCHAR'b')", "(c2_derived_23) = (VARCHAR'c')"), List.of(List.of()), query);
            @Language("SQL") String queryWithProjectRewrite = "SELECT c1, lower(c2) from test_table1 WHERE lower(c2) = 'b' UNION ALL SELECT c1, lower(c2) from test_table2 WHERE lower(c2) = 'c'";
            assertQuery(queryWithProjectRewrite, "VALUES (123, 'b'), (120, 'c')");
            // project nodes are gone after derived column rewrite.
            assertPlanFilterAndProject(List.of("(c2_derived) = (VARCHAR'b')", "(c2_derived_22) = (VARCHAR'c')"), List.of(List.of()), queryWithProjectRewrite);
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS test_table1");
            assertUpdate("DROP TABLE IF EXISTS test_table2");
        }
    }

    @Test
    public void testSubqueriesRewrite()
    {
        try {
            assertUpdate(CREATE_TABLE_SQL);
            assertUpdate(CREATE_TABLE_SQL2);
            assertUpdate("INSERT INTO test_table1 VALUES (123, 'B', 12.2, lower('B')), (120, 'C', 12.3, lower('C')), (121, 'A', 12.1, lower('A'))", 3);
            assertUpdate("INSERT INTO test_table2 VALUES (123, 'B', 12.2, lower('B'), concat('A', lower('B'))), (120, 'C', 12.3, lower('C'), concat('A', lower('C')))," +
                    " (121, 'A', 12.1, lower('A'), concat('A', lower('A')))", 3);
            @Language("SQL") String query = "SELECT a, b FROM (SELECT c1 as a, lower(c2) AS b FROM test_table1 WHERE lower(c2) = 'b')";
            assertQuery(query, "VALUES (123, 'b')");
            assertPlanFilterPredicate("(c2_derived) = (VARCHAR'b')", query);
            // join with non co-related subqueries using CTE.
            @Language("SQL") String query2 = "WITH\n" +
                    "  t1 AS (SELECT c1 as a, lower(c2) AS b FROM test_table1 WHERE lower(c2) = 'b'),\n" +
                    "  t2 AS (SELECT c1 as a, lower(c2) AS b FROM test_table2 WHERE concat('A', lower(c2)) = concat('A', lower('B')))\n" +
                    "SELECT t1.*, t2.*\n" +
                    "FROM t1\n" +
                    "JOIN t2 ON t1.a = t2.a";
            assertQuery(query2, "VALUES (123, 'b', 123, 'b')");
            assertPlanFilterAndProject(List.of("(c2_derived) = (VARCHAR'b')", "(concat(VARCHAR'A', c2_derived_46)) = (VARCHAR'Ab')"),
                    List.of(List.of("c2_derived", "c1", "combine_hash(BIGINT'0', COALESCE($operator$hash_code(c1), BIGINT'0'))"),
                            List.of("c2_derived_46", "c1_8", "combine_hash(BIGINT'0', COALESCE($operator$hash_code(c1_8), BIGINT'0'))")), query2);
            @Language("SQL") String query3 =
                    "SELECT lower(a) FROM ( SELECT t1.c1, t2.c1, t1.c2 as a FROM test_table1 t1, test_table2 t2 WHERE (lower(t1.c2) = 'b'))  ms, test2 WHERE (a = c2)";
            assertUpdate("CREATE TABLE test2 (c1 BIGINT, c2 VARCHAR, c1_derived decimal(19, 2) AS CAST(c1 AS decimal) * 10.5 PERSISTENT)");
            assertUpdate("INSERT INTO test2 VALUES (123, 'B', 123 * 10.5), (120, 'C', 120 * 10.5), (121, 'A', 121 * 10.5)", 3);
            // TODO: fix: This test is flaky, a different plan is generated each time we run this query and as a result even the output is different each time.
            // This happens due to fact the projection `lower(a)` has two different but equivalent derived col rewrite rule.
            // 1) The result should not have varied, that is a bug. And the generated plans are logically equivalent. Why they produce different result is unknown !
            // 2) Somehow equi-join optimizer that comes after this - messes it up too.
            // the generated filter expressions ends up looking like: filterPredicate = ((lower(c2)) = (VARCHAR'b')) AND ((c2_derived) = (VARCHAR'b'))
            assertQuery(query3, "VALUES ('b'), ('b'), ('b')");
            assertPlanFilterAndProject(List.of("(c2_derived) = (VARCHAR'b')", "(c2_derived2) = (VARCHAR'Ab')"),
                    List.of(List.of("c2", "c2_derived", "combine_hash(BIGINT'0', COALESCE($operator$hash_code(c2), BIGINT'0'))"),
                            List.of("c2_22", "combine_hash(BIGINT'0', COALESCE($operator$hash_code(c2_22), BIGINT'0'))")), query3);
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS test_table1");
            assertUpdate("DROP TABLE IF EXISTS test_table2");
            assertUpdate("DROP TABLE IF EXISTS test2");
        }
    }

    @Test
    public void testJoinsRewrite()
    {
        try {
            assertUpdate(CREATE_TABLE_SQL);
            assertUpdate(CREATE_TABLE_SQL2);
            assertUpdate("INSERT INTO test_table1 VALUES (123, 'B', 12.2, lower('B')), (120, 'C', 12.3, lower('C')), (121, 'A', 12.1, lower('A'))", 3);
            assertUpdate("INSERT INTO test_table2 VALUES (123, 'B', 12.2, lower('B'), concat('A', lower('B'))), (120, 'C', 12.3, lower('C'), concat('A', lower('C'))), (121, 'A', 12.1, lower('A'), concat('A', lower('A')))", 3);
            @Language("SQL") String query = "SELECT t1.c2, t2.c1 FROM test_table1 t1, test_table2 t2 WHERE lower(t1.c2) = 'a'";
            assertQuery(query, "VALUES ('A', 121), ('A', 120), ('A', 123)");
            assertPlanFilterAndProject("(c2_derived) = (VARCHAR'a')", ImmutableList.of("c2_derived", "c1"), query);
            @Language("SQL") String query2 = "SELECT t1.c2, t2.c1 FROM test_table1 t1, test_table2 t2 WHERE lower(t1.c2) = 'a' and  concat('A', lower(t2.c2)) = 'Aa'";
            assertQuery(query2, "VALUES ('A', 121)");
            assertPlanFilterAndProject(ImmutableList.of("(c2_derived) = (VARCHAR'a')", "(concat(VARCHAR'A', c2_derived_17)) = (VARCHAR'Aa')"), ImmutableList.of(ImmutableList.of("c1_0")), query2);
            @Language("SQL") String queryWithProjectionRewrite = "SELECT lower(t1.c2), t2.c1 FROM test_table1 t1, test_table2 t2 WHERE lower(t2.c2) = 'a'";
            assertQuery(queryWithProjectionRewrite, "VALUES ('b', 121), ('c', 121), ('a', 121)");
            assertPlanFilterAndProject("(c2_derived) = (VARCHAR'a')", ImmutableList.of("c2_derived", "c1"), query);
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS test_table1");
            assertUpdate("DROP TABLE IF EXISTS test_table2");
        }
    }

    @Test
    public void testSelectWithDerivedColumnNotProjectedFilterPredicateRewrite()
    {
        try {
            assertUpdate(CREATE_TABLE_SQL);
            assertUpdate("INSERT INTO test_table1 VALUES (123, 'B', 12.2, lower('B')), (120, 'C', 12.3, lower('C')), (121, 'A', 12.1, lower('A'))", 3);
            // The following query does not project derived column i.e. c2_derived.
            @Language("SQL") String query = "SELECT c1 FROM test_table1 WHERE lower(c2) = 'a'";
            assertQuery(query, "VALUES 121");
            @Language("SQL") String query1 = "SELECT c1 FROM test_table1 WHERE lower(c2) = 'a' AND c1 = 121";
            assertQuery(query1, "VALUES 121");
            // assertPlan did not work correctly.
            assertPlanFilterPredicate("(c2_derived) = (VARCHAR'a')", query);
            assertPlanFilterPredicate("((c2_derived) = (VARCHAR'a')) AND ((c1) = (BIGINT'121'))", query1);
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS test_table1");
        }
    }

    @Test
    public void testWithMoreThanOneUDFAndMultiArgUDFsSpecified()
    { // TODO: fix flaky
        try {
            assertUpdate(" CREATE TABLE test_table2 (                   \n" +
                         "     \"c1\" bigint,                                                 \n" +
                         "     \"c2\" varchar,                                                \n" +
                         "     \"c3\" double,\n" +
                         "     \"c2_derived\" varchar GENERATED ALWAYS AS lower(c2) PERSISTENT,\n" +
                         "     \"c2_derived2\" varchar AS lpad(c2, BIGINT'10', 'X') \n" +
                         "  )");

            assertUpdate("INSERT INTO test_table2 VALUES (123, 'B', 12.2, lower('B'), lpad('B', 10, 'X')), (120, 'C', 12.3, lower('C'), lpad('C', 10, 'X'))," +
                    " (121, 'A', 12.1, lower('A'), lpad('A', 10, 'X'))", 3);
            @Language("SQL") String query = "SELECT c1, c2 FROM test_table2 WHERE c1 = 100 OR (lower(c2) = 'a' AND lpad(c2, 10, 'X') = 'XXXXXXXXXA' ) OR c2 LIKE '%Z%'";
            assertQuery(query, "VALUES (121, 'A')");
            assertQuery("SELECT c1 FROM test_table2 WHERE lower(c2) = 'a' AND c1 = 121", "VALUES 121");
            assertPlanFilterPredicate("((((c1) = (BIGINT'100')) OR ((STRPOS(c2, VARCHAR'Z')) <> (BIGINT'0'))) OR ((c2_derived) = (VARCHAR'a'))) AND " +
                    "((((c1) = (BIGINT'100')) OR ((STRPOS(c2, VARCHAR'Z')) <> (BIGINT'0'))) OR ((c2_derived2) = (VARCHAR'XXXXXXXXXA')))", query);
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS test_table2");
        }
    }

    @Test
    public void testSpecWithMultipleDerivedColumnsRulesDefinitionsOnSameColumns()
    { // i.e. Expressions with overlapping rules: Rule 1: lower(c2) -> c2_derived
        // Rule 2: concat('A', c2) -> c2_derived2
        try {
            assertUpdate(CREATE_TABLE_SQL2);
            assertUpdate("INSERT INTO test_table2 VALUES (123, 'B', 12.2, lower('B'), concat('B', lower('B'))), (120, 'C', 12.3, lower('C'), concat('C', lower('C')))," +
                    " (121, 'A', 12.1, lower('A'), concat('A', lower('A')))", 3);
            @Language("SQL") String query = "SELECT c1, c2 FROM test_table2 WHERE concat('A', lower(c2)) = 'Aa'";
            assertQuery(query, "VALUES (121, 'A')");
            @Language("SQL") String query2 = "SELECT c1, c2 FROM test_table2 WHERE (concat('A', lower(c2)) = 'Aa') OR  lower(c2) = 'b'";
            assertQuery(query2, "VALUES (121, 'A'), (123, 'B')");
            assertPlanFilterPredicate("(concat(VARCHAR'A', c2_derived)) = (VARCHAR'Aa')", query);
            assertPlanFilterPredicate("((concat(VARCHAR'A', c2_derived)) = (VARCHAR'Aa')) OR ((c2_derived) = (VARCHAR'b'))", query2);
        }
        finally {
            assertUpdate("DROP TABLE test_table2");
        }
    }

    @Test
    public void testExpressionSpecsWithImplicitTypeCasts()
    {
        try {
            assertUpdate("CREATE TABLE test2 (c1 BIGINT, c2 VARCHAR, c1_derived decimal(19, 2) AS CAST(c1 AS decimal) * 10.5 PERSISTENT)");
            assertUpdate("CREATE TABLE test3 (c1 BIGINT, c2 VARCHAR, c1_derived decimal(19, 2) AS 10.5 * CAST(c1 AS decimal) PERSISTENT)");
            assertUpdate("INSERT INTO test2 VALUES (123, 'B', 123 * 10.5), (120, 'C', 120 * 10.5)," +
                    " (121, 'A', 121 * 10.5)", 3);
            assertUpdate("INSERT INTO test3 VALUES (123, 'B', 123 * 10.5), (120, 'C', 120 * 10.5)," +
                    " (121, 'A', 121 * 10.5)", 3);
            @Language("SQL") String query = "SELECT c1,c2 from test2 WHERE c1 * 10.5 > 1200";
            assertQuery(query, "VALUES (121, 'A'), (123, 'B'), (120, 'C')");
            assertPlanFilterPredicate("(c1_derived) > (DECIMAL'1200.0')", query);
            @Language("SQL") String query2 = "SELECT c1,c2 from test3 WHERE 10.5 * c1 > 1200";
            assertQuery(query2, "VALUES (121, 'A'), (123, 'B'), (120, 'C')");
            assertPlanFilterPredicate("(c1_derived) > (DECIMAL'1200.0')", query2);
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS test2");
            assertUpdate("DROP TABLE IF EXISTS test3");
        }
    }

    @Test(expectedExceptions = RuntimeException.class)
    public void testExpressionSpecsWithImplicitTypeCastsNeg()
    {
        try {
            assertUpdate(" CREATE TABLE test2 (c1 BIGINT, c2 VARCHAR, c1_derived double AS c1 * 10.5 PERSISTENT)");
            assertUpdate("INSERT INTO test2 VALUES (123, 'B', 123 * 10.5), (120, 'C', 120 * 10.5)," +
                    " (121, 'A', 121 * 10.5)", 3);
            @Language("SQL") String query = "SELECT c1,c2 from test2 WHERE c1 * 10.5 > 1200";
            assertQueryError(query, "derivedColumn: c1_derived 's Type: double did not match with return type :decimal(38,1) of the expression :c1*10.5");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS test2");
        }
    }

    public void testQueriesWithExpressionInAggregation()
    {
        try {
            assertUpdate(CREATE_TABLE_SQL);
            assertUpdate("INSERT INTO test_table1 VALUES (123, 'B', 12.2, lower('B')), (120, 'C', 12.3, lower('C')), (121, 'A', 12.1, lower('A'))", 3);
            assertUpdate("CREATE TABLE test2 (c1 BIGINT, c2 VARCHAR, c1_derived decimal(19,2) AS CAST(c1 AS decimal) * 10.5 PERSISTENT)");
            assertUpdate("INSERT INTO test2 VALUES (123, 'B', 123 * 10.5), (120, 'C', 120 * 10.5)," +
                    " (121, 'A', 121 * 10.5)", 3);
            @Language("SQL") String query = "SELECT\n" +
                    "   lower(t1.c2),\n" +
                    "   avg(t1.c1)\n" +
                    "FROM\n" +
                    "   test_table1 as t1,\n" +
                    "   test2 as t2\n" +
                    "WHERE\n" +
                    "   t2.c1 = t1.c1\n" +
                    "GROUP BY t1.c2\n" +
                    "ORDER BY lower(t1.c2) ASC, avg(t1.c1) ASC\n" +
                    "LIMIT 2\n";
            assertQuery(query, "VALUES ('a', 121.0), ('b', 123.0)");
            // Asserting all 4 projections.
            assertPlanFilterAndProject(ImmutableList.of(), ImmutableList.of(
                    ImmutableList.of("c2_derived", "avg_10"),
                    ImmutableList.of("c1", "c2", "c2_derived",
                            "combine_hash(combine_hash(BIGINT'0', COALESCE($operator$hash_code(c2_derived), BIGINT'0')), COALESCE($operator$hash_code(c2), BIGINT'0'))"),
                    ImmutableList.of("c1", "c2", "c2_derived", "combine_hash(BIGINT'0', COALESCE($operator$hash_code(c1), BIGINT'0'))"),
                    ImmutableList.of("c1_0", "combine_hash(BIGINT'0', COALESCE($operator$hash_code(c1_0), BIGINT'0'))")), query);
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS test2");
            assertUpdate("DROP TABLE IF EXISTS test_table1");
        }
    }

    public void testQueriesWithUnionNodeUnderProjection()
    {
        try {
            assertUpdate(CREATE_TABLE_SQL);
            assertUpdate("INSERT INTO test_table1 VALUES (123, 'B', 12.2, lower('B')), (120, 'C', 12.3, lower('C')), (121, 'A', 12.1, lower('A'))", 3);
            assertUpdate(" CREATE TABLE test2 (c1 BIGINT, c2 VARCHAR, c1_derived decimal(19,2) AS CAST(c1 AS decimal) * 10.5 PERSISTENT)");
            assertUpdate("INSERT INTO test2 VALUES (123, 'B', 123 * 10.5), (120, 'C', 120 * 10.5)," +
                    " (121, 'A', 121 * 10.5)", 3);
            @Language("SQL") String query = "SELECT lower(a), b * 10.5\n" +
                    "    FROM (\n" +
                    "    SELECT c2 as a, c1 as b FROM test_table1 WHERE lower(c2) = 'b'\n" +
                    "    UNION\n" +
                    "    SELECT c2 as a, c1 as b FROM test2 WHERE c1 * 10.5 = 1291.5\n" +
                    "    )\n";
            assertQuery(query, "VALUES ('b', 1291.5)");
            // TODO: The rewrite of projections is non trivial, for following reasons.
            // 1. We should be able to establish that columns that exist in two tables with same alias are actually equivalent.
            // 2. Second they should have derived column definitions because they are from two different tables,
            // The filter expressions are rewritten correctly.
            assertPlanFilterAndProject(List.of("(c2_derived) = (VARCHAR'b')"), List.of(
                    List.of("lower(c2_16)", "(CAST(c1_17 AS decimal(19,0))) * (DECIMAL'10.5')"),
                    List.of("c2_16", "c1_17", "combine_hash(combine_hash(BIGINT'0', COALESCE($operator$hash_code(c2_16), BIGINT'0')), COALESCE($operator$hash_code(c1_17), BIGINT'0'))"),
                    List.of("c2", "c1", "$hashvalue_28"),
                    List.of("c2", "c1", "c2_derived", "combine_hash(combine_hash(BIGINT'0', COALESCE($operator$hash_code(c2), BIGINT'0')), COALESCE($operator$hash_code(c1), BIGINT'0'))"),
                    List.of("c2_16", "c1_17", "combine_hash(combine_hash(BIGINT'0', COALESCE($operator$hash_code(c2_16), BIGINT'0')), COALESCE($operator$hash_code(c1_17), BIGINT'0'))"),
                    List.of("c2_5", "c1_4", "$hashvalue_31"),
                    List.of("c2_5", "c1_4", "combine_hash(combine_hash(BIGINT'0', COALESCE($operator$hash_code(c2_5), BIGINT'0')), COALESCE($operator$hash_code(c1_4), BIGINT'0'))")), query);
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS test2");
            assertUpdate("DROP TABLE IF EXISTS test_table1");
        }
    }

    @Test
    public void testAddColumn()
    {
        try {
            assertUpdate(" CREATE TABLE test2 (c1 BIGINT, c2 VARCHAR)");
            assertUpdate("INSERT INTO test2 VALUES (123, 'B'), (120, 'C')", 2);
            assertUpdate("ALTER TABLE test2 ADD COLUMN c2_derived VARCHAR AS lower(c2) PERSISTENT");
            // TODO: in a future version added derived column may be auto synced - so, an explicit update may be redundant.
            assertUpdate("UPDATE test2 SET c2_derived = lower(c2)", 2);
            @Language("SQL") String query = "SELECT c1,c2 from test2 WHERE lower(c2) = 'c'";
            assertQuery(query, "VALUES (120, 'C')");
            assertPlanFilterPredicate("(c2_derived) = (VARCHAR'c')", query);
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS test2");
        }
    }

    @Test
    public void testDeleteColumn()
    {
        try {
            assertUpdate(" CREATE TABLE test2 (c1 BIGINT, c2 VARCHAR)");
            assertUpdate("INSERT INTO test2 VALUES (123, 'B'), (120, 'C')", 2);
            assertUpdate("ALTER TABLE test2 ADD COLUMN c2_derived VARCHAR AS lower(c2) PERSISTENT");
            // TODO: in a future version added derived column may be auto synced - so, an explicit update may be redundant.
            assertUpdate("UPDATE test2 SET c2_derived = lower(c2)", 2);
            @Language("SQL") String query = "SELECT c1,c2 from test2 WHERE lower(c2) = 'c'";
            assertQuery(query, "VALUES (120, 'C')");
            assertPlanFilterPredicate("(c2_derived) = (VARCHAR'c')", query);
            // After deleting the derived column, the rewrite should not happen and query results are correct.
            assertUpdate("ALTER TABLE test2 DROP COLUMN c2_derived");
            assertPlanFilterPredicate("(lower(c2)) = (VARCHAR'c')", query);
            assertQuery(query, "VALUES (120, 'C')");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS test2");
        }
    }

    private void assertPlanFilterPredicate(String expectedFilterPredicate, @Language("SQL") String query)
    {
        MaterializedResultWithPlan resultWithPlan = getQueryRunner().executeWithPlan(getSession(), query, WarningCollector.NOOP);
        FilterNode filter = PlanNodeSearcher.searchFrom(resultWithPlan.getQueryPlan().getRoot()).where(planNode -> planNode instanceof FilterNode).findOnlyElement();
        String formattedRowExpression = ROW_EXPRESSION_SERVICE.formatRowExpression(getSession().toConnectorSession(), filter.getPredicate());
        assertEquals(formattedRowExpression, expectedFilterPredicate);
    }

    private void assertNoProjectNode(@Language("SQL") String query)
    {
        MaterializedResultWithPlan resultWithPlan = getQueryRunner().executeWithPlan(getSession(), query, WarningCollector.NOOP);
        int count = PlanNodeSearcher.searchFrom(resultWithPlan.getQueryPlan().getRoot()).where(planNode -> planNode instanceof ProjectNode).count();
        assertEquals(count, 0);
    }

    private void assertPlanFilterAndProject(String expectedFilterPredicate, List<String> expectedAssignments, @Language("SQL") String query)
    {
        assertPlanFilterAndProject(ImmutableList.of(expectedFilterPredicate), ImmutableList.of(expectedAssignments), query);
    }

    private void assertPlanFilterAndProject(List<String> expectedFilterPredicates, List<List<String>> expectedAssignments, @Language("SQL") String query)
    {
        MaterializedResultWithPlan resultWithPlan = getQueryRunner().executeWithPlan(getSession(), query, WarningCollector.NOOP);
        List<ProjectNode> projects = PlanNodeSearcher.searchFrom(resultWithPlan.getQueryPlan().getRoot()).where(planNode -> planNode instanceof ProjectNode).findAll();
        List<FilterNode> filters = PlanNodeSearcher.searchFrom(resultWithPlan.getQueryPlan().getRoot()).where(planNode -> planNode instanceof FilterNode).findAll();
        List<String> formattedRowExpressions = new ArrayList<>();
        for (FilterNode filter : filters) {
            formattedRowExpressions.add(ROW_EXPRESSION_SERVICE.formatRowExpression(getSession().toConnectorSession(), filter.getPredicate()));
        }
        int i = 0;
        for (ProjectNode project : projects) {
            List<String> actualAssignment = project.getAssignments().entrySet().stream().map(Map.Entry::getValue).map(rowExpression ->
                    ROW_EXPRESSION_SERVICE.formatRowExpression(getSession().toConnectorSession(), rowExpression)).toList();
            assertEqualsIgnoreOrder(actualAssignment, expectedAssignments.get(i));
            i++;
        }
        assertEqualsIgnoreOrder(formattedRowExpressions, expectedFilterPredicates);
    }
}
