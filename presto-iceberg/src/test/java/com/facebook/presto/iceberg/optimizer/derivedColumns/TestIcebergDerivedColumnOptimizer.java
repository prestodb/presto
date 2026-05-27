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
import com.google.common.collect.ImmutableMap;
import org.assertj.core.util.Files;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.util.Optional;

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
                    " \"c2_derived\" varchar AS lower(c2) PERSISTENT\n" +
                    " )";

    @Language("SQL") private static final String CREATE_TABLE_SQL2 =
            " CREATE TABLE test_table2 (                   \n" +
                    "     \"c1\" bigint,                                                 \n" +
                    "     \"c2\" varchar,                                                \n" +
                    "     \"c3\" double,\n" +
                    "     \"c2_derived\" varchar AS lower(c2) PERSISTENT,\n" +
                    "     \"c2_derived2\" varchar AS concat('A', lower(c2)) PERSISTENT\n" +
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
            assertPlan("SELECT * FROM test_table1 WHERE lower(c2) = 'a'",
                    anyTree(filter("(c2_derived) = (VARCHAR'a')", tableScan("test_table1",
                            ImmutableMap.of("c1", "c1", "c2", "c2", "c2_derived", "c2_derived")))));
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS test_table1");
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
            assertPlanFilterPredicate("((c1) = (BIGINT'121')) AND ((c2_derived) = (VARCHAR'a'))", query1);
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS test_table1");
        }
    }

    @Test
    public void testUdfSpecWithMoreThanOneUDFAndMultiArgUDFsSpecified()
    {
        try {
            assertUpdate(" CREATE TABLE test_table2 (                   \n" +
                    "     \"c1\" bigint,                                                 \n" +
                    "     \"c2\" varchar,                                                \n" +
                    "     \"c3\" double,\n" +
                    "     \"c2_derived\" varchar GENERATED ALWAYS AS lower(c2) PERSISTENT,\n" +
                    "     \"c2_derived2\" varchar AS lpad(c2, 10, 'X') PERSISTENT\n" +
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
    public void testSpecWithOverlappingDerivedColumnsRulesDefinition()
    { // i.e. Expressions with overlapping rules: Rule 1: lower(c2) -> c2_derived
        // Rule 2: concat('A', lower(c2)) -> c2_derived2
        try {
            assertUpdate(CREATE_TABLE_SQL2);
            assertUpdate("INSERT INTO test_table2 VALUES (123, 'B', 12.2, lower('B'), concat('B', lower('B'))), (120, 'C', 12.3, lower('C'), concat('C', lower('C')))," +
                    " (121, 'A', 12.1, lower('A'), concat('A', lower('A')))", 3);
            @Language("SQL") String query = "SELECT c1, c2 FROM test_table2 WHERE concat('A', lower(c2)) = 'Aa'";
            assertQuery(query, "VALUES (121, 'A')");
            @Language("SQL") String query2 = "SELECT c1, c2 FROM test_table2 WHERE (concat('A', lower(c2)) = 'Aa') OR  lower(c2) = 'b'";
            assertQuery(query2, "VALUES (121, 'A'), (123, 'B')");
            assertPlanFilterPredicate("(c2_derived2) = (VARCHAR'Aa')", query);
            assertPlanFilterPredicate("((c2_derived2) = (VARCHAR'Aa')) OR ((c2_derived) = (VARCHAR'b'))", query2);
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS test_table2");
        }
    }

    @Test
    public void testExpressionSpecsWithImplicitTypeCasts()
    {
        try {
            assertUpdate(" CREATE TABLE test2 (c1 BIGINT, c2 VARCHAR, c1_derived decimal(19, 2) AS c1 * 10.5 PERSISTENT)");
            assertUpdate(" CREATE TABLE test3 (c1 BIGINT, c2 VARCHAR, c1_derived decimal(19, 2) AS 10.5 * c1 PERSISTENT)");
            assertUpdate("INSERT INTO test2 VALUES (123, 'B', 123 * 10.5), (120, 'C', 120 * 10.5)," +
                    " (121, 'A', 121 * 10.5)", 3);
            assertUpdate("INSERT INTO test3 VALUES (123, 'B', 123 * 10.5), (120, 'C', 120 * 10.5)," +
                    " (121, 'A', 121 * 10.5)", 3);
            @Language("SQL") String query = "SELECT c1,c2 from test2 WHERE c1 * 10.5 > 1200";
            @Language("SQL") String query2 = "SELECT c1,c2 from test3 WHERE 10.5 * c1 > 1200";
            assertQuery(query, "VALUES (121, 'A'), (123, 'B'), (120, 'C')");
            assertPlanFilterPredicate("(c1_derived) > (DECIMAL'1200.0')", query);
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
}
