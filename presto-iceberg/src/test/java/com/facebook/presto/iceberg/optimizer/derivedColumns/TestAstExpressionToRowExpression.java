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
import com.facebook.presto.iceberg.IcebergColumnHandle;
import com.facebook.presto.iceberg.IcebergQueryRunner;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.DeterminismEvaluator;
import com.facebook.presto.spi.relation.DomainTranslator;
import com.facebook.presto.spi.relation.ExpressionOptimizer;
import com.facebook.presto.spi.relation.PredicateCompiler;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.RowExpressionService;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.gen.RowExpressionPredicateCompiler;
import com.facebook.presto.sql.parser.ParsingOptions;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.optimizations.PlanNodeSearcher;
import com.facebook.presto.sql.planner.planPrinter.RowExpressionFormatter;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.facebook.presto.sql.relational.RowExpressionDeterminismEvaluator;
import com.facebook.presto.sql.relational.RowExpressionDomainTranslator;
import com.facebook.presto.sql.relational.RowExpressionOptimizer;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import org.assertj.core.util.Files;
import org.intellij.lang.annotations.Language;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.iceberg.CatalogType.REST;
import static com.facebook.presto.iceberg.rest.IcebergRestTestUtil.getRestServer;
import static com.facebook.presto.iceberg.rest.IcebergRestTestUtil.restConnectorProperties;
import static com.facebook.presto.sql.parser.ParsingOptions.DecimalLiteralTreatment.AS_DECIMAL;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

@Test
public class TestAstExpressionToRowExpression
        extends AbstractTestQueryFramework
{
    private File warehouseLocation;
    private TestingHttpServer restServer;
    private AstExpressionToRowExpression astExpressionToRowExpression;
    private ConnectorSession session;
    private RowExpressionService rowExpressionService;
    @Language("SQL") private static final String CREATE_TABLE_QUERY =
            "CREATE TABLE test_table (\n" +
                    "    c1 BIGINT,\n" +
                    "    c2 DOUBLE,\n" +
                    "    bigint_col BIGINT,\n" +
                    "    smallint_col SMALLINT,\n" +
                    "    department_id_smallint SMALLINT,\n" +
                    "    integer_col INTEGER,\n" +
                    "    decimal_col DECIMAL(10, 2),\n" +
                    "    double_col DOUBLE PRECISION,\n" +
                    "    real_col REAL,\n" +
                    "    varchar_col VARCHAR(255),\n" +
                    "    char_col VARCHAR(1),\n" +
                    "    height_real REAL,\n" +
                    "    full_time SMALLINT,\n" +
                    "    boolean_col BOOLEAN,\n" +
                    "    is_remote_boolean BOOLEAN,\n" +
                    "    team_head_varchar VARCHAR(255),\n" +
                    "    reference_id_bigint BIGINT,\n" +
                    "    date_col DATE,\n" +
                    "    event_name_varchar VARCHAR(255)\n" +
                    ")\n";
    @Language("SQL") private static final String INSERT_QUERY =
            "INSERT INTO test_table (\n" +
                    "c1,\n" +
                    "c2,\n" +
                    "bigint_col,\n" +
                    "smallint_col,\n" +
                    "department_id_smallint,\n" +
                    "integer_col,\n" +
                    "decimal_col,\n" +
                    "double_col,\n" +
                    "real_col,\n" +
                    "varchar_col,\n" +
                    "char_col,\n" +
                    "height_real,\n" +
                    "full_time,\n" +
                    "boolean_col,\n" +
                    "is_remote_boolean,\n" +
                    "team_head_varchar,\n" +
                    "reference_id_bigint,\n" +
                    "date_col,\n" +
                    "event_name_varchar\n" +
                    ") VALUES\n" +
                    "(10, DOUBLE '10.5', 1, CAST(30 AS SMALLINT), CAST(1 AS SMALLINT), 5, 60000.00, 5000.00, 4.5, 'Alice', 'F', 5.4, CAST(1 AS SMALLINT), TRUE, FALSE, 'John', 1, DATE '2024-12-01', 'Annual Company Retreat'),\n" +
                    "(-1, DOUBLE '-1.5', 2, CAST(25 AS SMALLINT), CAST(2 AS SMALLINT), 3, 55000.00, 4000.00, 4.2, 'Bob', 'M', 5.8, CAST(1 AS SMALLINT), TRUE, TRUE, 'Sarah', 1, DATE '2024-11-15', 'Monthly Team Meeting'),\n" +
                    "(3, DOUBLE '-3.5', 3, CAST(28 AS SMALLINT), CAST(1 AS SMALLINT), 4, 58000.00, 4500.00, 4.3, 'Charlie', 'M', 6.0, CAST(1 AS SMALLINT), TRUE, FALSE, 'John', 1, DATE '2024-10-10', 'Quarterly Review'),\n" +
                    "(4, DOUBLE '4.5', 4, CAST(35 AS SMALLINT), CAST(3 AS SMALLINT), 8, 72000.00, 6000.00, 4.7, 'Diana', 'F', 5.6, CAST(1 AS SMALLINT), TRUE, TRUE, 'Mike', 1, DATE '2024-09-20', 'Holiday Party'),\n" +
                    "(5, DOUBLE '5.5' , 5, CAST(22 AS SMALLINT), CAST(2 AS SMALLINT), 1, 48000.00, 3000.00, 4.0, 'Eve', 'F', 5.5, CAST(1 AS SMALLINT), TRUE, FALSE, 'Sarah', 1, DATE '2024-08-05', 'Training Workshop'),\n" +
                    "(6, DOUBLE '6.5', 6, CAST(32 AS SMALLINT), CAST(1 AS SMALLINT), 6, 64000.00, 5500.00, 4.5, 'Frank', 'M', 5.7, CAST(1 AS SMALLINT), FALSE, TRUE, 'John', 1, DATE '2024-07-15', 'Product Launch'),\n" +
                    "(7, DOUBLE '7.5', 7, CAST(29 AS SMALLINT), CAST(3 AS SMALLINT), 4, 59000.00, 3500.00, 4.1, 'Grace', 'F', 5.8, CAST(0 AS SMALLINT), TRUE, TRUE, 'Mike', 1, DATE '2024-06-10', 'Leadership Summit'),\n" +
                    "(8, DOUBLE '8.5', 8, CAST(31 AS SMALLINT), CAST(2 AS SMALLINT), 7, 70000.00, 6000.00, 4.6, 'Henry', 'M', 6.2, CAST(1 AS SMALLINT), TRUE, FALSE, 'Sarah', 2, DATE '2024-05-01', 'Team Building Day'),\n" +
                    "(9, DOUBLE '9.5', 9, CAST(24 AS SMALLINT), CAST(1 AS SMALLINT), 2, 50000.00, 2000.00, 4.2, 'Ivy', 'F', 5.4, CAST(1 AS SMALLINT), TRUE, TRUE, 'John', 1, DATE '2024-04-15', 'Innovation Fair'),\n" +
                    "(10, DOUBLE '10.5', 10, CAST(26 AS SMALLINT), CAST(3 AS SMALLINT), 5, 62000.00, 4000.00, 4.3, 'Jack', 'M', 6.1, CAST(1 AS SMALLINT), FALSE, FALSE, 'Mike', 3, DATE '2024-03-30', 'End-of-Year Celebration')\n";

    @BeforeClass
    @Override
    public void init()
            throws Exception
    {
        warehouseLocation = Files.newTemporaryFolder();
        restServer = getRestServer(warehouseLocation.getAbsolutePath());
        restServer.start();
        super.init();
        DistributedQueryRunner queryRunner = getDistributedQueryRunner();
        FunctionAndTypeManager functionAndTypeManager = queryRunner.getMetadata().getFunctionAndTypeManager();
        StandardFunctionResolution functionResolution = new FunctionResolution(functionAndTypeManager.getFunctionAndTypeResolver());
        astExpressionToRowExpression = new AstExpressionToRowExpression(functionResolution, functionAndTypeManager);
        rowExpressionService = new RowExpressionService()
        {
            @Override
            public DomainTranslator getDomainTranslator()
            {
                return new RowExpressionDomainTranslator(getQueryRunner().getMetadata());
            }

            @Override
            public ExpressionOptimizer getExpressionOptimizer(ConnectorSession session)
            {
                return new RowExpressionOptimizer(getQueryRunner().getMetadata());
            }

            @Override
            public PredicateCompiler getPredicateCompiler()
            {
                return new RowExpressionPredicateCompiler(getQueryRunner().getMetadata());
            }

            @Override
            public DeterminismEvaluator getDeterminismEvaluator()
            {
                return new RowExpressionDeterminismEvaluator(getQueryRunner().getMetadata());
            }

            @Override
            public String formatRowExpression(ConnectorSession session, RowExpression expression)
            {
                return new RowExpressionFormatter(getQueryRunner().getMetadata().getFunctionAndTypeManager()).formatRowExpression(session, expression);
            }
        };
        assertUpdate(CREATE_TABLE_QUERY);
        assertUpdate(INSERT_QUERY, 10);
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
                        .build())
                .setDataDirectory(Optional.of(warehouseLocation.toPath()))
                .setCreateTpchTables(false)
                .build()
                .getQueryRunner();
    }

    public void testBasicArithmeticExpressions()
    {
        checkExpression("c1 + c2");
        checkExpression("c1 * c2");
        checkExpression("c1 + bigint_col");
        checkExpression("double_col * smallint_col");
        checkExpression("smallint_col + real_col");
        checkExpression("c2 / double_col");
        checkExpression("smallint_col + bigint_col");
        checkExpression("double_col - real_col");
    }

    private void checkExpression(String expression)
    {
        @Language("SQL") String query = String.format("SELECT %s from test_table", expression);
        Plan plan = getQueryRunner().executeWithPlan(getSession(), query, WarningCollector.NOOP).getQueryPlan();
        ProjectNode project = PlanNodeSearcher.searchFrom(plan.getRoot()).where(planNode -> planNode instanceof ProjectNode).findOnlyElement();
        Map<VariableReferenceExpression, RowExpression> rowExpressionMap = project.getAssignments().getMap();
        assertEquals(rowExpressionMap.size(), 1, "Only one mapping should exist");
        RowExpression expected = rowExpressionMap.values().stream().findFirst().get();

        Set<TableScanNode> tableScanNodes = plan.getRoot().accept(new FindTableScanNodesPlanVisitor(), null);
        assertEquals(tableScanNodes.size(), 1);

        ImmutableMap<String, ColumnMetadata> columnMetadataMap = tableScanNodes.stream().findFirst().get().getAssignments().entrySet().stream().collect(toImmutableMap(key -> key.getKey().getName(),
                value -> {
                    assertNotNull(value);
                    IcebergColumnHandle columnHandle = (IcebergColumnHandle) value.getValue();
                    return ColumnMetadata.builder().setName(columnHandle.getName()).setType(columnHandle.getType()).build();
                }));
        List<String> parserWarnings = new ArrayList<>();
        Expression expressionParsed = getSqlParser().createExpression(expression,
                ParsingOptions.builder().setWarningConsumer(parsingWarning -> {
                    String message = format("derived column expression: %s has parse warnings: %s", expression, parsingWarning.getMessage());
                    parserWarnings.add(message);
                }).setDecimalLiteralTreatment(AS_DECIMAL).build());
        assertTrue(parserWarnings.isEmpty(), "Found warnings: " + Joiner.on(",").join(parserWarnings));

        RowExpression actual = astExpressionToRowExpression.process(expressionParsed, columnMetadataMap);
        String actualFormatted = rowExpressionService.formatRowExpression(getSession().toConnectorSession(), actual);

        String expectedFormatted = rowExpressionService.formatRowExpression(getSession().toConnectorSession(), expected);
        assertEquals(actual.getType(), expected.getType());
        // A row expression may not be equivalent even if their formatted versions are equal. e.g. they may have implicit casts etc..
        assertTrue(actual.accept(new RowExpressionEquivalenceVisitor(), expected), format("\nActual row expression: %s : %s and \nExpected row expression: %s : %s", actual, actualFormatted, expected, expectedFormatted));
        assertEquals(actualFormatted, expectedFormatted);
    }
}
