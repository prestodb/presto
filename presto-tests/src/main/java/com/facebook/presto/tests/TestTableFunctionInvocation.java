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
package com.facebook.presto.tests;

import com.facebook.presto.connector.tvf.TestTVFConnectorColumnHandle;
import com.facebook.presto.connector.tvf.TestTVFConnectorFactory;
import com.facebook.presto.connector.tvf.TestTVFConnectorPlugin;
import com.facebook.presto.connector.tvf.TestingTableFunctions;
import com.facebook.presto.connector.tvf.TestingTableFunctions.SimpleTableFunction;
import com.facebook.presto.connector.tvf.TestingTableFunctions.SimpleTableFunction.SimpleTableFunctionHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.connector.TableFunctionApplicationResult;
import com.facebook.presto.spi.function.SchemaFunctionName;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tpch.TpchPlugin;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.stream.IntStream;

import static com.facebook.presto.common.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.connector.tvf.TestTVFConnectorFactory.TestTVFConnector.TestTVFConnectorSplit.TEST_TVF_CONNECTOR_SPLIT;
import static com.facebook.presto.connector.tvf.TestingTableFunctions.ConstantFunction.getConstantFunctionSplitSource;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static com.google.common.collect.ImmutableMap.toImmutableMap;

public class TestTableFunctionInvocation
        extends AbstractTestQueryFramework
{
    private static final String TESTING_CATALOG = "testing_catalog1";
    private static final String TABLE_FUNCTION_SCHEMA = "table_function_schema";

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return DistributedQueryRunner.builder(testSessionBuilder()
                        .setCatalog(TESTING_CATALOG)
                        .setSchema(TABLE_FUNCTION_SCHEMA)
                        .build())
                .build();
    }

    @Override
    protected QueryRunner createExpectedQueryRunner()
            throws Exception
    {
        DistributedQueryRunner result = DistributedQueryRunner.builder(testSessionBuilder()
                        .setCatalog("tpch")
                        .setSchema(TINY_SCHEMA_NAME)
                        .build())
                .build();
        result.installPlugin(new TpchPlugin());
        result.createCatalog("tpch", "tpch");
        return result;
    }

    @BeforeClass
    public void setUp()
    {
        DistributedQueryRunner queryRunner = getDistributedQueryRunner();

        BiFunction<ConnectorSession, ConnectorTableHandle, Map<String, TestTVFConnectorColumnHandle>> getColumnHandles = (session, tableHandle) -> IntStream.range(0, 100)
                .boxed()
                .map(i -> "column_" + i)
                .collect(toImmutableMap(column -> column, column -> new TestTVFConnectorColumnHandle(column, createUnboundedVarcharType()) {}));

        queryRunner.installPlugin(new TestTVFConnectorPlugin(TestTVFConnectorFactory.builder()
                .withTableFunctions(ImmutableSet.of(new SimpleTableFunction(),
                        new TestingTableFunctions.IdentityFunction(),
                        new TestingTableFunctions.IdentityPassThroughFunction(),
                        new TestingTableFunctions.RepeatFunction(),
                        new TestingTableFunctions.EmptyOutputFunction(),
                        new TestingTableFunctions.EmptyOutputWithPassThroughFunction(),
                        new TestingTableFunctions.EmptySourceFunction(),
                        new TestingTableFunctions.TestInputsFunction(),
                        new TestingTableFunctions.PassThroughInputFunction(),
                        new TestingTableFunctions.TestInputFunction(),
                        new TestingTableFunctions.TestSingleInputRowSemanticsFunction(),
                        new TestingTableFunctions.ConstantFunction()))
                .withApplyTableFunction((session, handle) -> {
                    if (handle instanceof SimpleTableFunctionHandle) {
                        SimpleTableFunctionHandle functionHandle = (SimpleTableFunctionHandle) handle;
                        return Optional.of(new TableFunctionApplicationResult<>(functionHandle.getTableHandle(), functionHandle.getTableHandle().getColumns().orElseThrow(() -> new IllegalStateException("Columns are missing"))));
                    }
                    return Optional.empty();
                })
                .withGetColumnHandles(getColumnHandles)
                .withTableFunctionProcessorProvider(
                        connectorTableFunctionHandle -> {
                            if (connectorTableFunctionHandle instanceof TestingTableFunctions.TestingTableFunctionHandle) {
                                switch (((TestingTableFunctions.TestingTableFunctionHandle) connectorTableFunctionHandle).getSchemaFunctionName().getFunctionName()) {
                                    case "identity_function":
                                        return new TestingTableFunctions.IdentityFunction.IdentityFunctionProcessorProvider();
                                    case "identity_pass_through_function":
                                        return new TestingTableFunctions.IdentityPassThroughFunction.IdentityPassThroughFunctionProcessorProvider();
                                    case "empty_output":
                                        return new TestingTableFunctions.EmptyOutputFunction.EmptyOutputProcessorProvider();
                                    case "empty_output_with_pass_through":
                                        return new TestingTableFunctions.EmptyOutputWithPassThroughFunction.EmptyOutputWithPassThroughProcessorProvider();
                                    case "empty_source":
                                        return new TestingTableFunctions.EmptySourceFunction.EmptySourceFunctionProcessorProvider();
                                    case "test_inputs_function":
                                        return new TestingTableFunctions.TestInputsFunction.TestInputsFunctionProcessorProvider();
                                    case "pass_through":
                                        return new TestingTableFunctions.PassThroughInputFunction.PassThroughInputProcessorProvider();
                                    case "test_input":
                                        return new TestingTableFunctions.TestInputFunction.TestInputProcessorProvider();
                                    case "test_single_input_function":
                                        return new TestingTableFunctions.TestSingleInputRowSemanticsFunction.TestSingleInputFunctionProcessorProvider();
                                    default:
                                        throw new IllegalArgumentException("unexpected table function: " + ((TestingTableFunctions.TestingTableFunctionHandle) connectorTableFunctionHandle).getSchemaFunctionName());
                                }
                            }
                            else if (connectorTableFunctionHandle instanceof TestingTableFunctions.RepeatFunction.RepeatFunctionHandle) {
                                return new TestingTableFunctions.RepeatFunction.RepeatFunctionProcessorProvider();
                            }
                            else if (connectorTableFunctionHandle instanceof TestingTableFunctions.ConstantFunction.ConstantFunctionHandle) {
                                return new TestingTableFunctions.ConstantFunction.ConstantFunctionProcessorProvider();
                            }
                            return null;
                        })
                .withTableFunctionResolver(TestingTableFunctions.RepeatFunction.RepeatFunctionHandle.class)
                .withTableFunctionResolver(TestingTableFunctions.TestingTableFunctionHandle.class)
                .withTableFunctionResolver(TestingTableFunctions.ConstantFunction.ConstantFunctionHandle.class)
                .withTableFunctionSplitResolver(TestingTableFunctions.ConstantFunction.ConstantFunctionSplit.class)
                .withTableFunctionSplitSource(
                        connectorTableFunctionHandle -> {
                            if (connectorTableFunctionHandle instanceof TestingTableFunctions.ConstantFunction.ConstantFunctionHandle) {
                                return getConstantFunctionSplitSource((TestingTableFunctions.ConstantFunction.ConstantFunctionHandle) connectorTableFunctionHandle);
                            }
                            else if (connectorTableFunctionHandle instanceof TestingTableFunctions.TestingTableFunctionHandle && ((TestingTableFunctions.TestingTableFunctionHandle) connectorTableFunctionHandle).getSchemaFunctionName().equals(new SchemaFunctionName("system", "empty_source"))) {
                                return new FixedSplitSource(ImmutableList.of(TEST_TVF_CONNECTOR_SPLIT));
                            }
                            return null;
                        })
                .build()));
        queryRunner.createCatalog(TESTING_CATALOG, "testTVF");

        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog("tpch", "tpch");
    }

    @Test
    public void testPrimitiveDefaultArgument()
    {
        assertQuery("SELECT boolean_column FROM TABLE(system.simple_table_function(column => 'boolean_column', ignored => 1))", "SELECT true WHERE false");

        // skip the `ignored` argument.
        assertQuery("SELECT boolean_column FROM TABLE(system.simple_table_function(column => 'boolean_column'))",
                "SELECT true WHERE false");
    }

    @Test
    public void testNoArgumentsPassed()
    {
        assertQuery("SELECT col FROM TABLE(system.simple_table_function())",
                  "SELECT true WHERE false");
    }

    @Test
    public void testIdentityFunction()
    {
        assertQuery("SELECT b, a FROM TABLE(system.identity_function(input => TABLE(VALUES (1, 2), (3, 4), (5, 6)) T(a, b)))",
                "VALUES (2, 1), (4, 3), (6, 5)");

        assertQuery("SELECT b, a FROM TABLE(system.identity_pass_through_function(input => TABLE(VALUES (1, 2), (3, 4), (5, 6)) T(a, b)))",
                "VALUES (2, 1), (4, 3), (6, 5)");

        // null partitioning value
        assertQuery("SELECT i.b, a FROM TABLE(system.identity_function(input => TABLE(VALUES ('x', 1), ('y', 2), ('z', null)) T(a, b) PARTITION BY b)) i",
                "VALUES (1, 'x'), (2, 'y'), (null, 'z')");

        assertQuery("SELECT b, a FROM TABLE(system.identity_pass_through_function(input => TABLE(VALUES ('x', 1), ('y', 2), ('z', null)) T(a, b) PARTITION BY b))",
                "VALUES (1, 'x'), (2, 'y'), (null, 'z')");

        // the identity_function copies all input columns and outputs them as proper columns.
        // the table tpch.tiny.orders has a hidden column row_number, which is not exposed to the function.
        assertQuery("SELECT * FROM TABLE(system.identity_function(input => TABLE(tpch.tiny.region)))",
                "SELECT * FROM tpch.tiny.region");

        // the identity_pass_through_function passes all input columns on output using the pass-through mechanism (as opposed to producing proper columns).
        // the table tpch.tiny.orders has a hidden column row_number, which is exposed to the pass-through mechanism.
        // the passed-through column row_number preserves its hidden property.
        assertQuery("SELECT row_number, * FROM TABLE(system.identity_pass_through_function(input => TABLE(tpch.tiny.orders)))",
                "SELECT row_number, * FROM tpch.tiny.orders");
    }

    @Test
    public void testRepeatFunction()
    {
        assertQuery("SELECT * FROM TABLE(system.repeat(TABLE(VALUES (1, 2), (3, 4), (5, 6))))",
                "VALUES (1, 2), (1, 2), (3, 4), (3, 4), (5, 6), (5, 6)");

        assertQuery("SELECT * FROM TABLE(system.repeat(TABLE(VALUES ('a', true), ('b', false)), 4))",
                "VALUES ('a', true), ('b', false), ('a', true), ('b', false), ('a', true), ('b', false), ('a', true), ('b', false)");

        assertQuery("SELECT * FROM TABLE(system.repeat(TABLE(VALUES ('a', true), ('b', false)) t(x, y) PARTITION BY x,4))",
                "VALUES ('a', true), ('b', false), ('a', true), ('b', false), ('a', true), ('b', false), ('a', true), ('b', false)");

        assertQuery("SELECT * FROM TABLE(system.repeat(TABLE(VALUES ('a', true), ('b', false)) t(x, y) ORDER BY y, 4))",
                "VALUES ('a', true), ('b', false), ('a', true), ('b', false), ('a', true), ('b', false), ('a', true), ('b', false)");

        assertQuery("SELECT * FROM TABLE(system.repeat(TABLE(VALUES ('a', true), ('b', false)) t(x, y) PARTITION BY x ORDER BY y, 4))",
                "VALUES ('a', true), ('b', false), ('a', true), ('b', false), ('a', true), ('b', false), ('a', true), ('b', false)");

        assertQuery("SELECT * FROM TABLE(system.repeat(TABLE(tpch.tiny.part), 3))",
                "SELECT * FROM tpch.tiny.part UNION ALL TABLE tpch.tiny.part UNION ALL TABLE tpch.tiny.part");

        assertQuery("SELECT * FROM TABLE(system.repeat(TABLE(tpch.tiny.part) PARTITION BY type, 3))",
                "SELECT * FROM tpch.tiny.part UNION ALL TABLE tpch.tiny.part UNION ALL TABLE tpch.tiny.part");

        assertQuery("SELECT * FROM TABLE(system.repeat(TABLE(tpch.tiny.part) ORDER BY size, 3))",
                "SELECT * FROM tpch.tiny.part UNION ALL TABLE tpch.tiny.part UNION ALL TABLE tpch.tiny.part");

        assertQuery("SELECT * FROM TABLE(system.repeat(TABLE(tpch.tiny.part) PARTITION BY type ORDER BY size, 3))",
                "SELECT * FROM tpch.tiny.part UNION ALL TABLE tpch.tiny.part UNION ALL TABLE tpch.tiny.part");
    }

    @Test
    public void testFunctionsReturningEmptyPages()
    {
        // the functions empty_output and empty_output_with_pass_through return an empty Page for each processed input Page. the argument has KEEP WHEN EMPTY property

        // non-empty input, no pass-trough columns

        assertQuery("SELECT * FROM TABLE(system.empty_output(TABLE(tpch.tiny.orders)))",
                "SELECT true WHERE false");

        // non-empty input, pass-through partitioning column
        assertQuery("SELECT * FROM TABLE(system.empty_output(TABLE(tpch.tiny.orders) PARTITION BY orderstatus))",
                "SELECT true, 'X' WHERE false");

        // non-empty input, argument has pass-trough columns
        assertQuery("SELECT * FROM TABLE(system.empty_output_with_pass_through(TABLE(tpch.tiny.orders)))",
                "SELECT true, * FROM tpch.tiny.orders WHERE false");

        // non-empty input, argument has pass-trough columns, partitioning column present
        assertQuery("SELECT * FROM TABLE(system.empty_output_with_pass_through(TABLE(tpch.tiny.orders) PARTITION BY orderstatus))",
                "SELECT true, * FROM tpch.tiny.orders WHERE false");

        // empty input, no pass-trough columns
        assertQuery("SELECT * FROM TABLE(system.empty_output(TABLE(SELECT * FROM tpch.tiny.orders WHERE false)))",
                "SELECT true WHERE false");

        // empty input, pass-through partitioning column
        assertQuery("SELECT * FROM TABLE(system.empty_output(TABLE(SELECT * FROM tpch.tiny.orders WHERE false) PARTITION BY orderstatus))",
                "SELECT true, 'X' WHERE false");

        // empty input, argument has pass-trough columns
        assertQuery("SELECT * FROM TABLE(system.empty_output_with_pass_through(TABLE(SELECT * FROM tpch.tiny.orders WHERE false)))",
                "SELECT true, * FROM tpch.tiny.orders WHERE false");

        // empty input, argument has pass-trough columns, partitioning column present
        assertQuery("SELECT * FROM TABLE(system.empty_output_with_pass_through(TABLE(SELECT * FROM tpch.tiny.orders WHERE false) PARTITION BY orderstatus)) ",
                "SELECT true, * FROM tpch.tiny.orders WHERE false");

        // function empty_source returns an empty Page for each Split it processes
        assertQuery("SELECT * FROM TABLE(system.empty_source())",
                "SELECT true WHERE false");
    }

    @Test
    public void testInputPartitioning()
    {
        // table function test_inputs_function has four table arguments. input_1 has row semantics. input_2, input_3 and input_4 have set semantics.
        // the function outputs one row per each tuple of partition it processes. The row includes a true value, and partitioning values.
        assertQuery("SELECT *" +
                        "FROM TABLE(system.test_inputs_function(" +
                        "input_1 => TABLE(VALUES 1, 2, 3)," +
                        "input_2 => TABLE(VALUES 4, 5, 4, 5, 4) t2(x2) PARTITION BY x2," +
                        "input_3 => TABLE(VALUES 6, 7, 6) t3(x3) PARTITION BY x3," +
                        "input_4 => TABLE(VALUES 8, 9)))",
                "VALUES (true, 4, 6), (true, 4, 7), (true, 5, 6), (true, 5, 7)");

        assertQuery("SELECT * FROM TABLE(system.test_inputs_function(" +
                        "input_1 => TABLE(VALUES 1, 2, 3)," +
                        "input_2 => TABLE(VALUES 4, 5, 4, 5, 4) t2(x2) PARTITION BY x2," +
                        "input_3 => TABLE(VALUES 6, 7, 6) t3(x3) PARTITION BY x3," +
                        "input_4 => TABLE(VALUES 8, 9) t4(x4) PARTITION BY x4))",
                "VALUES (true, 4, 6, 8), (true, 4, 6, 9), (true, 4, 7, 8), (true, 4, 7, 9), (true, 5, 6, 8), (true, 5, 6, 9), (true, 5, 7, 8), (true, 5, 7, 9)");

        assertQuery("SELECT * FROM TABLE(system.test_inputs_function(" +
                        "input_1 => TABLE(VALUES 1, 2, 3)," +
                        "input_2 => TABLE(VALUES 4, 5, 4, 5, 4) t2(x2) PARTITION BY x2," +
                        "input_3 => TABLE(VALUES 6, 7, 6) t3(x3) PARTITION BY x3," +
                        "input_4 => TABLE(VALUES 8, 8) t4(x4) PARTITION BY x4))",
                "VALUES (true, 4, 6, 8), (true, 4, 7, 8), (true, 5, 6, 8), (true, 5, 7, 8)");

        // null partitioning values
        assertQuery("SELECT * FROM TABLE(system.test_inputs_function(" +
                        "input_1 => TABLE(VALUES 1, null)," +
                        "input_2 => TABLE(VALUES 2, null, 2, null) t2(x2) PARTITION BY x2," +
                        "input_3 => TABLE(VALUES 3, null, 3, null) t3(x3) PARTITION BY x3," +
                        "input_4 => TABLE(VALUES null, null) t4(x4) PARTITION BY x4))",
                "VALUES (true, 2, 3, null), (true, 2, null, null), (true, null, 3, null), (true, null, null, null)");

        assertQuery("SELECT * FROM TABLE(system.test_inputs_function(" +
                        "input_1 => TABLE(VALUES 1, 2, 3)," +
                        "input_2 => TABLE(VALUES 4, 5, 4, 5, 4)," +
                        "input_3 => TABLE(VALUES 6, 7, 6)," +
                        "input_4 => TABLE(VALUES 8, 9)))",
                "VALUES true");

        assertQuery("SELECT DISTINCT regionkey, nationkey FROM TABLE(system.test_inputs_function(" +
                        "input_1 => TABLE(tpch.tiny.nation)," +
                        "input_2 => TABLE(tpch.tiny.nation) PARTITION BY regionkey ORDER BY name," +
                        "input_3 => TABLE(tpch.tiny.customer) PARTITION BY nationkey," +
                        "input_4 => TABLE(tpch.tiny.customer)))",
                "SELECT DISTINCT n.regionkey, c.nationkey FROM tpch.tiny.nation n, tpch.tiny.customer c");
    }

    @Test
    public void testEmptyPartitions()
    {
        // input_1 has row semantics, so it is prune when empty. input_2, input_3 and input_4 have set semantics, and are keep when empty by default
        assertQuery("SELECT * FROM TABLE(system.test_inputs_function(" +
                        "input_1 => TABLE(VALUES 1, 2, 3)," +
                        "input_2 => TABLE(SELECT 2 WHERE false)," +
                        "input_3 => TABLE(SELECT 3 WHERE false)," +
                        "input_4 => TABLE(SELECT 4 WHERE false)))",
                "VALUES true");

        assertQueryReturnsEmptyResult("SELECT * FROM TABLE(system.test_inputs_function(" +
                "input_1 => TABLE(SELECT 1 WHERE false)," +
                "input_2 => TABLE(VALUES 2)," +
                "input_3 => TABLE(VALUES 3)," +
                "input_4 => TABLE(VALUES 4)))");

        assertQuery("SELECT * FROM TABLE(system.test_inputs_function(" +
                        "input_1 => TABLE(VALUES 1, 2, 3)," +
                        "input_2 => TABLE(SELECT 2 WHERE false) t2(x2) PARTITION BY x2," +
                        "input_3 => TABLE(SELECT 3 WHERE false) t3(x3) PARTITION BY x3," +
                        "input_4 => TABLE(SELECT 4 WHERE false) t4(x4) PARTITION BY x4))",
                "VALUES (true, CAST(null AS integer), CAST(null AS integer), CAST(null AS integer))");

        assertQuery("SELECT * FROM TABLE(system.test_inputs_function(" +
                        "input_1 => TABLE(VALUES 1, 2, 3)," +
                        "input_2 => TABLE(SELECT 2 WHERE false) t2(x2) PARTITION BY x2," +
                        "input_3 => TABLE(VALUES 3, 4, 4) t3(x3) PARTITION BY x3," +
                        "input_4 => TABLE(VALUES 4, 4, 4, 5, 5, 5, 5) t4(x4) PARTITION BY x4))",
                "VALUES (true, CAST(null AS integer), 3, 4), (true, null, 4, 4), (true, null, 4, 5), (true, null, 3, 5)");

        assertQuery("SELECT * FROM TABLE(system.test_inputs_function(" +
                        "input_1 => TABLE(VALUES 1, 2, 3)," +
                        "input_2 => TABLE(SELECT 2 WHERE false) t2(x2) PARTITION BY x2," +
                        "input_3 => TABLE(SELECT 3 WHERE false) t3(x3) PARTITION BY x3," +
                        "input_4 => TABLE(VALUES 4, 5) t4(x4) PARTITION BY x4))",
                "VALUES (true, CAST(null AS integer), CAST(null AS integer), 4), (true, null, null, 5)");

        assertQueryReturnsEmptyResult("SELECT * FROM TABLE(system.test_inputs_function(" +
                "input_1 => TABLE(VALUES 1, 2, 3)," +
                "input_2 => TABLE(SELECT 2 WHERE false) t2(x2) PARTITION BY x2 PRUNE WHEN EMPTY," +
                "input_3 => TABLE(SELECT 3 WHERE false) t3(x3) PARTITION BY x3," +
                "input_4 => TABLE(VALUES 4, 5) t4(x4) PARTITION BY x4))");
    }

    @Test
    public void testCopartitioning()
    {
        // all tanbles are by default KEEP WHEN EMPTY. If there is no matching partition, it is null-completed
        assertQuery("SELECT * FROM TABLE(system.test_inputs_function(" +
                        "input_1 => TABLE(VALUES 1, 2, 3)," +
                        "input_2 => TABLE(VALUES 1, 1, 2, 2) t2(x2) PARTITION BY x2," +
                        "input_3 => TABLE(VALUES 4, 5) t3(x3)," +
                        "input_4 => TABLE(VALUES 2, 2, 2, 3) t4(x4) PARTITION BY x4 " +
                        "COPARTITION (t2, t4)))",
                "VALUES (true, 1, null), (true, 2, 2), (true, null, 3)");

        // partition `3` from input_4 is pruned because there is no matching partition in input_2
        assertQuery("SELECT *" +
                        "FROM TABLE(system.test_inputs_function(" +
                        "input_1 => TABLE(VALUES 1, 2, 3)," +
                        "input_2 => TABLE(VALUES 1, 1, 2, 2) t2(x2) PARTITION BY x2 PRUNE WHEN EMPTY," +
                        "input_3 => TABLE(VALUES 4, 5) t3(x3)," +
                        "input_4 => TABLE(VALUES 2, 2, 2, 3) t4(x4) PARTITION BY x4 " +
                        "COPARTITION (t2, t4)))",
                "VALUES (true, 1, null), (true, 2, 2)");

        // partition `1` from input_2 is pruned because there is no matching partition in input_4
        assertQuery("SELECT *" +
                        "FROM TABLE(system.test_inputs_function(" +
                        "input_1 => TABLE(VALUES 1, 2, 3)," +
                        "input_2 => TABLE(VALUES 1, 1, 2, 2) t2(x2) PARTITION BY x2," +
                        "input_3 => TABLE(VALUES 4, 5) t3(x3)," +
                        "input_4 => TABLE(VALUES 2, 2, 2, 3) t4(x4) PARTITION BY x4 PRUNE WHEN EMPTY " +
                        "COPARTITION (t2, t4)))",
                "VALUES (true, 2, 2), (true, null, 3)");

        assertQuery("SELECT *" +
                        "FROM TABLE(system.test_inputs_function(" +
                        "input_1 => TABLE(VALUES 1, 2, 3)," +
                        "input_2 => TABLE(VALUES 1, 1, 2, 2) t2(x2) PARTITION BY x2 PRUNE WHEN EMPTY," +
                        "input_3 => TABLE(VALUES 4, 5) t3(x3)," +
                        "input_4 => TABLE(VALUES 2, 2, 2, 3) t4(x4) PARTITION BY x4 PRUNE WHEN EMPTY " +
                        "COPARTITION (t2, t4)))",
                "VALUES (true, 2, 2)");

        // null partitioning values
        assertQuery("SELECT *" +
                        "FROM TABLE(system.test_inputs_function(" +
                        "input_1 => TABLE(VALUES 1, 2, 3)," +
                        "input_2 => TABLE(VALUES 1, 1, null, null, 2, 2) t2(x2) PARTITION BY x2," +
                        "input_3 => TABLE(VALUES 4, 5) t3(x3)," +
                        "input_4 => TABLE(VALUES null, 2, 2, 2, 3) t4(x4) PARTITION BY x4 " +
                        "COPARTITION (t2, t4)))",
                "VALUES (true, 1, null), (true, 2, 2), (true, null, null), (true, null, 3)");

        assertQuery("SELECT *" +
                        "FROM TABLE(system.test_inputs_function(" +
                        "input_1 => TABLE(VALUES 1, 2, 3)," +
                        "input_2 => TABLE(VALUES 1, 1, null, null, 2, 2) t2(x2) PARTITION BY x2 PRUNE WHEN EMPTY," +
                        "input_3 => TABLE(VALUES 4, 5) t3(x3)," +
                        "input_4 => TABLE(VALUES null, 2, 2, 2, 3) t4(x4) PARTITION BY x4 PRUNE WHEN EMPTY " +
                        "COPARTITION (t2, t4)))",
                "VALUES (true, 2, 2), (true, null, null)");

        assertQuery("SELECT *" +
                        "FROM TABLE(system.test_inputs_function(" +
                        "input_1 => TABLE(VALUES 1, 2, 3)," +
                        "input_2 => TABLE(VALUES 1, 1, null, null) t2(x2) PARTITION BY x2," +
                        "input_3 => TABLE(VALUES 2, 2, null) t3(x3) PARTITION BY x3," +
                        "input_4 => TABLE(VALUES 2, 3, 3) t4(x4) PARTITION BY x4 " +
                        "COPARTITION (t2, t4, t3)))",
                "VALUES (true, 1, null, null), (true, null, null, null), (true, null, 2, 2), (true, null, null, 3)");

        assertQuery("SELECT *" +
                        "FROM TABLE(system.test_inputs_function(" +
                        "input_1 => TABLE(VALUES 1, 2, 3)," +
                        "input_2 => TABLE(VALUES 1, 1, null, null) t2(x2) PARTITION BY x2," +
                        "input_3 => TABLE(VALUES 2, 2, null) t3(x3) PARTITION BY x3 PRUNE WHEN EMPTY," +
                        "input_4 => TABLE(VALUES 2, 3, 3) t4(x4) PARTITION BY x4 " +
                        "COPARTITION (t2, t4, t3)))",
                "VALUES (true, CAST(null AS integer), null, null), (true, null, 2, 2)");

        assertQuery("SELECT *" +
                        "FROM TABLE(system.test_inputs_function(" +
                        "input_1 => TABLE(VALUES 1, 2, 3)," +
                        "input_2 => TABLE(VALUES 1, 1, null, null) t2(x2) PARTITION BY x2 PRUNE WHEN EMPTY," +
                        "input_3 => TABLE(VALUES 2, 2, null) t3(x3) PARTITION BY x3," +
                        "input_4 => TABLE(VALUES 2, 3, 3) t4(x4) PARTITION BY x4 " +
                        "COPARTITION (t2, t4, t3)))",
                "VALUES (true, 1, CAST(null AS integer), CAST(null AS integer)), (true, null, null, null)");

        assertQueryReturnsEmptyResult(
                "SELECT *" +
                        "FROM TABLE(system.test_inputs_function(" +
                        "input_1 => TABLE(VALUES 1, 2, 3)," +
                        "input_2 => TABLE(VALUES 1, 1, null, null) t2(x2) PARTITION BY x2 PRUNE WHEN EMPTY," +
                        "input_3 => TABLE(VALUES 2, 2, null) t3(x3) PARTITION BY x3," +
                        "input_4 => TABLE(VALUES 2, 3, 3) t4(x4) PARTITION BY x4 PRUNE WHEN EMPTY " +
                        "COPARTITION (t2, t4, t3)))");
    }

    @Test
    public void testPassThroughWithEmptyPartitions()
    {
        assertQuery("SELECT * FROM TABLE(system.pass_through(" +
                        "TABLE(VALUES (1, 'a'), (2, 'b')) t1(a1, b1) PARTITION BY a1," +
                        "TABLE(VALUES (2, 'x'), (3, 'y')) t2(a2, b2) PARTITION BY a2 " +
                        "COPARTITION (t1, t2)))",
                "VALUES (true, false, 1, 'a', null, null), (true, true, 2, 'b', 2, 'x'), (false, true, null, null, 3, 'y')");

        assertQuery("SELECT * FROM TABLE(system.pass_through(" +
                        "TABLE(VALUES (1, 'a'), (2, 'b')) t1(a1, b1) PARTITION BY a1," +
                        "TABLE(SELECT 2, 'x' WHERE false) t2(a2, b2) PARTITION BY a2 " +
                        "COPARTITION (t1, t2)))",
                "VALUES (true, false, 1, 'a', CAST(null AS integer), CAST(null AS VARCHAR(1))), (true, false, 2, 'b', null, null)");

        assertQuery("SELECT * FROM TABLE(system.pass_through(" +
                        "TABLE(VALUES (1, 'a'), (2, 'b')) t1(a1, b1) PARTITION BY a1," +
                        "TABLE(SELECT 2, 'x' WHERE false) t2(a2, b2) PARTITION BY a2))",
                "VALUES (true, false, 1, 'a', CAST(null AS integer), CAST(null AS VARCHAR(1))), (true, false, 2, 'b', null, null)");
    }

    @Test
    public void testPassThroughWithEmptyInput()
    {
        assertQuery("SELECT * FROM TABLE(system.pass_through(TABLE(SELECT 1, 'x' WHERE false) t1(a1, b1) PARTITION BY a1, TABLE(SELECT 2, 'y' WHERE false) t2(a2, b2) PARTITION BY a2 COPARTITION (t1, t2)))",
                "VALUES (false, false, CAST(null AS integer), CAST(null AS VARCHAR(1)), CAST(null AS integer), CAST(null AS VARCHAR(1)))");

        assertQuery("SELECT * FROM TABLE(system.pass_through(TABLE(SELECT 1, 'x' WHERE false) t1(a1, b1) PARTITION BY a1, TABLE(SELECT 2, 'y' WHERE false) t2(a2, b2) PARTITION BY a2))",
                "VALUES (false, false, CAST(null AS integer), CAST(null AS VARCHAR(1)), CAST(null AS integer), CAST(null AS VARCHAR(1)))");
    }

    @Test
    public void testInput()
    {
        assertQuery("SELECT got_input FROM TABLE(system.test_input(TABLE(VALUES 1)))", "VALUES true");

        assertQuery("SELECT got_input FROM TABLE(system.test_input(TABLE(VALUES 1, 2, 3) t(a) PARTITION BY a))",
                "VALUES true, true, true");

        assertQuery("SELECT got_input FROM TABLE(system.test_input(TABLE(SELECT 1 WHERE false)))", "VALUES false");

        assertQuery("SELECT got_input FROM TABLE(system.test_input(TABLE(SELECT 1 WHERE false) t(a) PARTITION BY a))",
                "VALUES false");

        assertQuery("SELECT got_input FROM TABLE(system.test_input(TABLE(SELECT * FROM tpch.tiny.orders WHERE false)))", "VALUES false");

        assertQuery("SELECT got_input FROM TABLE(system.test_input(TABLE(SELECT * FROM tpch.tiny.orders WHERE false) PARTITION BY orderstatus ORDER BY orderkey))", "VALUES false");
    }

    @Test
    public void testSingleSourceWithRowSemantics()
    {
        assertQuery("SELECT * FROM TABLE(system.test_single_input_function(TABLE(VALUES (true), (false), (true))))", "VALUES true");
    }

    @Test
    public void testConstantFunction()
    {
        assertQuery("SELECT * FROM TABLE(system.constant(5))", "VALUES 5");

        assertQuery("SELECT * FROM TABLE(system.constant(2, 10))", "VALUES (2), (2), (2), (2), (2), (2), (2), (2), (2), (2)");

        assertQuery("SELECT * FROM TABLE(system.constant(null, 3))", "VALUES (CAST(null AS integer)), (null), (null)");

        // value as constant expression
        assertQuery("SELECT * FROM TABLE(system.constant(5 * 4, 3))", "VALUES (20), (20), (20)");

        assertQueryFails("SELECT * FROM TABLE(system.constant(2147483648, 3))", "line 1:37: Cannot cast type bigint to integer");

        assertQuery("SELECT count(*), count(DISTINCT constant_column), min(constant_column) FROM TABLE(system.constant(2, 1000000))", "VALUES (BIGINT '1000000', BIGINT '1', 2)");
    }

    @Test
    public void testPruneAllColumns()
    {
        // function identity_pass_through_function has no proper outputs. It outputs input columns using the pass-through mechanism.
        // in this case, no pass-through columns are referenced, so they are all pruned. The function effectively produces no columns.
        assertQuery("SELECT 'a' FROM TABLE(system.identity_pass_through_function(input => TABLE(VALUES 1, 2, 3)))",
                "VALUES 'a', 'a', 'a'");

        // all pass-through columns are pruned. Also, the input is empty, and it has KEEP WHEN EMPTY property, so the function is executed on empty partition.
        assertQuery("SELECT 'a' FROM TABLE(system.identity_pass_through_function(input => TABLE(SELECT 1 WHERE false)))",
                "SELECT 'a' WHERE false");

        // all pass-through columns are pruned. Also, the input is empty, and it has PRUNE WHEN EMPTY property, so the function is pruned out.
        assertQuery("SELECT 'a' FROM TABLE(system.identity_pass_through_function(input => TABLE(SELECT 1 WHERE false) PRUNE WHEN EMPTY))",
                "SELECT 'a' WHERE false");
    }

    @Test
    public void testPrunePassThroughColumns()
    {
        // function pass_through has 2 proper columns, and it outputs all columns from both inputs using the pass-through mechanism.
        // all columns are referenced
        assertQuery("SELECT p1, p2, x1, x2, y1, y2 " +
                        "FROM TABLE(system.pass_through( " +
                        "                            TABLE(VALUES (1, 'a'), (2, 'b'), (3, 'c')) t1(x1, x2)," +
                        "                            TABLE(VALUES (4, 'd'), (5, 'e')) t2(y1, y2))) t(p1, p2)",
                "VALUES (true, true, 3, 'c', 5, 'e')");

        // all pass-through columns are referenced. Proper columns are not referenced, but they are not pruned.
        assertQuery("SELECT x1, x2, y1, y2 " +
                        "FROM TABLE(system.pass_through( " +
                        "                            TABLE(VALUES (1, 'a'), (2, 'b'), (3, 'c')) t1(x1, x2)," +
                        "                            TABLE(VALUES (4, 'd'), (5, 'e')) t2(y1, y2))) t(p1, p2)",
                "VALUES (3, 'c', 5, 'e')");

        // some pass-through columns are referenced. Unreferenced pass-through columns are pruned.
        assertQuery("SELECT x2, y2 " +
                        "FROM TABLE(system.pass_through(" +
                        "                            TABLE(VALUES (1, 'a'), (2, 'b'), (3, 'c')) t1(x1, x2)," +
                        "                            TABLE(VALUES (4, 'd'), (5, 'e')) t2(y1, y2))) t(p1, p2)",
                "VALUES ('c', 'e')");

        assertQuery("SELECT y1, y2 " +
                        "FROM TABLE(system.pass_through( " +
                        "                            TABLE(VALUES (1, 'a'), (2, 'b'), (3, 'c')) t1(x1, x2)," +
                        "                            TABLE(VALUES (4, 'd'), (5, 'e')) t2(y1, y2))) t(p1, p2)",
                "VALUES (5, 'e')");

        // no pass-through columns are referenced. Unreferenced pass-through columns are pruned.
        assertQuery("SELECT 'x' " +
                        "FROM TABLE(system.pass_through( " +
                        "                            TABLE(VALUES (1, 'a'), (2, 'b'), (3, 'c')) t1(x1, x2)," +
                        "                            TABLE(VALUES (4, 'd'), (5, 'e')) t2(y1, y2))) t(p1, p2)",
                "VALUES ('x')");
    }

    @Test
    public void testPrunePassThroughColumnsWithEmptyInput()
    {
        // function pass_through has 2 proper columns, and it outputs all columns from both inputs using the pass-through mechanism.
        // all columns are referenced
        assertQuery("SELECT p1, p2, x1, x2, y1, y2 " +
                        "FROM TABLE(system.pass_through( " +
                        "                            TABLE(SELECT 1, 'a' WHERE FALSE) t1(x1, x2)," +
                        "                            TABLE(SELECT 4, 'd' WHERE FALSE) t2(y1, y2))) t(p1, p2)",
                "VALUES (false, false, CAST(null AS integer), CAST(null AS varchar(1)), CAST(null AS integer), CAST(null AS varchar(1)))");

        // all pass-through columns are referenced. Proper columns are not referenced, but they are not pruned.
        assertQuery("SELECT x1, x2, y1, y2 " +
                        "FROM TABLE(system.pass_through( " +
                        "                            TABLE(SELECT 1, 'a' WHERE FALSE) t1(x1, x2)," +
                        "                            TABLE(SELECT 4, 'd' WHERE FALSE) t2(y1, y2))) t(p1, p2) ",
                "VALUES (CAST(null AS integer), CAST(null AS varchar(1)), CAST(null AS integer), CAST(null AS varchar(1)))");

        // some pass-through columns are referenced. Unreferenced pass-through columns are pruned.
        assertQuery("SELECT x2, y2 " +
                        "FROM TABLE(system.pass_through( " +
                        "                            TABLE(SELECT 1, 'a' WHERE FALSE) t1(x1, x2)," +
                        "                            TABLE(SELECT 4, 'd' WHERE FALSE) t2(y1, y2))) t(p1, p2)",
                "VALUES (CAST(null AS varchar(1)), CAST(null AS varchar(1)))");

        assertQuery("SELECT y1, y2 " +
                        "FROM TABLE(system.pass_through( " +
                        "                            TABLE(SELECT 1, 'a' WHERE FALSE) t1(x1, x2)," +
                        "                            TABLE(SELECT 4, 'd' WHERE FALSE) t2(y1, y2))) t(p1, p2)",
                "VALUES (CAST(null AS integer), CAST(null AS varchar(1)))");

        // no pass-through columns are referenced. Unreferenced pass-through columns are pruned.
        assertQuery("SELECT 'x' " +
                        "FROM TABLE(system.pass_through(" +
                        "                            TABLE(SELECT 1, 'a' WHERE FALSE) t1(x1, x2)," +
                        "                            TABLE(SELECT 4, 'd' WHERE FALSE) t2(y1, y2))) t(p1, p2)",
                "VALUES ('x')");
    }
}
