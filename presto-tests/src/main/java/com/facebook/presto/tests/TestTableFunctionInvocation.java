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

import com.facebook.presto.Session;
import com.facebook.presto.connector.tvf.MockConnectorColumnHandle;
import com.facebook.presto.connector.tvf.MockConnectorFactory;
import com.facebook.presto.connector.tvf.MockConnectorPlugin;
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
import static com.facebook.presto.connector.tvf.MockConnectorFactory.MockConnector.MockConnectorSplit.MOCK_CONNECTOR_SPLIT;
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

        BiFunction<ConnectorSession, ConnectorTableHandle, Map<String, MockConnectorColumnHandle>> getColumnHandles = (session, tableHandle) -> IntStream.range(0, 100)
                .boxed()
                .map(i -> "column_" + i)
                .collect(toImmutableMap(column -> column, column -> new MockConnectorColumnHandle(column, createUnboundedVarcharType()) {}));

        queryRunner.installPlugin(new MockConnectorPlugin(MockConnectorFactory.builder()
                .withTableFunctions(ImmutableSet.of(
                        new SimpleTableFunction(),
                        new TestingTableFunctions.IdentityFunction(),
                        new TestingTableFunctions.IdentityPassThroughFunction()))
//                        new TestingTableFunctions.RepeatFunction(),
//                        new TestingTableFunctions.EmptyOutputFunction(),
//                        new TestingTableFunctions.EmptyOutputWithPassThroughFunction(),
//                        new TestingTableFunctions.TestInputsFunction(),
//                        new TestingTableFunctions.PassThroughInputFunction(),
//                        new TestingTableFunctions.TestInputFunction(),
//                        new TestingTableFunctions.TestSingleInputRowSemanticsFunction(),
//                        new TestingTableFunctions.ConstantFunction(),
//                        new TestingTableFunctions.EmptySourceFunction()
                .withApplyTableFunction((session, handle) -> {
                    if (handle instanceof SimpleTableFunctionHandle) {
                        SimpleTableFunctionHandle functionHandle = (SimpleTableFunctionHandle) handle;
                        return Optional.of(new TableFunctionApplicationResult<>(functionHandle.getTableHandle(), functionHandle.getTableHandle().getColumns().orElseThrow(() -> new IllegalStateException("Columns are missing"))));
                    }
                    return Optional.empty();
                })
                .withGetTableFunctionProcessorProvider(Optional.of(name -> {
                    if (name.equals(new SchemaFunctionName("system", "identity_function"))) {
                        return new TestingTableFunctions.IdentityFunction.IdentityFunctionProcessorProvider();
                    }
                    else if (name.equals(new SchemaFunctionName("system", "identity_pass_through_function"))) {
                        return new TestingTableFunctions.IdentityPassThroughFunction.IdentityPassThroughFunctionProcessorProvider();
                    }
                    else if (name.equals(new SchemaFunctionName("system", "repeat"))) {
                        return new TestingTableFunctions.RepeatFunction.RepeatFunctionProcessorProvider();
                    }
                    else if (name.equals(new SchemaFunctionName("system", "empty_output"))) {
                        return new TestingTableFunctions.EmptyOutputFunction.EmptyOutputProcessorProvider();
                    }
                    else if (name.equals(new SchemaFunctionName("system", "empty_output_with_pass_through"))) {
                        return new TestingTableFunctions.EmptyOutputWithPassThroughFunction.EmptyOutputWithPassThroughProcessorProvider();
                    }
                    else if (name.equals(new SchemaFunctionName("system", "test_inputs_function"))) {
                        return new TestingTableFunctions.TestInputsFunction.TestInputsFunctionProcessorProvider();
                    }
                    else if (name.equals(new SchemaFunctionName("system", "pass_through"))) {
                        return new TestingTableFunctions.PassThroughInputFunction.PassThroughInputProcessorProvider();
                    }
                    else if (name.equals(new SchemaFunctionName("system", "test_input"))) {
                        return new TestingTableFunctions.TestInputFunction.TestInputProcessorProvider();
                    }
                    else if (name.equals(new SchemaFunctionName("system", "test_single_input_function"))) {
                        return new TestingTableFunctions.TestSingleInputRowSemanticsFunction.TestSingleInputFunctionProcessorProvider();
                    }
                    else if (name.equals(new SchemaFunctionName("system", "constant"))) {
                        return new TestingTableFunctions.ConstantFunction.ConstantFunctionProcessorProvider();
                    }
                    else if (name.equals(new SchemaFunctionName("system", "empty_source"))) {
                        return new TestingTableFunctions.EmptySourceFunction.EmptySourceFunctionProcessorProvider();
                    }
                    return null;
                }))
                .withGetColumnHandles(getColumnHandles)
//                .withTableFunctionSplitSource(
//                        new SchemaFunctionName("system", "constant"),
//                        handle -> getConstantFunctionSplitSource((TestingTableFunctions.ConstantFunction.ConstantFunctionHandle) handle))
//                .withTableFunctionSplitSource(
//                        new SchemaFunctionName("system", "empty_source"),
//                        handle -> new FixedSplitSource(ImmutableList.of(MOCK_CONNECTOR_SPLIT)))
                .withTableFunctionSplitSource(
                        new SchemaFunctionName("system", "identity_function"),
                        handle -> new FixedSplitSource(ImmutableList.of(MOCK_CONNECTOR_SPLIT)))
                .build()));
        queryRunner.createCatalog(TESTING_CATALOG, "mock");

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

        /*
        TODO: Skipped due to partitioning.
        // null partitioning value
        assertThat(query("SELECT i.b, a FROM TABLE(system.identity_function(input => TABLE(VALUES ('x', 1), ('y', 2), ('z', null)) T(a, b) PARTITION BY b)) i"))
                .matches("VALUES (1, 'x'), (2, 'y'), (null, 'z')");

        assertThat(query("SELECT b, a FROM TABLE(system.identity_pass_through_function(input => TABLE(VALUES ('x', 1), ('y', 2), ('z', null)) T(a, b) PARTITION BY b))"))
                .matches("VALUES (1, 'x'), (2, 'y'), (null, 'z')");
        */
        // the identity_function copies all input columns and outputs them as proper columns.
        // the table tpch.tiny.orders has a hidden column row_number, which is not exposed to the function.
        assertQuery("SELECT * FROM TABLE(system.identity_function(input => TABLE(tpch.tiny.region)))",
                "SELECT * FROM tpch.tiny.region");

        /*
        // the identity_pass_through_function passes all input columns on output using the pass-through mechanism (as opposed to producing proper columns).
        // the table tpch.tiny.orders has a hidden column row_number, which is exposed to the pass-through mechanism.
        // the passed-through column row_number preserves its hidden property.
        assertThat(query("SELECT row_number, * FROM TABLE(system.identity_pass_through_function(input => TABLE(tpch.tiny.orders)))"))
                .matches("SELECT row_number, * FROM tpch.tiny.orders");
        */
    }

    /*
    @Test
    public void testRepeatFunction()
    {
        assertThat(simpleQuery("""
                SELECT *
                FROM TABLE(system.repeat(TABLE(VALUES (1, 2), (3, 4), (5, 6))))
                """))
                .matches("VALUES (1, 2), (1, 2), (3, 4), (3, 4), (5, 6), (5, 6)");

        assertThat(query("""
                SELECT *
                FROM TABLE(system.repeat(
                                        TABLE(VALUES ('a', true), ('b', false)),
                                        4))
                """))
                .matches("VALUES ('a', true), ('b', false), ('a', true), ('b', false), ('a', true), ('b', false), ('a', true), ('b', false)");

        assertThat(query("""
                SELECT *
                FROM TABLE(system.repeat(
                                        TABLE(VALUES ('a', true), ('b', false)) t(x, y) PARTITION BY x,
                                        4))
                """))
                .matches("VALUES ('a', true), ('b', false), ('a', true), ('b', false), ('a', true), ('b', false), ('a', true), ('b', false)");

        assertThat(query("""
                SELECT *
                FROM TABLE(system.repeat(
                                        TABLE(VALUES ('a', true), ('b', false)) t(x, y) ORDER BY y,
                                        4))
                """))
                .matches("VALUES ('a', true), ('b', false), ('a', true), ('b', false), ('a', true), ('b', false), ('a', true), ('b', false)");

        assertThat(query("""
                SELECT *
                FROM TABLE(system.repeat(
                                        TABLE(VALUES ('a', true), ('b', false)) t(x, y) PARTITION BY x ORDER BY y,
                                        4))
                """))
                .matches("VALUES ('a', true), ('b', false), ('a', true), ('b', false), ('a', true), ('b', false), ('a', true), ('b', false)");

        assertThat(query("""
                SELECT *
                FROM TABLE(system.repeat(TABLE(tpch.tiny.part), 3))
                """))
                .matches("SELECT * FROM tpch.tiny.part UNION ALL TABLE tpch.tiny.part UNION ALL TABLE tpch.tiny.part");

        assertThat(query("""
                SELECT *
                FROM TABLE(system.repeat(TABLE(tpch.tiny.part) PARTITION BY type, 3))
                """))
                .matches("SELECT * FROM tpch.tiny.part UNION ALL TABLE tpch.tiny.part UNION ALL TABLE tpch.tiny.part");

        assertThat(query("""
                SELECT *
                FROM TABLE(system.repeat(TABLE(tpch.tiny.part) ORDER BY size, 3))
                """))
                .matches("SELECT * FROM tpch.tiny.part UNION ALL TABLE tpch.tiny.part UNION ALL TABLE tpch.tiny.part");

        assertThat(query("""
                SELECT *
                FROM TABLE(system.repeat(TABLE(tpch.tiny.part) PARTITION BY type ORDER BY size, 3))
                """))
                .matches("SELECT * FROM tpch.tiny.part UNION ALL TABLE tpch.tiny.part UNION ALL TABLE tpch.tiny.part");
    }

    @Test
    public void testFunctionsReturningEmptyPages()
    {
        // the functions empty_output and empty_output_with_pass_through return an empty Page for each processed input Page. the argument has KEEP WHEN EMPTY property

        // non-empty input, no pass-trough columns
        assertThat(query("""
                SELECT *
                FROM TABLE(system.empty_output(TABLE(tpch.tiny.orders)))
                """))
                .matches("SELECT true WHERE false");

        // non-empty input, pass-through partitioning column
        assertThat(query("""
                SELECT *
                FROM TABLE(system.empty_output(TABLE(tpch.tiny.orders) PARTITION BY orderstatus))
                """))
                .matches("SELECT true, 'X' WHERE false");

        // non-empty input, argument has pass-trough columns
        assertThat(query("""
                SELECT *
                FROM TABLE(system.empty_output_with_pass_through(TABLE(tpch.tiny.orders)))
                """))
                .matches("SELECT true, * FROM tpch.tiny.orders WHERE false");

        // non-empty input, argument has pass-trough columns, partitioning column present
        assertThat(query("""
                SELECT *
                FROM TABLE(system.empty_output_with_pass_through(TABLE(tpch.tiny.orders) PARTITION BY orderstatus))
                """))
                .matches("SELECT true, * FROM tpch.tiny.orders WHERE false");

        // empty input, no pass-trough columns
        assertThat(query("""
                SELECT *
                FROM TABLE(system.empty_output(TABLE(SELECT * FROM tpch.tiny.orders WHERE false)))
                """))
                .matches("SELECT true WHERE false");

        // empty input, pass-through partitioning column
        assertThat(query("""
                SELECT *
                FROM TABLE(system.empty_output(TABLE(SELECT * FROM tpch.tiny.orders WHERE false) PARTITION BY orderstatus))
                """))
                .matches("SELECT true, 'X' WHERE false");

        // empty input, argument has pass-trough columns
        assertThat(query("""
                SELECT *
                FROM TABLE(system.empty_output_with_pass_through(TABLE(SELECT * FROM tpch.tiny.orders WHERE false)))
                """))
                .matches("SELECT true, * FROM tpch.tiny.orders WHERE false");

        // empty input, argument has pass-trough columns, partitioning column present
        assertThat(query("""
                SELECT *
                FROM TABLE(system.empty_output_with_pass_through(TABLE(SELECT * FROM tpch.tiny.orders WHERE false) PARTITION BY orderstatus))
                """))
                .matches("SELECT true, * FROM tpch.tiny.orders WHERE false");

        // function empty_source returns an empty Page for each Split it processes
        assertThat(query("""
                SELECT *
                FROM TABLE(system.empty_source())
                """))
                .matches("SELECT true WHERE false");
    }

    @Test
    public void testInputPartitioning()
    {
        // table function test_inputs_function has four table arguments. input_1 has row semantics. input_2, input_3 and input_4 have set semantics.
        // the function outputs one row per each tuple of partition it processes. The row includes a true value, and partitioning values.
        assertThat(query("""
                SELECT *
                FROM TABLE(system.test_inputs_function(
                               input_1 => TABLE(VALUES 1, 2, 3),
                               input_2 => TABLE(VALUES 4, 5, 4, 5, 4) t2(x2) PARTITION BY x2,
                               input_3 => TABLE(VALUES 6, 7, 6) t3(x3) PARTITION BY x3,
                               input_4 => TABLE(VALUES 8, 9)))
                """))
                .matches("VALUES (true, 4, 6), (true, 4, 7), (true, 5, 6), (true, 5, 7)");

        assertThat(query("""
                SELECT *
                FROM TABLE(system.test_inputs_function(
                               input_1 => TABLE(VALUES 1, 2, 3),
                               input_2 => TABLE(VALUES 4, 5, 4, 5, 4) t2(x2) PARTITION BY x2,
                               input_3 => TABLE(VALUES 6, 7, 6) t3(x3) PARTITION BY x3,
                               input_4 => TABLE(VALUES 8, 9) t4(x4) PARTITION BY x4))
                """))
                .matches("VALUES (true, 4, 6, 8), (true, 4, 6, 9), (true, 4, 7, 8), (true, 4, 7, 9), (true, 5, 6, 8), (true, 5, 6, 9), (true, 5, 7, 8), (true, 5, 7, 9)");

        assertThat(query("""
                SELECT *
                FROM TABLE(system.test_inputs_function(
                               input_1 => TABLE(VALUES 1, 2, 3),
                               input_2 => TABLE(VALUES 4, 5, 4, 5, 4) t2(x2) PARTITION BY x2,
                               input_3 => TABLE(VALUES 6, 7, 6) t3(x3) PARTITION BY x3,
                               input_4 => TABLE(VALUES 8, 8) t4(x4) PARTITION BY x4))
                """))
                .matches("VALUES (true, 4, 6, 8), (true, 4, 7, 8), (true, 5, 6, 8), (true, 5, 7, 8)");

        // null partitioning values
        assertThat(query("""
                SELECT *
                FROM TABLE(system.test_inputs_function(
                               input_1 => TABLE(VALUES 1, null),
                               input_2 => TABLE(VALUES 2, null, 2, null) t2(x2) PARTITION BY x2,
                               input_3 => TABLE(VALUES 3, null, 3, null) t3(x3) PARTITION BY x3,
                               input_4 => TABLE(VALUES null, null) t4(x4) PARTITION BY x4))
                """))
                .matches("VALUES (true, 2, 3, null), (true, 2, null, null), (true, null, 3, null), (true, null, null, null)");

        assertThat(query("""
                SELECT *
                FROM TABLE(system.test_inputs_function(
                               input_1 => TABLE(VALUES 1, 2, 3),
                               input_2 => TABLE(VALUES 4, 5, 4, 5, 4),
                               input_3 => TABLE(VALUES 6, 7, 6),
                               input_4 => TABLE(VALUES 8, 9)))
                """))
                .matches("VALUES true");

        assertThat(query("""
                SELECT DISTINCT regionkey, nationkey
                FROM TABLE(system.test_inputs_function(
                               input_1 => TABLE(tpch.tiny.nation),
                               input_2 => TABLE(tpch.tiny.nation) PARTITION BY regionkey ORDER BY name,
                               input_3 => TABLE(tpch.tiny.customer) PARTITION BY nationkey,
                               input_4 => TABLE(tpch.tiny.customer)))
                """))
                .matches("SELECT DISTINCT n.regionkey, c.nationkey FROM tpch.tiny.nation n, tpch.tiny.customer c");
    }

    @Test
    public void testEmptyPartitions()
    {
        // input_1 has row semantics, so it is prune when empty. input_2, input_3 and input_4 have set semantics, and are keep when empty by default
        assertThat(query("""
                SELECT *
                FROM TABLE(system.test_inputs_function(
                               input_1 => TABLE(VALUES 1, 2, 3),
                               input_2 => TABLE(SELECT 2 WHERE false),
                               input_3 => TABLE(SELECT 3 WHERE false),
                               input_4 => TABLE(SELECT 4 WHERE false)))
                """))
                .matches("VALUES true");

        assertThat(query("""
                SELECT *
                FROM TABLE(system.test_inputs_function(
                               input_1 => TABLE(SELECT 1 WHERE false),
                               input_2 => TABLE(VALUES 2),
                               input_3 => TABLE(VALUES 3),
                               input_4 => TABLE(VALUES 4)))
                """))
                .returnsEmptyResult();

        assertThat(query("""
                SELECT *
                FROM TABLE(system.test_inputs_function(
                               input_1 => TABLE(VALUES 1, 2, 3),
                               input_2 => TABLE(SELECT 2 WHERE false) t2(x2) PARTITION BY x2,
                               input_3 => TABLE(SELECT 3 WHERE false) t3(x3) PARTITION BY x3,
                               input_4 => TABLE(SELECT 4 WHERE false) t4(x4) PARTITION BY x4))
                """))
                .matches("VALUES (true, CAST(null AS integer), CAST(null AS integer), CAST(null AS integer))");

        assertThat(query("""
                SELECT *
                FROM TABLE(system.test_inputs_function(
                               input_1 => TABLE(VALUES 1, 2, 3),
                               input_2 => TABLE(SELECT 2 WHERE false) t2(x2) PARTITION BY x2,
                               input_3 => TABLE(VALUES 3, 4, 4) t3(x3) PARTITION BY x3,
                               input_4 => TABLE(VALUES 4, 4, 4, 5, 5, 5, 5) t4(x4) PARTITION BY x4))
                """))
                .matches("VALUES (true, CAST(null AS integer), 3, 4), (true, null, 4, 4), (true, null, 4, 5), (true, null, 3, 5)");

        assertThat(query("""
                SELECT *
                FROM TABLE(system.test_inputs_function(
                               input_1 => TABLE(VALUES 1, 2, 3),
                               input_2 => TABLE(SELECT 2 WHERE false) t2(x2) PARTITION BY x2,
                               input_3 => TABLE(SELECT 3 WHERE false) t3(x3) PARTITION BY x3,
                               input_4 => TABLE(VALUES 4, 5) t4(x4) PARTITION BY x4))
                """))
                .matches("VALUES (true, CAST(null AS integer), CAST(null AS integer), 4), (true, null, null, 5)");

        assertThat(query("""
                SELECT *
                FROM TABLE(system.test_inputs_function(
                               input_1 => TABLE(VALUES 1, 2, 3),
                               input_2 => TABLE(SELECT 2 WHERE false) t2(x2) PARTITION BY x2 PRUNE WHEN EMPTY,
                               input_3 => TABLE(SELECT 3 WHERE false) t3(x3) PARTITION BY x3,
                               input_4 => TABLE(VALUES 4, 5) t4(x4) PARTITION BY x4))
                """))
                .returnsEmptyResult();
    }

    @Test
    public void testCopartitioning()
    {
        // all tanbles are by default KEEP WHEN EMPTY. If there is no matching partition, it is null-completed
        assertThat(query("""
                SELECT *
                FROM TABLE(system.test_inputs_function(
                               input_1 => TABLE(VALUES 1, 2, 3),
                               input_2 => TABLE(VALUES 1, 1, 2, 2) t2(x2) PARTITION BY x2,
                               input_3 => TABLE(VALUES 4, 5) t3(x3),
                               input_4 => TABLE(VALUES 2, 2, 2, 3) t4(x4) PARTITION BY x4
                               COPARTITION (t2, t4)))
                """))
                .matches("VALUES (true, 1, null), (true, 2, 2), (true, null, 3)");

        // partition `3` from input_4 is pruned because there is no matching partition in input_2
        assertThat(query("""
                SELECT *
                FROM TABLE(system.test_inputs_function(
                               input_1 => TABLE(VALUES 1, 2, 3),
                               input_2 => TABLE(VALUES 1, 1, 2, 2) t2(x2) PARTITION BY x2 PRUNE WHEN EMPTY,
                               input_3 => TABLE(VALUES 4, 5) t3(x3),
                               input_4 => TABLE(VALUES 2, 2, 2, 3) t4(x4) PARTITION BY x4
                               COPARTITION (t2, t4)))
                """))
                .matches("VALUES (true, 1, null), (true, 2, 2)");

        // partition `1` from input_2 is pruned because there is no matching partition in input_4
        assertThat(query("""
                SELECT *
                FROM TABLE(system.test_inputs_function(
                               input_1 => TABLE(VALUES 1, 2, 3),
                               input_2 => TABLE(VALUES 1, 1, 2, 2) t2(x2) PARTITION BY x2,
                               input_3 => TABLE(VALUES 4, 5) t3(x3),
                               input_4 => TABLE(VALUES 2, 2, 2, 3) t4(x4) PARTITION BY x4 PRUNE WHEN EMPTY
                               COPARTITION (t2, t4)))
                """))
                .matches("VALUES (true, 2, 2), (true, null, 3)");

        assertThat(query("""
                SELECT *
                FROM TABLE(system.test_inputs_function(
                               input_1 => TABLE(VALUES 1, 2, 3),
                               input_2 => TABLE(VALUES 1, 1, 2, 2) t2(x2) PARTITION BY x2 PRUNE WHEN EMPTY,
                               input_3 => TABLE(VALUES 4, 5) t3(x3),
                               input_4 => TABLE(VALUES 2, 2, 2, 3) t4(x4) PARTITION BY x4 PRUNE WHEN EMPTY
                               COPARTITION (t2, t4)))
                """))
                .matches("VALUES (true, 2, 2)");

        // null partitioning values
        assertThat(query("""
                SELECT *
                FROM TABLE(system.test_inputs_function(
                               input_1 => TABLE(VALUES 1, 2, 3),
                               input_2 => TABLE(VALUES 1, 1, null, null, 2, 2) t2(x2) PARTITION BY x2,
                               input_3 => TABLE(VALUES 4, 5) t3(x3),
                               input_4 => TABLE(VALUES null, 2, 2, 2, 3) t4(x4) PARTITION BY x4
                               COPARTITION (t2, t4)))
                """))
                .matches("VALUES (true, 1, null), (true, 2, 2), (true, null, null), (true, null, 3)");

        assertThat(query("""
                SELECT *
                FROM TABLE(system.test_inputs_function(
                               input_1 => TABLE(VALUES 1, 2, 3),
                               input_2 => TABLE(VALUES 1, 1, null, null, 2, 2) t2(x2) PARTITION BY x2 PRUNE WHEN EMPTY,
                               input_3 => TABLE(VALUES 4, 5) t3(x3),
                               input_4 => TABLE(VALUES null, 2, 2, 2, 3) t4(x4) PARTITION BY x4 PRUNE WHEN EMPTY
                               COPARTITION (t2, t4)))
                """))
                .matches("VALUES (true, 2, 2), (true, null, null)");

        assertThat(query("""
                SELECT *
                FROM TABLE(system.test_inputs_function(
                               input_1 => TABLE(VALUES 1, 2, 3),
                               input_2 => TABLE(VALUES 1, 1, null, null) t2(x2) PARTITION BY x2,
                               input_3 => TABLE(VALUES 2, 2, null) t3(x3) PARTITION BY x3,
                               input_4 => TABLE(VALUES 2, 3, 3) t4(x4) PARTITION BY x4
                               COPARTITION (t2, t4, t3)))
                """))
                .matches("VALUES (true, 1, null, null), (true, null, null, null), (true, null, 2, 2), (true, null, null, 3)");

        assertThat(query("""
                SELECT *
                FROM TABLE(system.test_inputs_function(
                               input_1 => TABLE(VALUES 1, 2, 3),
                               input_2 => TABLE(VALUES 1, 1, null, null) t2(x2) PARTITION BY x2,
                               input_3 => TABLE(VALUES 2, 2, null) t3(x3) PARTITION BY x3 PRUNE WHEN EMPTY,
                               input_4 => TABLE(VALUES 2, 3, 3) t4(x4) PARTITION BY x4
                               COPARTITION (t2, t4, t3)))
                """))
                .matches("VALUES (true, CAST(null AS integer), null, null), (true, null, 2, 2)");

        assertThat(query("""
                SELECT *
                FROM TABLE(system.test_inputs_function(
                               input_1 => TABLE(VALUES 1, 2, 3),
                               input_2 => TABLE(VALUES 1, 1, null, null) t2(x2) PARTITION BY x2 PRUNE WHEN EMPTY,
                               input_3 => TABLE(VALUES 2, 2, null) t3(x3) PARTITION BY x3,
                               input_4 => TABLE(VALUES 2, 3, 3) t4(x4) PARTITION BY x4
                               COPARTITION (t2, t4, t3)))
                """))
                .matches("VALUES (true, 1, CAST(null AS integer), CAST(null AS integer)), (true, null, null, null)");

        assertThat(query("""
                SELECT *
                FROM TABLE(system.test_inputs_function(
                               input_1 => TABLE(VALUES 1, 2, 3),
                               input_2 => TABLE(VALUES 1, 1, null, null) t2(x2) PARTITION BY x2 PRUNE WHEN EMPTY,
                               input_3 => TABLE(VALUES 2, 2, null) t3(x3) PARTITION BY x3,
                               input_4 => TABLE(VALUES 2, 3, 3) t4(x4) PARTITION BY x4 PRUNE WHEN EMPTY
                               COPARTITION (t2, t4, t3)))
                """))
                .returnsEmptyResult();
    }

    @Test
    public void testPassThroughWithEmptyPartitions()
    {
        assertThat(query("""
                SELECT *
                FROM TABLE(system.pass_through(
                                            TABLE(VALUES (1, 'a'), (2, 'b')) t1(a1, b1) PARTITION BY a1,
                                            TABLE(VALUES (2, 'x'), (3, 'y')) t2(a2, b2) PARTITION BY a2
                                            COPARTITION (t1, t2)))
                """))
                .matches("VALUES (true, false, 1, 'a', null, null), (true, true, 2, 'b', 2, 'x'), (false, true, null, null, 3, 'y')");

        assertThat(query("""
                SELECT *
                FROM TABLE(system.pass_through(
                                            TABLE(VALUES (1, 'a'), (2, 'b')) t1(a1, b1) PARTITION BY a1,
                                            TABLE(SELECT 2, 'x' WHERE false) t2(a2, b2) PARTITION BY a2
                                            COPARTITION (t1, t2)))
                """))
                .matches("VALUES (true, false, 1, 'a', CAST(null AS integer), CAST(null AS VARCHAR(1))), (true, false, 2, 'b', null, null)");

        assertThat(query("""
                SELECT *
                FROM TABLE(system.pass_through(
                                            TABLE(VALUES (1, 'a'), (2, 'b')) t1(a1, b1) PARTITION BY a1,
                                            TABLE(SELECT 2, 'x' WHERE false) t2(a2, b2) PARTITION BY a2))
                """))
                .matches("VALUES (true, false, 1, 'a', CAST(null AS integer), CAST(null AS VARCHAR(1))), (true, false, 2, 'b', null, null)");
    }

    @Test
    public void testPassThroughWithEmptyInput()
    {
        assertThat(query("""
                SELECT *
                FROM TABLE(system.pass_through(
                                            TABLE(SELECT 1, 'x' WHERE false) t1(a1, b1) PARTITION BY a1,
                                            TABLE(SELECT 2, 'y' WHERE false) t2(a2, b2) PARTITION BY a2
                                            COPARTITION (t1, t2)))
                """))
                .matches("VALUES (false, false, CAST(null AS integer), CAST(null AS VARCHAR(1)), CAST(null AS integer), CAST(null AS VARCHAR(1)))");

        assertThat(query("""
                SELECT *
                FROM TABLE(system.pass_through(
                                            TABLE(SELECT 1, 'x' WHERE false) t1(a1, b1) PARTITION BY a1,
                                            TABLE(SELECT 2, 'y' WHERE false) t2(a2, b2) PARTITION BY a2))
                """))
                .matches("VALUES (false, false, CAST(null AS integer), CAST(null AS VARCHAR(1)), CAST(null AS integer), CAST(null AS VARCHAR(1)))");
    }

    @Test
    public void testInput()
    {
        assertThat(query("""
                SELECT got_input
                FROM TABLE(system.test_input(TABLE(VALUES 1)))
                """))
                .matches("VALUES true");

        assertThat(query("""
                SELECT got_input
                FROM TABLE(system.test_input(TABLE(VALUES 1, 2, 3) t(a) PARTITION BY a))
                """))
                .matches("VALUES true, true, true");

        assertThat(query("""
                SELECT got_input
                FROM TABLE(system.test_input(TABLE(SELECT 1 WHERE false)))
                """))
                .matches("VALUES false");

        assertThat(query("""
                SELECT got_input
                FROM TABLE(system.test_input(TABLE(SELECT 1 WHERE false) t(a) PARTITION BY a))
                """))
                .matches("VALUES false");

        assertThat(query("""
                SELECT got_input
                FROM TABLE(system.test_input(TABLE(SELECT * FROM tpch.tiny.orders WHERE false)))
                """))
                .matches("VALUES false");

        assertThat(query("""
                SELECT got_input
                FROM TABLE(system.test_input(TABLE(SELECT * FROM tpch.tiny.orders WHERE false) PARTITION BY orderstatus ORDER BY orderkey))
                """))
                .matches("VALUES false");
    }

    @Test
    public void testSingleSourceWithRowSemantics()
    {
        assertThat(query("""
                SELECT *
                FROM TABLE(system.test_single_input_function(TABLE(VALUES (true), (false), (true))))
                """))
                .matches("VALUES true");
    }

    @Test
    public void testConstantFunction()
    {
        assertThat(query("""
                SELECT *
                FROM TABLE(system.constant(5))
                """))
                .matches("VALUES 5");

        assertThat(query("""
                SELECT *
                FROM TABLE(system.constant(2, 10))
                """))
                .matches("VALUES (2), (2), (2), (2), (2), (2), (2), (2), (2), (2)");

        assertThat(query("""
                SELECT *
                FROM TABLE(system.constant(null, 3))
                """))
                .matches("VALUES (CAST(null AS integer)), (null), (null)");

        // value as constant expression
        assertThat(query("""
                SELECT *
                FROM TABLE(system.constant(5 * 4, 3))
                """))
                .matches("VALUES (20), (20), (20)");

        // value out of range for INTEGER type: Integer.MAX_VALUE + 1
        assertThatThrownBy(() -> query("""
                SELECT *
                FROM TABLE(system.constant(2147483648, 3))
                """))
                .hasMessage("line 2:28: Cannot cast type bigint to integer");

        assertThat(query("""
                SELECT count(*), count(DISTINCT constant_column), min(constant_column)
                FROM TABLE(system.constant(2, 1000000))
                """))
                .matches("VALUES (BIGINT '1000000', BIGINT '1', 2)");
    }

    */
}
