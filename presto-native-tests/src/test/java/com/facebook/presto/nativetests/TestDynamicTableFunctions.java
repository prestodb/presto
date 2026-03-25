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
package com.facebook.presto.nativetests;

import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;
import org.testng.log4testng.Logger;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.nativeHiveQueryRunnerBuilder;
import static com.facebook.presto.sidecar.NativeSidecarPluginQueryRunnerUtils.setupNativeSidecarPlugin;

public class TestDynamicTableFunctions
        extends AbstractTestQueryFramework
{
    private static final Logger logger = Logger.getLogger(TestDynamicTableFunctions.class);
    private String storageFormat;

    @BeforeSuite
    public void buildNativeLibrary()
            throws IOException, InterruptedException
    {
        Path localPluginDir = getLocalPluginDirectory();

        // Check if the testing plugin library already exists in local directory
        if (Files.exists(localPluginDir) && hasPluginLibrary(localPluginDir)) {
            logger.info("Testing table functions plugin library found in local directory, skipping build");
            return;
        }

        logger.info("Testing table functions plugin library not found locally, building...");
        Path prestoRoot = findPrestoRoot();

        // Build the plugin using the local Makefile in presto-native-tests
        String workingDir = prestoRoot
                .resolve("presto-native-tests/presto_cpp/tests/custom_tvf_functions").toAbsolutePath().toString();

        ProcessBuilder builder = new ProcessBuilder("make", "release");
        builder.directory(new File(workingDir));
        builder.redirectErrorStream(true);

        Process process = builder.start();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                System.out.println("[BUILD OUTPUT] " + line);
            }
        }

        int exitCode = process.waitFor();
        if (exitCode != 0) {
            throw new IllegalStateException("C++ build failed with exit code " + exitCode);
        }
        logger.info("Testing table functions plugin library built successfully at: " + localPluginDir);
    }

    @BeforeClass
    @Override
    public void init()
            throws Exception
    {
        storageFormat = System.getProperty("storageFormat", "PARQUET");
        super.init();
    }

    @Override
    protected QueryRunner createQueryRunner() throws Exception
    {
        Path pluginDir = getLocalPluginDirectory();
        if (!Files.exists(pluginDir) || !hasPluginLibrary(pluginDir)) {
            throw new IllegalStateException(
                    "Plugin library not found in: " + pluginDir +
                            ". Please ensure the build completed successfully.");
        }

        QueryRunner queryRunner = nativeHiveQueryRunnerBuilder()
                .setStorageFormat(storageFormat)
                .setAddStorageFormatToPath(true)
                .setUseThrift(true)
                .setPluginDirectory(Optional.of(pluginDir.toString()))
                .setCoordinatorSidecarEnabled(true)
                .setLoadTvfPlugin(true)
                .build();
        setupNativeSidecarPlugin(queryRunner);
        return queryRunner;
    }

    @Test
    public void testRepeatTableFunction()
    {
        // Test repeating a simple table
        assertQuery(
                "SELECT * FROM TABLE(repeat(" +
                        "    INPUT => TABLE(SELECT 1 as x, 'a' as y), " +
                        "    COUNT => 3))",
                "VALUES (1, 'a'), (1, 'a'), (1, 'a')");

        // Test with multiple input rows
        assertQuery(
                "SELECT * FROM TABLE(repeat(" +
                        "    INPUT => TABLE(SELECT * FROM (VALUES (1, 'a'), (2, 'b')) t(x, y)), " +
                        "    COUNT => 2))",
                "VALUES (1, 'a'), (1, 'a'), (2, 'b'), (2, 'b')");

        // Test with COUNT = 1 (should return original rows)
        assertQuery(
                "SELECT * FROM TABLE(repeat(" +
                        "    INPUT => TABLE(SELECT 42 as num), " +
                        "    COUNT => 1))",
                "VALUES 42");

        // Test with larger COUNT
        assertQuery(
                "SELECT count(*) FROM TABLE(repeat(" +
                        "    INPUT => TABLE(SELECT * FROM (VALUES 1, 2, 3) t(x)), " +
                        "    COUNT => 5))",
                "VALUES 15");

        // Test with multiple input rows no count argument
        assertQuery(
                "SELECT * FROM TABLE(repeat(" +
                        "    INPUT => TABLE(SELECT * FROM (VALUES (1, 'a'), (2, 'b')) t(x, y))))",
                "VALUES (1, 'a'), (1, 'a'), (2, 'b'), (2, 'b')");
    }

    @Test
    public void testIdentityTableFunction()
    {
        // Test identity with single column
        assertQuery(
                "SELECT * FROM TABLE(identity_function(" +
                        "    INPUT => TABLE(SELECT 1 as x)))",
                "VALUES 1");

        // Test identity with multiple columns
        assertQuery(
                "SELECT * FROM TABLE(identity_function(" +
                        "    INPUT => TABLE(SELECT 1 as x, 'hello' as y, true as z)))",
                "VALUES (1, 'hello', true)");

        // Test identity with multiple rows
        assertQuery(
                "SELECT * FROM TABLE(identity_function(" +
                        "    INPUT => TABLE(SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) t(num, letter))))",
                "VALUES (1, 'a'), (2, 'b'), (3, 'c')");

        // Test identity preserves order
        assertQuery(
                "SELECT * FROM TABLE(identity_function(" +
                        "    INPUT => TABLE(SELECT * FROM (VALUES 5, 3, 1, 4, 2) t(x) ORDER BY x)))",
                "VALUES 1, 2, 3, 4, 5");
    }

    // Tests from TestTableFunctionInvocation
    @Test
    public void testPrimitiveDefaultArgument()
    {
        assertQuery("SELECT boolean_column FROM TABLE(simple_table_function(column => 'boolean_column', ignored => 1))", "SELECT true WHERE false");

        // skip the `ignored` argument.
        assertQuery("SELECT boolean_column FROM TABLE(simple_table_function(column => 'boolean_column'))",
                "SELECT true WHERE false");
    }

    @Test
    public void testNoArgumentsPassed()
    {
        assertQuery("SELECT col FROM TABLE(simple_table_function())",
                "SELECT true WHERE false");
    }

    @Test
    public void testIdentityFunction()
    {
        assertQuery("SELECT b, a FROM TABLE(identity_function(input => TABLE(VALUES (1, 2), (3, 4), (5, 6)) T(a, b)))",
                "VALUES (2, 1), (4, 3), (6, 5)");

        assertQuery("SELECT b, a FROM TABLE(identity_pass_through_function(input => TABLE(VALUES (1, 2), (3, 4), (5, 6)) T(a, b)))",
                "VALUES (2, 1), (4, 3), (6, 5)");

        // null partitioning value
        assertQuery("SELECT i.b, a FROM TABLE(identity_function(input => TABLE(VALUES ('x', 1), ('y', 2), ('z', null)) T(a, b) PARTITION BY b)) i",
                "VALUES (1, 'x'), (2, 'y'), (null, 'z')");

        assertQuery("SELECT b, a FROM TABLE(identity_pass_through_function(input => TABLE(VALUES ('x', 1), ('y', 2), ('z', null)) T(a, b) PARTITION BY b))",
                "VALUES (1, 'x'), (2, 'y'), (null, 'z')");
    }

    @Test
    public void testPruneAllColumns()
    {
        // function identity_pass_through_function has no proper outputs. It outputs input columns using the pass-through mechanism.
        // in this case, no pass-through columns are referenced, so they are all pruned. The function effectively produces no columns.
        assertQuery("SELECT 'a' FROM TABLE(identity_pass_through_function(input => TABLE(VALUES 1, 2, 3)))",
                "VALUES 'a', 'a', 'a'");

        // Test with empty input and all columns pruned (KEEP WHEN EMPTY)
        // This was previously crashing due to a framework bug in appendPassThroughColumns
        // when outputType_ had 0 children (all columns pruned). Fixed by adding early return.
        assertQuery("SELECT 'a' FROM TABLE(identity_pass_through_function(input => TABLE(SELECT 1 WHERE false)))",
                "SELECT 'a' WHERE false");

        // all pass-through columns are pruned. Also, the input is empty, and it has PRUNE WHEN EMPTY property, so the function is pruned out.
        assertQuery("SELECT 'a' FROM TABLE(identity_pass_through_function(input => TABLE(SELECT 1 WHERE false) PRUNE WHEN EMPTY))",
                "SELECT 'a' WHERE false");
    }

    @Test
    public void testRepeatFunction()
    {
        assertQuery(
                "SELECT * FROM TABLE(repeat(" +
                        "    INPUT => TABLE(" +
                        "        SELECT * FROM (VALUES (1, 2), (3, 4), (5, 6)) t(x, y)" +
                        "    ), " +
                        "    COUNT => 2))",
                "VALUES (1, 2), (1, 2), (3, 4), (3, 4), (5, 6), (5, 6)");

        assertQuery("SELECT * FROM TABLE(repeat(INPUT => TABLE(VALUES (1, 2), (3, 4), (5, 6)), COUNT => 2))",
                "VALUES (1, 2), (1, 2), (3, 4), (3, 4), (5, 6), (5, 6)");

        assertQuery("SELECT * FROM TABLE(repeat(INPUT => TABLE(VALUES ('a', true), ('b', false)), COUNT => 4))",
                "VALUES ('a', true), ('b', false), ('a', true), ('b', false), ('a', true), ('b', false), ('a', true), ('b', false)");

        assertQuery("SELECT * FROM TABLE(repeat(INPUT => TABLE(VALUES ('a', true), ('b', false)) t(x, y) PARTITION BY x, COUNT => 4))",
                "VALUES ('a', true), ('b', false), ('a', true), ('b', false), ('a', true), ('b', false), ('a', true), ('b', false)");

        // Test with 3 columns: 1 partitioning column (x) + 2 non-partitioning columns (y, z)
        assertQuery("SELECT * FROM TABLE(repeat(INPUT => TABLE(VALUES ('a', true, 1), ('b', false, 2)) t(x, y, z) PARTITION BY x, COUNT => 3))",
                "VALUES ('a', true, 1), ('a', true, 1), ('a', true, 1), ('b', false, 2), ('b', false, 2), ('b', false, 2)");

        assertQuery("SELECT * FROM TABLE(repeat(INPUT => TABLE(VALUES ('a', true), ('b', false)) t(x, y) ORDER BY y, COUNT => 4))",
                "VALUES ('a', true), ('b', false), ('a', true), ('b', false), ('a', true), ('b', false), ('a', true), ('b', false)");

        assertQuery("SELECT * FROM TABLE(repeat(INPUT => TABLE(VALUES ('a', true), ('b', false)) t(x, y) PARTITION BY x ORDER BY y, COUNT => 4))",
                "VALUES ('a', true), ('b', false), ('a', true), ('b', false), ('a', true), ('b', false), ('a', true), ('b', false)");
    }
    @Test
    public void testFunctionsReturningEmptyPages()
    {
        // the functions empty_output and empty_output_with_pass_through return an empty Page for each processed input Page. the argument has KEEP WHEN EMPTY property

        // non-empty input, no pass-through columns
        assertQuery(
                "SELECT * FROM TABLE(empty_output(INPUT => TABLE(SELECT 1 as x, 'a' as y)))",
                "SELECT true WHERE false");

        // non-empty input, pass-through partitioning column
        assertQuery(
                "SELECT * FROM TABLE(empty_output(INPUT => TABLE(SELECT * FROM (VALUES (1, 'a'), (2, 'b')) t(x, y)) PARTITION BY x))",
                "SELECT true WHERE false");

        // non-empty input, argument has pass-through columns
        assertQuery(
                "SELECT * FROM TABLE(empty_output_with_pass_through(INPUT => TABLE(SELECT 1 as x, 'a' as y)))",
                "SELECT true, 1, 'a' WHERE false");

        // non-empty input, argument has pass-through columns, partitioning column present
        assertQuery(
                "SELECT * FROM TABLE(empty_output_with_pass_through(INPUT => TABLE(SELECT * FROM (VALUES (1, 'a'), (2, 'b')) t(x, y)) PARTITION BY x))",
                "SELECT true, 1, 'a' WHERE false");

        // empty input, no pass-through columns
        assertQuery(
                "SELECT * FROM TABLE(empty_output(INPUT => TABLE(SELECT 1 WHERE false)))",
                "SELECT true WHERE false");

        // empty input, pass-through partitioning column
        assertQuery(
                "SELECT * FROM TABLE(empty_output(INPUT => TABLE(SELECT * FROM (VALUES (1, 'a')) t(x, y) WHERE false) PARTITION BY x))",
                "SELECT true WHERE false");

        // empty input, argument has pass-through columns
        assertQuery(
                "SELECT * FROM TABLE(empty_output_with_pass_through(INPUT => TABLE(SELECT 1 WHERE false)))",
                "SELECT true, 1 WHERE false");

        // empty input, argument has pass-through columns, partitioning column present
        assertQuery(
                "SELECT * FROM TABLE(empty_output_with_pass_through(INPUT => TABLE(SELECT * FROM (VALUES (1, 'a')) t(x, y) WHERE false) PARTITION BY x))",
                "SELECT true, 1, 'a' WHERE false");

        // Source function with no input arguments
        assertQuery(
                "SELECT * FROM TABLE(empty_source())",
                "SELECT true WHERE false");
    }

    @Test
    public void testConstantFunction()
    {
        // Test with a simple constant value
        assertQuery(
                "SELECT * FROM TABLE(constant(VALUE => 42, N => 5))",
                "VALUES 42, 42, 42, 42, 42");

        // Test with default N value (should be 1)
        assertQuery(
                "SELECT * FROM TABLE(constant(VALUE => 100))",
                "VALUES 100");

        // Test with NULL value
        assertQuery(
                "SELECT * FROM TABLE(constant(VALUE => NULL, N => 3))",
                "VALUES NULL, NULL, NULL");

        // Test with larger count
        assertQuery(
                "SELECT count(*) FROM TABLE(constant(VALUE => 7, N => 10000))",
                "VALUES 10000");

        // Test that all values are the same constant
        assertQuery(
                "SELECT DISTINCT constant_column FROM TABLE(constant(VALUE => 99, N => 100))",
                "VALUES 99");

        // Test with N = 1
        assertQuery(
                "SELECT * FROM TABLE(constant(VALUE => 1, N => 1))",
                "VALUES 1");
    }

    @Test
    public void testSingleSourceWithRowSemantics()
    {
        assertQuery("SELECT * FROM TABLE(test_single_input_function(TABLE(VALUES (true), (false), (true))))", "VALUES true");
    }

    @Test
    public void testPrunePassThroughColumns()
    {
        // function pass_through has 2 proper columns, and it outputs all columns from both inputs using the pass-through mechanism.
        // all columns are referenced
        assertQuery("SELECT p1, p2, x1, x2, y1, y2 " +
                        "FROM TABLE(pass_through( " +
                        "                            TABLE(VALUES (1, 'a'), (2, 'b'), (3, 'c')) t1(x1, x2)," +
                        "                            TABLE(VALUES (4, 'd'), (5, 'e')) t2(y1, y2))) t(p1, p2)",
                "VALUES (true, true, 3, 'c', 5, 'e')");

        // all pass-through columns are referenced. Proper columns are not referenced, but they are not pruned.
        assertQuery("SELECT x1, x2, y1, y2 " +
                        "FROM TABLE(pass_through( " +
                        "                            TABLE(VALUES (1, 'a'), (2, 'b'), (3, 'c')) t1(x1, x2)," +
                        "                            TABLE(VALUES (4, 'd'), (5, 'e')) t2(y1, y2))) t(p1, p2)",
                "VALUES (3, 'c', 5, 'e')");

        // some pass-through columns are referenced. Unreferenced pass-through columns are pruned.
        assertQuery("SELECT x2, y2 " +
                        "FROM TABLE(pass_through(" +
                        "                            TABLE(VALUES (1, 'a'), (2, 'b'), (3, 'c')) t1(x1, x2)," +
                        "                            TABLE(VALUES (4, 'd'), (5, 'e')) t2(y1, y2))) t(p1, p2)",
                "VALUES ('c', 'e')");

        assertQuery("SELECT y1, y2 " +
                        "FROM TABLE(pass_through( " +
                        "                            TABLE(VALUES (1, 'a'), (2, 'b'), (3, 'c')) t1(x1, x2)," +
                        "                            TABLE(VALUES (4, 'd'), (5, 'e')) t2(y1, y2))) t(p1, p2)",
                "VALUES (5, 'e')");

        // no pass-through columns are referenced. Unreferenced pass-through columns are pruned.
        assertQuery("SELECT 'x' " +
                        "FROM TABLE(pass_through( " +
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
                        "FROM TABLE(pass_through( " +
                        "                            TABLE(SELECT 1, 'a' WHERE FALSE) t1(x1, x2)," +
                        "                            TABLE(SELECT 4, 'd' WHERE FALSE) t2(y1, y2))) t(p1, p2)",
                "VALUES (false, false, CAST(null AS integer), CAST(null AS varchar(1)), CAST(null AS integer), CAST(null AS varchar(1)))");

        // all pass-through columns are referenced. Proper columns are not referenced, but they are not pruned.
        assertQuery("SELECT x1, x2, y1, y2 " +
                        "FROM TABLE(pass_through( " +
                        "                            TABLE(SELECT 1, 'a' WHERE FALSE) t1(x1, x2)," +
                        "                            TABLE(SELECT 4, 'd' WHERE FALSE) t2(y1, y2))) t(p1, p2) ",
                "VALUES (CAST(null AS integer), CAST(null AS varchar(1)), CAST(null AS integer), CAST(null AS varchar(1)))");

        // some pass-through columns are referenced. Unreferenced pass-through columns are pruned.
        assertQuery("SELECT x2, y2 " +
                        "FROM TABLE(pass_through( " +
                        "                            TABLE(SELECT 1, 'a' WHERE FALSE) t1(x1, x2)," +
                        "                            TABLE(SELECT 4, 'd' WHERE FALSE) t2(y1, y2))) t(p1, p2)",
                "VALUES (CAST(null AS varchar(1)), CAST(null AS varchar(1)))");

        assertQuery("SELECT y1, y2 " +
                        "FROM TABLE(pass_through( " +
                        "                            TABLE(SELECT 1, 'a' WHERE FALSE) t1(x1, x2)," +
                        "                            TABLE(SELECT 4, 'd' WHERE FALSE) t2(y1, y2))) t(p1, p2)",
                "VALUES (CAST(null AS integer), CAST(null AS varchar(1)))");

        // no pass-through columns are referenced. Unreferenced pass-through columns are pruned.
        assertQuery("SELECT 'x' " +
                        "FROM TABLE(pass_through(" +
                        "                            TABLE(SELECT 1, 'a' WHERE FALSE) t1(x1, x2)," +
                        "                            TABLE(SELECT 4, 'd' WHERE FALSE) t2(y1, y2))) t(p1, p2)",
                "VALUES ('x')");
    }

    @Test
    public void testInputPartitioning()
    {
        // table function test_inputs_function has four table arguments. input_1 has row semantics. input_2, input_3 and input_4 have set semantics.
        // the function outputs one row per each tuple of partition it processes. The row includes a true value, and partitioning values.
        assertQuery("SELECT *" +
                        "FROM TABLE(test_inputs_function(" +
                        "input_1 => TABLE(VALUES 1, 2, 3)," +
                        "input_2 => TABLE(VALUES 4, 5, 4, 5, 4) t2(x2) PARTITION BY x2," +
                        "input_3 => TABLE(VALUES 6, 7, 6) t3(x3) PARTITION BY x3," +
                        "input_4 => TABLE(VALUES 8, 9)))",
                "VALUES (true, 4, 6), (true, 4, 7), (true, 5, 6), (true, 5, 7)");

        assertQuery("SELECT * FROM TABLE(test_inputs_function(" +
                        "input_1 => TABLE(VALUES 1, 2, 3)," +
                        "input_2 => TABLE(VALUES 4, 5, 4, 5, 4) t2(x2) PARTITION BY x2," +
                        "input_3 => TABLE(VALUES 6, 7, 6) t3(x3) PARTITION BY x3," +
                        "input_4 => TABLE(VALUES 8, 9) t4(x4) PARTITION BY x4))",
                "VALUES (true, 4, 6, 8), (true, 4, 6, 9), (true, 4, 7, 8), (true, 4, 7, 9), (true, 5, 6, 8), (true, 5, 6, 9), (true, 5, 7, 8), (true, 5, 7, 9)");

        assertQuery("SELECT * FROM TABLE(test_inputs_function(" +
                        "input_1 => TABLE(VALUES 1, 2, 3)," +
                        "input_2 => TABLE(VALUES 4, 5, 4, 5, 4) t2(x2) PARTITION BY x2," +
                        "input_3 => TABLE(VALUES 6, 7, 6) t3(x3) PARTITION BY x3," +
                        "input_4 => TABLE(VALUES 8, 8) t4(x4) PARTITION BY x4))",
                "VALUES (true, 4, 6, 8), (true, 4, 7, 8), (true, 5, 6, 8), (true, 5, 7, 8)");

        // null partitioning values
        assertQuery("SELECT * FROM TABLE(test_inputs_function(" +
                        "input_1 => TABLE(VALUES 1, null)," +
                        "input_2 => TABLE(VALUES 2, null, 2, null) t2(x2) PARTITION BY x2," +
                        "input_3 => TABLE(VALUES 3, null, 3, null) t3(x3) PARTITION BY x3," +
                        "input_4 => TABLE(VALUES null, null) t4(x4) PARTITION BY x4))",
                "VALUES (true, 2, 3, null), (true, 2, null, null), (true, null, 3, null), (true, null, null, null)");

        assertQuery("SELECT * FROM TABLE(test_inputs_function(" +
                        "input_1 => TABLE(VALUES 1, 2, 3)," +
                        "input_2 => TABLE(VALUES 4, 5, 4, 5, 4)," +
                        "input_3 => TABLE(VALUES 6, 7, 6)," +
                        "input_4 => TABLE(VALUES 8, 9)))",
                "VALUES true");
    }

    @Test
    public void testEmptyPartitions()
    {
        // input_1 has row semantics, so it is prune when empty. input_2, input_3 and input_4 have set semantics, and are keep when empty by default
        assertQuery("SELECT * FROM TABLE(test_inputs_function(" +
                        "input_1 => TABLE(VALUES 1, 2, 3)," +
                        "input_2 => TABLE(SELECT 2 WHERE false)," +
                        "input_3 => TABLE(SELECT 3 WHERE false)," +
                        "input_4 => TABLE(SELECT 4 WHERE false)))",
                "VALUES true");

        assertQueryReturnsEmptyResult("SELECT * FROM TABLE(test_inputs_function(" +
                "input_1 => TABLE(SELECT 1 WHERE false)," +
                "input_2 => TABLE(VALUES 2)," +
                "input_3 => TABLE(VALUES 3)," +
                "input_4 => TABLE(VALUES 4)))");

        assertQuery("SELECT * FROM TABLE(test_inputs_function(" +
                        "input_1 => TABLE(VALUES 1, 2, 3)," +
                        "input_2 => TABLE(SELECT 2 WHERE false) t2(x2) PARTITION BY x2," +
                        "input_3 => TABLE(SELECT 3 WHERE false) t3(x3) PARTITION BY x3," +
                        "input_4 => TABLE(SELECT 4 WHERE false) t4(x4) PARTITION BY x4))",
                "VALUES (true, CAST(null AS integer), CAST(null AS integer), CAST(null AS integer))");

        assertQuery("SELECT * FROM TABLE(test_inputs_function(" +
                        "input_1 => TABLE(VALUES 1, 2, 3)," +
                        "input_2 => TABLE(SELECT 2 WHERE false) t2(x2) PARTITION BY x2," +
                        "input_3 => TABLE(VALUES 3, 4, 4) t3(x3) PARTITION BY x3," +
                        "input_4 => TABLE(VALUES 4, 4, 4, 5, 5, 5, 5) t4(x4) PARTITION BY x4))",
                "VALUES (true, CAST(null AS integer), 3, 4), (true, null, 4, 4), (true, null, 4, 5), (true, null, 3, 5)");

        assertQuery("SELECT * FROM TABLE(test_inputs_function(" +
                        "input_1 => TABLE(VALUES 1, 2, 3)," +
                        "input_2 => TABLE(SELECT 2 WHERE false) t2(x2) PARTITION BY x2," +
                        "input_3 => TABLE(SELECT 3 WHERE false) t3(x3) PARTITION BY x3," +
                        "input_4 => TABLE(VALUES 4, 5) t4(x4) PARTITION BY x4))",
                "VALUES (true, CAST(null AS integer), CAST(null AS integer), 4), (true, null, null, 5)");

        assertQueryReturnsEmptyResult("SELECT * FROM TABLE(test_inputs_function(" +
                "input_1 => TABLE(VALUES 1, 2, 3)," +
                "input_2 => TABLE(SELECT 2 WHERE false) t2(x2) PARTITION BY x2 PRUNE WHEN EMPTY," +
                "input_3 => TABLE(SELECT 3 WHERE false) t3(x3) PARTITION BY x3," +
                "input_4 => TABLE(VALUES 4, 5) t4(x4) PARTITION BY x4))");
    }

    @Test
    public void testCopartitioning()
    {
        // all tables are by default KEEP WHEN EMPTY. If there is no matching partition, it is null-completed
        assertQuery("SELECT * FROM TABLE(test_inputs_function(" +
                        "input_1 => TABLE(VALUES 1, 2, 3)," +
                        "input_2 => TABLE(VALUES 1, 1, 2, 2) t2(x2) PARTITION BY x2," +
                        "input_3 => TABLE(VALUES 4, 5) t3(x3)," +
                        "input_4 => TABLE(VALUES 2, 2, 2, 3) t4(x4) PARTITION BY x4 " +
                        "COPARTITION (t2, t4)))",
                "VALUES (true, 1, null), (true, 2, 2), (true, null, 3)");

        // partition `3` from input_4 is pruned because there is no matching partition in input_2
        assertQuery("SELECT *" +
                        "FROM TABLE(test_inputs_function(" +
                        "input_1 => TABLE(VALUES 1, 2, 3)," +
                        "input_2 => TABLE(VALUES 1, 1, 2, 2) t2(x2) PARTITION BY x2 PRUNE WHEN EMPTY," +
                        "input_3 => TABLE(VALUES 4, 5) t3(x3)," +
                        "input_4 => TABLE(VALUES 2, 2, 2, 3) t4(x4) PARTITION BY x4 " +
                        "COPARTITION (t2, t4)))",
                "VALUES (true, 1, null), (true, 2, 2)");

        // partition `1` from input_2 is pruned because there is no matching partition in input_4
        assertQuery("SELECT *" +
                        "FROM TABLE(test_inputs_function(" +
                        "input_1 => TABLE(VALUES 1, 2, 3)," +
                        "input_2 => TABLE(VALUES 1, 1, 2, 2) t2(x2) PARTITION BY x2," +
                        "input_3 => TABLE(VALUES 4, 5) t3(x3)," +
                        "input_4 => TABLE(VALUES 2, 2, 2, 3) t4(x4) PARTITION BY x4 PRUNE WHEN EMPTY " +
                        "COPARTITION (t2, t4)))",
                "VALUES (true, 2, 2), (true, null, 3)");

        assertQuery("SELECT *" +
                        "FROM TABLE(test_inputs_function(" +
                        "input_1 => TABLE(VALUES 1, 2, 3)," +
                        "input_2 => TABLE(VALUES 1, 1, 2, 2) t2(x2) PARTITION BY x2 PRUNE WHEN EMPTY," +
                        "input_3 => TABLE(VALUES 4, 5) t3(x3)," +
                        "input_4 => TABLE(VALUES 2, 2, 2, 3) t4(x4) PARTITION BY x4 PRUNE WHEN EMPTY " +
                        "COPARTITION (t2, t4)))",
                "VALUES (true, 2, 2)");

        // null partitioning values
        assertQuery("SELECT *" +
                        "FROM TABLE(test_inputs_function(" +
                        "input_1 => TABLE(VALUES 1, 2, 3)," +
                        "input_2 => TABLE(VALUES 1, 1, null, null, 2, 2) t2(x2) PARTITION BY x2," +
                        "input_3 => TABLE(VALUES 4, 5) t3(x3)," +
                        "input_4 => TABLE(VALUES null, 2, 2, 2, 3) t4(x4) PARTITION BY x4 " +
                        "COPARTITION (t2, t4)))",
                "VALUES (true, 1, null), (true, 2, 2), (true, null, null), (true, null, 3)");

        assertQuery("SELECT *" +
                        "FROM TABLE(test_inputs_function(" +
                        "input_1 => TABLE(VALUES 1, 2, 3)," +
                        "input_2 => TABLE(VALUES 1, 1, null, null, 2, 2) t2(x2) PARTITION BY x2 PRUNE WHEN EMPTY," +
                        "input_3 => TABLE(VALUES 4, 5) t3(x3)," +
                        "input_4 => TABLE(VALUES null, 2, 2, 2, 3) t4(x4) PARTITION BY x4 PRUNE WHEN EMPTY " +
                        "COPARTITION (t2, t4)))",
                "VALUES (true, 2, 2), (true, null, null)");

        assertQuery("SELECT *" +
                        "FROM TABLE(test_inputs_function(" +
                        "input_1 => TABLE(VALUES 1, 2, 3)," +
                        "input_2 => TABLE(VALUES 1, 1, null, null) t2(x2) PARTITION BY x2," +
                        "input_3 => TABLE(VALUES 2, 2, null) t3(x3) PARTITION BY x3," +
                        "input_4 => TABLE(VALUES 2, 3, 3) t4(x4) PARTITION BY x4 " +
                        "COPARTITION (t2, t4, t3)))",
                "VALUES (true, 1, null, null), (true, null, null, null), (true, null, 2, 2), (true, null, null, 3)");

        assertQuery("SELECT *" +
                        "FROM TABLE(test_inputs_function(" +
                        "input_1 => TABLE(VALUES 1, 2, 3)," +
                        "input_2 => TABLE(VALUES 1, 1, null, null) t2(x2) PARTITION BY x2," +
                        "input_3 => TABLE(VALUES 2, 2, null) t3(x3) PARTITION BY x3 PRUNE WHEN EMPTY," +
                        "input_4 => TABLE(VALUES 2, 3, 3) t4(x4) PARTITION BY x4 " +
                        "COPARTITION (t2, t4, t3)))",
                "VALUES (true, CAST(null AS integer), null, null), (true, null, 2, 2)");

        assertQuery("SELECT *" +
                        "FROM TABLE(test_inputs_function(" +
                        "input_1 => TABLE(VALUES 1, 2, 3)," +
                        "input_2 => TABLE(VALUES 1, 1, null, null) t2(x2) PARTITION BY x2 PRUNE WHEN EMPTY," +
                        "input_3 => TABLE(VALUES 2, 2, null) t3(x3) PARTITION BY x3," +
                        "input_4 => TABLE(VALUES 2, 3, 3) t4(x4) PARTITION BY x4 " +
                        "COPARTITION (t2, t4, t3)))",
                "VALUES (true, 1, CAST(null AS integer), CAST(null AS integer)), (true, null, null, null)");

        assertQueryReturnsEmptyResult(
                "SELECT *" +
                        "FROM TABLE(test_inputs_function(" +
                        "input_1 => TABLE(VALUES 1, 2, 3)," +
                        "input_2 => TABLE(VALUES 1, 1, null, null) t2(x2) PARTITION BY x2 PRUNE WHEN EMPTY," +
                        "input_3 => TABLE(VALUES 2, 2, null) t3(x3) PARTITION BY x3," +
                        "input_4 => TABLE(VALUES 2, 3, 3) t4(x4) PARTITION BY x4 PRUNE WHEN EMPTY " +
                        "COPARTITION (t2, t4, t3)))");
    }

    private static Path getLocalPluginDirectory()
    {
        Path prestoRoot = findPrestoRoot();
        // Check both debug and release build directories
        List<Path> candidates = ImmutableList.of(
                prestoRoot.resolve("presto-native-tests/_build/debug/presto_cpp/tests/custom_tvf_functions"),
                prestoRoot.resolve("presto-native-tests/_build/release/presto_cpp/tests/custom_tvf_functions"));
        // Return the first one that exists, or the release path as default
        return candidates.stream()
                .filter(Files::exists)
                .findFirst()
                .orElse(candidates.get(0));
    }

    private static boolean hasPluginLibrary(Path pluginDir)
    {
        if (!Files.exists(pluginDir)) {
            return false;
        }
        try (Stream<Path> files = Files.list(pluginDir)) {
            return files.anyMatch(path -> {
                String name = path.getFileName().toString();
                return name.startsWith("libpresto_testing_tvf_plugin") &&
                        (name.endsWith(".so") || name.endsWith(".dylib"));
            });
        }
        catch (IOException e) {
            return false;
        }
    }

    private static Path findPrestoRoot()
    {
        Path dir = Paths.get(System.getProperty("user.dir")).toAbsolutePath();
        while (dir != null) {
            if (Files.exists(dir.resolve("presto-native-tests")) ||
                    Files.exists(dir.resolve("presto-native-execution"))) {
                return dir;
            }
            dir = dir.getParent();
        }
        throw new IllegalStateException("Could not locate presto root directory.");
    }
}

