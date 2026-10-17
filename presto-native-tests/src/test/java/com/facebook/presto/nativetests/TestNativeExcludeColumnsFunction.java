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

import com.facebook.presto.testing.ExpectedQueryRunner;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.javaHiveQueryRunnerBuilder;
import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.nativeHiveQueryRunnerBuilder;
import static com.facebook.presto.sidecar.NativeSidecarPluginQueryRunnerUtils.setupNativeSidecarPlugin;
import static java.lang.Boolean.parseBoolean;

/**
 * Test for exclude_columns table function with native execution using inline VALUES data.
 */
public class TestNativeExcludeColumnsFunction
        extends AbstractTestQueryFramework
{
    private String storageFormat;
    private boolean sidecarEnabled;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        QueryRunner expected = nativeHiveQueryRunnerBuilder()
                .setStorageFormat(storageFormat)
                .setAddStorageFormatToPath(true)
                .setUseThrift(true)
                .setCoordinatorSidecarEnabled(sidecarEnabled)
                .setLoadTvfPlugin(true)
                .build();
        if (sidecarEnabled) {
            setupNativeSidecarPlugin(expected);
        }
        expected.loadFunctionsFromTableFunctionRegistries();
        return expected;
    }

    @Override
    protected ExpectedQueryRunner createExpectedQueryRunner()
            throws Exception
    {
        return javaHiveQueryRunnerBuilder()
                .setStorageFormat(storageFormat)
                .setAddStorageFormatToPath(true)
                .build();
    }

    @BeforeClass
    @Override
    public void init()
            throws Exception
    {
        storageFormat = System.getProperty("storageFormat", "PARQUET");
        sidecarEnabled = parseBoolean(System.getProperty("sidecarEnabled", "true"));
        super.init();
    }

    @Test
    public void testExcludeColumnsFunction()
    {
        // Test excluding a single column
        assertQuery("SELECT * " +
                        "FROM TABLE(exclude_columns( " +
                        "    input => TABLE(SELECT * FROM (VALUES (1, 'USA', 2, 'United States')) t(nationkey, name, regionkey, comment))," +
                        "    columns => DESCRIPTOR(comment)))",
                "SELECT * FROM (VALUES (1, 'USA', 2)) t(nationkey, name, regionkey)");

        // Test excluding multiple columns
        assertQuery("SELECT * " +
                        "FROM TABLE(exclude_columns( " +
                        "    input => TABLE(SELECT * FROM (VALUES (1, 'USA', 2, 'United States')) t(nationkey, name, regionkey, comment)), " +
                        "    columns => DESCRIPTOR(regionkey, nationkey)))",
                "SELECT * FROM (VALUES ('USA', 'United States')) t(name, comment)");
    }

    @Test
    public void testInvalidArgument()
    {
        // Test null descriptor
        assertQueryFails("SELECT * " +
                        "FROM TABLE(exclude_columns( " +
                        "    input => TABLE(SELECT * FROM (VALUES (1, 'USA', 2, 'United States')) t(nationkey, name, regionkey, comment)), " +
                        "    columns => CAST(null AS DESCRIPTOR)))",
                "COLUMNS descriptor is null");

        // Test empty descriptor
        assertQueryFailsExact("SELECT * " +
                        "FROM TABLE(exclude_columns( " +
                        "    input => TABLE(SELECT * FROM (VALUES (1, 'USA', 2, 'United States')) t(nationkey, name, regionkey, comment)), " +
                        "    columns => DESCRIPTOR()))",
                "line 1:156: Invalid descriptor argument COLUMNS. Descriptors should be formatted as 'DESCRIPTOR(name [type], ...)'");

        // Test non-existent columns
        assertQueryFailsExact("SELECT * " +
                        "FROM TABLE(exclude_columns( " +
                        "    input => TABLE(SELECT * FROM (VALUES (1, 'USA', 2, 'United States')) t(nationkey, name, regionkey, comment)), " +
                        "    columns => DESCRIPTOR(foo, comment, bar)))",
                "Excluded columns: [foo, bar] not present in the table");

        // Test descriptor with types
        assertQueryFailsExact("SELECT * " +
                        "FROM TABLE(exclude_columns( " +
                        "    input => TABLE(SELECT * FROM (VALUES (1, 'USA', 2, 'United States')) t(nationkey, name, regionkey, comment)), " +
                        "    columns => DESCRIPTOR(nationkey bigint, comment)))",
                "(2 vs. 1) Descriptor names and types must have the same size: 2 names vs 1 types");

        assertQueryFails("SELECT * " +
                        "FROM TABLE(exclude_columns( " +
                        "    input => TABLE(SELECT * FROM (VALUES (1, 'USA', 2, 'United States')) t(nationkey, name, regionkey, comment)), " +
                        "    columns => DESCRIPTOR(nationkey bigint, comment varchar)))",
                "COLUMNS descriptor contains types");

        // Test excluding all columns
        assertQueryFails("SELECT * " +
                        "FROM TABLE(exclude_columns( " +
                        "    input => TABLE(SELECT * FROM (VALUES (1, 'USA', 2, 'United States')) t(nationkey, name, regionkey, comment)), " +
                        "    columns => DESCRIPTOR(nationkey, name, regionkey, comment)))",
                "All columns are excluded");
    }

    @Test
    public void testColumnResolution()
    {
        // excluded column names are matched case-insensitive
        assertQuery("SELECT * " +
                        "FROM TABLE(exclude_columns( " +
                        "    input => TABLE(SELECT 1, 2, 3, 4, 5) t(a, B, \"c\", \"D\", e), " +
                        "    columns => DESCRIPTOR(\"A\", \"b\", C, d)))",
                "SELECT 5");
    }

    @Test
    public void testReturnedColumnNames()
    {
        // the function preserves the incoming column names. (However, due to how the analyzer handles identifiers, these are not the canonical names according to the SQL identifier semantics.)
        assertQuery("SELECT a, b, c, d " +
                        "FROM TABLE(exclude_columns( " +
                        "    input => TABLE(SELECT 1, 2, 3, 4, 5) t(a, B, \"c\", \"D\", e), " +
                        "    columns => DESCRIPTOR(e)))",
                "SELECT 1, 2, 3, 4");
    }

    @Test
    public void testAnonymousColumn()
    {
        // cannot exclude an unnamed columns. the unnamed columns are passed on unnamed.
        assertQuery("SELECT * " +
                        "FROM TABLE(exclude_columns( " +
                        "    input => TABLE(SELECT 1 a, 2, 3 c, 4), " +
                        "    columns => DESCRIPTOR(a, c)))",
                "SELECT 2, 4");
    }

    @Test
    public void testDuplicateExcludedColumn()
    {
        // duplicates in excluded column names are allowed
        assertQuery("SELECT * " +
                        "FROM TABLE(exclude_columns( " +
                        "    input => TABLE(SELECT * FROM (VALUES (1, 'USA', 2, 'United States')) t(nationkey, name, regionkey, comment)), " +
                        "    columns => DESCRIPTOR(comment, name, comment)))",
                "SELECT * FROM (VALUES (1, 2)) t(nationkey, regionkey)");
    }

    @Test
    public void testDuplicateInputColumn()
    {
        // all input columns with given name are excluded
        assertQuery("SELECT * " +
                        "FROM TABLE(exclude_columns( " +
                        "    input => TABLE(SELECT 1, 2, 3, 4, 5) t(a, b, c, a, b), " +
                        "    columns => DESCRIPTOR(a, b)))",
                "SELECT 3");
    }

    @Test
    public void testBigInput()
    {
        // Test with multiple rows to simulate larger input
        assertQuery("SELECT * " +
                        "FROM TABLE(exclude_columns( " +
                        "    input => TABLE(SELECT * FROM (VALUES " +
                        "        (1, 100, 1000.50, 'O', CAST('1996-01-02' AS DATE), '5-LOW', 'Clerk#000000951', 0, 'nstructions sleep furiously among '), " +
                        "        (2, 101, 2000.75, 'F', CAST('1996-12-01' AS DATE), '1-URGENT', 'Clerk#000000880', 0, ' foxes. pending accounts at the pending'), " +
                        "        (3, 102, 3000.25, 'F', CAST('1993-10-14' AS DATE), '5-LOW', 'Clerk#000000955', 0, 'sly final accounts boost. carefully regular ideas cajole carefully')) " +
                        "        t(orderkey, custkey, totalprice, orderstatus, orderdate, orderpriority, clerk, shippriority, comment)), " +
                        "    columns => DESCRIPTOR(orderstatus, orderdate, orderpriority, clerk, shippriority, comment)))",
                "SELECT * FROM (VALUES " +
                        "(1, 100, 1000.50), " +
                        "(2, 101, 2000.75), " +
                        "(3, 102, 3000.25)) " +
                        "t(orderkey, custkey, totalprice)");
    }
}
