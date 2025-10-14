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

import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tpch.TpchPlugin;
import org.testng.annotations.Test;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;

public class TestExcludeColumnsFunction
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(testSessionBuilder().build()).build();
        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog("tpch", "tpch");
        return queryRunner;
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

    @Test
    public void testExcludeColumnsFunction()
    {
        assertQuery("SELECT * FROM tpch.tiny.nation",
                "SELECT nationkey, name, regionkey, comment FROM tpch.tiny.nation");

        assertQuery("SELECT * " +
                        "FROM TABLE(exclude_columns( " +
                        "                    input => TABLE(tpch.tiny.nation)," +
                        "                    columns => DESCRIPTOR(comment)))",
                "SELECT nationkey, name, regionkey FROM tpch.tiny.nation");

        assertQuery("SELECT * " +
                        "FROM TABLE(exclude_columns( " +
                        "                    input => TABLE(tpch.tiny.nation), " +
                        "                    columns => DESCRIPTOR(regionkey, nationkey)))",
                "SELECT name, comment FROM tpch.tiny.nation");
    }

    @Test
    public void testInvalidArgument()
    {
        assertQueryFails("SELECT *\n" +
                        "FROM TABLE(exclude_columns(\n" +
                        "                    input => TABLE(tpch.tiny.nation),\n" +
                        "                    columns => CAST(null AS DESCRIPTOR)))\n",
                "COLUMNS descriptor is null");

        assertQueryFailsExact("SELECT *\n" +
                        "FROM TABLE(exclude_columns(\n" +
                        "                    input => TABLE(tpch.tiny.nation),\n" +
                        "                    columns => DESCRIPTOR()))\n",
                "line 4:21: Invalid descriptor argument COLUMNS. Descriptors should be formatted as 'DESCRIPTOR(name [type], ...)'");

        assertQueryFailsExact("SELECT *\n" +
                        "FROM TABLE(exclude_columns(\n" +
                        "                    input => TABLE(tpch.tiny.nation),\n" +
                        "                    columns => DESCRIPTOR(foo, comment, bar)))\n",
                "Excluded columns: [foo, bar] not present in the table");

        assertQueryFails("SELECT *\n" +
                        "FROM TABLE(exclude_columns(\n" +
                        "                    input => TABLE(tpch.tiny.nation),\n" +
                        "                    columns => DESCRIPTOR(nationkey bigint, comment)))\n",
                "COLUMNS descriptor contains types");

        assertQueryFails("SELECT *\n" +
                        "FROM TABLE(exclude_columns(\n" +
                        "                    input => TABLE(tpch.tiny.nation),\n" +
                        "                    columns => DESCRIPTOR(nationkey, name, regionkey, comment)))\n",
                "All columns are excluded");
    }

    @Test
    public void testColumnResolution()
    {
        // excluded column names are matched case-insensitive
        assertQuery("SELECT *\n" +
                        "FROM TABLE(exclude_columns(\n" +
                        "                    input => TABLE(SELECT 1, 2, 3, 4, 5) t(a, B, \"c\", \"D\", e),\n" +
                        "                    columns => DESCRIPTOR(\"A\", \"b\", C, d)))\n",
                "SELECT 5");
    }

    @Test
    public void testReturnedColumnNames()
    {
        // the function preserves the incoming column names. (However, due to how the analyzer handles identifiers, these are not the canonical names according to the SQL identifier semantics.)
        assertQuery("SELECT a, b, c, d\n" +
                        "FROM TABLE(exclude_columns(\n" +
                        "                    input => TABLE(SELECT 1, 2, 3, 4, 5) t(a, B, \"c\", \"D\", e),\n" +
                        "                    columns => DESCRIPTOR(e)))\n",
                "SELECT 1, 2, 3, 4");
    }

    @Test
    public void testHiddenColumn()
    {
        assertQuery("SELECT row_number FROM tpch.tiny.region",
                "SELECT * FROM UNNEST(sequence(0, 4))");

        // the hidden column is not provided to the function
        assertQueryFails("SELECT row_number\n" +
                        "FROM TABLE(exclude_columns(\n" +
                        "                    input => TABLE(tpch.tiny.nation),\n" +
                        "                    columns => DESCRIPTOR(comment)))\n",
                "line 1:8: Column 'row_number' cannot be resolved");

        assertQueryFailsExact("SELECT *\n" +
                        "FROM TABLE(exclude_columns(\n" +
                        "                    input => TABLE(tpch.tiny.nation),\n" +
                        "                    columns => DESCRIPTOR(row_number)))\n",
                "Excluded columns: [row_number] not present in the table");
    }

    @Test
    public void testAnonymousColumn()
    {
        // cannot exclude an unnamed columns. the unnamed columns are passed on unnamed.
        assertQuery("SELECT *\n" +
                        "FROM TABLE(exclude_columns(\n" +
                        "                    input => TABLE(SELECT 1 a, 2, 3 c, 4),\n" +
                        "                    columns => DESCRIPTOR(a, c)))\n",
                "SELECT 2, 4");
    }

    @Test
    public void testDuplicateExcludedColumn()
    {
        // duplicates in excluded column names are allowed
        assertQuery("SELECT *\n" +
                        "FROM TABLE(exclude_columns(\n" +
                        "                    input => TABLE(tpch.tiny.nation),\n" +
                        "                    columns => DESCRIPTOR(comment, name, comment)))\n",
                "SELECT nationkey, regionkey FROM tpch.tiny.nation");
    }

    @Test
    public void testDuplicateInputColumn()
    {
        // all input columns with given name are excluded
        assertQuery("SELECT *\n" +
                        "FROM TABLE(exclude_columns(\n" +
                        "                    input => TABLE(SELECT 1, 2, 3, 4, 5) t(a, b, c, a, b),\n" +
                        "                    columns => DESCRIPTOR(a, b)))\n",
                "SELECT 3");
    }

    @Test
    public void testFunctionResolution()
    {
        assertQuery("SELECT *\n" +
                        "FROM TABLE(system.builtin.exclude_columns(\n" +
                        "                    input => TABLE(tpch.tiny.nation),\n" +
                        "                    columns => DESCRIPTOR(comment)))\n",
                "SELECT *\n" +
                        "FROM TABLE(exclude_columns(\n" +
                        "                    input => TABLE(tpch.tiny.nation),\n" +
                        "                    columns => DESCRIPTOR(comment)))\n");
    }

    @Test
    public void testBigInput()
    {
        assertQuery("SELECT *\n" +
                        "FROM TABLE(exclude_columns(\n" +
                        "                    input => TABLE(tpch.tiny.orders),\n" +
                        "                    columns => DESCRIPTOR(orderstatus, orderdate, orderpriority, clerk, shippriority, comment)))\n",
                "SELECT orderkey, custkey, totalprice FROM tpch.tiny.orders");
    }
}