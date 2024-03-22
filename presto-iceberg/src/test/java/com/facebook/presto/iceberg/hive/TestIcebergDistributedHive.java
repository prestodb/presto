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
package com.facebook.presto.iceberg.hive;

import com.facebook.presto.Session;
import com.facebook.presto.hive.metastore.ExtendedHiveMetastore;
import com.facebook.presto.hive.metastore.file.FileHiveMetastore;
import com.facebook.presto.iceberg.IcebergDistributedTestBase;
import com.facebook.presto.iceberg.IcebergUtil;
import com.facebook.presto.metadata.CatalogManager;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.collect.ImmutableMap;
import org.apache.iceberg.Table;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.regex.Pattern;

import static com.facebook.presto.hive.metastore.CachingHiveMetastore.memoizeMetastore;
import static com.facebook.presto.iceberg.CatalogType.HIVE;
import static com.facebook.presto.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static com.facebook.presto.tests.sql.TestTable.randomTableSuffix;
import static java.lang.String.format;
import static org.testng.Assert.assertFalse;

@Test
public class TestIcebergDistributedHive
        extends IcebergDistributedTestBase
{
    public TestIcebergDistributedHive()
    {
        super(HIVE, ImmutableMap.of("iceberg.hive-statistics-merge-strategy", "USE_NULLS_FRACTION_AND_NDV"));
    }

    @Override
    public void testNDVsAtSnapshot()
    {
        // ignore because HMS doesn't support statistics versioning
    }

    @Override
    public void testStatsByDistance()
    {
        // ignore because HMS doesn't support statistics versioning
    }

    @Override
    protected Table loadTable(String tableName)
    {
        CatalogManager catalogManager = getDistributedQueryRunner().getCoordinator().getCatalogManager();
        ConnectorId connectorId = catalogManager.getCatalog(ICEBERG_CATALOG).get().getConnectorId();

        return IcebergUtil.getHiveIcebergTable(getFileHiveMetastore(),
                getHdfsEnvironment(),
                getQueryRunner().getDefaultSession().toConnectorSession(connectorId),
                SchemaTableName.valueOf("tpch." + tableName));
    }

    protected ExtendedHiveMetastore getFileHiveMetastore()
    {
        FileHiveMetastore fileHiveMetastore = new FileHiveMetastore(getHdfsEnvironment(),
                getCatalogDirectory().getPath(),
                "test");
        return memoizeMetastore(fileHiveMetastore, false, 1000, 0);
    }

    @Test
    public void testSortByAllTypes()
    {
        String tableName = "test_sort_by_all_types_" + randomTableSuffix();
        assertUpdate("" +
                "CREATE TABLE " + tableName + " (" +
                "  a_boolean boolean, " +
                "  an_integer integer, " +
                "  a_bigint bigint, " +
                "  a_real real, " +
                "  a_double double, " +
                "  a_short_decimal decimal(5,2), " +
                "  a_long_decimal decimal(38,20), " +
                "  a_varchar varchar, " +
                "  a_varbinary varbinary, " +
                "  a_date date, " +
                "  a_timestamp timestamp, " +
                "  an_array array(varchar), " +
                "  a_map map(integer, varchar) " +
                ") " +
                "WITH (" +
                "sorted_by = ARRAY[" +
                "  'a_boolean', " +
                "  'an_integer', " +
                "  'a_bigint', " +
                "  'a_real', " +
                "  'a_double', " +
                "  'a_short_decimal', " +
                "  'a_long_decimal', " +
                "  'a_varchar', " +
                "  'a_varbinary', " +
                "  'a_date', " +
                "  'a_timestamp' " +
                "  ]" +
                ")");
        String values = "(" +
                "true, " +
                "1, " +
                "BIGINT '2', " +
                "REAL '3.0', " +
                "DOUBLE '4.0', " +
                "DECIMAL '5.00', " +
                "CAST(DECIMAL '6.00' AS decimal(38,20)), " +
                "VARCHAR 'seven', " +
                "X'88888888', " +
                "DATE '2022-09-09', " +
                "TIMESTAMP '2022-11-11 11:11:11.000000', " +
                "ARRAY[VARCHAR 'four', 'teen'], " +
                "MAP(ARRAY[15], ARRAY[VARCHAR 'fifteen']))";
        String highValues = "(" +
                "true, " +
                "999999999, " +
                "BIGINT '999999999', " +
                "REAL '999.999', " +
                "DOUBLE '999.999', " +
                "DECIMAL '999.99', " +
                "DECIMAL '6.00', " +
                "'zzzzzzzzzzzzzz', " +
                "X'FFFFFFFF', " +
                "DATE '2099-12-31', " +
                "TIMESTAMP '2099-12-31 23:59:59.000000', " +
                "ARRAY['zzzz', 'zzzz'], " +
                "MAP(ARRAY[999], ARRAY['zzzz']))";
        String lowValues = "(" +
                "false, " +
                "0, " +
                "BIGINT '0', " +
                "REAL '0', " +
                "DOUBLE '0', " +
                "DECIMAL '0', " +
                "DECIMAL '0', " +
                "'', " +
                "X'00000000', " +
                "DATE '2000-01-01', " +
                "TIMESTAMP '2000-01-01 00:00:00.000000', " +
                "ARRAY['', ''], " +
                "MAP(ARRAY[0], ARRAY['']))";

        assertUpdate("INSERT INTO " + tableName + " VALUES " + values + ", " + highValues + ", " + lowValues, 3);
        dropTable(getSession(), tableName);
    }

    @Test
    public void testEmptySortedByList()
    {
        String tableName = "test_empty_sorted_by_list_" + randomTableSuffix();
        assertUpdate("" +
                "CREATE TABLE " + tableName + " (a_boolean boolean, an_integer integer) " +
                "  WITH (partitioning = ARRAY['an_integer'], sorted_by = ARRAY[])");
        dropTable(getSession(), tableName);
    }

    @Test(dataProvider = "sortedTableWithQuotedIdentifierCasing")
    public void testCreateSortedTableWithQuotedIdentifierCasing(String columnName, String sortField)
    {
        String tableName = "test_create_sorted_table_with_quotes_" + randomTableSuffix();
        assertUpdate(format("CREATE TABLE %s (%s bigint) WITH (sorted_by = ARRAY['%s'])", tableName, columnName, sortField));
        dropTable(getSession(), tableName);
    }

    @DataProvider(name = "sortedTableWithQuotedIdentifierCasing")
    public static Object[][] sortedTableWithQuotedIdentifierCasing()
    {
        return new Object[][] {
                {"col", "col"},
                {"\"col\"", "col"},
                {"col", "\"col\""},
                {"\"col\"", "\"col\""},
        };
    }

    @Test(dataProvider = "sortedTableWithSortTransform")
    public void testCreateSortedTableWithSortTransform(String columnName, String sortField)
    {
        String tableName = "test_sort_with_transform_" + randomTableSuffix();
        String query = format("CREATE TABLE %s (%s TIMESTAMP) WITH (sorted_by = ARRAY['%s'])", tableName, columnName, sortField);
        assertQueryFails(query, Pattern.quote(format("Unable to parse sort field: [%s]", sortField)));
    }

    @DataProvider(name = "sortedTableWithSortTransform")
    public static Object[][] sortedTableWithSortTransform()
    {
        return new Object[][] {
                {"col", "bucket(col, 3)"},
                {"col", "bucket(\"col\", 3)"},
                {"col", "truncate(col, 3)"},
                {"col", "year(col)"},
                {"col", "month(col)"},
                {"col", "date(col)"},
                {"col", "hour(col)"},
        };
    }

    protected void dropTable(Session session, String table)
    {
        assertUpdate(session, "DROP TABLE " + table);
        assertFalse(getQueryRunner().tableExists(session, table));
    }
}
