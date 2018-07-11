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
package com.facebook.presto.tests.hive;

import com.google.common.collect.ImmutableMap;
import io.prestodb.tempto.ProductTest;
import io.prestodb.tempto.assertions.QueryAssert.Row;
import io.prestodb.tempto.query.QueryResult;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static com.facebook.presto.tests.TestGroups.STORAGE_FORMATS;
import static com.facebook.presto.tests.utils.JdbcDriverUtils.setSessionProperty;
import static com.facebook.presto.tests.utils.QueryExecutors.onHive;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestodb.tempto.assertions.QueryAssert.Row.row;
import static io.prestodb.tempto.assertions.QueryAssert.assertThat;
import static io.prestodb.tempto.query.QueryExecutor.defaultQueryExecutor;
import static io.prestodb.tempto.query.QueryExecutor.query;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class TestHiveStorageFormats
        extends ProductTest
{
    private static final String TPCH_SCHEMA = "tiny";

    @DataProvider(name = "storage_formats")
    public static Object[][] storageFormats()
    {
        return new StorageFormat[][] {
                {storageFormat("ORC")},
                {storageFormat("ORC", ImmutableMap.of("hive.orc_optimized_writer_enabled", "true", "hive.orc_optimized_writer_validate", "true"))},
                {storageFormat("DWRF")},
                {storageFormat("PARQUET")},
                {storageFormat("PARQUET", ImmutableMap.of("hive.parquet_optimized_reader_enabled", "true"))},
                {storageFormat("RCBINARY", ImmutableMap.of("hive.rcfile_optimized_writer_enabled", "false", "hive.rcfile_optimized_writer_validate", "false"))},
                {storageFormat("RCBINARY", ImmutableMap.of("hive.rcfile_optimized_writer_enabled", "true", "hive.rcfile_optimized_writer_validate", "true"))},
                {storageFormat("RCTEXT", ImmutableMap.of("hive.rcfile_optimized_writer_enabled", "false", "hive.rcfile_optimized_writer_validate", "false"))},
                {storageFormat("RCTEXT", ImmutableMap.of("hive.rcfile_optimized_writer_enabled", "true", "hive.rcfile_optimized_writer_validate", "true"))},
                {storageFormat("SEQUENCEFILE")},
                {storageFormat("TEXTFILE")},
                {storageFormat("AVRO")}
        };
    }

    @Test(dataProvider = "storage_formats", groups = {STORAGE_FORMATS})
    public void testInsertIntoTable(StorageFormat storageFormat)
    {
        setSessionProperties(storageFormat);

        String tableName = "storage_formats_test_insert_into_" + storageFormat.getName().toLowerCase(Locale.ENGLISH);

        query(format("DROP TABLE IF EXISTS %s", tableName));

        String createTable = format(
                "CREATE TABLE %s(" +
                        "   orderkey      BIGINT," +
                        "   partkey       BIGINT," +
                        "   suppkey       BIGINT," +
                        "   linenumber    INTEGER," +
                        "   quantity      DOUBLE," +
                        "   extendedprice DOUBLE," +
                        "   discount      DOUBLE," +
                        "   tax           DOUBLE," +
                        "   linestatus    VARCHAR," +
                        "   shipinstruct  VARCHAR," +
                        "   shipmode      VARCHAR," +
                        "   comment       VARCHAR," +
                        "   returnflag    VARCHAR" +
                        ") WITH (format='%s')",
                tableName,
                storageFormat.getName());
        query(createTable);

        String insertInto = format("INSERT INTO %s " +
                "SELECT " +
                "orderkey, partkey, suppkey, linenumber, quantity, extendedprice, discount, tax, " +
                "linestatus, shipinstruct, shipmode, comment, returnflag " +
                "FROM tpch.%s.lineitem", tableName, TPCH_SCHEMA);
        query(insertInto);

        assertSelect("select sum(tax), sum(discount), sum(linenumber) from %s", tableName);

        query(format("DROP TABLE %s", tableName));
    }

    @Test(dataProvider = "storage_formats", groups = {STORAGE_FORMATS})
    public void testCreateTableAs(StorageFormat storageFormat)
    {
        setSessionProperties(storageFormat);

        String tableName = "storage_formats_test_create_table_as_select_" + storageFormat.getName().toLowerCase(Locale.ENGLISH);

        query(format("DROP TABLE IF EXISTS %s", tableName));

        String createTableAsSelect = format(
                "CREATE TABLE %s WITH (format='%s') AS " +
                        "SELECT " +
                        "partkey, suppkey, extendedprice " +
                        "FROM tpch.%s.lineitem",
                tableName,
                storageFormat.getName(),
                TPCH_SCHEMA);
        query(createTableAsSelect);

        assertSelect("select sum(extendedprice), sum(suppkey), count(partkey) from %s", tableName);

        query(format("DROP TABLE %s", tableName));
    }

    @Test(dataProvider = "storage_formats", groups = {STORAGE_FORMATS})
    public void testInsertIntoPartitionedTable(StorageFormat storageFormat)
    {
        setSessionProperties(storageFormat);

        String tableName = "storage_formats_test_insert_into_partitioned_" + storageFormat.getName().toLowerCase(Locale.ENGLISH);

        query(format("DROP TABLE IF EXISTS %s", tableName));

        String createTable = format(
                "CREATE TABLE %s(" +
                        "   orderkey      BIGINT," +
                        "   partkey       BIGINT," +
                        "   suppkey       BIGINT," +
                        "   linenumber    INTEGER," +
                        "   quantity      DOUBLE," +
                        "   extendedprice DOUBLE," +
                        "   discount      DOUBLE," +
                        "   tax           DOUBLE," +
                        "   linestatus    VARCHAR," +
                        "   shipinstruct  VARCHAR," +
                        "   shipmode      VARCHAR," +
                        "   comment       VARCHAR," +
                        "   returnflag    VARCHAR" +
                        ") WITH (format='%s', partitioned_by = ARRAY['returnflag'])",
                tableName,
                storageFormat.getName());
        query(createTable);

        String insertInto = format("INSERT INTO %s " +
                "SELECT " +
                "orderkey, partkey, suppkey, linenumber, quantity, extendedprice, discount, tax, " +
                "linestatus, shipinstruct, shipmode, comment, returnflag " +
                "FROM tpch.%s.lineitem", tableName, TPCH_SCHEMA);
        query(insertInto);

        assertSelect("select sum(tax), sum(discount), sum(length(returnflag)) from %s", tableName);

        query(format("DROP TABLE %s", tableName));
    }

    @Test(dataProvider = "storage_formats", groups = {STORAGE_FORMATS})
    public void testCreatePartitionedTableAs(StorageFormat storageFormat)
    {
        setSessionProperties(storageFormat);

        String tableName = "storage_formats_test_create_table_as_select_partitioned_" + storageFormat.getName().toLowerCase(Locale.ENGLISH);

        query(format("DROP TABLE IF EXISTS %s", tableName));

        String createTableAsSelect = format(
                "CREATE TABLE %s WITH (format='%s', partitioned_by = ARRAY['returnflag']) AS " +
                        "SELECT " +
                        "tax, discount, returnflag " +
                        "FROM tpch.%s.lineitem",
                tableName,
                storageFormat.getName(),
                TPCH_SCHEMA);
        query(createTableAsSelect);

        assertSelect("select sum(tax), sum(discount), sum(length(returnflag)) from %s", tableName);

        query(format("DROP TABLE %s", tableName));
    }

    @Test
    public void testSnappyCompressedParquetTableCreatedInHive()
    {
        String tableName = "table_created_in_hive_parquet";

        query("DROP TABLE IF EXISTS " + tableName);

        onHive().executeQuery(format(
                "CREATE TABLE %s (" +
                        "   c_bigint BIGINT," +
                        "   c_varchar VARCHAR(255))" +
                        "STORED AS PARQUET " +
                        "TBLPROPERTIES(\"parquet.compression\"=\"SNAPPY\")",
                tableName));

        onHive().executeQuery(format("INSERT INTO %s VALUES(1, 'test data')", tableName));

        assertThat(query("SELECT * FROM " + tableName)).containsExactly(row(1, "test data"));

        query("DROP TABLE " + tableName);
    }

    private static void assertSelect(String query, String tableName)
    {
        QueryResult expected = query(format(query, "tpch." + TPCH_SCHEMA + ".lineitem"));
        List<Row> expectedRows = expected.rows().stream()
                .map((columns) -> row(columns.toArray()))
                .collect(toImmutableList());
        QueryResult actual = query(format(query, tableName));
        assertThat(actual)
                .hasColumns(expected.getColumnTypes())
                .containsExactly(expectedRows);
    }

    private static void setSessionProperties(StorageFormat storageFormat)
    {
        setSessionProperties(storageFormat.getSessionProperties());
    }

    private static void setSessionProperties(Map<String, String> sessionProperties)
    {
        Connection connection = defaultQueryExecutor().getConnection();
        try {
            // create more than one split
            setSessionProperty(connection, "task_writer_count", "4");
            setSessionProperty(connection, "redistribute_writes", "false");
            for (Map.Entry<String, String> sessionProperty : sessionProperties.entrySet()) {
                setSessionProperty(connection, sessionProperty.getKey(), sessionProperty.getValue());
            }
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static StorageFormat storageFormat(String name)
    {
        return storageFormat(name, ImmutableMap.of());
    }

    private static StorageFormat storageFormat(String name, Map<String, String> sessionProperties)
    {
        return new StorageFormat(name, sessionProperties);
    }

    private static class StorageFormat
    {
        private final String name;
        private final Map<String, String> sessionProperties;

        private StorageFormat(String name, Map<String, String> sessionProperties)
        {
            this.name = requireNonNull(name, "name is null");
            this.sessionProperties = requireNonNull(sessionProperties, "sessionProperties is null");
        }

        public String getName()
        {
            return name;
        }

        public Map<String, String> getSessionProperties()
        {
            return sessionProperties;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("name", name)
                    .add("sessionProperties", sessionProperties)
                    .toString();
        }
    }
}
