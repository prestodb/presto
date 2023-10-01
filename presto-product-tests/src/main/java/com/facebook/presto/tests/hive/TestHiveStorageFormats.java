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

import com.facebook.presto.tests.utils.JdbcDriverUtils;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import io.prestodb.tempto.ProductTest;
import io.prestodb.tempto.assertions.QueryAssert.Row;
import io.prestodb.tempto.query.QueryResult;
import org.apache.parquet.hadoop.ParquetWriter;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Ignore;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.facebook.presto.tests.TestGroups.STORAGE_FORMATS;
import static com.facebook.presto.tests.hive.util.TemporaryHiveTable.randomTableSuffix;
import static com.facebook.presto.tests.utils.JdbcDriverUtils.setSessionProperty;
import static com.facebook.presto.tests.utils.QueryExecutors.onHive;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Maps.immutableEntry;
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
                {storageFormat("ORC", ImmutableMap.of("hive.orc_optimized_writer_enabled", "false"))},
                {storageFormat("ORC", ImmutableMap.of("hive.orc_optimized_writer_enabled", "true", "hive.orc_optimized_writer_validate", "true"))},
                {storageFormat("DWRF")},
                {storageFormat("PARQUET")},
                {storageFormat("RCBINARY", ImmutableMap.of("hive.rcfile_optimized_writer_enabled", "false", "hive.rcfile_optimized_writer_validate", "false"))},
                {storageFormat("RCBINARY", ImmutableMap.of("hive.rcfile_optimized_writer_enabled", "true", "hive.rcfile_optimized_writer_validate", "true"))},
                {storageFormat("RCTEXT", ImmutableMap.of("hive.rcfile_optimized_writer_enabled", "false", "hive.rcfile_optimized_writer_validate", "false"))},
                {storageFormat("RCTEXT", ImmutableMap.of("hive.rcfile_optimized_writer_enabled", "true", "hive.rcfile_optimized_writer_validate", "true"))},
                {storageFormat("SEQUENCEFILE")},
                {storageFormat("TEXTFILE")},
                {storageFormat("AVRO")},
                {storageFormat("PAGEFILE")}
        };
    }

    @Test(dataProvider = "storage_formats", groups = {STORAGE_FORMATS})
    public void testInsertIntoTable(StorageFormat storageFormat)
    {
        // only admin user is allowed to change session properties
        setRole("admin");
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
        // only admin user is allowed to change session properties
        setRole("admin");
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
        // only admin user is allowed to change session properties
        setRole("admin");
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
        // only admin user is allowed to change session properties
        setRole("admin");
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

        onHive().executeQuery("DROP TABLE IF EXISTS " + tableName);

        onHive().executeQuery(format(
                "CREATE TABLE %s (" +
                        "   c_bigint BIGINT," +
                        "   c_varchar VARCHAR(255))" +
                        "STORED AS PARQUET " +
                        "TBLPROPERTIES(\"parquet.compression\"=\"SNAPPY\")",
                tableName));

        onHive().executeQuery(format("INSERT INTO %s VALUES(1, 'test data')", tableName));

        assertThat(query("SELECT * FROM " + tableName)).containsExactly(row(1, "test data"));

        onHive().executeQuery("DROP TABLE " + tableName);
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

    // These are regression tests for issue: https://github.com/trinodb/trino/issues/5518
    // The Parquet session properties are set to ensure that the correct situations in the Parquet writer are met to replicate the bug.
    // Not included in the STORAGE_FORMATS group since they require a large insert, which takes some time.
    @Test
    @Ignore
    public void testLargeParquetInsert()
    {
        DataSize reducedRowGroupSize = DataSize.succinctBytes(ParquetWriter.DEFAULT_PAGE_SIZE / 4);
        runLargeInsert(storageFormat(
                "PARQUET",
                ImmutableMap.of(
                        "hive.parquet_writer_page_size", reducedRowGroupSize.toString(),
                        "task_writer_count", "1")));
    }

    @Test
    @Ignore
    public void testLargeParquetInsertWithNativeWriter()
    {
        DataSize reducedRowGroupSize = DataSize.succinctBytes(ParquetWriter.DEFAULT_PAGE_SIZE / 4);
        runLargeInsert(storageFormat(
                "PARQUET",
                ImmutableMap.of(
                        "hive.experimental_parquet_optimized_writer_enabled", "true",
                        "hive.parquet_writer_page_size", reducedRowGroupSize.toString(),
                        "task_writer_count", "1")));
    }

    @Test
    @Ignore
    public void testLargeOrcInsert()
    {
        runLargeInsert(storageFormat("ORC", ImmutableMap.of("hive.orc_optimized_writer_validate", "true")));
    }

    private void runLargeInsert(StorageFormat storageFormat)
    {
        String tableName = "test_large_insert_" + storageFormat.getName() + randomTableSuffix();
        setSessionProperties(storageFormat);
        query("CREATE TABLE " + tableName + " WITH (" + storageFormat.getStoragePropertiesAsSql() + ") AS SELECT * FROM tpch.sf1.lineitem WHERE false");
        query("INSERT INTO " + tableName + " SELECT * FROM tpch.sf1.lineitem");

        assertThat(query("SELECT count(*) FROM " + tableName)).containsOnly(row(6001215L));
        onHive().executeQuery("DROP TABLE " + tableName);
    }

    private static void setRole(String role)
    {
        Connection connection = defaultQueryExecutor().getConnection();
        try {
            JdbcDriverUtils.setRole(connection, role);
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
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
        return new StorageFormat(name, sessionProperties, ImmutableMap.of());
    }

    private static class StorageFormat
    {
        private final String name;
        private final Map<String, String> properties;
        private final Map<String, String> sessionProperties;

        private StorageFormat(String name, Map<String, String> sessionProperties, Map<String, String> properties)
        {
            this.name = requireNonNull(name, "name is null");
            this.sessionProperties = requireNonNull(sessionProperties, "sessionProperties is null");
            this.properties = requireNonNull(properties, "properties is null");
        }

        public String getName()
        {
            return name;
        }

        public Map<String, String> getSessionProperties()
        {
            return sessionProperties;
        }

        public Map<String, String> getProperties()
        {
            return sessionProperties;
        }

        public String getStoragePropertiesAsSql()
        {
            return Stream.concat(
                            Stream.of(immutableEntry("format", name)),
                            properties.entrySet().stream())
                    .map(entry -> format("%s = '%s'", entry.getKey(), entry.getValue()))
                    .collect(Collectors.joining(", "));
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
