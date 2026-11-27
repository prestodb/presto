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
package com.facebook.presto.iceberg;

import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.hadoop.HadoopOutputFile;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.testng.annotations.Test;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.facebook.presto.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static java.lang.String.format;
import static org.apache.iceberg.TableProperties.DEFAULT_WRITE_METRICS_MODE;
import static org.apache.iceberg.TableProperties.METRICS_MODE_COLUMN_CONF_PREFIX;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

/**
 * Abstract base class for E2E tests that verify ANALYZE succeeds on Iceberg tables
 * whose schemas were written by an external engine (e.g. Spark) for both unpartitioned and identity-partitioned tables.
 */
public abstract class AbstractTestIcebergAnalyzeUppercaseColumns
        extends AbstractTestQueryFramework
{
    protected static final String TEST_SCHEMA = "tpch";
    protected static final int WIDE_TABLE_EXTRA_COLUMNS = 101;

    /**
     * Rows written into the partitioned table: {id, value, region}.
     * Three partitions with deliberately mixed casing on the partition value:
     * all-uppercase, all-lowercase, mixed — one row each.
     */
    protected static final Object[][] PARTITIONED_TABLE_ROWS = {
        {1, "alpha", "US-EAST"},
        {2, "beta", "eu-west"},
        {3, "gamma", "Ap-South"},
    };

    protected abstract String getTableName();

    protected abstract String getPartitionedTableName();

    protected abstract File getHadoopWarehouseLocation();

    @Test
    public void testAnalyzeSucceeds()
    {
        assertQuerySucceeds(format("ANALYZE %s.%s", TEST_SCHEMA, getTableName()));
    }

    @Test(dependsOnMethods = "testAnalyzeSucceeds")
    public void testShowStatsAfterAnalyze()
    {
        assertQuerySucceeds(format("ANALYZE %s.%s", TEST_SCHEMA, getTableName()));

        MaterializedResult stats = getQueryRunner().execute(format("SHOW STATS FOR %s.%s", TEST_SCHEMA, getTableName()));
        List<MaterializedRow> rows = stats.getMaterializedRows();

        // Presto lowercases column names; find the 'id' row.
        MaterializedRow idRow = rows.stream()
                .filter(row -> "id".equalsIgnoreCase((String) row.getField(0)))
                .findFirst()
                .orElse(null);

        assertNotNull(idRow, "SHOW STATS FOR must return a row for the 'id' column after ANALYZE");

        // distinct_values_count is field index 2; it must be non-null after ANALYZE.
        assertNotNull(idRow.getField(2), "distinct_values_count for 'id' must be populated after ANALYZE " +
                        "on a table whose schema has uppercase column names");
    }

    @Test
    public void testSelectFromTable()
    {
        assertQuery(format("SELECT count(*) FROM %s.%s", TEST_SCHEMA, getTableName()), "VALUES (3)");
    }

    @Test(dependsOnMethods = "testAnalyzeSucceeds")
    public void testQueryWithFilterAfterAnalyzeOnUppercaseColumnsTable()
    {
        assertQuery(format("SELECT id FROM %s.%s WHERE id > 1 ORDER BY id", TEST_SCHEMA, getTableName()), "VALUES (2), (3)");
    }

    /**
     * Load an Iceberg HadoopCatalog pointing at the "external" warehouse directory.
     * This simulates an external engine (Spark, Flink, …) that can write tables with
     * uppercase column names.
     */
    protected Catalog loadHadoopCatalog()
    {
        return CatalogUtil.loadCatalog(
                HadoopCatalog.class.getName(),
                "hadoop_test",
                ImmutableMap.of("warehouse", getHadoopWarehouseLocation().toURI().toString()),
                new Configuration());
    }

    /**
     * Write a single record to the table as a new Parquet data file and commit it as
     * a new snapshot — the same write path used by Spark internally.
     * Supports both unpartitioned and identity-partitioned tables.
     */
    protected static void writeRecord(Table table, Record record)
            throws Exception
    {
        String filename = "data-" + UUID.randomUUID() + ".parquet";
        org.apache.hadoop.fs.Path filePath = new org.apache.hadoop.fs.Path(
                table.location(), "data/" + filename);

        DataWriter<Record> writer = Parquet
                .writeData(HadoopOutputFile.fromPath(filePath, new Configuration()))
                .forTable(table)
                .createWriterFunc(GenericParquetWriter::create)
                .withPartition(partitionKeyFor(table, record))
                .overwrite()
                .build();
        try {
            writer.write(record);
        }
        finally {
            writer.close();
        }

        table.newAppend()
                .appendFile(writer.toDataFile())
                .commit();
    }

    /**
     * Write a single record to a partitioned table using an explicit pre-built
     * {@code partitionRecord} ({@link GenericRecord} over the partition type).
     * This variant is used when the caller needs to control the exact partition
     * struct — for example, to exercise mixed-case partition values written by
     * an external engine.
     */
    protected static void writePartitionedRecord(Table table, PartitionSpec spec, GenericRecord partitionRecord, Record record)
            throws Exception
    {
        String filename = "data-" + UUID.randomUUID() + ".parquet";
        org.apache.hadoop.fs.Path filePath = new org.apache.hadoop.fs.Path(
                table.location(), "data/" + filename);

        DataWriter<Record> writer = Parquet
                .writeData(HadoopOutputFile.fromPath(filePath, new Configuration()))
                .forTable(table)
                .createWriterFunc(GenericParquetWriter::create)
                .withPartition(partitionRecord)
                .overwrite()
                .build();
        try {
            writer.write(record);
        }
        finally {
            writer.close();
        }

        table.newAppend()
                .appendFile(writer.toDataFile())
                .commit();
    }

    /**
     * Compute the {@link StructLike} partition key for a record.
     * For unpartitioned tables this returns {@code null}, which {@code DataWriter} accepts.
     * For partitioned tables the key is built by extracting the identity-partition field
     * values from the record in the order they appear in the partition spec.
     */
    protected static StructLike partitionKeyFor(Table table, Record record)
    {
        if (table.spec().isUnpartitioned()) {
            return null;
        }
        PartitionKey partitionKey = new PartitionKey(table.spec(), table.schema());
        partitionKey.partition(record);
        return partitionKey;
    }

    protected static Schema createUnpartitionedSchema()
    {
        return new Schema(
                org.apache.iceberg.types.Types.NestedField.required(1, "ID", org.apache.iceberg.types.Types.IntegerType.get()),
                org.apache.iceberg.types.Types.NestedField.optional(2, "VALUE", org.apache.iceberg.types.Types.StringType.get()));
    }

    protected static Schema createPartitionedSchema()
    {
        return new Schema(
                org.apache.iceberg.types.Types.NestedField.required(1, "ID", org.apache.iceberg.types.Types.IntegerType.get()),
                org.apache.iceberg.types.Types.NestedField.optional(2, "VALUE", org.apache.iceberg.types.Types.StringType.get()),
                org.apache.iceberg.types.Types.NestedField.required(3, "REGION", org.apache.iceberg.types.Types.StringType.get()));
    }

    protected void registerTable(String tableName, String tableLocation)
    {
        assertQuerySucceeds(format("CALL %s.system.register_table('%s', '%s', '%s')", ICEBERG_CATALOG, TEST_SCHEMA, tableName, tableLocation));
    }

    protected void unregisterTableQuietly(String tableName)
    {
        try {
            assertQuerySucceeds(format("CALL %s.system.unregister_table('%s', '%s')", ICEBERG_CATALOG, TEST_SCHEMA, tableName));
        }
        catch (Exception ignored) {
        }
    }

    /**
     * Write three rows to an unpartitioned table with uppercase ID/VALUE columns and
     * register it in the Presto catalog.
     */
    protected void setupUnpartitionedTable(Catalog hadoopCatalog, String tableName)
            throws Exception
    {
        setupUnpartitionedTable(hadoopCatalog, tableName, ImmutableMap.of());
    }

    protected void setupUnpartitionedTable(Catalog hadoopCatalog, String tableName, Map<String, String> tableProperties)
            throws Exception
    {
        Schema schema = createUnpartitionedSchema();
        Table table = hadoopCatalog.createTable(
                org.apache.iceberg.catalog.TableIdentifier.of(TEST_SCHEMA, tableName),
                schema,
                PartitionSpec.unpartitioned(),
                tableProperties);

        writeRecord(table, GenericRecord.create(schema).copy("ID", 1, "VALUE", "one"));
        table.refresh();
        writeRecord(table, GenericRecord.create(schema).copy("ID", 2, "VALUE", "two"));
        table.refresh();
        writeRecord(table, GenericRecord.create(schema).copy("ID", 3, "VALUE", "three"));

        registerTable(tableName, table.location());
    }

    /**
     * Creates a table with uppercase columns {@code ID}, {@code VALUE} and {@code REGION},
     * identity-partitioned on {@code REGION}, writes one row into each of three partitions
     * with deliberately mixed-case values ({@link #PARTITIONED_TABLE_ROWS}), then registers
     * the table in the Presto catalog.
     */
    protected void setupPartitionedTable(Catalog hadoopCatalog, String tableName)
            throws Exception
    {
        Schema schema = createPartitionedSchema();
        PartitionSpec spec = PartitionSpec.builderFor(schema).identity("REGION").build();
        Table table = hadoopCatalog.createTable(
                org.apache.iceberg.catalog.TableIdentifier.of(TEST_SCHEMA, tableName),
                schema,
                spec);

        for (Object[] row : PARTITIONED_TABLE_ROWS) {
            Record record = GenericRecord.create(schema).copy("ID", row[0], "VALUE", row[1], "REGION", row[2]);
            GenericRecord partitionRecord = GenericRecord.create(spec.partitionType());
            partitionRecord.setField("REGION", row[2]);
            writePartitionedRecord(table, spec, partitionRecord, record);
            table.refresh();
        }

        registerTable(tableName, table.location());
    }

    protected String getWideTableName()
    {
        return getTableName() + "_wide";
    }

    protected String getValueMetricsNoneTableName()
    {
        return getTableName() + "_value_none";
    }

    protected String getValueMetricsFullTableName()
    {
        return getTableName() + "_value_full";
    }

    /**
     * Creates the tables whose ANALYZE depends on looking up Iceberg metrics modes by the exact
     * (uppercase) column name: one with more than 100 columns, and two with per-column
     * metrics properties on {@code VALUE}.
     */
    protected void setupMetricsModeTables(Catalog hadoopCatalog)
            throws Exception
    {
        setupWideTable(hadoopCatalog, getWideTableName());
        setupUnpartitionedTable(hadoopCatalog, getValueMetricsNoneTableName(), ImmutableMap.of(
                METRICS_MODE_COLUMN_CONF_PREFIX + "VALUE", "none"));
        setupUnpartitionedTable(hadoopCatalog, getValueMetricsFullTableName(), ImmutableMap.of(
                DEFAULT_WRITE_METRICS_MODE, "none",
                METRICS_MODE_COLUMN_CONF_PREFIX + "VALUE", "full"));
    }

    protected void unregisterMetricsModeTables()
    {
        unregisterTableQuietly(getWideTableName());
        unregisterTableQuietly(getValueMetricsNoneTableName());
        unregisterTableQuietly(getValueMetricsFullTableName());
    }

    /**
     * {@code ID} followed by {@code C001} ... {@code C101}. With more than 100 columns Iceberg's
     * default metrics mode becomes {@code none} and only the first 100 columns get
     * {@code truncate(16)}, keyed by their exact (uppercase) names.
     */
    protected static Schema createWideSchema()
    {
        ImmutableList.Builder<Types.NestedField> fields = ImmutableList.builder();
        fields.add(Types.NestedField.required(1, "ID", Types.IntegerType.get()));
        for (int i = 1; i <= WIDE_TABLE_EXTRA_COLUMNS; i++) {
            fields.add(Types.NestedField.optional(i + 1, format("C%03d", i), Types.IntegerType.get()));
        }
        return new Schema(fields.build());
    }

    protected void setupWideTable(Catalog hadoopCatalog, String tableName)
            throws Exception
    {
        Schema schema = createWideSchema();
        Table table = hadoopCatalog.createTable(
                org.apache.iceberg.catalog.TableIdentifier.of(TEST_SCHEMA, tableName),
                schema,
                PartitionSpec.unpartitioned());

        for (int row = 1; row <= 3; row++) {
            GenericRecord record = GenericRecord.create(schema);
            for (Types.NestedField field : schema.columns()) {
                record.setField(field.name(), row);
            }
            writeRecord(table, record);
            table.refresh();
        }

        registerTable(tableName, table.location());
    }

    @Test
    public void testShowStatsAfterAnalyzeOnWideTable()
    {
        assertQuerySucceeds(format("ANALYZE %s.%s", TEST_SCHEMA, getWideTableName()));

        MaterializedResult stats = getQueryRunner().execute(format("SHOW STATS FOR %s.%s", TEST_SCHEMA, getWideTableName()));

        assertNotNull(distinctValuesCount(stats, "id"), "distinct_values_count for 'id' must be populated after ANALYZE " +
                "on a table with more than 100 uppercase columns");
        assertNotNull(distinctValuesCount(stats, "c099"), "distinct_values_count for 'c099' (100th column) must be populated after ANALYZE");
        assertNull(distinctValuesCount(stats, "c100"), "'c100' is the 101st column, so its metrics mode is none and ANALYZE must skip it");
        assertNull(distinctValuesCount(stats, "c101"), "'c101' is beyond the first 100 columns, so its metrics mode is none and ANALYZE must skip it");
    }

    @Test
    public void testAnalyzeSkipsUppercaseColumnWithMetricsModeNone()
    {
        assertQuerySucceeds(format("ANALYZE %s.%s", TEST_SCHEMA, getValueMetricsNoneTableName()));

        MaterializedResult stats = getQueryRunner().execute(format("SHOW STATS FOR %s.%s", TEST_SCHEMA, getValueMetricsNoneTableName()));

        assertNotNull(distinctValuesCount(stats, "id"), "'id' has the default metrics mode, so ANALYZE must collect its distinct_values_count");
        assertNull(distinctValuesCount(stats, "value"), "'VALUE' has metrics mode none, so ANALYZE must skip it");
    }

    @Test
    public void testAnalyzeCollectsUppercaseColumnWithMetricsModeFull()
    {
        assertQuerySucceeds(format("ANALYZE %s.%s", TEST_SCHEMA, getValueMetricsFullTableName()));

        MaterializedResult stats = getQueryRunner().execute(format("SHOW STATS FOR %s.%s", TEST_SCHEMA, getValueMetricsFullTableName()));

        assertNull(distinctValuesCount(stats, "id"), "the table's default metrics mode is none, so ANALYZE must skip 'id'");
        assertNotNull(distinctValuesCount(stats, "value"), "'VALUE' has metrics mode full, so ANALYZE must collect its distinct_values_count");
    }

    private static Object distinctValuesCount(MaterializedResult stats, String columnName)
    {
        return stats.getMaterializedRows().stream()
                .filter(row -> columnName.equals(row.getField(0)))
                .findFirst()
                .orElseThrow(() -> new AssertionError("SHOW STATS returned no row for column " + columnName))
                .getField(2);
    }

    @Test
    public void testAnalyzeSucceedsOnPartitionedTable()
    {
        assertQuerySucceeds(format("ANALYZE %s.%s", TEST_SCHEMA, getPartitionedTableName()));
    }

    @Test(dependsOnMethods = "testAnalyzeSucceedsOnPartitionedTable")
    public void testShowStatsAfterAnalyzeOnPartitionedTable()
    {
        assertQuerySucceeds(format("ANALYZE %s.%s", TEST_SCHEMA, getPartitionedTableName()));

        MaterializedResult stats = getQueryRunner().execute(format("SHOW STATS FOR %s.%s", TEST_SCHEMA, getPartitionedTableName()));
        List<MaterializedRow> rows = stats.getMaterializedRows();

        MaterializedRow idRow = rows.stream()
                .filter(row -> "id".equalsIgnoreCase((String) row.getField(0)))
                .findFirst()
                .orElse(null);

        assertNotNull(idRow, "SHOW STATS FOR must return a row for the 'id' column after ANALYZE on partitioned table");
        assertNotNull(idRow.getField(2), "distinct_values_count for 'id' must be populated after ANALYZE " +
                        "on a partitioned catalog table with uppercase column names");
    }

    @Test
    public void testSelectFromPartitionedTable()
    {
        assertQuery(format("SELECT count(*) FROM %s.%s", TEST_SCHEMA, getPartitionedTableName()), "VALUES (3)");
        assertQuery(format("SELECT id FROM %s.%s WHERE region = 'US-EAST'", TEST_SCHEMA, getPartitionedTableName()), "VALUES (1)");
        assertQuery(format("SELECT id FROM %s.%s WHERE region = 'eu-west'", TEST_SCHEMA, getPartitionedTableName()), "VALUES (2)");
        assertQuery(format("SELECT id FROM %s.%s WHERE region = 'Ap-South'", TEST_SCHEMA, getPartitionedTableName()), "VALUES (3)");
    }

    @Test(dependsOnMethods = "testAnalyzeSucceedsOnPartitionedTable")
    public void testQueryWithFilterAfterAnalyzeOnPartitioned()
    {
        assertQuery(format("SELECT id FROM %s.%s WHERE region IN ('US-EAST', 'Ap-South') ORDER BY id", TEST_SCHEMA, getPartitionedTableName()), "VALUES (1), (3)");
    }
}
