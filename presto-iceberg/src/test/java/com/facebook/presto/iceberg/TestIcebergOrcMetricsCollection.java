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

import com.facebook.presto.Session;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.facebook.presto.tpch.TpchPlugin;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.nio.file.Path;
import java.util.Map;

import static com.facebook.presto.SystemSessionProperties.MAX_DRIVERS_PER_TASK;
import static com.facebook.presto.SystemSessionProperties.TASK_CONCURRENCY;
import static com.facebook.presto.SystemSessionProperties.TASK_WRITER_COUNT;
import static com.facebook.presto.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static com.facebook.presto.iceberg.TestIcebergOrcMetricsCollection.DataFileRecord.toDataFileRecord;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

public class TestIcebergOrcMetricsCollection
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Session session = testSessionBuilder()
                .setCatalog(ICEBERG_CATALOG)
                .setSchema("test_schema")
                .setSystemProperty(TASK_CONCURRENCY, "1")
                .setSystemProperty(TASK_WRITER_COUNT, "1")
                .setSystemProperty(MAX_DRIVERS_PER_TASK, "1")
                .setCatalogSessionProperty(ICEBERG_CATALOG, "orc_string_statistics_limit", Integer.MAX_VALUE + "B")
                .build();
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(session)
                .setNodeCount(1)
                .build();

        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog("tpch", "tpch");

        Path catalogDir = queryRunner.getCoordinator().getBaseDataDir().resolve("iceberg_data").resolve("catalog");

        queryRunner.installPlugin(new IcebergPlugin());
        Map<String, String> icebergProperties = ImmutableMap.<String, String>builder()
                .put("hive.metastore", "file")
                .put("hive.metastore.catalog.dir", catalogDir.toFile().toURI().toString())
                .build();

        queryRunner.createCatalog(ICEBERG_CATALOG, "iceberg", icebergProperties);

        queryRunner.execute("CREATE SCHEMA test_schema");

        return queryRunner;
    }

    @Test
    public void testBasic()
    {
        assertUpdate("CREATE TABLE orders WITH (format = 'ORC') AS SELECT * FROM tpch.tiny.orders", 15000);
        MaterializedResult materializedResult = computeActual("SELECT * FROM \"orders$files\"");
        assertEquals(materializedResult.getRowCount(), 1);
        DataFileRecord datafile = toDataFileRecord(materializedResult.getMaterializedRows().get(0));

        // Check file format
        assertEquals(datafile.getFileFormat(), "ORC");

        // Check file row count
        assertEquals(datafile.getRecordCount(), 15000L);

        // Check per-column value count
        datafile.getValueCounts().values().forEach(valueCount -> assertEquals(valueCount, (Long) 15000L));

        // Check per-column null value count
        datafile.getNullValueCounts().values().forEach(nullValueCount -> assertEquals(nullValueCount, (Long) 0L));

        // Check per-column lower bound
        Map<Integer, String> lowerBounds = datafile.getLowerBounds();
        assertQuery("SELECT min(orderkey) FROM tpch.tiny.orders", "VALUES " + lowerBounds.get(1));
        assertQuery("SELECT min(custkey) FROM tpch.tiny.orders", "VALUES " + lowerBounds.get(2));
        assertQuery("SELECT min(orderstatus) FROM tpch.tiny.orders", "VALUES '" + lowerBounds.get(3) + "'");
        assertQuery("SELECT min(totalprice) FROM tpch.tiny.orders", "VALUES " + lowerBounds.get(4));
        assertQuery("SELECT min(orderdate) FROM tpch.tiny.orders", "VALUES DATE '" + lowerBounds.get(5) + "'");
        assertQuery("SELECT min(orderpriority) FROM tpch.tiny.orders", "VALUES '" + lowerBounds.get(6) + "'");
        assertQuery("SELECT min(clerk) FROM tpch.tiny.orders", "VALUES '" + lowerBounds.get(7) + "'");
        assertQuery("SELECT min(shippriority) FROM tpch.tiny.orders", "VALUES " + lowerBounds.get(8));
        assertQuery("SELECT min(comment) FROM tpch.tiny.orders", "VALUES '" + lowerBounds.get(9) + "'");

        // Check per-column upper bound
        Map<Integer, String> upperBounds = datafile.getUpperBounds();
        assertQuery("SELECT max(orderkey) FROM tpch.tiny.orders", "VALUES " + upperBounds.get(1));
        assertQuery("SELECT max(custkey) FROM tpch.tiny.orders", "VALUES " + upperBounds.get(2));
        assertQuery("SELECT max(orderstatus) FROM tpch.tiny.orders", "VALUES '" + upperBounds.get(3) + "'");
        assertQuery("SELECT max(totalprice) FROM tpch.tiny.orders", "VALUES " + upperBounds.get(4));
        assertQuery("SELECT max(orderdate) FROM tpch.tiny.orders", "VALUES DATE '" + upperBounds.get(5) + "'");
        assertQuery("SELECT max(orderpriority) FROM tpch.tiny.orders", "VALUES '" + upperBounds.get(6) + "'");
        assertQuery("SELECT max(clerk) FROM tpch.tiny.orders", "VALUES '" + upperBounds.get(7) + "'");
        assertQuery("SELECT max(shippriority) FROM tpch.tiny.orders", "VALUES " + upperBounds.get(8));
        assertQuery("SELECT max(comment) FROM tpch.tiny.orders", "VALUES '" + upperBounds.get(9) + "'");

        assertUpdate("DROP TABLE orders");
    }

    @Test
    public void testWithNulls()
    {
        assertUpdate("CREATE TABLE test_with_nulls (_integer INTEGER, _real REAL, _string VARCHAR)  WITH (format = 'ORC')");
        assertUpdate("INSERT INTO test_with_nulls VALUES (7, 3.4, 'aaa'), (3, 4.5, 'bbb'), (4, null, 'ccc'), (null, null, 'ddd')", 4);
        MaterializedResult materializedResult = computeActual("SELECT * FROM \"test_with_nulls$files\"");
        assertEquals(materializedResult.getRowCount(), 1);
        DataFileRecord datafile = toDataFileRecord(materializedResult.getMaterializedRows().get(0));

        // Check per-column value count
        datafile.getValueCounts().values().forEach(valueCount -> assertEquals(valueCount, (Long) 4L));

        // Check per-column null value count
        assertEquals(datafile.getNullValueCounts().get(1), (Long) 1L);
        assertEquals(datafile.getNullValueCounts().get(2), (Long) 2L);
        assertEquals(datafile.getNullValueCounts().get(3), (Long) 0L);

        // Check per-column lower bound
        assertEquals(datafile.getLowerBounds().get(1), "3");
        assertEquals(datafile.getLowerBounds().get(2), "3.4");
        assertEquals(datafile.getLowerBounds().get(3), "aaa");

        assertUpdate("DROP TABLE test_with_nulls");

        assertUpdate("CREATE TABLE test_all_nulls (_integer INTEGER) WITH (format = 'ORC')");
        assertUpdate("INSERT INTO test_all_nulls VALUES null, null, null", 3);
        materializedResult = computeActual("SELECT * FROM \"test_all_nulls$files\"");
        assertEquals(materializedResult.getRowCount(), 1);
        datafile = toDataFileRecord(materializedResult.getMaterializedRows().get(0));

        // Check per-column value count
        assertEquals(datafile.getValueCounts().get(1), (Long) 3L);

        // Check per-column null value count
        assertEquals(datafile.getNullValueCounts().get(1), (Long) 3L);

        // Check that lower bounds and upper bounds are nulls. (There's no non-null record)
        assertNull(datafile.getLowerBounds());
        assertNull(datafile.getUpperBounds());

        assertUpdate("DROP TABLE test_all_nulls");
    }

    @Test
    public void testNestedTypes()
    {
        assertUpdate("CREATE TABLE test_nested_types (col1 INTEGER, col2 ROW (f1 INTEGER, f2 ARRAY(INTEGER), f3 DOUBLE)) WITH (format = 'ORC')");
        assertUpdate("INSERT INTO test_nested_types VALUES " +
                "(7, ROW(3, ARRAY[10, 11, 19], 1.9)), " +
                "(-9, ROW(4, ARRAY[13, 16, 20], -2.9)), " +
                "(8, ROW(0, ARRAY[14, 17, 21], 3.9)), " +
                "(3, ROW(10, ARRAY[15, 18, 22], 4.9))", 4);
        MaterializedResult materializedResult = computeActual("SELECT * FROM \"test_nested_types$files\"");
        assertEquals(materializedResult.getRowCount(), 1);
        DataFileRecord datafile = toDataFileRecord(materializedResult.getMaterializedRows().get(0));

        Map<Integer, String> lowerBounds = datafile.getLowerBounds();
        Map<Integer, String> upperBounds = datafile.getUpperBounds();

        // Only
        // 1. top-level primitive columns
        // 2. and nested primitive fields that are not descendants of LISTs or MAPs
        // should appear in lowerBounds or UpperBounds
        assertEquals(lowerBounds.size(), 3);
        assertEquals(upperBounds.size(), 3);

        // col1
        assertEquals(lowerBounds.get(1), "-9");
        assertEquals(upperBounds.get(1), "8");

        // col2.f1 (key in lowerBounds/upperBounds is Iceberg ID)
        assertEquals(lowerBounds.get(3), "0");
        assertEquals(upperBounds.get(3), "10");

        // col2.f3 (key in lowerBounds/upperBounds is Iceberg ID)
        assertEquals(lowerBounds.get(5), "-2.9");
        assertEquals(upperBounds.get(5), "4.9");

        assertUpdate("DROP TABLE test_nested_types");
    }

    public static class DataFileRecord
    {
        private final String filePath;
        private final String fileFormat;
        private final long recordCount;
        private final long fileSizeInBytes;
        private final Map<Integer, Long> columnSizes;
        private final Map<Integer, Long> valueCounts;
        private final Map<Integer, Long> nullValueCounts;
        private final Map<Integer, String> lowerBounds;
        private final Map<Integer, String> upperBounds;

        public static DataFileRecord toDataFileRecord(MaterializedRow row)
        {
            assertEquals(row.getFieldCount(), 11);
            return new DataFileRecord(
                    (String) row.getField(0),
                    (String) row.getField(1),
                    (long) row.getField(2),
                    (long) row.getField(3),
                    row.getField(4) != null ? ImmutableMap.copyOf((Map<Integer, Long>) row.getField(4)) : null,
                    row.getField(5) != null ? ImmutableMap.copyOf((Map<Integer, Long>) row.getField(5)) : null,
                    row.getField(6) != null ? ImmutableMap.copyOf((Map<Integer, Long>) row.getField(6)) : null,
                    row.getField(7) != null ? ImmutableMap.copyOf((Map<Integer, String>) row.getField(7)) : null,
                    row.getField(8) != null ? ImmutableMap.copyOf((Map<Integer, String>) row.getField(8)) : null);
        }

        private DataFileRecord(
                String filePath,
                String fileFormat,
                long recordCount,
                long fileSizeInBytes,
                Map<Integer, Long> columnSizes,
                Map<Integer, Long> valueCounts,
                Map<Integer, Long> nullValueCounts,
                Map<Integer, String> lowerBounds,
                Map<Integer, String> upperBounds)
        {
            this.filePath = filePath;
            this.fileFormat = fileFormat;
            this.recordCount = recordCount;
            this.fileSizeInBytes = fileSizeInBytes;
            this.columnSizes = columnSizes;
            this.valueCounts = valueCounts;
            this.nullValueCounts = nullValueCounts;
            this.lowerBounds = lowerBounds;
            this.upperBounds = upperBounds;
        }

        public String getFilePath()
        {
            return filePath;
        }

        public String getFileFormat()
        {
            return fileFormat;
        }

        public long getRecordCount()
        {
            return recordCount;
        }

        public long getFileSizeInBytes()
        {
            return fileSizeInBytes;
        }

        public Map<Integer, Long> getColumnSizes()
        {
            return columnSizes;
        }

        public Map<Integer, Long> getValueCounts()
        {
            return valueCounts;
        }

        public Map<Integer, Long> getNullValueCounts()
        {
            return nullValueCounts;
        }

        public Map<Integer, String> getLowerBounds()
        {
            return lowerBounds;
        }

        public Map<Integer, String> getUpperBounds()
        {
            return upperBounds;
        }
    }
}
