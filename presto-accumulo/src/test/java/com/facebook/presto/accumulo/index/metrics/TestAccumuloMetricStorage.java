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
package com.facebook.presto.accumulo.index.metrics;

import com.facebook.presto.accumulo.AccumuloQueryRunner;
import com.facebook.presto.accumulo.conf.AccumuloConfig;
import com.facebook.presto.accumulo.index.metrics.MetricsStorage.TimestampPrecision;
import com.facebook.presto.accumulo.metadata.AccumuloTable;
import com.facebook.presto.accumulo.model.AccumuloColumnHandle;
import com.facebook.presto.accumulo.serializers.LexicoderRowSerializer;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Bytes;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;

import static com.facebook.presto.accumulo.index.metrics.MetricsStorage.TimestampPrecision.DAY;
import static com.facebook.presto.accumulo.index.metrics.MetricsStorage.TimestampPrecision.HOUR;
import static com.facebook.presto.accumulo.index.metrics.MetricsStorage.TimestampPrecision.MINUTE;
import static com.facebook.presto.accumulo.index.metrics.MetricsStorage.TimestampPrecision.SECOND;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestAccumuloMetricStorage
        extends TestAbstractMetricStorage
{
    private static final Map<TimestampPrecision, byte[]> TIMESTAMP_CARDINALITY_FAMILIES = ImmutableMap.of(
            SECOND, "_tss".getBytes(UTF_8),
            MINUTE, "_tsm".getBytes(UTF_8),
            HOUR, "_tsh".getBytes(UTF_8),
            DAY, "_tsd".getBytes(UTF_8));

    private Connector connector;

    @Override
    public MetricsStorage getMetricsStorage(AccumuloConfig config)
    {
        return new AccumuloMetricsStorage(connector);
    }

    @BeforeClass
    @Override
    public void setupClass()
            throws Exception
    {
        config.setUsername("root");
        config.setPassword("secret");

        connector = AccumuloQueryRunner.getAccumuloConnector();
        connector.securityOperations().changeUserAuthorizations("root", new Authorizations("private", "moreprivate", "foo", "bar", "xyzzy"));

        super.setupClass();
    }

    @AfterMethod
    public void cleanup()
            throws Exception
    {
        for (String table : connector.tableOperations().list()) {
            if (table.contains(super.table.getFullTableName()) || table.contains(super.table2.getFullTableName())) {
                connector.tableOperations().delete(table);
            }
        }
    }

    @Test
    public void testCreateTable()
            throws Exception
    {
        AccumuloTable table = getTable("test_accumulo_metric_storage_create");
        storage.create(table);
        assertTrue(connector.tableOperations().exists(table.getIndexTableName() + "_metrics"));
    }

    @Test
    public void testCreateTableAlreadyExists()
            throws Exception
    {
        AccumuloTable table = getTable("test_accumulo_metric_storage_create_already_exists");
        connector.tableOperations().create(table.getIndexTableName() + "_metrics");
        storage.create(table);
        assertTrue(connector.tableOperations().exists(table.getIndexTableName() + "_metrics"));
    }

    @Test
    public void testDropTable()
            throws Exception
    {
        AccumuloTable table = getTable("test_accumulo_metric_storage_drop_table");
        storage.create(table);
        assertTrue(connector.tableOperations().exists(table.getIndexTableName() + "_metrics"));
        storage.drop(table);
        assertFalse(connector.tableOperations().exists(table.getIndexTableName() + "_metrics"));
    }

    @Test
    public void testDropTableDoesNotExist()
            throws Exception
    {
        AccumuloTable table = getTable("test_accumulo_metric_storage_drop_table_does_not_exist");
        storage.drop(table);
        assertFalse(connector.tableOperations().exists(table.getIndexTableName() + "_metrics"));
    }

    @Override
    public void testDropExternalTable()
            throws Exception
    {
        AccumuloTable externalTable = getTable("test_accumulo_metric_storage_drop_external_table", true);
        storage.create(externalTable);
        assertTrue(connector.tableOperations().exists(externalTable.getIndexTableName() + "_metrics"));
        storage.drop(externalTable);
        assertTrue(connector.tableOperations().exists(externalTable.getIndexTableName() + "_metrics"));
    }

    @Test
    public void testRenameTable()
            throws Exception
    {
        AccumuloTable table = getTable("test_accumulo_metric_storage_rename_table");
        AccumuloTable table2 = getTable("test_accumulo_metric_storage_rename_table2");
        storage.create(table);
        storage.rename(table, table2);
        assertFalse(connector.tableOperations().exists(table.getIndexTableName() + "_metrics"));
        assertTrue(connector.tableOperations().exists(table2.getIndexTableName() + "_metrics"));
    }

    @Test(expectedExceptions = PrestoException.class)
    public void testRenameTableDoesNotExist()
            throws Exception
    {
        AccumuloTable table = getTable("test_accumulo_metric_storage_rename_table_does_not_exist");
        AccumuloTable table2 = getTable("test_accumulo_metric_storage_rename_table_does_not_exist2");
        storage.rename(table, table2);
    }

    @Test(expectedExceptions = PrestoException.class)
    public void testRenameTableNewTableExists()
            throws Exception
    {
        AccumuloTable table = getTable("test_accumulo_metric_storage_rename_new_table_exists");
        AccumuloTable table2 = getTable("test_accumulo_metric_storage_rename_new_table_exists2");
        storage.create(table);
        connector.tableOperations().create(table2.getIndexTableName() + "_metrics");
        storage.rename(table, table2);
    }

    @Test
    public void testExists()
            throws Exception
    {
        AccumuloTable table = getTable("test_accumulo_metric_storage_exists");
        assertFalse(storage.exists(table.getSchemaTableName()));
        storage.create(table);
        assertTrue(storage.exists(table.getSchemaTableName()));
    }

    @Override
    public void testTimestampInserts()
            throws Exception
    {
        AccumuloTable table = new AccumuloTable(
                "default",
                "test_accumulo_timestamp_inserts",
                ImmutableList.of(c1, new AccumuloColumnHandle("time", Optional.of("cf"), Optional.of("time"), TIMESTAMP_TYPE.get(), 1, "", true)),
                "id",
                false,
                LexicoderRowSerializer.class.getCanonicalName(),
                null,
                Optional.of(storage.getClass().getCanonicalName()),
                true);

        storage.create(table);

        assertTrue(storage.exists(table.getSchemaTableName()));

        MetricsWriter writer = storage.newWriter(table);

        writer.incrementCardinality(bb(TIMESTAMP), bb("cf_time"), new ColumnVisibility(), true);
        writer.flush();

        assertEquals(getAccumuloHashValue(table, SECOND, "cf_time", SECOND_TIMESTAMP), 1L);
        assertEquals(getAccumuloHashValue(table, MINUTE, "cf_time", MINUTE_TIMESTAMP), 1L);
        assertEquals(getAccumuloHashValue(table, HOUR, "cf_time", HOUR_TIMESTAMP), 1L);
        assertEquals(getAccumuloHashValue(table, DAY, "cf_time", DAY_TIMESTAMP), 1L);

        writer.incrementCardinality(bb(TIMESTAMP), bb("cf_time"), new ColumnVisibility(), true);
        writer.flush();

        assertEquals(getAccumuloHashValue(table, SECOND, "cf_time", SECOND_TIMESTAMP), 2L);
        assertEquals(getAccumuloHashValue(table, MINUTE, "cf_time", MINUTE_TIMESTAMP), 2L);
        assertEquals(getAccumuloHashValue(table, HOUR, "cf_time", HOUR_TIMESTAMP), 2L);
        assertEquals(getAccumuloHashValue(table, DAY, "cf_time", DAY_TIMESTAMP), 2L);

        writer.incrementCardinality(bb(TIMESTAMP), bb("cf_time"), new ColumnVisibility(), true);
        writer.flush();

        assertEquals(getAccumuloHashValue(table, SECOND, "cf_time", SECOND_TIMESTAMP), 3L);
        assertEquals(getAccumuloHashValue(table, MINUTE, "cf_time", MINUTE_TIMESTAMP), 3L);
        assertEquals(getAccumuloHashValue(table, HOUR, "cf_time", HOUR_TIMESTAMP), 3L);
        assertEquals(getAccumuloHashValue(table, DAY, "cf_time", DAY_TIMESTAMP), 3L);

        writer.incrementCardinality(bb(TIMESTAMP), bb("cf_time"), new ColumnVisibility(), true);
        writer.close();

        assertEquals(getAccumuloHashValue(table, SECOND, "cf_time", SECOND_TIMESTAMP), 4L);
        assertEquals(getAccumuloHashValue(table, MINUTE, "cf_time", MINUTE_TIMESTAMP), 4L);
        assertEquals(getAccumuloHashValue(table, HOUR, "cf_time", HOUR_TIMESTAMP), 4L);
        assertEquals(getAccumuloHashValue(table, DAY, "cf_time", DAY_TIMESTAMP), 4L);
    }

    private long getAccumuloHashValue(AccumuloTable table, TimestampPrecision level, String family, Long value)
            throws Exception
    {
        Scanner scanner = null;
        try {
            scanner = connector.createScanner(table.getIndexTableName() + "_metrics", new Authorizations());
            scanner.setRange(new Range(new Text(serializer.encode(TIMESTAMP_TYPE.get(), value))));
            scanner.fetchColumn(new Text(Bytes.concat(family.getBytes(UTF_8), TIMESTAMP_CARDINALITY_FAMILIES.get(level))), new Text("___card___"));

            long sum = 0;
            for (Entry<Key, Value> entry : scanner) {
                sum += Long.parseLong(entry.getValue().toString());
            }
            return sum;
        }
        finally {
            if (scanner != null) {
                scanner.close();
            }
        }
    }

    private AccumuloTable getTable(String tablename)
    {
        return getTable(tablename, false);
    }

    private AccumuloTable getTable(String tablename, boolean external)
    {
        return new AccumuloTable(table.getSchema(), tablename, table.getColumns(), table.getRowId(), external, table.getSerializerClassName(), table.getScanAuthorizations(), table.getMetricsStorageClass(), table.isTruncateTimestamps());
    }
}
