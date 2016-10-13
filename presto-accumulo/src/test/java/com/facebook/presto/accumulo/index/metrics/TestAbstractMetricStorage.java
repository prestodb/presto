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

import com.facebook.presto.accumulo.conf.AccumuloConfig;
import com.facebook.presto.accumulo.metadata.AccumuloTable;
import com.facebook.presto.accumulo.model.AccumuloColumnHandle;
import com.facebook.presto.accumulo.serializers.LexicoderRowSerializer;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.type.TimestampType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.type.ArrayType;
import com.google.common.collect.ImmutableList;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static java.nio.ByteBuffer.wrap;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public abstract class TestAbstractMetricStorage
{
    protected static final Optional<Type> TIMESTAMP_TYPE = Optional.of(TimestampType.TIMESTAMP);
    protected static final DateTimeFormatter PARSER = ISODateTimeFormat.dateTimeParser();
    protected static final Long START_TIMESTAMP = PARSER.parseDateTime("2001-08-22T00:00:00.000+0000").getMillis();
    protected static final Long END_TIMESTAMP = PARSER.parseDateTime("2001-08-23T00:00:00.000+0000").getMillis();
    protected static final Long TIMESTAMP = PARSER.parseDateTime("2001-08-22T03:04:05.321+0000").getMillis();
    protected static final Long SECOND_TIMESTAMP = PARSER.parseDateTime("2001-08-22T03:04:05.000+0000").getMillis();
    protected static final Long MINUTE_TIMESTAMP = PARSER.parseDateTime("2001-08-22T03:04:00.000+0000").getMillis();
    protected static final Long HOUR_TIMESTAMP = PARSER.parseDateTime("2001-08-22T03:00:00.000+0000").getMillis();
    protected static final Long DAY_TIMESTAMP = PARSER.parseDateTime("2001-08-22T00:00:00.000+0000").getMillis();

    protected AccumuloConfig config = new AccumuloConfig();
    protected AccumuloTable table;
    protected AccumuloTable table2;

    protected AccumuloColumnHandle c1 = new AccumuloColumnHandle("id", Optional.empty(), Optional.empty(), VARCHAR, 0, "", false);
    protected AccumuloColumnHandle c2 = new AccumuloColumnHandle("age", Optional.of("cf"), Optional.of("age"), BIGINT, 1, "", true);
    protected AccumuloColumnHandle c3 = new AccumuloColumnHandle("firstname", Optional.of("cf"), Optional.of("firstname"), VARCHAR, 2, "", true);
    protected AccumuloColumnHandle c4 = new AccumuloColumnHandle("arr", Optional.of("cf"), Optional.of("arr"), new ArrayType(VARCHAR), 3, "", true);

    protected LexicoderRowSerializer serializer = new LexicoderRowSerializer();
    protected MetricsStorage storage;

    public abstract MetricsStorage getMetricsStorage(AccumuloConfig config);

    @Test
    public abstract void testCreateTable()
            throws Exception;

    @Test
    public abstract void testCreateTableAlreadyExists()
            throws Exception;

    @Test
    public abstract void testDropTable()
            throws Exception;

    @Test
    public abstract void testDropTableDoesNotExist()
            throws Exception;

    @Test
    public abstract void testDropExternalTable()
            throws Exception;

    @Test
    public abstract void testRenameTable()
            throws Exception;

    @Test(expectedExceptions = PrestoException.class)
    public abstract void testRenameTableDoesNotExist()
            throws Exception;

    @Test(expectedExceptions = PrestoException.class)
    public abstract void testRenameTableNewTableExists()
            throws Exception;

    @Test
    public abstract void testExists()
            throws Exception;

    @Test
    public abstract void testTimestampInserts()
            throws Exception;

    @BeforeClass
    public void setupClass()
            throws Exception
    {
        storage = getMetricsStorage(config);
        table = new AccumuloTable("default", "index_test_table", ImmutableList.of(c1, c2, c3, c4), "id", false, LexicoderRowSerializer.class.getCanonicalName(), null, Optional.of(storage.getClass().getCanonicalName()), false);
        table2 = new AccumuloTable("default", "index_test_table_two", ImmutableList.of(c1, c2, c3, c4), "id", false, LexicoderRowSerializer.class.getCanonicalName(), null, Optional.of(storage.getClass().getCanonicalName()), false);
    }

    @Test
    public void testIncrementRows()
            throws Exception
    {
        storage.create(table);

        MetricsWriter writer = storage.newWriter(table);
        assertEquals(storage.newReader().getNumRowsInTable(table.getSchema(), table.getTable()), 0);
        writer.incrementRowCount();
        writer.flush();
        assertEquals(storage.newReader().getNumRowsInTable(table.getSchema(), table.getTable()), 1);
        writer.incrementRowCount();
        writer.flush();
        assertEquals(storage.newReader().getNumRowsInTable(table.getSchema(), table.getTable()), 2);
        writer.incrementRowCount();
        writer.flush();
        assertEquals(storage.newReader().getNumRowsInTable(table.getSchema(), table.getTable()), 3);
    }

    @Test
    public void testIncrementRowsWithVisibility()
            throws Exception
    {
        storage.create(table);

        MetricsWriter writer = storage.newWriter(table);
        assertEquals(storage.newReader().getNumRowsInTable(table.getSchema(), table.getTable()), 0);
        writer.incrementRowCount();
        writer.flush();
        assertEquals(storage.newReader().getNumRowsInTable(table.getSchema(), table.getTable()), 1);
        writer.incrementRowCount();
        writer.flush();
        assertEquals(storage.newReader().getNumRowsInTable(table.getSchema(), table.getTable()), 2);
        writer.incrementRowCount();
        writer.flush();
        assertEquals(storage.newReader().getNumRowsInTable(table.getSchema(), table.getTable()), 3);

        writer.incrementRowCount();
        writer.flush();
        assertEquals(storage.newReader().getNumRowsInTable(table.getSchema(), table.getTable()), 4);

        writer.incrementRowCount();
        writer.flush();
        assertEquals(storage.newReader().getNumRowsInTable(table.getSchema(), table.getTable()), 5);
    }

    @Test
    public void testGetCardinality()
            throws Exception
    {
        storage.create(table);

        MetricsWriter writer = storage.newWriter(table);

        assertEquals(storage.newReader().getCardinality(mck("abc", "cf", "firstname", false, new Authorizations())), 0);
        assertEquals(storage.newReader().getCardinality(mck("def", "cf", "firstname", false, new Authorizations())), 0);
        assertEquals(storage.newReader().getCardinality(mck("ghi", "cf", "firstname", false, new Authorizations())), 0);
        assertEquals(storage.newReader().getCardinality(mck("1", "cf", "age", false, new Authorizations())), 0);
        assertEquals(storage.newReader().getCardinality(mck("2", "cf", "age", false, new Authorizations())), 0);
        assertEquals(storage.newReader().getCardinality(mck("3", "cf", "age", false, new Authorizations())), 0);

        writer.incrementCardinality(bb("abc"), bb("cf_firstname"), new ColumnVisibility(), false);
        writer.incrementCardinality(bb("def"), bb("cf_firstname"), new ColumnVisibility(), false);
        writer.incrementCardinality(bb("ghi"), bb("cf_firstname"), new ColumnVisibility(), false);
        writer.incrementCardinality(bb("1"), bb("cf_age"), new ColumnVisibility(), false);
        writer.incrementCardinality(bb("2"), bb("cf_age"), new ColumnVisibility(), false);
        writer.incrementCardinality(bb("3"), bb("cf_age"), new ColumnVisibility(), false);
        writer.flush();

        assertEquals(storage.newReader().getCardinality(mck("abc", "cf", "firstname", false, new Authorizations())), 1);
        assertEquals(storage.newReader().getCardinality(mck("def", "cf", "firstname", false, new Authorizations())), 1);
        assertEquals(storage.newReader().getCardinality(mck("ghi", "cf", "firstname", false, new Authorizations())), 1);
        assertEquals(storage.newReader().getCardinality(mck("1", "cf", "age", false, new Authorizations())), 1);
        assertEquals(storage.newReader().getCardinality(mck("2", "cf", "age", false, new Authorizations())), 1);
        assertEquals(storage.newReader().getCardinality(mck("3", "cf", "age", false, new Authorizations())), 1);

        writer.incrementCardinality(bb("abc"), bb("cf_firstname"), new ColumnVisibility(), false);
        writer.incrementCardinality(bb("def"), bb("cf_firstname"), new ColumnVisibility(), false);
        writer.incrementCardinality(bb("ghi"), bb("cf_firstname"), new ColumnVisibility(), false);
        writer.incrementCardinality(bb("1"), bb("cf_age"), new ColumnVisibility(), false);
        writer.incrementCardinality(bb("2"), bb("cf_age"), new ColumnVisibility(), false);
        writer.incrementCardinality(bb("3"), bb("cf_age"), new ColumnVisibility(), false);
        writer.flush();

        assertEquals(storage.newReader().getCardinality(mck("abc", "cf", "firstname", false, new Authorizations())), 2);
        assertEquals(storage.newReader().getCardinality(mck("def", "cf", "firstname", false, new Authorizations())), 2);
        assertEquals(storage.newReader().getCardinality(mck("ghi", "cf", "firstname", false, new Authorizations())), 2);
        assertEquals(storage.newReader().getCardinality(mck("1", "cf", "age", false, new Authorizations())), 2);
        assertEquals(storage.newReader().getCardinality(mck("2", "cf", "age", false, new Authorizations())), 2);
        assertEquals(storage.newReader().getCardinality(mck("3", "cf", "age", false, new Authorizations())), 2);

        writer.incrementCardinality(bb("abc"), bb("cf_firstname"), new ColumnVisibility("foo"), false);
        writer.incrementCardinality(bb("def"), bb("cf_firstname"), new ColumnVisibility("foo"), false);
        writer.incrementCardinality(bb("ghi"), bb("cf_firstname"), new ColumnVisibility("foo"), false);
        writer.incrementCardinality(bb("1"), bb("cf_age"), new ColumnVisibility("foo"), false);
        writer.incrementCardinality(bb("2"), bb("cf_age"), new ColumnVisibility("foo"), false);
        writer.incrementCardinality(bb("3"), bb("cf_age"), new ColumnVisibility("foo"), false);
        writer.flush();

        assertEquals(storage.newReader().getCardinality(mck("abc", "cf", "firstname", false, new Authorizations())), 2);
        assertEquals(storage.newReader().getCardinality(mck("def", "cf", "firstname", false, new Authorizations())), 2);
        assertEquals(storage.newReader().getCardinality(mck("ghi", "cf", "firstname", false, new Authorizations())), 2);
        assertEquals(storage.newReader().getCardinality(mck("1", "cf", "age", false, new Authorizations())), 2);
        assertEquals(storage.newReader().getCardinality(mck("2", "cf", "age", false, new Authorizations())), 2);
        assertEquals(storage.newReader().getCardinality(mck("3", "cf", "age", false, new Authorizations())), 2);

        assertEquals(storage.newReader().getCardinality(mck("abc", "cf", "firstname", false, new Authorizations("foo"))), 3);
        assertEquals(storage.newReader().getCardinality(mck("def", "cf", "firstname", false, new Authorizations("foo"))), 3);
        assertEquals(storage.newReader().getCardinality(mck("ghi", "cf", "firstname", false, new Authorizations("foo"))), 3);
        assertEquals(storage.newReader().getCardinality(mck("1", "cf", "age", false, new Authorizations("foo"))), 3);
        assertEquals(storage.newReader().getCardinality(mck("2", "cf", "age", false, new Authorizations("foo"))), 3);
        assertEquals(storage.newReader().getCardinality(mck("3", "cf", "age", false, new Authorizations("foo"))), 3);

        writer.incrementCardinality(bb("abc"), bb("cf_firstname"), new ColumnVisibility("bar"), false);
        writer.incrementCardinality(bb("def"), bb("cf_firstname"), new ColumnVisibility("bar"), false);
        writer.incrementCardinality(bb("ghi"), bb("cf_firstname"), new ColumnVisibility("bar"), false);
        writer.incrementCardinality(bb("1"), bb("cf_age"), new ColumnVisibility("bar"), false);
        writer.incrementCardinality(bb("2"), bb("cf_age"), new ColumnVisibility("bar"), false);
        writer.incrementCardinality(bb("3"), bb("cf_age"), new ColumnVisibility("bar"), false);
        writer.flush();

        assertEquals(storage.newReader().getCardinality(mck("abc", "cf", "firstname", false, new Authorizations())), 2);
        assertEquals(storage.newReader().getCardinality(mck("def", "cf", "firstname", false, new Authorizations())), 2);
        assertEquals(storage.newReader().getCardinality(mck("ghi", "cf", "firstname", false, new Authorizations())), 2);
        assertEquals(storage.newReader().getCardinality(mck("1", "cf", "age", false, new Authorizations())), 2);
        assertEquals(storage.newReader().getCardinality(mck("2", "cf", "age", false, new Authorizations())), 2);
        assertEquals(storage.newReader().getCardinality(mck("3", "cf", "age", false, new Authorizations())), 2);

        assertEquals(storage.newReader().getCardinality(mck("abc", "cf", "firstname", false, new Authorizations("foo"))), 3);
        assertEquals(storage.newReader().getCardinality(mck("def", "cf", "firstname", false, new Authorizations("foo"))), 3);
        assertEquals(storage.newReader().getCardinality(mck("ghi", "cf", "firstname", false, new Authorizations("foo"))), 3);
        assertEquals(storage.newReader().getCardinality(mck("1", "cf", "age", false, new Authorizations("foo"))), 3);
        assertEquals(storage.newReader().getCardinality(mck("2", "cf", "age", false, new Authorizations("foo"))), 3);
        assertEquals(storage.newReader().getCardinality(mck("3", "cf", "age", false, new Authorizations("foo"))), 3);

        assertEquals(storage.newReader().getCardinality(mck("abc", "cf", "firstname", false, new Authorizations("foo", "bar"))), 4);
        assertEquals(storage.newReader().getCardinality(mck("def", "cf", "firstname", false, new Authorizations("foo", "bar"))), 4);
        assertEquals(storage.newReader().getCardinality(mck("ghi", "cf", "firstname", false, new Authorizations("foo", "bar"))), 4);
        assertEquals(storage.newReader().getCardinality(mck("1", "cf", "age", false, new Authorizations("foo", "bar"))), 4);
        assertEquals(storage.newReader().getCardinality(mck("2", "cf", "age", false, new Authorizations("foo", "bar"))), 4);
        assertEquals(storage.newReader().getCardinality(mck("3", "cf", "age", false, new Authorizations("foo", "bar"))), 4);
    }

    @Test
    public void testGetCardinalityWithRange()
            throws Exception
    {
        storage.create(table);

        MetricsWriter writer = storage.newWriter(table);

        assertEquals(storage.newReader().getCardinality(mck("a", "z", "cf", "firstname", false, new Authorizations())), 0);
        assertEquals(storage.newReader().getCardinality(mck("0", "9", "cf", "age", false, new Authorizations())), 0);

        writer.incrementCardinality(bb("abc"), bb("cf_firstname"), new ColumnVisibility(), false);
        writer.incrementCardinality(bb("def"), bb("cf_firstname"), new ColumnVisibility(), false);
        writer.incrementCardinality(bb("ghi"), bb("cf_firstname"), new ColumnVisibility(), false);
        writer.incrementCardinality(bb("1"), bb("cf_age"), new ColumnVisibility(), false);
        writer.incrementCardinality(bb("2"), bb("cf_age"), new ColumnVisibility(), false);
        writer.incrementCardinality(bb("3"), bb("cf_age"), new ColumnVisibility(), false);
        writer.flush();

        assertEquals(storage.newReader().getCardinality(mck("a", "z", "cf", "firstname", false, new Authorizations())), 3);
        assertEquals(storage.newReader().getCardinality(mck("0", "9", "cf", "age", false, new Authorizations())), 3);

        writer.incrementCardinality(bb("abc"), bb("cf_firstname"), new ColumnVisibility(), false);
        writer.incrementCardinality(bb("def"), bb("cf_firstname"), new ColumnVisibility(), false);
        writer.incrementCardinality(bb("ghi"), bb("cf_firstname"), new ColumnVisibility(), false);
        writer.incrementCardinality(bb("1"), bb("cf_age"), new ColumnVisibility(), false);
        writer.incrementCardinality(bb("2"), bb("cf_age"), new ColumnVisibility(), false);
        writer.incrementCardinality(bb("3"), bb("cf_age"), new ColumnVisibility(), false);
        writer.flush();

        assertEquals(storage.newReader().getCardinality(mck("a", "z", "cf", "firstname", false, new Authorizations())), 6);
        assertEquals(storage.newReader().getCardinality(mck("0", "9", "cf", "age", false, new Authorizations())), 6);

        writer.incrementCardinality(bb("abc"), bb("cf_firstname"), new ColumnVisibility("foo"), false);
        writer.incrementCardinality(bb("def"), bb("cf_firstname"), new ColumnVisibility("foo"), false);
        writer.incrementCardinality(bb("ghi"), bb("cf_firstname"), new ColumnVisibility("foo"), false);
        writer.incrementCardinality(bb("1"), bb("cf_age"), new ColumnVisibility("foo"), false);
        writer.incrementCardinality(bb("2"), bb("cf_age"), new ColumnVisibility("foo"), false);
        writer.incrementCardinality(bb("3"), bb("cf_age"), new ColumnVisibility("foo"), false);
        writer.flush();

        assertEquals(storage.newReader().getCardinality(mck("a", "z", "cf", "firstname", false, new Authorizations())), 6);
        assertEquals(storage.newReader().getCardinality(mck("0", "9", "cf", "age", false, new Authorizations())), 6);

        assertEquals(storage.newReader().getCardinality(mck("a", "z", "cf", "firstname", false, new Authorizations("foo"))), 9);
        assertEquals(storage.newReader().getCardinality(mck("0", "9", "cf", "age", false, new Authorizations("foo"))), 9);

        writer.incrementCardinality(bb("abc"), bb("cf_firstname"), new ColumnVisibility("bar"), false);
        writer.incrementCardinality(bb("def"), bb("cf_firstname"), new ColumnVisibility("bar"), false);
        writer.incrementCardinality(bb("ghi"), bb("cf_firstname"), new ColumnVisibility("bar"), false);
        writer.incrementCardinality(bb("1"), bb("cf_age"), new ColumnVisibility("bar"), false);
        writer.incrementCardinality(bb("2"), bb("cf_age"), new ColumnVisibility("bar"), false);
        writer.incrementCardinality(bb("3"), bb("cf_age"), new ColumnVisibility("bar"), false);
        writer.flush();

        assertEquals(storage.newReader().getCardinality(mck("a", "z", "cf", "firstname", false, new Authorizations())), 6);
        assertEquals(storage.newReader().getCardinality(mck("0", "9", "cf", "age", false, new Authorizations())), 6);

        assertEquals(storage.newReader().getCardinality(mck("a", "z", "cf", "firstname", false, new Authorizations("foo"))), 9);
        assertEquals(storage.newReader().getCardinality(mck("0", "9", "cf", "age", false, new Authorizations("foo"))), 9);

        assertEquals(storage.newReader().getCardinality(mck("a", "z", "cf", "firstname", false, new Authorizations("foo", "bar"))), 12);
        assertEquals(storage.newReader().getCardinality(mck("0", "9", "cf", "age", false, new Authorizations("foo", "bar"))), 12);
    }

    @Test
    public void testGetCardinalities()
            throws Exception
    {
        storage.create(table);

        MetricsWriter writer = storage.newWriter(table);

        List<MetricCacheKey> keys = ImmutableList.of(
                mck("abc", "cf", "firstname", false, new Authorizations()),
                mck("def", "cf", "firstname", false, new Authorizations()),
                mck("ghi", "cf", "firstname", false, new Authorizations())
        );

        List<MetricCacheKey> fooKeys = ImmutableList.of(
                mck("abc", "cf", "firstname", false, new Authorizations("foo")),
                mck("def", "cf", "firstname", false, new Authorizations("foo")),
                mck("ghi", "cf", "firstname", false, new Authorizations("foo"))
        );

        List<MetricCacheKey> barKeys = ImmutableList.of(
                mck("abc", "cf", "firstname", false, new Authorizations("bar")),
                mck("def", "cf", "firstname", false, new Authorizations("bar")),
                mck("ghi", "cf", "firstname", false, new Authorizations("bar"))
        );

        List<MetricCacheKey> fooBarKeys = ImmutableList.of(
                mck("abc", "cf", "firstname", false, new Authorizations("foo", "bar")),
                mck("def", "cf", "firstname", false, new Authorizations("foo", "bar")),
                mck("ghi", "cf", "firstname", false, new Authorizations("foo", "bar"))
        );

        Map<MetricCacheKey, Long> cardinalities = storage.newReader().getCardinalities(keys);
        for (MetricCacheKey key : keys) {
            assertNotNull(cardinalities.get(key));
            assertEquals(cardinalities.get(key).longValue(), 0);
        }

        writer.incrementCardinality(bb("abc"), bb("cf_firstname"), new ColumnVisibility(), false);
        writer.incrementCardinality(bb("def"), bb("cf_firstname"), new ColumnVisibility(), false);
        writer.incrementCardinality(bb("ghi"), bb("cf_firstname"), new ColumnVisibility(), false);
        writer.flush();

        cardinalities = storage.newReader().getCardinalities(keys);
        for (MetricCacheKey key : keys) {
            assertNotNull(cardinalities.get(key));
            assertEquals(cardinalities.get(key).longValue(), 1);
        }

        writer.incrementCardinality(bb("abc"), bb("cf_firstname"), new ColumnVisibility(), false);
        writer.incrementCardinality(bb("def"), bb("cf_firstname"), new ColumnVisibility(), false);
        writer.incrementCardinality(bb("ghi"), bb("cf_firstname"), new ColumnVisibility(), false);
        writer.flush();

        cardinalities = storage.newReader().getCardinalities(keys);
        for (MetricCacheKey key : keys) {
            assertNotNull(cardinalities.get(key));
            assertEquals(cardinalities.get(key).longValue(), 2);
        }

        writer.incrementCardinality(bb("abc"), bb("cf_firstname"), new ColumnVisibility("foo"), false);
        writer.incrementCardinality(bb("def"), bb("cf_firstname"), new ColumnVisibility("foo"), false);
        writer.incrementCardinality(bb("ghi"), bb("cf_firstname"), new ColumnVisibility("foo"), false);
        writer.flush();

        cardinalities = storage.newReader().getCardinalities(keys);
        for (MetricCacheKey key : keys) {
            assertNotNull(cardinalities.get(key));
            assertEquals(cardinalities.get(key).longValue(), 2);
        }

        cardinalities = storage.newReader().getCardinalities(fooKeys);
        for (MetricCacheKey key : fooKeys) {
            assertNotNull(cardinalities.get(key));
            assertEquals(cardinalities.get(key).longValue(), 3);
        }

        writer.incrementCardinality(bb("abc"), bb("cf_firstname"), new ColumnVisibility("bar"), false);
        writer.incrementCardinality(bb("def"), bb("cf_firstname"), new ColumnVisibility("bar"), false);
        writer.incrementCardinality(bb("ghi"), bb("cf_firstname"), new ColumnVisibility("bar"), false);
        writer.flush();

        cardinalities = storage.newReader().getCardinalities(keys);
        for (MetricCacheKey key : keys) {
            assertNotNull(cardinalities.get(key));
            assertEquals(cardinalities.get(key).longValue(), 2);
        }

        cardinalities = storage.newReader().getCardinalities(fooKeys);
        for (MetricCacheKey key : fooKeys) {
            assertNotNull(cardinalities.get(key));
            assertEquals(cardinalities.get(key).longValue(), 3);
        }

        cardinalities = storage.newReader().getCardinalities(barKeys);
        for (MetricCacheKey key : barKeys) {
            assertNotNull(cardinalities.get(key));
            assertEquals(cardinalities.get(key).longValue(), 3);
        }

        cardinalities = storage.newReader().getCardinalities(fooBarKeys);
        for (MetricCacheKey key : fooBarKeys) {
            assertNotNull(cardinalities.get(key));
            assertEquals(cardinalities.get(key).longValue(), 4);
        }
    }

    @Test
    public void testTimestampReads()
            throws Exception
    {
        AccumuloTable table = new AccumuloTable(
                "default",
                "test_timestamp_reads",
                ImmutableList.of(c1, new AccumuloColumnHandle("time", Optional.of("cf"), Optional.of("time"), TIMESTAMP_TYPE.get(), 1, "", true)),
                "id",
                false,
                LexicoderRowSerializer.class.getCanonicalName(),
                null,
                Optional.of(storage.getClass().getCanonicalName()),
                true);

        MetricCacheKey metricCacheKey =
                new MetricCacheKey(
                        storage,
                        "default",
                        "test_timestamp_reads",
                        "cf",
                        "time",
                        true,
                        new Authorizations(),
                        new Range(
                                new Text(serializer.encode(TIMESTAMP_TYPE.get(), START_TIMESTAMP)),
                                true,
                                new Text(serializer.encode(TIMESTAMP_TYPE.get(), END_TIMESTAMP)), false));

        storage.create(table);

        assertTrue(storage.exists(table.getSchemaTableName()));

        MetricsWriter writer = storage.newWriter(table);

        writer.incrementCardinality(bb(TIMESTAMP), bb("cf_time"), new ColumnVisibility(), true);
        writer.flush();

        assertEquals(storage.newReader().getCardinality(metricCacheKey), 1L);

        writer.incrementCardinality(bb(TIMESTAMP), bb("cf_time"), new ColumnVisibility(), true);
        writer.flush();

        assertEquals(storage.newReader().getCardinality(metricCacheKey), 2L);

        writer.incrementCardinality(bb(TIMESTAMP), bb("cf_time"), new ColumnVisibility(), true);
        writer.flush();

        assertEquals(storage.newReader().getCardinality(metricCacheKey), 3L);

        writer.incrementCardinality(bb(TIMESTAMP), bb("cf_time"), new ColumnVisibility(), true);
        writer.close();

        assertEquals(storage.newReader().getCardinality(metricCacheKey), 4L);
    }

    protected static ByteBuffer bb(String v)
    {
        return wrap(b(v));
    }

    protected static byte[] b(String v)
    {
        return v.getBytes(UTF_8);
    }

    protected ByteBuffer bb(Long v)
    {
        return wrap(serializer.encode(TIMESTAMP_TYPE.get(), v));
    }

    protected MetricCacheKey mck(String value, String family, String qualifier, boolean truncateTimestamp, Authorizations auths)
    {
        return new MetricCacheKey(storage, table.getSchema(), table.getTable(), family, qualifier, truncateTimestamp, auths, new Range(new Text(value.getBytes(UTF_8))));
    }

    protected MetricCacheKey mck(String begin, String end, String family, String qualifier, boolean truncateTimestamp, Authorizations auths)
    {
        return new MetricCacheKey(storage, table.getSchema(), table.getTable(), family, qualifier, truncateTimestamp, auths, new Range(new Text(begin.getBytes(UTF_8)), new Text(end.getBytes(UTF_8))));
    }
}
