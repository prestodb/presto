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
package com.facebook.presto.raptor.systemtables;

import com.facebook.presto.raptor.RaptorMetadata;
import com.facebook.presto.raptor.metadata.ColumnInfo;
import com.facebook.presto.raptor.metadata.ColumnStats;
import com.facebook.presto.raptor.metadata.MetadataDao;
import com.facebook.presto.raptor.metadata.ShardInfo;
import com.facebook.presto.raptor.metadata.ShardManager;
import com.facebook.presto.raptor.metadata.TableColumn;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.predicate.ValueSet;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.metadata.MetadataUtil.TableMetadataBuilder.tableMetadataBuilder;
import static com.facebook.presto.raptor.metadata.SchemaDaoUtil.createTablesWithRetry;
import static com.facebook.presto.raptor.metadata.TestDatabaseShardManager.createShardManager;
import static com.facebook.presto.raptor.systemtables.ShardMetadataRecordCursor.SHARD_METADATA;
import static com.facebook.presto.spi.predicate.Range.greaterThan;
import static com.facebook.presto.spi.predicate.Range.lessThanOrEqual;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.VarcharType.createVarcharType;
import static com.facebook.presto.testing.MaterializedResult.DEFAULT_PRECISION;
import static com.facebook.presto.testing.TestingConnectorSession.SESSION;
import static io.airlift.slice.Slices.utf8Slice;
import static java.util.stream.Collectors.toList;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestShardMetadataRecordCursor
{
    private static final SchemaTableName DEFAULT_TEST_ORDERS = new SchemaTableName("test", "orders");

    private Handle dummyHandle;
    private ConnectorMetadata metadata;
    private DBI dbi;

    @BeforeMethod
    public void setup()
    {
        this.dbi = new DBI("jdbc:h2:mem:test" + System.nanoTime());
        dbi.registerMapper(new TableColumn.Mapper(new TypeRegistry()));

        this.dummyHandle = dbi.open();
        createTablesWithRetry(dbi);
        this.metadata = new RaptorMetadata("raptor", dbi, createShardManager(dbi));

        // Create table
        metadata.createTable(SESSION, tableMetadataBuilder(DEFAULT_TEST_ORDERS)
                .column("orderkey", BIGINT)
                .column("orderdate", DATE)
                .property("temporal_column", "orderdate")
                .build());
    }

    @AfterMethod(alwaysRun = true)
    public void teardown()
    {
        dummyHandle.close();
    }

    @Test
    public void testSimple()
            throws Exception
    {
        ShardManager shardManager = createShardManager(dbi);

        // Add shards to the table
        long tableId = 1;
        OptionalInt bucketNumber = OptionalInt.empty();
        UUID uuid1 = UUID.randomUUID();
        UUID uuid2 = UUID.randomUUID();
        UUID uuid3 = UUID.randomUUID();
        ShardInfo shardInfo1 = new ShardInfo(uuid1, bucketNumber, ImmutableSet.of("node1"), ImmutableList.of(), 1, 10, 100, 0x1234);
        ShardInfo shardInfo2 = new ShardInfo(uuid2, bucketNumber, ImmutableSet.of("node2"), ImmutableList.of(), 2, 20, 200, 0xCAFEBABEDEADBEEFL);
        ShardInfo shardInfo3 = new ShardInfo(uuid3, bucketNumber, ImmutableSet.of("node3"), ImmutableList.of(), 3, 30, 300, 0xFEDCBA0987654321L);
        List<ShardInfo> shards = ImmutableList.of(shardInfo1, shardInfo2, shardInfo3);

        long transactionId = shardManager.beginTransaction();

        shardManager.commitShards(
                transactionId,
                tableId,
                ImmutableList.of(
                        new ColumnInfo(1, BIGINT),
                        new ColumnInfo(2, DATE)),
                shards,
                Optional.empty(),
                0);

        Slice schema = utf8Slice(DEFAULT_TEST_ORDERS.getSchemaName());
        Slice table = utf8Slice(DEFAULT_TEST_ORDERS.getTableName());

        DateTime date1 = DateTime.parse("2015-01-01T00:00");
        DateTime date2 = DateTime.parse("2015-01-02T00:00");
        TupleDomain<Integer> tupleDomain = TupleDomain.withColumnDomains(
                ImmutableMap.<Integer, Domain>builder()
                        .put(0, Domain.singleValue(createVarcharType(10), schema))
                        .put(1, Domain.create(ValueSet.ofRanges(lessThanOrEqual(createVarcharType(10), table)), true))
                        .put(8, Domain.create(ValueSet.ofRanges(lessThanOrEqual(BIGINT, date1.getMillis()), greaterThan(BIGINT, date2.getMillis())), true))
                        .put(9, Domain.create(ValueSet.ofRanges(lessThanOrEqual(BIGINT, date1.getMillis()), greaterThan(BIGINT, date2.getMillis())), true))
                        .build());

        List<MaterializedRow> actual;
        try (RecordCursor cursor = new ShardMetadataSystemTable(dbi).cursor(null, SESSION, tupleDomain)) {
            actual = getMaterializedResults(cursor, SHARD_METADATA.getColumns());
        }
        assertEquals(actual.size(), 3);

        List<MaterializedRow> expected = ImmutableList.of(
                new MaterializedRow(DEFAULT_PRECISION, schema, table, utf8Slice(uuid1.toString()), null, 100L, 10L, 1L, utf8Slice("0000000000001234"), null, null),
                new MaterializedRow(DEFAULT_PRECISION, schema, table, utf8Slice(uuid2.toString()), null, 200L, 20L, 2L, utf8Slice("cafebabedeadbeef"), null, null),
                new MaterializedRow(DEFAULT_PRECISION, schema, table, utf8Slice(uuid3.toString()), null, 300L, 30L, 3L, utf8Slice("fedcba0987654321"), null, null));

        assertEquals(actual, expected);
    }

    @Test
    public void testDateTemporalColumnNoFilter()
            throws Exception
    {
        ShardManager shardManager = createShardManager(dbi);

        DateTime date1 = new DateTime(2017, 1, 1, 0, 0, 0, DateTimeZone.UTC);
        DateTime date2 = new DateTime(2017, 1, 2, 0, 0, 0, DateTimeZone.UTC);

        // These values are what the cursor should display
        long milli1 = date1.getMillis();
        long milli2 = date2.getMillis();

        // These values are stored in the index shard table (e.g. x_shards_table1)
        int days1 = (int) (milli1 / TimeUnit.DAYS.toMillis(1));
        int days2 = (int) (milli2 / TimeUnit.DAYS.toMillis(1));

        // Add shards to the table
        long tableId = 1;
        OptionalInt bucketNumber = OptionalInt.empty();
        UUID uuid1 = UUID.randomUUID();
        UUID uuid2 = UUID.randomUUID();
        UUID uuid3 = UUID.randomUUID();

        ColumnStats columnStats1 = new ColumnStats(2, days1, days1);
        ColumnStats columnStats2 = new ColumnStats(2, days1, days2);
        ColumnStats columnStats3 = new ColumnStats(2, days2, days2);

        ShardInfo shardInfo1 = new ShardInfo(uuid1, bucketNumber, ImmutableSet.of("node1"), ImmutableList.of(columnStats1), 1, 10, 100, 0x1234);
        ShardInfo shardInfo2 = new ShardInfo(uuid2, bucketNumber, ImmutableSet.of("node2"), ImmutableList.of(columnStats2), 2, 20, 200, 0xCAFEBABEDEADBEEFL);
        ShardInfo shardInfo3 = new ShardInfo(uuid3, bucketNumber, ImmutableSet.of("node3"), ImmutableList.of(columnStats3), 3, 30, 300, 0xFEDCBA0987654321L);
        List<ShardInfo> shards = ImmutableList.of(shardInfo1, shardInfo2, shardInfo3);

        long transactionId = shardManager.beginTransaction();

        shardManager.commitShards(
                transactionId,
                tableId,
                ImmutableList.of(
                        new ColumnInfo(1, BIGINT),
                        new ColumnInfo(2, DATE)),
                shards,
                Optional.empty(),
                0);

        Slice schema = utf8Slice(DEFAULT_TEST_ORDERS.getSchemaName());
        Slice table = utf8Slice(DEFAULT_TEST_ORDERS.getTableName());

        TupleDomain<Integer> tupleDomain = TupleDomain.withColumnDomains(
                ImmutableMap.<Integer, Domain>builder()
                        .put(0, Domain.singleValue(createVarcharType(10), schema))
                        .put(1, Domain.create(ValueSet.ofRanges(lessThanOrEqual(createVarcharType(10), table)), true))
                        .build());

        List<MaterializedRow> actual;
        try (RecordCursor cursor = new ShardMetadataSystemTable(dbi).cursor(null, SESSION, tupleDomain)) {
            actual = getMaterializedResults(cursor, SHARD_METADATA.getColumns());
        }
        assertEquals(actual.size(), 3);

        List<MaterializedRow> expected = ImmutableList.of(
                new MaterializedRow(DEFAULT_PRECISION, schema, table, utf8Slice(uuid1.toString()), null, 100L, 10L, 1L, utf8Slice("0000000000001234"), milli1, milli1),
                new MaterializedRow(DEFAULT_PRECISION, schema, table, utf8Slice(uuid2.toString()), null, 200L, 20L, 2L, utf8Slice("cafebabedeadbeef"), milli1, milli2),
                new MaterializedRow(DEFAULT_PRECISION, schema, table, utf8Slice(uuid3.toString()), null, 300L, 30L, 3L, utf8Slice("fedcba0987654321"), milli2, milli2));

        assertEquals(actual, expected);
    }

    @Test
    public void testDateTemporalColumnWithFilter()
            throws Exception
    {
        ShardManager shardManager = createShardManager(dbi);

        DateTime date1 = new DateTime(2017, 1, 1, 0, 0, 0, DateTimeZone.UTC);
        DateTime date2 = new DateTime(2017, 1, 2, 0, 0, 0, DateTimeZone.UTC);

        // These values are what the cursor should display
        long milli1 = date1.getMillis();
        long milli2 = date2.getMillis();

        // These values are stored in the index shard table (e.g. x_shards_table1)
        int days1 = (int) (milli1 / TimeUnit.DAYS.toMillis(1));
        int days2 = (int) (milli2 / TimeUnit.DAYS.toMillis(1));

        // Add shards to the table
        long tableId = 1;
        OptionalInt bucketNumber = OptionalInt.empty();
        UUID uuid1 = UUID.randomUUID();
        UUID uuid2 = UUID.randomUUID();
        UUID uuid3 = UUID.randomUUID();

        ColumnStats columnStats1 = new ColumnStats(2, days1, days1);
        ColumnStats columnStats2 = new ColumnStats(2, days1, days2);
        ColumnStats columnStats3 = new ColumnStats(2, days2, days2);

        ShardInfo shardInfo1 = new ShardInfo(uuid1, bucketNumber, ImmutableSet.of("node1"), ImmutableList.of(columnStats1), 1, 10, 100, 0x1234);
        ShardInfo shardInfo2 = new ShardInfo(uuid2, bucketNumber, ImmutableSet.of("node2"), ImmutableList.of(columnStats2), 2, 20, 200, 0xCAFEBABEDEADBEEFL);
        ShardInfo shardInfo3 = new ShardInfo(uuid3, bucketNumber, ImmutableSet.of("node3"), ImmutableList.of(columnStats3), 3, 30, 300, 0xFEDCBA0987654321L);
        List<ShardInfo> shards = ImmutableList.of(shardInfo1, shardInfo2, shardInfo3);

        long transactionId = shardManager.beginTransaction();

        shardManager.commitShards(
                transactionId,
                tableId,
                ImmutableList.of(
                        new ColumnInfo(1, BIGINT),
                        new ColumnInfo(2, DATE)),
                shards,
                Optional.empty(),
                0);

        Slice schema = utf8Slice(DEFAULT_TEST_ORDERS.getSchemaName());
        Slice table = utf8Slice(DEFAULT_TEST_ORDERS.getTableName());

        TupleDomain<Integer> tupleDomain = TupleDomain.withColumnDomains(
                ImmutableMap.<Integer, Domain>builder()
                        .put(0, Domain.singleValue(createVarcharType(10), schema))
                        .put(1, Domain.create(ValueSet.ofRanges(lessThanOrEqual(createVarcharType(10), table)), true))
                        // Should only select shard3
                        .put(8, Domain.create(ValueSet.ofRanges(greaterThan(BIGINT, date1.getMillis())), false))
                        .put(9, Domain.create(ValueSet.ofRanges(greaterThan(BIGINT, date1.getMillis())), false))
                        .build());

        List<MaterializedRow> actual;
        try (RecordCursor cursor = new ShardMetadataSystemTable(dbi).cursor(null, SESSION, tupleDomain)) {
            actual = getMaterializedResults(cursor, SHARD_METADATA.getColumns());
        }
        assertEquals(actual.size(), 1);

        List<MaterializedRow> expected = ImmutableList.of(
                new MaterializedRow(DEFAULT_PRECISION, schema, table, utf8Slice(uuid3.toString()), null, 300L, 30L, 3L, utf8Slice("fedcba0987654321"), milli2, milli2));

        assertEquals(actual, expected);
    }

    @Test
    public void testNoSchemaFilter()
            throws Exception
    {
        // Create "orders" table in a different schema
        metadata.createTable(SESSION, tableMetadataBuilder(new SchemaTableName("other", "orders"))
                .column("orderkey", BIGINT)
                .build());

        // Create another table that should not be selected
        metadata.createTable(SESSION, tableMetadataBuilder(new SchemaTableName("schema1", "foo"))
                .column("orderkey", BIGINT)
                .build());

        TupleDomain<Integer> tupleDomain = TupleDomain.withColumnDomains(
                ImmutableMap.<Integer, Domain>builder()
                        .put(1, Domain.singleValue(createVarcharType(10), utf8Slice("orders")))
                        .build());

        MetadataDao metadataDao = dummyHandle.attach(MetadataDao.class);
        Set<Long> actual = ImmutableSet.copyOf(ShardMetadataRecordCursor.getTableIds(dbi, tupleDomain));
        Set<Long> expected = ImmutableSet.of(
                metadataDao.getTableInformation("other", "orders").getTableId(),
                metadataDao.getTableInformation("test", "orders").getTableId());
        assertEquals(actual, expected);
    }

    @Test
    public void testNoTableFilter()
            throws Exception
    {
        // Create "orders" table in a different schema
        metadata.createTable(SESSION, tableMetadataBuilder(new SchemaTableName("test", "orders2"))
                .column("orderkey", BIGINT)
                .build());

        // Create another table that should not be selected
        metadata.createTable(SESSION, tableMetadataBuilder(new SchemaTableName("schema1", "foo"))
                .column("orderkey", BIGINT)
                .build());

        TupleDomain<Integer> tupleDomain = TupleDomain.withColumnDomains(
                ImmutableMap.<Integer, Domain>builder()
                        .put(0, Domain.singleValue(createVarcharType(10), utf8Slice("test")))
                        .build());

        MetadataDao metadataDao = dummyHandle.attach(MetadataDao.class);
        Set<Long> actual = ImmutableSet.copyOf(ShardMetadataRecordCursor.getTableIds(dbi, tupleDomain));
        Set<Long> expected = ImmutableSet.of(
                metadataDao.getTableInformation("test", "orders").getTableId(),
                metadataDao.getTableInformation("test", "orders2").getTableId());
        assertEquals(actual, expected);
    }

    private static List<MaterializedRow> getMaterializedResults(RecordCursor cursor, List<ColumnMetadata> columns)
    {
        List<Type> types = columns.stream().map(ColumnMetadata::getType).collect(toList());

        ImmutableList.Builder<MaterializedRow> rowBuilder = ImmutableList.builder();
        for (int i = 0; i < types.size(); i++) {
            assertEquals(cursor.getType(i), types.get(i));
        }

        while (cursor.advanceNextPosition()) {
            List<Object> values = new ArrayList<>();
            for (int i = 0; i < columns.size(); i++) {
                Type type = columns.get(i).getType();
                Class<?> javaType = type.getJavaType();
                if (cursor.isNull(i)) {
                    values.add(null);
                }
                else if (javaType == boolean.class) {
                    values.add(cursor.getBoolean(i));
                }
                else if (javaType == long.class) {
                    values.add(cursor.getLong(i));
                }
                else if (javaType == double.class) {
                    values.add(cursor.getDouble(i));
                }
                else if (javaType == Slice.class) {
                    values.add(cursor.getSlice(i));
                }
            }
            rowBuilder.add(new MaterializedRow(DEFAULT_PRECISION, values));
        }
        return rowBuilder.build();
    }
}
