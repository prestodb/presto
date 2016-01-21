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
import com.facebook.presto.raptor.metadata.ShardDelta;
import com.facebook.presto.raptor.metadata.ShardInfo;
import com.facebook.presto.raptor.metadata.ShardManager;
import com.facebook.presto.raptor.metadata.Table;
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
import io.airlift.json.JsonCodec;
import io.airlift.slice.Slice;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static com.facebook.presto.metadata.MetadataUtil.TableMetadataBuilder.tableMetadataBuilder;
import static com.facebook.presto.raptor.metadata.SchemaDaoUtil.createTablesWithRetry;
import static com.facebook.presto.raptor.metadata.TestDatabaseShardManager.createShardManager;
import static com.facebook.presto.raptor.systemtables.ShardMetadataRecordCursor.SHARD_METADATA;
import static com.facebook.presto.spi.predicate.Range.lessThanOrEqual;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.testing.MaterializedResult.DEFAULT_PRECISION;
import static com.facebook.presto.testing.TestingConnectorSession.SESSION;
import static io.airlift.json.JsonCodec.jsonCodec;
import static io.airlift.slice.Slices.utf8Slice;
import static java.util.stream.Collectors.toList;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestShardMetadataRecordCursor
{
    private static final JsonCodec<ShardInfo> SHARD_INFO_CODEC = jsonCodec(ShardInfo.class);
    private static final JsonCodec<ShardDelta> SHARD_DELTA_CODEC = jsonCodec(ShardDelta.class);
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
        this.metadata = new RaptorMetadata("raptor", dbi, createShardManager(dbi), SHARD_INFO_CODEC, SHARD_DELTA_CODEC);
        createTablesWithRetry(dbi);

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
    public void testTemporalColumnDate()
            throws Exception
    {
        ShardManager shardManager = createShardManager(dbi);

        SchemaTableName tableName = new SchemaTableName("testsimple", "orders");
        metadata.createTable(SESSION, tableMetadataBuilder(tableName)
                .property("temporal_column", "orderdate")
                .column("orderkey", BIGINT)
                .column("orderdate", DATE)
                .build());

        MetadataDao metadataDao = dummyHandle.attach(MetadataDao.class);
        Table tableInformation = metadataDao.getTableInformation(tableName.getSchemaName(), tableName.getTableName());
        long tableId = tableInformation.getTableId();

        UUID uuid1 = UUID.randomUUID();
        UUID uuid2 = UUID.randomUUID();
        UUID uuid3 = UUID.randomUUID();
        ShardInfo shardInfo1 = new ShardInfo(uuid1, ImmutableSet.of("node1"), ImmutableList.of(new ColumnStats(2, 1, 2)), 1, 10, 100);
        ShardInfo shardInfo2 = new ShardInfo(uuid2, ImmutableSet.of("node2"), ImmutableList.of(new ColumnStats(2, 1, 1)), 2, 20, 200);
        ShardInfo shardInfo3 = new ShardInfo(uuid3, ImmutableSet.of("node3"), ImmutableList.of(new ColumnStats(2, 3, 3)), 3, 30, 300);
        List<ShardInfo> shards = ImmutableList.of(shardInfo1, shardInfo2, shardInfo3);

        long transactionId = shardManager.beginTransaction();

        shardManager.commitShards(
                transactionId,
                tableId,
                ImmutableList.of(
                        new ColumnInfo(1, BIGINT),
                        new ColumnInfo(2, DATE)),
                shards,
                Optional.empty());

        Slice schema = utf8Slice(tableName.getSchemaName());
        Slice table = utf8Slice(tableName.getTableName());

        TupleDomain<Integer> predicate = TupleDomain.all();
        List<MaterializedRow> actual;
        try (RecordCursor cursor = new ShardMetadataSystemTable(dbi).cursor(null, SESSION, predicate)) {
            actual = getMaterializedResults(cursor, SHARD_METADATA.getColumns());
        }
        assertEquals(actual.size(), 3);

        List<MaterializedRow> expected = ImmutableList.of(
                new MaterializedRow(DEFAULT_PRECISION, schema, table, utf8Slice(uuid1.toString()), 100, 10, 1, null, null, 1, 2),
                new MaterializedRow(DEFAULT_PRECISION, schema, table, utf8Slice(uuid2.toString()), 200, 20, 2, null, null, 1, 1),
                new MaterializedRow(DEFAULT_PRECISION, schema, table, utf8Slice(uuid3.toString()), 300, 30, 3, null, null, 3, 3));

        assertEquals(actual, expected);
    }

    @Test
    public void testTemporalColumnTimestamp()
            throws Exception
    {
        ShardManager shardManager = createShardManager(dbi);

        SchemaTableName tableName = new SchemaTableName("testsimple", "orders");
        metadata.createTable(SESSION, tableMetadataBuilder(tableName)
                .property("temporal_column", "orderdate")
                .column("orderkey", BIGINT)
                .column("orderdate", TIMESTAMP)
                .build());

        MetadataDao metadataDao = dummyHandle.attach(MetadataDao.class);
        Table tableInformation = metadataDao.getTableInformation(tableName.getSchemaName(), tableName.getTableName());
        long tableId = tableInformation.getTableId();

        UUID uuid1 = UUID.randomUUID();
        UUID uuid2 = UUID.randomUUID();
        UUID uuid3 = UUID.randomUUID();
        ShardInfo shardInfo1 = new ShardInfo(uuid1, ImmutableSet.of("node1"), ImmutableList.of(new ColumnStats(2, 1, 2)), 1, 10, 100);
        ShardInfo shardInfo2 = new ShardInfo(uuid2, ImmutableSet.of("node2"), ImmutableList.of(new ColumnStats(2, 1, 1)), 2, 20, 200);
        ShardInfo shardInfo3 = new ShardInfo(uuid3, ImmutableSet.of("node3"), ImmutableList.of(new ColumnStats(2, 3, 3)), 3, 30, 300);
        List<ShardInfo> shards = ImmutableList.of(shardInfo1, shardInfo2, shardInfo3);

        long transactionId = shardManager.beginTransaction();

        shardManager.commitShards(
                transactionId,
                tableId,
                ImmutableList.of(
                        new ColumnInfo(1, BIGINT),
                        new ColumnInfo(2, DATE)),
                shards,
                Optional.empty());

        Slice schema = utf8Slice(tableName.getSchemaName());
        Slice table = utf8Slice(tableName.getTableName());

        TupleDomain<Integer> predicate = TupleDomain.all();
        List<MaterializedRow> actual;
        try (RecordCursor cursor = new ShardMetadataSystemTable(dbi).cursor(null, SESSION, predicate)) {
            actual = getMaterializedResults(cursor, SHARD_METADATA.getColumns());
        }
        assertEquals(actual.size(), 3);

        List<MaterializedRow> expected = ImmutableList.of(
                new MaterializedRow(DEFAULT_PRECISION, schema, table, utf8Slice(uuid1.toString()), 100, 10, 1, 1, 2, null, null),
                new MaterializedRow(DEFAULT_PRECISION, schema, table, utf8Slice(uuid2.toString()), 200, 20, 2, 1, 1, null, null),
                new MaterializedRow(DEFAULT_PRECISION, schema, table, utf8Slice(uuid3.toString()), 300, 30, 3, 3, 3, null, null));

        assertEquals(actual, expected);
    }

    @Test
    public void testPredicate()
            throws Exception
    {
        ShardManager shardManager = createShardManager(dbi);

        SchemaTableName tableName = new SchemaTableName("testsimple", "orders");
        metadata.createTable(SESSION, tableMetadataBuilder(tableName)
                .property("temporal_column", "orderdate")
                .column("orderkey", BIGINT)
                .column("orderdate", TIMESTAMP)
                .build());

        MetadataDao metadataDao = dummyHandle.attach(MetadataDao.class);
        Table tableInformation = metadataDao.getTableInformation(tableName.getSchemaName(), tableName.getTableName());
        long tableId = tableInformation.getTableId();

        UUID uuid1 = UUID.randomUUID();
        UUID uuid2 = UUID.randomUUID();
        UUID uuid3 = UUID.randomUUID();
        ShardInfo shardInfo1 = new ShardInfo(uuid1, ImmutableSet.of("node1"), ImmutableList.of(new ColumnStats(2, 1, 2)), 1, 10, 100);
        ShardInfo shardInfo2 = new ShardInfo(uuid2, ImmutableSet.of("node2"), ImmutableList.of(new ColumnStats(2, 1, 1)), 2, 20, 200);
        ShardInfo shardInfo3 = new ShardInfo(uuid3, ImmutableSet.of("node3"), ImmutableList.of(new ColumnStats(2, 3, 3)), 3, 30, 300);
        List<ShardInfo> shards = ImmutableList.of(shardInfo1, shardInfo2, shardInfo3);

        long transactionId = shardManager.beginTransaction();

        shardManager.commitShards(
                transactionId,
                tableId,
                ImmutableList.of(
                        new ColumnInfo(1, BIGINT),
                        new ColumnInfo(2, DATE)),
                shards,
                Optional.empty());

        Slice schema = utf8Slice(tableName.getSchemaName());
        Slice table = utf8Slice(tableName.getTableName());

        TupleDomain<Integer> predicate = TupleDomain.withColumnDomains(
                ImmutableMap.<Integer, Domain>builder()
                        .put(getIndexOf("row_count"), Domain.create(ValueSet.ofRanges(lessThanOrEqual(BIGINT, 2L)), true))
                        .put(getIndexOf("compressed_size"), Domain.create(ValueSet.ofRanges(lessThanOrEqual(BIGINT, 20L)), true))
                        .build());
        List<MaterializedRow> actual;
        try (RecordCursor cursor = new ShardMetadataSystemTable(dbi).cursor(null, SESSION, predicate)) {
            actual = getMaterializedResults(cursor, SHARD_METADATA.getColumns());
        }
        assertEquals(actual.size(), 2);

        List<MaterializedRow> expected = ImmutableList.of(
                new MaterializedRow(DEFAULT_PRECISION, schema, table, utf8Slice(uuid1.toString()), 100, 10, 1, 1, 2, null, null),
                new MaterializedRow(DEFAULT_PRECISION, schema, table, utf8Slice(uuid2.toString()), 200, 20, 2, 1, 1, null, null));

        assertEquals(actual, expected);
    }

    private static int getIndexOf(String columnName)
    {
        List<ColumnMetadata> columns = SHARD_METADATA.getColumns();
        for (int i = 0; i < columns.size(); i++) {
            if (columnName.equals(columns.get(i).getName())) {
                return i;
            }
        }
        throw new IllegalArgumentException("invalid column name");
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
                        .put(1, Domain.singleValue(VARCHAR, utf8Slice("orders")))
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
                        .put(0, Domain.singleValue(VARCHAR, utf8Slice("test")))
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
            List<Object> values = new ArrayList<>(types.size());
            for (int i = 0; i < columns.size(); i++) {
                Type type = columns.get(i).getType();
                Class<?> javaType = type.getJavaType();
                if (cursor.isNull(i)) {
                    values.add(i, null);
                }
                else if (javaType == boolean.class) {
                    values.add(i, cursor.getBoolean(i));
                }
                else if (javaType == long.class) {
                    values.add(i, cursor.getLong(i));
                }
                else if (javaType == double.class) {
                    values.add(i, cursor.getDouble(i));
                }
                else if (javaType == Slice.class) {
                    values.add(i, cursor.getSlice(i));
                }
            }
            rowBuilder.add(new MaterializedRow(DEFAULT_PRECISION, values));
        }
        return rowBuilder.build();
    }
}
