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

import com.facebook.presto.raptor.RaptorConnectorId;
import com.facebook.presto.raptor.RaptorMetadata;
import com.facebook.presto.raptor.metadata.ColumnInfo;
import com.facebook.presto.raptor.metadata.DatabaseShardManager;
import com.facebook.presto.raptor.metadata.ShardDelta;
import com.facebook.presto.raptor.metadata.ShardInfo;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorMetadata;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.Domain;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TupleDomain;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.testing.MaterializedRow;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.json.JsonCodec;
import io.airlift.slice.Slice;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.IDBI;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static com.facebook.presto.metadata.MetadataUtil.TableMetadataBuilder.tableMetadataBuilder;
import static com.facebook.presto.raptor.systemtables.ShardMetadataRecordCursor.SHARD_METADATA;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
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
    private IDBI dbi;

    @BeforeMethod
    public void setup()
    {
        this.dbi = new DBI("jdbc:h2:mem:test" + System.nanoTime());
        this.dummyHandle = dbi.open();
        this.metadata = new RaptorMetadata(new RaptorConnectorId("default"), dbi, new DatabaseShardManager(dbi), SHARD_INFO_CODEC, SHARD_DELTA_CODEC);
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
        // Create table
        ConnectorTableMetadata tableMetadata = tableMetadataBuilder(DEFAULT_TEST_ORDERS)
                .column("orderkey", BIGINT)
                .column("custkey", BIGINT)
                .column("totalprice", DOUBLE)
                .column("orderdate", DATE)
                .column("orderstate", VARCHAR)
                .property("temporal_column", "orderdate")
                .build();
        metadata.createTable(SESSION, tableMetadata);

        DatabaseShardManager shardManager = new DatabaseShardManager(dbi);

        // Add shards to the table
        long tableId = 1;
        UUID uuid1 = UUID.randomUUID();
        UUID uuid2 = UUID.randomUUID();
        UUID uuid3 = UUID.randomUUID();
        ShardInfo shardInfo1 = new ShardInfo(uuid1, ImmutableSet.of("node1"), ImmutableList.of(), 1, 10, 100);
        ShardInfo shardInfo2 = new ShardInfo(uuid2, ImmutableSet.of("node2"), ImmutableList.of(), 2, 20, 200);
        ShardInfo shardInfo3 = new ShardInfo(uuid3, ImmutableSet.of("node3"), ImmutableList.of(), 3, 30, 300);
        List<ShardInfo> shards = ImmutableList.of(shardInfo1, shardInfo2, shardInfo3);

        shardManager.commitShards(
                tableId,
                ImmutableList.of(
                        new ColumnInfo(1, BIGINT),
                        new ColumnInfo(2, BIGINT),
                        new ColumnInfo(3, DOUBLE),
                        new ColumnInfo(4, DATE),
                        new ColumnInfo(5, VARCHAR)),
                shards,
                Optional.empty());

        Slice schema = utf8Slice(DEFAULT_TEST_ORDERS.getSchemaName());
        Slice table = utf8Slice(DEFAULT_TEST_ORDERS.getTableName());

        TupleDomain<Integer> tupleDomain = TupleDomain.withColumnDomains(
                ImmutableMap.<Integer, Domain>builder()
                        .put(0, Domain.singleValue(schema))
                        .put(1, Domain.singleValue(table))
                        .build());

        List<MaterializedRow> actual;
        try (RecordCursor cursor = new ShardMetadataSystemTable(dbi).cursor(SESSION, tupleDomain)) {
            actual = getMaterializedResults(cursor, SHARD_METADATA.getColumns());
        }
        assertEquals(actual.size(), 3);

        List<MaterializedRow> expected = ImmutableList.of(
                new MaterializedRow(DEFAULT_PRECISION, schema, table, utf8Slice(uuid1.toString()), 100, 10, 1),
                new MaterializedRow(DEFAULT_PRECISION, schema, table, utf8Slice(uuid2.toString()), 200, 20, 2),
                new MaterializedRow(DEFAULT_PRECISION, schema, table, utf8Slice(uuid3.toString()), 300, 30, 3));

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
                    continue;
                }
                if (javaType == boolean.class) {
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
