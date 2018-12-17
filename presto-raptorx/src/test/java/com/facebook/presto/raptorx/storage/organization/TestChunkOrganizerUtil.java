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
package com.facebook.presto.raptorx.storage.organization;

import com.facebook.presto.raptorx.metadata.ChunkManager;
import com.facebook.presto.raptorx.metadata.ChunkMetadata;
import com.facebook.presto.raptorx.metadata.ColumnInfo;
import com.facebook.presto.raptorx.metadata.DatabaseChunkManager;
import com.facebook.presto.raptorx.metadata.Metadata;
import com.facebook.presto.raptorx.metadata.NodeIdCache;
import com.facebook.presto.raptorx.metadata.SchemaCreator;
import com.facebook.presto.raptorx.metadata.TableInfo;
import com.facebook.presto.raptorx.metadata.TestingEnvironment;
import com.facebook.presto.raptorx.storage.ChunkInfo;
import com.facebook.presto.raptorx.storage.ColumnStats;
import com.facebook.presto.raptorx.storage.CompressionType;
import com.facebook.presto.raptorx.transaction.Transaction;
import com.facebook.presto.raptorx.transaction.TransactionWriter;
import com.facebook.presto.raptorx.util.TestingDatabase;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import org.jdbi.v3.core.Jdbi;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;

import static com.facebook.presto.raptorx.storage.organization.ChunkOrganizerUtil.getOrganizationEligibleChunks;
import static com.facebook.presto.raptorx.util.DatabaseUtil.createJdbi;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.VarcharType.createVarcharType;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.airlift.slice.Slices.wrappedBuffer;

@Test(singleThreaded = true)
public class TestChunkOrganizerUtil
{
    private File dataDir;
    private TestingDatabase database;
    private ChunkManager chunkManager;
    private Metadata metadata;
    private NodeIdCache nodeIdCache;
    private TransactionWriter transactionWriter;
    @BeforeMethod
    public void setup()
    {
        database = new TestingDatabase();
        new SchemaCreator(database).create();
        dataDir = Files.createTempDir();
        TestingEnvironment environment = new TestingEnvironment(database);
        this.metadata = environment.getMetadata();
        this.nodeIdCache = environment.getNodeIdCache();
        chunkManager = new DatabaseChunkManager(nodeIdCache, database, environment.getTypeManager());
        transactionWriter = environment.getTransactionWriter();
    }

    @AfterMethod(alwaysRun = true)
    public void teardown()
            throws Exception
    {
        database.close();
        deleteRecursively(dataDir.toPath(), ALLOW_INSECURE);
    }

    @Test
    public void testGetOrganizationEligibleChunks()
    {
        int day1 = 1111;
        int day2 = 2222;
        long tableId = metadata.nextTableId();
        String nodeName = "node1";
        long nodeId = nodeIdCache.getNodeId(nodeName);
        Transaction transaction = new Transaction(metadata, metadata.nextTransactionId(), metadata.getCurrentCommitId());
        long distributionId = transaction.createDistribution(ImmutableList.<Type>builder().add(BIGINT).build(),
                ImmutableList.<Long>builder()
                        .add(nodeId)
                        .add(nodeId)
                        .add(nodeId)
                        .build());
        transaction.createTable(
                tableId,
                7,
                "test",
                distributionId,
                OptionalLong.of(20),
                CompressionType.ZSTD,
                System.currentTimeMillis(),
                Optional.empty(),
                ImmutableList.<ColumnInfo>builder()
                        .add(column(10, "orderkey", BIGINT, 1, OptionalInt.of(2)))
                        .add(column(20, "orderdate", DATE, 2, OptionalInt.empty()))
                        .add(column(30, "orderstatus", createVarcharType(3), 3, OptionalInt.of(1)))
                        .build(), false);
        TableInfo tableInfo = transaction.getTableInfo(tableId);
        List<ColumnInfo> tableColumns = tableInfo.getColumns();
        Map<String, ColumnInfo> tableColumnMap = Maps.uniqueIndex(tableColumns, ColumnInfo::getColumnName);

        long orderDate = tableColumnMap.get("orderdate").getColumnId();
        long orderKey = tableColumnMap.get("orderkey").getColumnId();
        long orderStatus = tableColumnMap.get("orderstatus").getColumnId();

        List<ChunkInfo> chunks = ImmutableList.<ChunkInfo>builder()
                .add(chunkInfo(
                        100,
                        1,
                        ImmutableList.of(
                                new ColumnStats(orderDate, day1, day1 + 10),
                                new ColumnStats(orderKey, 13L, 14L),
                                new ColumnStats(orderStatus, "aaa".getBytes(), "abc".getBytes()))))
                .add(chunkInfo(
                        101,
                        2,
                        ImmutableList.of(
                                new ColumnStats(orderDate, day2, day2 + 100),
                                new ColumnStats(orderKey, 2L, 20L),
                                new ColumnStats(orderStatus, "aaa".getBytes(), "abc".getBytes()))))
                .add(chunkInfo(
                        102,
                        1,
                        ImmutableList.of(
                                new ColumnStats(orderDate, day1, day2),
                                new ColumnStats(orderKey, 2L, 11L),
                                new ColumnStats(orderStatus, "aaa".getBytes(), "abc".getBytes()))))
                .add(chunkInfo(
                        103,
                        2,
                        ImmutableList.of(
                                new ColumnStats(orderDate, day1, day2),
                                new ColumnStats(orderKey, 2L, null),
                                new ColumnStats(orderStatus, "aaa".getBytes(), "abc".getBytes()))))
                .add(chunkInfo(
                        104,
                        2,
                        ImmutableList.of(
                                new ColumnStats(orderDate, day1, day2 + 200),
                                new ColumnStats(orderKey, 2L, 11L),
                                new ColumnStats(orderStatus, "aaa".getBytes(), "abc".getBytes()))))
                .build();

        transaction.insertChunks(tableId, chunks);
        transactionWriter.write(transaction.getActions(), OptionalLong.empty());
        Set<ChunkMetadata> chunkMetadatas = chunkManager.getNodeChunkMetas(nodeName);

        Long temporalColumnId = tableInfo.getTemporalColumnId().getAsLong();
        ColumnInfo temporalColumn = tableInfo.getTemporalColumn().get();
        List<Jdbi> shardDbi = database.getShards().stream()
                .map(shard -> createJdbi(shard.getConnection()))
                .collect(toImmutableList());
        Set<ChunkIndexInfo> actual = ImmutableSet.copyOf(getOrganizationEligibleChunks(shardDbi, tableInfo, chunkMetadatas, false));
        List<ChunkIndexInfo> expected = getChunkIndexInfo(tableInfo, chunks, temporalColumn, Optional.empty());

        assertEquals(actual, expected);

        List<ColumnInfo> sortColumns = tableInfo.getSortColumns();
        Set<ChunkIndexInfo> actualSortRange = ImmutableSet.copyOf(getOrganizationEligibleChunks(shardDbi, tableInfo, chunkMetadatas, true));
        List<ChunkIndexInfo> expectedSortRange = getChunkIndexInfo(tableInfo, chunks, temporalColumn, Optional.of(sortColumns));

        assertEquals(actualSortRange, expectedSortRange);
    }

    private static List<ChunkIndexInfo> getChunkIndexInfo(TableInfo tableInfo, List<ChunkInfo> chunks, ColumnInfo temporalColumn, Optional<List<ColumnInfo>> sortColumns)
    {
        long tableId = tableInfo.getTableId();
        Type temporalType = temporalColumn.getType();

        ImmutableList.Builder<ChunkIndexInfo> builder = ImmutableList.builder();
        for (ChunkInfo chunk : chunks) {
            ColumnStats temporalColumnStats = chunk.getColumnStats().stream()
                    .filter(columnStats -> columnStats.getColumnId() == temporalColumn.getColumnId())
                    .findFirst()
                    .get();

            if (temporalColumnStats.getMin() == null || temporalColumnStats.getMax() == null) {
                continue;
            }

            Optional<ChunkRange> sortRange = Optional.empty();
            if (sortColumns.isPresent()) {
                Map<Long, ColumnStats> columnIdToStats = Maps.uniqueIndex(chunk.getColumnStats(), ColumnStats::getColumnId);
                ImmutableList.Builder<Type> typesBuilder = ImmutableList.builder();
                ImmutableList.Builder<Object> minBuilder = ImmutableList.builder();
                ImmutableList.Builder<Object> maxBuilder = ImmutableList.builder();
                boolean isChunkEligible = true;
                for (ColumnInfo sortColumn : sortColumns.get()) {
                    ColumnStats columnStats = columnIdToStats.get(sortColumn.getColumnId());
                    typesBuilder.add(sortColumn.getType());

                    if (columnStats.getMin() == null || columnStats.getMax() == null) {
                        isChunkEligible = false;
                        break;
                    }

                    minBuilder.add(convertValue(columnStats.getMin()));
                    maxBuilder.add(convertValue(columnStats.getMax()));
                }

                if (!isChunkEligible) {
                    continue;
                }

                List<Type> types = typesBuilder.build();
                List<Object> minValues = minBuilder.build();
                List<Object> maxValues = maxBuilder.build();
                sortRange = Optional.of(ChunkRange.of(new Tuple(types, minValues), new Tuple(types, maxValues)));
            }
            builder.add(new ChunkIndexInfo(
                    tableId,
                    chunk.getBucketNumber(),
                    chunk.getChunkId(),
                    chunk.getRowCount(),
                    chunk.getUncompressedSize(),
                    sortRange,
                    Optional.of(ChunkRange.of(
                            new Tuple(temporalType, temporalColumnStats.getMin()),
                            new Tuple(temporalType, temporalColumnStats.getMax())))));
        }
        return builder.build();
    }

    public static Object convertValue(Object o)
    {
        if (o instanceof byte[]) {
            return wrappedBuffer((byte[]) o).toStringUtf8();
        }
        else {
            return o;
        }
    }

    public static ColumnInfo column(long columnId, String name, Type type, int ordinal, OptionalInt sortOrdinal)
    {
        return new ColumnInfo(columnId, name, type, Optional.empty(), ordinal, OptionalInt.empty(), sortOrdinal);
    }

    public static ChunkInfo chunkInfo(long chunkId, int bucketNumber, List<ColumnStats> columnStats)
    {
        return new ChunkInfo(chunkId, bucketNumber, columnStats, 0, 0, 0, 0);
    }
}
