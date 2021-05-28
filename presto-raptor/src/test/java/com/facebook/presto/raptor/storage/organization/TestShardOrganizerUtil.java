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
package com.facebook.presto.raptor.storage.organization;

import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.raptor.RaptorMetadata;
import com.facebook.presto.raptor.metadata.ColumnInfo;
import com.facebook.presto.raptor.metadata.ColumnStats;
import com.facebook.presto.raptor.metadata.MetadataDao;
import com.facebook.presto.raptor.metadata.ShardInfo;
import com.facebook.presto.raptor.metadata.ShardManager;
import com.facebook.presto.raptor.metadata.ShardMetadata;
import com.facebook.presto.raptor.metadata.Table;
import com.facebook.presto.raptor.metadata.TableColumn;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.common.type.VarcharType.createVarcharType;
import static com.facebook.presto.metadata.FunctionAndTypeManager.createTestFunctionAndTypeManager;
import static com.facebook.presto.metadata.MetadataUtil.TableMetadataBuilder.tableMetadataBuilder;
import static com.facebook.presto.raptor.metadata.SchemaDaoUtil.createTablesWithRetry;
import static com.facebook.presto.raptor.metadata.TestDatabaseShardManager.createShardManager;
import static com.facebook.presto.raptor.metadata.TestDatabaseShardManager.shardInfo;
import static com.facebook.presto.raptor.storage.organization.ShardOrganizerUtil.getOrganizationEligibleShards;
import static com.facebook.presto.testing.TestingConnectorSession.SESSION;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestShardOrganizerUtil
{
    private static final List<ColumnInfo> COLUMNS = ImmutableList.of(
            new ColumnInfo(1, TIMESTAMP),
            new ColumnInfo(2, BIGINT),
            new ColumnInfo(3, VARCHAR));

    private DBI dbi;
    private Handle dummyHandle;
    private File dataDir;
    private ShardManager shardManager;
    private MetadataDao metadataDao;
    private ConnectorMetadata metadata;

    @BeforeMethod
    public void setup()
    {
        FunctionAndTypeManager functionAndTypeManager = createTestFunctionAndTypeManager();
        dbi = new DBI("jdbc:h2:mem:test" + System.nanoTime() + "_" + ThreadLocalRandom.current().nextInt());
        dbi.registerMapper(new TableColumn.Mapper(functionAndTypeManager));
        dummyHandle = dbi.open();
        createTablesWithRetry(dbi);
        dataDir = Files.createTempDir();

        metadata = new RaptorMetadata("raptor", dbi, createShardManager(dbi), functionAndTypeManager);

        metadataDao = dbi.onDemand(MetadataDao.class);
        shardManager = createShardManager(dbi);
    }

    @AfterMethod(alwaysRun = true)
    public void teardown()
            throws Exception
    {
        dummyHandle.close();
        deleteRecursively(dataDir.toPath(), ALLOW_INSECURE);
    }

    @Test
    public void testGetOrganizationEligibleShards()
    {
        int day1 = 1111;
        int day2 = 2222;

        SchemaTableName tableName = new SchemaTableName("default", "test");
        metadata.createTable(SESSION, tableMetadataBuilder(tableName)
                        .column("orderkey", BIGINT)
                        .column("orderdate", DATE)
                        .column("orderstatus", createVarcharType(3))
                        .property("ordering", ImmutableList.of("orderstatus", "orderkey"))
                        .property("temporal_column", "orderdate")
                        .property("table_supports_delta_delete", false)
                        .build(),
                false);
        Table tableInfo = metadataDao.getTableInformation(tableName.getSchemaName(), tableName.getTableName());
        List<TableColumn> tableColumns = metadataDao.listTableColumns(tableInfo.getTableId());
        Map<String, TableColumn> tableColumnMap = Maps.uniqueIndex(tableColumns, TableColumn::getColumnName);

        long orderDate = tableColumnMap.get("orderdate").getColumnId();
        long orderKey = tableColumnMap.get("orderkey").getColumnId();
        long orderStatus = tableColumnMap.get("orderstatus").getColumnId();

        List<ShardInfo> shards = ImmutableList.<ShardInfo>builder()
                .add(shardInfo(
                        UUID.randomUUID(),
                        "node1",
                        ImmutableList.of(
                                new ColumnStats(orderDate, day1, day1 + 10),
                                new ColumnStats(orderKey, 13L, 14L),
                                new ColumnStats(orderStatus, "aaa", "abc"))))
                .add(shardInfo(
                        UUID.randomUUID(),
                        "node1",
                        ImmutableList.of(
                                new ColumnStats(orderDate, day2, day2 + 100),
                                new ColumnStats(orderKey, 2L, 20L),
                                new ColumnStats(orderStatus, "aaa", "abc"))))
                .add(shardInfo(
                        UUID.randomUUID(),
                        "node1",
                        ImmutableList.of(
                                new ColumnStats(orderDate, day1, day2),
                                new ColumnStats(orderKey, 2L, 11L),
                                new ColumnStats(orderStatus, "aaa", "abc"))))
                .add(shardInfo(
                        UUID.randomUUID(),
                        "node1",
                        ImmutableList.of(
                                new ColumnStats(orderDate, day1, day2),
                                new ColumnStats(orderKey, 2L, null),
                                new ColumnStats(orderStatus, "aaa", "abc"))))
                .add(shardInfo(
                        UUID.randomUUID(),
                        "node1",
                        ImmutableList.of(
                                new ColumnStats(orderDate, day1, null),
                                new ColumnStats(orderKey, 2L, 11L),
                                new ColumnStats(orderStatus, "aaa", "abc"))))
                .build();

        long transactionId = shardManager.beginTransaction();
        shardManager.commitShards(transactionId, tableInfo.getTableId(), COLUMNS, shards, Optional.empty(), 0);
        Set<ShardMetadata> shardMetadatas = shardManager.getNodeShardsAndDeltas("node1");

        Long temporalColumnId = metadataDao.getTemporalColumnId(tableInfo.getTableId());
        TableColumn temporalColumn = metadataDao.getTableColumn(tableInfo.getTableId(), temporalColumnId);

        Set<ShardIndexInfo> actual = ImmutableSet.copyOf(getOrganizationEligibleShards(dbi, metadataDao, tableInfo, shardMetadatas, false));
        List<ShardIndexInfo> expected = getShardIndexInfo(tableInfo, shards, temporalColumn, Optional.empty());

        assertEquals(actual, expected);

        List<TableColumn> sortColumns = metadataDao.listSortColumns(tableInfo.getTableId());
        Set<ShardIndexInfo> actualSortRange = ImmutableSet.copyOf(getOrganizationEligibleShards(dbi, metadataDao, tableInfo, shardMetadatas, true));
        List<ShardIndexInfo> expectedSortRange = getShardIndexInfo(tableInfo, shards, temporalColumn, Optional.of(sortColumns));

        assertEquals(actualSortRange, expectedSortRange);
    }

    private static List<ShardIndexInfo> getShardIndexInfo(Table tableInfo, List<ShardInfo> shards, TableColumn temporalColumn, Optional<List<TableColumn>> sortColumns)
    {
        long tableId = tableInfo.getTableId();
        Type temporalType = temporalColumn.getDataType();

        ImmutableList.Builder<ShardIndexInfo> builder = ImmutableList.builder();
        for (ShardInfo shard : shards) {
            ColumnStats temporalColumnStats = shard.getColumnStats().stream()
                    .filter(columnStats -> columnStats.getColumnId() == temporalColumn.getColumnId())
                    .findFirst()
                    .get();

            if (temporalColumnStats.getMin() == null || temporalColumnStats.getMax() == null) {
                continue;
            }

            Optional<ShardRange> sortRange = Optional.empty();
            if (sortColumns.isPresent()) {
                Map<Long, ColumnStats> columnIdToStats = Maps.uniqueIndex(shard.getColumnStats(), ColumnStats::getColumnId);
                ImmutableList.Builder<Type> typesBuilder = ImmutableList.builder();
                ImmutableList.Builder<Object> minBuilder = ImmutableList.builder();
                ImmutableList.Builder<Object> maxBuilder = ImmutableList.builder();
                boolean isShardEligible = true;
                for (TableColumn sortColumn : sortColumns.get()) {
                    ColumnStats columnStats = columnIdToStats.get(sortColumn.getColumnId());
                    typesBuilder.add(sortColumn.getDataType());

                    if (columnStats.getMin() == null || columnStats.getMax() == null) {
                        isShardEligible = false;
                        break;
                    }

                    minBuilder.add(columnStats.getMin());
                    maxBuilder.add(columnStats.getMax());
                }

                if (!isShardEligible) {
                    continue;
                }

                List<Type> types = typesBuilder.build();
                List<Object> minValues = minBuilder.build();
                List<Object> maxValues = maxBuilder.build();
                sortRange = Optional.of(ShardRange.of(new Tuple(types, minValues), new Tuple(types, maxValues)));
            }
            builder.add(new ShardIndexInfo(
                    tableId,
                    OptionalInt.empty(),
                    shard.getShardUuid(),
                    false,
                    Optional.empty(),
                    shard.getRowCount(),
                    shard.getUncompressedSize(),
                    sortRange,
                    Optional.of(ShardRange.of(
                            new Tuple(temporalType, temporalColumnStats.getMin()),
                            new Tuple(temporalType, temporalColumnStats.getMax())))));
        }
        return builder.build();
    }
}
